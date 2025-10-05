#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fire Danger Warning → DIVERA 24/7 (Infogruppe)
- Daily-Mode (07:00 lokal): prüft die heutige Vorhersage und sendet 1 Mitteilung,
  wenn es mindestens eine Stunde gibt, in der ALLE Bedingungen gleichzeitig erfüllt sind.
- Acute-Mode (alle 5 Min.): prüft die aktuellen Werte; sendet sofort,
  dedupliziert pro Stunde.

AND-Logik (fest):
  Temperatur > TMAX_C
  UND relative Feuchte < RH_MIN
  UND (Wind_Mittel > WIND_KMH ODER Böe > WIND_KMH)

Quelle: Open-Meteo Forecast API mit Modellwahl ICON-D2 → ICON-EU → GFS.
Variablen: temperature_2m, relative_humidity_2m, wind_speed_10m, wind_gusts_10m.
Einheiten: °C, km/h; Zeitzone Europe/Berlin.

ENV (GitHub Secrets/Vars):
  DIVERA_ACCESSKEY_INFO   # Accesskey zum Erstellen der Mitteilung (Infogruppe)
  FIRE_RIC                # Ziel-RIC, default "#170001"
  FIRE_MODE               # "daily" oder "acute"

Optionale ENV:
  LAT="51.15608" LON="6.66705"
  TMAX_C=30 RH_MIN=30 WIND_KMH=30
  MODEL_PREF="icon_d2,icon_eu,gfs"
  STATE_FILE="fire_seen.json"
  HTTP_RETRIES=5 HTTP_TIMEOUT_CONNECT=5 HTTP_TIMEOUT_READ=45
"""

import os, json, time, random
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
import requests
from datetime import datetime, date, timezone

# ---------- ENV / Defaults ----------
DIVERA_ACCESSKEY_INFO = os.getenv("DIVERA_ACCESSKEY_INFO", "").strip()
FIRE_RIC = os.getenv("FIRE_RIC", "#170001").strip()
FIRE_MODE = os.getenv("FIRE_MODE", "daily").strip().lower()

# Standardkoordinaten: Neuss (51.15608, 6.66705)
LAT = float(os.getenv("LAT", "51.15608"))
LON = float(os.getenv("LON", "6.66705"))

TMAX_C = float(os.getenv("TMAX_C", "30"))
RH_MIN = float(os.getenv("RH_MIN", "30"))
WIND_KMH = float(os.getenv("WIND_KMH", "30"))

MODEL_PREF = [m.strip() for m in os.getenv("MODEL_PREF", "icon_d2,icon_eu,gfs").split(",") if m.strip()]

STATE_FILE = Path(os.getenv("STATE_FILE", "fire_seen.json"))

HTTP_RETRIES         = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ    = int(os.getenv("HTTP_TIMEOUT_READ", "45"))
UA = {"User-Agent": "fire-danger-watch/1.1 (+github-actions)"}

OPEN_METEO = "https://api.open-meteo.com/v1/forecast"
DIVERA_API = "https://app.divera247.com/api/news"

# ---------- HTTP Helpers ----------
def _jget(url: str, params: Dict[str,Any]) -> Optional[Dict[str,Any]]:
    for attempt in range(1, HTTP_RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] GET failed after {HTTP_RETRIES} tries: {e}")
                return None
            wait = (2**(attempt-1)) + random.uniform(0,0.5)
            print(f"[Info] GET retry {attempt} in {wait:.1f}s … ({e})")
            time.sleep(wait)

def _post_divera(title: str, text: str) -> bool:
    if not DIVERA_ACCESSKEY_INFO:
        print("[Warn] DIVERA_ACCESSKEY_INFO fehlt.")
        return False
    url = f"{DIVERA_API}?accesskey={DIVERA_ACCESSKEY_INFO}"
    body = {"title": title[:120], "text": text[:8000], "ric": FIRE_RIC}
    for attempt in range(1, HTTP_RETRIES+1):
        try:
            r = requests.post(url, json=body,
                              headers={**UA, "Accept":"application/json","Content-Type":"application/json"},
                              timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            if r.status_code >= 300:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
            return True
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] DIVERA send failed after {HTTP_RETRIES} tries: {e}")
                return False
            wait = (2**(attempt-1)) + random.uniform(0,0.5)
            print(f"[Info] DIVERA retry {attempt} in {wait:.1f}s … ({e})")
            time.sleep(wait)

# ---------- State ----------
def _load_state() -> Dict[str,Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_state(st: Dict[str,Any]):
    STATE_FILE.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

# ---------- Open-Meteo fetch ----------
def fetch_forecast(model: str,
                   want_current: bool,
                   start_date: Optional[str]=None,
                   end_date: Optional[str]=None) -> Optional[Dict[str,Any]]:
    params = {
        "latitude": LAT,
        "longitude": LON,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_gusts_10m",
        "wind_speed_unit": "kmh",
        "timezone": "Europe/Berlin",
        "models": model,
    }
    if want_current:
        params["current"] = "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_gusts_10m"
    if start_date and end_date:
        params["start_date"] = start_date
        params["end_date"] = end_date
    return _jget(OPEN_METEO, params)

def best_model_response(want_current: bool, start_date: Optional[str]=None, end_date: Optional[str]=None
                        ) -> Tuple[Optional[str], Optional[Dict[str,Any]]]:
    for m in MODEL_PREF:
        data = fetch_forecast(m, want_current, start_date, end_date)
        if data and ("hourly" in data):
            return m, data
    return None, None

# ---------- Logic ----------
def _timeseries_today(data: Dict[str,Any]) -> List[Dict[str,Any]]:
    tz_times = data["hourly"]["time"]
    t = data["hourly"]["temperature_2m"]
    rh = data["hourly"]["relative_humidity_2m"]
    ws = data["hourly"]["wind_speed_10m"]
    wg = data["hourly"]["wind_gusts_10m"]
    out = []
    today = date.today().isoformat()
    for i, ts in enumerate(tz_times):
        if ts.startswith(today):
            out.append({"time":ts, "t":float(t[i]), "rh":float(rh[i]), "ws":float(ws[i]), "wg":float(wg[i])})
    return out

def _first_exceed_all(tsrows: List[Dict[str,Any]]) -> Dict[str, Optional[str]]:
    """
    Liefert erste Uhrzeit, zu der ALLE Bedingungen erfüllt sind,
    sowie Tagesextrema für Infozeile.
    """
    hit_all = None
    tmax = rhmin = None
    wsmax = wgmax = None
    for r in tsrows:
        # Extrema
        tmax  = r["t"]  if tmax is None or r["t"] > tmax else tmax
        rhmin = r["rh"] if rhmin is None or r["rh"] < rhmin else rhmin
        wsmax = r["ws"] if wsmax is None or r["ws"] > wsmax else wsmax
        wgmax = r["wg"] if wgmax is None or r["wg"] > wgmax else wgmax
        # AND-Regel
        if (r["t"] > TMAX_C) and (r["rh"] < RH_MIN) and ((r["ws"] > WIND_KMH) or (r["wg"] > WIND_KMH)):
            if not hit_all:
                hit_all = r["time"][11:16]  # HH:MM
    return {
        "hit_all": hit_all,
        "tmax": f"{tmax:.1f}" if tmax is not None else None,
        "rhmin": f"{rhmin:.0f}" if rhmin is not None else None,
        "wsmax": f"{wsmax:.0f}" if wsmax is not None else None,
        "wgmax": f"{wgmax:.0f}" if wgmax is not None else None,
    }

def _fmt_daily_text(model: str, info: Dict[str,Optional[str]]) -> str:
    lines = []
    lines.append(f"Modell: {model.upper()}  ·  Standort: Neuss (LAT {LAT:.5f}, LON {LON:.5f})")
    if info["hit_all"]:
        lines.append(f"Erste Stunde mit ALLEN Bedingungen ab {info['hit_all']} Uhr.")
    else:
        lines.append("Heute keine Stunde mit gleichzeitiger Überschreitung aller Schwellen.")
    extras = []
    if info["tmax"]:  extras.append(f"Tmax: {info['tmax']}°C (>{TMAX_C:.0f}°C gefordert)")
    if info["rhmin"]: extras.append(f"rF min: {info['rhmin']}% (<{RH_MIN:.0f}% gefordert)")
    if info["wsmax"]: extras.append(f"Wind max: {info['wsmax']} km/h")
    if info["wgmax"]: extras.append(f"Böen max: {info['wgmax']} km/h")
    if extras:
        lines.append(" | ".join(extras))
    lines.append("Regel: Temp > Schwelle, rF < Schwelle, und (Wind_Mittel oder Böe) > Schwelle.")
    return "\n".join(lines)

def run_daily() -> int:
    today = date.today().isoformat()
    model, data = best_model_response(
        want_current=False, start_date=today, end_date=today
    )
    if not data:
        print("[Warn] Keine Vorhersagedaten erhalten.")
        return 0
    rows = _timeseries_today(data)
    info = _first_exceed_all(rows)
    if not info["hit_all"]:
        print("Daily: keine Stunde mit ALLEN Bedingungen → keine Mitteilung.")
        return 0

    title = "🚩 Fire Danger Warning"
    text = _fmt_daily_text(model or "auto", info)
    sent = _post_divera(title, text)
    return int(bool(sent))

def run_acute() -> int:
    # current prüfen (AND-Regel), dedupe pro Stunde
    st = _load_state()
    now_hr_key = datetime.now().strftime("%Y-%m-%dT%H")
    last_sent_hr = st.get("acute_last_sent_hr")

    model, data = best_model_response(want_current=True)
    if not data or "current" not in data:
        print("[Warn] Keine 'current' Daten.")
        return 0

    cur = data["current"]
    try:
        t  = float(cur.get("temperature_2m"))
        rh = float(cur.get("relative_humidity_2m"))
        ws = float(cur.get("wind_speed_10m"))
        wg = float(cur.get("wind_gusts_10m"))
    except Exception:
        print("[Warn] Ungültige 'current'-Werte.")
        return 0

    ok = (t > TMAX_C) and (rh < RH_MIN) and ((ws > WIND_KMH) or (wg > WIND_KMH))
    if not ok:
        print("Acute: AND-Regel nicht erfüllt → keine Mitteilung.")
        return 0

    if last_sent_hr == now_hr_key:
        print("Acute: bereits in dieser Stunde gemeldet.")
        return 0

    parts = [
        f"Modell: {(model or 'auto').upper()}  ·  Standort: Neuss (LAT {LAT:.5f}, LON {LON:.5f})",
        f"Temp jetzt {t:.1f}°C  (> {TMAX_C:.0f}°C)",
        f"rF jetzt {rh:.0f}%   (< {RH_MIN:.0f}%)",
        f"Wind jetzt {ws:.0f} km/h  /  Böen {wg:.0f} km/h  (> {WIND_KMH:.0f} km/h gefordert für mind. eine der beiden)",
        "Regel: Temp > Schwelle, rF < Schwelle, und (Wind_Mittel oder Böe) > Schwelle."
    ]
    title = "🚩 Fire Danger Warning"
    text = "\n".join(parts)

    sent = _post_divera(title, text)
    if sent:
        st["acute_last_sent_hr"] = now_hr_key
        _save_state(st)
    return int(bool(sent))

def main():
    if FIRE_MODE == "acute":
        n = run_acute()
        print(f"Mitteilungen gesendet (acute): {n}")
    else:
        n = run_daily()
        print(f"Mitteilungen gesendet (daily): {n}")

if __name__ == "__main__":
    main()
