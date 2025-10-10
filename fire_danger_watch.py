#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fire Danger Warning → DIVERA 24/7

Logik (AND):
  Temp > TMAX_C  UND  rel. Feuchte < RH_MIN  UND  (Wind_Mittel > WIND_MEAN_KMH ODER Böe > WIND_GUST_KMH)

Modi:
- FIRE_MODE=daily  →  1× täglich (07:00 lokal): prüft heutigen Forecast, sendet 1 Mitteilung (dedupe per day)
- FIRE_MODE=acute  →  alle 5 min: Hysterese (nur beim Übergang „unter → über“ senden; re-armen bei „über → unter“)

Beide Mitteilungen mit:
  • „Geltungsdauer von: … bis: …“
  • RIC (Infogruppe)
"""

import os, json, time
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
import requests
from datetime import datetime, date

# -------- ENV / Defaults --------
DIVERA_ACCESSKEY_INFO = os.getenv("DIVERA_ACCESSKEY_INFO", "").strip()
FIRE_RIC              = os.getenv("FIRE_RIC", "#170001").strip()
FIRE_MODE             = os.getenv("FIRE_MODE", "daily").strip().lower()

LAT = float(os.getenv("LAT", "51.15608"))
LON = float(os.getenv("LON", "6.66705"))

TMAX_C         = float(os.getenv("TMAX_C", "30"))
RH_MIN         = float(os.getenv("RH_MIN", "30"))
WIND_MEAN_KMH  = float(os.getenv("WIND_MEAN_KMH", "25"))
WIND_GUST_KMH  = float(os.getenv("WIND_GUST_KMH", "30"))

MODEL_PREF = [m.strip() for m in os.getenv("MODEL_PREF", "icon_d2,icon_eu,gfs").split(",") if m.strip()]
STATE_FILE = Path(os.getenv("STATE_FILE", ".state/fire_state.json"))

HTTP_RETRIES          = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_TIMEOUT_CONNECT  = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ     = int(os.getenv("HTTP_TIMEOUT_READ", "45"))

DIVERA_API = "https://app.divera247.com/api/news"
UA = {"User-Agent":"fire-danger-watch/1.0 (+github actions)"}

# --------------------------------

def _load_state() -> Dict[str, Any]:
    try:
        if STATE_FILE.exists():
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_state(st: Dict[str, Any]):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

def _ok(row: Dict[str,float]) -> bool:
    return (row["t"] > TMAX_C) and (row["rh"] < RH_MIN) and ((row["ws"] > WIND_MEAN_KMH) or (row["wg"] > WIND_GUST_KMH))

def _fmt_de(ts_iso: str) -> str:
    # ts like "2025-10-10T14:00"
    try:
        dt = datetime.fromisoformat(ts_iso)
    except Exception:
        # fallback for "2025-10-10T14:00:00Z" → cut Z
        dt = datetime.fromisoformat(ts_iso.replace("Z",""))
    return dt.strftime("%d.%m.%Y %H:%M Uhr")

def fetch_forecast(model: str, want_current: bool, start_date: Optional[str]=None, end_date: Optional[str]=None) -> Optional[Dict[str,Any]]:
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

    url = "https://api.open-meteo.com/v1/forecast"
    for attempt in range(1, HTTP_RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] Open-Meteo ({model}) fehlgeschlagen: {e}")
                return None
            time.sleep(1.5*attempt)

def best_model_response(want_current: bool, start_date: Optional[str]=None, end_date: Optional[str]=None) -> Tuple[Optional[str], Optional[Dict[str,Any]]]:
    for m in MODEL_PREF:
        data = fetch_forecast(m, want_current=want_current, start_date=start_date, end_date=end_date)
        if data and "hourly" in data and "time" in data["hourly"]:
            return m, data
    return None, None

def _timeseries(data: Dict[str,Any]) -> List[Dict[str,Any]]:
    tz_times = data["hourly"]["time"]
    t  = data["hourly"]["temperature_2m"]
    rh = data["hourly"]["relative_humidity_2m"]
    ws = data["hourly"]["wind_speed_10m"]
    wg = data["hourly"]["wind_gusts_10m"]
    out = []
    for i, ts in enumerate(tz_times):
        out.append({"time":ts, "t":float(t[i]), "rh":float(rh[i]), "ws":float(ws[i]), "wg":float(wg[i])})
    return out

def _timeseries_today(data: Dict[str,Any]) -> List[Dict[str,Any]]:
    today = date.today().isoformat()
    return [r for r in _timeseries(data) if r["time"].startswith(today)]

def _windows_ok(rows: List[Dict[str,Any]]) -> List[Tuple[str,str]]:
    """ Liefert Liste zusammenhängender Zeitfenster (start_iso, end_iso), in denen _ok==True. """
    wins: List[Tuple[str,str]] = []
    cur_start: Optional[str] = None
    last_time: Optional[str] = None
    for r in rows:
        ok = _ok(r)
        if ok and cur_start is None:
            cur_start = r["time"]
        if ok:
            last_time = r["time"]
        if (not ok) and cur_start is not None:
            wins.append((cur_start, last_time))
            cur_start = None
            last_time = None
    if cur_start is not None and last_time is not None:
        wins.append((cur_start, last_time))
    return wins

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
            time.sleep(1.5*attempt)

# ---------- DAILY ----------
def run_daily() -> int:
    """07:00 Lokal: Heutige Vorhersage einmalig senden (dedupe per day) – mit Geltungsdauer."""
    st = _load_state()
    today_str = date.today().isoformat()
    if st.get("daily_sent_date") == today_str:
        print("Daily: bereits heute gesendet – überspringe.")
        return 0

    model, data = best_model_response(want_current=False, start_date=today_str, end_date=today_str)
    if not data:
        print("[Warn] Keine Vorhersagedaten erhalten.")
        return 0

    rows = _timeseries_today(data)
    wins = _windows_ok(rows)
    if not wins:
        print("Daily: Heute kein Zeitfenster mit ALLEN Bedingungen → keine Mitteilung.")
        st["daily_sent_date"] = today_str  # optional: trotzdem markieren, um Mehrfachläufe zu vermeiden
        _save_state(st)
        return 0

    # relevanter Zeitraum = erster Treffer → letzter Treffer des Tages
    start_iso, _ = wins[0]
    _, end_iso   = wins[-1]
    gelt_von = _fmt_de(start_iso)
    # Ende: +1h ans Ende, damit „bis“ intuitiv das Ende der letzten vollen Stunde meint
    end_dt = datetime.fromisoformat(end_iso)
    end_dt = end_dt.replace(minute=0)  # schon auf volle Stunde
    end_dt_str = (end_dt.replace(minute=0)).strftime("%d.%m.%Y %H:00 Uhr")

    title = "🚩 Fire Danger Warning (Forecast)"
    lines = []
    lines.append(f"Modell: {(model or 'auto').upper()}  ·  Standort: Neuss (LAT {LAT:.5f}, LON {LON:.5f})")
    lines.append(f"Geltungsdauer von: {gelt_von} bis: {end_dt_str}")
    lines.append(f"Regel: Temp>{TMAX_C:.0f}°C, rF<{RH_MIN:.0f}%, Wind_Mittel>{WIND_MEAN_KMH:.0f} km/h ODER Böe>{WIND_GUST_KMH:.0f} km/h.")
    text = "\n".join(lines)

    sent = _post_divera(title, text)
    if sent:
        st["daily_sent_date"] = today_str
        _save_state(st)
    return int(bool(sent))

# ---------- ACUTE (Hysterese) ----------
def run_acute() -> int:
    """
    Alle 5 Min: sende beim Übergang „unter → über“.
    Re-arm erst, wenn AND-Regel wieder unterschritten wurde.
    „Geltungsdauer“ = [aktuelles Ereignis Start, prognostiziertes Ende].
    """
    st = _load_state()
    armed = st.get("acute_armed", True)  # True = bereit zu melden
    cur_event_start = st.get("acute_event_start_iso")  # ISO Start des aktuellen Ereignisses (wenn aktiv)

    # Hole current + hourly (für Endzeit-Prognose)
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
        print("[Warn] Unvollständige current-Daten.")
        return 0

    ok_now = (t > TMAX_C) and (rh < RH_MIN) and ((ws > WIND_MEAN_KMH) or (wg > WIND_GUST_KMH))

    if ok_now:
        # Falls wir „bereit“ sind → melden und entwaffnen (armed=False)
        if armed:
            now_iso = datetime.now().strftime("%Y-%m-%dT%H:00")
            # Prognostizierte Endzeit aus Forecast:
            rows = _timeseries(data)
            # suche erstes Zeitfenster, das die aktuelle Stunde enthält
            wins = _windows_ok(rows)
            end_iso = None
            for s,e in wins:
                if s <= now_iso <= e:
                    end_iso = e
                    break
            if end_iso is None:
                # fallback: nächste Stunde, in der Regel nicht erfüllt ist
                end_iso = now_iso

            title = "🚩 Fire Danger Warning"
            parts = [
                f"Modell: {(model or 'auto').upper()}  ·  Standort: Neuss (LAT {LAT:.5f}, LON {LON:.5f})",
                f"Geltungsdauer von: {_fmt_de(now_iso)} bis: {_fmt_de(end_iso)}",
                f"Jetzt-Werte: Temp {t:.1f}°C  (> {TMAX_C:.0f}°C), rF {rh:.0f}% (<{RH_MIN:.0f}%), Wind {ws:.0f} km/h / Böen {wg:.0f} km/h",
                f"Regel erfüllt (Wind_Mittel>{WIND_MEAN_KMH:.0f} ODER Böe>{WIND_GUST_KMH:.0f})."
            ]
            text = "\n".join(parts)

            if _post_divera(title, text):
                st["acute_armed"] = False
                st["acute_event_start_iso"] = now_iso
                _save_state(st)
                print("Acute: Alarm gesendet, armed=False.")
                return 1
            else:
                print("Acute: Senden fehlgeschlagen (bleibt armed=True).")
                return 0
        else:
            # bereits im Ereignis → nichts senden
            print("Acute: Bedingungen weiterhin erfüllt, bereits im Ereignis (armed=False) → keine neue Mitteilung.")
            return 0
    else:
        # Bedingungen nicht erfüllt → ggf. re-armen
        if not armed:
            st["acute_armed"] = True
            st["acute_event_start_iso"] = None
            _save_state(st)
            print("Acute: Bedingungen unterschritten → re-armed (armed=True).")
        else:
            print("Acute: Bedingungen nicht erfüllt (armed bereits True).")
        return 0

def main():
    if FIRE_MODE == "acute":
        n = run_acute()
    else:
        n = run_daily()
    print(f"Done. Sent: {n}")

if __name__ == "__main__":
    main()
