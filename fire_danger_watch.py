#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fire Danger Warning → DIVERA 24/7

AND-Regel (alle müssen erfüllt sein):
  Temp > TMAX_C  UND  rel. Feuchte < RH_MIN  UND  (Wind_Mittel > WIND_MEAN_KMH ODER Böe > WIND_GUST_KMH)

Modi:
- FIRE_MODE=daily  →  1× täglich (07:00 lokal): prüft heutigen Forecast, sendet 1 Mitteilung (dedupe pro Tag)
- FIRE_MODE=acute  →  alle 5 min: Hysterese (nur beim Übergang „unter → über“ senden; re-armen bei „über → unter“)

Beide Mitteilungen enthalten:
  „Geltungsdauer von: … bis: …“
"""

import os
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple
import requests
from datetime import datetime, date, timedelta

# ===================== ENV / Defaults =====================
DIVERA_ACCESSKEY_INFO = os.getenv("DIVERA_ACCESSKEY_INFO", "").strip()
FIRE_RIC              = os.getenv("FIRE_RIC", "#170001").strip()  # Standard: Infogruppe
FIRE_MODE             = os.getenv("FIRE_MODE", "daily").strip().lower()

LAT = float(os.getenv("LAT", "51.15608"))
LON = float(os.getenv("LON", "6.66705"))

TMAX_C         = float(os.getenv("TMAX_C", "30"))
RH_MIN         = float(os.getenv("RH_MIN", "30"))
WIND_MEAN_KMH  = float(os.getenv("WIND_MEAN_KMH", "25"))
WIND_GUST_KMH  = float(os.getenv("WIND_GUST_KMH", "30"))

# Modell-Priorität (Open-Meteo): icon_d2, icon_eu, gfs
MODEL_PREF = [m.strip() for m in os.getenv("MODEL_PREF", "icon_d2,icon_eu,gfs").split(",") if m.strip()]

STATE_FILE = Path(os.getenv("STATE_FILE", ".state/fire_state.json"))

HTTP_RETRIES          = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_TIMEOUT_CONNECT  = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ     = int(os.getenv("HTTP_TIMEOUT_READ", "45"))

DIVERA_API = "https://app.divera247.com/api/news"
UA = {"User-Agent": "fire-danger-watch/1.1 (+github actions)"}
# ==========================================================


# ===================== State I/O =====================
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
# =====================================================


# ===================== Regeln/Format =====================
def _ok(row: Dict[str, float]) -> bool:
    """AND-Regel: Temp>… UND rF<… UND (Wind_Mittel>… ODER Böe>…)."""
    return (row["t"] > TMAX_C) and (row["rh"] < RH_MIN) and ((row["ws"] > WIND_MEAN_KMH) or (row["wg"] > WIND_GUST_KMH))

def _fmt_de(ts_iso: str) -> str:
    """ISO „YYYY-MM-DDTHH:MM“ → „DD.MM.YYYY HH:MM Uhr“ (lokale TZ wird durch Open-Meteo geliefert)."""
    try:
        dt = datetime.fromisoformat(ts_iso.replace("Z", ""))
        return dt.strftime("%d.%m.%Y %H:%M Uhr")
    except Exception:
        return ts_iso
# =========================================================


# ===================== Forecast Fetch =====================
def fetch_forecast(model: str, want_current: bool, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] Open-Meteo ({model}) fehlgeschlagen: {e}")
                return None
            time.sleep(1.5 * attempt)

def best_model_response(want_current: bool, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    for m in MODEL_PREF:
        data = fetch_forecast(m, want_current=want_current, start_date=start_date, end_date=end_date)
        if data and "hourly" in data and "time" in data["hourly"]:
            return m, data
    return None, None
# ==========================================================


# ===================== Zeitreihe robust =====================
def _timeseries(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Robuste Zeitreihe: Arrays ausrichten, Nulls überspringen, Böen-None → Mittelwind-Fallback."""
    H = data.get("hourly", {}) or {}
    tz_times = H.get("time") or []
    t  = H.get("temperature_2m") or []
    rh = H.get("relative_humidity_2m") or []
    ws = H.get("wind_speed_10m") or []
    wg = H.get("wind_gusts_10m") or []

    n = min(len(tz_times), len(t), len(rh), len(ws), len(wg))
    out: List[Dict[str, Any]] = []
    skipped = 0

    for i in range(n):
        ts = tz_times[i]
        vi_t, vi_rh, vi_ws, vi_wg = t[i], rh[i], ws[i], wg[i]
        if vi_t is None or vi_rh is None or vi_ws is None:
            skipped += 1
            continue
        if vi_wg is None:
            vi_wg = vi_ws
        try:
            out.append({
                "time": ts,
                "t":   float(vi_t),
                "rh":  float(vi_rh),
                "ws":  float(vi_ws),
                "wg":  float(vi_wg),
            })
        except Exception:
            skipped += 1

    if skipped:
        print(f"[Info] _timeseries: {skipped} Stunden wegen Null/Typenproblemen übersprungen.")
    return out

def _timeseries_today(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = _timeseries(data)
    today = date.today().isoformat()
    return [r for r in rows if r["time"].startswith(today)]
# ===========================================================


# ===================== Fensterbildung =====================
def _windows_ok(rows: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """Zusammenhängende Zeitfenster (start_iso, end_iso), in denen die AND-Regel erfüllt ist."""
    wins: List[Tuple[str, str]] = []
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
# ===========================================================


# ===================== DIVERA =====================
def _post_divera(title: str, text: str) -> bool:
    if not DIVERA_ACCESSKEY_INFO:
        print("[Warn] DIVERA_ACCESSKEY_INFO fehlt.")
        return False
    url = f"{DIVERA_API}?accesskey={DIVERA_ACCESSKEY_INFO}"
    body = {
        "title": title[:120],
        "text": text[:8000],
        "ric": FIRE_RIC
    }
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.post(url, json=body,
                              headers={**UA, "Accept": "application/json", "Content-Type": "application/json"},
                              timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            if r.status_code >= 300:
                raise RuntimeError(f"HTTP {r.status_code}: {r.text[:400]}")
            return True
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] DIVERA send failed after {HTTP_RETRIES} tries: {e}")
                return False
            time.sleep(1.5 * attempt)
# ==================================================


# ===================== DAILY =====================
def run_daily() -> int:
    """
    07:00 Lokal: Heutige Vorhersage einmalig senden (dedupe pro Tag) – mit Geltungsdauer.
    """
    st = _load_state()
    today_str = date.today().isoformat()
    if st.get("daily_sent_date") == today_str:
        print("Daily: bereits heute gesendet – überspringe.")
        return 0

    model, data = best_model_response(want_current=False, start_date=today_str, end_date=today_str)
    if not data:
        print("[Warn] Daily: Keine Vorhersagedaten erhalten.")
        return 0

    rows = _timeseries_today(data)
    if not rows:
        print("[Warn] Daily: heute keine validen Stundenwerte – vermutlich Modell-Update-Fenster. Kein Versand.")
        # Optional: nicht als gesendet markieren → später am Tag kann noch gesendet werden
        return 0

    wins = _windows_ok(rows)
    if not wins:
        print("Daily: Heute kein Zeitfenster mit ALLEN Bedingungen → keine Mitteilung.")
        # Optional: als „heute erledigt“ markieren, damit kein Re-Run später am Tag sendet:
        # st["daily_sent_date"] = today_str; _save_state(st)
        return 0

    # relevanter Zeitraum = vom ersten Fenster-Start bis zum Ende des letzten Fensters
    first_start_iso, _ = wins[0]
    _, last_end_iso    = wins[-1]

    # Für „bis“ runden wir auf das Ende der betroffenen Stunde(n):
    try:
        end_dt = datetime.fromisoformat(last_end_iso)
        end_dt_rounded = end_dt.replace(minute=0)  # volle Stunde
        end_dt_str = end_dt_rounded.strftime("%d.%m.%Y %H:00 Uhr")
    except Exception:
        end_dt_str = _fmt_de(last_end_iso)

    title = "🚩 Fire Danger Warning (Forecast)"
    lines = []
    lines.append(f"Modell: {(model or 'auto').upper()}  ·  Standort: Neuss (LAT {LAT:.5f}, LON {LON:.5f})")
    lines.append(f"Geltungsdauer von: {_fmt_de(first_start_iso)} bis: {end_dt_str}")
    lines.append(f"Regel: Temp>{TMAX_C:.0f}°C, rF<{RH_MIN:.0f}%, Wind_Mittel>{WIND_MEAN_KMH:.0f} km/h ODER Böe>{WIND_GUST_KMH:.0f} km/h.")
    text = "\n".join(lines)

    sent = _post_divera(title, text)
    if sent:
        st["daily_sent_date"] = today_str
        _save_state(st)
    return int(bool(sent))
# ==================================================


# ===================== ACUTE (Hysterese) =====================
def run_acute() -> int:
    """
    Alle 5 Min: sende beim Übergang „unter → über“.
    Re-arm erst, wenn AND-Regel wieder unterschritten wurde.
    „Geltungsdauer“ = [aktuelles Ereignis Start, prognostiziertes Ende].
    """
    st = _load_state()
    armed = st.get("acute_armed", True)  # True = bereit für Alarm
    cur_event_start = st.get("acute_event_start_iso")  # ISO der Startstunde des laufenden Ereignisses

    # Hole current + hourly (für Endzeit-Prognose)
    model, data = best_model_response(want_current=True)
    if not data or "current" not in data:
        print("[Warn] Acute: Keine 'current' Daten.")
        return 0

    cur = data["current"]
    try:
        t  = float(cur.get("temperature_2m"))
        rh = float(cur.get("relative_humidity_2m"))
        ws = float(cur.get("wind_speed_10m"))
        wg = cur.get("wind_gusts_10m")
        wg = float(ws if wg is None else wg)  # Fallback für Null-Böen
    except Exception:
        print("[Warn] Acute: Unvollständige current-Daten.")
        return 0

    ok_now = (t > TMAX_C) and (rh < RH_MIN) and ((ws > WIND_MEAN_KMH) or (wg > WIND_GUST_KMH))

    if ok_now:
        if armed:
            # Ereignis startet jetzt (auf volle Stunde runden)
            now = datetime.now().replace(minute=0, second=0, microsecond=0)
            now_iso = now.strftime("%Y-%m-%dT%H:%M")

            # Prognostizierte Endzeit: aus Stundenreihe das Fenster finden, das die Startstunde umfasst
            rows = _timeseries(data)
            if not rows:
                print("[Warn] Acute: keine gültigen Stundenwerte verfügbar. Überspringe ohne Alarm.")
                return 0

            wins = _windows_ok(rows)
            end_iso = None
            for s, e in wins:
                if s <= now_iso <= e:
                    end_iso = e
                    break
            if end_iso is None:
                # Fallback: eine Stunde später
                end_iso = (now + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")

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
                print("Acute: Senden fehlgeschlagen (armed bleibt True).")
                return 0
        else:
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
# ============================================================


def main():
    if FIRE_MODE == "acute":
        sent = run_acute()
    else:
        sent = run_daily()
    print(f"Done. Sent: {sent}")


if __name__ == "__main__":
    main()
