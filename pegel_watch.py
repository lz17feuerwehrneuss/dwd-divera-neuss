#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pegel-Watch -> DIVERA 24/7 (Mitteilung bei Überschreitung der Hochwassermarke)

- Rhein, Pegel Düsseldorf: Abruf via PEGELONLINE REST-API (Wasserstand 'W' in cm)
- Erft, Pegel Neubrück: Abruf via Erftverband "Aktuelle Werte"-Tabelle (HTML parse)
- Schickt eine private Mitteilung an eine RIC (default #170002), sobald ein Pegel
  die konfigurierte Schwelle beim Anstieg ÜBERSCHREITET (Dedupe: nur beim Cross-Up).
- Retries mit Backoff, robuste Zeitausgabe, schöne Titel/Texts.

ENV (Actions Secrets empfohlen):
  DIVERA_ACCESSKEY_SUB   # Mitteilungen erstellen (dein Untereinheit-Key)
  WATER_RIC              # Ziel-RIC (default "#170001")
  TG_*                   # (optional) könnte ergänzt werden, falls du auch TG willst

Optionale ENV:
  DUS_THRESHOLD_CM=710       # Marke I Düsseldorf (cm)
  NEU_THRESHOLD_CM=145       # EV-Einsatzplan Neubrück (cm)
  HTTP_RETRIES=5 HTTP_TIMEOUT_CONNECT=5 HTTP_TIMEOUT_READ=45

Quellen:
- PEGELONLINE Doku: https://www.pegelonline.wsv.de/webservices/rest-api/v2 (Dokuseite) 
- ELWIS Pegel Düsseldorf Marken (M_I = 710 cm)
- Erftverband Pegel Neubrück Warnwerte (EV-Einsatzplan 145 cm)
"""

import os, json, time, random, re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import requests
from datetime import datetime, timezone

# ---------- Konfig & Defaults ----------
DIVERA_ACCESSKEY_SUB = os.getenv("DIVERA_ACCESSKEY_SUB", "").strip()
WATER_RIC = os.getenv("WATER_RIC", "#170002").strip()

DUS_THRESHOLD_CM = int(os.getenv("DUS_THRESHOLD_CM", "710"))   # Rhein Düsseldorf Marke I
NEU_THRESHOLD_CM = int(os.getenv("NEU_THRESHOLD_CM", "145"))   # Erft Neubrück EV-Einsatzplan

HTTP_RETRIES         = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ    = int(os.getenv("HTTP_TIMEOUT_READ", "45"))
UA = {"User-Agent": "pegel-watch/1.0 (+github-actions)"}

STATE_FILE = Path("pegel_seen.json")  # speichert zuletzt bekannten "above/below"-Status pro Pegel

DIVERA_API_URL = "https://app.divera247.com/api/news"
PEGELONLINE_API = "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
ERFT_AKTW_TAB = "https://www.erftverband.de/mapserver/arcshp/flussgebiet/klima_abfluss/howis/html/ev_w_tab_aktwerte.html"

# ---------- HTTP Hilfen ----------
def _jget(url: str, params: Dict[str,Any]=None, headers:Dict[str,str]=None) -> Optional[Dict[str,Any]]:
    params = params or {}
    headers = {**UA, **(headers or {})}
    for attempt in range(1, HTTP_RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] GET {url} failed after {HTTP_RETRIES} tries: {e}")
                return None
            wait = (2**(attempt-1)) + random.uniform(0,0.5)
            print(f"[Info] GET retry {attempt} in {wait:.1f}s … ({e})")
            time.sleep(wait)

def _get_text(url: str) -> Optional[str]:
    for attempt in range(1, HTTP_RETRIES+1):
        try:
            r = requests.get(url, headers=UA, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] GET-TEXT {url} failed after {HTTP_RETRIES} tries: {e}")
                return None
            wait = (2**(attempt-1)) + random.uniform(0,0.5)
            print(f"[Info] GET-TEXT retry {attempt} in {wait:.1f}s … ({e})")
            time.sleep(wait)

def _post_divera(payload: Dict[str,Any]) -> bool:
    if not DIVERA_ACCESSKEY_SUB:
        print("[Warn] DIVERA_ACCESSKEY_SUB fehlt.")
        return False
    u = f"{DIVERA_API_URL}?accesskey={DIVERA_ACCESSKEY_SUB}"
    for attempt in range(1, HTTP_RETRIES+1):
        try:
            r = requests.post(u, json=payload,
                              headers={**UA, "Accept":"application/json", "Content-Type":"application/json"},
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

# ---------- Utils ----------
def _load_state() -> Dict[str,Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_state(st: Dict[str,Any]):
    STATE_FILE.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")

def _fmt_local(ts_iso: str) -> str:
    try:
        if ts_iso.endswith("Z"):
            dt = datetime.fromisoformat(ts_iso.replace("Z","+00:00"))
        else:
            dt = datetime.fromisoformat(ts_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        return ts_iso

# ---------- Rhein Düsseldorf (PEGELONLINE) ----------
def fetch_duesseldorf_cm() -> Tuple[Optional[int], Optional[str]]:
    """
    Liefert (cm, zeitISO) vom Pegel 'DÜSSELDORF' (Rhein) via PEGELONLINE.
    """
    # 1) Stationen per fuzzyId (mit & ohne Umlaut) einschränken
    for fuzzy in ("Düsseldorf", "Duesseldorf", "dusseldorf"):
        params = {
            "waters": "RHEIN",
            "fuzzyId": fuzzy,
            "includeTimeseries": "true",
            "includeCurrentMeasurement": "true"
        }
        data = _jget(f"{PEGELONLINE_API}/stations.json", params=params)
        if not data:
            continue
        # 2) passende Station finden (shortname exakt 'DÜSSELDORF')
        for st in data:
            if (st.get("shortname") or "").upper() == "DÜSSELDORF":
                # timeseries 'W' suchen
                tsmap = st.get("timeseries") or []
                for ts in tsmap:
                    if ts.get("shortname") == "W":
                        cur = ts.get("currentMeasurement") or {}
                        val = cur.get("value")
                        ts_iso = cur.get("timestamp")
                        if val is not None:
                            try:
                                return int(round(float(val))), ts_iso
                            except Exception:
                                return None, ts_iso
        # fallback: nimm die erste Station in Rhein mit 'W' falls 'DÜSSELDORF' nicht exakt passt
        for st in data:
            tsmap = st.get("timeseries") or []
            for ts in tsmap:
                if ts.get("shortname") == "W":
                    cur = ts.get("currentMeasurement") or {}
                    val = cur.get("value")
                    ts_iso = cur.get("timestamp")
                    if val is not None:
                        try:
                            return int(round(float(val))), ts_iso
                        except Exception:
                            return None, ts_iso
    return None, None

# ---------- Erft Neubrück (Erftverband-Tabelle) ----------
def fetch_neubrueck_cm() -> Tuple[Optional[int], Optional[str]]:
    """
    Parst die Erftverband-Tabelle und greift die Zeile 'Neubrück (Erft)' ab.
    Format in der Zeile (Stand 2025-10-05):
    'Neubrück (Erft) | 05.10.25 11:20 | 84 | ...'
    """
    html = _get_text(ERFT_AKTW_TAB)
    if not html:
        return None, None
    # Finde Zeile mit 'Neubrück (Erft)'
    # Danach stehen Datum/Uhrzeit (dd.mm.yy HH:MM) und der Wasserstand (cm)
    row_re = re.compile(r"Neubr\u00fcck\s*\(Erft\).*?(\d{2}\.\d{2}\.\d{2}\s+\d{2}:\d{2}).*?(\d+)\s", re.S)
    m = row_re.search(html)
    if not m:
        # Umlaut-Variante 'Neubrueck' fallback
        row_re2 = re.compile(r"Neubrueck\s*\(Erft\).*?(\d{2}\.\d{2}\.\d{2}\s+\d{2}:\d{2}).*?(\d+)\s", re.S|re.I)
        m = row_re2.search(html)
    if not m:
        return None, None
    when_str, val_str = m.group(1), m.group(2)
    try:
        cm = int(val_str)
    except Exception:
        cm = None
    # Zeit in ISO erzeugen (lokale Angabe -> nehmen wir ohne TZ als UTC-frei)
    try:
        # dd.mm.yy -> 20xx annehmen
        dt = datetime.strptime(when_str, "%d.%m.%y %H:%M")
        ts_iso = dt.isoformat()
    except Exception:
        ts_iso = when_str
    return cm, ts_iso

# ---------- Mitteilung bauen ----------
def build_news_payload(title: str, text: str) -> Dict[str,Any]:
    return {
        "title": title[:120],
        "text": text[:8000],
        "ric": WATER_RIC,
        "private": True
    }

def main():
    state = _load_state()
    total = 0

    # 1) Düsseldorf (Rhein)
    dus_cm, dus_ts = fetch_duesseldorf_cm()
    if dus_cm is not None:
        above_old = bool(state.get("dus_above", False))
        above_new = dus_cm >= DUS_THRESHOLD_CM
        if above_new and not above_old:
            title = f"⚠️ HWM überschritten: Rhein – Pegel Düsseldorf"
            lines = [
                f"Aktueller Wasserstand: {dus_cm} cm (Marke I: {DUS_THRESHOLD_CM} cm)",
            ]
            if dus_ts:
                lines.append(f"Stand: {_fmt_local(dus_ts)} Uhr")
            lines.append("Quelle: PEGELONLINE (WSV)")
            payload = build_news_payload(title, "\n".join(lines))
            if _post_divera(payload):
                total += 1
        # Zustand merken
        state["dus_above"] = above_new

    # 2) Neubrück (Erft)
    neu_cm, neu_ts = fetch_neubrueck_cm()
    if neu_cm is not None:
        above_old = bool(state.get("neu_above", False))
        above_new = neu_cm >= NEU_THRESHOLD_CM
        if above_new and not above_old:
            title = f"⚠️ HWM überschritten: Erft – Pegel Neubrück"
            lines = [
                f"Aktueller Wasserstand: {neu_cm} cm (EV-Einsatzplan: {NEU_THRESHOLD_CM} cm)",
            ]
            if neu_ts:
                lines.append(f"Stand: {neu_ts} (lokale Zeitangabe)")
            lines.append("Quelle: Erftverband – Aktuelle Werte")
            payload = build_news_payload(title, "\n".join(lines))
            if _post_divera(payload):
                total += 1
        state["neu_above"] = above_new

    _save_state(state)
    print(f"Mitteilungen gesendet: {total} (Düsseldorf: {int(state.get('dus_above', False))}, Neubrück: {int(state.get('neu_above', False))})")

if __name__ == "__main__":
    main()




