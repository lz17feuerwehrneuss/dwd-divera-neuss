#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD → DIVERA 24/7 (Mitteilungen) – robust:
- Holt DWD-Warnungen (WFS/JSON) für definierte WARNCELLIDs (Default: Neuss 805162024)
- Filter: Mindest-Warnstufe (SEVERITY) + Phänomen (EVENT)
- Warnzeitraum (ONSET/EXPIRES/EFFECTIVE) lokalisiert (mit Zeitzonen-Fallback)
- Retries mit Exponential-Backoff + Jitter für DWD-Abruf
- Deduplizierung via lokaler Datei dwd_seen.json
- DIVERA-Post via accesskey (Query) + ric

Python 3.12/3.13; Abhängigkeiten: requests  (optional: tzdata)
"""

import os
import json
import time
import random
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# =======================
# KONFIGURATION
# =======================

# DIVERA Accesskey (empfohlen als Env-Var)
DIVERA_ACCESSKEY = os.getenv(
    "DIVERA_ACCESSKEY",
    "cbYfiHFy4wprKiMTzimh-B-2PeGAe2oPVu399X7zZxxqDOOkih_NyxkL6jc-qPH2"
)
DIVERA_API_URL = os.getenv("DIVERA_API_URL", "https://app.divera247.com/api/news")
DIVERA_RIC = os.getenv("DIVERA_RIC", "#170001")
DIVERA_GROUP_IDS = os.getenv("DIVERA_GROUP_IDS", "")

# DWD-Zielgebiet(e) – Standard Neuss
WARNCELL_IDS = [x.strip() for x in os.getenv("WARNCELL_IDS", "805162024").split(",") if x.strip()]
if not WARNCELL_IDS:
    WARNCELL_IDS = ["805162024"]  # Fallback, falls Env leer ist

# Filter (leer = kein Filter)
SEVERITY_MIN = (os.getenv("SEVERITY_MIN", "") or "").strip().lower()  # minor|moderate|severe|extreme
EVENT_ALLOW = [x.strip().lower() for x in os.getenv("EVENT_ALLOW", "").split(",") if x.strip()]
EVENT_DENY  = [x.strip().lower() for x in os.getenv("EVENT_DENY", "").split(",") if x.strip()]

# Dedupe-Datei
STATE_FILE = Path(os.getenv("STATE_FILE", "dwd_seen.json"))

# HTTP-Parameter & Retries
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ    = int(os.getenv("HTTP_TIMEOUT_READ", "45"))
HTTP_RETRIES         = int(os.getenv("HTTP_RETRIES", "5"))  # inkl. Erstversuch
HEADERS = {"User-Agent": "dwd2divera/1.0 (+github-actions; contact=admin@localhost)"}

# Severity-Ranking
SEVERITY_ORDER = {"":0, "unknown":0, "minor":1, "moderate":2, "severe":3, "extreme":4}

# =======================
# ZEITZONEN-FALLBACK
# =======================

def _get_local_zone():
    """Bevorzugt Europe/Berlin; fällt auf System-Lokalzeit oder UTC zurück."""
    try:
        return ZoneInfo("Europe/Berlin")
    except ZoneInfoNotFoundError:
        pass
    try:
        return datetime.now().astimezone().tzinfo
    except Exception:
        pass
    return ZoneInfo("UTC")

TZ_LOCAL = _get_local_zone()

# =======================
# HILFSFUNKTIONEN
# =======================

def build_dwd_wfs_url(ids: List[str]) -> str:
    ids_join = ",".join(ids)
    return (
        "https://maps.dwd.de/geoserver/dwd/ows"
        "?service=WFS&version=2.0.0&request=GetFeature"
        "&typeName=dwd:Warnungen_Gemeinden"
        f"&CQL_FILTER=WARNCELLID%20IN%20({ids_join})"
        "&outputFormat=application/json"
    )

def load_seen() -> set:
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_seen(seen: set) -> None:
    STATE_FILE.write_text(
        json.dumps(sorted(list(seen)), ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def _get_json_with_retries(url: str) -> Optional[dict]:
    """GET JSON mit Retries, Backoff und Jitter. Gibt None bei endgültigem Fehlschlag zurück."""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(
                url,
                headers=HEADERS,
                timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ)
            )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] DWD Abruf dauerhaft fehlgeschlagen nach {HTTP_RETRIES} Versuchen: {e}")
                return None
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            print(f"[Info] DWD Versuch {attempt} fehlgeschlagen ({e}); retry in {sleep_s:.1f}s …")
            time.sleep(sleep_s)

def _parse_dt_any(s: Optional[str]) -> Optional[datetime]:
    """ISO-Zeit robust parsen (Z/Offset/ohne TZ→UTC) und in lokale TZ konvertieren."""
    if not s:
        return None
    try:
        if s.endswith('Z'):
            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(TZ_LOCAL)
    except Exception:
        return None

def _fmt_dt(dt: Optional[datetime]) -> Optional[str]:
    return dt.strftime("%d.%m.%Y %H:%M") if dt else None

def fetch_dwd_warnings() -> List[Dict[str, Any]]:
    """DWD-Warnungen per WFS (JSON) abrufen und in ein handliches Dict-Format bringen."""
    url = build_dwd_wfs_url(WARNCELL_IDS)
    data = _get_json_with_retries(url)
    if data is None:
        return []

    warnings: List[Dict[str, Any]] = []
    for f in data.get("features", []):
        p = f.get("properties") or {}
        if not p:
            continue
        warnings.append({
            "identifier": p.get("IDENTIFIER"),
            "headline": p.get("HEADLINE"),
            "event": p.get("EVENT"),
            "severity": (p.get("SEVERITY") or "").lower(),
            "urgency": p.get("URGENCY"),
            "certainty": p.get("CERTAINTY"),
            "description": (p.get("DESCRIPTION") or "").strip(),
            "instruction": (p.get("INSTRUCTION") or "").strip(),
            "sent": p.get("SENT"),
            "effective": p.get("EFFECTIVE"),
            "onset": p.get("ONSET"),
            "expires": p.get("EXPIRES"),
            "name": p.get("NAME"),
            "warncellid": p.get("WARNCELLID"),
            "web": p.get("WEB") or "https://www.dwd.de/warnungen",
        })
    return warnings

def passes_filters(w: Dict[str, Any]) -> bool:
    if SEVERITY_MIN:
        sev = w.get("severity") or ""
        if SEVERITY_ORDER.get(sev, 0) < SEVERITY_ORDER.get(SEVERITY_MIN, 0):
            return False
    if EVENT_ALLOW:
        ev = (w.get("event") or "").lower()
        if not any(k in ev for k in EVENT_ALLOW):
            return False
    if EVENT_DENY:
        ev = (w.get("event") or "").lower()
        if any(k in ev for k in EVENT_DENY):
            return False
    return True

def build_divera_payload(w: Dict[str, Any]) -> Dict[str, Any]:
    title = f"DWD-Warnung: {w.get('event') or 'Ereignis'} ({w.get('severity') or '-'})"

    # Zeitfenster
    dt_effective = _fmt_dt(_parse_dt_any(w.get("effective")))
    dt_onset     = _fmt_dt(_parse_dt_any(w.get("onset")))
    dt_expires   = _fmt_dt(_parse_dt_any(w.get("expires")))

    zeitzeile_parts = []
    if dt_onset and dt_expires:
        zeitzeile_parts.append(f"Gültig: {dt_onset}–{dt_expires} Uhr")
    elif dt_effective and dt_expires:
        zeitzeile_parts.append(f"Gültig: {dt_effective}–{dt_expires} Uhr")
    elif dt_expires:
        zeitzeile_parts.append(f"Gültig bis: {dt_expires} Uhr")
    elif dt_onset:
        zeitzeile_parts.append(f"Gültig ab: {dt_onset} Uhr")

    parts = []
    if w.get("headline"):
        parts.append(w["headline"])
    if zeitzeile_parts:
        parts.append(" · ".join(zeitzeile_parts))
    if w.get("description"):
        parts.append(w["description"])
    if w.get("instruction"):
        parts.append(f"⚠️ Hinweise: {w['instruction']}")

    meta = []
    if w.get("name"):
        meta.append(f"Gebiet: {w['name']} [{w.get('warncellid')}]")
    if w.get("sent"):
        sent_loc = _fmt_dt(_parse_dt_any(w.get("sent")))
        if sent_loc:
            meta.append(f"Gesendet: {sent_loc} Uhr")
    if w.get("urgency") or w.get("certainty"):
        meta.append(f"Dringlichkeit: {w.get('urgency')}, Sicherheit: {w.get('certainty')}")
    meta.append(f"Quelle: DWD · {w.get('web')}")
    parts.append("\n".join(meta))

    payload: Dict[str, Any] = {
        "title": title[:120],
        "text": "\n\n".join(parts)[:8000],
    }

    # Zielgruppe
    if DIVERA_GROUP_IDS.strip():
        try:
            payload["group_ids"] = [int(x) for x in DIVERA_GROUP_IDS.split(",") if x.strip()]
        except ValueError:
            pass
    payload["ric"] = DIVERA_RIC

    return payload

def post_to_divera(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not DIVERA_ACCESSKEY or len(DIVERA_ACCESSKEY) < 20:
        raise RuntimeError("DIVERA_ACCESSKEY fehlt/ungültig.")
    u = f"{DIVERA_API_URL}?accesskey={DIVERA_ACCESSKEY}"
    r = requests.post(
        u,
        headers={"Content-Type":"application/json","Accept":"application/json"},
        json=payload,
        timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ)
    )
    if r.status_code >= 300:
        raise RuntimeError(f"DIVERA API Fehler {r.status_code}: {r.text[:500]}")
    try:
        return r.json()
    except Exception:
        return {"status": r.status_code}

# =======================
# MAIN
# =======================

def main() -> None:
    seen = load_seen()
    new_seen = set(seen)

    warnings = fetch_dwd_warnings()  # [] bei DWD-Ausfall
    sent_count = 0

    for w in warnings:
        if not passes_filters(w):
            continue

        ident = w.get("identifier") or f"{w.get('headline')}|{w.get('sent')}|{w.get('warncellid')}"
        if ident in seen:
            continue

        payload = build_divera_payload(w)
        try:
            post_to_divera(payload)
            new_seen.add(ident)
            sent_count += 1
        except Exception as e:
            print(f"[Fehler] DIVERA-Post: {e}")

    save_seen(new_seen)
    print(f"Neue Mitteilungen gesendet: {sent_count}")

if __name__ == "__main__":
    main()
