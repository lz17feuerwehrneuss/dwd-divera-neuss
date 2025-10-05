#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD → DIVERA 24/7 (Mitteilungen) – Dual-RIC & NRW-Warnlagebericht-Anhang mit Cache & Log
Sicherheits-Hardening (unverändert):
- KEIN hartkodierter Accesskey: DIVERA_ACCESSKEY MUSS als Secret/ENV gesetzt sein
- Fehlerlogs ohne Response-Body/URL (Leak-Prevention)
Neu:
- Schwellwerte über STUFEN 1–4 statt englischer Begriffe
  Mapping: 1=minor, 2=moderate, 3=severe, 4=extreme
  ENV:
    SEVERITY_MIN_LEVEL   -> globale Mindeststufe (leer = kein Filter)
    RIC2_THRESHOLD_LEVEL -> Mindeststufe für Einsatzabteilung (Default 3)
Titel: zeigt „(Stufe X)“ statt englischer Severity.
"""

import os
import re
import json
import time
import random
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional

import requests
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# =======================
# KONFIGURATION
# =======================

# --- DIVERA ---
DIVERA_ACCESSKEY = os.getenv("DIVERA_ACCESSKEY", "").strip()  # <— MUSS per Secret gesetzt sein!
DIVERA_API_URL = os.getenv("DIVERA_API_URL", "https://app.divera247.com/api/news").strip()

# RICs
RIC_INFO    = os.getenv("RIC_INFO",   "#170001").strip()  # Infogruppe: alle Warnungen
RIC_EINSATZ = os.getenv("RIC_EINSATZ","#170002").strip()  # Einsatzabteilung: ab Schwellstufe

# Optional: zusätzliche group_ids per ENV (Komma)
DIVERA_GROUP_IDS_INFO    = os.getenv("DIVERA_GROUP_IDS_INFO", "").strip()
DIVERA_GROUP_IDS_EINSATZ = os.getenv("DIVERA_GROUP_IDS_EINSATZ", "").strip()

# --- DWD-Warnzellen ---
WARNCELL_IDS = [x.strip() for x in os.getenv("WARNCELL_IDS", "805162024").split(",") if x.strip()]
if not WARNCELL_IDS:
    WARNCELL_IDS = ["805162024"]

# --- Filter global ---
# Neu: Level (1..4) statt englischer Severity-Strings
SEVERITY_MIN_LEVEL = os.getenv("SEVERITY_MIN_LEVEL", "").strip()  # "", "1", "2", "3", "4"
EVENT_ALLOW = [x.strip().lower() for x in os.getenv("EVENT_ALLOW", "").split(",") if x.strip()]
EVENT_DENY  = [x.strip().lower() for x in os.getenv("EVENT_DENY", "").split(",") if x.strip()]

# Unwetter-Schwelle für die Einsatzabteilung → Level (Default 3=severe)
RIC2_THRESHOLD_LEVEL = int(os.getenv("RIC2_THRESHOLD_LEVEL", "3"))

# Dedupe-Datei
STATE_FILE = Path(os.getenv("STATE_FILE", "dwd_seen.json"))

# HTTP & Retries
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ    = int(os.getenv("HTTP_TIMEOUT_READ", "45"))
HTTP_RETRIES         = int(os.getenv("HTTP_RETRIES", "5"))
HEADERS = {"User-Agent": "dwd2divera/1.7 (+github-actions; contact=ops@localhost)"}

# Severity-Mapping
# Quelle: DWD CAP – severity: minor/moderate/severe/extreme → 1..4
SEVERITY_TO_LEVEL = {
    "minor":    1,
    "moderate": 2,
    "severe":   3,
    "extreme":  4,
}
LEVEL_TO_SEVERITY = {v: k for k, v in SEVERITY_TO_LEVEL.items()}

# --- NRW Warnlagebericht-Append ---
APPEND_WARNLAGE = (os.getenv("APPEND_WARNLAGE", "true").lower() in ("1","true","yes","on"))
WARNLAGE_URL_NRW = os.getenv("WARNLAGE_URL_NRW",
    "https://www.dwd.de/DE/wetter/warnungen_aktuell/warnlagebericht/nordrhein_westfalen/warnlage_nrw_node.html"
).strip()
APPEND_SEPARATOR = os.getenv("APPEND_SEPARATOR", "\n\n──────────\n")

# Cache-Datei für den Warnlagebericht
WARNLAGE_CACHE_FILE = Path(os.getenv("WARNLAGE_CACHE_FILE", "warnlage_cache.json"))
WARNLAGE_TTL_SECONDS = int(os.getenv("WARNLAGE_TTL_SECONDS", str(12 * 3600)))  # 12h

# =======================
# ZEITZONEN-FALLBACK
# =======================

def _get_local_zone():
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
# HTTP/JSON
# =======================

def _get_json_with_retries(url: str) -> Optional[dict]:
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] GET failed after {HTTP_RETRIES} tries: {type(e).__name__}")
                return None
            time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.5))

def _get_text_with_retries(url: str) -> Optional[str]:
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] GET(text) failed after {HTTP_RETRIES} tries: {type(e).__name__}")
                return None
            time.sleep((2 ** (attempt - 1)) + random.uniform(0, 0.5))

# =======================
# DWD WARNUNGEN (WFS)
# =======================

DWD_WFS = "https://maps.dwd.de/geoserver/dwd/ows"

def build_dwd_wfs_url(ids: List[str]) -> str:
    ids_join = ",".join(ids)
    return (
        f"{DWD_WFS}?service=WFS&version=2.0.0&request=GetFeature"
        f"&typeName=dwd:Warnungen_Gemeinden&CQL_FILTER=WARNCELLID%20IN%20({ids_join})"
        f"&outputFormat=application/json"
    )

def fetch_dwd_warnings() -> List[Dict[str, Any]]:
    data = _get_json_with_retries(build_dwd_wfs_url(WARNCELL_IDS))
    if data is None:
        return []
    out: List[Dict[str, Any]] = []
    for f in data.get("features", []):
        p = f.get("properties") or {}
        if not p: continue
        sev_str = (p.get("SEVERITY") or "").lower().strip()
        sev_level = SEVERITY_TO_LEVEL.get(sev_str, 0)
        out.append({
            "identifier": p.get("IDENTIFIER"),
            "headline": p.get("HEADLINE"),
            "event": p.get("EVENT"),
            "severity": sev_str,          # original
            "level": sev_level,           # NEU: numerische Stufe
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
    return out

# =======================
# FILTER & ZEIT
# =======================

def _parse_int_or_empty(v: str) -> Optional[int]:
    v = v.strip()
    if not v:
        return None
    try:
        n = int(v)
        if 1 <= n <= 4:
            return n
    except Exception:
        pass
    return None

def passes_filters_global(w: Dict[str, Any]) -> bool:
    min_level = _parse_int_or_empty(SEVERITY_MIN_LEVEL)
    if min_level is not None:
        if int(w.get("level", 0)) < min_level:
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

def is_unwetter_for_ric2(w: Dict[str, Any]) -> bool:
    try:
        level = int(w.get("level", 0))
    except Exception:
        level = 0
    return level >= int(RIC2_THRESHOLD_LEVEL)

def _parse_dt_any(s: Optional[str]) -> Optional[datetime]:
    if not s: return None
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

# =======================
# WARNLAGEBERICHT + CACHE
# =======================

def _html_to_text(segment: str) -> str:
    segment = re.sub(r"<script[^>]*>.*?</script>", " ", segment, flags=re.S|re.I)
    segment = re.sub(r"<style[^>]*>.*?</style>", " ", segment, flags=re.S|re.I)
    text = re.sub(r"<[^>]+>", " ", segment)
    return re.sub(r"\s+", " ", text).strip()

def _extract_issue_str(html: str) -> Optional[str]:
    norm = re.sub(r"\s+", " ", html)
    for pat in [
        r"(Ausgegeben|Ausgabe|Stand)\s*[:vom]*\s*\d{2}\.\d{2}\.\d{4}[, ]+\d{1,2}:\d{2}\s*Uhr",
        r"(Ausgegeben|Ausgabe|Stand)\s*[:vom]*\s*\d{2}\.\d{2}\.\d{2}[, ]+\d{1,2}:\d{2}\s*Uhr",
    ]:
        m = re.search(pat, norm, flags=re.I)
        if m: return _html_to_text(m.group(0))
    return None

def _extract_entwicklung_segment(html: str) -> Optional[str]:
    norm = re.sub(r"\s+", " ", html)
    m_start = re.search(r"Entwicklung der\s+WETTER-?\s*und\s*WARNLAGE", norm, re.I)
    if not m_start: return None
    tail = norm[m_start.end():]
    m_end = re.search(r"(?:<h[1-6][^>]*>|#\s|Weitere\s+Entwicklung|Nächste\s+Aktualisierung|</section>|</article>)", tail, re.I)
    segment = tail[:m_end.start()] if m_end else tail
    return _html_to_text(segment)[:1800].strip()

def _extract_full_text(html: str) -> Optional[str]:
    try:
        m_h1 = re.search(r"<h1[^>]*>.*?WARNLAGEBERICHT.*?</h1>", html, flags=re.S|re.I)
        tail = html[m_h1.end():] if m_h1 else html
        return _html_to_text(tail)[:2200].strip()
    except Exception:
        return None

def _load_warnlage_cache() -> Dict[str, Any]:
    if WARNLAGE_CACHE_FILE.exists():
        try:
            return json.loads(WARNLAGE_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_warnlage_cache(cache: Dict[str, Any]) -> None:
    WARNLAGE_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def fetch_warnlagebericht_nrw_cached() -> Optional[str]:
    if not APPEND_WARNLAGE:
        return None
    cache = _load_warnlage_cache()
    now_ts = int(time.time())

    if cache.get("no_marker") and cache.get("saved_ts"):
        if now_ts - int(cache["saved_ts"]) < WARNLAGE_TTL_SECONDS and cache.get("text"):
            print("[Info] Warnlagebericht: Cache-Hit (TTL, kein Issue-Marker).")
            return cache["text"]

    html = _get_text_with_retries(WARNLAGE_URL_NRW)
    if not html:
        if cache.get("text"):
            print("[Warn] Warnlagebericht: Abruf fehlgeschlagen – verwende Cache.")
        return cache.get("text")

    issue = _extract_issue_str(html)
    entwicklung = _extract_entwicklung_segment(html)
    fulltext = _extract_full_text(html)
    chosen_text = (entwicklung or fulltext or "").strip()
    if not chosen_text:
        print("[Info] Warnlagebericht: Keine verwertbaren Textabschnitte gefunden.")
        return None

    text_hash = hashlib.sha256(chosen_text.encode("utf-8")).hexdigest()

    if issue:
        if cache.get("issue") == issue and cache.get("text"):
            print(f"[Info] Warnlagebericht: Cache-Hit (Issue unverändert).")
            return cache["text"]
        cache = {"issue": issue, "text": chosen_text, "hash": text_hash, "saved_ts": now_ts, "no_marker": False}
        _save_warnlage_cache(cache)
        print("[Info] Warnlagebericht: Aktualisiert (neue Ausgabe erkannt).")
        return chosen_text
    else:
        if cache.get("no_marker") and cache.get("hash") == text_hash and cache.get("text"):
            if now_ts - int(cache.get("saved_ts", 0)) < WARNLAGE_TTL_SECONDS:
                print("[Info] Warnlagebericht: Cache-Hit (identischer Inhalt, innerhalb TTL).")
                return cache["text"]
        cache = {"issue": None, "text": chosen_text, "hash": text_hash, "saved_ts": now_ts, "no_marker": True}
        _save_warnlage_cache(cache)
        print("[Info] Warnlagebericht: Aktualisiert (kein Marker, neuer/aktualisierter Inhalt).")
        return chosen_text

# =======================
# PAYLOAD & SEND
# =======================

def _fmt_title_with_level(event: Optional[str], level: int, prefix: str = "") -> str:
    evt = event or "Ereignis"
    lvl = f"Stufe {level}" if level > 0 else "-"
    return f"{prefix}DWD-Warnung: {evt} ({lvl})"

def build_divera_payload(w: Dict[str, Any], ric: str, group_ids_env: str = "", title_prefix: str = "") -> Dict[str, Any]:
    level = int(w.get("level", 0))
    title = _fmt_title_with_level(w.get("event"), level, prefix=title_prefix)

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
    if w.get("headline"):     parts.append(w["headline"])
    if zeitzeile_parts:       parts.append(" · ".join(zeitzeile_parts))
    if w.get("description"):  parts.append(w["description"])
    if w.get("instruction"):  parts.append(f"⚠️ Hinweise: {w['instruction']}")

    meta = []
    if w.get("name"):         meta.append(f"Gebiet: {w['name']} [{w.get('warncellid')}]")
    if w.get("sent"):
        sent_loc = _fmt_dt(_parse_dt_any(w.get("sent")))
        if sent_loc:          meta.append(f"Gesendet: {sent_loc} Uhr")
    if w.get("urgency") or w.get("certainty"):
        meta.append(f"Dringlichkeit: {w.get('urgency')}, Sicherheit: {w.get('certainty')}")
    # Zusatz: englischer Begriff in Klammern für Transparenz
    sev_str = w.get("severity") or "-"
    if level > 0:
        meta.append(f"Warnstufe: {level} ({sev_str})")
    meta.append(f"Quelle: DWD · {w.get('web')}")
    parts.append("\n".join(meta))

    annex = fetch_warnlagebericht_nrw_cached()
    if annex:
        cache = _load_warnlage_cache()
        label = "NRW – Entwicklung der WETTER- und WARNLAGE (DWD)"
        if cache.get("issue"):
            label += f" · {cache['issue']}"
        parts.append(f"{APPEND_SEPARATOR}{label}:\n{annex}")

    payload: Dict[str, Any] = {
        "title": title[:120],
        "text": "\n\n".join(parts)[:8000],
        "ric": ric
    }

    if group_ids_env:
        try:
            payload["group_ids"] = [int(x) for x in group_ids_env.split(",") if x.strip()]
        except ValueError:
            pass

    return payload

def post_to_divera(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not DIVERA_ACCESSKEY or len(DIVERA_ACCESSKEY) < 10:
        raise RuntimeError("DIVERA_ACCESSKEY fehlt/ungültig (per Secret setzen).")
    u = f"{DIVERA_API_URL}?accesskey={DIVERA_ACCESSKEY}"
    try:
        r = requests.post(
            u,
            headers={"Content-Type":"application/json","Accept":"application/json"},
            json=payload,
            timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ)
        )
        if r.status_code >= 300:
            raise RuntimeError(f"DIVERA API Fehler {r.status_code}")
        try:
            return r.json()
        except Exception:
            return {"status": r.status_code}
    except Exception as e:
        raise RuntimeError(f"DIVERA API Fehler: {type(e).__name__}") from None

# =======================
# MAIN
# =======================

def main() -> None:
    seen = load_seen()
    new_seen = set(seen)

    warnings = fetch_dwd_warnings()
    total_count = 0
    info_count = 0
    einsatz_count = 0

    for w in warnings:
        if not passes_filters_global(w):
            continue

        ident = w.get("identifier") or f"{w.get('headline')}|{w.get('sent')}|{w.get('warncellid')}"

        # 1) Infogruppe – immer
        key_info = f"{ident}|{RIC_INFO}"
        if (key_info not in seen) and (ident not in seen):
            try:
                post_to_divera(build_divera_payload(w, RIC_INFO, DIVERA_GROUP_IDS_INFO, title_prefix=""))
                new_seen.add(key_info); total_count += 1; info_count += 1
            except Exception as e:
                print(f"[Fehler] DIVERA-Post (Info): {e}")

        # 2) Einsatzabteilung – ab Stufe RIC2_THRESHOLD_LEVEL
        if is_unwetter_for_ric2(w):
            key_einsatz = f"{ident}|{RIC_EINSATZ}"
            if key_einsatz not in seen:
                try:
                    post_to_divera(build_divera_payload(w, RIC_EINSATZ, DIVERA_GROUP_IDS_EINSATZ, title_prefix="⚠️ "))
                    new_seen.add(key_einsatz); total_count += 1; einsatz_count += 1
                except Exception as e:
                    print(f"[Fehler] DIVERA-Post (Einsatz): {e}")

    save_seen(new_seen)
    print(f"Neue Mitteilungen gesendet: {total_count} (Info: {info_count}, Einsatz: {einsatz_count})")

# =======================
# STATE
# =======================

def load_seen() -> set:
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def save_seen(seen: set) -> None:
    STATE_FILE.write_text(json.dumps(sorted(list(seen)), ensure_ascii=False, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
