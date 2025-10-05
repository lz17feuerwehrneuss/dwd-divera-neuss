#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DWD → DIVERA 24/7 (Mitteilungen) – Dual-RIC & NRW-Warnlagebericht-Anhang mit Cache

Features:
- RIC #170001 (Infogruppe): ALLE Warnungen
- RIC #170002 (Einsatzabteilung): NUR Unwetter (SEVERITY >= severe) mit ⚠️ im Titel
- Per-RIC-Deduplizierung: 'identifier|RIC' (rückwärtskompatibel)
- Filter (SEVERITY/EVENT), Warnzeitraum (ONSET/EXPIRES/EFFECTIVE)
- Retries mit Exponential-Backoff + Jitter
- Zeitzonen-Fallback (läuft auch ohne tzdata)
- Abschlusszeile: Gesamt + Aufschlüsselung Info/Einsatz
- NRW-Warnlagebericht-Anhang
  * bevorzugt Abschnitt "Entwicklung der WETTER- und WARNLAGE"
  * sonst kompletter Lagebericht
  * optische Trennlinie
  * NEU: Cache mit Ausgabezeit-Erkennung (nur aktualisieren, wenn sich Ausgabe ändert)

Python 3.12/3.13; Abhängigkeit: requests
"""

import os
import re
import json
import time
import random
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import requests
from datetime import datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

# =======================
# KONFIGURATION
# =======================

# --- DIVERA ---
DIVERA_ACCESSKEY = os.getenv(
    "DIVERA_ACCESSKEY",
    "cbYfiHFy4wprKiMTzimh-B-2PeGAe2oPVu399X7zZxxqDOOkih_NyxkL6jc-qPH2"
)
DIVERA_API_URL = os.getenv("DIVERA_API_URL", "https://app.divera247.com/api/news")

# RICs
RIC_INFO    = os.getenv("RIC_INFO",   "#170001")  # Infogruppe: alle Warnungen
RIC_EINSATZ = os.getenv("RIC_EINSATZ","#170002")  # Einsatzabteilung: Unwetter (severe+)

# Optional: zusätzliche group_ids per ENV (Komma)
DIVERA_GROUP_IDS_INFO    = os.getenv("DIVERA_GROUP_IDS_INFO", "")
DIVERA_GROUP_IDS_EINSATZ = os.getenv("DIVERA_GROUP_IDS_EINSATZ", "")

# --- DWD-Warnzellen ---
# Neuss-Standard: 805162024 (kann per ENV überschrieben werden)
WARNCELL_IDS = [x.strip() for x in os.getenv("WARNCELL_IDS", "805162024").split(",") if x.strip()]
if not WARNCELL_IDS:
    WARNCELL_IDS = ["805162024"]

# --- Filter global (leer = kein Filter) ---
SEVERITY_MIN = (os.getenv("SEVERITY_MIN", "") or "").strip().lower()  # minor|moderate|severe|extreme
EVENT_ALLOW = [x.strip().lower() for x in os.getenv("EVENT_ALLOW", "").split(",") if x.strip()]
EVENT_DENY  = [x.strip().lower() for x in os.getenv("EVENT_DENY", "").split(",") if x.strip()]

# Unwetter-Schwelle für die Einsatzabteilung
SEVERITY_THRESHOLD_RIC2 = os.getenv("SEVERITY_THRESHOLD_RIC2", "severe").strip().lower()

# Dedupe-Datei
STATE_FILE = Path(os.getenv("STATE_FILE", "dwd_seen.json"))

# HTTP & Retries
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ    = int(os.getenv("HTTP_TIMEOUT_READ", "45"))
HTTP_RETRIES         = int(os.getenv("HTTP_RETRIES", "5"))
HEADERS = {"User-Agent": "dwd2divera/1.4 (+github-actions; contact=admin@localhost)"}

# Severity-Ranking
SEVERITY_ORDER = {"":0, "unknown":0, "minor":1, "moderate":2, "severe":3, "extreme":4}

# --- NRW Warnlagebericht-Append ---
APPEND_WARNLAGE = (os.getenv("APPEND_WARNLAGE", "true").lower() in ("1","true","yes","on"))
WARNLAGE_URL_NRW = os.getenv("WARNLAGE_URL_NRW",
    "https://www.dwd.de/DE/wetter/warnungen_aktuell/warnlagebericht/nordrhein_westfalen/warnlage_nrw_node.html"
).strip()
APPEND_SEPARATOR = os.getenv("APPEND_SEPARATOR", "\n\n──────────\n").encode("utf-8").decode("utf-8")

# Cache-Datei für den Warnlagebericht
WARNLAGE_CACHE_FILE = Path(os.getenv("WARNLAGE_CACHE_FILE", "warnlage_cache.json"))
# maximale „Vertrauensdauer“, falls die Seite keine Ausgabezeit erkennen lässt (Sicherheitsnetz)
WARNLAGE_TTL_SECONDS = int(os.getenv("WARNLAGE_TTL_SECONDS", str(12 * 3600)))  # 12 Stunden

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
# HILFSFUNKTIONEN HTTP/JSON
# =======================

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

def _get_text_with_retries(url: str) -> Optional[str]:
    """GET Text/HTML mit Retries."""
    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            r.encoding = r.apparent_encoding or "utf-8"
            return r.text
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] Text-Abruf fehlgeschlagen nach {HTTP_RETRIES} Versuchen: {e}")
                return None
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            print(f"[Info] Text retry {attempt} in {sleep_s:.1f}s … ({e})")
            time.sleep(sleep_s)

# =======================
# DWD WARNUNGEN (WFS)
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

# =======================
# FILTER & FORMAT
# =======================

def passes_filters_global(w: Dict[str, Any]) -> bool:
    """Globale (optionale) Filter anwenden."""
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

def is_unwetter_for_ric2(w: Dict[str, Any]) -> bool:
    """Unwetter-Schwelle für Einsatzabteilung prüfen (severe+)."""
    sev = (w.get("severity") or "").lower()
    return SEVERITY_ORDER.get(sev, 0) >= SEVERITY_ORDER.get(SEVERITY_THRESHOLD_RIC2, 3)

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

# =======================
# NRW – WARNLAGEBERICHT (HTML → Text) + CACHE
# =======================

def _html_to_text(segment: str) -> str:
    text = re.sub(r"<[^>]+>", " ", segment)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _extract_issue_str(html: str) -> Optional[str]:
    """
    Versucht eine Ausgabezeit/Stand-Angabe zu finden, z. B.:
    - 'Ausgegeben: 05.10.2025, 07:30 Uhr'
    - 'Stand: 05.10.2025 07:30 Uhr'
    - 'Ausgabe vom 05.10.2025, 07:30 Uhr'
    Gibt den gefundenen String zurück (als Marker), sonst None.
    """
    norm = re.sub(r"\s+", " ", html)
    patterns = [
        r"(Ausgegeben|Ausgabe|Stand)\s*[:vom]*\s*\d{2}\.\d{2}\.\d{4}[, ]+\d{1,2}:\d{2}\s*Uhr",
        r"(Ausgegeben|Ausgabe|Stand)\s*[:vom]*\s*\d{2}\.\d{2}\.\d{2}[, ]+\d{1,2}:\d{2}\s*Uhr",
    ]
    for pat in patterns:
        m = re.search(pat, norm, flags=re.I)
        if m:
            return _html_to_text(m.group(0))
    return None

def _extract_entwicklung_segment(html: str) -> Optional[str]:
    """Bevorzugter Abschnitt 'Entwicklung der WETTER- und WARNLAGE' → Plaintext."""
    norm = re.sub(r"\s+", " ", html)
    start_rx = re.compile(r"Entwicklung der\s+WETTER-?\s*und\s*WARNLAGE", re.I)
    m_start = start_rx.search(norm)
    if not m_start:
        return None
    tail = norm[m_start.end():]
    m_end = re.search(r"(?:<h[1-6][^>]*>|#\s|Weitere\s+Entwicklung|Nächste\s+Aktualisierung|</section>|</article>)", tail, re.I)
    segment = tail[:m_end.start()] if m_end else tail
    return _html_to_text(segment)[:1800].strip()

def _extract_full_text(html: str) -> Optional[str]:
    """Fallback: Großer Textblock (nach H1 bis vor Footer) → Plaintext."""
    try:
        big = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.S|re.I)
        big = re.sub(r"<style[^>]*>.*?</style>", " ", big, flags=re.S|re.I)
        m_h1 = re.search(r"<h1[^>]*>.*?WARNLAGEBERICHT.*?</h1>", big, flags=re.S|re.I)
        tail = big[m_h1.end():] if m_h1 else big
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
    """
    Holt den Warnlagebericht NRW mit Cache:
    - Wenn 'Ausgegeben/Stand'-Marker identisch zum Cache ist → verwende Cache-Text
    - Sonst extrahiere erneut (Entwicklung bevorzugt, sonst Full), aktualisiere Cache
    - Falls kein Marker gefunden wird, verwende TTL-Cache per Zeitstempel + Content-Hash
    """
    if not APPEND_WARNLAGE:
        return None

    cache = _load_warnlage_cache()
    now_ts = int(time.time())

    # Wenn wir keinen HTML-Abruf machen wollen (TTL-Cache ohne Marker):
    if cache.get("no_marker") and cache.get("saved_ts"):
        if now_ts - int(cache["saved_ts"]) < WARNLAGE_TTL_SECONDS and cache.get("text"):
            return cache["text"]

    html = _get_text_with_retries(WARNLAGE_URL_NRW)
    if not html:
        # Fallback: nutze Cache, wenn vorhanden
        return cache.get("text")

    issue = _extract_issue_str(html)  # z. B. "Ausgegeben: 05.10.2025, 07:30 Uhr"
    entwicklung = _extract_entwicklung_segment(html)
    fulltext = _extract_full_text(html)
    chosen_text = (entwicklung or fulltext or "").strip()
    if not chosen_text:
        # nichts extrahiert → kein Anhang
        return None

    # Marker/Hash
    text_hash = hashlib.sha256(chosen_text.encode("utf-8")).hexdigest()

    if issue:
        # Wenn Marker gleich → verwende Cache (auch wenn Text geringfügig anders geparst wurde)
        if cache.get("issue") == issue and cache.get("text"):
            return cache["text"]
        # Issue neu → Cache aktualisieren
        cache = {"issue": issue, "text": chosen_text, "hash": text_hash, "saved_ts": now_ts, "no_marker": False}
        _save_warnlage_cache(cache)
        return chosen_text
    else:
        # Kein Issue-Marker gefunden → TTL-Cache über Hash
        if cache.get("no_marker") and cache.get("hash") == text_hash and cache.get("text"):
            # gleicher Text und innerhalb TTL → Cache
            if now_ts - int(cache.get("saved_ts", 0)) < WARNLAGE_TTL_SECONDS:
                return cache["text"]
        # Cache neu/aktualisieren
        cache = {"issue": None, "text": chosen_text, "hash": text_hash, "saved_ts": now_ts, "no_marker": True}
        _save_warnlage_cache(cache)
        return chosen_text

# =======================
# PAYLOAD BAUEN & SENDEN
# =======================

def build_divera_payload(w: Dict[str, Any], ric: str, group_ids_env: str = "", title_prefix: str = "") -> Dict[str, Any]:
    title = f"{title_prefix}DWD-Warnung: {w.get('event') or 'Ereignis'} ({w.get('severity') or '-'})"

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
    meta.append(f"Quelle: DWD · {w.get('web')}")
    parts.append("\n".join(meta))

    # --- NRW-Warnlagebericht anhängen (mit Cache) ---
    annex = fetch_warnlagebericht_nrw_cached()
    if annex:
        # wenn wir einen Issue-Marker im Cache haben, hängen wir ihn als Kopf voran
        cache = _load_warnlage_cache()
        label = "NRW – Entwicklung der WETTER- und WARNLAGE (DWD)"
        if cache.get("issue"):
            label += f" · {cache['issue']}"
        parts.append(f"{APPEND_SEPARATOR}{label}:\n{annex}")

    payload: Dict[str, Any] = {
        "title": title[:120],
        "text": "\n\n".join(parts)[:8000],
        "ric": ric
        # Hinweis: 'private' wird von /api/news derzeit ignoriert; daher nicht gesetzt
    }

    if group_ids_env.strip():
        try:
            payload["group_ids"] = [int(x) for x in group_ids_env.split(",") if x.strip()]
        except ValueError:
            pass

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
# MAIN (Dual-RIC Versand + Abschlusszählung)
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

        # -- 1) Infogruppe: immer (ohne ⚠️) --
        key_info = f"{ident}|{RIC_INFO}"
        already_info = (key_info in seen) or (ident in seen)  # alt: ohne '|'
        if not already_info:
            payload_info = build_divera_payload(w, RIC_INFO, DIVERA_GROUP_IDS_INFO, title_prefix="")
            try:
                post_to_divera(payload_info)
                new_seen.add(key_info)
                total_count += 1
                info_count += 1
            except Exception as e:
                print(f"[Fehler] DIVERA-Post (Infogruppe): {e}")

        # -- 2) Einsatzabteilung: nur Unwetter (mit ⚠️ im Titel) --
        if is_unwetter_for_ric2(w):
            key_einsatz = f"{ident}|{RIC_EINSATZ}"
            already_einsatz = (key_einsatz in seen)
            if not already_einsatz:
                payload_einsatz = build_divera_payload(
                    w, RIC_EINSATZ, DIVERA_GROUP_IDS_EINSATZ, title_prefix="⚠️ "
                )
                try:
                    post_to_divera(payload_einsatz)
                    new_seen.add(key_einsatz)
                    total_count += 1
                    einsatz_count += 1
                except Exception as e:
                    print(f"[Fehler] DIVERA-Post (Einsatzabteilung): {e}")

    save_seen(new_seen)
    print(f"Neue Mitteilungen gesendet: {total_count} (Info: {info_count}, Einsatz: {einsatz_count})")

# =======================
# STATE I/O
# =======================

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

# =======================
# ENTRY
# =======================

if __name__ == "__main__":
    main()
