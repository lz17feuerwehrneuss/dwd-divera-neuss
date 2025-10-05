#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DIVERA (Zentrale) -> Mitteilung in Untereinheit + Telegram
- Pollt /api/last-alarm der Zentraleinheit
- Dedupe via seen_alarms.json
- Erstellt Mitteilung in Untereinheit (/api/news, private, RIC)
- Sendet Telegram-Nachricht (sendMessage, HTML)
- Retries/Backoff; robuste Feldzuordnung

Env (GitHub Secrets empfohlen):
  DIVERA_ACCESSKEY_CENTRAL   # Zentraleinheit (LESEN)
  DIVERA_ACCESSKEY_SUB       # Untereinheit (SCHREIBEN)
  SUB_RIC                    # z. B. "#170002"
  TG_BOT_TOKEN               # Telegram Bot-Token
  TG_CHAT_ID                 # Telegram Chat/Kanal-ID (z.B. -1001935...)
Optional:
  NEWS_PRIVATE=true|false    # default true
  HTTP_RETRIES=5, HTTP_TIMEOUT_CONNECT=5, HTTP_TIMEOUT_READ=45
"""

import os, json, time, random
from pathlib import Path
from typing import Any, Dict, Optional
import requests
from datetime import datetime, timezone

# ===== Konfig/Env =====
DIVERA_ACCESSKEY_CENTRAL = os.getenv("DIVERA_ACCESSKEY_CENTRAL", "").strip()
DIVERA_ACCESSKEY_SUB     = os.getenv("DIVERA_ACCESSKEY_SUB", "").strip()
SUB_RIC                  = os.getenv("SUB_RIC", "").strip()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID   = os.getenv("TG_CHAT_ID", "").strip()

NEWS_PRIVATE = (os.getenv("NEWS_PRIVATE", "true").lower() in ("1","true","yes","on"))

HTTP_RETRIES          = int(os.getenv("HTTP_RETRIES", "5"))
HTTP_TIMEOUT_CONNECT  = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ     = int(os.getenv("HTTP_TIMEOUT_READ", "45"))
UA = {"User-Agent": "alarm-broadcast/1.0 (+github-actions)"}

STATE_FILE = Path("seen_alarms.json")

DIVERA_BASE = "https://app.divera247.com"
URL_LAST_ALARM = f"{DIVERA_BASE}/api/last-alarm"
URL_NEWS       = f"{DIVERA_BASE}/api/news"

def _jget(url: str, params: Dict[str,Any], retries=HTTP_RETRIES) -> Optional[Dict[str,Any]]:
    for attempt in range(1, retries+1):
        try:
            r = requests.get(url, params=params, headers=UA, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries:
                print(f"[Warn] GET {url} failed after {retries} tries: {e}")
                return None
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0,0.5)
            print(f"[Info] GET retry {attempt} in {sleep_s:.1f}s … ({e})")
            time.sleep(sleep_s)

def _jpost(url: str, params: Dict[str,Any], body: Dict[str,Any], retries=HTTP_RETRIES) -> bool:
    for attempt in range(1, retries+1):
        try:
            r = requests.post(url, params=params, json=body,
                              headers={**UA, "Accept":"application/json","Content-Type":"application/json"},
                              timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            if r.status_code >= 300:
                raise RuntimeError(f"HTTP {r.status_code}")  # Body nicht loggen
            return True
        except Exception as e:
            if attempt == retries:
                print(f"[Warn] POST failed after {retries} tries: {type(e).__name__}")
                return False
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0,0.5)
            print(f"[Info] POST retry {attempt} in {sleep_s:.1f}s … ({type(e).__name__})")
            time.sleep(sleep_s)

def _load_seen() -> set:
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text(encoding="utf-8")))
        except Exception:
            return set()
    return set()

def _save_seen(s: set):
    STATE_FILE.write_text(json.dumps(sorted(list(s)), ensure_ascii=False, indent=2), encoding="utf-8")

def _first(*vals, default=""):
    for v in vals:
        if v:
            return v
    return default

def _fmt_dt_local(iso_str: str) -> str:
    try:
        if iso_str.endswith("Z"):
            dt = datetime.fromisoformat(iso_str.replace("Z","+00:00"))
        else:
            dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso_str

def _escape_html(s: str) -> str:
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def extract_alarm_fields(data: Dict[str,Any]) -> Dict[str,str]:
    """Robust Felder aus last-alarm JSON ziehen."""
    keyword = _first(data.get("keyword"), data.get("title"), data.get("tacticalMode"),
                     data.get("einsatzstichwort"), default="Einsatz")
    message = _first(data.get("message"), data.get("text"), data.get("notes"),
                     data.get("meldebild"), default="")
    address = _first(data.get("address"), data.get("location"), data.get("place"), default="")

    recipients = []
    for key in ("groups","recipients","target_groups","alarm_groups"):
        val = data.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, str):
                    recipients.append(item)
                elif isinstance(item, dict):
                    recipients.append(_first(item.get("name"), item.get("title"), default="Gruppe"))
    recips = ", ".join(sorted(set(r for r in recipients if r)))

    ts = _first(data.get("time"), data.get("timestamp"), data.get("created"), default="")
    ts_fmt = _fmt_dt_local(ts) if ts else ""

    alarm_id = _first(data.get("id"), data.get("alarm_id"), data.get("uuid"), default="")

    return {
        "id": str(alarm_id),
        "keyword": str(keyword).strip(),
        "message": str(message).strip(),
        "address": str(address).strip(),
        "recipients": recips,
        "time_local": ts_fmt
    }

def create_divera_news(accesskey_sub: str, ric: str, fields: Dict[str,str]) -> bool:
    title = f"Einsatz: {fields['keyword']}" + (f" – {fields['address']}" if fields['address'] else "")
    parts = []
    if fields['message']:    parts.append(f"Meldung: {fields['message']}")
    if fields['address']:    parts.append(f"Adresse: {fields['address']}")
    if fields['recipients']: parts.append(f"Empfänger: {fields['recipients']}")
    if fields['time_local']: parts.append(f"Zeit: {fields['time_local']} Uhr")

    payload = {
        "title": title[:120],
        "text": "\n".join(parts)[:8000],
        "ric": ric,
        "private": True if NEWS_PRIVATE else False
    }
    return _jpost(URL_NEWS, params={"accesskey": accesskey_sub}, body=payload)

def send_telegram(bot_token: str, chat_id: str, fields: Dict[str,str]) -> bool:
    title = f"<b>ALARM: { _escape_html(fields['keyword']) }</b>"
    lines = [title]
    if fields['message']:    lines.append(f"<b>Meldung:</b> {_escape_html(fields['message'])}")
    if fields['address']:    lines.append(f"<b>Adresse:</b> {_escape_html(fields['address'])}")
    if fields['recipients']: lines.append(f"<b>Empfänger:</b> {_escape_html(fields['recipients'])}")
    if fields['time_local']: lines.append(f"<b>Zeit:</b> {_escape_html(fields['time_local'])} Uhr")
    text = "\n".join(lines)

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    body = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    for attempt in range(1, HTTP_RETRIES+1):
        try:
            r = requests.post(url, json=body, headers=UA, timeout=(HTTP_TIMEOUT_CONNECT, HTTP_TIMEOUT_READ))
            if r.status_code >= 300:
                raise RuntimeError(f"HTTP {r.status_code}")  # Body/URL nicht loggen
            return True
        except Exception as e:
            if attempt == HTTP_RETRIES:
                print(f"[Warn] Telegram send failed after {HTTP_RETRIES} tries: {type(e).__name__}")
                return False
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            print(f"[Info] Telegram retry {attempt} in {sleep_s:.1f}s … ({type(e).__name__})")
            time.sleep(sleep_s)

def main():
    if not DIVERA_ACCESSKEY_CENTRAL:
        print("[Warn] DIVERA_ACCESSKEY_CENTRAL fehlt – Abbruch.")
        return

    seen = _load_seen()
    sent_sub = False
    sent_tg  = False

    # 1) letzten Einsatz aus Zentrale holen
    data = _jget(URL_LAST_ALARM, params={"accesskey": DIVERA_ACCESSKEY_CENTRAL})
    if not data:
        print("Keine Daten von /api/last-alarm.")
        print("Einsatzübertragungen: 0 (Untereinheit: 0, Telegram: 0)")
        return

    fields = extract_alarm_fields(data)
    alarm_id = fields["id"] or f"{fields['keyword']}|{fields['time_local']}|{fields['address']}"
    key = f"alarm:{alarm_id}"

    if key in seen:
        print("Kein neuer Einsatz (bereits verarbeitet).")
        print("Einsatzübertragungen: 0 (Untereinheit: 0, Telegram: 0)")
        return

    # 2) Mitteilung an Untereinheit (#170002, privat)
    if DIVERA_ACCESSKEY_SUB and SUB_RIC:
        ok_news = create_divera_news(DIVERA_ACCESSKEY_SUB, SUB_RIC, fields)
        sent_sub = bool(ok_news)
    else:
        print("[Info] Untereinheit-Keys/RIС nicht gesetzt – übersprungen.")

    # 3) Telegram
    if TG_BOT_TOKEN and TG_CHAT_ID:
        ok_tg = send_telegram(TG_BOT_TOKEN, TG_CHAT_ID, fields)
        sent_tg = bool(ok_tg)
    else:
        print("[Info] Telegram-Keys nicht gesetzt – übersprungen.")

    if sent_sub or sent_tg:
        seen.add(key)
        _save_seen(seen)

    total = int(sent_sub) + int(sent_tg)
    print(f"Einsatzübertragungen: {total} (Untereinheit: {int(sent_sub)}, Telegram: {int(sent_tg)})")

if __name__ == "__main__":
    main()

