# --- NEU/GEÄNDERT OBEN BEI DEN KONSTANTEN ---
import time
import random

# getrennte Timeouts (Sekunden)
HTTP_TIMEOUT_CONNECT = int(os.getenv("HTTP_TIMEOUT_CONNECT", "5"))
HTTP_TIMEOUT_READ    = int(os.getenv("HTTP_TIMEOUT_READ", "45"))
HTTP_RETRIES         = int(os.getenv("HTTP_RETRIES", "5"))  # inkl. Erstversuch

HEADERS = {
    "User-Agent": "dwd2divera/1.0 (+github-actions; contact=admin@localhost)"
}

# --- NEU: Helper für robuste GETs (JSON) ---
def _get_json_with_retries(url: str) -> dict | None:
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
            # letzter Versuch -> aufgeben
            if attempt == HTTP_RETRIES:
                print(f"[Warn] DWD Abruf dauerhaft fehlgeschlagen nach {HTTP_RETRIES} Versuchen: {e}")
                return None
            # Backoff: 1, 2, 4, 8... + Jitter
            sleep_s = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            print(f"[Info] DWD Versuch {attempt} fehlgeschlagen ({e}); retry in {sleep_s:.1f}s …")
            time.sleep(sleep_s)

# --- ERSETZE DIE ALTE fetch_dwd_warnings() DURCH DIESE VERSION ---
def fetch_dwd_warnings() -> List[Dict[str, Any]]:
    """DWD-Warnungen per WFS (JSON) abrufen und in ein handliches Dict-Format bringen."""
    url = build_dwd_wfs_url(WARNCELL_IDS)
    data = _get_json_with_retries(url)
    if data is None:
        # Keine harten Abbrüche mehr – leer zurückgeben, Job läuft weiter
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

# --- IN main(): den bisherigen try/except-Block beim Abruf ersetzen ---
def main() -> None:
    if not DIVERA_ACCESSKEY or len(DIVERA_ACCESSKEY) < 20:
        raise SystemExit("DIVERA_ACCESSKEY fehlt/ungültig. Bitte setzen.")

    seen = load_seen()
    new_seen = set(seen)

    warnings = fetch_dwd_warnings()  # <— gibt [] zurück, wenn Abruf scheitert
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
