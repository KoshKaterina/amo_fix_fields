"""SQLite-хранилище коллтрекинга сайта (см. call_click_service.py).

Четыре таблицы:
    site_events    - события сайта: клик по номеру (tel_click) и вход с метками (utm_visit).
                     Время at ставит сервер WordPress при приёме, а не браузер.
    call_starts    - начала входящих звонков Sipuni (call_id, телефон, время). Нужны, чтобы
                     понять, сколько разных людей звонило в окне клика.
    phone_clients  - связка «телефон из заявки сайта ↔ ClientID Метрики». Даёт повторного клиента.
    attributions   - что уже записано в сделку (ref = call:<id> или visit:<event_id>): одна
                     запись на звонок, одно примечание на визит в сделке.

Синхронный sqlite3, вызывать через asyncio.to_thread. Файл на томе /app/var
(docker-compose.yml: ./amo_fix_fields/var:/app/var), рестарт контейнера его не стирает.
"""

import json
import os
import sqlite3
import time
from contextlib import contextmanager

DB_PATH = os.getenv("CALL_CLICK_DB_PATH", "/app/var/call_click.sqlite3")


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with _connect() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS site_events (
            event_id TEXT PRIMARY KEY, kind TEXT NOT NULL, at REAL NOT NULL,
            number TEXT NOT NULL DEFAULT '', client_id TEXT NOT NULL DEFAULT '',
            payload TEXT NOT NULL, matched_call TEXT)""")
        conn.execute("CREATE INDEX IF NOT EXISTS site_events_at ON site_events (kind, at)")
        conn.execute("CREATE INDEX IF NOT EXISTS site_events_client ON site_events (client_id, at)")
        conn.execute("""CREATE TABLE IF NOT EXISTS call_starts (
            call_id TEXT PRIMARY KEY, phone TEXT NOT NULL, started_at REAL NOT NULL)""")
        conn.execute("CREATE INDEX IF NOT EXISTS call_starts_at ON call_starts (started_at)")
        conn.execute("""CREATE TABLE IF NOT EXISTS phone_clients (
            phone TEXT NOT NULL, client_id TEXT NOT NULL, at REAL NOT NULL,
            PRIMARY KEY (phone, client_id))""")
        conn.execute("""CREATE TABLE IF NOT EXISTS attributions (
            lead_id INTEGER NOT NULL, ref TEXT NOT NULL, at REAL NOT NULL,
            PRIMARY KEY (lead_id, ref))""")


def _event_row(r) -> dict:
    payload = json.loads(r[5] or "{}")
    return {"event_id": r[0], "kind": r[1], "at": r[2], "number": r[3], "client_id": r[4],
            "payload": payload, "matched_call": r[6]}


_EV_COLS = "event_id, kind, at, number, client_id, payload, matched_call"


def add_event(event: dict) -> bool:
    """True - новое событие, False - повтор того же event_id."""
    with _connect() as conn:
        cur = conn.execute(
            f"INSERT OR IGNORE INTO site_events ({_EV_COLS}) VALUES (?, ?, ?, ?, ?, ?, NULL)",
            (event["event_id"], event["kind"], float(event["at"]), event.get("number", ""),
             event.get("client_id", ""), json.dumps(event, ensure_ascii=False)),
        )
        return cur.rowcount == 1


def clicks_between(start: float, end: float) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT {_EV_COLS} FROM site_events WHERE kind = 'tel_click' AND at BETWEEN ? AND ? ORDER BY at",
            (start, end),
        ).fetchall()
    return [_event_row(r) for r in rows]


def mark_matched(event_id: str, call_id: str) -> None:
    with _connect() as conn:
        conn.execute("UPDATE site_events SET matched_call = ? WHERE event_id = ? AND matched_call IS NULL",
                     (call_id, event_id))


def record_call_start(call_id: str, phone: str, started_at: float) -> None:
    with _connect() as conn:
        conn.execute("INSERT OR IGNORE INTO call_starts (call_id, phone, started_at) VALUES (?, ?, ?)",
                     (call_id, phone, float(started_at)))


def call_phones_between(start: float, end: float) -> set[str]:
    """Разные телефоны входящих звонков в интервале. Скрытый номер считается отдельным звонящим."""
    with _connect() as conn:
        rows = conn.execute("SELECT call_id, phone FROM call_starts WHERE started_at BETWEEN ? AND ?",
                            (start, end)).fetchall()
    return {phone or ("hidden:" + call_id) for call_id, phone in rows}


def link_phone(phone: str, client_id: str, at: float | None = None) -> None:
    if not phone or not client_id:
        return
    with _connect() as conn:
        conn.execute(
            "INSERT INTO phone_clients (phone, client_id, at) VALUES (?, ?, ?) "
            "ON CONFLICT(phone, client_id) DO UPDATE SET at = excluded.at",
            (phone, client_id, float(at or time.time())),
        )


def client_ids_for_phone(phone: str) -> list[str]:
    with _connect() as conn:
        rows = conn.execute("SELECT client_id FROM phone_clients WHERE phone = ? ORDER BY at DESC",
                            (phone,)).fetchall()
    return [r[0] for r in rows]


def latest_utm_event(client_ids: list[str], since: float) -> dict | None:
    """Самое свежее событие сайта с метками от любого из ClientID (вход с меткой или клик)."""
    if not client_ids:
        return None
    marks = ",".join("?" for _ in client_ids)
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT {_EV_COLS} FROM site_events WHERE client_id IN ({marks}) AND at >= ? ORDER BY at DESC",
            (*client_ids, since),
        ).fetchall()
    for r in rows:
        ev = _event_row(r)
        if ev["payload"].get("utm"):
            return ev
    return None


def claim_attribution(lead_id: int, ref: str) -> bool:
    """True - эту ссылку в сделку ещё не писали (и теперь она занята)."""
    with _connect() as conn:
        cur = conn.execute("INSERT OR IGNORE INTO attributions (lead_id, ref, at) VALUES (?, ?, ?)",
                           (int(lead_id), ref, time.time()))
        return cur.rowcount == 1


def release_attribution(lead_id: int, ref: str) -> None:
    """Запись в amo не удалась - освобождаем ссылку, чтобы следующий звонок мог повторить."""
    with _connect() as conn:
        conn.execute("DELETE FROM attributions WHERE lead_id = ? AND ref = ?", (int(lead_id), ref))


def cleanup(now: float, click_keep_s: float, visit_keep_s: float, link_keep_s: float) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM site_events WHERE kind = 'tel_click' AND at < ? AND client_id = ''",
                     (now - click_keep_s,))
        conn.execute("DELETE FROM site_events WHERE at < ?", (now - visit_keep_s,))
        conn.execute("DELETE FROM call_starts WHERE started_at < ?", (now - click_keep_s,))
        conn.execute("DELETE FROM phone_clients WHERE at < ?", (now - link_keep_s,))
        conn.execute("DELETE FROM attributions WHERE at < ?", (now - link_keep_s,))
