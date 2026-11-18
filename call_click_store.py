"""SQLite-хранилище коллтрекинга сайта (см. call_click_service.py).

Три таблицы:
    site_events    - клики по номеру на сайте (tel_click). Время at ставит сервер WordPress при
                     приёме, а не браузер. Хранятся 2 суток.
    call_starts    - начала входящих звонков Sipuni (call_id, телефон, время). Нужны, чтобы
                     понять, сколько разных людей звонило в окне клика. 2 суток.
    attributions   - что уже записано (ref = call:<id> в сделку, contact-call:<id> в контакт),
                     одна запись на звонок. 30 суток.
    metrika_calls  - звонки, отправленные в Метрику (call_id, ClientID, источник, время): правило
                     «тот же клиент по тому же источнику не чаще раза в 3 дня». 3 суток.
ClientID Метрики (решение Кати 18.11.2026) живёт только в кликах (2 суток) и в metrika_calls
(3 суток); связки «телефон ↔ ClientID» нет, прежняя таблица phone_clients удаляется при старте.

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
        conn.execute("DROP TABLE IF EXISTS phone_clients")
        conn.execute("DELETE FROM site_events WHERE kind != 'tel_click'")
        conn.execute("""CREATE TABLE IF NOT EXISTS metrika_calls (
            call_id TEXT PRIMARY KEY, client_id TEXT NOT NULL, source_key TEXT NOT NULL,
            at REAL NOT NULL, upload_id TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT '')""")
        conn.execute("CREATE INDEX IF NOT EXISTS metrika_calls_client ON metrika_calls (client_id, source_key, at)")
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


def metrika_claim(call_id: str, client_id: str, key: str, at: float, repeat_s: float, now: float) -> bool:
    """True - звонок надо отправить в Метрику (и он записан). False - этот звонок уже отправляли или
    тот же ClientID по тому же источнику отправлялся меньше repeat_s назад."""
    with _connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM metrika_calls WHERE call_id = ?", (call_id,)).fetchone():
            return False
        if conn.execute("SELECT 1 FROM metrika_calls WHERE client_id = ? AND source_key = ? AND at > ?",
                        (client_id, key, float(at) - repeat_s)).fetchone():
            return False
        conn.execute("INSERT INTO metrika_calls (call_id, client_id, source_key, at) VALUES (?, ?, ?, ?)",
                     (call_id, client_id, key, float(at)))
        return True


def metrika_release(call_id: str) -> None:
    """Загрузка не удалась - снимаем отметку, иначе повтор этого клиента молча потеряется."""
    with _connect() as conn:
        conn.execute("DELETE FROM metrika_calls WHERE call_id = ?", (call_id,))


def metrika_mark(call_id: str, upload_id: str, status: str) -> None:
    with _connect() as conn:
        conn.execute("UPDATE metrika_calls SET upload_id = ?, status = ? WHERE call_id = ?",
                     (upload_id, status, call_id))


def cleanup(now: float, click_keep_s: float, attribution_keep_s: float, metrika_keep_s: float = 3 * 86400) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM metrika_calls WHERE at < ?", (now - metrika_keep_s,))
        conn.execute("DELETE FROM site_events WHERE at < ?", (now - click_keep_s,))
        conn.execute("DELETE FROM call_starts WHERE started_at < ?", (now - click_keep_s,))
        conn.execute("DELETE FROM attributions WHERE at < ?", (now - attribution_keep_s,))
