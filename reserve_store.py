"""SQLite-хранилище состояния резерва (см. reserve_service.py).

Нужно отдельно от остального проекта: там всё состояние живёт в памяти
процесса (queue_manager.lead_last_processed, ms_status_sync._seen и т.д.) и
не переживает рестарт/деплой — а 3-дневный таймер тайм-аута резерва должен.
Синхронный sqlite3 (без внешних зависимостей) — вызывается из reserve_service
через asyncio.to_thread, чтобы не блокировать event loop.
"""

import datetime
import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.getenv("RESERVE_DB_PATH", "reserve_state.sqlite3")

_UTC = datetime.timezone.utc


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init() -> None:
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reserve_state (
                lead_id INTEGER PRIMARY KEY,
                ms_order_uuid TEXT NOT NULL,
                pipeline_id INTEGER NOT NULL,
                reserved_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )


def mark_reserved(lead_id: int, ms_order_uuid: str, pipeline_id: int) -> None:
    """Идемпотентно: если запись по сделке уже есть, reserved_at НЕ трогаем —
    таймер не перезапускается при повторной постановке резерва (например, при
    изменении корзины) в рамках одного цикла резерва."""
    now = datetime.datetime.now(_UTC).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO reserve_state (lead_id, ms_order_uuid, pipeline_id, reserved_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(lead_id) DO UPDATE SET
                ms_order_uuid = excluded.ms_order_uuid,
                pipeline_id = excluded.pipeline_id,
                updated_at = excluded.updated_at
            """,
            (lead_id, ms_order_uuid, pipeline_id, now, now),
        )


def get(lead_id: int) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            "SELECT lead_id, ms_order_uuid, pipeline_id, reserved_at, updated_at "
            "FROM reserve_state WHERE lead_id = ?",
            (lead_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "lead_id": row[0],
        "ms_order_uuid": row[1],
        "pipeline_id": row[2],
        "reserved_at": row[3],
        "updated_at": row[4],
    }


def clear(lead_id: int) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM reserve_state WHERE lead_id = ?", (lead_id,))


def list_expired(cutoff_iso: str) -> list[dict]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT lead_id, ms_order_uuid, pipeline_id, reserved_at, updated_at "
            "FROM reserve_state WHERE reserved_at < ?",
            (cutoff_iso,),
        ).fetchall()
    return [
        {
            "lead_id": r[0],
            "ms_order_uuid": r[1],
            "pipeline_id": r[2],
            "reserved_at": r[3],
            "updated_at": r[4],
        }
        for r in rows
    ]
