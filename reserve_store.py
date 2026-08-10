"""SQLite-хранилище состояния резерва (см. reserve_service.py).

Нужно отдельно от остального проекта: там всё состояние живёт в памяти
процесса (queue_manager.lead_last_processed и т.д.) и не переживает рестарт/деплой,
а 3-дневный таймер тайм-аута резерва должен.
Синхронный sqlite3 (без внешних зависимостей) — вызывается из reserve_service
через asyncio.to_thread, чтобы не блокировать event loop.
"""

import datetime
import os
import sqlite3
from contextlib import contextmanager

# /app/var примонтирован с хоста (docker-compose.yml: ./amo_fix_fields/var:/app/var) — туда же
# пишут order_watchdog и showroom_alert. Без этого база легла бы в /app внутрь
# контейнера и стиралась на каждой пересборке — то есть ровно то, ради чего
# хранилище заводилось (таймер должен переживать деплой), не работало бы.
DB_PATH = os.getenv("RESERVE_DB_PATH", "/app/var/reserve_state.sqlite3")

_UTC = datetime.timezone.utc


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


_COLS = "lead_id, ms_order_uuid, pipeline_id, reserved_at, updated_at, timeout_exempt"


def _row(r) -> dict:
    return {
        "lead_id": r[0],
        "ms_order_uuid": r[1],
        "pipeline_id": r[2],
        "reserved_at": r[3],
        "updated_at": r[4],
        "timeout_exempt": bool(r[5]),
    }


def init() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reserve_state (
                lead_id INTEGER PRIMARY KEY,
                ms_order_uuid TEXT NOT NULL,
                pipeline_id INTEGER NOT NULL,
                reserved_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                timeout_exempt INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        # База могла быть создана раньше, без колонки — добавляем на месте.
        have = {r[1] for r in conn.execute("PRAGMA table_info(reserve_state)").fetchall()}
        if "timeout_exempt" not in have:
            conn.execute(
                "ALTER TABLE reserve_state ADD COLUMN timeout_exempt INTEGER NOT NULL DEFAULT 0"
            )


def mark_reserved(
    lead_id: int, ms_order_uuid: str, pipeline_id: int, timeout_exempt: bool = False
) -> None:
    """Идемпотентно: если запись по сделке уже есть, reserved_at НЕ трогаем —
    таймер не перезапускается при повторной постановке резерва (например, при
    изменении корзины) в рамках одного цикла резерва.

    timeout_exempt — резерв бессрочный (оплачено либо отложено осознанно).
    Такая запись всё равно живёт в таблице: по ней фоновая сверка ловит отгрузку.
    Признак обновляется на каждом вызове: сделка могла дойти до оплаты."""
    now = datetime.datetime.now(_UTC).isoformat()
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO reserve_state
                (lead_id, ms_order_uuid, pipeline_id, reserved_at, updated_at, timeout_exempt)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(lead_id) DO UPDATE SET
                ms_order_uuid = excluded.ms_order_uuid,
                pipeline_id = excluded.pipeline_id,
                updated_at = excluded.updated_at,
                timeout_exempt = excluded.timeout_exempt
            """,
            (lead_id, ms_order_uuid, pipeline_id, now, now, 1 if timeout_exempt else 0),
        )


def get(lead_id: int) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            f"SELECT {_COLS} FROM reserve_state WHERE lead_id = ?", (lead_id,)
        ).fetchone()
    return _row(row) if row is not None else None


def clear(lead_id: int) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM reserve_state WHERE lead_id = ?", (lead_id,))


def list_expired(cutoff_iso: str) -> list[dict]:
    """Записи старше тайм-аута, по которым таймер вообще действует. Оплаченные
    и осознанно отложенные (timeout_exempt) сюда НЕ попадают — их резерв
    бессрочен и снимается только по статусу или по отгрузке."""
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT {_COLS} FROM reserve_state "
            "WHERE reserved_at < ? AND timeout_exempt = 0",
            (cutoff_iso,),
        ).fetchall()
    return [_row(r) for r in rows]


def list_active() -> list[dict]:
    """Все заказы, под которыми сейчас что-то зарезервировано — включая
    бессрочные. Фоновая сверка ходит по этому списку искать отгрузки:
    отгрузка статус сделки не меняет, значит вебхука по ней не придёт."""
    with _connect() as conn:
        rows = conn.execute(f"SELECT {_COLS} FROM reserve_state").fetchall()
    return [_row(r) for r in rows]
