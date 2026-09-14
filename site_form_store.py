"""SQLite-очередь заявок с форм сайта (см. site_form_service.py).

Зачем отдельное хранилище. Заявка схемы 2 принимается ответом 200 сразу после записи сюда,
а сделка в amo создаётся фоном с повторами. Так недоступность amo не превращается в ошибку
для человека на сайте, а рестарт контейнера не теряет принятую заявку.

Ключ - submission_id: одна попытка человека на сайте. Повтор той же попытки (двойной клик,
повтор запроса сервером WP после таймаута) находит строку и второй сделки не создаёт.

Статусы:
    pending  - принята, сделки ещё нет
    created  - сделка создана (lead_id записан), осталась доводка: этап, теги, примечание
    done     - всё сделано, персональные данные из строки стёрты
    failed   - повторы кончились; данные хранятся до очистки для ручного разбора

Синхронный sqlite3 без зависимостей, вызывается через asyncio.to_thread.
"""

import os
import sqlite3
import time
from contextlib import contextmanager

# /app/var примонтирован с хоста (docker-compose.yml: ./amo_fix_fields/var:/app/var):
# без этого очередь стиралась бы на каждой пересборке.
DB_PATH = os.getenv("SITE_FORM_DB_PATH", "/app/var/site_form.sqlite3")

_COLS = "submission_id, form, status, payload, lead_id, unsorted_uid, attempts, next_try_at, last_error, created_at, updated_at"


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _row(r) -> dict:
    return dict(zip([c.strip() for c in _COLS.split(",")], r))


def init_db() -> None:
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS site_form_submissions (
                submission_id TEXT PRIMARY KEY,
                form TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT,
                lead_id INTEGER,
                unsorted_uid TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_try_at REAL NOT NULL,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS site_form_due ON site_form_submissions (status, next_try_at)"
        )


def insert_pending(submission_id: str, form: str, payload_json: str, now: float | None = None) -> tuple[bool, dict]:
    """Кладёт заявку в очередь. (True, строка) - новая; (False, строка) - такая попытка уже была."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO site_form_submissions "
            "(submission_id, form, status, payload, attempts, next_try_at, created_at, updated_at) "
            "VALUES (?, ?, 'pending', ?, 0, ?, ?, ?)",
            (submission_id, form, payload_json, now, now, now),
        )
        inserted = cur.rowcount == 1
        row = conn.execute(
            f"SELECT {_COLS} FROM site_form_submissions WHERE submission_id = ?", (submission_id,)
        ).fetchone()
    return inserted, _row(row)


def get(submission_id: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            f"SELECT {_COLS} FROM site_form_submissions WHERE submission_id = ?", (submission_id,)
        ).fetchone()
    return _row(row) if row else None


def due(now: float | None = None, limit: int = 20) -> list[dict]:
    now = time.time() if now is None else now
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT {_COLS} FROM site_form_submissions "
            "WHERE status IN ('pending', 'created') AND next_try_at <= ? "
            "ORDER BY next_try_at LIMIT ?",
            (now, limit),
        ).fetchall()
    return [_row(r) for r in rows]


def mark_created(submission_id: str, lead_id: int, unsorted_uid: str | None, now: float | None = None) -> None:
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE site_form_submissions SET status = 'created', lead_id = ?, unsorted_uid = ?, updated_at = ? "
            "WHERE submission_id = ?",
            (int(lead_id), unsorted_uid, now, submission_id),
        )


def mark_done(submission_id: str, lead_id: int, now: float | None = None) -> None:
    """Сделка доведена - стираем заявку целиком: персональные данные живут в amo, не здесь."""
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE site_form_submissions SET status = 'done', lead_id = ?, payload = NULL, last_error = NULL, "
            "updated_at = ? WHERE submission_id = ?",
            (int(lead_id), now, submission_id),
        )


def mark_retry(submission_id: str, attempts: int, next_try_at: float, error: str, now: float | None = None) -> None:
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE site_form_submissions SET attempts = ?, next_try_at = ?, last_error = ?, updated_at = ? "
            "WHERE submission_id = ?",
            (int(attempts), next_try_at, error[:500], now, submission_id),
        )


def mark_failed(submission_id: str, attempts: int, error: str, now: float | None = None) -> None:
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE site_form_submissions SET status = 'failed', attempts = ?, last_error = ?, updated_at = ? "
            "WHERE submission_id = ?",
            (int(attempts), error[:500], now, submission_id),
        )


def purge(keep_days: int, now: float | None = None) -> int:
    """Удаляет законченные и проваленные заявки старше keep_days. Возвращает число строк."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "DELETE FROM site_form_submissions WHERE status IN ('done', 'failed') AND updated_at < ?",
            (now - keep_days * 86400,),
        )
        return cur.rowcount
