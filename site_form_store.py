"""SQLite-очередь заявок с форм сайта (см. site_form_service.py).

Зачем отдельное хранилище. Заявка схемы 2 принимается ответом 200 сразу после записи сюда,
а сделка в amo создаётся фоном с повторами. Так недоступность amo не превращается в ошибку
для человека на сайте, а рестарт контейнера не теряет принятую заявку.

Ключ - submission_id: одна попытка человека на сайте. Повтор той же попытки (двойной клик,
повтор запроса сервером WP после таймаута) находит строку и второй сделки не создаёт.

Статусы:
    pending  - принята, сделки ещё нет
    processing - pending атомарно взята одним worker перед внешним create
    created  - сделка создана (lead_id записан), осталась доводка: этап, теги, примечание
    finishing - created атомарно взята одним worker для доводки без нового create
    held     - новый тип формы временно выключен или его карта недостоверна; попытки не тратятся
    uncertain - внешний create мог состояться; автоматический повтор запрещён до сверки
    done     - всё сделано, персональные данные из строки стёрты
    failed   - повторы кончились; данные хранятся до очистки для ручного разбора

Синхронный sqlite3 без зависимостей; claim вызывается прямо, чтобы отмена
корутины не оставила UPDATE работающим в фоновом потоке без владельца.
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


def claim_pending(submission_id: str, now: float | None = None) -> bool:
    """Один worker получает право на create; конкурентный UPDATE вернёт False.

    processing намеренно не возвращается в due автоматически: после падения
    между успешным amo create и mark_created неизвестно, создана ли сделка.
    Такую строку нужно сверить вручную до повторной отправки.
    """
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE site_form_submissions SET status = 'processing', updated_at = ? "
            "WHERE submission_id = ? AND status = 'pending' AND next_try_at <= ?",
            (now, submission_id, now),
        )
        return cur.rowcount == 1


def claim_created(submission_id: str, now: float | None = None) -> bool:
    """Один worker доводит существующую сделку; второй не дублирует примечание."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE site_form_submissions SET status = 'finishing', updated_at = ? "
            "WHERE submission_id = ? AND status = 'created' AND lead_id IS NOT NULL AND next_try_at <= ?",
            (now, submission_id, now),
        )
        return cur.rowcount == 1


def release_claim(submission_id: str, now: float | None = None) -> bool:
    """Возвращает pending только когда внешний create ещё не начинался."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE site_form_submissions SET status = 'pending', updated_at = ? "
            "WHERE submission_id = ? AND status = 'processing' AND lead_id IS NULL",
            (now, submission_id),
        )
        return cur.rowcount == 1


def release_created_claim(submission_id: str, now: float | None = None) -> bool:
    """До начала примечания возвращает только к доводке сохранённой сделки."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE site_form_submissions SET status = 'created', updated_at = ? "
            "WHERE submission_id = ? AND status = 'finishing' AND lead_id IS NOT NULL",
            (now, submission_id),
        )
        return cur.rowcount == 1


def mark_uncertain(submission_id: str, reason: str, now: float | None = None) -> bool:
    """Не возвращает в due недоказанный исход внешнего create или note."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE site_form_submissions SET status = 'uncertain', last_error = ?, updated_at = ? "
            "WHERE submission_id = ? AND status IN ('processing', 'finishing')",
            (reason[:500], now, submission_id),
        )
        return cur.rowcount == 1


def mark_created(
    submission_id: str, lead_id: int, unsorted_uid: str | None, now: float | None = None,
    *, claim_finish: bool = False,
) -> bool:
    now = time.time() if now is None else now
    status = "finishing" if claim_finish else "created"
    expected_status = " AND status = 'processing'" if claim_finish else ""
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE site_form_submissions SET status = ?, lead_id = ?, unsorted_uid = ?, updated_at = ? "
            f"WHERE submission_id = ?{expected_status}",
            (status, int(lead_id), unsorted_uid, now, submission_id),
        )
        return cur.rowcount == 1


def mark_done(
    submission_id: str, lead_id: int, now: float | None = None, *, require_finishing: bool = False,
) -> bool:
    """Сделка доведена - стираем заявку целиком: персональные данные живут в amo, не здесь."""
    now = time.time() if now is None else now
    expected_status = " AND status = 'finishing'" if require_finishing else ""
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE site_form_submissions SET status = 'done', lead_id = ?, payload = NULL, last_error = NULL, "
            f"updated_at = ? WHERE submission_id = ?{expected_status}",
            (int(lead_id), now, submission_id),
        )
        return cur.rowcount == 1


def mark_retry(submission_id: str, attempts: int, next_try_at: float, error: str, now: float | None = None) -> None:
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE site_form_submissions SET status = CASE "
            "WHEN status = 'processing' THEN 'pending' WHEN status = 'finishing' THEN 'created' "
            "ELSE status END, "
            "attempts = ?, next_try_at = ?, last_error = ?, updated_at = ? "
            "WHERE submission_id = ?",
            (int(attempts), next_try_at, error[:500], now, submission_id),
        )


def mark_held(submission_id: str, reason: str, now: float | None = None) -> None:
    """Удерживает принятую заявку вне due без расходования попыток и потери lead_id."""
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE site_form_submissions SET status = 'held', last_error = ?, updated_at = ? "
            "WHERE submission_id = ? AND status IN ('pending', 'processing', 'created', 'finishing')",
            (reason[:500], now, submission_id),
        )


def release_held(forms: list[str], now: float | None = None) -> int:
    """При восстановлении карты возвращает held сразу в due, не создавая вторую сделку."""
    if not forms:
        return 0
    now = time.time() if now is None else now
    placeholders = ", ".join("?" for _ in forms)
    with _connect() as conn:
        cur = conn.execute(
            f"UPDATE site_form_submissions SET status = CASE WHEN lead_id IS NULL THEN 'pending' ELSE 'created' END, "
            "next_try_at = ?, last_error = NULL, updated_at = ? "
            f"WHERE status = 'held' AND form IN ({placeholders})",
            (now, now, *forms),
        )
        return cur.rowcount


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
