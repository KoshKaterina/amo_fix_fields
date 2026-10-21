"""SQLite-очередь событий GetCourse и связки «GetCourse → amo» (см. getcourse_service.py).

Зачем отдельное хранилище. Процесс GetCourse ждёт ответа не дольше 10 секунд и при сбое
вызов не повторяет. Поэтому событие принимается ответом 200 сразу после записи сюда, а сделка
в amo создаётся или дополняется фоном с повторами. Рестарт контейнера принятое не теряет.

Ключ - event_id = «ID заказа GetCourse:событие». Повтор того же события (процесс перезапустили,
сверка нашла пропуск, который на самом деле дошёл) находит строку и второй раз не обрабатывается.

Статусы:
    pending    - ждёт обработки (в том числе повтора: next_try_at в будущем)
    processing - взята обработчиком
    done       - сделка создана или дополнена; персональные данные из строки стёрты
    skipped    - обрабатывать нечего (оплата без открытой сделки, тестовый фильтр); данные стёрты
    review     - нужен человек: несколько открытых сделок, конфликт контактов, нет ключей
    failed     - повторы кончились; данные хранятся до очистки для ручного разбора

Повтор безопасен на любом шаге, кроме создания сделки: примечание защищено маркером, теги
дописываются к имеющимся, поля заполняются только пустые. Для создания стоит отметка
create_started: если ответ amo потерян, событие ждёт и потом заново ищет человека - созданная
в тот раз сделка найдётся как открытая и будет дополнена, а не продублирована.
"""

import os
import sqlite3
import time
from contextlib import contextmanager

# /app/var примонтирован с хоста (docker-compose.yml: ./amo_fix_fields/var:/app/var):
# без этого очередь стиралась бы на каждой пересборке.
DB_PATH = os.getenv("GETCOURSE_DB_PATH", "/app/var/getcourse.sqlite3")

_COLS = ("event_id, event, order_id, order_number, gc_user_id, status, payload, lead_id, contact_id, "
         "action, create_started, attempts, next_try_at, last_error, created_at, updated_at")
_NAMES = [c.strip() for c in _COLS.split(",")]

FINAL = ("done", "skipped", "review", "failed")


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _row(r) -> dict:
    return dict(zip(_NAMES, r))


def init_db() -> None:
    directory = os.path.dirname(DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS getcourse_events (
                event_id TEXT PRIMARY KEY,
                event TEXT NOT NULL,
                order_id TEXT NOT NULL,
                order_number TEXT,
                gc_user_id TEXT,
                status TEXT NOT NULL,
                payload TEXT,
                lead_id INTEGER,
                contact_id INTEGER,
                action TEXT,
                create_started INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_try_at REAL NOT NULL,
                last_error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            )"""
        )
        conn.execute("CREATE INDEX IF NOT EXISTS getcourse_due ON getcourse_events (status, next_try_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS getcourse_order ON getcourse_events (order_id)")
        # Пользователь GetCourse -> контакт amo: самый надёжный ключ для следующих заказов
        # того же человека (почту и телефон он может сменить, номер ученика - нет).
        conn.execute(
            """CREATE TABLE IF NOT EXISTS getcourse_contacts (
                gc_user_id TEXT PRIMARY KEY,
                contact_id INTEGER NOT NULL,
                updated_at REAL NOT NULL
            )"""
        )
        # О чём уже сказали в технический чат: новая страница формы, пропущенный заказ.
        # Алерт по каждому ключу - один раз, а не на каждой сверке.
        conn.execute(
            "CREATE TABLE IF NOT EXISTS getcourse_seen (key TEXT PRIMARY KEY, first_seen REAL NOT NULL)"
        )


def insert_pending(event_id: str, event: str, order_id: str, order_number: str, gc_user_id: str,
                   payload_json: str, now: float | None = None) -> tuple[bool, dict]:
    """Кладёт событие в очередь. (True, строка) - новое; (False, строка) - такое уже было."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO getcourse_events "
            "(event_id, event, order_id, order_number, gc_user_id, status, payload, next_try_at, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?)",
            (event_id, event, order_id, order_number, gc_user_id, payload_json, now, now, now),
        )
        inserted = cur.rowcount == 1
        row = conn.execute(f"SELECT {_COLS} FROM getcourse_events WHERE event_id = ?", (event_id,)).fetchone()
    return inserted, _row(row)


def get(event_id: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(f"SELECT {_COLS} FROM getcourse_events WHERE event_id = ?", (event_id,)).fetchone()
    return _row(row) if row else None


def due(now: float | None = None, limit: int = 20) -> list[dict]:
    now = time.time() if now is None else now
    with _connect() as conn:
        rows = conn.execute(
            f"SELECT {_COLS} FROM getcourse_events WHERE status = 'pending' AND next_try_at <= ? "
            "ORDER BY next_try_at LIMIT ?",
            (now, limit),
        ).fetchall()
    return [_row(r) for r in rows]


def claim(event_id: str, now: float | None = None) -> bool:
    """Право на обработку получает один обработчик; конкурентный UPDATE вернёт False."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE getcourse_events SET status = 'processing', updated_at = ? "
            "WHERE event_id = ? AND status = 'pending' AND next_try_at <= ?",
            (now, event_id, now),
        )
        return cur.rowcount == 1


def mark_create_started(event_id: str, now: float | None = None) -> None:
    """Пишется ДО запроса на создание: после него потерянный ответ уже не доказывает, что сделки нет."""
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE getcourse_events SET create_started = 1, updated_at = ? WHERE event_id = ?",
            (now, event_id),
        )


def set_lead(event_id: str, lead_id: int, contact_id: int | None, now: float | None = None) -> None:
    """Запоминает сделку сразу после создания: следующий повтор дополняет её, а не ищет заново."""
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE getcourse_events SET lead_id = ?, contact_id = COALESCE(?, contact_id), updated_at = ? "
            "WHERE event_id = ?",
            (int(lead_id), int(contact_id) if contact_id else None, now, event_id),
        )


def reschedule(event_id: str, attempts: int, next_try_at: float, error: str, now: float | None = None) -> None:
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "UPDATE getcourse_events SET status = 'pending', attempts = ?, next_try_at = ?, last_error = ?, "
            "updated_at = ? WHERE event_id = ? AND status = 'processing'",
            (int(attempts), next_try_at, error[:500], now, event_id),
        )


def finish(event_id: str, status: str, action: str, *, lead_id: int | None = None,
           contact_id: int | None = None, error: str | None = None, now: float | None = None) -> bool:
    """Итог события. done/skipped стирают присланные данные: персональные данные живут в amo, не здесь."""
    if status not in FINAL:
        raise ValueError(status)
    now = time.time() if now is None else now
    erase = status in ("done", "skipped")
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE getcourse_events SET status = ?, action = ?, "
            "lead_id = COALESCE(?, lead_id), contact_id = COALESCE(?, contact_id), "
            f"last_error = ?, updated_at = ?{', payload = NULL' if erase else ''} "
            "WHERE event_id = ? AND status = 'processing'",
            (status, action[:200], int(lead_id) if lead_id else None, int(contact_id) if contact_id else None,
             (error or None) and error[:500], now, event_id),
        )
        return cur.rowcount == 1


def recover_stale(older_than_s: float, delay_s: float, now: float | None = None) -> int:
    """processing, брошенные упавшим процессом, - обратно в pending с паузой.

    Пауза нужна из-за создания сделки: если процесс упал после запроса в amo, сделка могла
    появиться. Через паузу поиск человека её найдёт и дополнит, а не создаст вторую.
    """
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE getcourse_events SET status = 'pending', next_try_at = ?, "
            "last_error = 'recovered-after-restart', updated_at = ? "
            "WHERE status = 'processing' AND updated_at < ?",
            (now + delay_s, now, now - older_than_s),
        )
        return cur.rowcount


def link_contact(gc_user_id: str, contact_id: int, now: float | None = None) -> None:
    if not gc_user_id or not contact_id:
        return
    now = time.time() if now is None else now
    with _connect() as conn:
        conn.execute(
            "INSERT INTO getcourse_contacts (gc_user_id, contact_id, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(gc_user_id) DO UPDATE SET contact_id = excluded.contact_id, updated_at = excluded.updated_at",
            (str(gc_user_id), int(contact_id), now),
        )


def contact_for(gc_user_id: str) -> int | None:
    if not gc_user_id:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT contact_id FROM getcourse_contacts WHERE gc_user_id = ?", (str(gc_user_id),)
        ).fetchone()
    return int(row[0]) if row else None


def first_seen(key: str, now: float | None = None) -> bool:
    """True - ключ видим впервые (и запоминаем его)."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute("INSERT OR IGNORE INTO getcourse_seen (key, first_seen) VALUES (?, ?)", (key, now))
        return cur.rowcount == 1


def known_orders(values: list[str], event: str) -> set[str]:
    """Какие из значений уже есть в очереди с этим событием - как ID заказа ИЛИ как его номер.

    Сверка не знает наверняка, что выгрузка GetCourse зовёт «ID заказа»: внутренний ID из
    адреса карточки или номер, который видит человек. Поэтому сравниваем с обоими.
    """
    if not values:
        return set()
    found: set[str] = set()
    with _connect() as conn:
        for i in range(0, len(values), 400):
            chunk = [str(x) for x in values[i:i + 400]]
            placeholders = ", ".join("?" for _ in chunk)
            rows = conn.execute(
                f"SELECT order_id, order_number FROM getcourse_events WHERE event = ? "
                f"AND (order_id IN ({placeholders}) OR order_number IN ({placeholders}))",
                (event, *chunk, *chunk),
            ).fetchall()
            wanted = set(chunk)
            for order_id, order_number in rows:
                found.update(v for v in (order_id, order_number) if v in wanted)
    return found


def counts() -> dict[str, int]:
    with _connect() as conn:
        rows = conn.execute("SELECT status, COUNT(*) FROM getcourse_events GROUP BY status").fetchall()
    return {r[0]: r[1] for r in rows}


def purge(keep_days: int, now: float | None = None) -> int:
    """Удаляет законченные строки старше keep_days. review и failed тоже: к этому сроку их разобрали."""
    now = time.time() if now is None else now
    with _connect() as conn:
        cur = conn.execute(
            "DELETE FROM getcourse_events WHERE status IN ('done', 'skipped', 'review', 'failed') AND updated_at < ?",
            (now - keep_days * 86400,),
        )
        return cur.rowcount
