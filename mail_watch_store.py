"""Память сторожа писем (см. mail_watch.py): что уже разобрали и где остановились.

Зачем на диске, а не в памяти процесса. Соседи вроде new_lead_watch держат состояние в
памяти осознанно — там ценность алерта живёт один день. Здесь наоборот: рестарт с пустой
памятью означает второй разбор тех же писем, а при включённом создании — вторую сделку по
той же переписке. Дубль сделки видит менеджер и клиент, поэтому память переживает
пересборку контейнера.

Три вещи, которые надо помнить:
    1. `last_ts` — правый край разобранного окна событий. Следующий проход читает от него.
    2. note_id — конкретное примечание-письмо. Дедуп первого уровня: то же самое событие
       прилетело второй раз (перекрытие окна, повтор прохода) — молча пропускаем.
    3. message_id — физическое письмо. Дедуп второго уровня и главный: ОДНО письмо amo
       кладёт примечанием в НЕСКОЛЬКО сделок контакта (замер 26.09.2026: 13 писем из 289
       легли в две-три сделки). Без этого ключа одно письмо породило бы три сделки.

Плюс `thread_id` → сделка, которую мы по этой переписке уже завели: пока она открыта,
следующее письмо той же цепочки новой сделки не создаёт (клиент за три дня пишет три
раза — это один диалог, а не три обращения).

Синхронный sqlite3 без зависимостей, вызовы оборачиваются в asyncio.to_thread — по
образцу site_form_store.py.
"""

import os
import sqlite3
import time
from contextlib import contextmanager

# /app/var примонтирован с хоста (docker-compose.yml), иначе память стиралась бы
# на каждой пересборке образа — ровно то, от чего этот файл и защищает.
DB_PATH = os.getenv("MAIL_WATCH_DB_PATH", "/app/var/mail_watch.sqlite3")

# Решения, которыми помечается разобранное письмо. Нужны не для логики, а для разбора
# глазами: «почему сторож промолчал» должно отвечаться запросом к этой таблице.
DECISION_CREATED = "created"            # сделка создана
DECISION_REPORT_ONLY = "report-only"    # подошло по всем правилам, но создание выключено
DECISION_OPEN_LEAD = "open-lead"        # письмо попало и в открытую сделку — не наш случай
DECISION_CONTACT_HAS_OPEN = "contact-has-open"  # у клиента есть другая открытая сделка
DECISION_THREAD_ACTIVE = "thread-active"        # по этой переписке наша сделка уже открыта
DECISION_SERVICE_SENDER = "service-sender"      # робот, рассылка, уведомление
DECISION_DUPLICATE = "duplicate"        # то же письмо, уже разобрано в другой сделке
DECISION_FAILED = "failed"              # не смогли разобрать (amo не ответил и т.п.)


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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_notes (
                note_id     INTEGER PRIMARY KEY,
                message_id  TEXT,
                thread_id   TEXT,
                entity_type TEXT,
                entity_id   INTEGER,
                decision    TEXT,
                lead_id     INTEGER,
                mail_at     INTEGER,
                sender      TEXT,
                subject     TEXT,
                created_at  INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_seen_message ON seen_notes(message_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_seen_thread ON seen_notes(thread_id)")


def get_last_ts() -> int:
    with _connect() as conn:
        row = conn.execute("SELECT value FROM state WHERE key = 'last_ts'").fetchone()
    if not row:
        return 0
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return 0


def set_last_ts(ts: int) -> None:
    """Правый край окна двигаем только вперёд: параллельный или запоздавший проход не
    должен откатывать границу назад и заставлять перечитывать сутки событий."""
    with _connect() as conn:
        row = conn.execute("SELECT value FROM state WHERE key = 'last_ts'").fetchone()
        current = 0
        if row:
            try:
                current = int(row[0])
            except (TypeError, ValueError):
                current = 0
        if int(ts) <= current:
            return
        conn.execute(
            "INSERT INTO state(key, value) VALUES('last_ts', ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(int(ts)),),
        )


def note_seen(note_id: int) -> bool:
    with _connect() as conn:
        row = conn.execute("SELECT 1 FROM seen_notes WHERE note_id = ?", (int(note_id),)).fetchone()
    return row is not None


def message_decision(message_id: str) -> dict | None:
    """Разбирали ли уже ЭТО ЖЕ письмо (возможно, в другой сделке). Возвращает первую
    запись с решением, отличным от `duplicate` — то есть ту, где решение принималось."""
    if not message_id:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT note_id, decision, lead_id, entity_id FROM seen_notes "
            "WHERE message_id = ? AND decision IS NOT NULL AND decision != ? "
            "ORDER BY created_at LIMIT 1",
            (str(message_id), DECISION_DUPLICATE),
        ).fetchone()
    if not row:
        return None
    return {"note_id": row[0], "decision": row[1], "lead_id": row[2], "entity_id": row[3]}


def thread_lead(thread_id: str) -> int | None:
    """Сделка, созданная нами по этой переписке (последняя, если их несколько)."""
    if not thread_id:
        return None
    with _connect() as conn:
        row = conn.execute(
            "SELECT lead_id FROM seen_notes WHERE thread_id = ? AND decision = ? AND lead_id IS NOT NULL "
            "ORDER BY created_at DESC LIMIT 1",
            (str(thread_id), DECISION_CREATED),
        ).fetchone()
    return row[0] if row else None


def mark(
    note_id: int,
    *,
    decision: str,
    message_id: str | None = None,
    thread_id: str | None = None,
    entity_type: str | None = None,
    entity_id: int | None = None,
    lead_id: int | None = None,
    mail_at: int | None = None,
    sender: str | None = None,
    subject: str | None = None,
) -> None:
    """Адрес и тему храним не для логики, а чтобы на вопрос «почему сторож сработал (или
    промолчал) на этом письме» отвечала одна строка таблицы, а не археология по логам."""
    with _connect() as conn:
        conn.execute(
            "INSERT INTO seen_notes(note_id, message_id, thread_id, entity_type, entity_id, "
            "decision, lead_id, mail_at, sender, subject, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(note_id) DO UPDATE SET decision = excluded.decision, "
            "lead_id = COALESCE(excluded.lead_id, seen_notes.lead_id)",
            (
                int(note_id),
                str(message_id) if message_id else None,
                str(thread_id) if thread_id else None,
                entity_type,
                int(entity_id) if entity_id else None,
                decision,
                int(lead_id) if lead_id else None,
                int(mail_at) if mail_at else None,
                sender,
                subject,
                int(time.time()),
            ),
        )


def recent(limit: int = 50) -> list[dict]:
    """Последние разборы — для статусной ручки и разбора «почему промолчали»."""
    with _connect() as conn:
        rows = conn.execute(
            "SELECT note_id, message_id, thread_id, entity_type, entity_id, decision, lead_id, "
            "mail_at, sender, subject, created_at FROM seen_notes "
            "ORDER BY created_at DESC, note_id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    keys = ("note_id", "message_id", "thread_id", "entity_type", "entity_id",
            "decision", "lead_id", "mail_at", "sender", "subject", "created_at")
    return [dict(zip(keys, r)) for r in rows]


def counts_by_decision() -> dict[str, int]:
    with _connect() as conn:
        rows = conn.execute(
            "SELECT decision, COUNT(*) FROM seen_notes GROUP BY decision"
        ).fetchall()
    return {r[0] or "?": r[1] for r in rows}
