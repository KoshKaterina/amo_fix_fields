"""SQLite-хранилище состояния авто-режима (см. autopilot.py).

Зачем отдельно от остального проекта: всё состояние здесь живёт в памяти процесса
(queue_manager, дедуп-окна в ozon_invoice) и не переживает рестарт. Авто-режиму этого
мало по трём причинам, каждая из них - отправленное дважды сообщение живому человеку:

1. **Гейт от повторного вебхука.** `/lead_change` приходит на ЛЮБОЕ изменение сделки, а не
   на смену этапа: правка поля менеджером, синк из МойСклада, тег, смена ответственного. По
   сделке, спокойно стоящей на этапе, прилетят десятки вебхуков. Окна в памяти (как
   `RECENT_TTL_S` у соседа) хватает на две минуты и не хватает на рестарт.
2. **Рестарт в момент запуска бота.** Отметок ДВЕ - `launch_attempted_at` до вызова amoCRM и
   `launch_ok_at` после. Запись с первой и без второй при старте НЕ перезапускаем, а зовём
   человека: лучше не отправить, чем отправить дважды.
3. **Сон до утра.** Заказ, пришедший ночью, ждёт начала рабочих часов. Память этого не
   переживёт, а ждать иногда приходится десять часов.

Синхронный sqlite3 без внешних зависимостей - вызывается из `autopilot.py` через
`asyncio.to_thread`, как это делает `reserve_store`.

⚠️ Ключ - ПАРА «сделка и этап», а не сделка. Одна сделка проходит несколько этапов маршрута,
и на каждом у неё своё ожидание. Ключ по сделке схлопнул бы историю прохода в одну строку и
разрешил бы повторный запуск бота на этапе, где он уже отработал.
"""

import datetime
import os
import sqlite3
from contextlib import contextmanager

# /app/var примонтирован с хоста - туда же пишут reserve_store, order_watchdog и
# showroom_alert. Без этого база легла бы внутрь контейнера и стиралась на каждой
# пересборке, то есть ровно то, ради чего хранилище заводится, не работало бы.
DB_PATH = os.getenv("AUTOPILOT_DB_PATH", "/app/var/autopilot_state.sqlite3")

_UTC = datetime.timezone.utc

# Фазы ведения. Это не «красивое перечисление», а ответ на вопрос «чего мы ждём прямо
# сейчас»: по нему движок решает, что делать с пришедшим событием.
PHASE_LAUNCHING = "launching"    # бот вызван, ответа amoCRM ещё нет
PHASE_DELIVERY = "delivery"      # ждём статус доставки от Wazzup
PHASE_REPLY = "reply"            # ждём ответ клиента
PHASE_SLEEPING = "sleeping"      # вне рабочих часов, продолжим в wake_at
PHASE_DONE = "done"              # этап пройден, бот больше не нужен
PHASE_STOPPED = "stopped"        # остановились и позвали человека


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _now() -> str:
    return datetime.datetime.now(_UTC).isoformat()


_COLS = (
    "lead_id, status_id, pipeline_id, bot_id, phase, launch_attempted_at, launch_ok_at, "
    "chat_id, wake_at, created_at, updated_at, note"
)


def _row(r) -> dict:
    return {
        "lead_id": r[0],
        "status_id": r[1],
        "pipeline_id": r[2],
        "bot_id": r[3],
        "phase": r[4],
        "launch_attempted_at": r[5],
        "launch_ok_at": r[6],
        "chat_id": r[7],
        "wake_at": r[8],
        "created_at": r[9],
        "updated_at": r[10],
        "note": r[11],
    }


def init() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS autopilot_state (
                lead_id INTEGER NOT NULL,
                status_id INTEGER NOT NULL,
                pipeline_id INTEGER NOT NULL,
                bot_id INTEGER,
                phase TEXT NOT NULL,
                launch_attempted_at TEXT,
                launch_ok_at TEXT,
                chat_id TEXT,
                wake_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                note TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (lead_id, status_id)
            )
            """
        )
        # По чату ищем сделку, когда прилетает сообщение от клиента: Wazzup знает чат, но не
        # знает сделку. Без индекса это был бы полный перебор на каждое входящее.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_autopilot_state_chat ON autopilot_state (chat_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_autopilot_state_wake ON autopilot_state (wake_at)"
        )


def get(lead_id: int, status_id: int) -> dict | None:
    with _connect() as conn:
        cur = conn.execute(
            f"SELECT {_COLS} FROM autopilot_state WHERE lead_id = ? AND status_id = ?",
            (lead_id, status_id),
        )
        row = cur.fetchone()
    return _row(row) if row else None


def claim(lead_id: int, status_id: int, pipeline_id: int) -> bool:
    """Взять пару «сделка и этап» в работу. True - взяли, False - уже занято.

    Это и есть гейт от повторного вебхука: первый вебхук создаёт строку и получает True,
    все последующие по той же паре получают False и не делают ничего. Атомарность даёт сам
    первичный ключ, а не проверка «сначала посмотрим, потом вставим» - между «посмотрим» и
    «вставим» успевает пройти второй вебхук.
    """
    now = _now()
    with _connect() as conn:
        try:
            conn.execute(
                "INSERT INTO autopilot_state "
                "(lead_id, status_id, pipeline_id, phase, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (lead_id, status_id, pipeline_id, PHASE_LAUNCHING, now, now),
            )
        except sqlite3.IntegrityError:
            return False
    return True


def update(lead_id: int, status_id: int, **fields) -> None:
    """Точечная правка. Неизвестные колонки отвергаем: опечатка в имени поля иначе тихо
    ничего не сделает, а мы будем искать её в логике движка."""
    allowed = {
        "bot_id", "phase", "launch_attempted_at", "launch_ok_at", "chat_id", "wake_at", "note",
    }
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError(f"autopilot_store.update: неизвестные поля {sorted(unknown)}")
    if not fields:
        return
    sets = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [_now(), lead_id, status_id]
    with _connect() as conn:
        conn.execute(
            f"UPDATE autopilot_state SET {sets}, updated_at = ? "
            "WHERE lead_id = ? AND status_id = ?",
            values,
        )


def mark_launch_attempted(lead_id: int, status_id: int, bot_id: int) -> None:
    """Отметка ДО вызова amoCRM. См. пункт 2 в шапке модуля."""
    update(lead_id, status_id, bot_id=bot_id, launch_attempted_at=_now())


def mark_launch_ok(lead_id: int, status_id: int, chat_id: str | None = None) -> None:
    """Отметка ПОСЛЕ ответа amoCRM. Между этой и предыдущей - окно, в котором рестарт
    оставляет запись «попытка была, результат неизвестен»."""
    fields: dict = {"launch_ok_at": _now(), "phase": PHASE_DELIVERY}
    if chat_id:
        fields["chat_id"] = str(chat_id)
    update(lead_id, status_id, **fields)


def list_unfinished_launches() -> list[dict]:
    """Записи «попытка запуска была, подтверждения нет» - их разбирают на старте процесса.

    Перезапускать такое НЕЛЬЗЯ: возможно, бот отработал и клиент уже получил сообщение, а мы
    просто не успели записать. Зовём человека.
    """
    with _connect() as conn:
        cur = conn.execute(
            f"SELECT {_COLS} FROM autopilot_state "
            "WHERE launch_attempted_at IS NOT NULL AND launch_ok_at IS NULL "
            "AND phase = ?",
            (PHASE_LAUNCHING,),
        )
        rows = cur.fetchall()
    return [_row(r) for r in rows]


def find_by_chat(chat_id: str) -> list[dict]:
    """Сделки, которые ждут чего-то по этому чату. Обычно одна; несколько - когда сделка
    прошла несколько этапов, и на каждом осталась своя строка."""
    with _connect() as conn:
        cur = conn.execute(
            f"SELECT {_COLS} FROM autopilot_state WHERE chat_id = ? AND phase IN (?, ?)",
            (str(chat_id), PHASE_DELIVERY, PHASE_REPLY),
        )
        rows = cur.fetchall()
    return [_row(r) for r in rows]


def list_due(now: datetime.datetime | None = None) -> list[dict]:
    """Спящие, которым пора просыпаться."""
    moment = (now or datetime.datetime.now(_UTC)).isoformat()
    with _connect() as conn:
        cur = conn.execute(
            f"SELECT {_COLS} FROM autopilot_state "
            "WHERE phase = ? AND wake_at IS NOT NULL AND wake_at <= ?",
            (PHASE_SLEEPING, moment),
        )
        rows = cur.fetchall()
    return [_row(r) for r in rows]


def finish(lead_id: int, status_id: int, phase: str, note: str = "") -> None:
    """Закрыть ведение пары «сделка и этап»: этап пройден либо остановились."""
    update(lead_id, status_id, phase=phase, note=note[:500])


def drop_lead(lead_id: int) -> int:
    """Снять сделку с ведения целиком - её закрыли, увели в другую воронку, забрал человек."""
    with _connect() as conn:
        cur = conn.execute("DELETE FROM autopilot_state WHERE lead_id = ?", (lead_id,))
        return cur.rowcount


def purge_older_than(days: int) -> int:
    """Уборка за собой. Своих напоминаний молчащему клиенту мы не шлём (решение Кати
    08.09.2026), значит запись «ждём ответа» иначе живёт вечно, и каждое изменение сделки
    будет пытаться её продолжить. Снимаем МОЛЧА: это уборка, а не дожим клиента."""
    cutoff = (datetime.datetime.now(_UTC) - datetime.timedelta(days=days)).isoformat()
    with _connect() as conn:
        cur = conn.execute("DELETE FROM autopilot_state WHERE updated_at < ?", (cutoff,))
        return cur.rowcount


def stats() -> dict[str, int]:
    """Сколько сделок в какой фазе - для строки живости на экране панели."""
    with _connect() as conn:
        cur = conn.execute("SELECT phase, COUNT(*) FROM autopilot_state GROUP BY phase")
        return {row[0]: row[1] for row in cur.fetchall()}
