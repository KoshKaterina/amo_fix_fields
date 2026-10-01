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
import json
import os
import sqlite3
from contextlib import contextmanager

# /app/var примонтирован с хоста - туда же пишут reserve_store, order_watchdog и
# showroom_alert. Без этого база легла бы внутрь контейнера и стиралась на каждой
# пересборке, то есть ровно то, ради чего хранилище заводится, не работало бы.
DB_PATH = os.getenv("AUTOPILOT_DB_PATH", "/app/var/autopilot_state.sqlite3")

# ⚠️ У режима «Призрак» состояние ОТДЕЛЬНОЕ, и это не удобство, а необходимость (27.09.2026).
# Ключ состояния - пара «сделка и этап», и занятая пара второй раз не берётся никогда: так
# работает гейт от повторного вебхука. Гоняй призрак по боевому потоку в общей базе - он занял
# бы пары по всем живым сделкам, и после включения боя робот эти сделки уже не тронул бы. То
# же и наоборот: призрак не должен видеть боевые ожидания и менять им фазу.
SHADOW_DB_PATH = os.getenv("AUTOPILOT_SHADOW_DB_PATH", DB_PATH + ".shadow")

# Как узнать, что сейчас режим призрака. Ставит `autopilot.py` при старте; без него считаем,
# что призрака нет - хранилище само про режимы ничего не знает и знать не должно.
_shadow_probe = None

_UTC = datetime.timezone.utc

# Фазы ведения. Это не «красивое перечисление», а ответ на вопрос «чего мы ждём прямо
# сейчас»: по нему движок решает, что делать с пришедшим событием.
PHASE_LAUNCHING = "launching"    # бот вызван, ответа amoCRM ещё нет
PHASE_DELIVERY = "delivery"      # ждём статус доставки от Wazzup
PHASE_REPLY = "reply"            # ждём ответ клиента
PHASE_SLEEPING = "sleeping"      # вне рабочих часов, продолжим в wake_at
PHASE_DONE = "done"              # этап пройден, бот больше не нужен
PHASE_STOPPED = "stopped"        # остановились и позвали человека


def set_shadow_probe(probe) -> None:
    """Сказать хранилищу, чем узнавать режим призрака. `None` - забыть (нужно тестам)."""
    global _shadow_probe
    _shadow_probe = probe


def db_path() -> str:
    """База, в которую пишем прямо сейчас. Спрашиваем режим на КАЖДОМ обращении, а не
    запоминаем при старте: режим меняют руками на экране в любой момент, и запомненный
    выбор увёл бы часть операций в чужую базу."""
    if _shadow_probe is None:
        return DB_PATH
    try:
        return SHADOW_DB_PATH if _shadow_probe() else DB_PATH
    except Exception:
        # Не смогли узнать режим - пишем в боевую базу. Это честнее обратного: боевое
        # ведение сделки важнее чистоты репетиции.
        return DB_PATH


@contextmanager
def _connect():
    conn = sqlite3.connect(db_path(), timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _now() -> str:
    return datetime.datetime.now(_UTC).isoformat()


_COLS = (
    "lead_id, status_id, pipeline_id, bot_id, phase, launch_attempted_at, launch_ok_at, "
    "chat_id, wake_at, created_at, updated_at, note, delivery, contact_name, reply_since"
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
        "delivery": _loads(r[12]),
        # Имя контакта - второй ключ поиска переписки: телеграмный чат по телефону не
        # находится (правка Кати 01.10.2026). Держим здесь, чтобы подбор не ходил за именем
        # в amoCRM каждые пять минут.
        "contact_name": r[13] if len(r) > 13 else "",
        # Когда начали ждать ОТВЕТ клиента. Отдельно от `updated_at`, потому что
        # `updated_at` двигает любая правка строки - и срок ожидания не истекал никогда
        # (разбор 01.10.2026: сделки висели 39 и 94 часа без единого алерта).
        "reply_since": r[14] if len(r) > 14 else "",
    }


def _loads(raw) -> list[dict]:
    """Копилка статусов доставки. Битую строку читаем как пустую: строка журнала не стоит
    того, чтобы из-за неё встало ведение сделки."""
    try:
        value = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return []
    return value if isinstance(value, list) else []


def init() -> None:
    """Схема - в ОБЕИХ базах, боевой и призрачной: режим переключают на экране в любой
    момент, и база должна быть готова заранее, а не создаваться на первом же событии."""
    for path in (DB_PATH, SHADOW_DB_PATH):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        _init_one(path)


def _init_one(path: str) -> None:
    conn = sqlite3.connect(path, timeout=10)
    try:
        _create_schema(conn)
        conn.commit()
    finally:
        conn.close()


def _create_schema(conn) -> None:
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
            delivery TEXT NOT NULL DEFAULT '[]',
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
    # Отметки «об этом по этой сделке уже сказали». Отдельная таблица, а не поле в
    # состоянии: уведомляем и о сделках, которые робот НЕ ведёт (заявка не «Заказ»), -
    # у них строки состояния нет и быть не должно.
    #
    # ⚠️ Почему на диске, а не в памяти процесса: пока дедуп жил множеством в памяти,
    # каждая пересборка контейнера обнуляла его, и по всем сделкам, стоящим на входном
    # этапе, уведомление уходило заново. Ключ - пара «сделка и повод».
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS autopilot_notified (
            lead_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (lead_id, kind)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_autopilot_notified_at ON autopilot_notified (created_at)"
    )
    _add_missing_columns(conn)


def _add_missing_columns(conn) -> None:
    """Догнать схему на базах, созданных прежними версиями.

    `CREATE TABLE IF NOT EXISTS` существующую таблицу не меняет, а боевая база живёт на диске
    с сентября - без этого шага новая колонка появилась бы только у тех, кто начинает с нуля.
    """
    have = {row[1] for row in conn.execute("PRAGMA table_info(autopilot_state)")}
    if "contact_name" not in have:
        conn.execute(
            "ALTER TABLE autopilot_state ADD COLUMN contact_name TEXT NOT NULL DEFAULT ''"
        )
    if "reply_since" not in have:
        conn.execute(
            "ALTER TABLE autopilot_state ADD COLUMN reply_since TEXT NOT NULL DEFAULT ''"
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
        "contact_name", "reply_since",
    }
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError(f"autopilot_store.update: неизвестные поля {sorted(unknown)}")
    if not fields:
        return
    # ⚠️ Вход в фазу ожидания ответа отмечаем ОДИН раз и больше не трогаем (правка Кати
    # 01.10.2026). До этого срок ожидания считался от `updated_at`, а его двигает любая правка
    # строки - подбор из переписки, статус доставки, запоминание чата. Робот сам обнулял свой
    # счётчик, и сделки висели в ожидании 39 и 94 часа, не получив ни одного алерта.
    if fields.get("phase") == PHASE_REPLY and "reply_since" not in fields:
        with _connect() as conn:
            cur = conn.execute(
                "SELECT reply_since FROM autopilot_state WHERE lead_id = ? AND status_id = ?",
                (lead_id, status_id),
            )
            got = cur.fetchone()
        if not (got and got[0]):
            fields["reply_since"] = _now()

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


def mark_launch_ok(lead_id: int, status_id: int, chat_id: str | None = None,
                   contact_name: str | None = None) -> None:
    """Отметка ПОСЛЕ ответа amoCRM. Между этой и предыдущей - окно, в котором рестарт
    оставляет запись «попытка была, результат неизвестен»."""
    fields: dict = {"launch_ok_at": _now(), "phase": PHASE_DELIVERY}
    if chat_id:
        fields["chat_id"] = str(chat_id)
    if contact_name:
        fields["contact_name"] = str(contact_name)[:255]
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


def start_next_bot(lead_id: int, status_id: int, bot_id: int) -> None:
    """Передать ход следующему боту ЭТОГО ЖЕ этапа.

    Ботов на этапе бывает несколько, и они идут цепочкой: первый спросил и получил ответ,
    второй пишет следующее. Строка состояния при этом одна на пару «сделка и этап», поэтому
    её надо честно обнулить под нового бота - иначе он унаследует чужие отметки запуска и
    чужую копилку статусов доставки, и окно ожидания у него истечёт ещё до отправки.
    """
    with _connect() as conn:
        conn.execute(
            "UPDATE autopilot_state SET bot_id = ?, phase = ?, launch_attempted_at = NULL, "
            "launch_ok_at = NULL, delivery = '[]', updated_at = ? "
            "WHERE lead_id = ? AND status_id = ?",
            (int(bot_id), PHASE_LAUNCHING, _now(), lead_id, status_id),
        )


def add_delivery_status(lead_id: int, status_id: int, entry: dict) -> list[dict]:
    """Дописать статус Wazzup в копилку сделки и вернуть копилку целиком.

    ⚠️ Именно на диск, а не в память процесса. Окно ожидания доставки - четверть часа, и
    выкатка внутри него не редкость. Держи мы статусы в памяти, каждый рестарт объявлял бы
    доставленные сообщения недоставленными и звал человека к сделкам, где всё в порядке.
    """
    with _connect() as conn:
        cur = conn.execute(
            "SELECT delivery FROM autopilot_state WHERE lead_id = ? AND status_id = ?",
            (lead_id, status_id),
        )
        row = cur.fetchone()
        if row is None:
            return []
        items = _loads(row[0])
        items.append(entry)
        conn.execute(
            "UPDATE autopilot_state SET delivery = ?, updated_at = ? "
            "WHERE lead_id = ? AND status_id = ?",
            (json.dumps(items, ensure_ascii=False), _now(), lead_id, status_id),
        )
    return items


def list_for_lead(lead_id: int) -> list[dict]:
    """Все строки сделки. По ним движок понимает, вёл ли он эту сделку вообще - и не лезет
    в чужие, которые человек провёл руками."""
    with _connect() as conn:
        cur = conn.execute(
            f"SELECT {_COLS} FROM autopilot_state WHERE lead_id = ?", (lead_id,)
        )
        rows = cur.fetchall()
    return [_row(r) for r in rows]


def list_by_phase(phase: str) -> list[dict]:
    """Кто сейчас в этой фазе. Нужно фоновому тику: окно ожидания доставки истекает
    молча, никакого события об этом не приходит."""
    with _connect() as conn:
        cur = conn.execute(
            f"SELECT {_COLS} FROM autopilot_state WHERE phase = ?", (phase,)
        )
        rows = cur.fetchall()
    return [_row(r) for r in rows]


def drop_lead(lead_id: int) -> int:
    """Снять сделку с ведения целиком - её закрыли, увели в другую воронку, забрал человек."""
    with _connect() as conn:
        cur = conn.execute("DELETE FROM autopilot_state WHERE lead_id = ?", (lead_id,))
        return cur.rowcount


def purge_older_than(days: int) -> int:
    """Уборка за собой. Своих напоминаний молчащему клиенту мы не шлём (решение Кати
    08.09.2026), значит запись «ждём ответа» иначе живёт вечно, и каждое изменение сделки
    будет пытаться её продолжить. Снимаем МОЛЧА: это уборка, а не дожим клиента.

    Чистим ОБЕ базы, боевую и призрачную: призрачную иначе не убирает никто - робот живёт в
    ней только в дни репетиций, а уборка идёт из его же фонового цикла.
    """
    cutoff = (datetime.datetime.now(_UTC) - datetime.timedelta(days=days)).isoformat()
    gone = 0
    for path in {db_path(), DB_PATH, SHADOW_DB_PATH}:
        if not os.path.exists(path):
            continue
        conn = sqlite3.connect(path, timeout=10)
        try:
            cur = conn.execute("DELETE FROM autopilot_state WHERE updated_at < ?", (cutoff,))
            gone += cur.rowcount
            conn.commit()
        finally:
            conn.close()
    return gone


def claim_notice(lead_id: int, kind: str) -> bool:
    """Взять право сказать про сделку один раз. True - говорим, False - уже говорили.

    Тот же приём, что в `claim`: атомарность даёт первичный ключ, а не «посмотрим и вставим».
    Между «посмотрим» и «вставим» успевает пройти второй вебхук, а цена промаха здесь -
    второе сообщение в рабочий чат по той же сделке.
    """
    with _connect() as conn:
        try:
            conn.execute(
                "INSERT INTO autopilot_notified (lead_id, kind, created_at) VALUES (?, ?, ?)",
                (lead_id, kind, _now()),
            )
        except sqlite3.IntegrityError:
            return False
    return True


def purge_notices_older_than(days: int) -> int:
    """Уборка отметок. Держать их вечно незачем: сделка, о которой говорили месяц назад, на
    входной этап больше не вернётся, а вернётся - сказать о ней заново правильно."""
    cutoff = (datetime.datetime.now(_UTC) - datetime.timedelta(days=days)).isoformat()
    with _connect() as conn:
        cur = conn.execute("DELETE FROM autopilot_notified WHERE created_at < ?", (cutoff,))
        return cur.rowcount


def stats() -> dict[str, int]:
    """Сколько сделок в какой фазе - для строки живости на экране панели."""
    with _connect() as conn:
        cur = conn.execute("SELECT phase, COUNT(*) FROM autopilot_state GROUP BY phase")
        return {row[0]: row[1] for row in cur.fetchall()}
