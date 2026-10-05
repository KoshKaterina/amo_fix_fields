"""SQLite-хранилище ПОСЛЕДНЕГО ВИДЕННОГО этапа сделки.

Зачем понадобилось (разбор затора очереди 05.10.2026). Наша подписка вебхуков в
amo (id 48211134, `team.sunscrypt.ru/amo/lead_change`) собрана на события
`add_lead` и `update_lead` — **без `status_lead`**. Значит amo не присылает нам
отдельного события «сменился этап», а в теле `leads[update][0]` лежит только
НОВЫЙ `status_id` и нет прошлого. Отличить «сделка только что закрылась» от
«старую закрытую сделку кто-то тронул» по одному вебхуку невозможно в принципе.

Чем это обошлось. `webhooks.py` ставил задачу `office_transfer` по ЗНАЧЕНИЮ
статуса (`status_id in {142, 143}`), то есть на любое обновление давно закрытой
сделки. 05.10 всплеск вебхуков по ~650 старым сделкам создал **208 задач за
минуту**, тогда как реальных переходов в 143 по журналу событий amo за тот же
период было **11**. Данные не портились — `process_office_transfer` дочитывает
сделку и гасит такие гейтом `OFFICE_TRANSFER_SINCE_TS`, — но каждая ложная
задача стоила одного `get_lead_full` и занимала единственный воркер дорожки
`amo`, вытесняя клиентские задачи: платёжные ссылки СБП и распределение лидов.
Дорожка встала на часы.

Тот же класс баги уже ловили на накладных СДЭК — см. докстринг
`waybill_pending_store.py` (27.09.2026): «webhooks.py смотрит на ЗНАЧЕНИЕ
status_id, а не на факт перехода».

Почему на диске, а не окном в памяти. Ровно по причине из `autopilot_store.py`:
окна в памяти не хватает на пересборку контейнера, а катаем мы часто. Сразу
после рестарта хранилище пустое, и первый вебхук по каждой сделке считается
переходом — это осознанно консервативно: лучше лишняя задача, которую погасит
гейт, чем пропущенный настоящий перенос в Офис.

Синхронный sqlite3 без внешних зависимостей, вызывается из `webhooks.py` через
`asyncio.to_thread` — тем же приёмом, что `autopilot_store` и `reserve_store`.
"""

import logging
import os
import sqlite3
import time
from contextlib import contextmanager

# /app/var примонтирован с хоста — туда же пишут autopilot_store, reserve_store и
# waybill_pending_store. Без этого база легла бы внутрь контейнера и стиралась на
# каждой пересборке, то есть ровно то, ради чего хранилище заводится, не работало бы.
DB_PATH = os.getenv("LEAD_STATUS_DB_PATH", "/app/var/lead_status.sqlite3")

# Сколько держим строку по сделке, которую давно никто не трогал. Строка нужна
# только чтобы узнать прошлый этап; по сделке, молчащей три месяца, следующее
# обновление спокойно считается переходом (его погасит гейт по closed_at).
PRUNE_OLDER_THAN_S = int(os.getenv("LEAD_STATUS_PRUNE_OLDER_THAN_S", str(90 * 86400)))

logger = logging.getLogger("uvicorn")

_initialized = False


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init() -> None:
    """Создать таблицу. Зовётся один раз на старте (lifespan в webhooks.py)."""
    global _initialized
    with _conn() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS lead_status (
                   lead_id   INTEGER PRIMARY KEY,
                   status_id INTEGER NOT NULL,
                   seen_at   INTEGER NOT NULL
               )"""
        )
    _initialized = True


def prune() -> int:
    """Убрать строки по сделкам, которых давно не видели. Возвращает число строк."""
    if PRUNE_OLDER_THAN_S <= 0:
        return 0
    cutoff = int(time.time()) - PRUNE_OLDER_THAN_S
    with _conn() as conn:
        cur = conn.execute("DELETE FROM lead_status WHERE seen_at < ?", (cutoff,))
        return cur.rowcount or 0


def note_and_changed(lead_id, status_id) -> bool:
    """Запомнить этап сделки и сказать, СМЕНИЛСЯ ли он с прошлого раза.

    `True`  — этап другой, чем в прошлый раз, ИЛИ сделку видим впервые;
    `False` — этап тот же самый, то есть этот вебхук про что-то другое
              (поле, тег, примечание, ответственный), а не про переход.

    Первое появление сделки намеренно считается переходом: после рестарта база
    пустая, и пропустить настоящий перенос дороже, чем поставить лишнюю задачу.

    Любая ошибка хранилища — тоже `True`: гейт обязан пропускать при сомнении,
    иначе сломанная база молча остановит переносы в Офис.
    """
    try:
        lid = int(lead_id)
        sid = int(status_id)
    except (TypeError, ValueError):
        return True
    now = int(time.time())
    try:
        with _conn() as conn:
            row = conn.execute(
                "SELECT status_id FROM lead_status WHERE lead_id = ?", (lid,)
            ).fetchone()
            conn.execute(
                "INSERT INTO lead_status (lead_id, status_id, seen_at) VALUES (?, ?, ?) "
                "ON CONFLICT(lead_id) DO UPDATE SET status_id = excluded.status_id, "
                "seen_at = excluded.seen_at",
                (lid, sid, now),
            )
    except Exception:
        logger.exception("lead_status_store: сделка %s — ошибка хранилища, считаем переходом", lid)
        return True
    if row is None:
        return True
    return int(row[0]) != sid


def last_seen(lead_id):
    """Прошлый известный этап сделки или None. Для тестов и разбора."""
    try:
        with _conn() as conn:
            row = conn.execute(
                "SELECT status_id FROM lead_status WHERE lead_id = ?", (int(lead_id),)
            ).fetchone()
    except Exception:
        return None
    return int(row[0]) if row else None
