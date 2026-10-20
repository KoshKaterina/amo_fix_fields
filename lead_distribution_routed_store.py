"""SQLite-хранилище «сделка уже распределена» для lead_distribution.

Зачем (разбор 20.10.2026). Защита от повторной раздачи жила списком `routed_ids`
внутри `var/lead_distribution_counters.json` — вместе с СУТОЧНЫМИ счётчиками
нагрузки. `_load_counters_state()` при смене даты отдаёт пустое состояние, и
список обнулялся в полночь МСК. Тег «распределено» новым сделкам с 25.08.2026 не
ставится, так что после полуночи сделку больше ничего не защищало: переночевала
на входном этапе профиля, утром кто-то закрыл задачу «Новый заказ с сайта» —
amo шлёт `update_lead`, сделка всё ещё на точке входа, и её раздают заново.
Живой случай: сделки 36582977 и 36582987 ушли от Егора Константинова к Кириллу
Полесскому. За 30 дней таких повторных раздач со сменой ответственного — 53.

Поэтому запись «распределена» отделена от счётчиков: строка на сделку, живёт
`ROUTED_TTL_S` (30 дней), переживает полночь и пересборку контейнера. Счётчики
нагрузки остаются в json и по-прежнему суточные.

Снимается запись в двух случаях: истёк срок (prune) и сделку вернули на точку
входа профиля с ДРУГОГО этапа (`forget`, зовёт webhooks.py по lead_status_store) —
это осознанный ручной возврат, такую сделку надо раздать заново.

Синхронный sqlite3 без внешних зависимостей, вызывается из lead_distribution
через `asyncio.to_thread` — тем же приёмом, что lead_status_store и reserve_store.
"""

import logging
import os
import sqlite3
import time
from contextlib import contextmanager

# /app/var примонтирован с хоста (docker-compose.yml: ./amo_fix_fields/var:/app/var).
# Внутри контейнера база стиралась бы на каждой пересборке — а пережить её и есть
# половина смысла хранилища.
DB_PATH = os.getenv("LEAD_DISTRIBUTION_ROUTED_DB_PATH", "/app/var/lead_distribution_routed.sqlite3")

# Сколько помним решение. Сделка, простоявшая на точке входа больше месяца, при
# следующем обновлении раздаётся заново — так решено 20.10.2026.
ROUTED_TTL_S = int(os.getenv("LEAD_DISTRIBUTION_ROUTED_TTL_S", str(30 * 86400)))

logger = logging.getLogger("uvicorn")


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        _ensure_table(conn)
        yield conn
        conn.commit()
    finally:
        conn.close()


def _ensure_table(conn) -> None:
    # Каждый вызов, а не один раз на старте: путь к базе в тестах меняется на
    # ходу, а CREATE IF NOT EXISTS по уже готовой таблице ничего не стоит.
    conn.execute(
        """CREATE TABLE IF NOT EXISTS lead_distribution_routed (
               lead_id          INTEGER PRIMARY KEY,
               routed_at        INTEGER NOT NULL,
               assigned_user_id INTEGER,
               profile_id       TEXT
           )"""
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS ix_ld_routed_at ON lead_distribution_routed (routed_at)"
    )


def init() -> None:
    """Создать таблицу. Зовётся из lead_distribution.init на старте."""
    with _conn():
        pass


def _cutoff(now: int | None = None) -> int:
    return int(now if now is not None else time.time()) - ROUTED_TTL_S


def is_routed(lead_id) -> bool:
    """Распределена ли сделка за последние `ROUTED_TTL_S`. Ошибка хранилища —
    исключение наружу: решать за вызывающего, раздавать или нет, тут нельзя."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT routed_at FROM lead_distribution_routed WHERE lead_id = ?", (int(lead_id),)
        ).fetchone()
    return row is not None and int(row[0]) >= _cutoff()


def mark_routed(lead_id, assigned_user_id=None, profile_id=None, routed_at: int | None = None) -> None:
    """Записать решение. Повторная запись по той же сделке обновляет строку."""
    ts = int(routed_at if routed_at is not None else time.time())
    with _conn() as conn:
        conn.execute(
            "INSERT INTO lead_distribution_routed (lead_id, routed_at, assigned_user_id, profile_id) "
            "VALUES (?, ?, ?, ?) ON CONFLICT(lead_id) DO UPDATE SET "
            "routed_at = excluded.routed_at, assigned_user_id = excluded.assigned_user_id, "
            "profile_id = excluded.profile_id",
            (int(lead_id), ts,
             int(assigned_user_id) if assigned_user_id is not None else None,
             str(profile_id) if profile_id is not None else None),
        )


def backfill(rows) -> int:
    """Одноразовое заполнение из журнала панели: [(lead_id, routed_at, user, profile)].
    Строку, которая уже есть и НОВЕЕ входной, не трогаем — живые записи главнее
    журнала. Возвращает число вставленных или обновлённых строк."""
    changed = 0
    with _conn() as conn:
        for lead_id, routed_at, user_id, profile_id in rows:
            cur = conn.execute(
                "INSERT INTO lead_distribution_routed (lead_id, routed_at, assigned_user_id, profile_id) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(lead_id) DO UPDATE SET "
                "routed_at = excluded.routed_at, assigned_user_id = excluded.assigned_user_id, "
                "profile_id = excluded.profile_id "
                "WHERE excluded.routed_at > lead_distribution_routed.routed_at",
                (int(lead_id), int(routed_at),
                 int(user_id) if user_id not in (None, "") else None,
                 str(profile_id) if profile_id not in (None, "") else None),
            )
            changed += cur.rowcount or 0
    return changed


def forget(lead_id) -> bool:
    """Снять запись — сделку вернули на точку входа, её надо раздать заново.
    Возвращает True, если запись была."""
    with _conn() as conn:
        cur = conn.execute("DELETE FROM lead_distribution_routed WHERE lead_id = ?", (int(lead_id),))
        return bool(cur.rowcount)


def get(lead_id) -> dict | None:
    """Строка по сделке как есть (с просроченной тоже). Для тестов и разбора."""
    with _conn() as conn:
        row = conn.execute(
            "SELECT lead_id, routed_at, assigned_user_id, profile_id "
            "FROM lead_distribution_routed WHERE lead_id = ?", (int(lead_id),)
        ).fetchone()
    if row is None:
        return None
    return {"lead_id": row[0], "routed_at": row[1], "assigned_user_id": row[2], "profile_id": row[3]}


def prune(now: int | None = None) -> int:
    """Удалить записи старше `ROUTED_TTL_S`. Возвращает число строк."""
    if ROUTED_TTL_S <= 0:
        return 0
    with _conn() as conn:
        cur = conn.execute(
            "DELETE FROM lead_distribution_routed WHERE routed_at < ?", (_cutoff(now),)
        )
        return cur.rowcount or 0
