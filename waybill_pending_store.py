"""SQLite-хранилище НЕЗАВЕРШЁННЫХ заказов СДЭК (см. waybill_service.py).

Зачем понадобилось (разбор 27.09.2026, сделки 36565053 и 36553383). СДЭК держал
заявки в state=ACCEPTED без cdek_number больше часа, и вылезли две дыры разом:

1. **Эхо тега рождало настоящий дубль отправления.** Когда фоновое дожидание
   сдавалось, `_fail` ставил тег «ошибка накладной» через patch_lead. amo на это
   изменение шлёт lead_change, а webhooks.py смотрит на ЗНАЧЕНИЕ status_id, а не
   на факт перехода — сделка всё ещё на «Сделать накладную», значит
   enqueue_waybill срабатывал снова. В логах секунда между отказом и повторной
   постановкой в очередь. Гейт «поле 571657 заполнено → не пересоздаём» тут
   бессилен: номера-то СДЭК и не дал, поле пустое.
2. **Фоновое дожидание не переживало пересборку контейнера.** `_resolve_pending_order`
   это обычная задача в памяти процесса: пересобрали amo-fix-fields — опрос умер
   молча, ни примечания в сделке, ни алерта. Именно так 27.09 второй UUID остался
   вообще без присмотра.

Обе дыры закрываются одним и тем же знанием «по этой сделке уже есть заказ СДЭК,
который ещё не получил номер». В памяти процесса его держать нельзя — оно нужно
ровно в тех случаях, когда процесс перезапустился.

Синхронный sqlite3 (без внешних зависимостей), вызывается из waybill_service
через asyncio.to_thread, чтобы не блокировать event loop — как reserve_store.
"""

import datetime
import os
import sqlite3
from contextlib import contextmanager

# /app/var примонтирован с хоста (docker-compose.yml) — иначе база легла бы внутрь
# контейнера и стиралась на каждой пересборке, то есть ровно то, ради чего
# хранилище заводится, не работало бы.
DB_PATH = os.getenv("WAYBILL_PENDING_DB_PATH", "/app/var/waybill_pending.sqlite3")

_UTC = datetime.timezone.utc

_COLS = "lead_id, order_uuid, source, created_at, last_checked_at, gave_up"


@contextmanager
def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _row(r) -> dict:
    return {
        "lead_id": r[0],
        "order_uuid": r[1],
        "source": r[2],
        "created_at": r[3],
        "last_checked_at": r[4],
        "gave_up": bool(r[5]),
    }


def _now() -> str:
    return datetime.datetime.now(_UTC).isoformat()


def init() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS waybill_pending (
                lead_id INTEGER PRIMARY KEY,
                order_uuid TEXT NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_checked_at TEXT NOT NULL,
                gave_up INTEGER NOT NULL DEFAULT 0
            )
            """
        )


def put(lead_id, order_uuid: str, source: str) -> None:
    """Запомнить заказ СДЭК, который ещё не получил номер.

    Ключ — сделка, а не uuid: по одной сделке одновременно НЕ должно быть двух
    незавершённых заказов, в этом весь смысл. Если строка уже есть, значит по
    сделке уже висит заказ, и вызывающий обязан был это проверить гейтом.
    """
    with _connect() as conn:
        conn.execute(
            f"INSERT OR REPLACE INTO waybill_pending ({_COLS}) VALUES (?, ?, ?, ?, ?, ?)",
            (int(lead_id), str(order_uuid), str(source), _now(), _now(), 0),
        )


def get(lead_id) -> dict | None:
    with _connect() as conn:
        r = conn.execute(
            f"SELECT {_COLS} FROM waybill_pending WHERE lead_id = ?", (int(lead_id),)
        ).fetchone()
    return _row(r) if r else None


def touch(lead_id, *, gave_up: bool | None = None) -> None:
    """Отметить, что заказ проверяли. gave_up=True — фоновое дожидание кончилось,
    но строку НЕ убираем: она продолжает держать гейт, чтобы эхо и /retry не
    создали второй заказ поверх живого."""
    with _connect() as conn:
        if gave_up is None:
            conn.execute(
                "UPDATE waybill_pending SET last_checked_at = ? WHERE lead_id = ?",
                (_now(), int(lead_id)),
            )
        else:
            conn.execute(
                "UPDATE waybill_pending SET last_checked_at = ?, gave_up = ? WHERE lead_id = ?",
                (_now(), 1 if gave_up else 0, int(lead_id)),
            )


def drop(lead_id) -> None:
    """Заказ довели до конца (номер записан) либо СДЭК его отклонил — сделка
    свободна, новый заказ по ней создавать снова можно."""
    with _connect() as conn:
        conn.execute("DELETE FROM waybill_pending WHERE lead_id = ?", (int(lead_id),))


def all_rows() -> list[dict]:
    """Все незавершённые заказы — для возобновления опроса на старте процесса."""
    with _connect() as conn:
        rows = conn.execute(f"SELECT {_COLS} FROM waybill_pending").fetchall()
    return [_row(r) for r in rows]
