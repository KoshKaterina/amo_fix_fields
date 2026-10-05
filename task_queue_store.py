"""SQLite-журнал ОЧЕРЕДИ ЗАДАЧ: дорожки переживают пересборку контейнера.

Зачем (разбор затора 05.10.2026). Дорожки `queue_manager` — это
`asyncio.PriorityQueue` в памяти процесса. Пересобрали контейнер — очередь
исчезла целиком. В тот день в ней стояло 547 задач, и единственным способом
понять, что мы потеряли, была реконструкция по логам (`ENQUEUE` минус
`DEQUEUE`). Это сработало, но так жить нельзя: катаем мы часто, а «какие задачи
умерли» не должно быть детективной работой.

Теперь каждая постановка в очередь пишет строку, снятие с очереди помечает её
`running`, а успешное завершение удаляет. На старте нерешённые строки
возвращаются в очередь.

⚠️ СЕМАНТИКА — «хотя бы один раз», и это главное, что надо понимать.

Строка со статусом `pending` (в очереди стояла, обработчик не начинался)
возвращается всегда: терять её нечем, обработчик к сделке ещё не прикасался.

Строка со статусом `running` (обработчик начался и не закончился — процесс
умер посередине) опаснее: часть внешних действий могла уже произойти. Поэтому
возврат решается ПО ТИПУ задачи, список `RESUME_RUNNING`:

- `waybill` — возвращаем. У накладных своё хранилище незавершённых заказов
  (`waybill_pending_store`) и `resume_pending_orders()` на старте: повторный
  заход увидит уже отданный в СДЭК заказ и второго не создаст.
- `lead_update`, `office_transfer`, `lead_distribution`, `cdek_sync`,
  `metrika_sync`, `jivo` — возвращаем. Все они дочитывают сделку заново и
  гасят себя сами, если делать нечего.
- `ozon_invoice` — **НЕ возвращаем.** `ext_id` платежа собирается как
  `amo-<lead>-<unixtime>`, то есть повторный заход создаст в Ozon ВТОРОЙ
  платёж, а не наткнётся на существующий. Если процесс умер между «платёж
  создан» и «ссылка записана в поле», автоматический повтор выдал бы клиенту
  две живые платёжки. Вместо этого зовём человека алертом.

Синхронный sqlite3 без внешних зависимостей: постановка в очередь
(`enqueue_*`) — синхронная функция, из неё `await` недоступен. WAL и
`synchronous=NORMAL`, чтобы запись стоила десятки микросекунд: на всплеске
05.10 было 932 вебхука в минуту, это ~15 записей в секунду.

Любая ошибка хранилища НЕ должна ломать очередь: задача всё равно ставится и
обрабатывается, просто без долговечности. Лучше работать без журнала, чем не
работать.
"""

import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager

# /app/var примонтирован с хоста — туда же пишут autopilot_store, reserve_store,
# waybill_pending_store и lead_status_store. Внутри контейнера база стёрлась бы
# на каждой пересборке, то есть ровно то, ради чего журнал заводится.
DB_PATH = os.getenv("TASK_QUEUE_DB_PATH", "/app/var/task_queue.sqlite3")

# Старше этого возвращать не будем: задача, провисевшая сутки, почти наверняка
# уже неактуальна (сделку доработали руками), а поднимать её — значит дёргать
# клиента по мёртвому поводу. Такие строки логируем и удаляем.
MAX_RESTORE_AGE_S = int(os.getenv("TASK_QUEUE_MAX_RESTORE_AGE_S", str(24 * 3600)))

# Какие типы задач безопасно возвращать, если их прервали ПОСРЕДИНЕ обработки.
# Обоснование по каждому — в докстринге модуля. Менять этот список, не прочитав
# его, нельзя: здесь разница между «повторили лишний раз» и «клиент получил
# вторую платёжку».
RESUME_RUNNING = frozenset({
    "lead_update", "office_transfer", "lead_distribution",
    "cdek_sync", "metrika_sync", "jivo", "waybill",
})

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"

logger = logging.getLogger("uvicorn")

_enabled = True


@contextmanager
def _conn():
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        yield conn
        conn.commit()
    finally:
        conn.close()


def init() -> None:
    """Создать таблицу. Зовётся на старте до восстановления очереди."""
    global _enabled
    try:
        with _conn() as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS tasks (
                       id          INTEGER PRIMARY KEY AUTOINCREMENT,
                       lane        TEXT NOT NULL,
                       priority    INTEGER NOT NULL,
                       sequence    INTEGER NOT NULL,
                       kind        TEXT NOT NULL,
                       lead_id     TEXT,
                       payload     TEXT NOT NULL,
                       enqueued_at REAL NOT NULL,
                       status      TEXT NOT NULL DEFAULT 'pending',
                       started_at  REAL
                   )"""
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_order "
                "ON tasks (lane, priority, sequence)"
            )
        _enabled = True
    except Exception:
        _enabled = False
        logger.exception(
            "task_queue_store: журнал очереди НЕ поднялся — работаем без долговечности, "
            "задачи будут теряться на пересборке"
        )


def is_enabled() -> bool:
    return _enabled


def add(lane: str, priority: int, sequence: int, kind: str, lead_id, payload: dict):
    """Записать поставленную задачу. Возвращает id строки или None."""
    if not _enabled:
        return None
    try:
        body = json.dumps(
            {k: v for k, v in payload.items() if k != "_row_id"},
            ensure_ascii=False, default=str,
        )
        with _conn() as conn:
            cur = conn.execute(
                "INSERT INTO tasks (lane, priority, sequence, kind, lead_id, payload, "
                "enqueued_at, status) VALUES (?,?,?,?,?,?,?,?)",
                (lane, int(priority), int(sequence), kind,
                 None if lead_id in (None, "") else str(lead_id),
                 body, time.time(), STATUS_PENDING),
            )
            return cur.lastrowid
    except Exception:
        logger.exception("task_queue_store: не записал задачу %s (сделка %s)", kind, lead_id)
        return None


def update_payload(row_id, payload: dict) -> None:
    """Коалесинг: по сделке пришёл свежий вебхук, payload в очереди обновили на
    месте — журнал должен увидеть то же самое, иначе после рестарта применится
    устаревшее состояние."""
    if not _enabled or row_id is None:
        return
    try:
        body = json.dumps(
            {k: v for k, v in payload.items() if k != "_row_id"},
            ensure_ascii=False, default=str,
        )
        with _conn() as conn:
            conn.execute("UPDATE tasks SET payload = ? WHERE id = ?", (body, int(row_id)))
    except Exception:
        logger.exception("task_queue_store: не обновил payload строки %s", row_id)


def mark_running(row_id) -> None:
    if not _enabled or row_id is None:
        return
    try:
        with _conn() as conn:
            conn.execute(
                "UPDATE tasks SET status = ?, started_at = ? WHERE id = ?",
                (STATUS_RUNNING, time.time(), int(row_id)),
            )
    except Exception:
        logger.exception("task_queue_store: не пометил строку %s как running", row_id)


def drop(row_id) -> None:
    """Задача доработана — строка больше не нужна."""
    if not _enabled or row_id is None:
        return
    try:
        with _conn() as conn:
            conn.execute("DELETE FROM tasks WHERE id = ?", (int(row_id),))
    except Exception:
        logger.exception("task_queue_store: не удалил строку %s", row_id)


def take_for_restore() -> dict:
    """Разобрать журнал на старте и ОЧИСТИТЬ его.

    Возвращает `{"resume": [...], "skipped_running": [...], "too_old": N}`:
    `resume` — строки к возврату в очередь, уже в правильном порядке;
    `skipped_running` — прерванные задачи, которые повторять нельзя (по ним
    зовём человека); `too_old` — сколько выкинули по возрасту.

    Журнал чистим целиком: всё, что надо вернуть, сейчас же будет поставлено
    заново и запишет свои новые строки. Иначе после двух рестартов подряд в
    таблице остались бы дубли.
    """
    if not _enabled:
        return {"resume": [], "skipped_running": [], "too_old": 0}
    try:
        now = time.time()
        with _conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM tasks ORDER BY lane, priority, sequence"
            ).fetchall()
            resume, skipped, too_old = [], [], 0
            for r in rows:
                age = now - float(r["enqueued_at"] or 0)
                if MAX_RESTORE_AGE_S > 0 and age > MAX_RESTORE_AGE_S:
                    too_old += 1
                    continue
                item = {
                    "lane": r["lane"], "priority": r["priority"], "kind": r["kind"],
                    "lead_id": r["lead_id"], "age_s": round(age, 1),
                    "status": r["status"],
                }
                try:
                    item["payload"] = json.loads(r["payload"])
                except Exception:
                    logger.warning("task_queue_store: строка %s с битым payload — пропускаю", r["id"])
                    continue
                if r["status"] == STATUS_RUNNING and r["kind"] not in RESUME_RUNNING:
                    skipped.append(item)
                else:
                    resume.append(item)
            conn.execute("DELETE FROM tasks")
        return {"resume": resume, "skipped_running": skipped, "too_old": too_old}
    except Exception:
        logger.exception("task_queue_store: разбор журнала не удался — восстанавливать нечего")
        return {"resume": [], "skipped_running": [], "too_old": 0}


def stats() -> dict:
    if not _enabled:
        return {"enabled": False}
    try:
        with _conn() as conn:
            total = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            running = conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE status = ?", (STATUS_RUNNING,)
            ).fetchone()[0]
        return {"enabled": True, "rows": total, "running": running}
    except Exception:
        return {"enabled": True, "error": True}
