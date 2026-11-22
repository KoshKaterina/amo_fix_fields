"""Переданные ИИ сделки (решение Кати 12.11.2026, «дать сделку»).

Обычно ИИ входит в сделку по новому сообщению клиента (`ai_engine._apply_resolution`). Для сделок, где клиент уже написал, а
менеджер ещё не ответил (заказы с сайта tangemshop.ru на этапе «Новый лид»), менеджер передаёт сделку явно: `ai_engine.give_deal`
ставит `owner = ai` и служебное событие `deal_given`, ИИ перечитывает переписку и продолжает с того места, где остановились.

Здесь одна маленькая таблица: когда сделку отдали. От неё считается `manager_cutoff` в ходе: сообщения менеджера ДО передачи ИИ
не останавливают (иначе человек, написавший клиенту и потом отдавший сделку ИИ, получил бы молчащего ИИ по правилу «менеджер в
переписке»). Сообщения менеджера после передачи останавливают, как и везде.

`mode` - режим (`test`/`full`/...), в котором сделку отдали: сделка из другого режима не наследуется (см. `ai_engine_modes`), её отметка
`manager_cutoff` и `forced` для нового ведения не действуют. У строк, записанных до режимов, колонка заполняется один раз при её
добавлении: воронка «Тест» значит `test`, остальное `full` (как для `deal_owner`).

Таблица создаётся при первом обращении (`IF NOT EXISTS`), в той же sqlite, что у `ai_engine_store`.
"""
import datetime
import sqlite3

import ai_engine_config as cfg
import ai_engine_store as store

_SCHEMA = "CREATE TABLE IF NOT EXISTS deal_given (deal_id INTEGER PRIMARY KEY, given_at REAL NOT NULL, given_iso TEXT NOT NULL)"


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.execute(_SCHEMA)
        try:
            conn.execute("ALTER TABLE deal_given ADD COLUMN forced INTEGER NOT NULL DEFAULT 0")     # таблица из прежнего выката без колонки
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE deal_given ADD COLUMN mode TEXT")
        except sqlite3.OperationalError:
            pass                                                # колонка уже есть
        else:
            _backfill_mode(conn)                                # колонка только что появилась: режим у всех прежних строк пуст
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def _backfill_mode(conn) -> None:
    """Режим прежних строк по воронке сделки из `deal_owner` (передача всегда оставляет там строку). Воронки нет: `full`."""
    try:
        conn.execute(
            "UPDATE deal_given SET mode = CASE WHEN (SELECT pipeline_id FROM deal_owner WHERE deal_owner.deal_id = deal_given.deal_id) = ? "
            "THEN 'test' ELSE 'full' END WHERE mode IS NULL", (cfg.PIPELINE_TEST,))
    except sqlite3.OperationalError:                            # таблицы владельцев ещё нет (отдельный запуск без `store.init`)
        conn.execute("UPDATE deal_given SET mode = 'full' WHERE mode IS NULL")


def mark(deal_id: int, now: float, forced: bool = False, mode: str | None = None) -> str:
    """Отметить передачу (повторная передача обновляет время и режим). `forced`: человек отдал сделку вопреки правилам дублей (A, C, D),
    они для неё больше не работают. `mode` - активный режим на момент передачи. Возвращает ISO-время."""
    iso = _iso(now)
    with _Db() as conn:
        conn.execute("INSERT INTO deal_given (deal_id, given_at, given_iso, forced, mode) VALUES (?,?,?,?,?) "
                     "ON CONFLICT(deal_id) DO UPDATE SET given_at=excluded.given_at, given_iso=excluded.given_iso, forced=excluded.forced, "
                     "mode=COALESCE(excluded.mode, mode)",
                     (int(deal_id), float(now), iso, 1 if forced else 0, mode))
    return iso


def mode_of(deal_id: int) -> str | None:
    """Режим, в котором сделку отдали; None, если её не передавали."""
    with _Db() as conn:
        row = conn.execute("SELECT mode FROM deal_given WHERE deal_id=?", (int(deal_id),)).fetchone()
        return row["mode"] if row else None


def _other_mode(row, mode: str | None) -> bool:
    """Передача сделана в другом режиме, чем `mode` (его называет вызывающий): такая отметка к нынешнему ведению не относится.
    Режим не назван или в строке пуст: отметка действует, как до режимов."""
    return bool(mode and row["mode"] and row["mode"] != mode)


def is_forced(deal_id: int, mode: str | None = None) -> bool:
    with _Db() as conn:
        row = conn.execute("SELECT forced, mode FROM deal_given WHERE deal_id=?", (int(deal_id),)).fetchone()
        return bool(row and row["forced"] and not _other_mode(row, mode))


def cutoff_iso(deal_id: int, mode: str | None = None) -> str | None:
    """Когда сделку отдали ИИ; None, если её не передавали (ИИ вошёл сам по сообщению клиента) или передача сделана в другом режиме."""
    with _Db() as conn:
        row = conn.execute("SELECT given_iso, mode FROM deal_given WHERE deal_id=?", (int(deal_id),)).fetchone()
        return row["given_iso"] if row and not _other_mode(row, mode) else None
