"""Отметки людей по сделке из панели Team (решение Кати 23.11.2026): «не требует ответа» и «удалить».

Две отметки, ручка `POST /internal/ai/v1/deal_mark` (`ai_engine_api`, тело `{"deal_id", "kind", "by"}`), действия движка в `ai_engine.mark_deal`:

  • `no_reply` («не требует ответа»): ожидающий ход и черновик по текущему сообщению клиента снимаются, а сделка не попадает в подбор существующих
    (`ai_engine_sweep`), пока клиент не напишет НОВОЕ сообщение позже отметки. Владелец сделки не меняется (вела ИИ, ведёт ИИ). Обычная обработка нового
    сообщения клиента работает как всегда: отметка действует только на сообщения, написанные до неё (`suppresses`).
  • `removed` («удалить»): ИИ эту сделку больше не ведёт, навсегда. Если вела ИИ, она освобождается: владелец становится человеком (без алертов и без
    задач), ждущий ход и отложенное касание отменяются, открытое предложение смены ответственного снимается. Запись владельца `human` с причиной `removed`
    остаётся навсегда, поэтому и вход по сообщению клиента (`ai_engine._apply_resolution`), и подбор считают сделку человеческой.

Ручная передача «Дать сделку ИИ» (`ai_engine.give_deal`) снимает обе отметки (`clear`, строка в журнале).

Повтор ничего не ломает: тот же вид отметки обновляет время (`no_reply`) или не делает ничего (`removed`); `removed` не понижается до `no_reply`; `no_reply` повышается
до `removed`. Таблицы создаются при первом обращении (`IF NOT EXISTS`) в той же sqlite, что у `ai_engine_store`: `deal_mark` (одна строка на сделку) и
`deal_mark_log` (журнал: поставлена, повторена, снята). Личных данных клиента здесь нет, в лог идут только номер сделки и вид отметки.
"""
import time
from typing import Any

import ai_engine_store as store

KIND_NO_REPLY, KIND_REMOVED = "no_reply", "removed"
KINDS = (KIND_NO_REPLY, KIND_REMOVED)
BY_MAX = 80

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS deal_mark (deal_id INTEGER PRIMARY KEY, kind TEXT NOT NULL, at REAL NOT NULL, "
    "by_user TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '');"
    "CREATE TABLE IF NOT EXISTS deal_mark_log (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, at REAL NOT NULL, "
    "event TEXT NOT NULL, kind TEXT NOT NULL DEFAULT '', by_user TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '')"
)


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def clean_by(by: Any) -> str:
    return " ".join(str(by or "").split())[:BY_MAX]


def get(deal_id: int) -> dict[str, Any] | None:
    with _Db() as conn:
        row = conn.execute("SELECT * FROM deal_mark WHERE deal_id=?", (int(deal_id),)).fetchone()
    return dict(row) if row else None


def put(deal_id: int, kind: str, now: float, by: str = "", note: str = "") -> dict[str, Any]:
    """Поставить отметку. Возвращает `{"state": "marked" | "already_removed", "kind": <действующий вид>, "prev": <прежний вид или None>, "repeat": bool}`.
    `removed` окончательна: повторное `removed` и любое `no_reply` поверх неё ничего не меняют (`already_removed`, в журнал строка `repeat`)."""
    deal_id, at, by = int(deal_id), float(now), clean_by(by)
    with _Db() as conn:
        cur = conn.execute("SELECT kind FROM deal_mark WHERE deal_id=?", (deal_id,)).fetchone()
        prev = cur["kind"] if cur else None
        if prev == KIND_REMOVED:
            conn.execute("INSERT INTO deal_mark_log (deal_id, at, event, kind, by_user, note) VALUES (?,?,?,?,?,?)",
                         (deal_id, at, "repeat", kind, by, "removed уже стоит"))
            return {"state": "already_removed", "kind": KIND_REMOVED, "prev": prev, "repeat": True}
        conn.execute("INSERT INTO deal_mark (deal_id, kind, at, by_user, note) VALUES (?,?,?,?,?) "
                     "ON CONFLICT(deal_id) DO UPDATE SET kind=excluded.kind, at=excluded.at, by_user=excluded.by_user, note=excluded.note",
                     (deal_id, kind, at, by, str(note)[:300]))
        conn.execute("INSERT INTO deal_mark_log (deal_id, at, event, kind, by_user, note) VALUES (?,?,?,?,?,?)",
                     (deal_id, at, "repeat" if prev == kind else "set", kind, by, str(note)[:300]))
        return {"state": "marked", "kind": kind, "prev": prev, "repeat": prev == kind}


def clear(deal_id: int, why: str, now: float | None = None, by: str = "") -> list[str]:
    """Снять отметки со сделки (ручная передача ИИ). Возвращает снятые виды (пусто: отметок не было). В журнал идёт строка `cleared`."""
    at = float(now if now is not None else time.time())
    with _Db() as conn:
        row = conn.execute("SELECT kind FROM deal_mark WHERE deal_id=?", (int(deal_id),)).fetchone()
        if row is None:
            return []
        conn.execute("DELETE FROM deal_mark WHERE deal_id=?", (int(deal_id),))
        conn.execute("INSERT INTO deal_mark_log (deal_id, at, event, kind, by_user, note) VALUES (?,?,?,?,?,?)",
                     (int(deal_id), at, "cleared", row["kind"], clean_by(by), str(why)[:300]))
        return [row["kind"]]


def suppresses(deal_id: int, last_client_ts: float | None) -> str | None:
    """Причина (строка), по которой отметка не пускает ИИ в сделку по сообщению клиента, написанному в `last_client_ts`; None: отметка не мешает.
    `removed` мешает всегда. `no_reply` только сообщению, написанному не позже отметки: новое сообщение позже отметки обрабатывается как обычно."""
    mark = get(deal_id)
    if mark is None:
        return None
    if mark["kind"] == KIND_REMOVED:
        return "сделка удалена из ведения ИИ (отметка «удалить»)"
    if last_client_ts is None or float(last_client_ts) <= float(mark["at"]):
        return "отметка «не требует ответа» стоит позже последнего сообщения клиента"
    return None


def counts() -> dict[str, int]:
    out = {KIND_NO_REPLY: 0, KIND_REMOVED: 0}
    with _Db() as conn:
        for kind, n in conn.execute("SELECT kind, COUNT(*) FROM deal_mark GROUP BY kind"):
            out[kind] = n
    return out


def exclude_ids(now: float, no_reply_keep_s: float) -> tuple[list[int], list[int]]:
    """Сделки для `exclude` подбора: все с `removed` и те `no_reply`, что поставлены не раньше чем `no_reply_keep_s` назад (дальше отметка проверяется по
    времени последнего сообщения клиента из карточки кандидата, `suppresses`)."""
    with _Db() as conn:
        removed = [int(r[0]) for r in conn.execute("SELECT deal_id FROM deal_mark WHERE kind=? ORDER BY at DESC", (KIND_REMOVED,))]
        recent = [int(r[0]) for r in conn.execute("SELECT deal_id FROM deal_mark WHERE kind=? AND at>? ORDER BY at DESC",
                                                  (KIND_NO_REPLY, float(now) - no_reply_keep_s))]
    return removed, recent


def journal(limit: int = 50) -> list[dict[str, Any]]:
    """Последние строки журнала отметок, свежее первым: для проверки руками и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM deal_mark_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]
