"""Отложенные касания ИИ-менеджера (решение Кати 02.11.2026).

Клиент САМ просит написать ему в названное время («напишите завтра в 12:00»). ИИ-сервис регистрирует касание у движка
(`/internal/ai/v1/followup`), движок держит таймер. В срок он ставит ИИ служебное событие `followup_due` с причиной и черновиком,
а текст ИИ пишет заново по свежим данным (цена, остаток, этап сделки) и отправляет обычным путём через `permit`.

Отмены (касание не уходит, статус `cancelled`):
  • клиент написал раньше (`client_wrote`) или менеджер написал в чат (`manager_wrote`): ИИ в следующем ходе получит пометку
    `followup_cancelled`, чтобы новое сообщение соответствовало контексту («твоё касание не ушло, клиент написал раньше»);
  • диалог ушёл человеку, сделка закрыта или сменился владелец (`handed_to_human`, `deal_closed`, `owner_changed`);
  • на сроке режим выключен, воронка или контакт вне режима, сервис ИИ недоступен слишком долго.
Срок за пределами окна отправки не отменяется: касание ждёт ближайшего окна (его открывает панель, часы МСК).
Задачу менеджеру касание не ставит. Если оно не вышло, в сделку пишется примечание и уходит алерт в чат ОП.

Всё состояние в sqlite движка (та же база, что у `ai_engine_store`); таблица создаётся при первом обращении.
"""
import datetime
from typing import Any

import ai_engine_store as store

MSK = datetime.timezone(datetime.timedelta(hours=3))
MIN_AHEAD_S = 10 * 60                 # раньше десяти минут не откладываем: это уже обычный ответ
MAX_AHEAD_S = 7 * 86400 + 3600        # до семи суток (час запаса на расхождение часов)
AI_DOWN_GIVE_UP_S = 30 * 60           # сервис ИИ недоступен дольше получаса после срока: касание отменяется
MAX_TEXT = 400

REASON_TEXT = {
    "client_wrote": "клиент написал раньше",
    "manager_wrote": "менеджер написал в чат",
    "handed_to_human": "диалог передан менеджеру",
    "deal_closed": "сделка закрыта",
    "owner_changed": "сменился владелец диалога",
    "released": "ИИ вернул сделку движку",
    "mode_off": "режим ИИ выключен",
    "pipeline_not_allowed": "сделка вне воронок режима",
    "contact_not_allowed": "контакт вне списка режима",
    "ai_down": "ИИ-сервис был недоступен",
    "replaced": "заменено новым касанием",
}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS followup (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    deal_id INTEGER NOT NULL,
    chat_key TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    turn_id INTEGER,
    due_at REAL NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    draft TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    cancel_reason TEXT,
    notify INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    closed_at REAL,
    first_due_seen REAL
);
CREATE INDEX IF NOT EXISTS ix_followup_status_due ON followup(status, due_at);
CREATE INDEX IF NOT EXISTS ix_followup_chat ON followup(chat_key, status);
CREATE INDEX IF NOT EXISTS ix_followup_deal ON followup(deal_id, status);
"""


class _Db:
    """Соединение со схемой: `with _Db() as conn`. Схема создаётся на каждом входе (`IF NOT EXISTS` дёшево)."""

    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def fmt_due(ts: float) -> str:
    return datetime.datetime.fromtimestamp(float(ts), MSK).strftime("%d.%m %H:%M")


def _row(r) -> dict[str, Any]:
    return dict(r)


def schedule(deal_id: int, chat_key: str, epoch: int, turn_id: int | None, due_at: float, reason: str, draft: str, now: float) -> dict:
    """Новое касание. Прежнее ждущее по этой сделке заменяется (одно касание на сделку)."""
    with _Db() as conn:
        conn.execute("UPDATE followup SET status='cancelled', cancel_reason='replaced', closed_at=? "
                     "WHERE deal_id=? AND status='pending'", (now, int(deal_id)))
        cur = conn.execute(
            "INSERT INTO followup (deal_id, chat_key, epoch, turn_id, due_at, reason, draft, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (int(deal_id), chat_key, int(epoch), turn_id, float(due_at), (reason or "")[:MAX_TEXT], (draft or "")[:MAX_TEXT], now))
        return _row(conn.execute("SELECT * FROM followup WHERE id=?", (cur.lastrowid,)).fetchone())


def pending_for_deal(deal_id: int) -> dict | None:
    with _Db() as conn:
        r = conn.execute("SELECT * FROM followup WHERE deal_id=? AND status='pending' ORDER BY id DESC LIMIT 1", (int(deal_id),)).fetchone()
        return _row(r) if r else None


def due(now: float) -> list[dict]:
    with _Db() as conn:
        return [_row(r) for r in conn.execute(
            "SELECT * FROM followup WHERE status='pending' AND due_at<=? ORDER BY due_at, id", (float(now),)).fetchall()]


def _cancel(conn, where: str, args: tuple, reason: str, now: float, notify: bool) -> list[dict]:
    rows = [_row(r) for r in conn.execute(f"SELECT * FROM followup WHERE status='pending' AND {where}", args).fetchall()]
    for r in rows:
        conn.execute("UPDATE followup SET status='cancelled', cancel_reason=?, notify=?, closed_at=? WHERE id=?",
                     (reason, 1 if notify else 0, now, r["id"]))
    return rows


def cancel_for_chat(chat_key: str, reason: str, now: float, notify: bool = False) -> list[dict]:
    with _Db() as conn:
        return _cancel(conn, "chat_key=?", (chat_key,), reason, now, notify)


def cancel_for_deal(deal_id: int, reason: str, now: float, notify: bool = False) -> list[dict]:
    with _Db() as conn:
        return _cancel(conn, "deal_id=?", (int(deal_id),), reason, now, notify)


def cancel_one(followup_id: int, reason: str, now: float, notify: bool = False) -> None:
    with _Db() as conn:
        conn.execute("UPDATE followup SET status='cancelled', cancel_reason=?, notify=?, closed_at=? WHERE id=? AND status='pending'",
                     (reason, 1 if notify else 0, now, int(followup_id)))


def mark_fired(followup_id: int, now: float) -> None:
    with _Db() as conn:
        conn.execute("UPDATE followup SET status='fired', closed_at=? WHERE id=? AND status='pending'", (now, int(followup_id)))


def note_first_due_seen(followup_id: int, now: float) -> float:
    """Когда касание впервые дозрело, но отправить его мешает ИИ-сервис (чтобы отменить после AI_DOWN_GIVE_UP_S)."""
    with _Db() as conn:
        r = conn.execute("SELECT first_due_seen FROM followup WHERE id=?", (int(followup_id),)).fetchone()
        if r and r["first_due_seen"]:
            return float(r["first_due_seen"])
        conn.execute("UPDATE followup SET first_due_seen=? WHERE id=?", (now, int(followup_id)))
        return now


def cancelled_notes(deal_id: int) -> list[dict]:
    """Отменённые касания, о которых ИИ ещё не знает (`notify=1`): уйдут в следующий ход как `followup_cancelled`."""
    with _Db() as conn:
        return [_row(r) for r in conn.execute(
            "SELECT * FROM followup WHERE deal_id=? AND status='cancelled' AND notify=1 ORDER BY id", (int(deal_id),)).fetchall()]


def clear_notify(ids: list[int]) -> None:
    if not ids:
        return
    with _Db() as conn:
        conn.executemany("UPDATE followup SET notify=0 WHERE id=?", [(int(i),) for i in ids])


def cancel_payload(rows: list[dict]) -> list[dict[str, Any]]:
    """Форма для хода (`followup_cancelled`): что планировалось и почему не ушло."""
    out = []
    for r in rows:
        out.append({"due_at": fmt_due(r["due_at"]), "reason": r["reason"], "draft": r["draft"],
                    "why": REASON_TEXT.get(r["cancel_reason"] or "", r["cancel_reason"] or "")})
    return out


def prune(now: float, keep_s: float) -> None:
    with _Db() as conn:
        conn.execute("DELETE FROM followup WHERE status<>'pending' AND closed_at IS NOT NULL AND closed_at<?", (now - keep_s,))
