"""SQLite-хранилище движка для ИИ-менеджера (см. `ai_engine.py`).

Здесь лежит всё, что должно пережить рестарт контейнера: владелец сделки с `epoch`, таймеры чатов,
накопленные сообщения клиентов, ходы и очередь их отправки. База своя (`ai_engine.sqlite3` в томе
`var`), с остальными базами движка не пересекается, а модуль не импортирует ничего тяжёлого: его
читает `autopilot.py`, и общая точка входа не должна тянуть за собой полконтура.

Таблицы:

  deal_owner    владелец сделки: `engine` | `ai` | `human`, `epoch` растёт на 1 при каждой смене
  chat_session  один чат = одна строка: чего ждём (пачку сообщений, ход, ничего) и когда
  inbox         сообщения клиентов, накопленные для хода; `seq` монотонен, по нему permit решает,
                писал ли клиент что-то после хода
  turn          ходы: полезная нагрузка, состояние отправки, итог
  event_sent    «служебное событие уже отправлено» (оплата приходит из вебхука сделки по много раз)

⚠️ Идентификаторы ходов начинаются не с единицы, а с текущих секунд эпохи. Сервис отсекает повтор
по `turn_id` (`200 duplicate`), и если базу движка потеряют, ходы с нуля совпали бы с уже виденными,
то есть новые ходы молча считались бы повторами.

⚠️ Путь к базе читается из `ai_engine_config.DB_PATH` на КАЖДОМ обращении, а не при импорте: тесты
подменяют его на временный файл, не влияя на соседей.
"""
import datetime
import json
import os
import sqlite3
import time
from contextlib import contextmanager

import ai_engine_config as cfg

OWNER_ENGINE = "engine"
OWNER_AI = "ai"
OWNER_HUMAN = "human"

# Состояния чата.
S_COLLECTING = "collecting"    # копим сообщения, в `next_at` - момент, когда пора решать
S_IN_TURN = "in_turn"          # ход поставлен в очередь или идёт у сервиса
S_AI = "ai"                    # ИИ владеет сделкой, ждём нового сообщения
S_HUMAN = "human"              # дело человека
S_RELEASED = "released"        # ИИ вернул сделку сценарию
S_SKIP = "skip"                # чат не наш (не в списке, нет сделки), смотрим снова после `skip_until`

# Состояния хода.
T_QUEUED = "queued"            # ждёт отправки (в том числе повтора)
T_BACKLOG = "backlog"          # паузы повторов кончились, ждёт, пока сервис оживёт
T_ACCEPTED = "accepted"        # сервис принял, ждём `result`
T_DONE = "done"
T_CANCELLED = "cancelled"

_ACTIVE_TURN_STATES = (T_QUEUED, T_BACKLOG, T_ACCEPTED)


def _now() -> float:
    return time.time()


@contextmanager
def _connect():
    conn = sqlite3.connect(cfg.DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init() -> None:
    os.makedirs(os.path.dirname(cfg.DB_PATH) or ".", exist_ok=True)
    with _connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS deal_owner (
                deal_id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                epoch INTEGER NOT NULL,
                since TEXT NOT NULL,
                chat_id TEXT,
                chat_type TEXT,
                channel_id TEXT,
                reason TEXT,
                chat_key TEXT,
                pipeline_id INTEGER,
                contact_ids TEXT NOT NULL DEFAULT '[]',
                mode TEXT,
                closed INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS ix_deal_owner_owner ON deal_owner (owner);
            CREATE INDEX IF NOT EXISTS ix_deal_owner_chat ON deal_owner (chat_key);

            CREATE TABLE IF NOT EXISTS chat_session (
                chat_key TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                chat_type TEXT NOT NULL DEFAULT '',
                phone TEXT NOT NULL DEFAULT '',
                contact_name TEXT NOT NULL DEFAULT '',
                state TEXT NOT NULL,
                deal_id INTEGER,
                batch_start REAL,
                first_batch INTEGER NOT NULL DEFAULT 1,
                last_client_at REAL,
                next_at REAL,
                resolve_deadline REAL,
                has_media INTEGER NOT NULL DEFAULT 0,
                pending_event TEXT,
                current_turn INTEGER,
                last_message_id TEXT NOT NULL DEFAULT '',
                skip_until REAL,
                fails INTEGER NOT NULL DEFAULT 0,
                note TEXT NOT NULL DEFAULT '',
                updated_at REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS ix_chat_session_due ON chat_session (state, next_at);
            CREATE INDEX IF NOT EXISTS ix_chat_session_deal ON chat_session (deal_id);

            CREATE TABLE IF NOT EXISTS inbox (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                chat_key TEXT NOT NULL,
                text TEXT NOT NULL DEFAULT '',
                msg_type TEXT NOT NULL DEFAULT 'text',
                sent_at TEXT NOT NULL DEFAULT '',
                received_at REAL NOT NULL,
                consumed_turn INTEGER
            );
            CREATE INDEX IF NOT EXISTS ix_inbox_chat ON inbox (chat_key, seq);

            CREATE TABLE IF NOT EXISTS turn (
                turn_id INTEGER PRIMARY KEY AUTOINCREMENT,
                deal_id INTEGER NOT NULL,
                epoch INTEGER NOT NULL,
                chat_key TEXT NOT NULL,
                reason TEXT NOT NULL,
                payload TEXT NOT NULL,
                up_to_seq INTEGER NOT NULL DEFAULT 0,
                state TEXT NOT NULL,
                send_attempts INTEGER NOT NULL DEFAULT 0,
                next_try_at REAL,
                created_at REAL NOT NULL,
                accepted_at REAL,
                finished_at REAL,
                outcome TEXT,
                outcome_reason TEXT
            );
            CREATE INDEX IF NOT EXISTS ix_turn_state ON turn (state, next_try_at);
            CREATE INDEX IF NOT EXISTS ix_turn_deal ON turn (deal_id);

            CREATE TABLE IF NOT EXISTS event_sent (
                deal_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                at REAL NOT NULL,
                PRIMARY KEY (deal_id, event_type)
            );

            -- алерт передачи диалога человеку: под ним менеджер реакцией возвращает диалог ИИ (ai_engine_return)
            CREATE TABLE IF NOT EXISTS alert_return (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                thread_id INTEGER,
                deal_id INTEGER NOT NULL,
                tags TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at REAL NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            );
            """
        )
        # Семя номеров ходов (см. шапку): только на пустой таблице, иначе сдвинули бы живые номера.
        empty = conn.execute("SELECT COUNT(*) FROM turn").fetchone()[0] == 0
        if empty:
            seeded = conn.execute("SELECT COUNT(*) FROM sqlite_sequence WHERE name='turn'").fetchone()[0]
            if not seeded:
                conn.execute("INSERT INTO sqlite_sequence (name, seq) VALUES ('turn', ?)", (int(_now()),))


def _dict(row) -> dict | None:
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------------------------
# алерты, которые можно закрыть реакцией (возврат диалога ИИ)
# ---------------------------------------------------------------------------------------------

ALERT_WAITING, ALERT_BUSY, ALERT_RETURNED, ALERT_CLOSED = "waiting", "busy", "returned", "closed"


def put_alert_return(chat_id: int, message_id: int, thread_id: int | None, deal_id: int, tags: str) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO alert_return (chat_id, message_id, thread_id, deal_id, tags, state, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (chat_id, message_id, thread_id, deal_id, tags, ALERT_WAITING, _now()),
        )


def get_alert_return(chat_id: int, message_id: int) -> dict | None:
    with _connect() as conn:
        return _dict(conn.execute(
            "SELECT * FROM alert_return WHERE chat_id = ? AND message_id = ?", (chat_id, message_id)).fetchone())


def claim_alert_return(chat_id: int, message_id: int) -> bool:
    """waiting -> busy одним шагом: две реакции подряд вернут диалог один раз."""
    with _connect() as conn:
        cur = conn.execute(
            "UPDATE alert_return SET state = ? WHERE chat_id = ? AND message_id = ? AND state = ?",
            (ALERT_BUSY, chat_id, message_id, ALERT_WAITING))
        return cur.rowcount == 1


def set_alert_return_state(chat_id: int, message_id: int, state: str) -> None:
    with _connect() as conn:
        conn.execute("UPDATE alert_return SET state = ? WHERE chat_id = ? AND message_id = ?", (state, chat_id, message_id))


def close_alerts_of_deal(deal_id: int) -> None:
    """Диалог вернулся: остальные ждущие алерты этой сделки больше не действуют."""
    with _connect() as conn:
        conn.execute("UPDATE alert_return SET state = ? WHERE deal_id = ? AND state = ?",
                     (ALERT_CLOSED, deal_id, ALERT_WAITING))


def purge_alert_return(older_than_s: float) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM alert_return WHERE created_at < ?", (_now() - older_than_s,))


# ---------------------------------------------------------------------------------------------
# владелец сделки
# ---------------------------------------------------------------------------------------------

def get_owner(deal_id: int) -> dict | None:
    with _connect() as conn:
        return _dict(conn.execute("SELECT * FROM deal_owner WHERE deal_id=?", (int(deal_id),)).fetchone())


def is_ai_owned(deal_id: int) -> bool:
    """Читает `autopilot.py`: сделкой владеет ИИ, значит боты авто-режима её не трогают."""
    row = get_owner(deal_id)
    return bool(row and row["owner"] == OWNER_AI and not row["closed"])


def list_owned(owner: str, include_closed: bool = False) -> list[dict]:
    with _connect() as conn:
        sql = "SELECT * FROM deal_owner WHERE owner=?"
        if not include_closed:
            sql += " AND closed=0"
        return [dict(r) for r in conn.execute(sql, (owner,)).fetchall()]


def set_owner(
    deal_id: int, owner: str, *, reason: str = "", chat: dict | None = None,
    pipeline_id: int | None = None, contact_ids: list[int] | None = None, mode: str | None = None,
    closed: bool | None = None,
) -> dict:
    """Сменить владельца. Эпоха растёт на 1 при КАЖДОМ вызове с новым владельцем; записи для
    сделки ещё нет, она рождается с эпохой 1 (переход из «engine, 0»)."""
    chat = chat or {}
    now_iso = _iso(_now())
    with _connect() as conn:
        cur = conn.execute("SELECT * FROM deal_owner WHERE deal_id=?", (int(deal_id),)).fetchone()
        if cur is None:
            conn.execute(
                "INSERT INTO deal_owner (deal_id, owner, epoch, since, chat_id, chat_type, channel_id, reason, "
                "chat_key, pipeline_id, contact_ids, mode, closed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (int(deal_id), owner, 1, now_iso, chat.get("chat_id"), chat.get("chat_type"),
                 chat.get("channel_id"), reason, chat.get("chat_key"), pipeline_id,
                 json.dumps(contact_ids or []), mode, 1 if closed else 0),
            )
        else:
            changed = cur["owner"] != owner
            epoch = cur["epoch"] + (1 if changed else 0)
            conn.execute(
                "UPDATE deal_owner SET owner=?, epoch=?, since=?, reason=?, "
                "chat_id=COALESCE(?, chat_id), chat_type=COALESCE(?, chat_type), "
                "channel_id=COALESCE(?, channel_id), chat_key=COALESCE(?, chat_key), "
                "pipeline_id=COALESCE(?, pipeline_id), contact_ids=COALESCE(?, contact_ids), "
                "mode=COALESCE(?, mode), closed=COALESCE(?, closed) WHERE deal_id=?",
                (owner, epoch, now_iso if changed else cur["since"], reason,
                 chat.get("chat_id"), chat.get("chat_type"), chat.get("channel_id"), chat.get("chat_key"),
                 pipeline_id, json.dumps(contact_ids) if contact_ids is not None else None, mode,
                 None if closed is None else (1 if closed else 0), int(deal_id)),
            )
        return dict(conn.execute("SELECT * FROM deal_owner WHERE deal_id=?", (int(deal_id),)).fetchone())


def mark_closed(deal_id: int) -> None:
    with _connect() as conn:
        conn.execute("UPDATE deal_owner SET closed=1 WHERE deal_id=?", (int(deal_id),))


# ---------------------------------------------------------------------------------------------
# чаты
# ---------------------------------------------------------------------------------------------

def get_session(chat_key: str) -> dict | None:
    with _connect() as conn:
        return _dict(conn.execute("SELECT * FROM chat_session WHERE chat_key=?", (chat_key,)).fetchone())


def session_for_deal(deal_id: int) -> dict | None:
    with _connect() as conn:
        return _dict(conn.execute(
            "SELECT * FROM chat_session WHERE deal_id=? ORDER BY updated_at DESC LIMIT 1", (int(deal_id),),
        ).fetchone())


_SESSION_COLS = (
    "channel_id", "chat_id", "chat_type", "phone", "contact_name", "state", "deal_id", "batch_start",
    "first_batch", "last_client_at", "next_at", "resolve_deadline", "has_media", "pending_event",
    "current_turn", "last_message_id", "skip_until", "fails", "note",
)


def put_session(chat_key: str, **fields) -> dict:
    """Создать или обновить строку чата. Незнакомое поле - ошибка, а не молчаливая запись."""
    bad = set(fields) - set(_SESSION_COLS)
    if bad:
        raise KeyError(f"нет такого поля чата: {sorted(bad)}")
    with _connect() as conn:
        cur = conn.execute("SELECT * FROM chat_session WHERE chat_key=?", (chat_key,)).fetchone()
        if cur is None:
            row = {c: None for c in _SESSION_COLS}
            row.update({"chat_type": "", "phone": "", "contact_name": "", "first_batch": 1, "has_media": 0,
                        "last_message_id": "", "fails": 0, "note": ""})
            row.update(fields)
            cols = ["chat_key", *_SESSION_COLS, "updated_at"]
            conn.execute(
                f"INSERT INTO chat_session ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                [chat_key, *[row[c] for c in _SESSION_COLS], _now()],
            )
        elif fields:
            sets = ", ".join(f"{k}=?" for k in fields)
            conn.execute(
                f"UPDATE chat_session SET {sets}, updated_at=? WHERE chat_key=?",
                [*fields.values(), _now(), chat_key],
            )
        return dict(conn.execute("SELECT * FROM chat_session WHERE chat_key=?", (chat_key,)).fetchone())


def discard_pending(chat_key: str) -> int:
    """Выбросить ещё не использованные сообщения чата (диалог ушёл человеку или чат не наш)."""
    with _connect() as conn:
        return conn.execute("DELETE FROM inbox WHERE chat_key=? AND consumed_turn IS NULL", (chat_key,)).rowcount


def chat_is_ai_owned(chat_key: str) -> bool:
    """Чатом сейчас владеет ИИ (его сделка не закрыта). Читает сторож розничных лидов."""
    with _connect() as conn:
        return conn.execute(
            "SELECT 1 FROM deal_owner WHERE chat_key=? AND owner=? AND closed=0 LIMIT 1",
            (chat_key, OWNER_AI),
        ).fetchone() is not None


def drop_session(chat_key: str, *, keep_inbox: bool = False) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM chat_session WHERE chat_key=?", (chat_key,))
        if not keep_inbox:
            conn.execute("DELETE FROM inbox WHERE chat_key=? AND consumed_turn IS NULL", (chat_key,))


def due_sessions(now: float) -> list[dict]:
    with _connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM chat_session WHERE state=? AND next_at IS NOT NULL AND next_at<=? ORDER BY next_at",
            (S_COLLECTING, now),
        ).fetchall()]


def sessions_by_state(state: str) -> list[dict]:
    with _connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM chat_session WHERE state=?", (state,)).fetchall()]


# ---------------------------------------------------------------------------------------------
# входящие сообщения клиентов
# ---------------------------------------------------------------------------------------------

def add_inbox(message_id: str, chat_key: str, text: str, msg_type: str, sent_at: str, received_at: float) -> int | None:
    """Положить сообщение. Повтор по `messageId` (вебхук ретраится) возвращает None."""
    with _connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO inbox (message_id, chat_key, text, msg_type, sent_at, received_at) "
            "VALUES (?,?,?,?,?,?)",
            (message_id, chat_key, text, msg_type, sent_at, received_at),
        )
        return cur.lastrowid if cur.rowcount else None


def pending_inbox(chat_key: str) -> list[dict]:
    with _connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM inbox WHERE chat_key=? AND consumed_turn IS NULL ORDER BY seq", (chat_key,),
        ).fetchall()]


def max_seq(chat_key: str) -> int:
    with _connect() as conn:
        return conn.execute("SELECT COALESCE(MAX(seq),0) FROM inbox WHERE chat_key=?", (chat_key,)).fetchone()[0]


def has_newer_than(chat_key: str, seq: int) -> bool:
    with _connect() as conn:
        return conn.execute(
            "SELECT 1 FROM inbox WHERE chat_key=? AND seq>? LIMIT 1", (chat_key, int(seq)),
        ).fetchone() is not None


def seq_of(message_id: str) -> int | None:
    with _connect() as conn:
        row = conn.execute("SELECT seq FROM inbox WHERE message_id=?", (message_id,)).fetchone()
        return row[0] if row else None


# ---------------------------------------------------------------------------------------------
# ходы
# ---------------------------------------------------------------------------------------------

def create_turn(
    *, deal_id: int, epoch: int, chat_key: str, reason: str, payload_fn, message_seqs: list[int], up_to_seq: int,
) -> dict:
    """Создать ход и в той же транзакции пометить сообщения использованными.

    `payload_fn(turn_id) -> dict` строит тело запроса, когда номер хода уже известен (он нужен
    внутри тела). Всё в одной транзакции: упасть между «ход записан» и «сообщения помечены» нельзя,
    иначе после рестарта клиентское сообщение ушло бы в два хода.
    """
    with _connect() as conn:
        cur = conn.execute(
            "INSERT INTO turn (deal_id, epoch, chat_key, reason, payload, up_to_seq, state, next_try_at, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (int(deal_id), int(epoch), chat_key, reason, "{}", int(up_to_seq), T_QUEUED, _now(), _now()),
        )
        turn_id = cur.lastrowid
        payload = payload_fn(turn_id)
        conn.execute("UPDATE turn SET payload=? WHERE turn_id=?", (json.dumps(payload, ensure_ascii=False), turn_id))
        if message_seqs:
            conn.executemany(
                "UPDATE inbox SET consumed_turn=? WHERE seq=?", [(turn_id, s) for s in message_seqs],
            )
        return dict(conn.execute("SELECT * FROM turn WHERE turn_id=?", (turn_id,)).fetchone())


def get_turn(turn_id: int) -> dict | None:
    with _connect() as conn:
        return _dict(conn.execute("SELECT * FROM turn WHERE turn_id=?", (int(turn_id),)).fetchone())


def turn_count(deal_id: int) -> int:
    with _connect() as conn:
        return conn.execute("SELECT COUNT(*) FROM turn WHERE deal_id=?", (int(deal_id),)).fetchone()[0]


def active_turn_for_deal(deal_id: int) -> dict | None:
    with _connect() as conn:
        marks = ",".join("?" * len(_ACTIVE_TURN_STATES))
        return _dict(conn.execute(
            f"SELECT * FROM turn WHERE deal_id=? AND state IN ({marks}) ORDER BY turn_id DESC LIMIT 1",
            (int(deal_id), *_ACTIVE_TURN_STATES),
        ).fetchone())


def turns_to_send(now: float) -> list[dict]:
    with _connect() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM turn WHERE state=? AND next_try_at IS NOT NULL AND next_try_at<=? ORDER BY turn_id",
            (T_QUEUED, now),
        ).fetchall()]


def turns_in_state(state: str) -> list[dict]:
    with _connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM turn WHERE state=? ORDER BY turn_id", (state,)).fetchall()]


def update_turn(turn_id: int, **fields) -> None:
    if not fields:
        return
    allowed = {"state", "send_attempts", "next_try_at", "accepted_at", "finished_at", "outcome", "outcome_reason"}
    bad = set(fields) - allowed
    if bad:
        raise KeyError(f"нет такого поля хода: {sorted(bad)}")
    sets = ", ".join(f"{k}=?" for k in fields)
    with _connect() as conn:
        conn.execute(f"UPDATE turn SET {sets} WHERE turn_id=?", [*fields.values(), int(turn_id)])


def unconsume(turn_id: int) -> int:
    """Вернуть сообщения хода в очередь (ход сорвался, а клиенту ответа ещё нет)."""
    with _connect() as conn:
        return conn.execute("UPDATE inbox SET consumed_turn=NULL WHERE consumed_turn=?", (int(turn_id),)).rowcount


def cancel_active_turns(deal_id: int) -> list[dict]:
    """Снять все живые ходы сделки. Возвращает снятые (чтобы послать сервису отмену)."""
    with _connect() as conn:
        marks = ",".join("?" * len(_ACTIVE_TURN_STATES))
        rows = [dict(r) for r in conn.execute(
            f"SELECT * FROM turn WHERE deal_id=? AND state IN ({marks})", (int(deal_id), *_ACTIVE_TURN_STATES),
        ).fetchall()]
        for r in rows:
            conn.execute(
                "UPDATE turn SET state=?, finished_at=?, outcome='cancelled' WHERE turn_id=?",
                (T_CANCELLED, _now(), r["turn_id"]),
            )
        return rows


# ---------------------------------------------------------------------------------------------
# служебные события, уборка
# ---------------------------------------------------------------------------------------------

def claim_event(deal_id: int, event_type: str) -> bool:
    """True - событие по сделке отправляется впервые."""
    with _connect() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO event_sent (deal_id, event_type, at) VALUES (?,?,?)",
            (int(deal_id), event_type, _now()),
        )
        return cur.rowcount == 1


def release_event(deal_id: int, event_type: str) -> None:
    with _connect() as conn:
        conn.execute("DELETE FROM event_sent WHERE deal_id=? AND event_type=?", (int(deal_id), event_type))


def prune(older_than_s: float) -> int:
    """Убрать старое: закрытые ходы, потреблённые сообщения, истёкшие пропуски."""
    edge = _now() - older_than_s
    with _connect() as conn:
        n = conn.execute(
            "DELETE FROM turn WHERE state IN (?,?) AND COALESCE(finished_at, created_at)<?",
            (T_DONE, T_CANCELLED, edge),
        ).rowcount
        n += conn.execute("DELETE FROM inbox WHERE consumed_turn IS NOT NULL AND received_at<?", (edge,)).rowcount
        n += conn.execute(
            "DELETE FROM chat_session WHERE state IN (?,?,?,?) AND updated_at<?",
            (S_SKIP, S_RELEASED, S_HUMAN, S_AI, edge),
        ).rowcount
        return n


def counts() -> dict:
    """Срез для `GET /` и ручки состояния: без текстов клиентов."""
    with _connect() as conn:
        out = {"owners": {}, "turns": {}, "sessions": {}}
        for r in conn.execute("SELECT owner, COUNT(*) FROM deal_owner WHERE closed=0 GROUP BY owner"):
            out["owners"][r[0]] = r[1]
        for r in conn.execute("SELECT state, COUNT(*) FROM turn GROUP BY state"):
            out["turns"][r[0]] = r[1]
        for r in conn.execute("SELECT state, COUNT(*) FROM chat_session GROUP BY state"):
            out["sessions"][r[0]] = r[1]
        return out


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
