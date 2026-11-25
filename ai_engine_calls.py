"""Звонок менеджера как вмешательство человека и примечание «ИИ-менеджер приостановил ведение сделки» (просьба Кати 25.11.2026).

Зачем. ИИ отпускал сделку только после СООБЩЕНИЯ менеджера клиенту (вебхук Wazzup), и молча. Позвонивший клиенту менеджер для движка не существовал: ИИ продолжал писать поверх разговора.

Что считается вмешательством (`intervention_kind`). Телефония (Sipuni и UIS) пишет примечания `call_in`/`call_out` в карточки КОНТАКТОВ amo (на сделках таких нет). `call_out` любой, в том числе без дозвона
(менеджер взялся за клиента). `call_in` только с состоявшимся разговором: `params.call_status == 4` (НЕ длительность: 2 = не взяли или бросил, 6 = недозвон). Пропущенный входящий не считается.

К какой сделке относится. `note.entity_id` входит в `deal_owner.contact_ids` записи `owner = ai`, не закрытой, и `created_at` звонка ПОЗЖЕ `deal_owner.since`: момента, когда ИИ взял или получил обратно сделку
(`since` меняется только при смене владельца, повторные ходы ИИ его не трогают). Звонки до взятия не считаются, иначе лид, созданный из звонка, ИИ не взял бы никогда. `contact_ids` пишут все пути взятия из самой
сделки; пуст он, если в тот момент у сделки не было контакта: тогда движок дочитывает его из сделки, когда появляется звонок (`_fill_contacts`).

Действие: тем же путём, что сообщение менеджера (`ai_engine._to_human(..., alert=None)`): владелец человек с причиной `manager_call`, ход, касания и лестница тишины сняты, без алерта и задачи.

Примечание о снятии (звонок И сообщение менеджера). `ai_engine._to_human` кладёт строку в `takeover_note`, только если ИИ действительно вёл сделку (`owner = ai`): повторный вебхук видит уже человека, второго
примечания нет. Отправка фоном (не под общим замком), сбой amo не ломает снятие: строка остаётся в sqlite, повтор с растущей паузой, не дольше суток. ИИ вернули раньше, чем amo приняла примечание: оно уже
неправда и не пишется. Для `ai_engine_pause` это не команда: команда это примечание ЧЕЛОВЕКА из одного слова, а это длинный текст от интеграции (автор 0).

Опрос по образцу `ai_engine_pause`: курсор по `updated_at` в sqlite с перекрытием `OVERLAP_S`, первый запуск начинает с «сейчас» (`watch_from`: звонки раньше него не исполняются), исполненные id в
`calls_note_seen`, фоновая задача из `tick_once` (не отдельный цикл), раз в `AI_CALLS_POLL_S`. Старые примечания amo иногда обновляет спустя годы, поэтому по времени фильтруем `created_at`.
Тем же запросом `ai_engine_mode_return` смотрит, звонили ли клиенту, пока ИИ был выключен (`had_call`).

Рубильник: `AI_CALLS_TAKEOVER=0` в .env движка и перезапуск контейнера: звонки не читаются и возврат после «Выключено» их не проверяет; примечания о снятии после сообщения менеджера остаются.

Таблицы создаются при первом обращении (`IF NOT EXISTS`) в той же sqlite, что у `ai_engine_store`. В логи и в базу идут номер сделки, id примечания и вид звонка: ни телефонов, ни имён, ни тел примечаний.
"""
import asyncio
import datetime
import json
import logging
from typing import Any

import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_modes as modes
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service

logger = logging.getLogger("uvicorn")

REASON = "manager_call"          # `deal_owner.reason`: снята звонком менеджера
NOTE_MESSAGE, NOTE_CALL_OUT, NOTE_CALL_IN = "message", "call_out", "call_in"
_NOTE = "ИИ-менеджер приостановил ведение сделки, причина: {why}. Чтобы вернуть ИИ, напишите в сделку примечание «старт»."
NOTE_TEXT = {                    # тексты согласованы с Катей, дословно
    NOTE_MESSAGE: _NOTE.format(why="живой менеджер отправил сообщение клиенту"),
    NOTE_CALL_OUT: _NOTE.format(why="живой менеджер совершил звонок"),
    NOTE_CALL_IN: _NOTE.format(why="живой менеджер ответил на звонок клиента"),
}
TALK_STATUS = "4"                # `params.call_status`: разговор состоялся

OVERLAP_S = 900                  # курсор отступает назад: примечание может появиться заметно позже времени, которым оно помечено; повторы отсекают id и `since`
PAGE_LIMIT, PAGES_MAX = 250, 8
AMO_TIMEOUT_S = 15
SEEN_KEEP_S = 3 * 86400
LOG_KEEP_S = 7 * 86400
HOLD_MAX_S = 3600                # не больше стольких секунд курсор ждёт сделку, контакты которой не удалось прочитать
FILL_PER_POLL = 10               # сколько сделок без контактов дочитывается за опрос
FILL_RETRY_S = 600               # у сделки в amo нет контактов: не спрашивать на каждом опросе
CHECK_SLACK_S = 60               # возврат после «Выключено»: звонок за секунды до снятия опрос мог ещё не увидеть
NOTES_CHECK_S = 10               # как часто искать неотправленные примечания о снятии
NOTE_RETRY_S, NOTE_RETRY_MAX_S, NOTE_GIVE_UP_S = 60, 3600, 86400

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS calls_kv (k TEXT PRIMARY KEY, v REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS calls_note_seen (note_id INTEGER PRIMARY KEY, at REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS calls_log (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, at REAL NOT NULL, kind TEXT NOT NULL, note_id INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS takeover_note (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, kind TEXT NOT NULL, at REAL NOT NULL, "
    "tries INTEGER NOT NULL DEFAULT 0, next_try_at REAL NOT NULL)"
)

_task: asyncio.Task | None = None
_next_run_at = 0.0
_next_notes_at = 0.0
_run_lock: asyncio.Lock | None = None
_note_lock: asyncio.Lock | None = None
_fill_wait: dict[int, float] = {}          # сделка без контактов в amo -> до какого времени не спрашивать снова
_state: dict[str, Any] = {"last_poll_at": None, "last_error": None}


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def enabled() -> bool:
    return bool(cfg.CALLS_TAKEOVER_ENABLED)


# =============================================================================================
# разбор
# =============================================================================================

def intervention_kind(note: dict[str, Any]) -> str | None:
    """`call_out` | `call_in` (отвеченный) | None. Решает вид примечания и `call_status`, длительность не смотрим."""
    kind = note.get("note_type")
    if kind == NOTE_CALL_OUT:
        return NOTE_CALL_OUT
    params = note.get("params") if isinstance(note.get("params"), dict) else {}
    if kind == NOTE_CALL_IN and str(params.get("call_status")) == TALK_STATUS:
        return NOTE_CALL_IN
    return None


def contact_ids_of_row(row: dict[str, Any]) -> set[int]:
    try:
        return {int(c) for c in json.loads(row.get("contact_ids") or "[]")}
    except (TypeError, ValueError):
        return set()


def _since_ts(row: dict[str, Any]) -> float:
    """Когда ИИ взял (получил обратно) сделку: `deal_owner.since`, UTC без долей секунды. Не прочиталось: ни один звонок не «позже»."""
    try:
        return datetime.datetime.strptime(str(row.get("since")), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        return float("inf")


def _parse(note: dict[str, Any]) -> tuple[float, int, int, float] | None:
    """(created_at, id примечания, id контакта, updated_at) или None: чужая форма."""
    try:
        created = float(note.get("created_at") or 0)
        return created, int(note["id"]), int(note["entity_id"]), float(note.get("updated_at") or created)
    except (KeyError, TypeError, ValueError):
        return None


# =============================================================================================
# база
# =============================================================================================

def cursor_get() -> tuple[float | None, float | None]:
    """(курсор `updated_at`, с какого момента вообще смотрим звонки)."""
    with _Db() as conn:
        kv = {r[0]: float(r[1]) for r in conn.execute("SELECT k, v FROM calls_kv")}
    return kv.get("cursor"), kv.get("watch_from")


def cursor_put(v: float, *, key: str = "cursor") -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO calls_kv (k, v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, float(v)))


def seen_has(note_id: int) -> bool:
    with _Db() as conn:
        return conn.execute("SELECT 1 FROM calls_note_seen WHERE note_id=?", (int(note_id),)).fetchone() is not None


def seen_add(note_id: int, now: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT OR IGNORE INTO calls_note_seen (note_id, at) VALUES (?,?)", (int(note_id), float(now)))
        conn.execute("DELETE FROM calls_note_seen WHERE at<?", (float(now) - SEEN_KEEP_S,))


def log_taken(deal_id: int, now: float, kind: str, note_id: int) -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO calls_log (deal_id, at, kind, note_id) VALUES (?,?,?,?)", (int(deal_id), float(now), kind, int(note_id)))
        conn.execute("DELETE FROM calls_log WHERE at<?", (float(now) - LOG_KEEP_S,))


def journal(limit: int = 50) -> list[dict[str, Any]]:
    """Сделки, снятые звонком, свежее первым: для проверки руками и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM calls_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]


def queue_note(deal_id: int, kind: str, now: float) -> None:
    """Примечание о снятии в очередь отправки. Прежнее неотправленное по этой сделке заменяется: актуально только последнее снятие."""
    with _Db() as conn:
        conn.execute("DELETE FROM takeover_note WHERE deal_id=?", (int(deal_id),))
        conn.execute("INSERT INTO takeover_note (deal_id, kind, at, next_try_at) VALUES (?,?,?,?)", (int(deal_id), kind, float(now), float(now)))


def notes_due(now: float) -> list[dict[str, Any]]:
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM takeover_note WHERE next_try_at<=? ORDER BY id", (float(now),)).fetchall()]


def note_drop(row_id: int) -> None:
    with _Db() as conn:
        conn.execute("DELETE FROM takeover_note WHERE id=?", (int(row_id),))


def note_defer(row_id: int, tries: int, next_try_at: float) -> None:
    with _Db() as conn:
        conn.execute("UPDATE takeover_note SET tries=?, next_try_at=? WHERE id=?", (int(tries), float(next_try_at), int(row_id)))


# =============================================================================================
# amo
# =============================================================================================

async def _amo_calls_since(since: float) -> tuple[list[dict[str, Any]], bool] | None:
    """Звонки (примечания контактов), изменённые не раньше `since`, по возрастанию `updated_at`: (примечания, выдача полная). None: amo не ответила."""
    out: list[dict[str, Any]] = []
    for page in range(1, PAGES_MAX + 1):
        data = await amo_service._do_get("/api/v4/contacts/notes", [
            ("filter[note_type][]", "call_in"), ("filter[note_type][]", "call_out"), ("filter[updated_at][from]", str(int(since))),
            ("order[updated_at]", "asc"), ("limit", str(PAGE_LIMIT)), ("page", str(page))])
        if data is None:
            return None
        out += (data.get("_embedded") or {}).get("notes") or []
        if not (data.get("_links") or {}).get("next"):
            return out, True
    logger.warning("ai_engine_calls: звонков за окно больше %s страниц, остальные увидит следующий опрос", PAGES_MAX)
    return out, False


async def had_call(contact_ids: set[int], since: float) -> bool | None:
    """Был ли звонок-вмешательство по этим контактам не раньше `since`. None: amo не ответила (или выдача обрезана и звонка в ней нет): «звонков нет» не доказано."""
    ids = {int(c) for c in contact_ids}
    if not ids:
        return False
    got = await asyncio.wait_for(_amo_calls_since(since), timeout=AMO_TIMEOUT_S * 6)
    if got is None:
        return None
    notes, complete = got
    for note in notes:
        item = _parse(note)
        if item is not None and item[2] in ids and item[0] >= since and intervention_kind(note):
            return True
    return False if complete else None


async def _fill_contacts(eng, rows: list[dict[str, Any]]) -> bool:
    """Сделкам ИИ с пустым `contact_ids` дописать контакты из amo (и в переданные строки). False: кого-то прочитать не вышло, курсор уходить вперёд не должен."""
    ok, asked = True, 0
    for row in rows:
        if contact_ids_of_row(row):
            continue
        deal_id = int(row["deal_id"])
        if _fill_wait.get(deal_id, 0) > eng._now():
            continue
        if asked >= FILL_PER_POLL:
            return False
        asked += 1
        try:
            lead = await asyncio.wait_for(amo_service.get_lead_full(deal_id, with_=("contacts",)), timeout=AMO_TIMEOUT_S)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("ai_engine_calls: контакты сделки %s не прочитаны (%s)", deal_id, exc.__class__.__name__)
            lead = None
        if lead is None:
            ok = False
            continue
        ids = dedup.contact_ids_of(lead)
        if not ids:
            _fill_wait[deal_id] = eng._now() + FILL_RETRY_S
            continue
        await eng._db(store.set_contact_ids, deal_id, ids)
        row["contact_ids"] = json.dumps(ids)
    return ok


# =============================================================================================
# примечание о снятии
# =============================================================================================

async def _post_note(deal_id: int, text: str) -> str:
    """`ok` | `retry` | `drop` (amo отвергает навсегда: сделки нет или тело не то)."""
    try:
        res = await asyncio.wait_for(amo_service.add_note(deal_id, text), timeout=AMO_TIMEOUT_S * 2)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("ai_engine_calls: примечание о снятии в сделку %s не записалось (%s)", deal_id, exc.__class__.__name__)
        return "retry"
    if isinstance(res, dict) and res.get("ok") is False:
        return "drop" if res.get("status_code") in (400, 404) else "retry"
    return "ok"


async def _send_note(eng, row: dict[str, Any]) -> None:
    deal_id = int(row["deal_id"])
    owner = await eng._db(store.get_owner, deal_id)
    if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"]:
        logger.info("ai_engine_calls: сделку %s уже снова ведёт ИИ, примечание о снятии не пишу", deal_id)
        await eng._db(note_drop, row["id"])
        return
    result = await _post_note(deal_id, NOTE_TEXT.get(row["kind"], NOTE_TEXT[NOTE_MESSAGE]))
    now = eng._now()
    if result == "ok":
        await eng._db(note_drop, row["id"])
    elif result == "drop" or now - float(row["at"]) >= NOTE_GIVE_UP_S:
        logger.error("ai_engine_calls: примечание о снятии в сделку %s так и не записалось, больше не пробую", deal_id)
        await eng._db(note_drop, row["id"])
    else:
        tries = int(row["tries"]) + 1
        logger.warning("ai_engine_calls: примечание о снятии в сделку %s не записалось, попытка %s, повторю позже", deal_id, tries)
        await eng._db(note_defer, row["id"], tries, now + min(NOTE_RETRY_S * 2 ** min(tries - 1, 10), NOTE_RETRY_MAX_S))


async def flush_notes(eng) -> None:
    """Отправить созревшие примечания о снятии. Два прохода одновременно не идут: примечание не пишется дважды. Не бросает (кроме отмены)."""
    global _note_lock
    if _note_lock is None:
        _note_lock = asyncio.Lock()
    async with _note_lock:
        try:
            for row in await eng._db(notes_due, eng._now()):
                await _send_note(eng, row)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine_calls: отправка примечаний о снятии упала")


# =============================================================================================
# опрос звонков
# =============================================================================================

async def _release(eng, deal_id: int, kind: str, note_id: int, called_at: float) -> bool:
    """Снять ИИ со сделки из-за звонка. Всё проверяется заново под общим замком движка. True: сняли."""
    async with eng._get_lock():
        row = await eng._db(store.get_owner, deal_id)
        if (row is None or row["owner"] != store.OWNER_AI or row["closed"] or modes.is_stale(row, settings.active_mode_id())
                or called_at <= _since_ts(row)):
            return False
        s = await eng._db(store.session_for_deal, deal_id)
        await eng._to_human(s, REASON, deal_id=deal_id, alert=None, note=kind)
        await eng._db(log_taken, deal_id, eng._now(), kind, note_id)
    logger.info("ai_engine_calls: сделка %s: звонок менеджера (%s, примечание %s), ИИ приостановил ведение", deal_id, kind, note_id)
    return True


async def _poll(eng) -> None:
    now = eng._now()
    cursor, start = await eng._db(cursor_get)
    if cursor is None or start is None:
        cursor = start = now                                   # первый запуск: прежние звонки не исполняем
        await eng._db(cursor_put, now, key="watch_from")
        await eng._db(cursor_put, now)
    got = await asyncio.wait_for(_amo_calls_since(cursor - OVERLAP_S), timeout=AMO_TIMEOUT_S * 6)
    if got is None:
        _state["last_error"] = "amo_unavailable"
        return
    newest = cursor
    counted: list[tuple[float, int, int, str]] = []            # (когда звонили, id примечания, контакт, вид)
    for note in got[0]:
        item = _parse(note)
        if item is None:
            continue
        created, note_id, contact_id, updated = item
        newest = max(newest, min(updated, now))
        kind = intervention_kind(note)
        if kind is not None and created >= int(start):
            counted.append((created, note_id, contact_id, kind))
    held = False
    fresh = [c for c in sorted(counted) if not await eng._db(seen_has, c[1])]      # раньше звонивший первым: он и назван в примечании
    if fresh:
        active = settings.active_mode_id()
        rows = [r for r in await eng._db(store.list_owned, store.OWNER_AI) if not modes.is_stale(r, active)]
        held = not await _fill_contacts(eng, rows)
        for created, note_id, contact_id, kind in fresh:
            acted = False
            for row in rows:
                if contact_id not in contact_ids_of_row(row) or created <= _since_ts(row):
                    continue
                try:
                    acted = await _release(eng, int(row["deal_id"]), kind, note_id, created) or acted
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - сбой одной сделки не должен оставить остальные; курсор подождёт
                    logger.exception("ai_engine_calls: не снял сделку %s после звонка", row.get("deal_id"))
                    held = True
            if acted:
                await eng._db(seen_add, note_id, eng._now())
    if held:
        newest = min(newest, max(cursor, now - HOLD_MAX_S))
    await eng._db(cursor_put, newest)
    _state["last_poll_at"], _state["last_error"] = now, "incomplete" if held else None


async def run_once(eng) -> None:
    """Один опрос звонков. Два одновременно не идут. Не бросает (кроме отмены): сбой не трогает курсор и таймеры."""
    global _run_lock
    if _run_lock is None:
        _run_lock = asyncio.Lock()
    async with _run_lock:
        try:
            await _poll(eng)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            _state["last_error"] = exc.__class__.__name__
            logger.exception("ai_engine_calls: опрос звонков упал")


async def tick(eng, now: float) -> None:
    """Из `ai_engine.tick_once`: неотправленные примечания о снятии (раз в `NOTES_CHECK_S`) и опрос звонков фоновой задачей (раз в `CALLS_POLL_S`). Не бросает."""
    global _task, _next_run_at, _next_notes_at
    try:
        if now >= _next_notes_at:
            _next_notes_at = now + NOTES_CHECK_S
            if await eng._db(notes_due, now):
                eng._spawn(flush_notes(eng))
        if not enabled() or (_task is not None and not _task.done()) or now < _next_run_at:
            return
        _next_run_at = now + max(5, int(cfg.CALLS_POLL_S))
        _task = eng._spawn(run_once(eng))
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_calls: не запустил опрос")


# =============================================================================================
# срез состояния
# =============================================================================================

def status_block(now: float) -> dict[str, Any]:
    """Блок `calls` среза `GET /`: `enabled`, `poll_s`, `last_poll_at` (последний удачный опрос), `last_error`, `removed_24h` (сделок снято звонком за сутки),
    `notes_pending` (примечания о снятии, ещё не принятые amo)."""
    with _Db() as conn:
        removed = conn.execute("SELECT COUNT(DISTINCT deal_id) FROM calls_log WHERE at>?", (float(now) - 86400,)).fetchone()[0]
        pending = conn.execute("SELECT COUNT(*) FROM takeover_note").fetchone()[0]
    last = _state["last_poll_at"]
    return {
        "enabled": enabled(),
        "poll_s": int(cfg.CALLS_POLL_S),
        "last_poll_at": None if last is None else datetime.datetime.fromtimestamp(last, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_error": _state["last_error"],
        "removed_24h": removed,
        "notes_pending": pending,
    }


def reset_for_tests() -> None:
    global _task, _next_run_at, _next_notes_at, _run_lock, _note_lock
    _task, _next_run_at, _next_notes_at, _run_lock, _note_lock = None, 0.0, 0.0, None, None
    _fill_wait.clear()
    _state.update({"last_poll_at": None, "last_error": None})
