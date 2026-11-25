"""Стоп и старт ИИ примечанием, закреплённая заметка «ИИ ведёт сделку» (решение Кати 24.11.2026).

Зачем. Менеджер должен видеть в карточке, что сделку ведёт ИИ, и одним словом останавливать и возвращать его. Внутренний чат amo для этого не годится:
событие `entity_direct_message` в `/api/v4/events` приходит без текста (только id сообщения), читать переписку сотрудников API не даёт. Поэтому команда
пишется ПРИМЕЧАНИЕМ в сделку (та же строка ввода в карточке, вкладка «Примечание»), клиент его не видит. Катя выбрала этот путь 24.11.2026.

Что делает модуль:

  • Заметка «ИИ ведёт сделку». Перед первым ходом ИИ по сделке (`ai_engine._send_turn`, до `POST /v1/turn`, то есть до генерации ответа) в сделку
    пишется примечание и закрепляется (`POST /api/v4/leads/notes/{id}/pin`). Когда сделка уходит от ИИ (передача человеку, «стоп», закрытие, режим `off`,
    снятие при смене режима, конец ответа вместо менеджера), заметка открепляется (`POST .../unpin`) сверкой раз в `POLL_S` (`_reconcile`). Сверка же
    закрепляет заметку у сделок ИИ, где её нет (сбой amo при первом ходе, сделки, взятые до выката), не больше `RECONCILE_PIN_MAX` за проход.
    Ответ вместо менеджера (перехват в режиме `reply`, причина владельца `intercept_reply`) заметку не получает: ИИ отвечает один раз, сделка остаётся
    у менеджера, о вмешательстве уже пишет своё примечание `ai_engine_intercept`.
  • Команды. Раз в `POLL_S` один запрос `GET /api/v4/leads/notes?filter[note_type][]=common&filter[updated_at][from]=…` (за час таких примечаний
    десятки). Команда: примечание человека (`created_by` не 0; сам движок пишет с 0), весь текст которого после чистки знаков равен «стоп» или «старт»
    (`parse_command`). «Стоп» → `ai_engine.manager_stop`: ИИ отпускает сделку (владелец `human` с причиной `paused`, ход и касания сняты, как у отметки
    «удалить», только обратимо), подбор, перехват и вход по сообщению такую сделку не берут, потому что у неё есть владелец. Закрепляется заметка
    «ИИ на паузе». «Старт» → `ai_engine.manager_start`: новая отсечка `manager_cutoff` (сообщения менеджера за время паузы ИИ видит, но они его не
    останавливают) и возврат диалога (`dialog_returned`): ИИ перечитывает переписку и сразу отвечает, если последнее слово за клиентом. Заметка паузы
    открепляется, закрепляется «ИИ ведёт сделку».

Повторы безопасны: обработанные команды помнит `pause_note_seen`, курсор опроса (`pause_kv.cursor`) идёт с перекрытием `OVERLAP_S`. «Старт», который
не прошёл из-за недоступности ИИ-сервиса, повторяется на следующих опросах, пока команде не больше `START_RETRY_S`, потом в сделку уходит примечание
«не получилось». Первый запуск начинает с текущего момента: старые примечания не исполняются.

Рубильник: `AI_PAUSE_NOTES=0` в .env движка и перезапуск контейнера: ни опроса, ни заметок. Таблицы создаются при первом обращении (`IF NOT EXISTS`)
в той же sqlite, что у `ai_engine_store`. Личных данных клиента здесь нет: номер сделки, id примечания, имя сотрудника из amo.
"""
import asyncio
import datetime
import logging
import re
from typing import Any

import ai_engine_config as cfg
import ai_engine_store as store
import amo_service

logger = logging.getLogger("uvicorn")

CMD_STOP, CMD_START = "stop", "start"
_WORDS = {
    "стоп": CMD_STOP, "стоп ии": CMD_STOP, "ии стоп": CMD_STOP, "stop": CMD_STOP,
    "старт": CMD_START, "старт ии": CMD_START, "ии старт": CMD_START, "start": CMD_START,
}
KIND_AI, KIND_PAUSE = "ai", "pause"

OVERLAP_S = 120                 # курсор опроса отступает назад: примечание, записанное в секунду опроса, не теряется
PAGE_LIMIT, PAGES_MAX = 250, 4
START_RETRY_S = 600             # «старт» при недоступном ИИ-сервисе повторяется столько секунд, потом примечание «не получилось»
SEEN_KEEP_S = 3 * 86400
RECONCILE_PIN_MAX = 5           # сколько недостающих заметок «ИИ ведёт сделку» закрепляет один проход сверки
AMO_TIMEOUT_S = 15
TEMP_REASONS = frozenset({"intercept_reply"})     # временный владелец ИИ (ответ вместо менеджера): заметку не закрепляем

TEXT_AI = "ИИ ведёт сделку. Чтобы остановить ИИ, напишите в сделку примечание «стоп»."
TEXT_AI_BACK = "ИИ снова ведёт сделку: «старт» от {by}, {at}. Чтобы остановить ИИ, напишите в сделку примечание «стоп»."
TEXT_PAUSE = "ИИ на паузе с {at}: «стоп» от {by}. ИИ не пишет клиенту и не берёт сделку. Вернуть ИИ: примечание «старт»."
TEXT_RELEASED = "Пауза снята («старт» от {by}). ИИ эту сделку раньше не вёл: войдёт в неё по обычным правилам."
TEXT_REFUSED = {
    "mode_off": "«Старт» от {by} не сработал: ИИ-менеджер сейчас выключен. Пауза снята.",
    "closed": "«Старт» от {by} не сработал: сделка закрыта.",
    "pipeline_not_allowed": "«Старт» от {by} не сработал: воронку этой сделки ИИ не ведёт. Пауза снята.",
    "ai_unavailable": "«Старт» от {by} не сработал: ИИ-сервис недоступен. Напишите «старт» ещё раз позже.",
}
TEXT_REFUSED_OTHER = "«Старт» от {by} не сработал ({detail}). Пауза снята."

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS deal_pin (deal_id INTEGER NOT NULL, kind TEXT NOT NULL, note_id INTEGER NOT NULL, at REAL NOT NULL, "
    "PRIMARY KEY (deal_id, kind));"
    "CREATE TABLE IF NOT EXISTS deal_pause (deal_id INTEGER PRIMARY KEY, at REAL NOT NULL, by_user TEXT NOT NULL DEFAULT '', note_id INTEGER);"
    "CREATE TABLE IF NOT EXISTS deal_pause_log (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, at REAL NOT NULL, "
    "event TEXT NOT NULL, by_user TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '');"
    "CREATE TABLE IF NOT EXISTS pause_note_seen (note_id INTEGER PRIMARY KEY, at REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS pause_kv (k TEXT PRIMARY KEY, v REAL NOT NULL)"
)

_task: asyncio.Task | None = None
_next_run_at = 0.0
_pin_lock: asyncio.Lock | None = None
_state: dict[str, Any] = {"last_poll_at": None, "last_error": None, "last_commands": 0}
_pin_fail_until: dict[int, float] = {}         # сделка -> до какого времени не пытаться закреплять перед ходом (amo не ответил): ход не ждёт amo каждый раз
PIN_FAIL_PAUSE_S = 300


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def enabled() -> bool:
    return bool(cfg.PAUSE_NOTES_ENABLED)


def _lock() -> asyncio.Lock:
    global _pin_lock
    if _pin_lock is None:
        _pin_lock = asyncio.Lock()
    return _pin_lock


def _msk(ts: float) -> str:
    return datetime.datetime.fromtimestamp(float(ts), datetime.timezone(datetime.timedelta(hours=3))).strftime("%d.%m %H:%M")


# =============================================================================================
# разбор команды
# =============================================================================================

def handover_text(note: str) -> str:
    """Текст закреплённой заметки, когда сделку отдали ИИ без сообщения клиента (подбор, «Дать ИИ», одобрение смены ответственного): текст передачи
    и как остановить ИИ, одним примечанием вместо двух."""
    n = str(note or "").strip().rstrip(".")
    if not n:
        return TEXT_AI
    return n[:1].upper() + n[1:] + ". Чтобы остановить ИИ, напишите в сделку примечание «стоп»."


def parse_command(text: Any) -> str | None:
    """`stop` | `start` | None. Весь текст примечания после чистки (регистр, «ё», кавычки и знаки препинания по краям и между словами) должен
    быть ровно командой: «Стоп», «стоп.», «СТАРТ ИИ», «stop». «Стоп-кран», «стоп, подожди» и любой длинный текст командой не считаются."""
    t = str(text or "").lower().replace("ё", "е")
    t = re.sub(r"[\s\"'«»“”„.,!?;:()\[\]-]+", " ", t).strip()
    return _WORDS.get(t)


# =============================================================================================
# база
# =============================================================================================

def pin_get(deal_id: int, kind: str) -> dict[str, Any] | None:
    with _Db() as conn:
        row = conn.execute("SELECT * FROM deal_pin WHERE deal_id=? AND kind=?", (int(deal_id), kind)).fetchone()
    return dict(row) if row else None


def pin_put(deal_id: int, kind: str, note_id: int, now: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO deal_pin (deal_id, kind, note_id, at) VALUES (?,?,?,?) "
                     "ON CONFLICT(deal_id, kind) DO UPDATE SET note_id=excluded.note_id, at=excluded.at", (int(deal_id), kind, int(note_id), float(now)))


def pin_drop(deal_id: int, kind: str) -> None:
    with _Db() as conn:
        conn.execute("DELETE FROM deal_pin WHERE deal_id=? AND kind=?", (int(deal_id), kind))


def pins(kind: str) -> list[dict[str, Any]]:
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM deal_pin WHERE kind=? ORDER BY at", (kind,)).fetchall()]


def pause_get(deal_id: int) -> dict[str, Any] | None:
    with _Db() as conn:
        row = conn.execute("SELECT * FROM deal_pause WHERE deal_id=?", (int(deal_id),)).fetchone()
    return dict(row) if row else None


def pause_put(deal_id: int, now: float, by: str) -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO deal_pause (deal_id, at, by_user) VALUES (?,?,?) "
                     "ON CONFLICT(deal_id) DO UPDATE SET at=excluded.at, by_user=excluded.by_user", (int(deal_id), float(now), str(by)[:80]))


def pause_drop(deal_id: int) -> bool:
    with _Db() as conn:
        return conn.execute("DELETE FROM deal_pause WHERE deal_id=?", (int(deal_id),)).rowcount > 0


def drop_pause_owner(deal_id: int) -> bool:
    """Убрать запись владельца, которую создали только «стоп» или «удалить» (человек, причина `paused`/`removed`, без чата): после «старт» сделка снова ничья."""
    with store._connect() as conn:
        return conn.execute("DELETE FROM deal_owner WHERE deal_id=? AND owner=? AND reason IN ('paused', 'removed') AND (chat_id IS NULL OR chat_id='')",
                            (int(deal_id), store.OWNER_HUMAN)).rowcount > 0


def log(deal_id: int, now: float, event: str, by: str = "", note: str = "") -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO deal_pause_log (deal_id, at, event, by_user, note) VALUES (?,?,?,?,?)",
                     (int(deal_id), float(now), event, str(by)[:80], str(note)[:300]))


def journal(limit: int = 50) -> list[dict[str, Any]]:
    """Последние строки журнала, свежее первым: для проверки руками и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM deal_pause_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]


def seen_has(note_id: int) -> bool:
    with _Db() as conn:
        return conn.execute("SELECT 1 FROM pause_note_seen WHERE note_id=?", (int(note_id),)).fetchone() is not None


def seen_add(note_id: int, now: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT OR IGNORE INTO pause_note_seen (note_id, at) VALUES (?,?)", (int(note_id), float(now)))
        conn.execute("DELETE FROM pause_note_seen WHERE at<?", (float(now) - SEEN_KEEP_S,))


def cursor_get() -> float | None:
    with _Db() as conn:
        row = conn.execute("SELECT v FROM pause_kv WHERE k='cursor'").fetchone()
    return float(row[0]) if row else None


def cursor_put(v: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT INTO pause_kv (k, v) VALUES ('cursor', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (float(v),))


def counts(now: float) -> dict[str, Any]:
    with _Db() as conn:
        paused = conn.execute("SELECT COUNT(*) FROM deal_pause").fetchone()[0]
        pinned = {k: n for k, n in conn.execute("SELECT kind, COUNT(*) FROM deal_pin GROUP BY kind")}
        day = {e: n for e, n in conn.execute("SELECT event, COUNT(*) FROM deal_pause_log WHERE at>? GROUP BY event", (float(now) - 86400,))}
    return {"paused": paused, "pinned": {KIND_AI: pinned.get(KIND_AI, 0), KIND_PAUSE: pinned.get(KIND_PAUSE, 0)}, "events_24h": day}


# =============================================================================================
# amo (подменяются в тестах)
# =============================================================================================

async def _amo_add_note(deal_id: int, text: str) -> int | None:
    """Примечание в сделку; id нового примечания или None (amo не ответил)."""
    url = amo_service._build_url(f"/api/v4/leads/{int(deal_id)}/notes")
    for attempt in (1, 2):
        try:
            resp = await amo_service.submit_request("POST", url, amo_service.headers,
                                                    json_body=[{"note_type": "common", "params": {"text": text}}])
            if resp.status_code in (200, 201):
                return int(resp.json()["_embedded"]["notes"][0]["id"])
            logger.warning("ai_engine_pause: примечание в сделку %s: amo ответил %s", deal_id, resp.status_code)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine_pause: примечание в сделку %s не записалось (попытка %s)", deal_id, attempt)
        if attempt == 1:
            await asyncio.sleep(2)
    return None


async def _amo_pin(note_id: int, pin: bool) -> bool:
    res = await amo_service._do_post(f"/api/v4/leads/notes/{int(note_id)}/{'pin' if pin else 'unpin'}", None)
    return bool(res.get("ok"))


async def _amo_notes_since(since: float) -> list[dict[str, Any]] | None:
    """Обычные примечания сделок, изменённые не раньше `since`, по возрастанию времени; None: amo не ответил."""
    out: list[dict[str, Any]] = []
    for page in range(1, PAGES_MAX + 1):
        data = await amo_service._do_get("/api/v4/leads/notes", [
            ("filter[note_type][]", "common"), ("filter[updated_at][from]", str(int(since))),
            ("order[updated_at]", "asc"), ("limit", str(PAGE_LIMIT)), ("page", str(page))])
        if data is None:
            return None
        notes = ((data or {}).get("_embedded") or {}).get("notes") or []
        out += notes
        if len(notes) < PAGE_LIMIT:
            return out
    logger.warning("ai_engine_pause: за опрос больше %s примечаний, хвост увидит следующий опрос", PAGE_LIMIT * PAGES_MAX)
    return out


async def _user_name(user_id: int) -> str:
    try:
        name = await asyncio.wait_for(amo_service.get_user_name(int(user_id)), timeout=AMO_TIMEOUT_S)
    except Exception:  # noqa: BLE001
        name = None
    return name or f"сотрудник {user_id}"


# =============================================================================================
# заметки
# =============================================================================================

async def _pin_note(deal_id: int, kind: str, text: str, now: float) -> bool:
    """Записать примечание (с префиксом «ИИ-менеджер: ») и закрепить; запомнить в `deal_pin`. False: не вышло (повторит сверка или следующий ход)."""
    note_id = await asyncio.wait_for(_amo_add_note(deal_id, "ИИ-менеджер: " + text), timeout=AMO_TIMEOUT_S * 2)
    if note_id is None:
        return False
    pinned = await asyncio.wait_for(_amo_pin(note_id, True), timeout=AMO_TIMEOUT_S)
    if not pinned:
        logger.warning("ai_engine_pause: примечание %s в сделке %s записано, но не закрепилось", note_id, deal_id)
    await asyncio.to_thread(pin_put, deal_id, kind, note_id, now)       # и незакреплённое помним: повторно не пишем, открепить его тоже не вредно
    return True


async def _unpin(deal_id: int, kind: str) -> None:
    row = await asyncio.to_thread(pin_get, deal_id, kind)
    if row is None:
        return
    try:
        ok = await asyncio.wait_for(_amo_pin(row["note_id"], False), timeout=AMO_TIMEOUT_S)
    except Exception:  # noqa: BLE001
        ok = False
    if ok:
        await asyncio.to_thread(pin_drop, deal_id, kind)
    else:
        logger.warning("ai_engine_pause: не открепил примечание %s в сделке %s, повторю сверкой", row["note_id"], deal_id)


async def ensure_ai_pin(eng, deal_id: int, owner: dict[str, Any] | None = None, text: str = TEXT_AI) -> None:
    """Перед ходом ИИ: закреплённая заметка «ИИ ведёт сделку» есть или появляется сейчас. Не бросает и не держит ход дольше таймаутов amo: заметка
    не должна останавливать ответ клиенту (не вышло: повторит сверка)."""
    if not enabled():
        return
    try:
        if owner is not None and (owner.get("reason") in TEMP_REASONS):
            return
        if _pin_fail_until.get(int(deal_id), 0) > eng._now():
            return
        async with _lock():
            if await asyncio.to_thread(pin_get, deal_id, KIND_AI) is not None:
                return
            await _unpin(deal_id, KIND_PAUSE)
            await asyncio.to_thread(pause_drop, deal_id)          # ИИ снова ведёт сделку (вернули из панели): паузы больше нет
            ok = False
            try:
                ok = await _pin_note(deal_id, KIND_AI, text, eng._now())
            finally:
                if ok:
                    _pin_fail_until.pop(int(deal_id), None)
                    await asyncio.to_thread(log, deal_id, eng._now(), "pinned_ai")
                else:
                    _pin_fail_until[int(deal_id)] = eng._now() + PIN_FAIL_PAUSE_S
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_pause: заметка «ИИ ведёт сделку» в сделке %s не закрепилась", deal_id)


# =============================================================================================
# опрос команд и сверка заметок
# =============================================================================================

async def tick(eng, now: float) -> None:
    """Из `ai_engine.tick_once`: раз в `POLL_S` запускает опрос фоновой задачей. Не бросает."""
    global _task, _next_run_at
    try:
        if not enabled() or (_task is not None and not _task.done()) or now < _next_run_at:
            return
        _next_run_at = now + max(5, int(cfg.PAUSE_POLL_S))
        _task = eng._spawn(run_once(eng))
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_pause: не запустил опрос")


_run_lock: asyncio.Lock | None = None


async def run_once(eng) -> None:
    """Один опрос команд и одна сверка заметок. Два опроса одновременно не идут: команда не исполняется дважды."""
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
            logger.exception("ai_engine_pause: опрос команд упал")
        try:
            await _reconcile(eng)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine_pause: сверка заметок упала")


async def _poll(eng) -> None:
    now = eng._now()
    cursor = await asyncio.to_thread(cursor_get)
    if cursor is None:
        await asyncio.to_thread(cursor_put, now)           # первый запуск: старые примечания не исполняем
        _state["last_poll_at"] = now
        return
    notes = await asyncio.wait_for(_amo_notes_since(cursor - OVERLAP_S), timeout=AMO_TIMEOUT_S * 6)
    if notes is None:
        _state["last_error"] = "amo_unavailable"
        return
    hold: float | None = None                              # время самой ранней команды, которую надо повторить: курсор за неё не уходит
    done = 0
    newest = cursor
    for n in notes:
        updated = float(n.get("updated_at") or n.get("created_at") or 0)
        newest = max(newest, min(updated, now))
        cmd = parse_command((n.get("params") or {}).get("text"))
        if cmd is None or not n.get("created_by") or n.get("note_type") not in (None, "common"):
            continue
        note_id, deal_id = int(n["id"]), int(n["entity_id"])
        if await asyncio.to_thread(seen_has, note_id):
            continue
        created = float(n.get("created_at") or updated)
        by = await _user_name(int(n["created_by"]))
        finished = await (_stop(eng, deal_id, by, created) if cmd == CMD_STOP else _start(eng, deal_id, by, created))
        if finished:
            await asyncio.to_thread(seen_add, note_id, eng._now())
            done += 1
        else:
            hold = updated if hold is None else min(hold, updated)
    new_cursor = max(cursor, newest) if hold is None else min(max(cursor, newest), hold)
    await asyncio.to_thread(cursor_put, new_cursor)
    _state["last_poll_at"], _state["last_commands"], _state["last_error"] = now, done, None


async def _stop(eng, deal_id: int, by: str, at: float) -> bool:
    """«Стоп». True: команда исполнена (или исполнять нечего), повторять не надо."""
    now = eng._now()
    if await asyncio.to_thread(pause_get, deal_id) is not None:
        owner = await eng._db(store.get_owner, deal_id)
        if owner is None or owner["owner"] != store.OWNER_AI or owner["closed"]:
            await asyncio.to_thread(log, deal_id, now, "stop_repeat", by)
            return True                                    # сделка и так на паузе: второй «стоп» ничего не меняет
        # пауза записана, но ИИ сделку уже ведёт (вернули кнопкой в панели, не «старт»): это новый «стоп»
    res = await eng.manager_stop(deal_id)
    if res.get("state") == "closed":
        await asyncio.to_thread(log, deal_id, now, "stop_closed", by)
        return True
    await asyncio.to_thread(pause_put, deal_id, now, by)
    await asyncio.to_thread(log, deal_id, now, "stop", by, "вёл ИИ" if res.get("was_ai") else "ИИ не вёл")
    logger.info("ai_engine_pause: сделка %s на паузе («стоп» от %s, ИИ %s)", deal_id, by, "вёл" if res.get("was_ai") else "не вёл")
    async with _lock():
        await _unpin(deal_id, KIND_AI)
        await _pin_note(deal_id, KIND_PAUSE, TEXT_PAUSE.format(at=_msk(at), by=by), now)
    return True


async def _start(eng, deal_id: int, by: str, at: float) -> bool:
    """«Старт». False: ИИ-сервис недоступен и команда моложе `START_RETRY_S`, повторить на следующем опросе."""
    now = eng._now()
    res = await eng.manager_start(deal_id)
    state, detail = res.get("state"), res.get("detail") or ""
    if state == "retry" and now - at < START_RETRY_S:
        return False
    paused = await asyncio.to_thread(pause_drop, deal_id)
    await asyncio.to_thread(log, deal_id, now, f"start_{state}", by, detail)
    logger.info("ai_engine_pause: сделка %s: «старт» от %s: %s %s", deal_id, by, state, detail)
    async with _lock():
        await _unpin(deal_id, KIND_PAUSE)
        if state == "returned":
            await _unpin(deal_id, KIND_AI)
            await _pin_note(deal_id, KIND_AI, TEXT_AI_BACK.format(by=by, at=_msk(at)), now)
        elif state == "released":
            if paused:
                await _note(deal_id, TEXT_RELEASED.format(by=by))
        elif state in ("refused", "retry"):
            key = "ai_unavailable" if state == "retry" else detail
            await _note(deal_id, TEXT_REFUSED.get(key, TEXT_REFUSED_OTHER).format(by=by, detail=detail or "причина не названа"))
    return True


async def _note(deal_id: int, text: str) -> None:
    try:
        await asyncio.wait_for(_amo_add_note(deal_id, "ИИ-менеджер: " + text), timeout=AMO_TIMEOUT_S * 2)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_pause: примечание в сделку %s не записалось", deal_id)


async def _reconcile(eng) -> None:
    """Заметка «ИИ ведёт сделку» закреплена ровно у сделок, которые ведёт ИИ: лишние открепить, недостающие (не больше `RECONCILE_PIN_MAX`) закрепить."""
    for row in await asyncio.to_thread(pins, KIND_AI):
        owner = await eng._db(store.get_owner, row["deal_id"])
        if owner is None or owner["owner"] != store.OWNER_AI or owner["closed"]:
            async with _lock():
                await _unpin(row["deal_id"], KIND_AI)
    pinned = {r["deal_id"] for r in await asyncio.to_thread(pins, KIND_AI)}
    added = 0
    for owner in await eng._db(_ai_owners):
        if added >= RECONCILE_PIN_MAX:
            break
        if owner["deal_id"] in pinned or owner.get("reason") in TEMP_REASONS:
            continue
        await ensure_ai_pin(eng, int(owner["deal_id"]), owner)
        added += 1


def _ai_owners() -> list[dict[str, Any]]:
    with store._connect() as conn:
        return [dict(r) for r in conn.execute("SELECT deal_id, reason FROM deal_owner WHERE owner=? AND closed=0 ORDER BY since DESC",
                                              (store.OWNER_AI,)).fetchall()]


def last_poll_at() -> float | None:
    """Когда команды из примечаний последний раз прочитаны удачно (с последнего запуска процесса): возврат после выключения (`ai_engine_mode_return`) ждёт этого, чтобы не пройти мимо «стоп»."""
    return _state["last_poll_at"]


def status_block(now: float) -> dict[str, Any]:
    """Блок `pause` среза `GET /`: включено ли, сколько сделок на паузе, сколько заметок закреплено, события за сутки, последний опрос."""
    out: dict[str, Any] = {"enabled": enabled(), "poll_s": int(cfg.PAUSE_POLL_S)}
    out.update(counts(now))
    last = _state["last_poll_at"]
    out["last_poll_at"] = None if last is None else datetime.datetime.fromtimestamp(last, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out["last_error"], out["last_commands"] = _state["last_error"], _state["last_commands"]
    return out


def reset_for_tests() -> None:
    global _task, _next_run_at, _pin_lock, _run_lock
    _task, _next_run_at, _pin_lock, _run_lock = None, 0.0, None, None
    _pin_fail_until.clear()
    _state.update({"last_poll_at": None, "last_error": None, "last_commands": 0})

