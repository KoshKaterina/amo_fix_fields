"""Возврат ИИ сделок, снятых выключением режима (решение Кати 25.11.2026: «сделки, снятые выключением режима, должны возвращаться ИИ при включении»).

Что было. При режиме «Выключено» `ai_engine._sweep_if_off` переводит каждую сделку ИИ в `owner = human`, `reason = mode_off`, чат `human`. После повторного включения их не брал никто: вход по сообщению клиента
выходит по человеческому чату, подбор не берёт сделку с записью владельца. Вернуть можно было только руками («Дать сделку ИИ», «старт»).

Что делает модуль. Режим снова включён: движок сам возвращает такие сделки тем же путём, что человек (`ai_engine._return_dialog_locked`, это тело `return_dialog`): `owner = ai`, `epoch + 1`, событие
`dialog_returned` (ИИ перечитывает переписку и отвечает, только если клиент ждёт).

Условия, все обязательны (сомнение решается в пользу людей):
  1. запись владельца: `human`, `reason = mode_off`, не закрыта, режим записи (`ai_engine_modes.mode_of`) равен активному. ⚠️ `modes.is_stale` здесь не годится: для записи человека он всегда ложь.
     Чужой режим не помечается: сделка ждёт, пока вернут её режим (или пока не пройдут трое суток);
  2. снята не раньше чем `filters.MANAGER_OUT_KEEP_S` назад (3 суток: таблица `manager_out` старше не хранит) и тогда, когда движок уже непрерывно запоминал сообщения менеджеров (`beat`, `watch_from`):
     до этой версии вебхук при «Выключено» отбрасывался целиком, и доказать, что менеджер не отвечал, по таким сделкам нечем. То же после простоя дольше `GAP_S` (откат на прежний код, выключенный движок);
  3. после снятия человек клиенту не писал: `filters.manager_last_out` по всем чатам этой сделки и по тому же клиенту на остальных слушаемых каналах не позже снятия. Канал чата должен слушаться.
     И не звонил: по контактам сделки нет исходящего или отвеченного входящего звонка с момента снятия (`ai_engine_calls.had_call`, запас `CHECK_SLACK_S`; amo молчит: повторить позже; `AI_CALLS_TAKEOVER=0`: не проверяется);
  4. сделка в amo открыта, воронка разрешена режимом и с промптом (`settings.pipeline_allowed`), на ней нет открытой задачи сотрудника, кроме пользователя ИИ (та же семантика, что у `ai_engine._task_guard`),
     нет отметок людей (`ai_engine_marks`) и паузы «стоп» (`ai_engine_pause`), чат не ведёт авто-режим; выбор сделки по чату (`dedup.resolve`, как на первом ходе после возврата) даёт именно её, иначе
     возврат кончился бы передачей людям с алертом и задачей (правила A, C, D);
  5. окно рабочих часов открыто, настройки панели свежие, движок включён, ИИ-сервис отвечает, команды «стоп» из amo прочитаны недавно.

Постоянная причина отказа: `reason` записи становится `mode_off_kept` (`KEPT`), сделка остаётся у людей и больше не проверяется. Временная (amo не ответила, авто-режим, чат в работе, словарь воронок ещё не
получен): `mode_off` остаётся, повтор не раньше чем через `RETRY_S`. «Ответ вместо менеджера» (перехват в режиме reply) помечается `mode_off_kept` сразу при выключении: эта сделка всегда оставалась у менеджера.

Нагрузка: проход раз в `INTERVAL_S` фоновой задачей (не отдельный цикл, `loops_alive` по-прежнему пять), до amo за проход доходит не больше `PER_RUN` сделок, две неудачи amo подряд ставят паузу.
Когда очередь разобрана, одна строка итога без пингов и имён уходит в чат ОП (только если что-то вернули).

Рубильник: `AI_MODE_RETURN_DISABLED=1` в .env движка и перезапуск контейнера. Срез `GET /` → `mode_return`. Таблицы создаются при первом обращении (`IF NOT EXISTS`) в той же sqlite, что у `ai_engine_store`.
Личных данных клиента здесь нет: номер сделки и причина.
"""
import asyncio
import datetime
import logging
from typing import Any

import ai_engine_approval as approval
import ai_engine_calls as calls
import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_filters as filters
import ai_engine_give as give
import ai_engine_intercept as intercept
import ai_engine_marks as marks
import ai_engine_modes as modes
import ai_engine_pause as pause
import ai_engine_prompts as prompts
import ai_engine_settings as settings
import ai_engine_store as store
import ai_engine_tasks
import amo_service
from shared.phones import digits_only

logger = logging.getLogger("uvicorn")

REASON = "mode_off"              # `deal_owner.reason`: снята выключением режима, ждёт возврата
KEPT = "mode_off_kept"           # снята выключением режима и оставлена людям: больше не проверяется
INTERVAL_S = 60                  # проход не чаще раза в минуту (в тестах меняется)
PER_RUN = 3                      # сделок за проход, доходящих до amo
RETRY_S = 600                    # временная причина и две неудачи amo подряд: повторить не раньше
BEAT_S = 60                      # как часто отмечать «движок на связи и запоминает сообщения менеджеров»
GAP_S = 900                      # простой дольше: про сделки, снятые раньше него, «менеджер не писал» уже не доказать
PAUSE_FRESH_S = 120              # команды «стоп» должны быть прочитаны не раньше этого срока назад
LOG_KEEP_S = 7 * 86400

# Решение по сделке: возвращена, оставлена людям, повторить позже, amo не ответила (повторить и считать неудачей), прервать проход, ничего не делать.
RETURNED, KEEP, RETRY, FAILED, STOP, SKIP = "returned", "kept", "retry", "failed", "stop", "skip"
Verdict = tuple[str, str]

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS mode_return_log (id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, at REAL NOT NULL, outcome TEXT NOT NULL, "
    "note TEXT NOT NULL DEFAULT '', reported INTEGER NOT NULL DEFAULT 0);"
    "CREATE TABLE IF NOT EXISTS mode_return_retry (deal_id INTEGER PRIMARY KEY, since TEXT NOT NULL, retry_at REAL NOT NULL, note TEXT NOT NULL DEFAULT '');"
    "CREATE TABLE IF NOT EXISTS mode_return_meta (k TEXT PRIMARY KEY, v REAL NOT NULL)"
)

_task: asyncio.Task | None = None
_next_run_at = 0.0
_beat_at: float | None = None
_pending_report = False           # есть исходы, о которых итог в чат ОП ещё не ушёл
_state: dict[str, Any] = {"last_run_at": None, "last_result": ""}


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _since_ts(row: dict[str, Any]) -> float | None:
    """Момент снятия (`deal_owner.since`, UTC без долей секунды) в секундах эпохи."""
    try:
        return datetime.datetime.strptime(str(row.get("since")), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:
        return None


# =============================================================================================
# хранилище
# =============================================================================================

def beat(now: float) -> float:
    """Отметить «движок на связи и запоминает сообщения менеджеров». Возвращает `watch_from`: с какого момента это известно без перерывов. Первый запуск или перерыв
    дольше `GAP_S` (простой, откат на прежний код, выключенный `AI_ENGINE_ENABLED`): отсчёт заново."""
    now = float(now)
    with _Db() as conn:
        meta = {r[0]: float(r[1]) for r in conn.execute("SELECT k, v FROM mode_return_meta")}
        start, alive = meta.get("watch_from"), meta.get("alive")
        if start is None or alive is None or now - alive > GAP_S:
            start = now
        for key, value in (("watch_from", start), ("alive", now)):
            conn.execute("INSERT INTO mode_return_meta (k, v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, value))
    return start


def watch_from() -> float | None:
    with _Db() as conn:
        row = conn.execute("SELECT v FROM mode_return_meta WHERE k='watch_from'").fetchone()
    return float(row[0]) if row else None


def waiting() -> list[dict[str, Any]]:
    """Сделки, снятые выключением режима и ещё не решённые, давно снятые первыми."""
    with store._connect() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM deal_owner WHERE owner=? AND reason=? AND closed=0 ORDER BY since, deal_id",
                                              (store.OWNER_HUMAN, REASON)).fetchall()]


def _log(conn, deal_id: int, now: float, outcome: str, note: str) -> None:
    conn.execute("INSERT INTO mode_return_log (deal_id, at, outcome, note) VALUES (?,?,?,?)", (int(deal_id), float(now), outcome, str(note)[:300]))
    conn.execute("DELETE FROM mode_return_retry WHERE deal_id=?", (int(deal_id),))
    conn.execute("DELETE FROM mode_return_log WHERE at<?", (float(now) - LOG_KEEP_S,))


def keep(row: dict[str, Any], now: float, note: str) -> bool:
    """Оставить сделку людям: причина `mode_off_kept`. Только пока запись та самая (человек, `mode_off`, тот же момент снятия); владельца, эпоху и `since` не трогает."""
    with _Db() as conn:
        done = conn.execute("UPDATE deal_owner SET reason=? WHERE deal_id=? AND owner=? AND reason=? AND since=?",
                            (KEPT, int(row["deal_id"]), store.OWNER_HUMAN, REASON, row["since"])).rowcount > 0
        if done:
            _log(conn, row["deal_id"], now, KEEP, note)
    return done


def log_returned(deal_id: int, now: float, note: str) -> None:
    with _Db() as conn:
        _log(conn, deal_id, now, RETURNED, note)


def defer(row: dict[str, Any], retry_at: float, note: str) -> None:
    """Временная причина: до `retry_at` сделку не проверять. Новое снятие (другой `since`) пауза не касается."""
    with _Db() as conn:
        conn.execute("INSERT INTO mode_return_retry (deal_id, since, retry_at, note) VALUES (?,?,?,?) "
                     "ON CONFLICT(deal_id) DO UPDATE SET since=excluded.since, retry_at=excluded.retry_at, note=excluded.note",
                     (int(row["deal_id"]), row["since"], float(retry_at), str(note)[:300]))


def deferred(now: float) -> set[tuple[int, str]]:
    with _Db() as conn:
        return {(int(r[0]), str(r[1])) for r in conn.execute("SELECT deal_id, since FROM mode_return_retry WHERE retry_at>?", (float(now),))}


def journal(limit: int = 50) -> list[dict[str, Any]]:
    """Последние решения, свежее первым: для проверки руками и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM mode_return_log ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()]


def _take_unreported(mode_id: str) -> tuple[int, int, int]:
    """(возвращено, оставлено, номер последней строки) по исходам, о которых ещё не писали в чат; строки помечаются учтёнными. Пока никого не вернули, а в активном режиме
    ещё есть ждущие сделки, не забирает ничего: оставленные людям войдут в итог вместе с возвращёнными."""
    pending_deals = any(modes.mode_of(r) == mode_id for r in waiting())
    with _Db() as conn:
        rows = conn.execute("SELECT id, outcome FROM mode_return_log WHERE reported=0").fetchall()
        returned = sum(r[1] == RETURNED for r in rows)
        if not rows or (not returned and pending_deals):
            return 0, 0, 0
        last = max(int(r[0]) for r in rows)
        conn.execute("UPDATE mode_return_log SET reported=1 WHERE reported=0 AND id<=?", (last,))
    return returned, sum(r[1] == KEEP for r in rows), last


def counts_since(since: float) -> dict[str, int]:
    with _Db() as conn:
        return {r[0]: r[1] for r in conn.execute("SELECT outcome, COUNT(*) FROM mode_return_log WHERE at>? GROUP BY outcome", (float(since),))}


# =============================================================================================
# проверки
# =============================================================================================

def blocked_by(now: float) -> str | None:
    """Почему возврат сейчас не работает (одной строкой) или None: можно. Только то, что решается без сети; «ИИ-сервис отвечает» проверяет проход."""
    if not cfg.ENABLED:
        return "движок выключен"
    if cfg.MODE_RETURN_DISABLED:
        return "аварийный рубильник AI_MODE_RETURN_DISABLED=1"
    if settings.effective_mode() == "off":                # сюда же протухшие настройки панели
        return "режим «Выключено» или настройки панели устарели"
    if not settings.send_window()["open_now"]:
        return "сейчас вне рабочих часов"
    if pause.enabled():
        at = pause.last_poll_at()
        if at is None or now - at > max(PAUSE_FRESH_S, 4 * cfg.PAUSE_POLL_S):
            return "примечания «стоп» из amo сейчас не читаются: ждём удачный опрос"
    return None


def _too_old(row: dict[str, Any], now: float, start: float | None) -> str | None:
    """Постоянный отказ по возрасту записи: «менеджер после снятия не писал» уже не доказать."""
    since = _since_ts(row)
    if since is None or now - since > filters.MANAGER_OUT_KEEP_S:
        return "снята больше трёх суток назад: писал ли менеджер потом, уже не проверить"
    if start is None or since < start - 1:                # `since` без долей секунды
        return "снята, когда движок не запоминал сообщения менеджеров непрерывно (до этой версии или перед простоем)"
    return None


def _chat_keys(row: dict[str, Any]) -> set[str]:
    """Чаты, в которых ответ менеджера значит «человек занялся клиентом»: чат записи владельца, все чаты, где движок вёл эту сделку (сессии и ходы), и тот же
    chat_id или телефон на остальных слушаемых каналах."""
    deal_id = int(row["deal_id"])
    keys, ids = {str(row.get("chat_key") or "")}, {str(row.get("chat_id") or "")}
    with store._connect() as conn:
        for r in conn.execute("SELECT chat_key, chat_id, phone FROM chat_session WHERE deal_id=?", (deal_id,)):
            keys.add(r["chat_key"])
            ids.update((r["chat_id"], digits_only(r["phone"]) if r["phone"] else ""))
        keys.update(r[0] for r in conn.execute("SELECT DISTINCT chat_key FROM turn WHERE deal_id=?", (deal_id,)))
    channels = set(cfg.CHANNELS) | {str(row.get("channel_id") or "")}
    keys.update(f"{channel}:{chat}" for channel in channels for chat in ids if channel and chat)
    return {k for k in keys if k}


def _other_ai_deal(chat_key: str, deal_id: int) -> bool:
    with store._connect() as conn:
        return conn.execute("SELECT 1 FROM deal_owner WHERE chat_key=? AND owner=? AND closed=0 AND deal_id<>? LIMIT 1",
                            (chat_key, store.OWNER_AI, int(deal_id))).fetchone() is not None


def _chat_of(row: dict[str, Any]) -> tuple[str, str]:
    """(chat_id, телефон) того чата, по которому пойдёт ход после возврата (`ai_engine._queue_event`: последняя сессия сделки, нет её: запись владельца)."""
    s = store.session_for_deal(int(row["deal_id"]))
    return (s["chat_id"], s.get("phone") or "") if s else (str(row.get("chat_id") or ""), "")


def _base_verdict(row: dict[str, Any]) -> Verdict | None:
    """Проверки по базе движка, без сети. None: возражений нет."""
    deal_id = int(row["deal_id"])
    chat_key, channel_id = row.get("chat_key"), row.get("channel_id")
    if not (chat_key and channel_id and row.get("chat_id")):
        return KEEP, "в записи сделки нет чата клиента"
    if channel_id not in cfg.CHANNELS and not intercept.is_tracked(chat_key):
        return KEEP, "канал чата движок не слушает: ответов менеджера там не видно"
    since = _since_ts(row) or 0.0
    if any((filters.manager_last_out(key) or 0.0) > since for key in _chat_keys(row)):
        return KEEP, "после снятия клиенту писал менеджер"
    mark = marks.get(deal_id)
    if mark is not None:
        return KEEP, f"стоит отметка человека ({mark['kind']})"
    if pause.pause_get(deal_id) is not None:
        return KEEP, "сделка на паузе (примечание «стоп»)"
    if intercept.kind_of(deal_id) == intercept.KIND_REPLY:
        return KEEP, "ответ вместо менеджера: сделка остаётся у менеджера"
    if intercept.manager_replied_after(deal_id):
        return KEEP, "перехват: менеджер уже ответил клиенту"
    if _other_ai_deal(chat_key, deal_id):
        return KEEP, "в этом чате ИИ уже ведёт другую сделку"
    s = store.get_session(chat_key)
    if s is not None and s["deal_id"] != deal_id and s["state"] in (store.S_COLLECTING, store.S_IN_TURN, approval.AWAITING):
        return RETRY, "чат сейчас в работе движка"
    return None


async def _local(eng, row: dict[str, Any]) -> Verdict | None:
    verdict = await eng._db(_base_verdict, row)
    if verdict is not None:
        return verdict
    chat_id, phone = await eng._db(_chat_of, row)
    if eng._autopilot_busy(chat_id, phone, int(row["deal_id"])):
        return RETRY, "сделку или чат ведёт авто-режим"
    return None


async def _remote(eng, row: dict[str, Any]) -> tuple[Verdict | None, int]:
    """Проверки по amo, вне общего замка: (решение или None «можно возвращать», воронка сделки сейчас)."""
    deal_id = int(row["deal_id"])
    lead = await amo_service.get_lead_full(deal_id, with_=("contacts",))
    if lead is None:
        return (FAILED, "amoCRM не ответила по сделке"), 0
    if not lead.get("id"):
        return (KEEP, "сделка в amoCRM не найдена"), 0
    if not dedup.is_open(lead):
        return (KEEP, "сделка закрыта"), 0
    pipeline = int(lead.get("pipeline_id") or 0)
    if not settings.pipeline_allowed(pipeline):
        if prompts.source() == prompts.SOURCE_STATIC:             # после рестарта словаря ещё нет: статический запас уже настоящего, решать по нему навсегда нельзя
            return (RETRY, "словарь воронок ИИ-сервиса ещё не получен"), 0
        return (KEEP, "воронку сделки ИИ не ведёт (режим не пускает или нет промпта)"), 0

    # Задачи: та же семантика, что у `ai_engine._task_guard` и подбора. Рубильник `AI_TASK_GUARD_DISABLED` сюда не относится.
    tasks = await ai_engine_tasks._get_open(deal_id)
    if tasks is None:
        return (FAILED, "amoCRM не ответила по задачам сделки"), 0
    user = filters.ai_user() or filters.DEFAULT_AI_USER
    if ai_engine_tasks.foreign_open_tasks(tasks, int(user["id"])):
        return (KEEP, "на сделке открыта задача сотрудника"), 0

    # Звонки клиенту, пока ИИ был выключен: те же правила, что у опроса звонков (`ai_engine_calls`).
    if calls.enabled():
        contacts = set(dedup.contact_ids_of(lead)) | calls.contact_ids_of_row(row)
        called = await calls.had_call(contacts, (_since_ts(row) or 0.0) - calls.CHECK_SLACK_S)
        if called is None:
            return (FAILED, "amoCRM не ответила по звонкам клиента"), 0
        if called:
            return (KEEP, "после снятия был звонок менеджера или разговор с клиентом"), 0

    # Тот же выбор сделки, что сделает первый ход после возврата (`ai_engine._process_collecting`): иначе возврат кончился бы алертом правил A, C, D и задачей.
    chat_id, phone = await eng._db(_chat_of, row)
    taken = await eng._db(intercept.is_taken, deal_id)
    forced = deal_id if taken or await eng._db(give.is_forced, deal_id) else None
    res = await dedup.resolve(chat_id, phone, known_deal_id=deal_id, force_deal_id=forced, any_pipeline_ids=frozenset({deal_id}) if taken else frozenset())
    if res.kind == dedup.K_ERROR:
        return (FAILED, "amoCRM не ответила на поиск сделок клиента"), 0
    if res.kind == dedup.K_SKIP:
        return (KEEP, "контакта нет в списке режима"), 0
    if res.kind == dedup.K_HUMAN:
        return (KEEP, f"у клиента несколько подходящих сделок или другая в работе (правило {res.rule})"), 0
    if res.kind != dedup.K_AI or res.deal is None or int(res.deal["id"]) != deal_id:
        return (KEEP, "сделка не находится по чату клиента"), 0
    return None, pipeline


def _wake_other_chats(deal_id: int) -> None:
    """Остальные чаты этой сделки, помеченные человеческими, пока она ждала возврата: снова ждут сообщения клиента. Чат хода-события уже переведён `_queue_event`."""
    with store._connect() as conn:
        keys = [r[0] for r in conn.execute("SELECT chat_key FROM chat_session WHERE deal_id=? AND state=?", (int(deal_id), store.S_HUMAN))]
    for key in keys:
        store.put_session(key, state=store.S_AI, next_at=None, pending_event=None, has_media=0, current_turn=None, first_batch=0)


async def _attempt(eng, row: dict[str, Any], mode_id: str) -> Verdict:
    verdict, pipeline = await _remote(eng, row)
    why = blocked_by(eng._now())
    if why or settings.active_mode_id() != mode_id:
        # Пока ходили в amo, выключили или сменили режим: проверки воронок и списка шли уже по другим настройкам, постоянный отказ по ним записывать нельзя.
        return STOP, why or "режим сменили, пока шла проверка"
    if verdict is not None:
        return verdict
    if not await eng._ai_is_up():
        return STOP, "ИИ-сервис не отвечает"
    deal_id = int(row["deal_id"])
    async with eng._get_lock():
        # Пока ходили в amo, могло измениться всё: режим, часы, владелец, ответ менеджера, «стоп», отметки.
        now = eng._now()
        why = blocked_by(now)
        if why or settings.active_mode_id() != mode_id:
            return STOP, why or "режим сменили, пока шла проверка"
        fresh = await eng._db(store.get_owner, deal_id)
        if (fresh is None or fresh["closed"] or fresh["owner"] != store.OWNER_HUMAN or fresh["reason"] != REASON
                or fresh["since"] != row["since"] or fresh["epoch"] != row["epoch"]):
            return SKIP, "запись о сделке изменилась, пока шла проверка"
        verdict = await _local(eng, fresh)
        if verdict is not None:
            return verdict
        if pipeline != fresh["pipeline_id"]:                      # перенесли, пока ИИ был выключен: от воронки зависит инициатива
            await eng._db(store.set_pipeline, deal_id, pipeline)
        code, body = await eng._return_dialog_locked(deal_id)
        if code != 200:
            detail = str(body.get("detail") or code)
            return (STOP if code == 503 else RETRY), f"возврат не прошёл: {detail}"
        await eng._db(_wake_other_chats, deal_id)
    return RETURNED, f"эпоха {body.get('epoch')}"


# =============================================================================================
# проход
# =============================================================================================

async def _record(eng, row: dict[str, Any], verdict: Verdict, out: dict[str, Any]) -> None:
    global _pending_report
    kind, note = verdict
    deal_id, now = int(row["deal_id"]), eng._now()
    if kind == KEEP:
        if not await eng._db(keep, row, now, note):
            return
        out["kept"] += 1
        _pending_report = True
    elif kind == RETURNED:
        await eng._db(log_returned, deal_id, now, note)
        out["returned"] += 1
        _pending_report = True
    elif kind in (RETRY, FAILED):
        await eng._db(defer, row, now + RETRY_S, note)
        out["retry"] += 1
    else:
        return
    logger.info("ai_engine_mode_return: сделка %s: %s (%s)", deal_id, kind, note)


async def _run(eng, out: dict[str, Any]) -> None:
    global _next_run_at
    now = eng._now()
    why = blocked_by(now)
    if why:
        out["skipped"] = why
        return
    mode_id = settings.active_mode_id()
    start = await eng._db(watch_from)
    postponed = await eng._db(deferred, now)
    due: list[dict[str, Any]] = []
    for row in await eng._db(waiting):
        old = _too_old(row, now, start)
        if old:
            await _record(eng, row, (KEEP, old), out)
        elif modes.mode_of(row) != mode_id:
            continue                                              # снята в другом режиме: в этом не возвращаем и не помечаем
        elif (int(row["deal_id"]), row["since"]) not in postponed:
            due.append(row)
    if not due:
        return
    if not await eng._ai_is_up():
        out["skipped"] = "ИИ-сервис не отвечает"
        return
    failures = 0
    for row in due:
        why = blocked_by(eng._now())
        if why or settings.active_mode_id() != mode_id:
            out["skipped"] = f"проход прерван: {why or 'режим сменили'}"
            out["more"] = False
            return
        networked = False
        try:
            verdict = await _local(eng, row)
            if verdict is None:
                if out["examined"] >= PER_RUN:
                    out["more"] = True                            # остальные в следующем проходе; отказы по базе разбираются все сразу
                    continue
                out["examined"] += 1
                networked = True
                verdict = await _attempt(eng, row, mode_id)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - сбой одной сделки не должен остановить остальные
            logger.exception("ai_engine_mode_return: сбой проверки сделки %s", row.get("deal_id"))
            verdict = (FAILED, "исключение при проверке (см. лог движка)")
        if verdict[0] == STOP:
            out["skipped"] = verdict[1]
            out["more"] = False
            return
        await _record(eng, row, verdict, out)
        if networked:
            failures = failures + 1 if verdict[0] == FAILED else 0
        if failures >= 2:
            out["skipped"] = f"две неудачи подряд (amoCRM), пауза {RETRY_S // 60} мин"
            out["more"] = False
            _next_run_at = max(_next_run_at, eng._now() + RETRY_S)
            return


async def report(eng) -> None:
    """Одна строка итога в чат ОП по накопленным исходам, если что-то вернули. Без пингов и имён. Не бросает."""
    global _pending_report
    _pending_report = False
    try:
        returned, kept, last = await eng._db(_take_unreported, settings.active_mode_id())
        if not returned:
            return
        test = " (тест)" if settings.effective_mode() == "test" else ""
        await eng._alert("mode_return", [f"ИИ-менеджер{test}: после включения режима сделок возвращено ИИ: {returned}, оставлено менеджерам: {kept}."],
                         dedupe=f"mode_return:{last}", ping=False)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_mode_return: итог возврата в чат ОП не ушёл")


async def run_once(eng) -> dict[str, Any]:
    """Один проход. Не бросает (кроме отмены). Сводка: `skipped`, `examined` (дошли до amo), `returned`, `kept`, `retry`, `more` (остались непроверенные)."""
    _state["last_run_at"] = eng._now()
    out: dict[str, Any] = {"skipped": None, "examined": 0, "returned": 0, "kept": 0, "retry": 0, "more": False}
    try:
        await _run(eng, out)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_mode_return: проход упал")
        out["skipped"] = "проход упал (см. лог движка)"
    summary = f"возвращено {out['returned']}, оставлено людям {out['kept']}, отложено {out['retry']}" + (f", {out['skipped']}" if out["skipped"] else "")
    _state["last_result"] = summary
    if out["returned"] or out["kept"] or out["retry"]:
        logger.info("ai_engine_mode_return: проход: %s", summary)
    if not out["more"]:
        await report(eng)                                          # очередь разобрана (или проход прерван): итог по накопленному
    return out


async def tick(eng, now: float) -> None:
    """Из `ai_engine.tick_once` каждый проход таймеров: дёшево. Раз в `BEAT_S` отмечает, что движок на связи; раз в `INTERVAL_S`, если ничего не мешает (`blocked_by`),
    запускает проход фоновой задачей. Не бросает."""
    global _task, _next_run_at, _beat_at, _pending_report
    try:
        if _beat_at is None or now - _beat_at >= BEAT_S:
            _beat_at = now
            await eng._db(beat, now)
        if (_task is not None and not _task.done()) or now < _next_run_at:
            return
        if blocked_by(now) is not None:
            if _pending_report:                                    # окно закрылось или режим выключили посреди разбора: итог не ждёт следующего дня
                _pending_report = False
                eng._spawn(report(eng))
            return
        _next_run_at = now + INTERVAL_S
        if _pending_report or await eng._db(waiting):              # обычное состояние «возвращать нечего»: фоновую задачу не заводим
            _task = eng._spawn(run_once(eng))
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_mode_return: не запустил проход")


# =============================================================================================
# срез состояния
# =============================================================================================

def status_block(now: float) -> dict[str, Any]:
    """Блок `mode_return` среза `GET /`: `enabled`, `waiting` (ждут решения), `waiting_other_mode` (из них сняты в другом режиме, чем активный), `returned_24h`,
    `kept_24h`, `blocked_by` (почему сейчас не работает), `watch_from` (снятые раньше не возвращаются), `last_run_at`, `last_result`."""
    rows = waiting()
    mode_id = settings.active_mode_id()
    day = counts_since(now - 86400)
    start, last = watch_from(), _state["last_run_at"]
    return {
        "enabled": bool(cfg.ENABLED and not cfg.MODE_RETURN_DISABLED),
        "waiting": len(rows),
        "waiting_other_mode": 0 if mode_id == "off" else sum(1 for r in rows if modes.mode_of(r) != mode_id),
        "returned_24h": day.get(RETURNED, 0),
        "kept_24h": day.get(KEEP, 0),
        "blocked_by": blocked_by(now),
        "watch_from": None if start is None else _iso(start),
        "last_run_at": None if last is None else _iso(last),
        "last_result": _state["last_result"],
    }


def reset_for_tests() -> None:
    global _task, _next_run_at, _beat_at, _pending_report
    _task, _next_run_at, _beat_at, _pending_report = None, 0.0, None, False
    _state.update({"last_run_at": None, "last_result": ""})
