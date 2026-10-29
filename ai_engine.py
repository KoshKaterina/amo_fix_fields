"""Движок для ИИ-менеджера: шестой потребитель вебхука Wazzup, таймеры, владелец сделки, ходы.

Сторона `amo_fix_fields` контракта `docs/ENGINE_CONTRACT.md` (репозиторий Sanskrit-AI-Manager, версия 2).
Что делает ИИ-сервис и что делает движок - в этом контракте; здесь только реализация движка.

Что здесь происходит, по порядку:

  1. `on_wazzup(payload)` - врезка в вебхук. Синхронная и мгновенная: кладёт тело в очередь и возвращает.
     Вебхук Wazzup обязан получить 200 сразу, а разбор идёт в фоновом цикле `_ingest_loop`.
  2. Сообщение клиента попадает в `inbox` и двигает таймер чата (`chat_session`). Сообщение менеджера
     (автор - ФИО или `Phone`) снимает таймер и отдаёт сделку человеку; эхо автоматики и самого ИИ
     (`Admin`, пусто) за менеджера не считается. Любое не текстовое сообщение - сразу человеку.
  3. `_tick_loop` раз в секунду берёт чаты, у которых пришёл срок: ищет сделку и выбирает её по
     правилам A, C, D (`ai_engine_dedup`), ставит `owner = ai` и кладёт ход в очередь.
  4. `_dispatch_loop` отправляет ходы сервису (`POST /v1/turn`, паузы 1, 5, 25 с, дальше ход лежит в
     очереди `backlog`), `_health_loop` раз в 15 секунд опрашивает `/v1/health`.
  5. Сервис зовёт `/internal/ai/v1/permit` перед каждой отправкой клиенту и `/result` по итогу хода
     (`ai_engine_api.py`). Итог двигает владельца сделки и назначает следующий ход.

⚠️ Всё состояние - в sqlite (`ai_engine_store`), память процесса только кэширует. Рестарт контейнера
ничего не теряет, кроме тел вебхуков, которые пришли в окно 502 при пересборке.

⚠️ Любая ошибка здесь не должна дойти до соседних потребителей вебхука: врезки в `webhooks.py`
обёрнуты `try/except`, а циклы ловят исключения на каждой итерации и пишут в лог.

⚠️ Правило B (послепродажный разговор), `returning` и `client_memory` из первой версии вынесены:
в ходе они всегда `null`.
"""
import asyncio
import datetime
import html
import json
import logging
import re
import time
from typing import Any

import ai_engine_client as client
import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_return
import ai_engine_tasks
import ai_engine_settings as settings
import ai_engine_store as store
import alerts
import amo_service
import telegram_bot
import wazzup_sla
from api import BASE_URL
from shared.phones import digits_only
from tg_recipients import NOTIFY_CHAT_ID, NOTIFY_THREAD_ID, mentions_for

logger = logging.getLogger("uvicorn")

# Слова для алерта «клиент прислал ...»: человеку нужно «картинку», а не `image`.
MEDIA_WORDS = {
    "image": "картинку", "video": "видео", "audio": "голосовое", "document": "файл", "vcard": "контакт",
    "geo": "геометку", "sticker": "стикер", "missing_call": "пропущенный звонок",
}

_queue: asyncio.Queue | None = None
_tasks: list[asyncio.Task] = []
_lock: asyncio.Lock | None = None
_bg: set[asyncio.Task] = set()

_ai_ok_at: float | None = None          # когда сервис последний раз ответил на /health
_ai_down_since: float | None = None
_ai_down_alerted = False
_off_alerted = False
_alert_seen: dict[str, float] = {}      # дедуп алертов в памяти: ключ -> когда слали
_rule_c_seen: dict[int, float] = {}     # сделка -> когда проверяли правило C
_rule_c_reads: list[float] = []         # когда читали сделки под правило C (для потолка в минуту)
RULE_C_READS_PER_MIN = 30
_last_housekeeping = 0.0
_last_expire = 0.0
_last_off_sweep = 0.0


def _now() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _db(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)


def _get_lock() -> asyncio.Lock:
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


def is_enabled() -> bool:
    return cfg.ENABLED


# =============================================================================================
# жизненный цикл
# =============================================================================================

async def init() -> None:
    global _queue, _tasks
    if not cfg.ENABLED:
        logger.info("ai_engine: ВЫКЛЮЧЕН (AI_ENGINE_ENABLED не 1) - ни цикла, ни базы, ни опроса панели")
        return
    await _db(store.init)
    ai_engine_return.install(return_dialog)
    settings.start()
    _queue = asyncio.Queue(maxsize=2000)
    _tasks = [
        asyncio.create_task(_guarded_loop(_ingest_loop, "приём вебхука")),
        asyncio.create_task(_guarded_loop(_tick_loop, "таймеры")),
        asyncio.create_task(_guarded_loop(_dispatch_loop, "отправка ходов")),
        asyncio.create_task(_guarded_loop(_health_loop, "опрос здоровья ИИ-сервиса")),
    ]
    logger.info(
        "ai_engine: включён; режим берётся из панели (пока off), каналов %s, сервис %s",
        len(cfg.CHANNELS), cfg.SERVICE_URL or "не задан",
    )


async def shutdown() -> None:
    global _tasks
    await settings.stop()
    for task in _tasks:
        task.cancel()
    for task in _tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: цикл завершился с ошибкой")
    _tasks = []
    for task in list(_bg):
        task.cancel()
    logger.info("ai_engine: остановлен")


async def _guarded_loop(loop_fn, what: str) -> None:
    """Цикл, который не умирает от одной ошибки: упавший цикл молчит, а молчание выглядит как «всё в порядке»."""
    while True:
        try:
            await loop_fn()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: цикл «%s» упал, перезапускаю через 5 с", what)
            await asyncio.sleep(5)


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg.add(task)
    task.add_done_callback(_bg.discard)


# =============================================================================================
# вход: вебхук Wazzup и вебхук сделки
# =============================================================================================

def on_wazzup(payload: Any) -> None:
    """Врезка в `POST /wazzup/{secret}`. Не блокирует и не бросает."""
    if not cfg.ENABLED or _queue is None or not isinstance(payload, dict):
        return
    if not isinstance(payload.get("messages"), list):
        return
    if settings.effective_mode() == "off":
        return
    try:
        _queue.put_nowait(("wazzup", payload))
    except asyncio.QueueFull:
        logger.error("ai_engine: очередь приёма полна - тело вебхука отброшено")


def on_lead_change(lead_id: Any, pipeline_id: Any, status_id: Any, nested: Any) -> None:
    """Врезка в `POST /lead_change`. Из тела берёт только то, что нужно, и кладёт в очередь."""
    if not cfg.ENABLED or _queue is None:
        return
    try:
        lead_id = int(lead_id)
    except (TypeError, ValueError):
        return
    if settings.effective_mode() == "off":
        return
    try:
        event = extract_lead_event(lead_id, pipeline_id, status_id, nested)
        _queue.put_nowait(("lead", event))
    except asyncio.QueueFull:
        logger.error("ai_engine: очередь приёма полна - событие сделки %s отброшено", lead_id)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: не разобрал событие сделки %s", lead_id)


def extract_lead_event(lead_id: int, pipeline_id: Any, status_id: Any, nested: Any) -> dict[str, Any]:
    """Из тела вебхука amo (вложенный словарь) достаёт: создана ли сделка, менялась ли цена и поля заказа,
    стал ли «Статус оплаты» равен «Оплачен»."""
    leads = (nested or {}).get("leads") or {} if isinstance(nested, dict) else {}
    added = bool(leads.get("add"))
    upd = (leads.get("update") or {}).get("0") or {}
    add0 = (leads.get("add") or {}).get("0") or {}
    body = upd or add0
    paid = False
    order_touched = False
    fields = body.get("custom_fields") or {}
    for info in fields.values() if isinstance(fields, dict) else []:
        try:
            fid = int((info or {}).get("id"))
        except (TypeError, ValueError):
            continue
        if fid in (576689, 576697, 576703):
            order_touched = True
        if fid == cfg.FIELD_PAYMENT_STATE:
            values = (info or {}).get("values") or {}
            first = values.get("0") if isinstance(values, dict) else None
            first = first or {}
            enum = first.get("enum") or first.get("enum_id")
            if str(enum or "") == str(cfg.ENUM_PAYMENT_STATE_PAID) or str(first.get("value") or "").strip() == "Оплачен":
                paid = True
    return {
        "lead_id": lead_id,
        "pipeline_id": _int_or_none(pipeline_id),
        "status_id": _int_or_none(status_id),
        "added": added,
        "paid": paid,
        "work_touched": order_touched,
    }


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _ingest_loop() -> None:
    while True:
        kind, item = await _queue.get()
        try:
            if kind == "wazzup":
                async with _get_lock():
                    await _handle_payload(item)
            elif kind == "lead":
                await _handle_lead_event(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: ошибка разбора входящего (%s)", kind)


# =============================================================================================
# разбор сообщений
# =============================================================================================

async def _handle_payload(payload: dict) -> None:
    for m in payload.get("messages") or []:
        if not isinstance(m, dict):
            continue
        try:
            await _handle_message(m)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: не разобрал сообщение Wazzup")


def chat_key_of(channel_id: str, chat_id: str) -> str:
    return f"{channel_id}:{chat_id}"


def is_robot_author(author: Any) -> bool:
    """Автоматика, а не человек: `Admin` (сейлсботы amo и сам ИИ через API), `bot`, пусто. Правило одно с
    авто-режимом (`autopilot.is_robot_echo`); импорт отложен, чтобы этот модуль не тянул авто-режим при загрузке."""
    try:
        import autopilot
        return autopilot.is_robot_echo(author)
    except Exception:  # noqa: BLE001
        return str(author or "").strip().lower() in {"admin", "bot", ""}


def _autopilot_busy(chat_id: str, phone: str, deal_id: int | None = None) -> bool:
    """Чат или сделку уже ведёт авто-режим (заказ с сайта): ИИ туда не входит, оба владельца сразу нельзя."""
    try:
        import autopilot
        import autopilot_store
        digits = [d for d in (digits_only(chat_id), digits_only(phone)) if d]
        if digits and autopilot.watches_chat(digits):
            return True
        if deal_id is not None:
            busy = (autopilot_store.PHASE_LAUNCHING, autopilot_store.PHASE_DELIVERY, autopilot_store.PHASE_REPLY,
                    autopilot_store.PHASE_PAYMENT, autopilot_store.PHASE_SLEEPING)
            return any(r.get("phase") in busy for r in autopilot_store.list_for_lead(int(deal_id)))
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: не смог спросить авто-режим, считаю, что он не занят")
    return False


async def _handle_message(m: dict) -> None:
    channel_id = str(m.get("channelId") or "")
    chat_id = str(m.get("chatId") or "")
    if not chat_id or channel_id not in cfg.CHANNELS:
        return
    chat_key = chat_key_of(channel_id, chat_id)
    # Направление считает SLA-таймер: правило одно на весь контур (isEcho, запасной признак - статус).
    if wazzup_sla._is_outbound(m):
        await _on_outbound(chat_key, m)
    else:
        await _on_inbound(chat_key, channel_id, chat_id, m)


async def _on_outbound(chat_key: str, m: dict) -> None:
    """Исходящее в чат. Автоматика и ИИ (`Admin`, пусто) - не менеджер и ничего не меняют. Человек (ФИО или
    `Phone`) в чате, который мы ведём или ждём, забирает диалог."""
    if is_robot_author(m.get("authorName")):
        return
    s = await _db(store.get_session, chat_key)
    if s is None or s["state"] in (store.S_HUMAN, store.S_SKIP, store.S_RELEASED):
        return
    logger.info("ai_engine: менеджер написал в чат %s - беру диалог у ИИ", chat_key)
    await _to_human(s, "manager_message", alert=None)


async def _on_inbound(chat_key: str, channel_id: str, chat_id: str, m: dict) -> None:
    now = _now()
    contact = m.get("contact") if isinstance(m.get("contact"), dict) else {}
    phone = str((contact or {}).get("phone") or "")
    message_id = str(m.get("messageId") or "")
    mtype = str(m.get("type") or "text").strip().lower()
    text = str(m.get("text") or "")
    is_media = mtype != "text" or not text.strip()
    if not message_id:
        message_id = f"noid:{chat_key}:{m.get('dateTime')}:{abs(hash(text))}"

    s = await _db(store.get_session, chat_key)
    if s is not None:
        if s["state"] in (store.S_RELEASED,):
            return
        if s["state"] == store.S_HUMAN:
            # Человеческий чат без привязки к сделке через сутки разбирается заново, с привязкой живёт, пока жива сделка.
            if s["deal_id"] is None and s["skip_until"] and s["skip_until"] < now:
                s = None
            else:
                return
        elif s["state"] == store.S_SKIP:
            if s["skip_until"] and s["skip_until"] > now:
                return
            s = None
    if s is None:
        if not settings.phone_may_match(phone, chat_id):
            return
        if _autopilot_busy(chat_id, phone):
            return

    timers = settings.timers()
    sent_at = _sent_at(m, now)
    seq = await _db(store.add_inbox, message_id, chat_key, "" if is_media else text, mtype if is_media else "text",
                    sent_at, now)
    if seq is None:
        return                                          # повтор вебхука

    if s is None:
        s = await _db(
            store.put_session, chat_key, channel_id=channel_id, chat_id=chat_id,
            chat_type=str(m.get("chatType") or ""), phone=phone, contact_name=str((contact or {}).get("name") or ""),
            state=store.S_COLLECTING, deal_id=None, batch_start=now, first_batch=1, last_client_at=now,
            next_at=now + timers["first_reply_wait_s"], resolve_deadline=now + timers["deal_resolve_timeout_s"],
            has_media=0, last_message_id=message_id,
        )
    else:
        fields: dict[str, Any] = {"last_client_at": now, "last_message_id": message_id}
        if s["state"] == store.S_AI:
            # Следующая пачка при ИИ-владельце: ждём только тишину, первое ожидание давно прошло.
            fields.update(state=store.S_COLLECTING, batch_start=now, first_batch=0)
        if is_media:
            fields["has_media"] = 1
        s = await _db(store.put_session, chat_key, **fields)

    if is_media:
        owner = await _db(store.get_owner, s["deal_id"]) if s["deal_id"] is not None else None
        if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"]:
            await _handoff_media(s, owner, mtype)
        else:
            await _db(store.put_session, chat_key, has_media=1, state=store.S_COLLECTING, next_at=now)
        return

    if s["state"] == store.S_COLLECTING:
        next_at = next_turn_at(
            first=bool(s["first_batch"]), batch_start=s["batch_start"], last=now, timers=timers,
        )
        await _db(store.put_session, chat_key, next_at=next_at if not s["has_media"] else now)


def _sent_at(m: dict, fallback_ts: float) -> str:
    raw = str(m.get("dateTime") or "").strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.datetime.strptime(raw, fmt).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            continue
    return _iso(fallback_ts)


def next_turn_at(*, first: bool, batch_start: float, last: float, timers: dict[str, int]) -> float:
    """Момент хода (контракт, «Как движок готовит ход», п. 4).

    Первая пачка: `max(T0 + first_reply_wait, last + debounce)`, но не позже `T0 + first_reply_wait + cap`.
    Дальше: `last + debounce`, но не позже `начало пачки + cap` (первого ожидания у них нет)."""
    debounce, cap = timers["debounce_s"], timers["debounce_cap_s"]
    if first:
        wait = timers["first_reply_wait_s"]
        return min(max(batch_start + wait, last + debounce), batch_start + wait + cap)
    return min(last + debounce, batch_start + cap)


# =============================================================================================
# таймеры: чат дозрел до решения
# =============================================================================================

async def _tick_loop() -> None:
    while True:
        await asyncio.sleep(cfg.TICK_S)
        try:
            await tick_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: ошибка цикла таймеров")


async def tick_once() -> None:
    """Один проход таймеров: чаты, у которых пришёл срок; раз в 10 с проверка «режим выключили»; раз в 30 с
    ходы без итога; раз в час уборка. Вынесено из цикла, чтобы тесты гоняли ровно тот же код."""
    global _last_housekeeping, _last_expire, _last_off_sweep
    now = _now()
    for s in await _db(store.due_sessions, now):
        try:
            await _process_collecting(s)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: ошибка обработки чата %s", s.get("chat_key"))
            await _db(store.put_session, s["chat_key"], next_at=_now() + cfg.RESOLVE_RETRY_S)
    if now - _last_off_sweep >= 10:
        _last_off_sweep = now
        async with _get_lock():
            await _sweep_if_off()
    if now - _last_expire >= 30:
        _last_expire = now
        async with _get_lock():
            await _expire_turns()
    if now - _last_housekeeping >= 3600:
        _last_housekeeping = now
        await _db(store.prune, cfg.RETENTION_DAYS * 86400)


async def _process_collecting(s: dict) -> None:
    """Чат дозрел. Сначала сеть (поиск сделок в amo) БЕЗ общего замка: пока amo думает, должны проходить
    итоги ходов и вебхуки (итог, не принятый за 10 секунд, сервис считает недоставленным). Решение принимается
    под замком и по свежей строке чата: если пока мы ходили в amo, чат сменил состояние, это решение устарело."""
    chat_key = s["chat_key"]
    mode = settings.effective_mode()
    if mode == "off":
        async with _get_lock():
            await _db(store.drop_session, chat_key)
        return
    owner = await _db(store.get_owner, s["deal_id"]) if s["deal_id"] is not None else None
    known = int(s["deal_id"]) if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"] else None
    res = await dedup.resolve(s["chat_id"], s["phone"], known_deal_id=known)
    async with _get_lock():
        fresh = await _db(store.get_session, chat_key)
        if fresh is None or fresh["state"] != store.S_COLLECTING or (fresh["next_at"] or 0) > _now():
            return
        await _apply_resolution(fresh, res, mode)


async def _apply_resolution(s: dict, res: dedup.Resolution, mode: str) -> None:
    chat_key = s["chat_key"]
    now = _now()
    owner = await _db(store.get_owner, s["deal_id"]) if s["deal_id"] is not None else None
    if owner is not None and owner["owner"] == store.OWNER_HUMAN:
        await _db(store.put_session, chat_key, state=store.S_HUMAN, next_at=None)
        await _db(store.discard_pending, chat_key)
        return

    if res.kind == dedup.K_ERROR:
        if now < (s["resolve_deadline"] or 0):
            await _db(store.put_session, chat_key, next_at=now + cfg.RESOLVE_RETRY_S)
        else:
            await _give_up_resolving(s, "amoCRM не отвечает, сделку найти не удалось")
        return
    if res.kind == dedup.K_NONE:
        if now < (s["resolve_deadline"] or 0):
            await _db(store.put_session, chat_key, next_at=now + cfg.RESOLVE_RETRY_S)
        else:
            await _give_up_resolving(s, "сделка клиента так и не появилась")
        return
    if res.kind == dedup.K_SKIP:
        await _db(store.put_session, chat_key, state=store.S_SKIP, skip_until=now + cfg.SKIP_TTL_S, next_at=None)
        await _db(store.discard_pending, chat_key)
        return
    if res.kind == dedup.K_GONE:
        await _deal_closed(int(s["deal_id"]), "сделка закрыта или ушла из воронок режима")
        return
    if res.kind == dedup.K_HUMAN:
        await _handoff_by_rule(s, res, owner)
        return

    # K_AI
    deal = res.deal
    deal_id = int(deal["id"])
    chat = _chat_dict(s)
    if owner is None:
        owner = await _db(store.get_owner, deal_id)
    if owner is not None and owner["owner"] == store.OWNER_HUMAN:
        await _db(store.put_session, chat_key, state=store.S_HUMAN, deal_id=deal_id, next_at=None)
        await _db(store.discard_pending, chat_key)
        return
    if owner is not None and owner["owner"] == store.OWNER_ENGINE:
        await _db(store.put_session, chat_key, state=store.S_RELEASED, deal_id=deal_id, next_at=None)
        await _db(store.discard_pending, chat_key)
        return
    if _autopilot_busy(s["chat_id"], s["phone"], deal_id):
        logger.info("ai_engine: сделку %s ведёт авто-режим - ИИ не входит", deal_id)
        await _db(store.put_session, chat_key, state=store.S_SKIP, deal_id=deal_id,
                  skip_until=now + cfg.SKIP_TTL_S, next_at=None)
        await _db(store.discard_pending, chat_key)
        return

    if s["has_media"]:
        # Медиа от клиента: ИИ не получает ничего, сделка сразу человеку (даже если ИИ ещё не владел).
        media_types = [r["msg_type"] for r in await _db(store.pending_inbox, chat_key) if r["msg_type"] != "text"]
        owner = await _db(
            store.set_owner, deal_id, store.OWNER_HUMAN, reason="media", chat=chat,
            pipeline_id=int(deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(deal), mode=mode,
        )
        await _db(store.put_session, chat_key, state=store.S_HUMAN, deal_id=deal_id, next_at=None, has_media=0)
        await _db(store.discard_pending, chat_key)
        await _alert("media", _media_lines(media_types, s, deal_id), dedupe=f"media:{chat_key}", return_deal=deal_id)
        _contact_task(deal_id, "media", _media_lines(media_types, s, deal_id))
        return

    if not await _ai_is_up():
        if owner is None:
            await _alert("ai_down_entry", [
                "Клиент написал, а ИИ-сервис не отвечает.",
                _client_line(s),
                "Диалог остаётся у менеджеров, как и до ИИ.",
                _link(deal_id),
            ], dedupe=f"ai_down_entry:{chat_key}")
            await _db(store.put_session, chat_key, state=store.S_SKIP, deal_id=deal_id,
                      skip_until=now + 300, next_at=None)
            await _db(store.discard_pending, chat_key)
        else:
            await _db(store.put_session, chat_key, next_at=now + cfg.HEALTH_POLL_S)
        return

    owner = await _db(
        store.set_owner, deal_id, store.OWNER_AI, reason="client_message", chat=chat,
        pipeline_id=int(deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(deal), mode=mode,
    )
    await _db(store.put_session, chat_key, deal_id=deal_id)
    s = await _db(store.get_session, chat_key)
    await _enqueue_turn(s, owner, mode)


def _chat_dict(s: dict) -> dict[str, Any]:
    return {"chat_id": s["chat_id"], "chat_type": s["chat_type"], "channel_id": s["channel_id"],
            "chat_key": s["chat_key"]}


async def _give_up_resolving(s: dict, why: str) -> None:
    """Сделку не нашли за `deal_resolve_timeout_s`. Для ограниченных режимов (тест, пилот) зовём человека: тот,
    кто тестирует, должен видеть, почему ИИ молчит. Для открытого прода молчим: обращение без сделки
    amo разбирает сам, а сторож розничных лидов зовёт людей там, где открытая сделка лежит в чужой воронке."""
    await _db(store.put_session, s["chat_key"], state=store.S_SKIP, skip_until=_now() + cfg.SKIP_TTL_S, next_at=None)
    await _db(store.discard_pending, s["chat_key"])
    # Сделки нет, поэтому контакт по id неизвестен: чат считается «из списка» только по телефону или chat_id
    # (WhatsApp: chat_id и есть телефон). Иначе алерт получал бы КАЖДЫЙ живой клиент канала, у которого нет
    # сделки в воронке режима (боевой тест 28.10.2026: клиенты «ТГ ОП» с сделками в «ОП розница»).
    if settings.contacts_restricted() and settings.contact_allowed([], [s["phone"], s["chat_id"]]):
        await _alert("deal_not_found", [f"Клиент написал, но {why}.", _client_line(s)],
                     dedupe=f"deal_not_found:{s['chat_key']}")


# =============================================================================================
# формирование хода
# =============================================================================================

async def _enqueue_turn(s: dict, owner: dict, mode: str, *, event: dict | None = None) -> None:
    chat_key = s["chat_key"]
    pending = [r for r in await _db(store.pending_inbox, chat_key) if r["msg_type"] == "text" and r["text"].strip()]
    pending_event = None
    if event is None and s.get("pending_event"):
        try:
            pending_event = json.loads(s["pending_event"])
        except ValueError:
            pending_event = None
        event = pending_event
    if not pending and event is None:
        await _db(store.put_session, chat_key, state=store.S_AI, next_at=None)
        return

    deal_id = int(owner["deal_id"])
    first = await _db(store.turn_count, deal_id) == 0
    if event is not None:
        reason = "event"
    elif s["fails"] > 0:
        reason = "retry"
    else:
        reason = "first_message" if first else "client_message"
    up_to_seq = await _db(store.max_seq, chat_key)
    up_to_id = pending[-1]["message_id"] if pending else s["last_message_id"]
    window = settings.send_window()

    def build(turn_id: int) -> dict[str, Any]:
        body: dict[str, Any] = {
            "turn_id": turn_id, "deal_id": deal_id, "epoch": int(owner["epoch"]), "mode": mode, "reason": reason,
            "chat": {"chat_id": s["chat_id"], "chat_type": s["chat_type"], "channel_id": s["channel_id"]},
            "messages": [{"message_id": r["message_id"], "text": r["text"], "sent_at": r["sent_at"]} for r in pending],
            "up_to_message_id": up_to_id, "now": _iso(_now()), "send_window": window,
            "returning": None, "client_memory": None,
        }
        if event is not None:
            body["event"] = {"type": event["type"], "at": event["at"]}
        return body

    turn = await _db(
        store.create_turn, deal_id=deal_id, epoch=int(owner["epoch"]), chat_key=chat_key, reason=reason,
        payload_fn=build, message_seqs=[r["seq"] for r in pending], up_to_seq=up_to_seq,
    )
    await _db(store.put_session, chat_key, state=store.S_IN_TURN, current_turn=turn["turn_id"], next_at=None,
              has_media=0, pending_event=None, deal_id=deal_id)
    logger.info("ai_engine: ход %s по сделке %s (%s, сообщений %s)", turn["turn_id"], deal_id, reason, len(pending))


async def _queue_event(owner: dict, event_type: str) -> None:
    """Поставить служебное событие: оно идёт отдельным ходом, как только предыдущий закончился."""
    s = await _db(store.session_for_deal, int(owner["deal_id"]))
    if s is None:
        # Рестарт или чистка унесли строку чата - собираем её из владельца сделки.
        if not owner.get("chat_id") or not owner.get("channel_id"):
            return
        s = await _db(
            store.put_session, owner["chat_key"] or chat_key_of(owner["channel_id"], owner["chat_id"]),
            channel_id=owner["channel_id"], chat_id=owner["chat_id"], chat_type=owner.get("chat_type") or "",
            state=store.S_AI, deal_id=int(owner["deal_id"]), first_batch=0,
        )
    event = json.dumps({"type": event_type, "at": _iso(_now())})
    fields: dict[str, Any] = {"pending_event": event}
    if s["state"] != store.S_IN_TURN:
        # Идущий ход не прерываем: событие дождётся его итога (`_after_turn`). Во всех остальных состояниях
        # (в том числе у человека, когда диалог возвращают) чат сразу дозревает до хода.
        fields.update(state=store.S_COLLECTING, next_at=_now(), first_batch=0, has_media=0)
        if s["state"] != store.S_COLLECTING:
            fields["batch_start"] = _now()
    await _db(store.put_session, s["chat_key"], **fields)


# =============================================================================================
# отправка ходов сервису и здоровье
# =============================================================================================

async def _dispatch_loop() -> None:
    while True:
        await asyncio.sleep(cfg.TICK_S)
        try:
            await dispatch_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: ошибка цикла отправки ходов")


async def dispatch_once() -> None:
    for turn in await _db(store.turns_to_send, _now()):
        await _send_turn(turn)


async def _send_turn(turn: dict) -> None:
    deal_id = int(turn["deal_id"])
    owner = await _db(store.get_owner, deal_id)
    if owner is None or owner["owner"] != store.OWNER_AI or owner["closed"] or owner["epoch"] != turn["epoch"]:
        # Владелец сменился, пока ход ждал: запоздавший ход не отправляем (его всё равно отбил бы permit).
        await _db(store.update_turn, turn["turn_id"], state=store.T_CANCELLED, finished_at=_now(), outcome="stale")
        return
    result = await client.post_turn(json.loads(turn["payload"]))
    if result.kind == client.SENT:
        await _db(store.update_turn, turn["turn_id"], state=store.T_ACCEPTED, accepted_at=_now())
        return
    if result.kind == client.RETRY:
        attempts = turn["send_attempts"] + 1
        if attempts <= len(cfg.SEND_RETRY_DELAYS_S):
            await _db(store.update_turn, turn["turn_id"], send_attempts=attempts,
                      next_try_at=_now() + cfg.SEND_RETRY_DELAYS_S[attempts - 1])
        else:
            await _db(store.update_turn, turn["turn_id"], send_attempts=attempts, state=store.T_BACKLOG, next_try_at=None)
            logger.error("ai_engine: ход %s не доставлен после повторов (%s), лежит в очереди backlog", turn["turn_id"],
                         result.detail)
        return
    # Отказ: токен, версия контракта, потолок режима сервиса. Повтор не поможет, нужен человек.
    logger.error("ai_engine: сервис отклонил ход %s: HTTP %s %s", turn["turn_id"], result.status, result.detail)
    async with _get_lock():
        s = await _db(store.get_session, turn["chat_key"])
        await _db(store.update_turn, turn["turn_id"], state=store.T_CANCELLED, finished_at=_now(),
                  outcome="refused", outcome_reason=f"HTTP {result.status}")
        if s is not None:
            await _to_human(s, f"service_refused_{result.status}", alert=[
                "ИИ-сервис не принял ход (настройка, а не сбой сети), диалог передан менеджеру.",
                f"Причина: HTTP {result.status}.",
                _client_line(s), _link(deal_id),
            ])


async def _health_loop() -> None:
    while True:
        try:
            await _probe_health()
            async with _get_lock():
                await _on_health_state()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: ошибка опроса здоровья")
        await asyncio.sleep(cfg.HEALTH_POLL_S)


async def _probe_health() -> bool:
    global _ai_ok_at, _ai_down_since
    h = await client.get_health()
    now = _now()
    if h is not None:
        _ai_ok_at = now
        _ai_down_since = None
        return True
    if _ai_down_since is None:
        _ai_down_since = now
    return False


async def _ai_is_up() -> bool:
    """Свежий ли ответ `/health`. Перед выбором `owner = ai` проверяем сами, не ждём цикла."""
    if _ai_ok_at is not None and _now() - _ai_ok_at <= max(3 * cfg.HEALTH_POLL_S, 45):
        return True
    return await _probe_health()


async def _on_health_state() -> None:
    """Последствия состояния сервиса: упал надолго - сделки человеку; ожил - дозаслать очередь."""
    global _ai_down_alerted
    now = _now()
    if _ai_down_since is None:
        _ai_down_alerted = False
        for t in await _db(store.turns_in_state, store.T_BACKLOG):
            owner = await _db(store.get_owner, int(t["deal_id"]))
            if owner and owner["owner"] == store.OWNER_AI and not owner["closed"] and owner["epoch"] == t["epoch"]:
                await _db(store.update_turn, t["turn_id"], state=store.T_QUEUED, send_attempts=0, next_try_at=now)
            else:
                await _db(store.update_turn, t["turn_id"], state=store.T_CANCELLED, finished_at=now, outcome="stale")
        return
    down_for = now - (_ai_ok_at if _ai_ok_at is not None else _ai_down_since)
    if down_for < settings.timers()["ai_down_release_s"]:
        return
    owned = await _db(store.list_owned, store.OWNER_AI)
    if not owned:
        return
    for row in owned:
        s = await _db(store.session_for_deal, int(row["deal_id"]))
        await _to_human(s, "ai_down", alert=None, deal_id=int(row["deal_id"]))
    if not _ai_down_alerted:
        _ai_down_alerted = True
        await _alert("ai_down", [
            f"ИИ-сервис не отвечает дольше {int(down_for // 60)} мин.",
            f"Сделок возвращено менеджерам: {len(owned)}. Клиентам ИИ пока не пишет.",
        ], dedupe="ai_down")


async def _sweep_if_off() -> None:
    """Режим выключили в панели (или настройки протухли надолго): сделки ИИ уходят людям, молчать им нельзя."""
    global _off_alerted
    if not cfg.ENABLED:
        return
    mode = settings.effective_mode()
    if mode != "off":
        _off_alerted = False
        return
    age = settings.age_s()
    explicit = settings.is_fresh()
    stale_long = age is not None and age > 3 * cfg.SETTINGS_STALE_S
    if not (explicit or stale_long):
        return
    owned = await _db(store.list_owned, store.OWNER_AI)
    if not owned:
        return
    for row in owned:
        s = await _db(store.session_for_deal, int(row["deal_id"]))
        await _to_human(s, "mode_off", alert=None, deal_id=int(row["deal_id"]))
    if not _off_alerted:
        _off_alerted = True
        await _alert("mode_off", [
            "ИИ-менеджер выключен в панели (или настройки панели недоступны слишком давно).",
            f"Сделок возвращено менеджерам: {len(owned)}.",
        ], dedupe="mode_off")


async def _expire_turns() -> None:
    now = _now()
    for t in await _db(store.turns_in_state, store.T_ACCEPTED):
        if t["accepted_at"] and now - t["accepted_at"] > cfg.TURN_TIMEOUT_S:
            logger.error("ai_engine: ход %s без итога %s с", t["turn_id"], cfg.TURN_TIMEOUT_S)
            await _on_turn_timeout(t)


# =============================================================================================
# итог хода и разрешение на отправку (зовёт ai_engine_api)
# =============================================================================================

def decide_permit(deal_id: int, epoch: int, turn_id: int, up_to_message_id: str) -> tuple[int, dict]:
    """Разрешение на отправку клиенту. Возвращает (HTTP-код, тело). Чистое чтение базы.

    Порядок проверок важен: закрытая сделка и выключенный режим отказывают при любой эпохе, дальше эпоха
    (старая -> 409, сервис читает это как `stale_epoch`), владелец, и только потом «клиент написал ещё»."""
    owner = store.get_owner(deal_id)
    if owner is None:
        return 200, {"allowed": False, "reason": "owner_changed"}
    if owner["closed"]:
        return 200, {"allowed": False, "reason": "deal_closed"}
    if settings.effective_mode() == "off":
        return 200, {"allowed": False, "reason": "mode_off"}
    if int(epoch) != int(owner["epoch"]):
        return 409, {"allowed": False, "reason": "stale_epoch"}
    if owner["owner"] != store.OWNER_AI:
        return 200, {"allowed": False, "reason": "owner_changed"}
    turn = store.get_turn(turn_id)
    if turn is not None:
        if turn["deal_id"] != deal_id or turn["state"] == store.T_CANCELLED:
            return 200, {"allowed": False, "reason": "owner_changed"}
        up_to_seq = turn["up_to_seq"]
        chat_key = turn["chat_key"]
    else:
        up_to_seq = store.seq_of(up_to_message_id)
        chat_key = owner["chat_key"]
        if up_to_seq is None or not chat_key:
            return 200, {"allowed": False, "reason": "newer_client_message"}
    if store.has_newer_than(chat_key, up_to_seq):
        return 200, {"allowed": False, "reason": "newer_client_message"}
    return 200, {"allowed": True}


async def handle_result(
    turn_id: int, deal_id: int, epoch: int, outcome: str, reason: str | None, pending_draft: dict | None,
) -> tuple[int, dict]:
    async with _get_lock():
        turn = await _db(store.get_turn, turn_id)
        if turn is None or turn["deal_id"] != deal_id:
            return 409, {"detail": "unknown_turn"}
        owner = await _db(store.get_owner, deal_id)
        if owner is None or int(epoch) != int(owner["epoch"]):
            if turn["state"] not in (store.T_DONE, store.T_CANCELLED):
                await _db(store.update_turn, turn_id, state=store.T_DONE, finished_at=_now(), outcome=outcome,
                          outcome_reason="stale_epoch")
            return 409, {"detail": "stale_epoch"}
        if turn["state"] in (store.T_DONE, store.T_CANCELLED):
            return 200, {"ok": True, "duplicate": True}
        if pending_draft:
            logger.info("ai_engine: ход %s вернул черновик %s - отложенная отправка в первой версии не поддерживается",
                        turn_id, pending_draft)
        s = await _db(store.get_session, turn["chat_key"])
        if outcome == "failed":
            await _on_turn_failed(turn, reason or "failed")
            return 200, {"ok": True}
        await _db(store.update_turn, turn_id, state=store.T_DONE, finished_at=_now(), outcome=outcome,
                  outcome_reason=reason)
        if s is not None:
            await _db(store.put_session, s["chat_key"], fails=0)
        if outcome == "handoff_to_human":
            await _to_human(s, "ai_handoff", deal_id=deal_id, alert=[
                "ИИ передал диалог менеджеру" + (f": {_clean(reason)}" if reason else "."),
                _client_line(s) if s else "", _link(deal_id),
            ], cancel=False)
        elif outcome == "release_to_engine":
            closed = bool(reason) and (reason.startswith("sale_completed") or reason.startswith("closed_nontarget"))
            await _db(store.set_owner, deal_id, store.OWNER_ENGINE, reason=reason or "release", closed=closed or None)
            await _db(store.cancel_active_turns, deal_id)
            if s is not None:
                await _db(store.discard_pending, s["chat_key"])
                if closed:
                    # Сделка закончена (продажа или закрытие): чат свободен для будущих обращений по правилам входа.
                    await _db(store.drop_session, s["chat_key"])
                else:
                    await _db(store.put_session, s["chat_key"], state=store.S_RELEASED, next_at=None,
                              pending_event=None, current_turn=None)
        elif outcome == "aborted":
            await _after_abort(turn, s, owner, reason)
        else:                                            # continue
            await _after_turn(s)
        return 200, {"ok": True}


async def _after_abort(turn: dict, s: dict | None, owner: dict, reason: str | None) -> None:
    if reason == "mode_mismatch":
        await _to_human(s, "mode_mismatch", deal_id=int(turn["deal_id"]), alert=[
            "ИИ-сервис не взял сделку: воронка не совпала с режимом. Диалог передан менеджеру.",
            _client_line(s) if s else "", _link(int(turn["deal_id"])),
        ])
        return
    # Клиент написал ещё либо движок не ответил на permit: клиент остался без ответа, ход назначается заново.
    redo = reason is None or reason == "newer_client_message" or str(reason).startswith("engine_unavailable")
    if redo:
        await _db(store.unconsume, turn["turn_id"])
        await _restore_event(turn)
    await _after_turn(s)


async def _restore_event(turn: dict) -> None:
    """Ход-событие сорвался: само событие (оплата, возврат диалога) не должно пропасть вместе с ним. Сообщения
    клиента возвращает `unconsume`, а событие лежит только в теле хода - кладём его обратно в чат."""
    try:
        event = json.loads(turn["payload"]).get("event")
    except ValueError:
        event = None
    if event:
        await _db(store.put_session, turn["chat_key"], pending_event=json.dumps(event))


async def _after_turn(s: dict | None) -> None:
    """После хода: есть новые сообщения или ждущее событие - назначаем следующий ход, нет - ждём клиента."""
    if s is None:
        return
    chat_key = s["chat_key"]
    s = await _db(store.get_session, chat_key)
    if s is None or s["state"] != store.S_IN_TURN:
        return
    pending = await _db(store.pending_inbox, chat_key)
    if s.get("pending_event"):
        await _db(store.put_session, chat_key, state=store.S_COLLECTING, next_at=_now(), current_turn=None)
    elif pending:
        timers = settings.timers()
        batch_start = pending[0]["received_at"]
        nxt = next_turn_at(first=False, batch_start=batch_start, last=s["last_client_at"] or batch_start, timers=timers)
        await _db(store.put_session, chat_key, state=store.S_COLLECTING, batch_start=batch_start, first_batch=0,
                  next_at=max(nxt, _now()), current_turn=None)
    else:
        await _db(store.put_session, chat_key, state=store.S_AI, next_at=None, current_turn=None)


async def _on_turn_timeout(turn: dict) -> None:
    """Итог хода так и не пришёл. Автоповтора НЕТ, и это осознанно: чаще всего потерян не ход, а доставка итога
    (движок пересобирали, пока сервис слал `result`), то есть клиенту ИИ уже ответил, и повтор написал бы ему
    второй раз. Ход закрывается, ИИ остаётся владельцем и ждёт следующего сообщения, а клиента без ответа
    подхватит SLA-таймер. Алерт зовёт человека посмотреть."""
    await _db(store.update_turn, turn["turn_id"], state=store.T_DONE, finished_at=_now(), outcome="timeout")
    s = await _db(store.get_session, turn["chat_key"])
    await _after_turn(s)
    await _alert("turn_timeout", [
        "ИИ-сервис не прислал итог хода. Повтор не делаю: клиенту ИИ мог уже ответить.",
        _client_line(s), _link(int(turn["deal_id"])),
    ], dedupe=f"turn_timeout:{turn['deal_id']}")


async def _on_turn_failed(turn: dict, why: str) -> None:
    """Сбой модели или провайдера: до двух повторов с паузой 30 и 120 с, потом человек."""
    await _db(store.update_turn, turn["turn_id"], state=store.T_DONE, finished_at=_now(), outcome="failed",
              outcome_reason=why)
    s = await _db(store.get_session, turn["chat_key"])
    if s is None:
        return
    fails = int(s["fails"]) + 1
    if fails <= len(cfg.FAIL_RETRY_DELAYS_S):
        await _db(store.unconsume, turn["turn_id"])
        await _restore_event(turn)
        await _db(store.put_session, s["chat_key"], fails=fails, state=store.S_COLLECTING,
                  next_at=_now() + cfg.FAIL_RETRY_DELAYS_S[fails - 1], current_turn=None,
                  batch_start=_now(), first_batch=0)
        return
    await _db(store.put_session, s["chat_key"], fails=0)
    await _to_human(s, "ai_failed", deal_id=int(turn["deal_id"]), alert=[
        "ИИ не смог ответить клиенту после трёх попыток, диалог передан менеджеру.",
        f"Причина: {_clean(why)}.", _client_line(s), _link(int(turn["deal_id"])),
    ])


# =============================================================================================
# передача человеку
# =============================================================================================

async def _to_human(
    s: dict | None, reason: str, *, alert: list[str] | None, deal_id: int | None = None, cancel: bool = True,
) -> None:
    """`owner = human`, `epoch + 1`, ход снят, сообщения чата не нужны, сервису best-effort отмена."""
    if deal_id is None and s is not None:
        deal_id = s["deal_id"]
    if deal_id is not None:
        owner = await _db(store.get_owner, int(deal_id))
        if owner is not None and owner["owner"] != store.OWNER_HUMAN:
            await _db(store.set_owner, int(deal_id), store.OWNER_HUMAN, reason=reason)
        elif owner is None and s is not None:
            await _db(store.set_owner, int(deal_id), store.OWNER_HUMAN, reason=reason, chat=_chat_dict(s))
        if cancel:
            for t in await _db(store.cancel_active_turns, int(deal_id)):
                _spawn(client.post_cancel(int(t["turn_id"])))
    if s is not None:
        await _db(store.put_session, s["chat_key"], state=store.S_HUMAN, next_at=None, pending_event=None,
                  has_media=0, current_turn=None, **({} if deal_id is None else {"deal_id": int(deal_id)}),
                  **({} if deal_id is not None else {"skip_until": _now() + cfg.HUMAN_CHAT_TTL_S}))
        await _db(store.discard_pending, s["chat_key"])
    if alert:
        await _alert(reason, [line for line in alert if line], dedupe=f"{reason}:{deal_id}",
                     return_deal=None if deal_id is None else int(deal_id))
        _contact_task(None if deal_id is None else int(deal_id), reason, [line for line in alert if line])


async def _handoff_media(s: dict, owner: dict, mtype: str) -> None:
    deal_id = int(owner["deal_id"])
    await _to_human(s, "media", deal_id=deal_id, alert=_media_lines([mtype], s, deal_id))


async def _handoff_by_rule(s: dict, res: dedup.Resolution, owner: dict | None) -> None:
    """Правила A, C, D: диалог человеку, ссылки на сделки в алерте. Сделки не закрываем и не объединяем."""
    others = res.others
    links = [_link(int(ld["id"])) for ld in others[:4]]
    if res.rule == "C":
        deal_id = int(s["deal_id"])
        await _to_human(s, "rule_C", deal_id=deal_id, alert=[
            "У клиента появилась другая сделка с заказом, ИИ остановлен.",
            _client_line(s), "Сделка чата: " + _link(deal_id), "Новая: " + " ".join(links),
        ])
        try:
            await asyncio.wait_for(amo_service.add_note(
                deal_id, f"У клиента появилась сделка {others[0]['id']} с заказом, ИИ остановлен. Диалог передан менеджеру.",
            ), timeout=15)
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: не записал заметку правила C в сделку %s", deal_id)
        return
    deal_id = int(res.deal["id"]) if res.deal else None
    if deal_id is not None:
        await _db(store.set_owner, deal_id, store.OWNER_HUMAN, reason=f"rule_{res.rule}", chat=_chat_dict(s),
                  pipeline_id=int(res.deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(res.deal),
                  mode=settings.effective_mode())
    await _db(store.put_session, s["chat_key"], state=store.S_HUMAN, next_at=None, deal_id=deal_id,
              skip_until=None if deal_id is not None else _now() + cfg.HUMAN_CHAT_TTL_S)
    await _db(store.discard_pending, s["chat_key"])
    head = ("У клиента несколько подходящих сделок, выбрать сам ИИ не берётся." if res.rule == "D"
            else "У клиента уже есть другая сделка с заказом или бюджетом, ИИ не входит.")
    await _alert(f"rule_{res.rule}", [head, _client_line(s), "Сделки: " + " ".join(links)],
                 dedupe=f"rule_{res.rule}:{s['chat_key']}", return_deal=deal_id)
    _contact_task(deal_id, f"rule_{res.rule}", [head])


async def _deal_closed(deal_id: int, why: str) -> None:
    """Сделка закрыта (или ушла из воронок режима, как после `complete_sale`): владелец снимается, ходы отменяются."""
    owner = await _db(store.get_owner, deal_id)
    if owner is not None and not owner["closed"]:
        if owner["owner"] == store.OWNER_AI:
            await _db(store.set_owner, deal_id, store.OWNER_ENGINE, reason=why, closed=True)
        else:
            await _db(store.mark_closed, deal_id)
    for t in await _db(store.cancel_active_turns, deal_id):
        _spawn(client.post_cancel(int(t["turn_id"])))
    s = await _db(store.session_for_deal, deal_id)
    if s is not None:
        await _db(store.drop_session, s["chat_key"])


# =============================================================================================
# события сделки: закрытие, оплата, правило C
# =============================================================================================

async def _handle_lead_event(ev: dict) -> None:
    lead_id = ev["lead_id"]
    if settings.effective_mode() == "off":
        return
    async with _get_lock():
        if await _lead_event_under_lock(ev):
            return
    if ev["added"] or ev["work_touched"]:
        await _rule_c_check(lead_id)


async def _lead_event_under_lock(ev: dict) -> bool:
    """Закрытие, уход из воронки, оплата: всё на базе, без сети. True - событие разобрано, правило C не нужно
    (сделка уже в нашей базе: закрыта, ведётся ИИ или лежит у человека)."""
    lead_id = ev["lead_id"]
    owner = await _db(store.get_owner, lead_id)
    if owner is not None and not owner["closed"] and ev["status_id"] in cfg.CLOSED_STATUS_IDS:
        await _deal_closed(lead_id, "deal_closed")
        return True
    if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"]:
        if ev["pipeline_id"] is not None and ev["pipeline_id"] not in settings.allowed_pipelines():
            await _deal_closed(lead_id, "left_pipeline")
            return True
        if _is_payment_event(ev, owner) and await _db(store.claim_event, lead_id, "payment_received"):
            logger.info("ai_engine: сделка %s оплачена - служебное событие payment_received", lead_id)
            await _queue_event(owner, "payment_received")
        return True
    return owner is not None


def _is_payment_event(ev: dict, owner: dict) -> bool:
    """«Оплата получена» в «ОП розница» либо «Статус оплаты = Оплачен» в «Тест» (там нет автоматики счёта)."""
    if ev["status_id"] == cfg.PAYMENT_RECEIVED_STATUS:
        return True
    return bool(ev["paid"]) and owner.get("pipeline_id") == cfg.PIPELINE_TEST


async def _rule_c_check(lead_id: int) -> None:
    """Правило C: у клиента ИИ появилась другая сделка с признаком работы. Читаем сделку только тогда, когда ИИ
    вообще кого-то ведёт, и не чаще раза в минуту на сделку: вебхук приходит на любое изменение любой сделки."""
    owned = await _db(store.list_owned, store.OWNER_AI)
    if not owned:
        return
    now = _now()
    if now - _rule_c_seen.get(lead_id, 0) < 60:
        return
    _rule_c_seen[lead_id] = now
    if len(_rule_c_seen) > 5000:
        _rule_c_seen.clear()
    # Потолок чтений amo под правило C: массовая правка сделок не должна превратиться в шквал запросов.
    window = [t for t in _rule_c_reads if now - t < 60]
    _rule_c_reads[:] = window
    if len(window) >= RULE_C_READS_PER_MIN:
        logger.warning("ai_engine: правило C: достигнут потолок чтений amo в минуту, сделка %s пропущена", lead_id)
        return
    _rule_c_reads.append(now)
    lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
    if not lead or not dedup.is_open(lead) or not dedup.has_work_signs(lead):
        return
    theirs = set(dedup.contact_ids_of(lead))
    async with _get_lock():
        for row in await _db(store.list_owned, store.OWNER_AI):
            mine = {int(c) for c in json.loads(row["contact_ids"] or "[]")}
            if not (mine & theirs) or int(row["deal_id"]) == lead_id:
                continue
            s = await _db(store.session_for_deal, int(row["deal_id"]))
            res = dedup.Resolution(dedup.K_HUMAN, rule="C", others=[lead])
            if s is None:
                s = {"chat_key": row["chat_key"], "deal_id": row["deal_id"], "chat_id": row["chat_id"] or "",
                     "contact_name": "", "phone": "", "chat_type": row["chat_type"] or "",
                     "channel_id": row["channel_id"] or ""}
            await _handoff_by_rule(s, res, row)


async def return_dialog(deal_id: int) -> tuple[int, dict]:
    """Человек вернул диалог ИИ (`dialog_returned`): `owner = ai`, `epoch + 1`, ход-событие сразу."""
    async with _get_lock():
        mode = settings.effective_mode()
        if mode == "off":
            return 409, {"detail": "mode_off"}
        owner = await _db(store.get_owner, deal_id)
        if owner is None or owner["closed"] or owner["owner"] == store.OWNER_AI:
            return 409, {"detail": "not_returnable"}
        if owner["pipeline_id"] not in settings.allowed_pipelines():
            return 409, {"detail": "pipeline_not_allowed"}
        if not await _ai_is_up():
            return 503, {"detail": "ai_unavailable"}
        owner = await _db(store.set_owner, deal_id, store.OWNER_AI, reason="dialog_returned", mode=mode)
        await _queue_event(owner, "dialog_returned")
        _spawn(ai_engine_tasks.release(deal_id))      # ИИ снова ведёт диалог: задачи «Связаться» по передаче закрываем
        return 200, {"ok": True, "epoch": owner["epoch"]}


# =============================================================================================
# алерты
# =============================================================================================

def _link(deal_id: int) -> str:
    return f'<a href="{BASE_URL}/leads/detail/{deal_id}">Открыть сделку</a>'


def _client_line(s: dict | None) -> str:
    if not s:
        return ""
    name = html.escape((s.get("contact_name") or "").strip()) or "клиент без имени"
    return f"👤 {name}"


def _clean(text: Any) -> str:
    return html.escape(" ".join(str(text or "").split())[:200])


def _media_lines(types: list[str], s: dict, deal_id: int) -> list[str]:
    words = ", ".join(sorted({MEDIA_WORDS.get(t, "вложение") for t in types})) or "вложение"
    return [f"Клиент прислал {words}, ИИ такое не читает. Диалог передан менеджеру.", _client_line(s), _link(deal_id)]


def _plain(text: str) -> str:
    """Строка алерта без тегов и сущностей: для текста задачи в amo."""
    return html.unescape(re.sub(r"<[^>]+>", "", text or "")).strip()


# Передачи, после которых ставится задача «Связаться»: тихие (alert=None) и передача самим ИИ (`ai_handoff`: задачу уже поставила
# тулза handoff сервиса) сюда не попадают, `mode_mismatch` это не обращение клиента.
_NO_TASK_REASONS = frozenset({"ai_handoff", "mode_mismatch"})


def _contact_task(deal_id: int | None, reason: str, why_lines: list[str]) -> None:
    if deal_id is None or reason in _NO_TASK_REASONS or not why_lines:
        return
    _spawn(ai_engine_tasks.create_contact_task(int(deal_id), reason, _plain(why_lines[0])))


async def _alert(key: str, lines: list[str], *, dedupe: str | None = None, return_deal: int | None = None) -> None:
    """Алерт в чат ОП. Сбой отправки не должен ломать разбор: пишем в лог и живём дальше.
    `return_deal` - алерт о передаче диалога по этой сделке: под ним бот ставит метку, и реакция тегнутого менеджера
    возвращает диалог ИИ (`ai_engine_return`)."""
    try:
        now = _now()
        if dedupe:
            if now - _alert_seen.get(dedupe, 0) < 600:
                return
            _alert_seen[dedupe] = now
            if len(_alert_seen) > 2000:
                _alert_seen.clear()
        head = "🤖 ИИ-менеджер" + (" (тест)" if settings.effective_mode() == "test" else "")
        text = "\n".join([head, mentions_for(None), *[line for line in lines if line]])
        d = alerts.decide(
            f"ai_{key}", legacy_text=text, parse_mode="HTML", chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID,
            values={"сообщение": text},
        )
        if d is None:
            return
        # Алерт зовётся под общим замком: зависший Телеграм не должен держать итоги ходов дольше 8 секунд.
        sent_to: list[tuple] = []
        await asyncio.wait_for(_deliver_alert(d, lambda chat, thread, mid: sent_to.append((chat, thread, mid))), timeout=8)
        if return_deal is not None and sent_to:
            chat, thread, mid = sent_to[-1]
            _spawn(ai_engine_return.remember(chat, thread, mid, return_deal, d.text))
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: алерт «%s» не ушёл", key)


async def _deliver_alert(d, on_sent=None) -> bool:
    return await telegram_bot.send_alert(d.text, on_sent=on_sent, **d.send_kwargs())


# =============================================================================================
# срез состояния
# =============================================================================================

def status() -> dict[str, Any]:
    """Для `GET /`: чтобы молчащий контур отличался от выключенного без чтения логов."""
    out: dict[str, Any] = {"enabled": cfg.ENABLED}
    if not cfg.ENABLED:
        return out
    out.update(
        mode=settings.effective_mode(), settings_age_s=settings.age_s(),
        ai_up=_ai_ok_at is not None and (_now() - _ai_ok_at) <= max(3 * cfg.HEALTH_POLL_S, 45),
        loops_alive=sum(1 for t in _tasks if not t.done()),
    )
    try:
        out["counts"] = store.counts()
    except Exception:  # noqa: BLE001
        out["counts"] = "ошибка чтения базы"
    out["return_by_reaction"] = ai_engine_return.stats()
    return out
