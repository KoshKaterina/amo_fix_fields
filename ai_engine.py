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

⚠️ Режим у сделки (`ai_engine_modes`): ИИ ведёт сделку, только если взял её в активном `mode_id` панели. Сменился режим, и сделки
чужого режима снимаются тихо (`_release_for_mode`); проверка стоит в каждой точке, где ИИ что-то делает со сделкой. Снятие уходит
событием `mode_release` в журнал ИИ панели (`ai_engine_events`, фоном, потеря допустима).

⚠️ Нет промпта: не пишем (решение Кати 23.11.2026, `ai_engine_prompts`). Воронку сделки решает `settings.pipeline_allowed`: разрешено режимом И для воронки
есть промпт в ИИ-сервисе (словарь `GET /v1/funnels`, опрос `prompts.poll_loop` раз в минуту). Сделка ИИ в воронке без промпта не берётся (`dedup.resolve`,
`give_deal`, подтверждение смены ответственного), не получает событий и касаний, а уже взятая и оказавшаяся в такой воронке снимается как `left_pipeline`
(событие сделки из amo) или теряется при следующем сообщении клиента (`dedup.resolve` отвечает `K_GONE`). Перехват (`ai_engine_intercept`) ведёт свои правила воронок.

⚠️ Подбор существующих сделок (решение Кати 23.11.2026, `ai_engine_sweep`): раз в 5 минут из `tick_once` (не отдельный цикл, `loops_alive` по-прежнему пять) фоновый прогон берёт по одной-две
сделки из списка панели теми же правилами, что при сообщении клиента, и отдаёт их ИИ событием `deal_given` через общую с `give_deal` часть `_hand_over`. Отметки людей по сделке
(`ai_engine_marks`, `mark_deal`): «не требует ответа» снимает ждущий ход и выводит сделку из подбора, «удалить» навсегда освобождает её от ИИ без алертов и задач; «Дать сделку ИИ» снимает обе.

⚠️ Открытая задача сотрудника и окно пропуска на пути по сообщению клиента (решение Кати 24.11.2026): ПЕРВОЕ взятие «существующей» сделки (`decision.existing`) блокирует любая открытая
задача amo на любого сотрудника, кроме пользователя ИИ (`_task_guard` в `_gate_take`; новые лиды, ведомые сделки, `give_deal`, перехват и подбор правилом не затронуты; рубильник
`AI_TASK_GUARD_DISABLED=1`). Отказ закрывает чат в `skip` до `retry_at`/`SKIP_TTL_S`, и сообщения клиента в окне больше не теряются: `_save_in_skip_window` кладёт их во входящие,
`store.due_sessions` возвращает чат в конце окна, `_process_collecting` разбирает его заново (один раз на окно, при новом отказе входящие сбрасываются и окно открывается снова).
"""
import asyncio
import datetime
import html
import json
import logging
import re
import sys
import time
from typing import Any

import ai_engine_approval as approval
import ai_engine_client as client
import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_events as panel_events
import ai_engine_filters as filters
import ai_engine_followup as followup
import ai_engine_give as give
import ai_engine_intercept as intercept
import ai_engine_marks as marks
import ai_engine_modes as modes
import ai_engine_prompts as prompts
import ai_engine_silence as silence
import ai_engine_sweep as sweep
import ai_engine_ur as ur_wave
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

# Вложения клиента, о которых ИИ узнаёт строкой-пометкой (02.11.2026, решение Кати): сам файл ему не показывают, он вежливо
# просит описать суть текстом (негатив и жалобы по контексту зовут менеджера, это правило в промпте). Остальные виды
# (контакт, геометка, пропущенный звонок, системные) по-прежнему сразу уходят человеку.
ATTACHMENT_TYPES = frozenset({"image", "video", "audio", "document", "sticker", "unsupported"})
ATTACHMENT_WORDS = {"image": "картинку", "video": "видео", "audio": "голосовое сообщение", "document": "файл", "sticker": "стикер"}


def attachment_text(mtype: str, caption: str = "") -> str:
    """Строка для ИИ вместо вложения. Подпись клиента (text у картинки, видео, документа) идёт следом как его слова."""
    if mtype == "unsupported":
        base = "[Клиент прислал вложение в формате, который наша система не поддерживает (стикер, реакция или другое); содержимого ИИ не видит]"
    else:
        base = f"[Клиент прислал {ATTACHMENT_WORDS.get(mtype, 'вложение')}; содержимого ИИ не видит]"
    caption = (caption or "").strip()
    return f"{base} Подпись клиента: {caption}" if caption else base


_queue: asyncio.Queue | None = None
_tasks: list[asyncio.Task] = []
_lock: asyncio.Lock | None = None
_bg: set[asyncio.Task] = set()

_ai_ok_at: float | None = None          # когда сервис последний раз ответил на /health
_ai_down_since: float | None = None
_ai_down_alerted = False
_off_alerted = False
_alert_seen: dict[str, float] = {}      # дедуп алертов в памяти: ключ -> когда слали
_send_failed_seen: dict[str, float] = {}   # отказ отправки клиенту: «сделка:причина» -> когда занесли в сделку и позвали людей
SEND_FAILED_DEDUPE_S = 900
_rule_c_seen: dict[int, float] = {}     # сделка -> когда проверяли правило C
_rule_c_reads: list[float] = []         # когда читали сделки под правило C (для потолка в минуту)
RULE_C_READS_PER_MIN = 30
_last_housekeeping = 0.0
_last_expire = 0.0
_last_off_sweep = 0.0
_last_mode_id: str | None = None        # активный режим на прошлом проходе: смена запускает снятие чужих сделок сразу, не через 10 с


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
        asyncio.create_task(_guarded_loop(prompts.poll_loop, "опрос словаря воронок ИИ-сервиса")),
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


def _spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _bg.add(task)
    task.add_done_callback(_bg.discard)
    return task


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
    if not chat_id:
        return
    chat_key = chat_key_of(channel_id, chat_id)
    await _intercept_observe(m, channel_id, chat_id, chat_key)          # временный перехват: любой канал, только база
    if channel_id not in cfg.CHANNELS and not await _db(intercept.is_tracked, chat_key):
        return
    # Направление считает SLA-таймер: правило одно на весь контур (isEcho, запасной признак - статус).
    if wazzup_sla._is_outbound(m):
        await _on_outbound(chat_key, m)
    else:
        await _on_inbound(chat_key, channel_id, chat_id, m)


async def _intercept_observe(m: dict, channel_id: str, chat_id: str, chat_key: str) -> None:
    """Перехват сделок (`ai_engine_intercept`): учёт неотвеченных сообщений клиентов и сообщений менеджеров. Не бросает."""
    try:
        if str(m.get("chatType") or "").strip().lower() not in intercept.CHAT_TYPES:
            return
        now = _now()
        if wazzup_sla._is_outbound(m):
            if not is_robot_author(m.get("authorName")):
                await _db(intercept.observe_human_out, chat_key, now)       # время получения: не раньше отправки, лучше лишний отказ ИИ
            return
        if not intercept.enabled():
            return
        contact = m.get("contact") if isinstance(m.get("contact"), dict) else {}
        phone = str((contact or {}).get("phone") or "")
        if not settings.phone_may_match(phone, chat_id):
            return
        await _db(intercept.observe_inbound, chat_key, channel_id, chat_id, str(m.get("chatType") or ""), phone,
                  str((contact or {}).get("name") or ""), now)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: перехват не разобрал сообщение чата %s", chat_key)


async def _on_outbound(chat_key: str, m: dict) -> None:
    """Исходящее в чат. Автоматика и ИИ (`Admin`, пусто) - не менеджер и ничего не меняют. Человек (ФИО или
    `Phone`) в чате, который мы ведём или ждём, забирает диалог."""
    if is_robot_author(m.get("authorName")):
        return
    try:
        await _db(filters.note_manager_out, chat_key, _now())      # «менеджер активен» для `existing.manager_quiet_min` (`ai_engine_filters`)
    except Exception:  # noqa: BLE001 - учёт активности менеджера не должен мешать приёму сообщений
        logger.exception("ai_engine: не записал время сообщения менеджера в чат %s", chat_key)
    await _followup_cancel(chat_key=chat_key, reason="manager_wrote", notify=True)      # отложенное касание ИИ не уходит
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
    is_attachment = mtype in ATTACHMENT_TYPES
    is_media = not is_attachment and (mtype != "text" or not text.strip())
    caption = "" if mtype == "unsupported" else text    # у unsupported text это заглушка самого Wazzup, не слова клиента
    if not message_id:
        message_id = f"noid:{chat_key}:{m.get('dateTime')}:{abs(hash(text))}"
    if await _db(store.seq_of, message_id) is None:         # не повтор вебхука: отложенное касание отменяется
        await _followup_cancel(chat_key=chat_key, reason="client_wrote", notify=True)

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
                # Окно пропуска: сообщение больше не теряется. Сохраняем во входящие, чат пересматривается один раз в конце окна (`_process_collecting`, `store.due_sessions`).
                await _save_in_skip_window(chat_key, chat_id, phone, message_id, mtype, text, caption, is_attachment, is_media, m, now)
                return
            s = None
    if s is not None and s["deal_id"] is not None:
        # Режим сменился, а сделка взята в другом: её ИИ больше не ведёт (`ai_engine_modes`). Чат забыт, а это сообщение разбираем как
        # первое в чате: в активном режиме у клиента может быть другая подходящая сделка.
        owner = await _db(store.get_owner, int(s["deal_id"]))
        active = settings.active_mode_id()
        if modes.is_stale(owner, active):
            await _release_for_mode(owner, active, chat_key=chat_key)
            s = None
    if s is None:
        if not settings.phone_may_match(phone, chat_id):
            return
        if _autopilot_busy(chat_id, phone):
            return

    timers = settings.timers()
    sent_at = _sent_at(m, now)
    seq = await _db(store.add_inbox, message_id, chat_key, caption if is_attachment else ("" if is_media else text),
                    mtype if (is_media or is_attachment) else "text", sent_at, now)
    if seq is None:
        return                                          # повтор вебхука

    if s is None:
        # Окно пропуска кончилось, а таймер ещё не вернул чат в разбор: сообщения клиента, сохранённые в окне (`_save_in_skip_window`), уже лежат во входящих и пойдут в эту же пачку.
        # Вложение без текста (геометка, контакт) среди них не должно потерять пометку «медиа»: `put_session` ниже сбрасывает её.
        carried_media = any(r["msg_type"] != "text" and r["msg_type"] not in ATTACHMENT_TYPES for r in await _db(store.pending_inbox, chat_key))
        s = await _db(
            store.put_session, chat_key, channel_id=channel_id, chat_id=chat_id,
            chat_type=str(m.get("chatType") or ""), phone=phone, contact_name=str((contact or {}).get("name") or ""),
            state=store.S_COLLECTING, deal_id=None, batch_start=now, first_batch=1, last_client_at=now,
            next_at=now + timers["first_reply_wait_s"], resolve_deadline=now + timers["deal_resolve_timeout_s"],
            has_media=1 if carried_media else 0, last_message_id=message_id,
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


async def _save_in_skip_window(chat_key: str, chat_id: str, phone: str, message_id: str, mtype: str, text: str, caption: str,
                               is_attachment: bool, is_media: bool, m: dict, now: float) -> None:
    """Сообщение клиента в окне пропуска (`S_SKIP`, `skip_until` в будущем: отказ по фильтрам, бюджету, задаче сотрудника, тишине менеджера, ИИ-сервис не отвечал и т.п.). Раньше `_on_inbound` выходил ДО
    `add_inbox`, и сообщение пропадало насовсем. Теперь после тех же дешёвых проверок, что у новой сессии (контакт в списке режима, не авто-режим), оно кладётся во входящие (повтор вебхука по `messageId`
    отсекается как всегда), а чат остаётся в `S_SKIP` до конца окна: `store.due_sessions` вернёт его по `skip_until`, `_process_collecting` переведёт в разбор, и сделка пройдёт ВСЕ проверки заново, ровно
    один раз на окно. Снова отказ: входящие сбрасываются, новое окно (`_apply_resolution`). Вложение без текста (геометка, контакт) помечает чат `has_media`, как у любого первого сообщения."""
    if not settings.phone_may_match(phone, chat_id):
        return
    if _autopilot_busy(chat_id, phone):
        return
    seq = await _db(store.add_inbox, message_id, chat_key, caption if is_attachment else ("" if is_media else text),
                    mtype if (is_media or is_attachment) else "text", _sent_at(m, now), now)
    if seq is None:
        return                                          # повтор вебхука
    fields: dict[str, Any] = {"last_client_at": now, "last_message_id": message_id}
    if is_media:
        fields["has_media"] = 1
    await _db(store.put_session, chat_key, **fields)


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
    global _last_housekeeping, _last_expire, _last_off_sweep, _last_mode_id
    now = _now()
    mode_id = settings.active_mode_id()
    if mode_id != _last_mode_id:
        _last_mode_id = mode_id
        async with _get_lock():
            await _sweep_mode_mismatch()
    for s in await _db(store.due_sessions, now):
        try:
            await _process_collecting(s)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ai_engine: ошибка обработки чата %s", s.get("chat_key"))
            await _db(store.put_session, s["chat_key"], next_at=_now() + cfg.RESOLVE_RETRY_S)
    await _followup_tick()
    await _silence_tick()
    await _ur_tick()
    await intercept.tick(sys.modules[__name__])
    await approval.tick(sys.modules[__name__], now)          # предложения смены ответственного: устаревание и доставка в панель (`ai_engine_approval`)
    if now - _last_off_sweep >= 10:
        _last_off_sweep = now
        async with _get_lock():
            await _sweep_if_off()
            await _sweep_mode_mismatch()
    if now - _last_expire >= 30:
        _last_expire = now
        async with _get_lock():
            await _expire_turns()
    if now - _last_housekeeping >= 3600:
        _last_housekeeping = now
        await _db(store.prune, cfg.RETENTION_DAYS * 86400)
    await sweep.tick(sys.modules[__name__], now)         # подбор существующих сделок: раз в 5 минут, прогон фоновой задачей, не отдельный цикл (`ai_engine_sweep`)


async def _process_collecting(s: dict) -> None:
    """Чат дозрел. Сначала сеть (поиск сделок в amo) БЕЗ общего замка: пока amo думает, должны проходить
    итоги ходов и вебхуки (итог, не принятый за 10 секунд, сервис считает недоставленным). Решение принимается
    под замком и по свежей строке чата: если пока мы ходили в amo, чат сменил состояние, это решение устарело."""
    chat_key = s["chat_key"]
    mode = settings.effective_mode()
    mode_id = settings.active_mode_id()
    if mode == "off":
        async with _get_lock():
            await _db(store.drop_session, chat_key)
        return
    if s["state"] == store.S_SKIP:
        # Окно пропуска кончилось, а во входящих лежат сообщения клиента, пришедшие в окне (`_save_in_skip_window`): чат возвращается в разбор, один раз на окно.
        # Клиент пишет прямо сейчас: ждём тишину `debounce_s`, первого ожидания нет (сообщения уже ждали окно).
        now = _now()
        timers = settings.timers()
        due = max(now, float(s["last_client_at"] or 0) + timers["debounce_s"])
        s = await _db(store.put_session, chat_key, state=store.S_COLLECTING, batch_start=now, first_batch=0, next_at=due, skip_until=None,
                      resolve_deadline=now + timers["deal_resolve_timeout_s"])
        logger.info("ai_engine: окно пропуска чата %s кончилось, сообщения клиента, пришедшие в окне, разбираются заново", chat_key)
        if due > now:
            return
    owner = await _db(store.get_owner, s["deal_id"]) if s["deal_id"] is not None else None
    if modes.is_stale(owner, mode_id):
        # Сделку ведёт ИИ, взявший её в другом режиме: снимаем и в amo за решением не ходим.
        async with _get_lock():
            fresh_owner = await _db(store.get_owner, int(s["deal_id"]))
            if modes.is_stale(fresh_owner, settings.active_mode_id()):
                await _release_for_mode(fresh_owner, settings.active_mode_id(), chat_key=chat_key)
        return
    known = int(s["deal_id"]) if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"] else None
    forced = known if known is not None and (await _db(give.is_forced, known) or await _db(intercept.is_taken, known)) else None
    taken = frozenset({known}) if known is not None and await _db(intercept.is_taken, known) else frozenset()   # перехваченная: любая воронка
    eligible = filters.make_eligibility(mode_id, [s["phone"], s["chat_id"]]) if known is None else None     # фильтры и бюджет при взятии: только для сделок, которых ИИ ещё не ведёт
    res = await dedup.resolve(s["chat_id"], s["phone"], known_deal_id=known, force_deal_id=forced, any_pipeline_ids=taken, eligible=eligible)
    if res.kind == dedup.K_AI and known is None and res.deal is not None:
        res = await _gate_take(s, res, mode_id)                  # товары (МойСклад), «менеджер активен», смена ответственного: тоже вне общего замка
    async with _get_lock():
        fresh = await _db(store.get_session, chat_key)
        if fresh is None or fresh["state"] != store.S_COLLECTING or (fresh["next_at"] or 0) > _now():
            return
        await _apply_resolution(fresh, res, mode, mode_id)


async def _task_guard(deal: dict) -> tuple[str, float] | None:
    """Правило «на сделке открыта задача сотрудника» для пути по сообщению клиента (решение Кати 24.11.2026): (причина отказа, когда пересмотреть) или None, если можно. Один живой запрос
    открытых задач сделки (`ai_engine_tasks._get_open`), та же семантика, что у подбора: любая открытая задача на любого сотрудника, кроме пользователя ИИ (`filters.ai_user()`), блокирует
    (`TASK_RETRY_S`, час). amo не ответила (None): «задачи не проверить», пересмотр через `TASK_UNCHECKED_RETRY_S`, это НЕ «задач нет»."""
    now = _now()
    tasks = await ai_engine_tasks._get_open(int(deal["id"]))
    if tasks is None:
        return "задачи не проверить (amo не ответила)", now + cfg.TASK_UNCHECKED_RETRY_S
    user = filters.ai_user() or filters.DEFAULT_AI_USER
    if ai_engine_tasks.foreign_open_tasks(tasks, int(user["id"])):
        return "на сделке открыта задача сотрудника", now + cfg.TASK_RETRY_S
    return None


async def _gate_take(s: dict, res: dedup.Resolution, mode_id: str, *, tasks_checked: bool = False) -> dedup.Resolution:
    """Решение «ИИ берёт эту сделку в первый раз» (`ai_engine_filters.decide_take`): фильтры, бюджет, товары (МойСклад), «существующие сделки»,
    «менеджер активен», открытая задача сотрудника, назначение ответственного. Сеть здесь, вне общего замка. Сделке, у которой в базе движка уже есть владелец (человек,
    движок, ИИ), решение не нужно: его принимали раньше. Отказ это `refused` (сделка не взята, чат пропускается), сбой проверки тоже отказ.
    Открытая задача сотрудника (`_task_guard`) блокирует только ПЕРВОЕ взятие «существующей» сделки (`decision.existing`: создана до границы `existing.since`, режим «Полный»): на новые лиды
    автоматика ставит задачу приёма, которую ИИ сам закрывает при первом входе (`release_intake`), с правилом он бы не отвечал на свежие лиды. Рубильник `AI_TASK_GUARD_DISABLED=1`.
    `tasks_checked=True`: задачи уже смотрели (подбор `ai_engine_sweep` проверяет их сам, второго запроса на сделку нет)."""
    deal = res.deal
    if await _db(store.get_owner, int(deal["id"])) is not None:
        return res
    explicit = settings.contacts_restricted() and settings.contact_allowed(dedup.contact_ids_of(deal), [s["phone"], s["chat_id"]])
    try:
        decision = await filters.decide_take(deal, mode_id=mode_id, chat_key=s["chat_key"], explicit=explicit)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: сбой проверки фильтров по сделке %s, сделку не беру", deal.get("id"))
        return dedup.Resolution(dedup.K_REFUSED, deal=deal, note="сбой проверки фильтров")
    if not decision.allow:
        return dedup.Resolution(dedup.K_REFUSED, deal=deal, note=decision.reason, retry_at=decision.retry_at)
    if decision.existing and not tasks_checked and not cfg.TASK_GUARD_DISABLED:
        try:
            blocked = await _task_guard(deal)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: сбой проверки задач сделки %s, сделку не беру", deal.get("id"))
            blocked = ("задачи не проверить (сбой проверки)", _now() + cfg.TASK_UNCHECKED_RETRY_S)
        if blocked is not None:
            return dedup.Resolution(dedup.K_REFUSED, deal=deal, note=blocked[0], retry_at=blocked[1])
    if decision.propose:
        # Смену ответственного нужно подтвердить человеку: открытое предложение по сделке уже ждёт решения либо недавно отклонено: не создаём второе
        why = await _db(approval.block_reason, int(deal["id"]), _now())
        if why:
            return dedup.Resolution(dedup.K_REFUSED, deal=deal, note=why)
    res.gate = decision
    return res


async def _apply_resolution(s: dict, res: dedup.Resolution, mode: str, mode_id: str | None = None) -> None:
    chat_key = s["chat_key"]
    now = _now()
    mode_id = mode_id or settings.active_mode_id()
    if settings.effective_mode() != mode or settings.active_mode_id() != mode_id:
        # Режим сменили, пока шёл поиск в amo: выбор сделки сделан по воронкам прежнего режима, применять его нельзя. Решаем заново.
        await _db(store.put_session, chat_key, next_at=now)
        return
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
    if res.kind == dedup.K_REFUSED:
        # ИИ сделку не берёт (фильтры, бюджет, товары, «существующие сделки»): ничего не пишем ни клиенту, ни в amo, ни людям; причина одной строкой в лог
        logger.info("ai_engine: сделка %s не взята: %s", (res.deal or {}).get("id"), res.note)
        skip_until = res.retry_at if res.retry_at and res.retry_at > now else now + cfg.SKIP_TTL_S
        await _db(store.put_session, chat_key, state=store.S_SKIP, skip_until=skip_until, next_at=None)
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
    if modes.is_stale(owner, mode_id):
        await _release_for_mode(owner, mode_id, chat_key=chat_key)         # эту сделку ИИ взял в другом режиме
        return
    if owner is not None and owner["owner"] == store.OWNER_HUMAN:
        await _db(store.put_session, chat_key, state=store.S_HUMAN, deal_id=deal_id, next_at=None)
        await _db(store.discard_pending, chat_key)
        return
    if owner is not None and owner["owner"] == store.OWNER_ENGINE:
        await _db(store.put_session, chat_key, state=store.S_RELEASED, deal_id=deal_id, next_at=None)
        await _db(store.discard_pending, chat_key)
        return
    if owner is None:
        # Отметка человека («удалить» или «не требует ответа» поверх этого сообщения, `ai_engine_marks`): сделку не берём. Сообщение клиента ПОЗЖЕ отметки
        # отметка не касается. Чат забывается целиком (не `skip`): иначе следующее сообщение клиента в ближайшие 10 минут было бы проигнорировано.
        why_marked = await _db(marks.suppresses, deal_id, s.get("last_client_at"))
        if why_marked:
            logger.info("ai_engine: сделка %s не взята: %s", deal_id, why_marked)
            await _db(store.drop_session, chat_key)
            return
    if _autopilot_busy(s["chat_id"], s["phone"], deal_id):
        logger.info("ai_engine: сделку %s ведёт авто-режим - ИИ не входит", deal_id)
        await _db(store.put_session, chat_key, state=store.S_SKIP, deal_id=deal_id,
                  skip_until=now + cfg.SKIP_TTL_S, next_at=None)
        await _db(store.discard_pending, chat_key)
        return

    if s["has_media"]:
        # Медиа от клиента: ИИ не получает ничего, сделка сразу человеку (даже если ИИ ещё не владел).
        media_types = [r["msg_type"] for r in await _db(store.pending_inbox, chat_key)
                       if r["msg_type"] != "text" and r["msg_type"] not in ATTACHMENT_TYPES]
        owner = await _db(
            store.set_owner, deal_id, store.OWNER_HUMAN, reason="media", chat=chat,
            pipeline_id=int(deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(deal), mode=mode_id,
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

    if owner is None and res.gate is not None and res.gate.propose:
        # «Перераспределить существующие» при включённом пилоте: ИИ просит разрешения, сделку пока не берёт и ничего не пишет в amo (`ai_engine_approval`)
        await approval.propose(sys.modules[__name__], s, deal, res.gate, mode_id)
        return

    if await _db(store.pending_inbox, chat_key):                # настоящее сообщение клиента (в любом канале), а не служебное событие:
        await _db(silence.clear, deal_id)                      # лестница тишины сначала
    first_entry = owner is None      # ИИ входит в сделку впервые (а не после возврата диалога)
    gate = res.gate if first_entry else None       # решение о взятии (`_gate_take`): назначить ответственного, поставить охрану инициативы
    if gate is not None and gate.reassign and not await filters.reassign_responsible(deal_id, gate.reassign):
        # «Перераспределить существующие»: смена ответственного не удалась, сделку не берём (причина в логе `reassign_responsible`)
        await _db(store.put_session, chat_key, state=store.S_SKIP, skip_until=now + cfg.SKIP_TTL_S, next_at=None)
        await _db(store.discard_pending, chat_key)
        return
    owner = await _db(
        store.set_owner, deal_id, store.OWNER_AI, reason="client_message", chat=chat,
        pipeline_id=int(deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(deal), mode=mode_id,
    )
    if gate is not None and gate.existing:
        await _db(filters.guard_on_take, deal_id)      # существующая сделка: инициатива (тишина, УР, касания) ждёт первого сообщения ИИ клиенту
    if first_entry:
        _spawn(ai_engine_tasks.release_intake(deal_id))     # системные «Связаться»/«Новый заказ с сайта»: теперь клиентом занят ИИ
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
    pending = [r for r in await _db(store.pending_inbox, chat_key)
               if (r["msg_type"] == "text" and r["text"].strip()) or r["msg_type"] in ATTACHMENT_TYPES]
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
    cancelled_rows = await _db(followup.cancelled_notes, deal_id)
    intercepted = bool(await _db(intercept.is_taken, deal_id))   # перехвачена у менеджера: только консультация, воронка любая
    mode_id = settings.active_mode_id()
    manager_cutoff = (await _db(intercept.cutoff_iso, deal_id)) or await _db(give.cutoff_iso, deal_id, mode_id)   # сделку отдали ИИ явно: сообщения менеджера до этого не останавливают
    given_forced = bool(await _db(give.is_forced, deal_id, mode_id)) or intercepted      # отдали вопреки параллельной сделке: ИИ её не учитывает
    # только при настройках версии 2 (`mode_id`, `review`, `initiative` по воронке сделки, блоки функций); перехваченная сделка: initiative как у перехвата
    mode_settings = settings.mode_settings(initiative=settings.initiative_allowed(owner.get("pipeline_id")) or intercepted)

    def build(turn_id: int) -> dict[str, Any]:
        body: dict[str, Any] = {
            "turn_id": turn_id, "deal_id": deal_id, "epoch": int(owner["epoch"]), "mode": mode, "reason": reason,
            "chat": {"chat_id": s["chat_id"], "chat_type": s["chat_type"], "channel_id": s["channel_id"]},
            "messages": [{"message_id": r["message_id"],
                          "text": r["text"] if r["msg_type"] == "text" else attachment_text(r["msg_type"], r["text"]),
                          "sent_at": r["sent_at"]} for r in pending],
            "up_to_message_id": up_to_id, "now": _iso(_now()), "send_window": window,
            "returning": None, "client_memory": None,
        }
        if event is not None:
            body["event"] = {"type": event["type"], "at": event["at"],
                             **{k: event[k] for k in ("reason", "draft", "due_at") if k in event}}
        if cancelled_rows:
            body["followup_cancelled"] = followup.cancel_payload(cancelled_rows)
        if manager_cutoff:
            body["manager_cutoff"] = manager_cutoff
        if given_forced:
            body["given_forced"] = True
        if intercepted:
            body["intercepted"] = True
        if mode_settings is not None:
            body["mode_settings"] = mode_settings
        return body

    turn = await _db(
        store.create_turn, deal_id=deal_id, epoch=int(owner["epoch"]), chat_key=chat_key, reason=reason,
        payload_fn=build, message_seqs=[r["seq"] for r in pending], up_to_seq=up_to_seq,
    )
    await _db(store.put_session, chat_key, state=store.S_IN_TURN, current_turn=turn["turn_id"], next_at=None,
              has_media=0, pending_event=None, deal_id=deal_id)
    if cancelled_rows:
        await _db(followup.clear_notify, [r["id"] for r in cancelled_rows])
    logger.info("ai_engine: ход %s по сделке %s (%s, сообщений %s)", turn["turn_id"], deal_id, reason, len(pending))


async def _initiative_ok(owner: dict) -> bool:
    """Можно ли по сделке инициативные действия: её воронка в `initiative_pipelines` (`settings.initiative_allowed`), иначе ИИ только
    отвечает. Перехваченная сделка живёт по правилам перехвата (лестницы тишины и окон УР у неё нет, воронка любая), для неё да."""
    return settings.initiative_allowed(owner.get("pipeline_id")) or bool(await _db(intercept.is_taken, int(owner["deal_id"])))


async def _event_blocked(owner: dict, event_type: str) -> str | None:
    """Служебное событие, которое ИИ по этой сделке получать не должен: функцию выключили в панели или воронка «только отвечаем».
    Страховка в общей точке постановки; сами таймеры (`_silence_step`, `_ur_tick`) отсекают это раньше и ещё снимают своё состояние.
    Причина словами или None, если событие можно ставить."""
    if event_type == "silent_alt":
        if not settings.silence_config()["enabled"]:
            return "лестница тишины выключена в настройках"
    elif event_type == "ur_wave":
        if not settings.ur_config()["enabled"]:
            return "окна УР выключены в настройках"
    else:
        return None
    return None if await _initiative_ok(owner) else "в воронке сделки ИИ только отвечает"


async def _queue_event(owner: dict, event_type: str, extra: dict | None = None) -> None:
    """Поставить служебное событие: оно идёт отдельным ходом, как только предыдущий закончился.
    Сделке, взятой в другом режиме, событий не ставим (её снимет `_release_for_mode`): это общая точка оплаты, возврата диалога,
    касаний, лестницы тишины, окон УР и перехвата. Инициативные события (`silent_alt`, `ur_wave`) не ставятся и тогда, когда функцию
    выключили или воронка сделки «только отвечаем» (`_event_blocked`)."""
    if modes.is_stale(owner, settings.active_mode_id()):
        logger.warning("ai_engine: событие %s по сделке %s не поставлено: сделка взята в другом режиме", event_type, owner.get("deal_id"))
        return
    blocked = await _event_blocked(owner, event_type)
    if blocked:
        logger.info("ai_engine: событие %s по сделке %s не поставлено: %s", event_type, owner.get("deal_id"), blocked)
        return
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
    event = json.dumps({"type": event_type, "at": _iso(_now()), **(extra or {})})
    fields: dict[str, Any] = {"pending_event": event}
    if s["state"] != store.S_IN_TURN:
        # Идущий ход не прерываем: событие дождётся его итога (`_after_turn`). Во всех остальных состояниях
        # (в том числе у человека, когда диалог возвращают) чат сразу дозревает до хода.
        fields.update(state=store.S_COLLECTING, next_at=_now(), first_batch=0, has_media=0)
        if s["state"] != store.S_COLLECTING:
            fields["batch_start"] = _now()
    await _db(store.put_session, s["chat_key"], **fields)


# =============================================================================================
# отложенные касания (ai_engine_followup)
# =============================================================================================

async def _followup_note(deal_id: int, text: str) -> None:
    try:
        await asyncio.wait_for(amo_service.add_note(int(deal_id), "ИИ-менеджер: " + text), timeout=15)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: не записал примечание об отложенном касании в сделку %s", deal_id)


async def _followup_cancel(*, chat_key: str | None = None, deal_id: int | None = None, reason: str, notify: bool = False) -> None:
    """Снять ждущие касания чата или сделки; в сделку идёт примечание, ИИ (при `notify`) узнает в следующем ходе."""
    try:
        rows: list[dict] = []
        if chat_key:
            rows += await _db(followup.cancel_for_chat, chat_key, reason, _now(), notify)
        if deal_id is not None:
            rows += await _db(followup.cancel_for_deal, int(deal_id), reason, _now(), notify)
    except Exception:  # noqa: BLE001 - отмена касания не должна ломать приём сообщений
        logger.exception("ai_engine: не отменил отложенное касание (%s)", reason)
        return
    for r in rows:
        _spawn(_followup_note(int(r["deal_id"]), f"отложенное касание на {followup.fmt_due(r['due_at'])} отменено: "
                                                f"{followup.REASON_TEXT.get(reason, reason)}."))


async def schedule_followup(deal_id: int, epoch: int, turn_id: int, up_to_message_id: str, due_at: float,
                            reason: str, draft: str) -> tuple[int, dict]:
    """Сервис регистрирует касание, о котором клиент попросил сам. Те же условия, что у разрешения на отправку."""
    async with _get_lock():
        code, out = decide_permit(deal_id, epoch, turn_id, up_to_message_id)
        if not out.get("allowed"):
            return code, out
        owner = await _db(store.get_owner, deal_id)
        if not settings.touches_available():
            return 422, {"allowed": False, "reason": "touch_unavailable"}              # Перехват: ИИ только отвечает, касаний нет
        if owner is not None and not await _initiative_ok(owner):
            return 422, {"allowed": False, "reason": "initiative_off"}                 # воронка «только отвечаем»: обещать клиенту сообщение нельзя
        ahead = float(due_at) - _now()
        if ahead < followup.MIN_AHEAD_S:
            return 422, {"allowed": False, "reason": "too_soon"}
        if ahead > settings.touch_max_ahead_s():                                     # `touch_max_days` из настроек режима (7 по умолчанию)
            return 422, {"allowed": False, "reason": "too_far"}
        chat_key = (owner or {}).get("chat_key") or ""
        if not chat_key:
            return 200, {"allowed": False, "reason": "owner_changed"}
        row = await _db(followup.schedule, deal_id, chat_key, int(owner["epoch"]), turn_id, float(due_at), reason, draft, _now())
    _spawn(_followup_note(deal_id, f"по просьбе клиента напишу ему {followup.fmt_due(due_at)}"
                                   + (f". Причина: {_plain(reason)}" if reason else "")))
    return 200, {"allowed": True, "id": row["id"], "due_at": followup.fmt_due(due_at)}


async def _followup_tick() -> None:
    now = _now()
    for f in await _db(followup.due, now):
        try:
            async with _get_lock():
                await _fire_followup(f)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: ошибка отложенного касания %s", f.get("id"))


async def _fire_followup(f: dict) -> None:
    """Срок пришёл. Проверяем, что касание всё ещё уместно, ждём окно отправки и ставим ИИ событие `followup_due`."""
    now = _now()
    deal_id = int(f["deal_id"])
    current = await _db(followup.pending_for_deal, deal_id)
    if current is None or int(current["id"]) != int(f["id"]):
        return                                          # отменено или заменено, пока шли к замку
    owner = await _db(store.get_owner, deal_id)
    if modes.is_stale(owner, settings.active_mode_id()):
        await _release_for_mode(owner, settings.active_mode_id())       # тихо: касание снимается без примечания и алерта
        return
    why = None
    if owner is None or owner["closed"]:
        why = "deal_closed"
    elif owner["owner"] != store.OWNER_AI or int(owner["epoch"]) != int(f["epoch"]):
        why = "owner_changed"
    elif not filters.initiative_allowed_for_deal(owner):
        why = "ИИ ещё не писал клиенту в этой существующей сделке"      # охрана инициативы (`ai_engine_filters`)
    elif settings.effective_mode() == "off":
        why = "mode_off"
    elif not settings.pipeline_allowed(owner["pipeline_id"]) and not await _db(intercept.is_taken, deal_id):
        why = "pipeline_not_allowed"
    elif not settings.touches_available():
        why = "mode_no_touch"                           # Перехват: касаний нет (сменили режим, пока касание ждало)
    elif not await _initiative_ok(owner):
        why = "initiative_off"                          # воронка «только отвечаем»: инициативного сообщения клиенту не будет
    else:
        ids = [int(c) for c in json.loads(owner.get("contact_ids") or "[]")]
        if not settings.contact_allowed(ids, [owner.get("chat_id") or ""]):
            why = "contact_not_allowed"
    if why:
        await _followup_fail(f, why)
        return
    if not settings.send_window()["open_now"]:
        return                                          # вне окна отправки касание ждёт ближайшего окна
    if not await _ai_is_up():
        seen = await _db(followup.note_first_due_seen, int(f["id"]), now)
        if now - seen > followup.AI_DOWN_GIVE_UP_S:
            await _followup_fail(f, "ai_down")
        return
    await _db(followup.mark_fired, int(f["id"]), now)
    await _queue_event(owner, "followup_due", extra={"reason": f["reason"], "draft": f["draft"], "due_at": followup.fmt_due(f["due_at"])})
    logger.info("ai_engine: отложенное касание по сделке %s: срок пришёл, ИИ получает событие", deal_id)


async def _followup_fail(f: dict, why: str) -> None:
    """Касание не вышло: примечание в сделке и алерт в чат ОП, задачи менеджеру не ставим (решение Кати)."""
    await _db(followup.cancel_one, int(f["id"]), why, _now(), False)
    deal_id = int(f["deal_id"])
    text = f"отложенное касание на {followup.fmt_due(f['due_at'])} не вышло: {followup.REASON_TEXT.get(why, why)}."
    _spawn(_followup_note(deal_id, text))
    await _alert("followup_failed", ["ИИ не смог написать клиенту в назначенное время.", _plain(text), _link(deal_id)],
                 dedupe=f"followup_failed:{f['id']}", return_deal=None)


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
    if modes.is_stale(owner, settings.active_mode_id()):
        # Режим сменился, пока ход ждал отправки: сделка чужого режима ИИ не достаётся, ход не уходит, сделку снимаем.
        async with _get_lock():
            fresh = await _db(store.get_owner, deal_id)
            if modes.is_stale(fresh, settings.active_mode_id()):
                await _release_for_mode(fresh, settings.active_mode_id())
        return
    payload = json.loads(turn["payload"])
    # Настройки режима берём на момент отправки, а не создания хода: ход мог лежать в очереди, пока включали «пилот».
    ms = settings.mode_settings(initiative=await _initiative_ok(owner))
    if ms is not None:
        payload["mode_settings"] = ms
    else:
        payload.pop("mode_settings", None)
    result = await client.post_turn(payload)
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


async def _sweep_mode_mismatch() -> None:
    """Активный режим сменился (Тест -> Полный и наоборот): сделки, которые ИИ взял в другом режиме, снимаются (`ai_engine_modes`).
    Раз в 10 с под замком, вместе с `_sweep_if_off`; те же проверки стоят и в каждой точке решения, этот проход лишь не оставляет
    сделки, до которых событий пока не дошло. Режим `off` сюда не относится: им занят `_sweep_if_off`, а протухшие настройки
    ничего снимать не должны."""
    if not cfg.ENABLED:
        return
    active = settings.active_mode_id()
    if active == "off":
        return
    for row in await _db(store.list_owned, store.OWNER_AI):
        if not modes.is_stale(row, active):
            continue
        try:
            await _release_for_mode(row, active)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - одна сделка не должна оставить остальные при чужом режиме
            logger.exception("ai_engine: не снял сделку %s при смене режима", row.get("deal_id"))


async def _release_for_mode(owner: dict, active: str, *, chat_key: str | None = None) -> None:
    """Сделка взята в другом режиме, чем активный `active`: тихо снять ИИ. Зовётся под общим замком.

    Правило в шапке `ai_engine_modes`: владелец возвращается движку (`epoch + 1`, идущий ход отбит по эпохе и отменён у сервиса),
    чат забыт, лестница тишины и отложенные касания сняты. В amo не пишется ничего, клиенту ничего не уходит, алерта нет: касания
    гасятся без примечания (`_followup_cancel` оставил бы его в сделке), а в журнал движка идёт «снята при смене режима»."""
    deal_id = int(owner["deal_id"])
    from_mode = modes.mode_of(owner)
    try:
        await _db(followup.cancel_for_deal, deal_id, "mode_changed", _now(), False)
    except Exception:  # noqa: BLE001 - касание не должно помешать снятию
        logger.exception("ai_engine: не отменил отложенное касание сделки %s при смене режима", deal_id)
    await _db(silence.clear, deal_id)
    row = await _db(store.set_owner, deal_id, store.OWNER_ENGINE, reason="mode_changed")
    for t in await _db(store.cancel_active_turns, deal_id):
        _spawn(client.post_cancel(int(t["turn_id"])))
    keys = {chat_key} if chat_key else set()
    s = await _db(store.session_for_deal, deal_id)
    if s is not None:
        keys.add(s["chat_key"])
    for key in keys:
        # Чат забывается целиком, а не помечается `released`: запрет для ИИ идёт по сделке (владелец `engine`), а у клиента в активном
        # режиме может быть другая подходящая сделка, и чат `released` не пустил бы ИИ в неё до уборки через 30 дней.
        await _db(store.drop_session, key)
    now = _now()
    await _db(modes.log_release, deal_id, from_mode, active, int(row["epoch"]), now)
    logger.info("ai_engine: сделка %s %s (%s -> %s), эпоха %s", deal_id, modes.RELEASED_NOTE, from_mode, active, row["epoch"])
    _publish_mode_release(deal_id, from_mode, active, int(row["epoch"]), now)


def _publish_mode_release(deal_id: int, from_mode: str | None, to_mode: str, epoch: int | None, now: float) -> None:
    """Событие снятия в журнал ИИ на экране панели (`ai_engine_events`). Зовётся ПОСЛЕ записи в `mode_release`, которая остаётся
    источником правды: отправка фоном (задача в `_bg`), не задерживает снятие, любой сбой ловится здесь и до снятия не доходит;
    панель недоступна или не знает ручку (движок выкатывается раньше панели): событие теряется, повторы и лог ограничены."""
    try:
        panel_events.enqueue(modes.release_event(deal_id, from_mode, to_mode, epoch, now), _spawn)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: не поставил событие снятия сделки %s в очередь панели", deal_id)


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
    if modes.is_stale(owner, settings.active_mode_id()):
        return 200, {"allowed": False, "reason": "owner_changed"}      # сделка взята в другом режиме: сервису это то же, что смена владельца
    if not settings.pipeline_allowed(owner["pipeline_id"]) and not intercept.is_taken(deal_id):
        # Последний рубеж перед клиентом: словарь воронок с промптом сузился или режим перестал пускать в воронку сделки, а событие сделки из amo
        # ещё не пришло и не сняло её (`left_pipeline`). Нет промпта: не пишем. Перехваченные сделки живут по правилам перехвата, воронка у них любая.
        return 200, {"allowed": False, "reason": "owner_changed"}
    if intercept.manager_replied_after(deal_id):          # перехват: после заметки «ИИ взял сделку» менеджер написал клиенту
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
            await _followup_cancel(deal_id=deal_id, reason="released")
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
        if outcome in ("continue", "handoff_to_human", "release_to_engine") or (
                outcome == "aborted" and not (reason is None or reason == "newer_client_message" or str(reason).startswith("engine_unavailable"))):
            await intercept.finish_reply(sys.modules[__name__], deal_id)     # перехват в режиме «ответить один раз»: сделка возвращается менеджеру
        return 200, {"ok": True}


async def _after_abort(turn: dict, s: dict | None, owner: dict, reason: str | None) -> None:
    if reason == "mode_mismatch":
        await _to_human(s, "mode_mismatch", deal_id=int(turn["deal_id"]), alert=[
            "ИИ-сервис не взял сделку: воронка не совпала с режимом. Диалог передан менеджеру.",
            _client_line(s) if s else "", _link(int(turn["deal_id"])),
        ])
        return
    if str(reason or "").startswith("stage_not_served"):
        # ИИ-сервис: на ЭТОМ этапе сделки (ОПТ, Академия) ИИ не отвечает. Ход закрыт без ответа клиенту, без алерта, без задачи и без передачи человеку, повтора нет
        # (сообщения хода остаются потреблёнными): сделка остаётся у ИИ, чат ждёт следующего сообщения клиента (когда сделка перейдёт на этап, где ИИ отвечает, он
        # ответит как обычно). В лог только код: текст причины пишет сервис, а ему личные данные не нужны, но и нам незачем.
        logger.info("ai_engine: ход %s по сделке %s закрыт без ответа: этап сделки ИИ не обслуживает (stage_not_served), клиент ждёт", turn["turn_id"], turn["deal_id"])
        await _after_turn(s)
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
    await _followup_cancel(chat_key=s["chat_key"] if s is not None else None, deal_id=deal_id, reason="handed_to_human")
    if deal_id is not None:
        await _db(silence.clear, int(deal_id))
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
                  mode=settings.active_mode_id())
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
    await _followup_cancel(deal_id=deal_id, reason="deal_closed")
    await _db(silence.clear, int(deal_id))
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
    if ev["status_id"] in cfg.CLOSED_STATUS_IDS:
        await approval.cancel_for_closed_deal(sys.modules[__name__], lead_id)       # открытое предложение смены ответственного по закрытой сделке снимается
    owner = await _db(store.get_owner, lead_id)
    if owner is not None and not owner["closed"] and ev["status_id"] in cfg.CLOSED_STATUS_IDS:
        await _deal_closed(lead_id, "deal_closed")
        return True
    if modes.is_stale(owner, settings.active_mode_id()):
        await _release_for_mode(owner, settings.active_mode_id())
        return True
    if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"]:
        if (ev["pipeline_id"] is not None and not settings.pipeline_allowed(ev["pipeline_id"])
                and not await _db(intercept.is_taken, lead_id)):
            await _deal_closed(lead_id, "left_pipeline")
            return True
        if ev["pipeline_id"] is not None and settings.pipelines_any() and owner.get("pipeline_id") != ev["pipeline_id"]:
            # Сделку перенесли в другую воронку, а ИИ ведёт любые: запомнить новую, от неё зависит «инициатива» (лестница, УР, касания).
            # Без `pipelines_any` сюда не доходим: другая воронка значила бы «ушла из воронок режима» (выше).
            await _db(store.set_pipeline, lead_id, int(ev["pipeline_id"]))
            owner = dict(owner, pipeline_id=int(ev["pipeline_id"]))
        if _is_payment_event(ev, owner) and await _db(store.claim_event, lead_id, "payment_received"):
            logger.info("ai_engine: сделка %s оплачена - служебное событие payment_received", lead_id)
            await _queue_event(owner, "payment_received")
        return True
    return owner is not None


def _is_payment_event(ev: dict, owner: dict) -> bool:
    """«Оплата получена» в «ОП розница» и TangemShop либо «Статус оплаты = Оплачен» в «Тест» (там нет автоматики счёта)."""
    if ev["status_id"] in cfg.PAYMENT_RECEIVED_STATUSES:
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
        active = settings.active_mode_id()
        for row in await _db(store.list_owned, store.OWNER_AI):
            mine = {int(c) for c in json.loads(row["contact_ids"] or "[]")}
            if not (mine & theirs) or int(row["deal_id"]) == lead_id or modes.is_stale(row, active):
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
        mode_id = settings.active_mode_id()
        owner = await _db(store.get_owner, deal_id)
        if modes.is_stale(owner, mode_id):
            await _release_for_mode(owner, mode_id)                  # ИИ ведёт сделку из другого режима: она уже не его, человек возвращает её заново
            owner = await _db(store.get_owner, deal_id)
        if owner is None or owner["closed"] or owner["owner"] == store.OWNER_AI:
            return 409, {"detail": "not_returnable"}
        if not settings.pipeline_allowed(owner["pipeline_id"]) and not await _db(intercept.is_taken, deal_id):
            return 409, {"detail": "pipeline_not_allowed"}
        if not await _ai_is_up():
            return 503, {"detail": "ai_unavailable"}
        owner = await _db(store.set_owner, deal_id, store.OWNER_AI, reason="dialog_returned", mode=mode_id)
        await _queue_event(owner, "dialog_returned")
        _spawn(ai_engine_tasks.release(deal_id))      # ИИ снова ведёт диалог: задачи «Связаться» по передаче закрываем
        return 200, {"ok": True, "epoch": owner["epoch"]}


async def give_deal(deal_id: int, chat_id: str, chat_type: str, channel_id: str, phone: str = "",
                    contact_name: str = "", force: bool = False, retrigger: bool = False) -> tuple[int, dict]:
    """Человек отдаёт ИИ существующую сделку (`deal_given`): `owner = ai`, `epoch + 1`, ход-событие сразу.

    Сделка выбирается тем же алгоритмом, что и при входе по сообщению клиента (`dedup.resolve`): чат клиента должен давать ровно эту
    сделку без правил A/D, контакт должен быть в списке режима, воронка в режиме. Иначе отказ с причиной, ничего не меняется.
    Подходит сделке без владельца и сделке у человека; сделку, которую ИИ уже ведёт, или закрытую не берём."""
    mode = settings.effective_mode()
    if mode == "off":
        return 409, {"detail": "mode_off"}
    mode_id = settings.active_mode_id()
    chat_id, channel_id = str(chat_id or "").strip(), str(channel_id or "").strip()
    if not chat_id or not channel_id:
        return 422, {"detail": "chat_required"}
    res = await dedup.resolve(chat_id, phone, known_deal_id=deal_id if retrigger else None,
                              force_deal_id=deal_id if force else None)       # сеть без общего замка, как в `_process_collecting`
    if res.kind == dedup.K_ERROR:
        return 503, {"detail": "amo_unavailable"}
    if res.kind in (dedup.K_NONE, dedup.K_GONE):
        return 404, {"detail": "deal_not_found"}
    if res.kind == dedup.K_SKIP:
        return 409, {"detail": "contact_not_in_list"}
    if res.kind == dedup.K_HUMAN:
        return 409, {"detail": "needs_human", "rule": res.rule, "others": [int(ld["id"]) for ld in (res.others or [])]}
    deal = res.deal
    if deal is None or int(deal["id"]) != int(deal_id):
        return 409, {"detail": "other_deal_chosen", "deal_id": int(deal["id"]) if deal else None}
    async with _get_lock():
        owner = await _db(store.get_owner, deal_id)
        if modes.is_stale(owner, mode_id):
            # Сделку ИИ взял в другом режиме: она уже не его. Человек отдаёт её заново, это новое ведение в активном режиме.
            await _release_for_mode(owner, mode_id)
            owner = await _db(store.get_owner, deal_id)
        if owner is not None and owner["closed"]:
            return 409, {"detail": "deal_closed"}
        if owner is not None and owner["owner"] == store.OWNER_AI:
            if not retrigger:
                return 409, {"detail": "already_ai"}
            # Перезапуск хода по сделке, которую ИИ уже ведёт (например, ответ клиента пришёл, пока канал не слушался): то же событие
            # `deal_given`, владелец, эпоха и пометки передачи не меняются.
            if not await _ai_is_up():
                return 503, {"detail": "ai_unavailable"}
            await _queue_event(owner, "deal_given")
            await _clear_marks(deal_id, "перезапуск хода кнопкой «Дать сделку ИИ»")
            logger.info("ai_engine: ход по сделке %s перезапущен вручную", deal_id)
            return 200, {"ok": True, "retriggered": True, "epoch": owner["epoch"], "deal_id": int(deal_id)}
        if _autopilot_busy(chat_id, phone, deal_id):
            return 409, {"detail": "autopilot_busy"}
        if not await _ai_is_up():
            return 503, {"detail": "ai_unavailable"}
        chat_key = chat_key_of(channel_id, chat_id)
        chat = {"chat_id": chat_id, "chat_type": chat_type, "channel_id": channel_id, "chat_key": chat_key}
        owner = await _hand_over(deal, chat=chat, phone=phone, contact_name=contact_name, mode_id=mode_id, forced=force,
                                 reason="deal_given", note="сделка передана ИИ-менеджеру, дальше переписку ведёт он")
        await _clear_marks(deal_id, "передача кнопкой «Дать сделку ИИ»")
    logger.info("ai_engine: сделка %s передана ИИ (эпоха %s, режим %s)", deal_id, owner["epoch"], mode_id)
    return 200, {"ok": True, "epoch": owner["epoch"], "deal_id": int(deal_id)}


async def _hand_over(deal: dict, *, chat: dict[str, Any], phone: str, contact_name: str, mode_id: str, forced: bool, reason: str, note: str,
                     release_tasks: bool = True) -> dict:
    """Общая часть «сделку отдали ИИ без нового сообщения клиента»: `give_deal` (человек нажал «Дать сделку ИИ») и подбор существующих (`ai_engine_sweep`, и
    подтверждение предложения, созданного подбором, `ai_engine_approval`). Зовётся под общим замком, когда все проверки уже пройдены. Делает то же самое, что делал
    `give_deal`: `owner = ai` (эпоха + 1, `reason` в записи владельца: для журнала различать ручную передачу и подбор), отметка передачи `give.mark` (от неё
    `manager_cutoff` хода: сообщения менеджера ДО передачи ИИ не останавливают), охрана инициативы для «существующей» сделки (`filters.guard_for_given`; фильтры и
    бюджет здесь НЕ проверяются, их проверил вызывающий), сессия чата, служебное событие `deal_given` (ИИ-сервис его знает, нового события нет) и примечание
    в сделку. `release_tasks`: закрыть системные задачи приёма лида («Связаться», «Новый заказ с сайта»); подбор их не трогает. Возвращает строку владельца."""
    deal_id = int(deal["id"])
    chat_key, chat_id, channel_id = chat["chat_key"], chat["chat_id"], chat["channel_id"]
    owner = await _db(
        store.set_owner, deal_id, store.OWNER_AI, reason=reason, chat=chat,
        pipeline_id=int(deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(deal), mode=mode_id,
    )
    await _db(give.mark, deal_id, _now(), forced, mode_id)
    await _db(filters.guard_for_given, deal, mode_id)      # «существующая» сделка: инициатива по ней ждёт первого сообщения ИИ; фильтры к ручной передаче не применяются
    await _db(store.put_session, chat_key, channel_id=channel_id, chat_id=chat_id, chat_type=chat.get("chat_type") or "", phone=phone,
              contact_name=contact_name, state=store.S_AI, deal_id=deal_id, first_batch=0, last_message_id="",
              next_at=None, has_media=0)
    await _queue_event(owner, "deal_given")
    if release_tasks:
        _spawn(ai_engine_tasks.release_intake(deal_id))   # системные «Связаться»/«Новый заказ с сайта»: клиентом теперь занят ИИ
    _spawn(_followup_note(deal_id, note))
    return owner


async def _clear_marks(deal_id: int, why: str) -> None:
    """Ручная передача ИИ снимает обе отметки людей со сделки (`ai_engine_marks`); в журнал отметок идёт строка `cleared`. Не бросает."""
    try:
        cleared = await _db(marks.clear, int(deal_id), why, _now())
        if cleared:
            logger.info("ai_engine: сделка %s: отметка человека (%s) снята: %s", deal_id, ", ".join(cleared), why)
    except Exception:  # noqa: BLE001 - снятие отметки не должно отменять уже сделанную передачу
        logger.exception("ai_engine: не снял отметки со сделки %s", deal_id)


async def mark_deal(deal_id: int, kind: str, by: str = "") -> tuple[int, dict]:
    """Отметка человека по сделке из панели (`POST /internal/ai/v1/deal_mark`, `ai_engine_marks`): «не требует ответа» (`no_reply`) и «удалить» (`removed`).

    Ответ 200 `{"ok": true, "state": "marked" | "already_removed", "kind": <действующая отметка>, "repeat": bool, ...}`; повтор безопасен (см. `marks.put`).
    422 `bad_kind` / `bad_deal`. Работает и при режиме `off`: отметка человека не зависит от режима, а ИИ-сделки в этом режиме и так уходят людям.
    Действия:
      • `no_reply`: ждущий ход и черновик по текущему сообщению снимаются (ход отменён у сервиса, ждущие сообщения и событие `deal_given` сброшены, открытое
        предложение смены ответственного снято), владелец не меняется; сделка выпадает из подбора, пока клиент не напишет новое сообщение;
      • `removed`: ИИ сделку больше не ведёт никогда. Ведёт ИИ: владелец `human` (причина `removed`) без алерта и без задачи, ход и касание отменены. Никто не ведёт:
        запись владельца `human` создаётся, чтобы и вход по сообщению, и подбор считали сделку человеческой навсегда."""
    if kind not in marks.KINDS:
        return 422, {"detail": "bad_kind"}
    try:
        deal_id = int(deal_id)
    except (TypeError, ValueError):
        return 422, {"detail": "bad_deal"}
    if deal_id <= 0:
        return 422, {"detail": "bad_deal"}
    async with _get_lock():
        put = await _db(marks.put, deal_id, kind, _now(), by)
        out: dict[str, Any] = {"ok": True, "state": put["state"], "kind": put["kind"], "repeat": put["repeat"]}
        if put["state"] != "already_removed":
            out.update(await (_mark_removed(deal_id) if put["kind"] == marks.KIND_REMOVED else _mark_no_reply(deal_id)))
    logger.info("ai_engine: сделка %s: отметка человека «%s» %s", deal_id, put["kind"], "повторена" if put["repeat"] else "поставлена")
    return 200, out


async def _mark_removed(deal_id: int) -> dict[str, Any]:
    """«Удалить»: ИИ эту сделку больше не ведёт. Под общим замком. Алертов и задач нет."""
    owner = await _db(store.get_owner, deal_id)
    s = await _db(store.session_for_deal, deal_id)
    await approval.cancel_for_deal(sys.modules[__name__], deal_id, "сделку убрали отметкой «удалить»")
    if owner is not None and owner["closed"]:
        return {"released": False}                                   # сделка закрыта: владельца не трогаем, отметка записана
    released = owner is not None and owner["owner"] == store.OWNER_AI
    if released or s is not None:
        await _to_human(s, "removed", deal_id=deal_id, alert=None)   # владелец `human`, ходы и касание сняты, лестница тишины снята, чат человеческий
    elif owner is None or owner["owner"] == store.OWNER_ENGINE:
        await _db(store.set_owner, deal_id, store.OWNER_HUMAN, reason="removed")
    return {"released": released}


async def _mark_no_reply(deal_id: int) -> dict[str, Any]:
    """«Не требует ответа»: ждущий ход и черновик по текущему сообщению снимаются, владелец остаётся. Под общим замком."""
    owner = await _db(store.get_owner, deal_id)
    await approval.cancel_for_deal(sys.modules[__name__], deal_id, "отметка «не требует ответа»")
    s = await _db(store.session_for_deal, deal_id)
    if s is not None and s["state"] == store.S_SKIP:
        await _db(store.drop_session, s["chat_key"])                 # предложение закрыто и оставило чат в `skip`: следующее сообщение клиента не должно теряться
        s = None
    cancelled = dropped = 0
    if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"]:
        for t in await _db(store.cancel_active_turns, deal_id):
            _spawn(client.post_cancel(int(t["turn_id"])))             # сервис не отправит ничего без `permit`, а `permit` по отменённому ходу отказывает
            cancelled += 1
        if s is not None and s["state"] in (store.S_COLLECTING, store.S_IN_TURN):
            dropped = await _db(store.discard_pending, s["chat_key"])
            fields: dict[str, Any] = {"state": store.S_AI, "next_at": None, "current_turn": None, "has_media": 0}
            try:
                if s.get("pending_event") and json.loads(s["pending_event"]).get("type") == "deal_given":
                    fields["pending_event"] = None                  # ждущая передача «перечитай и ответь» тоже по этому сообщению
            except ValueError:
                fields["pending_event"] = None
            await _db(store.put_session, s["chat_key"], **fields)
    return {"released": False, "cancelled_turns": cancelled, "dropped_messages": dropped}


# =============================================================================================
# лестница тишины (ai_engine_silence)
# =============================================================================================

async def reassign_decision(ref: str, deal_id: int, decision: str, decided_by: str) -> tuple[int, dict]:
    """Решение человека по предложению смены ответственного (`POST /internal/ai/v1/reassign/decision`): `approve` или `reject`. Вся логика в `ai_engine_approval`."""
    return await approval.decide(sys.modules[__name__], ref, deal_id, decision, decided_by)


async def silence_arm(deal_id: int) -> None:
    """ИИ получил разрешение написать клиенту: с этого момента идёт отсчёт тишины (первое сообщение отсчёт задаёт, остальные не сдвигают).
    Отсчёт не взводится, если лестницу выключили в панели или воронка сделки «только отвечаем»; время сообщения для окон УР пишется всегда."""
    try:
        owner = await _db(store.get_owner, int(deal_id))
        if await _db(intercept.is_taken, int(deal_id)):
            return                                          # перехваченная сделка: только консультация, лестницы тишины и окон УР нет
        if owner is not None and owner["owner"] == store.OWNER_AI and not owner["closed"] and owner.get("chat_key"):
            if settings.silence_config()["enabled"] and settings.initiative_allowed(owner.get("pipeline_id")):
                await _db(silence.arm, int(deal_id), owner["chat_key"], _now())
            await _db(ur_wave.note_sent, int(deal_id), _now())
    except Exception:  # noqa: BLE001 - отсчёт тишины не должен ломать отправку
        logger.exception("ai_engine: отсчёт тишины по сделке %s не взведён", deal_id)


def _span_text(seconds: float) -> str:
    """Срок словами для алерта: два часа это «двух часов» (так было всегда), остальное минутами."""
    minutes = round(seconds / 60)
    return "двух часов" if minutes == 120 else f"{minutes} мин"


async def _silence_tick() -> None:
    sil = settings.silence_config()
    if not sil["enabled"]:
        # Функцию выключили в панели: стоящие отсчёты снимаются (при включении отсчёт начнётся заново с первого сообщения ИИ, а не поднимет
        # лестницу по всем старым сделкам разом). События не создаются, алерт «клиент молчит» не уходит. Как при смене режима, клиенту и в amo ничего не пишется.
        if await _db(silence.clear_all):
            logger.info("ai_engine: лестница тишины выключена в настройках, стоящие отсчёты сняты")
        return
    now = _now()
    for row in await _db(silence.due, now, sil["alt_after_s"], sil["alert_after_s"]):
        try:
            async with _get_lock():
                await _silence_step(row)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: ошибка лестницы тишины по сделке %s", row.get("deal_id"))


async def _silence_step(row: dict) -> None:
    now = _now()
    deal_id = int(row["deal_id"])
    owner = await _db(store.get_owner, deal_id)
    if modes.is_stale(owner, settings.active_mode_id()):
        await _release_for_mode(owner, settings.active_mode_id())
        return
    if not filters.initiative_allowed_for_deal(owner):
        return                                              # существующая сделка: ИИ ещё не писал клиенту, лестница ждёт (`ai_engine_filters`)
    if owner is None or owner["closed"] or owner["owner"] != store.OWNER_AI or settings.effective_mode() == "off":
        await _db(silence.clear, deal_id)
        return
    sil = settings.silence_config()
    if not sil["enabled"] or not await _initiative_ok(owner):
        await _db(silence.clear, deal_id)                   # функцию выключили или сделку перенесли в воронку «только отвечаем»: отсчёт снят
        return
    if not settings.send_window()["open_now"]:
        return                                              # вне рабочих часов ИИ отсчёт ждёт
    if row["stage"] == 0:
        await _db(silence.set_stage, deal_id, 1, now)
        if await _ai_is_up():
            alt_min = round(sil["alt_after_s"] / 60)
            await _queue_event(owner, "silent_alt", extra={
                "reason": "клиент молчит час после нашего сообщения" if alt_min == 60 else f"клиент молчит {alt_min} мин после нашего сообщения"})
            logger.info("ai_engine: сделка %s: клиент молчит %s мин, ИИ пробует другой канал", deal_id, alt_min)
        return
    await _db(silence.set_stage, deal_id, 2, now)
    s = await _db(store.session_for_deal, deal_id)
    total = _span_text(sil["alt_after_s"] + sil["alert_after_s"])
    await _alert("silent_alert", [f"Клиент не отвечает на сообщения ИИ уже больше {total}, другой канал пробовали.",
                                  "Нужно связаться с клиентом вручную.", _client_line(s), _link(deal_id)],
                 dedupe=f"silent_alert:{deal_id}")


_MSK = datetime.timezone(datetime.timedelta(hours=3))


def _msk_at(ts: float, hhmm: str) -> float:
    """Время `hhmm` по Москве в тот же календарный день, что и момент `ts`."""
    d = datetime.datetime.fromtimestamp(ts, _MSK)
    h, m = (int(x) for x in hhmm.split(":"))
    return d.replace(hour=h, minute=m, second=0, microsecond=0).timestamp()


async def _ur_tick() -> None:
    """Окна перевода в УР: событие `ur_wave` всем сделкам ИИ в 17:50, а тем, кому ИИ писал в 17:00 и позже, во втором окне в 18:45."""
    ur = settings.ur_config()                                 # включение и времена из настроек режима (запас: AI_UR_WAVE_1, AI_UR_WAVE_2, AI_UR_LATE_FROM)
    now = _now()
    day = datetime.datetime.fromtimestamp(now, _MSK).strftime("%Y-%m-%d")
    if not ur["enabled"]:
        # Окна выключены (панель, Перехват или пусто в AI_UR_WAVE_1): события не создаются, ждавшие второго окна сегодня его не получат,
        # даже если окна включат снова до вечера. Клиенту и в amo ничего не пишется.
        if await _db(ur_wave.cancel_deferred, day, now):
            logger.info("ai_engine: окна УР выключены в настройках, ожидавшие второго окна сняты")
        return
    first_at = cfg.UR_WAVE_1_BY_DAY.get(day) or ur["wave1"]
    t1, t2, late = _msk_at(now, first_at), _msk_at(now, ur["wave2"]), _msk_at(now, ur["late_from"])
    if now < t1 or settings.effective_mode() == "off":
        return
    async with _get_lock():
        active = settings.active_mode_id()
        for owner in await _db(store.list_owned, store.OWNER_AI):
            deal_id = int(owner["deal_id"])
            if modes.is_stale(owner, active):
                await _release_for_mode(owner, active)      # сделка из другого режима окон УР не получает: её снимаем
                continue
            if not filters.initiative_allowed_for_deal(owner):
                continue                                    # существующая сделка: ИИ ещё не писал клиенту, окна УР ждут (`ai_engine_filters`)
            if await _db(intercept.is_taken, deal_id):
                continue                                    # перехваченная сделка: окна УР не её дело
            if not settings.initiative_allowed(owner.get("pipeline_id")):
                continue                                    # воронка «только отвечаем»: окон УР по ней нет
            try:
                w1 = await _db(ur_wave.state, day, deal_id, 1)
                if w1 is None:
                    last = await _db(ur_wave.last_sent, deal_id)
                    if last is not None and last >= late:
                        await _db(ur_wave.mark, day, deal_id, 1, "deferred", now)         # писали поздно: во второе окно
                    elif await _fire_ur_wave(owner, 1, first_at):
                        await _db(ur_wave.mark, day, deal_id, 1, "fired", now)
                elif w1 == "deferred" and now >= t2 and await _db(ur_wave.state, day, deal_id, 2) is None:
                    if await _fire_ur_wave(owner, 2, ur["wave2"]):
                        await _db(ur_wave.mark, day, deal_id, 2, "fired", now)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("ai_engine: окно УР по сделке %s не отработало", deal_id)


async def _fire_ur_wave(owner: dict, wave: int, hhmm: str) -> bool:
    if not await _ai_is_up():
        return False                                        # повторим на следующем проходе
    await _queue_event(owner, "ur_wave", extra={"reason": f"окно перевода в УР, волна {wave} ({hhmm} МСК)"})
    logger.info("ai_engine: сделка %s: окно УР, волна %s", owner["deal_id"], wave)
    return True


async def send_failed(deal_id: int, code: str, reason: str) -> dict[str, Any]:
    """ИИ-сервис не смог отправить клиенту сообщение (Wazzup отказал, канала нет): примечание в сделку, алерт в чат ОП с пингами и срочная задача
    «Связаться с клиентом» на ответственного. Владелец сделки НЕ меняется, ИИ остаётся ведущим. Один раз на сделку и причину за 15 минут:
    повторы (повторное одобрение, повтор хода) ничего не дублируют. Не бросает: сбой одной части не мешает остальным.

    Выключено «алерт сразу при неудачной отправке» (`functions.silence.alert_on_failed_send`): примечание, алерт и задача не создаются,
    остаётся строка в логе; о молчании клиента менеджеров предупредит лестница тишины, пока она включена. Не зависит от самой лестницы."""
    if not settings.silence_config()["alert_on_failed_send"]:
        logger.warning("ai_engine: сделка %s: отказ отправки клиенту (%s), алерт сразу выключен в настройках режима", deal_id, str(code)[:80])
        return {"ok": True, "deduped": False, "skipped": "alert_on_failed_send_off"}
    now = _now()
    key = f"{int(deal_id)}:{str(code)[:80]}"
    if now - _send_failed_seen.get(key, 0) < SEND_FAILED_DEDUPE_S:
        return {"ok": True, "deduped": True}
    _send_failed_seen[key] = now
    if len(_send_failed_seen) > 2000:
        _send_failed_seen.clear()
    text = " ".join(str(reason or "причина не названа").split())[:300]
    try:
        await asyncio.wait_for(amo_service.add_note(
            int(deal_id), f"Не удалось отправить клиенту сообщение: {text}. Нужно связаться с клиентом вручную."), timeout=15)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: примечание об отказе отправки в сделку %s не записалось", deal_id)
    s = await _db(store.session_for_deal, int(deal_id))
    await _alert("send_failed", [f"ИИ не смог отправить клиенту сообщение: {_clean(text)}.", "Нужно связаться с клиентом вручную.",
                                 _client_line(s), _link(int(deal_id))])
    await ai_engine_tasks.create_send_failure_task(int(deal_id), str(code)[:80], text)
    logger.warning("ai_engine: сделка %s: отказ отправки клиенту (%s), люди позваны", deal_id, code)
    return {"ok": True, "deduped": False}


async def raise_alert(key: str, lines: list[str]) -> None:
    """Алерт по требованию человека (админская ручка): тот же путь, что у алертов ИИ (шапка, пинги, дедупликации нет)."""
    await _alert(key, lines)


# Дежурные алерты ИИ-сервиса (`POST /team_alert`, 23.11.2026): ИИ зовёт руководителя или передаёт, что у клиента
# не работает сайт или приложение. Ключи совпадают с каталогом «Уведомлений» панели Team: панель решает выключатель, канал,
# шаблон и получателей; пока она про ключ не знает, уходит текст ниже в «Отдел продаж, Уведомления».
DUTY_EVENTS: dict[str, str] = {
    "ai_alert_head": "🙋 ИИ-менеджер: нужен руководитель",
    "ai_alert_tech": "🛠 ИИ-менеджер: не работает сайт или приложение",
}
DUTY_TEXT_MAX = 500
DUTY_DEDUPE_S = 600
_duty_seen: dict[str, float] = {}       # «сделка:событие:текст» -> когда отправили; повтор за 10 минут не шлём


async def duty_alert(deal_id: int, event: str, text: str) -> str:
    """Дежурный алерт по событию панели `event` (`DUTY_EVENTS`). Возвращает `sent`, `disabled` (выключено в панели или её канал
    у интеграции не настроен), `duplicate` (тот же текст по той же сделке за 10 минут) или `failed` (Телеграм не принял). Не бросает.
    Переменные шаблона (каталог Team, 23.11.2026): `{{текст_события}}` (текст ИИ), `{{сделка}}` (название сделки из amo, не
    прочиталось - «Сделка №<номер>»), `{{ссылка_на_сделку}}` (адрес только здесь), `{{клиент}}` (имя из чата ИИ, нет - пусто), `{{теги}}`; плюс `{{текст}}` с тем же текстом ИИ.
    ⚠️ `текст_события` в `alert_templates.HTML_VARIABLES`: рендер его не экранирует, поэтому экранируем здесь, иначе угловая скобка
    в словах клиента уронит сообщение в HTML-разметке."""
    now = _now()
    key = f"{int(deal_id)}:{event}:{text}"
    if now - _duty_seen.get(key, 0) < DUTY_DEDUPE_S:
        return "duplicate"
    if len(_duty_seen) > 500:
        for k, at in list(_duty_seen.items()):
            if now - at >= DUTY_DEDUPE_S:
                _duty_seen.pop(k, None)
    _duty_seen[key] = now                   # до отправки: параллельный повтор того же запроса вторым сообщением не уйдёт
    try:
        try:
            s = await _db(store.session_for_deal, int(deal_id))
        except Exception:  # noqa: BLE001
            s = None                        # имя клиента не главное: без него алерт всё равно нужен
        client_name = " ".join(str((s or {}).get("contact_name") or "").split())
        try:
            lead = await asyncio.wait_for(amo_service.get_lead_full(int(deal_id), with_=()), timeout=5)
        except Exception:  # noqa: BLE001
            lead = None                     # amo молчит: алерт важнее названия сделки
        deal_name = " ".join(str((lead or {}).get("name") or "").split()) or f"Сделка №{int(deal_id)}"
        head = DUTY_EVENTS[event] + (" (тест)" if settings.effective_mode() == "test" else "")
        mention = mentions_for(None)
        if cfg.ALERT_EXTRA_TAG and cfg.ALERT_EXTRA_TAG not in mention.split():
            mention = f"{mention} {cfg.ALERT_EXTRA_TAG}"
        legacy = "\n".join(line for line in (
            head, mention, f"👤 {html.escape(client_name)}" if client_name else "", f"💬 {html.escape(text)}", _link(int(deal_id)),
        ) if line)
        d = alerts.decide(
            event, legacy_text=legacy, parse_mode="HTML", chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID,
            lead=lead if isinstance(lead, dict) and lead.get("custom_fields_values") is not None else None,
            values={"сделка": deal_name, "ссылка_на_сделку": _link(int(deal_id)),
                    "клиент": client_name, "текст_события": html.escape(text, quote=False), "текст": text, "теги": mention},
        )
        if d is None:
            _duty_seen.pop(key, None)
            logger.info("ai_engine: сделка %s: алерт %s выключен в панели", deal_id, event)
            return "disabled"
        ok = await asyncio.wait_for(_deliver_alert(d), timeout=10)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: сделка %s: алерт %s не ушёл", deal_id, event)
        ok = False
    if not ok:
        _duty_seen.pop(key, None)           # не дошло: повтор сервиса должен пройти, а не упереться в дубль
        logger.warning("ai_engine: сделка %s: алерт %s не принят Телеграмом", deal_id, event)
        return "failed"
    logger.info("ai_engine: сделка %s: алерт %s отправлен", deal_id, event)
    return "sent"


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


async def _alert(key: str, lines: list[str], *, dedupe: str | None = None, return_deal: int | None = None, ping: bool = True) -> None:
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
        mention = mentions_for(None) if ping else ""                 # `ping=False`: сообщение в чат ОП без шапки и пингов, одной строкой
        if ping and cfg.ALERT_EXTRA_TAG and cfg.ALERT_EXTRA_TAG not in mention.split():
            mention = f"{mention} {cfg.ALERT_EXTRA_TAG}"            # пинг Кати во всех алертах ИИ-менеджера
        text = "\n".join([head, mention, *[line for line in lines if line]]) if ping else chr(10).join(line for line in lines if line)
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
        mode=settings.effective_mode(), mode_id=settings.active_mode_id(), settings_version=settings.settings_version(),
        settings_age_s=settings.age_s(),
        ai_up=_ai_ok_at is not None and (_now() - _ai_ok_at) <= max(3 * cfg.HEALTH_POLL_S, 45),
        loops_alive=sum(1 for t in _tasks if not t.done()),
        task_guard={"enabled": not cfg.TASK_GUARD_DISABLED, "retry_s": cfg.TASK_RETRY_S},      # правило «открытая задача сотрудника» на пути по сообщению клиента (`_gate_take`)
    )
    try:
        out["counts"] = store.counts()
    except Exception:  # noqa: BLE001
        out["counts"] = "ошибка чтения базы"
    try:
        # Что движок СЕЙЧАС берёт из настроек (этап 2): после выката сверить с панелью без чтения логов.
        out["pipelines_any"] = settings.pipelines_any()
        out["pipelines_any_allowed"] = settings.pipelines_any_allowed()          # разрешено ли «любая воронка» переменной движка `AI_PIPELINES_ANY_ALLOWED`
        out["initiative_pipelines"] = sorted(settings.initiative_pipelines())
        out["functions"] = settings.functions_wire()
    except Exception:  # noqa: BLE001
        out["functions"] = "ошибка чтения настроек"
    try:
        # Воронки, на которые есть промпт (`ai_engine_prompts`): откуда они (`service` | `cached` | `static`) и сколько секунд данным.
        out.update(prompts.status_fields(settings.effective_mode()))
    except Exception:  # noqa: BLE001
        out["prompt_pipelines"] = "ошибка чтения словаря воронок"
    out["return_by_reaction"] = ai_engine_return.stats()
    try:
        out["reassign_proposals_open"] = approval.count_open()       # предложения смены ответственного, ждущие решения человека (`ai_engine_approval`)
    except Exception:  # noqa: BLE001
        out["reassign_proposals_open"] = "ошибка чтения базы"
    try:
        out["sweep"] = sweep.status_block(_now())                     # подбор существующих сделок (`ai_engine_sweep`): настройки, бюджет, исходы за сутки, отметки людей
    except Exception:  # noqa: BLE001
        out["sweep"] = "ошибка чтения базы"
    return out
