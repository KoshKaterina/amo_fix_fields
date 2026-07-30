"""Перенос УР(142)/ЗНР(143) сделок [CLEVER] Основная в целевую воронку/этап
вместо нативного копирования (F5-виджет / «Создать сделку»).

Правила (условия читаются по СВЕЖЕЙ дочитанной сделке, не по телу вебхука —
select-поля сверяются по enum_id, не по тексту, чтобы не зависеть от того, как
менеджер видит подпись значения):

  УР (142), источник [CLEVER] Основная:
    1. Тип доставки содержит «курьером по москве» + Тип заявки=Заказ +
       Склад∈{Основной,Вскрытые} → Офис/«Оформить доставку»
    2. Тип доставки содержит «самовывоз из офиса» + Тип заявки=Заказ +
       Склад∈{Основной,Вскрытые} → Офис/«Самовывоз»
    3. Тип заявки=Заказ + Тип доставки содержит CDEK/СДЭК + Склад∈{Основной,
       Вскрытые} → Офис/«Сделать накладную». Схлопнуты правила 3+6+7 исходного
       списка Тианы — все три вели в один и тот же этап (#6 сама Тиана
       пометила дублем #3, #7 добавлял избыточный тег «тест» поверх того же
       условия).
    4. Тип заявки=Предзаказ → Офис/«Предзаказ оплачен»
    5. Тип заявки=Заказ + Склад=ЭРМС_Основной → Фулфилмент/«КОНТРОЛЬ»

  ЗНР (143), поле «Причина ЗИН» (577623, он же DUP_REASON_FIELD_ID):
    1. Причина ЗИН=Лист ожидания → воронка «Лист ожидания»/«Лист ожидания»
    2. Причина ЗИН=Академия → воронка «Академия»/«Первичный контакт»
       (переносим РЕАЛЬНУЮ сделку со всей историей — подтверждено Катей, а не
       создаём пустую копию, как делала прежняя нативная автоматика)

Смена ответственного на Екатерину Зубалий (RESPONSIBLE_OFFICE_MANAGER_USER_ID)
+ прежний ответственный в поле 578151 — ТОЛЬКО при переносе в Офис. Остальные
воронки (Фулфилмент/Лист ожидания/Академия) — ответственный не меняется.

«Онлайн чат» (реактивация клиента, пишущего в закрытую УР/ЗНР сделку) — ВНЕ
ОБЪЁМА этой доработки, не реализовано здесь (см. план: требует отдельного
спайка по обнаружению триггера).

Каждое правило — за собственным флагом (OFFICE_TRANSFER_RULE_*) поверх общего
OFFICE_TRANSFER_ENABLED: Тиана включает правило только после того, как
отключит соответствующую нативную автоматику в Digital Funnel.

Надёжность (первый приоритет по требованию задачи): быстрый путь — вебхук
(LANE_AMO, PRIORITY_NEW) → enqueue_office_transfer → process_office_transfer.
Страховка — периодическая reconciliation по ОКНУ ВРЕМЕНИ через /api/v4/events
(НЕ «все сделки, сейчас сидящие в статусе» — иначе задело бы сделки, висевшие
в УР/ЗНР ДО включения фичи, что прямо запрещено). Провал — тег + примечание +
Telegram-алерт; ретраи продолжаются каждый reconciliation-проход, тег не
блокирует повторные попытки (сделка никогда не теряется молча).

⚠️ ИЗВЕСТНЫЙ РИСК, обнаруженный при реализации (вне согласованного объёма
правок — ПРОВЕРЕНО, но НЕ исправлено здесь, нужно отдельное решение Кати):
metrika_sync.py._resolve_clever() резолвит «оригинал в CLEVER» для дубля в
Офисе/Фулфилменте по УСЛОВИЮ, что оригинал и дубль — РАЗНЫЕ сделки, связанные
полем 576689. После переноса (а не копирования) это больше не так — сделка
одна и та же. Для COD-заказов («наложка», see metrika_sync._classify: Офис/УР
и Фулфилмент/09-09.2 требуют need_resolve_clever=True) резолв не найдёт
сиблинга и заказ молча выпадет из отправки в Яндекс.Метрику (в логе будет
«не нашёл оригинал в CLEVER» — сегодня это ожидаемо для АРХИВНЫХ дублей,
после этой фичи станет систематическим для КАЖДОГО перенесённого COD-заказа).
kontrol_gate.py и ms_status_sync.py — ПРОВЕРЕНЫ, у них нет такого допущения
(читают 576689 с самой сделки / ищут по воронке+UUID без требования отдельного
оригинала), их трогать не нужно. metrika_sync.py — нужно трогать, но это
отдельное решение (влияет на согласованную бизнес-логику аналитики), не
включено в этот PR.
"""

import asyncio
import logging
import time

import amo_service
import tg_recipients
import telegram_bot
from waybill_config import (
    APPLICATION_TYPE_ORDER,
    APPLICATION_TYPE_PREORDER,
    DELIVERY_CDEK_MARKERS,
    DELIVERY_COURIER_MOSCOW_MARKER,
    DELIVERY_SHOWROOM_MARKER,
    DUP_REASON_FIELD_ID,
    FIELD_APPLICATION_TYPE,
    FIELD_DELIVERY_TYPE,
    FIELD_FORMER_RESPONSIBLE,
    FIELD_ORDER_WAREHOUSE,
    OFFICE_TRANSFER_ENABLED,
    OFFICE_TRANSFER_RECONCILE_INTERVAL_S,
    OFFICE_TRANSFER_RULE_UR_DELIVERY,
    OFFICE_TRANSFER_RULE_UR_FULFILLMENT,
    OFFICE_TRANSFER_RULE_UR_PICKUP,
    OFFICE_TRANSFER_RULE_UR_PREORDER,
    OFFICE_TRANSFER_RULE_UR_WAYBILL,
    OFFICE_TRANSFER_RULE_ZNR_ACADEMY,
    OFFICE_TRANSFER_RULE_ZNR_WAITLIST,
    OFFICE_TRANSFER_SINCE_TS,
    OFFICE_TRANSFER_STALE_ALERT_MIN,
    OFFICE_TRANSFER_WAREHOUSES,
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_FULFILLMENT,
    PIPELINE_OFFICE,
    PIPELINE_WAITLIST,
    REASON_ACADEMY,
    REASON_WAITLIST,
    RESPONSIBLE_OFFICE_MANAGER_USER_ID,
    STATUS_ACADEMY_FIRST_CONTACT,
    STATUS_CLOSED_LOST,
    STATUS_CREATE_WAYBILL,
    STATUS_FF_KONTROL,
    STATUS_OFFICE_DELIVERY,
    STATUS_OFFICE_PICKUP,
    STATUS_OFFICE_PREORDER_PAID,
    STATUS_SUCCESS,
    STATUS_WAITLIST,
    TAG_OFFICE_TRANSFER_ERROR,
    WAREHOUSE_ERMS_MAIN,
)

logger = logging.getLogger("uvicorn")

AMO_LEAD_URL = "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{}"


# ════════════════ чтение условий по свежей сделке ════════════════

def _application_type(lead: dict) -> int | None:
    return amo_service.get_custom_field_enum_id(lead, FIELD_APPLICATION_TYPE)


def _warehouse(lead: dict) -> int | None:
    return amo_service.get_custom_field_enum_id(lead, FIELD_ORDER_WAREHOUSE)


def _delivery_text(lead: dict) -> str:
    return str(amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) or "").casefold()


def _reason_enum(lead: dict) -> int | None:
    return amo_service.get_custom_field_enum_id(lead, DUP_REASON_FIELD_ID)


# ════════════════ матчеры правил — каждый: сделка → (pipeline_id, status_id) | None ════════════════

def _match_ur_delivery(lead: dict) -> tuple[int, int] | None:
    if not OFFICE_TRANSFER_RULE_UR_DELIVERY:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) not in OFFICE_TRANSFER_WAREHOUSES:
        return None
    if DELIVERY_COURIER_MOSCOW_MARKER not in _delivery_text(lead):
        return None
    return (PIPELINE_OFFICE, STATUS_OFFICE_DELIVERY)


def _match_ur_pickup(lead: dict) -> tuple[int, int] | None:
    if not OFFICE_TRANSFER_RULE_UR_PICKUP:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) not in OFFICE_TRANSFER_WAREHOUSES:
        return None
    if DELIVERY_SHOWROOM_MARKER not in _delivery_text(lead):
        return None
    return (PIPELINE_OFFICE, STATUS_OFFICE_PICKUP)


def _match_ur_waybill(lead: dict) -> tuple[int, int] | None:
    if not OFFICE_TRANSFER_RULE_UR_WAYBILL:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) not in OFFICE_TRANSFER_WAREHOUSES:
        return None
    text = _delivery_text(lead)
    if not any(marker in text for marker in DELIVERY_CDEK_MARKERS):
        return None
    return (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL)


def _match_ur_preorder(lead: dict) -> tuple[int, int] | None:
    if not OFFICE_TRANSFER_RULE_UR_PREORDER:
        return None
    if _application_type(lead) != APPLICATION_TYPE_PREORDER:
        return None
    return (PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID)


def _match_ur_fulfillment(lead: dict) -> tuple[int, int] | None:
    if not OFFICE_TRANSFER_RULE_UR_FULFILLMENT:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) != WAREHOUSE_ERMS_MAIN:
        return None
    return (PIPELINE_FULFILLMENT, STATUS_FF_KONTROL)


_UR_RULES = (
    _match_ur_delivery,
    _match_ur_pickup,
    _match_ur_waybill,
    _match_ur_preorder,
    _match_ur_fulfillment,
)


def _match_znr_waitlist(lead: dict) -> tuple[int, int] | None:
    if not OFFICE_TRANSFER_RULE_ZNR_WAITLIST:
        return None
    if _reason_enum(lead) != REASON_WAITLIST:
        return None
    return (PIPELINE_WAITLIST, STATUS_WAITLIST)


def _match_znr_academy(lead: dict) -> tuple[int, int] | None:
    if not OFFICE_TRANSFER_RULE_ZNR_ACADEMY:
        return None
    if _reason_enum(lead) != REASON_ACADEMY:
        return None
    return (PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT)


_ZNR_RULES = (
    _match_znr_waitlist,
    _match_znr_academy,
)


def _match_rules(lead: dict, status_id: int) -> tuple[int, int] | None:
    rules = _UR_RULES if status_id == STATUS_SUCCESS else _ZNR_RULES if status_id == STATUS_CLOSED_LOST else ()
    for fn in rules:
        target = fn(lead)
        if target is not None:
            return target
    return None


# ════════════════ диспетчер ════════════════

# lead_id → {"since": monotonic-независимый unix ts первой неудачи, "alerted": bool}
_pending_fail: dict[int, dict] = {}


def _clear_fail(lead_id: int) -> None:
    _pending_fail.pop(int(lead_id), None)


async def _stale_alert(lead: dict, state: dict) -> None:
    if OFFICE_TRANSFER_STALE_ALERT_MIN <= 0 or state["alerted"]:
        return
    age_min = (time.time() - state["since"]) / 60
    if age_min < OFFICE_TRANSFER_STALE_ALERT_MIN:
        return
    state["alerted"] = True
    lead_id = lead.get("id")
    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    await telegram_bot.send_alert(
        f"🚨 Сделка {lead_id} застряла в УР/ЗНР дольше {int(age_min)} мин, "
        f"автоперенос не удался — нужна ручная проверка.\n"
        f"{lead.get('name') or ''}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}",
        chat_id=tg_recipients.NOTIFY_CHAT_ID,
        message_thread_id=tg_recipients.NOTIFY_THREAD_ID,
    )


async def _fail(lead: dict, reason: str) -> None:
    """Перенос не удался: тег + примечание + (по истечении порога) один
    Telegram-алерт. НЕ блокирует повторные попытки reconciliation — тег
    только информирует, что сделка требует внимания."""
    lead_id = int(lead.get("id"))
    logger.warning("office_transfer %s: %s", lead_id, reason)
    state = _pending_fail.setdefault(lead_id, {"since": time.time(), "alerted": False})
    await amo_service.add_tag(lead_id, TAG_OFFICE_TRANSFER_ERROR)
    await amo_service.add_note(
        lead_id,
        f"⚠️ Автоперенос в целевую воронку не выполнен: {reason}. "
        f"Повторные попытки продолжаются автоматически.",
    )
    await _stale_alert(lead, state)


async def process_office_transfer(lead_id, source: str = "webhook") -> str:
    """Обработчик очереди (LANE_AMO) / reconciliation. Возвращает исход
    строкой (лог/тесты). Всегда дочитывает сделку заново — состояние могло
    смениться, пока задача ждала в очереди, или между reconciliation-проходами."""
    if not OFFICE_TRANSFER_ENABLED:
        return "disabled"

    lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        logger.warning("office_transfer %s: сделка не прочиталась", lead_id)
        return "failed-lead-read"

    status_id = int(lead.get("status_id") or 0)
    pipeline_id = int(lead.get("pipeline_id") or 0)
    if pipeline_id != PIPELINE_CLEVER_MAIN or status_id not in (STATUS_SUCCESS, STATUS_CLOSED_LOST):
        # Уже перенесена (нами или вручную), либо это не тот случай — идемпотентный
        # no-op. Гасит и эхо от нашего же PATCH, и повторную доставку вебхука.
        logger.info(
            "office_transfer %s: не в CLEVER/{142,143} (pipeline=%s status=%s) — скип",
            lead_id, pipeline_id, status_id,
        )
        _clear_fail(lead_id)
        return "skipped-not-applicable"

    target = _match_rules(lead, status_id)
    if target is None:
        return "no-match"
    target_pipeline_id, target_status_id = target

    patch_kwargs: dict = {"pipeline_id": target_pipeline_id, "status_id": target_status_id}
    if target_pipeline_id == PIPELINE_OFFICE:
        current_responsible = lead.get("responsible_user_id")
        if current_responsible != RESPONSIBLE_OFFICE_MANAGER_USER_ID:
            former_name = None
            if current_responsible:
                former_name = await amo_service.get_user_name(current_responsible)
            patch_kwargs["responsible_user_id"] = RESPONSIBLE_OFFICE_MANAGER_USER_ID
            patch_kwargs["custom_fields"] = {
                FIELD_FORMER_RESPONSIBLE: former_name or (str(current_responsible) if current_responsible else ""),
            }

    result = await amo_service.patch_lead(lead_id, **patch_kwargs)
    if not result.get("ok"):
        await _fail(lead, f"PATCH не прошёл (status_code={result.get('status_code')})")
        return "failed-patch"

    logger.info(
        "office_transfer %s: перенесена CLEVER/%s → воронка %s / этап %s (source=%s)",
        lead_id, status_id, target_pipeline_id, target_status_id, source,
    )
    _clear_fail(lead_id)

    if target_pipeline_id == PIPELINE_FULFILLMENT and target_status_id == STATUS_FF_KONTROL:
        # Страховка: не проверено live, шлёт ли amo свежий /lead_change вебхук
        # на PATCH, сделанный НАШИМ же кодом (а не UI-действием) — если нет,
        # штатный триггер enqueue_kontrol() в webhooks.py просто не сработает.
        # Дублируем вызов напрямую; enqueue_kontrol сам дедуплицирует по lead_id.
        # source="webhook" (не "office_transfer"!) — намеренно: это заставляет
        # process_kontrol_lead ПЕРЕЧИТАТЬ сделку и сверить, что она ДЕЙСТВИТЕЛЬНО
        # ещё на «КОНТРОЛЬ» к моменту обработки в очереди (гейт под source=="webhook"
        # в kontrol_gate.py) — тот же уровень свежести, что у настоящего вебхука,
        # а не «доверять данным без переповерки», как у батч-источников.
        from queue_manager import enqueue_kontrol
        enqueue_kontrol(lead_id, source="webhook")

    return "moved"


# ════════════════ reconciliation (окно по времени, НЕ «текущий статус») ════════════════

async def _entered_status_leads(pipeline_id: int, status_id: int, ts_from: int, ts_to: int) -> set[int]:
    """ID сделок, ПЕРЕШЕДШИХ в pipeline_id/status_id за окно [ts_from, ts_to) —
    по событию lead_status_changed. Порт _entered_ur_leads из kontrol_gate.py,
    обобщённый на произвольные pipeline/status. НЕ используем
    amo_service.get_leads_by_status() здесь — та возвращает ВСЁ, что сейчас
    сидит в статусе, независимо от времени входа, что нарушило бы требование
    «без ретроактивности» (задело бы сделки, висевшие в УР/ЗНР до cutover)."""
    leads: set[int] = set()
    page = 1
    while True:
        params = [
            ("filter[type]", "lead_status_changed"),
            ("filter[created_at][from]", str(ts_from)),
            ("filter[created_at][to]", str(ts_to)),
            ("filter[value_after][leads_statuses][0][pipeline_id]", str(pipeline_id)),
            ("filter[value_after][leads_statuses][0][status_id]", str(status_id)),
            ("limit", "100"), ("page", str(page)),
        ]
        d = await amo_service._do_get("/api/v4/events", params)
        evs = ((d or {}).get("_embedded") or {}).get("events") or []
        for e in evs:
            va = e.get("value_after") or []
            ls = (va[0].get("lead_status") if va else None) or {}
            if ls.get("id") == status_id and ls.get("pipeline_id") == pipeline_id:
                lid = e.get("entity_id")
                if lid is not None:
                    leads.add(int(lid))
        if len(evs) < 100:
            break
        page += 1
    return leads


_last_reconcile_ts: int = 0
_reconcile_task: asyncio.Task | None = None


async def _reconcile_once() -> str:
    global _last_reconcile_ts
    now = int(time.time())
    window_from = max(_last_reconcile_ts, OFFICE_TRANSFER_SINCE_TS)
    if window_from <= 0:
        logger.warning("office_transfer reconcile: OFFICE_TRANSFER_SINCE_TS не задан — проход пропущен")
        return "skipped-no-cutover"

    leads = await _entered_status_leads(PIPELINE_CLEVER_MAIN, STATUS_SUCCESS, window_from, now)
    leads |= await _entered_status_leads(PIPELINE_CLEVER_MAIN, STATUS_CLOSED_LOST, window_from, now)

    processed = 0
    for lead_id in leads:
        await process_office_transfer(lead_id, source="reconcile")
        processed += 1
    _last_reconcile_ts = now
    logger.info("office_transfer reconcile: окно [%s, %s), сделок в окне %s", window_from, now, processed)
    return f"processed={processed}"


async def _reconcile_loop() -> None:
    while True:
        await asyncio.sleep(OFFICE_TRANSFER_RECONCILE_INTERVAL_S)
        try:
            await _reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("office_transfer reconcile: ошибка прохода")


# ════════════════ config-drift проверка + запуск/остановка из lifespan ════════════════

# (флаг правила, pipeline_id, status_id, метка для лога) — сверяется с прогретым
# кэшем воронок ТОЛЬКО для включённых правил, чтобы не шуметь про ещё не
# скроенные (флаг=off) цели.
_RULE_TARGETS = (
    (OFFICE_TRANSFER_RULE_UR_DELIVERY, PIPELINE_OFFICE, STATUS_OFFICE_DELIVERY, "УР→Офис/Оформить доставку"),
    (OFFICE_TRANSFER_RULE_UR_PICKUP, PIPELINE_OFFICE, STATUS_OFFICE_PICKUP, "УР→Офис/Самовывоз"),
    (OFFICE_TRANSFER_RULE_UR_WAYBILL, PIPELINE_OFFICE, STATUS_CREATE_WAYBILL, "УР→Офис/Сделать накладную"),
    (OFFICE_TRANSFER_RULE_UR_PREORDER, PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID, "УР→Офис/Предзаказ оплачен"),
    (OFFICE_TRANSFER_RULE_UR_FULFILLMENT, PIPELINE_FULFILLMENT, STATUS_FF_KONTROL, "УР→Фулфилмент/КОНТРОЛЬ"),
    (OFFICE_TRANSFER_RULE_ZNR_WAITLIST, PIPELINE_WAITLIST, STATUS_WAITLIST, "ЗНР→Лист ожидания"),
    (OFFICE_TRANSFER_RULE_ZNR_ACADEMY, PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT, "ЗНР→Академия"),
)


async def _alert(text: str) -> None:
    try:
        await telegram_bot.send_alert(text)
    except Exception:
        logger.exception("office_transfer alert failed: %s", text)


async def _validate_enabled_targets() -> None:
    missing = [
        label for enabled, pid, sid, label in _RULE_TARGETS
        if enabled and amo_service.get_status_sort(sid, pid) is None
    ]
    if missing:
        msg = (
            "office_transfer: не найдены в прогретом кэше воронок целевые этапы: "
            f"{', '.join(missing)} — проверьте ID в waybill_config.py (переименовали/удалили этап?)"
        )
        logger.error(msg)
        await _alert(msg)


async def init() -> None:
    """Вызывается из lifespan ПОСЛЕ amo_service.warm_pipeline_cache(). Проверяет
    целевые этапы включённых правил, запускает reconciliation."""
    global _last_reconcile_ts
    if not OFFICE_TRANSFER_ENABLED:
        logger.info("office_transfer: ВЫКЛЮЧЕН (OFFICE_TRANSFER_ENABLED)")
        return

    await _validate_enabled_targets()

    if OFFICE_TRANSFER_SINCE_TS <= 0:
        msg = (
            "office_transfer: OFFICE_TRANSFER_ENABLED=1, но OFFICE_TRANSFER_SINCE_TS не задан — "
            "reconciliation НЕ запущена (иначе задело бы сделки, висевшие в УР/ЗНР до включения). "
            "Вебхук-путь при этом работает."
        )
        logger.error(msg)
        await _alert(msg)
        return

    _last_reconcile_ts = OFFICE_TRANSFER_SINCE_TS
    start_reconcile()


def start_reconcile() -> None:
    global _reconcile_task
    if OFFICE_TRANSFER_RECONCILE_INTERVAL_S <= 0:
        logger.info("office_transfer: reconciliation выключена (OFFICE_TRANSFER_RECONCILE_INTERVAL_S=0)")
        return
    _reconcile_task = asyncio.create_task(_reconcile_loop())
    logger.info(
        "office_transfer: reconciliation каждые %s сек, cutover=%s",
        OFFICE_TRANSFER_RECONCILE_INTERVAL_S, OFFICE_TRANSFER_SINCE_TS,
    )


async def stop_reconcile() -> None:
    global _reconcile_task
    if _reconcile_task is not None:
        _reconcile_task.cancel()
        try:
            await _reconcile_task
        except asyncio.CancelledError:
            pass
        _reconcile_task = None
