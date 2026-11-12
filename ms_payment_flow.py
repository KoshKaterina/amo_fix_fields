"""Запись оплаты в заказ МС: уведомления, автоповтор и триггер «Успешно реализовано» Офиса.

Саму запись делает `ms_payment` (чистая логика, без амо и Телеграма). Здесь всё, что вокруг:

1. Две точки входа.
   - `on_ozon_paid` - зовёт `ozon_invoice._mark_paid`, когда Ozon подтвердил оплату;
   - `process_office_payment` - сделка вошла в «Успешно реализовано» воронки Офис, и это
     самовывоз из офиса или наложка: клиент платит по факту выдачи, пишем остаток до суммы.

2. Уведомления разведены по природе сбоя (решение Кати 05.11.2026):
   - ЛОГИЧЕСКИЕ (данные не сходятся) - в «ОП Уведомления», с тегом ответственного;
   - ТЕХНИЧЕСКИЕ (МС недоступен) - в технический чат (канал `tech`, адрес не задан);
   - переплата и «заказ уже оплачен» - предупреждение в технический чат.
   Все четыре события настраиваются из панели (ключи `ms_payment_*`), пока панель про них
   не знает, работает адрес по умолчанию из этого файла (`alerts.decide` деградирует к нему).

   | исход                                   | событие           | куда          |
   |-----------------------------------------|-------------------|---------------|
   | created-short (оплата меньше заказа)    | ms_payment_short  | ОП Уведомления|
   | created-over (оплата больше заказа)     | ms_payment_over   | тех-чат       |
   | skipped-fully-paid (Ozon, дубль?)       | ms_payment_over   | тех-чат       |
   | skipped-zero-sum, failed-order-shape    | ms_payment_data   | ОП Уведомления|
   | сбой МС после автоповторов              | ms_payment_failed | тех-чат       |
   | created (сумма сошлась)                 | -                 | тихо          |

3. Автоповтор. Сбой МС (`ms_payment.RETRIABLE`) не теряет платёж: он ложится в очередь на
   диске (`MS_PAYMENT_PENDING_PATH`, переживает пересборку контейнера) и повторяется с паузой
   1, 2, 4, 8, 16, 32 минуты и дальше раз в час. Через 30 минут от первого сбоя уходит ОДИН
   технический алерт; через 24 часа - финальный и платёж снимается с очереди. Короткий сбой МС
   не шумит, а затянувшийся доходит до человека в тот же рабочий час. В сухом прогоне очередь не
   заводится: он ничего не пишет и людей не зовёт.

⚠️ Выключатели - в waybill_config.py: OZON_MS_PAYMENT_* и OFFICE_MS_PAYMENT_*; у каждого
режима свой флаг и свой сухой прогон (включён по умолчанию).
"""

import asyncio
import logging
import time

import alerts
import amo_service
import ms_payment
import telegram_bot
import tg_recipients
from ms_payment import RETRIABLE, Result
from shared.files import write_json_atomic
from waybill_config import (
    DELIVERY_SHOWROOM_MARKER,
    FIELD_DELIVERY_TYPE,
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_METHOD,
    MS_PAYMENT_PENDING_PATH,
    MS_PAYMENT_RETRY_ALERT_AFTER_S,
    MS_PAYMENT_RETRY_GIVE_UP_S,
    MS_PAYMENT_RETRY_TICK_S,
    OFFICE_MS_PAYMENT_DRY_RUN,
    OFFICE_MS_PAYMENT_ENABLED,
    OFFICE_MS_PAYMENT_MAX_AGE_H,
    OZON_MS_PAYMENT_DRY_RUN,
    OZON_MS_PAYMENT_ENABLED,
    PIPELINE_OFFICE,
    STATUS_SUCCESS,
    is_cod_payment,
)

logger = logging.getLogger("uvicorn")

# Одно и то же событие по одной сделке - не чаще раза в час (как _fail в ozon_invoice).
_ALERT_GAP_S = 3600.0
_alerted_recent: dict[tuple, float] = {}

# Повтор вебхука УР по одной сделке в пределах окна схлопывается (amo шлёт пачками).
_OFFICE_RECENT_TTL_S = 120.0
_office_recent: dict[int, float] = {}

_office_tasks: set = set()
_retry_task: asyncio.Task | None = None


# ───────────────────────────── уведомления ─────────────────────────────

def _classify(outcome: str, kind: str):
    """(событие, технический?, заголовок) для исхода или None - молчим."""
    if outcome == "created-short":
        return ("ms_payment_short", False,
                "Оплата меньше суммы заказа МС - заказ не оплачен целиком (платёж записан)")
    if outcome == "created-over":
        return ("ms_payment_over", True,
                "Оплата больше суммы заказа МС - платёж записан, проверьте переплату")
    if kind == "ozon" and outcome == "skipped-fully-paid":
        return ("ms_payment_over", True,
                "Оплата Ozon пришла по заказу, уже оплаченному целиком - платёж в МС НЕ записан")
    if outcome == "failed-order-shape" or (kind == "ozon" and outcome == "skipped-zero-sum"):
        return ("ms_payment_data", False,
                "Оплата получена, но в заказе МС не хватает данных - платёж не записан, внесите вручную")
    return None


def _lead_stub(entry_or_lead: dict) -> dict:
    return {
        "id": entry_or_lead.get("id") or entry_or_lead.get("lead_id"),
        "name": entry_or_lead.get("name") or entry_or_lead.get("lead_name") or "",
        "responsible_user_id": entry_or_lead.get("responsible_user_id") or entry_or_lead.get("responsible"),
    }


async def _alert(event_key: str, lead: dict, headline: str, detail: str, facts: dict, *, tech: bool) -> None:
    """Примечание в карточке + сообщение в Телеграм по событию панели `event_key`.

    tech=True: адрес не задан (технический чат по умолчанию), без тегов - как у прочих
    технических событий. tech=False: ОП Уведомления с тегом ответственного.
    Один и тот же исход по одной сделке - не чаще раза в час."""
    lead_id = lead.get("id")
    gap_key = (int(lead_id or 0), event_key, headline)
    now = time.time()
    for k, ts in list(_alerted_recent.items()):
        if now - ts > _ALERT_GAP_S:
            _alerted_recent.pop(k, None)
    if now - _alerted_recent.get(gap_key, 0.0) < _ALERT_GAP_S:
        return
    _alerted_recent[gap_key] = now

    name = lead.get("name") or f"сделка {lead_id}"
    mentions = "" if tech else tg_recipients.mentions_for(lead.get("responsible_user_id"))
    reason = f"{headline}\n{detail}" if detail else headline
    try:
        await amo_service.add_note(lead_id, f"⚠️ {reason}")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("ms_payment: lead %s: не удалось записать примечание", lead_id)
    legacy = f"⚠️ {reason}\n{name}\n{alerts.AMO_BASE_URL}/leads/detail/{lead_id}" + (f"\n{mentions}" if mentions else "")
    d = alerts.decide(
        event_key,
        legacy_text=legacy,
        chat_id=None if tech else tg_recipients.NOTIFY_CHAT_ID,
        thread_id=None if tech else tg_recipients.NOTIFY_THREAD_ID,
        lead=lead if lead.get("custom_fields_values") is not None else None,
        responsible_id=lead.get("responsible_user_id"),
        values={
            "причина": reason,
            "сделка": name,
            "ссылка_на_сделку": alerts.lead_link(lead_id),
            "теги": mentions,
            "заказ": facts.get("order", ""),
            "сумма_заказа": facts.get("sum", ""),
            "оплачено_до": facts.get("payed", ""),
            "платёж": facts.get("paid", ""),
            "итого": facts.get("after", ""),
        },
    )
    if d is not None:
        await telegram_bot.send_alert(d.text, **d.send_kwargs())


async def _after(lead: dict, kind: str, res: Result) -> None:
    """Реакция на исход, не требующий повтора: уведомление, если оно нужно."""
    verdict = _classify(res.outcome, kind)
    if verdict is None:
        return
    event_key, tech, headline = verdict
    try:
        await _alert(event_key, lead, headline, res.detail, res.facts, tech=tech)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("ms_payment: lead %s: не удалось отправить уведомление %s", lead.get("id"), event_key)


# ───────────────────────────── очередь повторов ────────────────────────

def _load_pending() -> dict:
    try:
        import json
        with open(MS_PAYMENT_PENDING_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        logger.exception("ms_payment: очередь повторов не прочиталась (%s) - считаю пустой", MS_PAYMENT_PENDING_PATH)
        return {}


def _save_pending(pend: dict) -> None:
    try:
        write_json_atomic(MS_PAYMENT_PENDING_PATH, pend)
    except Exception:
        logger.exception("ms_payment: очередь повторов не записалась (%s)", MS_PAYMENT_PENDING_PATH)


def pending_count() -> int:
    return len(_load_pending())


def _enqueue(kind: str, lead: dict, order_uuid, kopecks, key, label: str, res: Result) -> None:
    """Поставить платёж в очередь повторов (или обновить запись)."""
    pend = _load_pending()
    qkey = f"{kind}:{key}"
    now = time.time()
    entry = pend.get(qkey) or {
        "kind": kind, "key": str(key), "lead_id": lead.get("id"), "lead_name": lead.get("name") or "",
        "responsible": lead.get("responsible_user_id"), "order_uuid": str(order_uuid or ""),
        "kopecks": kopecks, "label": label, "first_at": now, "attempts": 0,
        "next_at": now + 60, "alerted": False,
    }
    entry["last_outcome"], entry["last_detail"] = res.outcome, res.detail
    pend[qkey] = entry
    _save_pending(pend)
    logger.warning("ms_payment: %s - %s, поставлен в очередь повторов (%s)", qkey, res.outcome, res.detail)


async def _execute(entry: dict) -> Result:
    try:
        if entry["kind"] == "ozon":
            return await ms_payment.record_ozon_payment(
                entry["order_uuid"], entry["kopecks"], entry["key"], dry_run=False)
        return await ms_payment.record_office_payment(
            entry["order_uuid"], entry["key"], label=entry.get("label") or "оплата при выдаче", dry_run=False)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("ms_payment: повтор %s:%s - необработанная ошибка", entry.get("kind"), entry.get("key"))
        return Result("failed-exception", "необработанная ошибка при повторе")


def _retry_delay(attempts: int) -> float:
    """1, 2, 4, 8, 16, 32 минуты, дальше раз в час."""
    return min(60.0 * (2 ** attempts), 3600.0)


async def retry_once(now: float | None = None) -> dict:
    """Один проход по очереди. Возвращает счётчики (для логов и тестов)."""
    now = time.time() if now is None else now
    pend = _load_pending()
    stats = {"due": 0, "done": 0, "failed": 0, "alerted": 0, "gave_up": 0}
    for qkey, e in list(pend.items()):
        if float(e.get("next_at") or 0) > now:
            continue
        stats["due"] += 1
        res = await _execute(e)
        lead = _lead_stub(e)
        if res.outcome in RETRIABLE or res.outcome == "skipped-inflight":
            if res.outcome != "skipped-inflight":
                e["attempts"] = int(e.get("attempts") or 0) + 1
            e["last_outcome"], e["last_detail"] = res.outcome, res.detail
            e["next_at"] = now + _retry_delay(int(e.get("attempts") or 0))
            stats["failed"] += 1
            age = now - float(e.get("first_at") or now)
            if age >= MS_PAYMENT_RETRY_GIVE_UP_S:
                await _alert("ms_payment_failed", lead,
                             "Платёж НЕ записан в МС: сутки повторов не помогли - внесите вручную",
                             f"{e['kind']} {e['key']}: {res.outcome}: {res.detail}", {}, tech=True)
                pend.pop(qkey, None)
                stats["gave_up"] += 1
            elif age >= MS_PAYMENT_RETRY_ALERT_AFTER_S and not e.get("alerted"):
                e["alerted"] = True
                await _alert("ms_payment_failed", lead,
                             f"МС недоступен {int(age // 60)} мин: платёж не записан, повторы продолжаются",
                             f"{e['kind']} {e['key']}: {res.outcome}: {res.detail}", {}, tech=True)
                stats["alerted"] += 1
        else:
            pend.pop(qkey, None)
            stats["done"] += 1
            logger.info("ms_payment: повтор %s - %s с попытки %s", qkey, res.outcome, int(e.get("attempts") or 0) + 1)
            await _after(lead, e["kind"], res)
        _save_pending(pend)
    return stats


async def _retry_loop() -> None:
    while True:
        await asyncio.sleep(MS_PAYMENT_RETRY_TICK_S)
        try:
            await retry_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ms_payment: ошибка прохода повторов")


def start() -> None:
    """Из lifespan (нужен работающий event loop). Цикл поднимается, только если включён
    хоть один из режимов в БОЕВОМ виде: сухой прогон очередь не заводит."""
    global _retry_task
    live = (OZON_MS_PAYMENT_ENABLED and not OZON_MS_PAYMENT_DRY_RUN) or (
        OFFICE_MS_PAYMENT_ENABLED and not OFFICE_MS_PAYMENT_DRY_RUN)
    if not live or MS_PAYMENT_RETRY_TICK_S <= 0:
        logger.info("ms_payment: автоповтор не запускается (боевой режим выключен)")
        return
    _retry_task = asyncio.create_task(_retry_loop())
    logger.info("ms_payment: автоповтор каждые %s с, в очереди сейчас %s", MS_PAYMENT_RETRY_TICK_S, pending_count())


async def stop() -> None:
    global _retry_task
    if _retry_task is not None:
        _retry_task.cancel()
        try:
            await _retry_task
        except asyncio.CancelledError:
            pass
        _retry_task = None


# ───────────────────────────── запуск записи ───────────────────────────

async def _run(kind: str, lead: dict, order_uuid, kopecks, key, *, dry: bool, label: str = "") -> str:
    lead_id = lead.get("id")
    try:
        if kind == "ozon":
            res = await ms_payment.record_ozon_payment(order_uuid, kopecks, key, dry_run=dry)
        else:
            res = await ms_payment.record_office_payment(order_uuid, key, label=label or "оплата при выдаче", dry_run=dry)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("ms_payment: lead %s: необработанная ошибка записи оплаты в МС", lead_id)
        res = Result("failed-exception", "необработанная ошибка")
    logger.info("ms_payment%s: %s lead %s -> %s (%s)", " DRY-RUN" if dry else "", kind, lead_id, res.outcome, res.detail)
    # В сухом прогоне людей не зовём и очередь не заводим: это уже запись в боевые системы.
    if dry:
        return res.outcome
    if res.outcome in RETRIABLE:
        _enqueue(kind, lead, order_uuid, kopecks, key, label, res)
        return "queued"
    await _after(lead, kind, res)
    return res.outcome


async def on_ozon_paid(lead: dict, kopecks, key: str) -> str:
    """Ozon подтвердил оплату -> платёж в заказе МС на ту сумму, что заплатил клиент.
    Никогда не бросает: деньги клиента уже списаны, сбой МС не должен ронять перевод сделки."""
    if not OZON_MS_PAYMENT_ENABLED:
        return "disabled"
    uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
    return await _run("ozon", lead, uuid, kopecks, key, dry=OZON_MS_PAYMENT_DRY_RUN)


def is_office_entry(pipeline_id, status_id) -> bool:
    """Гейт для webhooks.py: сделка в «Успешно реализовано», воронка Офис (или воронки в
    вебхуке нет - тогда решает process_office_payment по свежей сделке)."""
    try:
        if int(status_id) != STATUS_SUCCESS:
            return False
    except (TypeError, ValueError):
        return False
    if pipeline_id is None:
        return True
    try:
        return int(pipeline_id) == PIPELINE_OFFICE
    except (TypeError, ValueError):
        return False


def _office_label(lead: dict) -> str:
    """Тип оплаты при выдаче: самовывоз из офиса и/или наложка. Пусто - не наш случай."""
    delivery = str(amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) or "").casefold()
    method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
    parts = []
    if DELIVERY_SHOWROOM_MARKER.casefold() in delivery:
        parts.append("самовывоз из офиса")
    if is_cod_payment(method):
        parts.append("наложка")
    return ", ".join(parts)


async def process_office_payment(lead_id) -> str:
    """Сделка вошла в «Успешно реализовано» Офиса: самовывоз из офиса или наложка -> платёж на
    остаток до суммы заказа. Возвращает исход строкой (лог/тесты)."""
    if not OFFICE_MS_PAYMENT_ENABLED:
        return "disabled"
    now = time.monotonic()
    for k, ts in list(_office_recent.items()):
        if now - ts > _OFFICE_RECENT_TTL_S:
            _office_recent.pop(k, None)
    if int(lead_id) in _office_recent:
        return "skipped-recent"
    _office_recent[int(lead_id)] = now

    lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        logger.warning("ms_payment office: сделка %s не прочиталась из amo", lead_id)
        return "failed-lead-read"
    if int(lead.get("pipeline_id") or 0) != PIPELINE_OFFICE or int(lead.get("status_id") or 0) != STATUS_SUCCESS:
        return "skipped-moved"
    # Без ретроактивности: вебхук приходит на любую правку, в УР лежат тысячи старых сделок.
    closed_at = int(lead.get("closed_at") or 0)
    if not closed_at or time.time() - closed_at > OFFICE_MS_PAYMENT_MAX_AGE_H * 3600:
        logger.info("ms_payment office: сделка %s закрыта давно или closed_at нет (%s) - не трогаем", lead_id, closed_at)
        return "skipped-old"
    label = _office_label(lead)
    if not label:
        return "skipped-not-applicable"
    uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
    return await _run("office", lead, uuid, None, lead_id, dry=OFFICE_MS_PAYMENT_DRY_RUN, label=label)


def office_payment_bg(lead_id) -> None:
    """Из вебхука: быстрый ответ, запись фоном."""
    async def _go():
        try:
            await process_office_payment(lead_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("ms_payment office: сделка %s: необработанная ошибка", lead_id)

    task = asyncio.create_task(_go())
    _office_tasks.add(task)
    task.add_done_callback(_office_tasks.discard)
