"""amoCRM → InSales: заказу tangemshop.ru статус «Доставлен», когда сделка закрылась в Офисе.

Решение Кати 02.11.2026: сделка Tangemshop, дошедшая до «Офис / Успешно реализовано» (142),
отражается в InSales статусом заказа «Доставлен» (сейчас штатный, позже, возможно, свой —
поэтому код статуса в настройке INSALES_DELIVERED_STATUS). До этого обратной записи в
InSales не было вовсе: ТЗ 29.09.2026 прямо говорило «InSales для нас — только источник».

Отметка оплаты (решение Кати 02.11.2026, настройка INSALES_MARK_PAID): УР в Офисе значит,
что деньги получены, поэтому тем же PUT заказ отмечается оплаченным. Иначе рефералка Tangemshop
не начисляет партнёрам за предоплату, прошедшую мимо InSales (счёт Ozon из amo, перевод, карта в
шоуруме): для неё предоплата «оплачена» только по financial_status=paid. ⚠️ Оплаченный заказ
woo-sklad проводит в МойСклад входящим платежом, если оплат по заказу там ещё нет. Режим
«prepaid» отмечает только предоплату (наложку рефералка считает по «Доставлен»), «all» — все.

Принципы:
  - передаём ТОЛЬКО статус заказа и отметку оплаты; состав, адрес и остальное не трогаем;
  - только сделки Tangemshop (is_tangemshop_lead или имя «Заказ №… Tangemshop») и только
    Офис/142 — по состоянию сделки, ПЕРЕЧИТАННОМУ из API: вебхук /lead_change открыт без
    секрета (AGENTS.md), его телу не верим;
  - номер заказа — из поля 577415 («18858 Tangemshop»), запасной путь — имя сделки: мост
    amgroup умеет затирать зеркальные поля (knowledge/zatiranie-zerkalnyh-atributov-ms.md);
  - фильтра по номеру в API InSales нет (см. insales_client): ищем заказ среди изменённых
    с даты создания сделки минус запас — сделка рождается через минуты после заказа;
  - только вперёд: заказ уже в целевом статусе — статус не пишем; отменён или в возврате — не
    трогаем ничего; в другом статусе группы «доставлен» (его поставил человек) — статус не
    трогаем, но оплату отмечаем;
  - гард по дате ЗАКРЫТИЯ сделки (INSALES_STATUS_SINCE): закрытые раньше не трогаем — так
    включение не пройдётся задним числом по старым заказам, пока Катя не решит догонять;
  - дедуп на диске: номер → что мы уже довели («dostavlen» или «dostavlen+paid»). Переживает
    рестарт, иначе ночная сверка за 14 дней заново искала бы в InSales каждую закрытую сделку.
    Заказы, доведённые до включения отметки оплаты, сверка один раз пройдёт снова и доплатит.

Входы: вебхук (переход в 142 в Офисе, работа в фоне) и сверка metrika_sync.reconcile_window
(интрадей и ночная) — страховка на вебхуки, потерянные при пересборке контейнера.
"""

import asyncio
import datetime
import logging
import re

import alerts
import amo_service
import insales_client
import migration_freeze
from shared.files import read_json, write_json_atomic
from shared.timez import MSK
from waybill_config import (
    FIELD_SITE_ORDER_NUMBER,
    INSALES_DELIVERED_STATUS,
    INSALES_MARK_PAID,
    INSALES_STATUS_SENT_PATH,
    INSALES_STATUS_SINCE_TS,
    INSALES_STATUS_SYNC_ENABLED,
    PIPELINE_OFFICE,
    STATUS_SUCCESS,
    is_cod_payment,
    is_tangemshop_lead,
)

logger = logging.getLogger("uvicorn")

# Заказ создаётся раньше сделки на минуты, но сделку бывает заводят руками позже. Три дня
# запаса — около двадцати пяти лишних заказов, всё равно одна страница.
_LOOKUP_MARGIN_S = 3 * 86400

# Не трогаем вовсе: отказ и возвраты — решение людей.
_CLOSED_STATUSES = ("declined", "returned", "returning")
# Статус не пишем: к закрытым добавляется «доставлен» с другим кодом — человек уже поставил
# статус точнее нашего (например, «Получена оплата от СД»). Оплату при этом отмечаем.
_HANDS_OFF_STATUSES = _CLOSED_STATUSES + ("delivered",)

_FIELD_NUMBER = re.compile(r"^\s*(\d+)\s*tangemshop\s*$", re.IGNORECASE)
_NAME_NUMBER = re.compile(r"№\s*(\d+)\s+tangemshop", re.IGNORECASE)

_SENT_LIMIT = 5000

_enabled = False
_sent: dict[str, str] = {}
# Без «не нашли» в дедупе: заказ может найтись позже. Алерт — раз на номер за жизнь процесса.
_not_found_alerted: set[str] = set()
# Вебхук и сверка могут прийти за одной сделкой одновременно — работаем по одной.
_lock = asyncio.Lock()
_bg_tasks: set = set()


def is_enabled() -> bool:
    return _enabled


async def init() -> None:
    """Включаем синк только если заданы ключи InSales И поднят боевой флаг."""
    global _enabled, _sent
    if not insales_client.is_configured():
        logger.warning("InSales status sync: INSALES_API_KEY/INSALES_API_PASSWORD не заданы — ВЫКЛЮЧЕН")
        return
    if not INSALES_STATUS_SYNC_ENABLED:
        logger.warning(
            "InSales status sync: ключи заданы, но INSALES_STATUS_SYNC_ENABLED не включён — ВЫКЛЮЧЕН"
        )
        return
    await insales_client.init()
    data = read_json(INSALES_STATUS_SENT_PATH, default={})
    _sent = {str(k): str(v) for k, v in data.items()} if isinstance(data, dict) else {}
    _enabled = True
    logger.info(
        "InSales status sync включён (статус=%s, оплата=%s, since=%s, в дедупе %s)",
        INSALES_DELIVERED_STATUS,
        {"prepaid": "отмечаем у предоплаты", "all": "отмечаем у всех"}.get(INSALES_MARK_PAID, "не трогаем"),
        "выкл" if INSALES_STATUS_SINCE_TS is None else f"unix {INSALES_STATUS_SINCE_TS}",
        len(_sent),
    )


def _done_token() -> str:
    """Что считается доведённым заказом при текущих настройках (значение в дедупе)."""
    return INSALES_DELIVERED_STATUS + ("+paid" if INSALES_MARK_PAID else "")


def _should_mark_paid(order: dict) -> bool:
    """Отмечать ли оплату этому заказу при текущем режиме INSALES_MARK_PAID."""
    if not INSALES_MARK_PAID or order.get("financial_status") == "paid":
        return False
    return INSALES_MARK_PAID == "all" or not is_cod_payment(order.get("payment_title"))


async def shutdown() -> None:
    global _enabled
    _enabled = False
    await insales_client.aclose()
    logger.info("InSales status sync stopped")


async def _alert(text: str, event: str | None = None, values: dict | None = None) -> None:
    logger.error(text)
    try:
        from telegram_bot import send_alert
        body, kw = text, {}
        if event:
            d = alerts.decide(event, legacy_text=text, values=values or {})
            if d is None:
                logger.info("%s: уведомление выключено в панели", event)
                return
            body, kw = d.text, d.send_kwargs()
        await send_alert(body, **kw)
    except Exception:
        logger.exception("%s", "InSales alert failed: " + text)


def _remember(number: str, status: str) -> None:
    _sent[number] = status
    while len(_sent) > _SENT_LIMIT:
        _sent.pop(next(iter(_sent)))
    try:
        write_json_atomic(INSALES_STATUS_SENT_PATH, _sent)
    except Exception:
        logger.exception("InSales: не удалось записать дедуп %s", INSALES_STATUS_SENT_PATH)


def order_number(lead: dict) -> int | None:
    """Номер заказа InSales из сделки: поле 577415, затем имя. Не Tangemshop — None."""
    field = str(amo_service.get_custom_field_value(lead, FIELD_SITE_ORDER_NUMBER) or "")
    m = _FIELD_NUMBER.match(field)
    if m:
        return int(m.group(1))
    m = _NAME_NUMBER.search(str(lead.get("name") or ""))
    if m:
        return int(m.group(1))
    return None


def is_target(lead: dict | None) -> bool:
    """Сделка Tangemshop, закрытая в Офисе как успешная, и не раньше гарда."""
    if not lead:
        return False
    if lead.get("pipeline_id") != PIPELINE_OFFICE or lead.get("status_id") != STATUS_SUCCESS:
        return False
    if not is_tangemshop_lead(lead) and not _NAME_NUMBER.search(str(lead.get("name") or "")):
        return False
    closed = lead.get("closed_at")
    if INSALES_STATUS_SINCE_TS is not None and closed is not None and int(closed) < INSALES_STATUS_SINCE_TS:
        return False
    return True


def on_lead_change(lead_id, pipeline_id, status_id, status_changed: bool) -> None:
    """Из вебхука: сделка ТОЛЬКО ЧТО вошла в 142 (в Офисе или без воронки в теле).
    Без сети: проверки и запись — в фоне, ответ вебхука их не ждёт."""
    if not _enabled or lead_id is None or not status_changed:
        return
    if str(status_id) != str(STATUS_SUCCESS):
        return
    if pipeline_id is not None and str(pipeline_id) != str(PIPELINE_OFFICE):
        return
    schedule(lead_id, "webhook")


def schedule(lead_id, source: str) -> None:
    """Прогнать одну сделку в фоне. Кроме вебхука зовёт office_transfer: самовывоз он кладёт
    из УР воронки-источника сразу в УР Офиса, этап не меняется (142 → 142), и вебхук такую
    сделку за переход не считает. Без этого её подбирала бы только сверка, через часы."""
    if not _enabled or lead_id is None:
        return
    task = asyncio.create_task(_process_bg(lead_id, source))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _process_bg(lead_id, source: str = "webhook") -> None:
    try:
        await process_lead(lead_id, source=source)
    except Exception:
        logger.exception("InSales: ошибка по сделке %s", lead_id)


async def process_lead(lead_id, lead: dict | None = None, source: str = "reconcile") -> str:
    """Одна сделка. Возвращает исход (для логов и тестов): disabled, frozen, not_ours,
    no_number, dedup, not_found, already, hands_off, set, error."""
    if not _enabled:
        return "disabled"
    if lead is None:
        lead = await amo_service.get_lead_full(lead_id, with_=())
    if not is_target(lead):
        return "not_ours"
    # Окно миграции воронок: массовый перенос старых сделок в 142 не должен
    # переписывать статусы заказов в магазине.
    if await migration_freeze.skip(lead_id, "InSales", lead=lead):
        return "frozen"
    number = order_number(lead)
    if number is None:
        logger.info("InSales: сделка %s — номер заказа Tangemshop не нашёлся, пропуск", lead_id)
        return "no_number"
    key = str(number)
    target = INSALES_DELIVERED_STATUS
    done = _done_token()
    if _sent.get(key) == done:
        return "dedup"

    async with _lock:
        if _sent.get(key) == done:
            return "dedup"
        created = int(lead.get("created_at") or 0)
        since = datetime.datetime.fromtimestamp(max(0, created - _LOOKUP_MARGIN_S), MSK).isoformat()
        try:
            order = await insales_client.find_order_by_number(number, since)
            if order is None:
                if key not in _not_found_alerted:
                    _not_found_alerted.add(key)
                    await _alert(
                        f"InSales: заказ №{number} (сделка {lead_id}) не нашёлся в магазине — "
                        f"статус «Доставлен» не поставлен",
                        "insales_status_sync_error",
                        {"номер": key, "номер_сделки": lead_id, "ошибка": "заказ не найден"},
                    )
                return "not_found"
            current = (order.get("custom_status") or {}).get("permalink")
            system = order.get("fulfillment_status")
            paid = order.get("financial_status") == "paid"
            if system in _CLOSED_STATUSES:
                logger.info(
                    "InSales: заказ №%s (сделка %s) в «%s» (%s) — не трогаем",
                    number, lead_id, current, system,
                )
                return "hands_off"
            need_status = current != target and system not in _HANDS_OFF_STATUSES
            need_paid = _should_mark_paid(order)
            if not need_status and not need_paid:
                _remember(key, done)
                logger.info("InSales: заказ №%s уже «%s»%s — пропуск", number, current,
                            ", оплачен" if paid else "")
                return "already"
            await insales_client.update_order(
                order["id"], permalink=target if need_status else None, mark_paid=need_paid,
            )
        except insales_client.InSalesError as exc:
            await _alert(
                f"InSales: ошибка по заказу №{number} (сделка {lead_id}): {exc}",
                "insales_status_sync_error",
                {"номер": key, "номер_сделки": lead_id, "ошибка": str(exc)},
            )
            return "error"
        _remember(key, done)
    changes = []
    if need_status:
        changes.append(f"«{current}» → «{target}»")
    if need_paid:
        changes.append("оплачен")
    logger.info(
        "InSales: заказ №%s (сделка %s, %s) %s", number, lead_id, source, ", ".join(changes),
    )
    return "set"
