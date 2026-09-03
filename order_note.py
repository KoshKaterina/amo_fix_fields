"""Контакты покупателя из заказа сайта — примечанием в ленту сделки.

Костыль от 30.08.2026, до починки на стороне amgroup. Разбор сделки 36543929
(заказ №18712): woocommerce-sklad создаёт контрагента в МойСкладе с email из
заказа, а через ~15 секунд amgroup, склеивая заказ с УЖЕ существующим контактом
amo, перезаписывает карточку контрагента целиком — вместе с пустыми полями.
Email из заказа затирается, имя подменяется на amo'шное. По audit МойСклада:
11 случаев из 60 контрагентов, созданных с 15.08 (18%).

Что делает: на СОЗДАНИИ сделки дочитывает номер заказа сайта из поля 577415,
берёт заказ в WooCommerce и кладёт в ленту одно примечание вида

    Данные из заказа №18712 (с сайта, до склейки контактов):
    Имя: Сергей Малько
    Email: seregeimalko@yandex.ru
    Телефон: +79064975489

Почему из WooCommerce, а не из МойСклада: к моменту, когда мы читаем, в МС email
уже может быть затёрт — нужен первоисточник. Почему примечание, а не запись в
поля: писать в контакт/контрагента бессмысленно, следующая же синхронизация
amgroup затрёт это снова; примечание он не трогает.

Ничего не правит ни в amo, ни в МойСкладе, ни в WooCommerce — только читает и
добавляет примечание. Выключен по умолчанию: ORDER_NOTE_ENABLED=1 включает.

Идемпотентность: перед записью читаем примечания сделки и выходим, если наше
там уже есть (amo повторяет вебхуки, а контейнер переживает деплой без памяти).
"""

import asyncio
import logging

import amo_service
import woo_client
from waybill_config import (
    FIELD_SITE_ORDER_NUMBER,
    ORDER_NOTE_ENABLED,
    ORDER_NOTE_RETRY_DELAYS_S,
)

logger = logging.getLogger("uvicorn")

# По этому префиксу узнаём своё примечание при повторном заходе — менять только
# вместе с пониманием, что старые примечания перестанут считаться «уже есть».
NOTE_PREFIX = "Данные из заказа"

_bg_tasks: set = set()


def post_bg(lead_id) -> None:
    """На создании сделки — в фоне положить примечание с контактами из заказа.
    Быстрый: планирует задачу и сразу возвращает, ответ вебхука не ждёт сети."""
    if not ORDER_NOTE_ENABLED or lead_id is None:
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def build_note(order_number: str, order: dict) -> str | None:
    """Текст примечания из заказа WooCommerce. None → в заказе нет ни имени, ни
    почты, ни телефона (писать нечего, молчим)."""
    billing = order.get("billing") or {}
    name = " ".join(
        part for part in (
            (billing.get("first_name") or "").strip(),
            (billing.get("last_name") or "").strip(),
        ) if part
    )
    email = (billing.get("email") or "").strip()
    phone = (billing.get("phone") or "").strip()
    if not (name or email or phone):
        return None
    lines = [f"{NOTE_PREFIX} №{order_number} (с сайта, до склейки контактов):"]
    if name:
        lines.append(f"Имя: {name}")
    if email:
        lines.append(f"Email: {email}")
    if phone:
        lines.append(f"Телефон: {phone}")
    return "\n".join(lines)


async def _site_order_number(lead_id) -> str | None:
    """Номер заказа сайта из поля 577415, с повторами: amgroup заполняет поля
    через секунды после создания сделки, на первой попытке там обычно пусто.
    None → сделка не с сайта (звонок, чат) или поле так и не появилось."""
    for delay in [0.0] + list(ORDER_NOTE_RETRY_DELAYS_S):
        if delay:
            await asyncio.sleep(delay)
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if not lead:
            continue
        value = amo_service.get_custom_field_value(lead, FIELD_SITE_ORDER_NUMBER)
        value = str(value).strip() if value is not None else ""
        if value:
            return value
    return None


async def _already_posted(lead_id) -> bool:
    for note in await amo_service.get_lead_notes(lead_id):
        text = (note.get("params") or {}).get("text") or ""
        if text.startswith(NOTE_PREFIX):
            return True
    return False


async def _apply(lead_id) -> None:
    try:
        number = await _site_order_number(lead_id)
        if not number:
            return
        if not number.isdigit():
            # InSales/Tangemshop: номер идёт с суффиксом (« Tangemshop»), в
            # WooCommerce такого заказа нет — не ходим туда впустую.
            logger.info(
                "order_note: сделка %s — номер «%s» не из WooCommerce, пропускаем",
                lead_id, number,
            )
            return
        if await _already_posted(lead_id):
            return
        order = await woo_client.get_order(number)
        if not order:
            logger.warning(
                "order_note: сделка %s — заказа %s нет в WooCommerce", lead_id, number)
            return
        text = build_note(number, order)
        if not text:
            return
        result = await amo_service.add_note(lead_id, text)
        if result.get("ok"):
            logger.info(
                "order_note: сделка %s — контакты из заказа %s записаны примечанием",
                lead_id, number,
            )
        else:
            logger.warning(
                "order_note: сделка %s — не удалось добавить примечание: %s", lead_id, result)
    except Exception:
        logger.exception("order_note: ошибка на сделке %s", lead_id)
