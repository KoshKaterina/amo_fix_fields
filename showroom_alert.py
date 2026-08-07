"""Алерт в Telegram: новый заказ с самовывозом → записать клиента в шоурум.

Заказ приходит на сайте, тип доставки (577315) парсится из корзины (576703) и
прилетает вебхуком /lead_change через пару секунд после создания сделки. Если
доставка — любой из НАШИХ самовывозов (из офиса или из шоурума), шлём сообщение
в супергруппу ОП, топик ШОУРУМ, с @-тегом Кати: её задача записать клиента.

⚠️ Дискриминатор — DELIVERY_PICKUP_MARKERS: «CDEK: Самовывоз» (пункт выдачи СДЭК)
сюда НЕ попадает, это доставка, а не визит в офис.

Дедуп — по lead_id в памяти процесса: поле корзины обновляется несколько раз
(в т.ч. эхом от наших же PATCH), а сообщение нужно одно. Рестарт контейнера
дедуп обнуляет — тогда возможен повтор по сделке, которая в этот момент в
обработке; это дешевле пропуска. Построено по образцу uis_missed_call.py.
"""

import asyncio
import logging
from collections import deque

import amo_service
import telegram_bot
from api import BASE_URL
from tg_recipients import NOTIFY_CHAT_ID, SHOWROOM_ALERT_THREAD_ID, SHOWROOM_ALERT_TAG
from waybill_config import (
    DELIVERY_PICKUP_MARKERS,
    FIELD_COMPOSITION,
    FIELD_DELIVERY_TYPE,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()
_seen_leads: set = set()
_seen_order: deque = deque()
_SEEN_CAP = 5000


def _is_new(lead_id) -> bool:
    """True — по этой сделке ещё не слали (слать). False — уже слали (эхо вебхука)."""
    key = str(lead_id)
    if key in _seen_leads:
        return False
    _seen_leads.add(key)
    _seen_order.append(key)
    if len(_seen_order) > _SEEN_CAP:
        _seen_leads.discard(_seen_order.popleft())
    return True


def is_pickup(delivery_type) -> bool:
    """Тип доставки — наш самовывоз (офис или шоурум)? «CDEK: Самовывоз» — нет."""
    if not delivery_type:
        return False
    text = str(delivery_type).casefold()
    return any(marker in text for marker in DELIVERY_PICKUP_MARKERS)


def notify_bg(delivery_type, lead_id) -> None:
    """delivery_type = «Тип доставки» (577315), распарсенный из корзины.
    Самовывоз и по сделке ещё не слали → в фоне шлём алерт. Вебхук не блокирует."""
    if lead_id is None or not is_pickup(delivery_type):
        return
    if SHOWROOM_ALERT_THREAD_ID is None:
        logger.warning(
            "Шоурум-алерт: топик не настроен (SHOWROOM_ALERT_THREAD_ID=None) — "
            "сделка %s не уведомлена", lead_id,
        )
        return
    if not _is_new(lead_id):
        return
    task = asyncio.create_task(_apply(lead_id, delivery_type))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id, delivery_type) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        client = _client_name(lead) if lead else None
        composition = (
            amo_service.get_custom_field_value(lead, FIELD_COMPOSITION) if lead else None
        )
        # Тип доставки из сделки надёжнее распарсенного: к моменту чтения поле уже
        # записано. Нет — берём то, что пришло вебхуком.
        delivery = (
            amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) if lead else None
        ) or delivery_type
        price = (lead or {}).get("price")

        text = _build_message(lead_id, client, composition, delivery, price)
        ok = await telegram_bot.send_alert(
            text, parse_mode="HTML",
            chat_id=NOTIFY_CHAT_ID, message_thread_id=SHOWROOM_ALERT_THREAD_ID,
        )
        logger.info(
            "Шоурум-алерт: %s (сделка %s, доставка %s)",
            "отправлен" if ok else "НЕ отправлен", lead_id, delivery or "—",
        )
    except Exception:
        logger.exception("Шоурум-алерт: ошибка на сделке %s", lead_id)


def _client_name(lead: dict) -> str | None:
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    for contact in contacts:
        name = (contact.get("name") or "").strip()
        if name:
            return name
    return None


def _esc(s) -> str:
    return str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(lead_id, client, composition, delivery, price) -> str:
    lines = [
        "🏬 Новый заказ с самовывозом — записать клиента в шоурум",
        SHOWROOM_ALERT_TAG,
    ]
    if client:
        lines.append(f"👤 {_esc(client)}")
    if composition:
        lines.append(f"📦 {_esc(composition)}")
    if delivery:
        lines.append(f"🚚 {_esc(delivery)}")
    if price:
        lines.append(f"💰 {_esc(price)} ₽")
    lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)
