"""Ник Телеграма покупателя из МойСклада - в контакт amo («TelegramUsername_WZ»).

15.09.2026, задача корзины сайта. Покупатель оставляет ник на оформлении заказа,
woocommerce-sklad кладёт его в доп.поле контрагента МойСклада «Телеграм». Мост amgroup
доп.поля контрагента в amo не переносит, поэтому на СОЗДАНИИ сделки дочитываем заказ МС
по UUID (576689), берём ник у контрагента и пишем в поле контакта «TelegramUsername_WZ»
(577785). Из этого поля Wazzup берёт ник, чтобы менеджер мог написать первым в Телеграм
по нику, а не по номеру (частые первые сообщения по номеру Телеграм блокирует).

Пишем только в ПУСТОЕ поле: заполненное ставит сам Wazzup по живой переписке или менеджер.
И не пишем, если такой ник уже стоит у другого контакта: по этому полю антидубль склеивает
контакты, а склейку в amo не отменить (13.08.2026 так слиплись три разных человека).

Ничего не правит ни в МойСкладе, ни в сделке - только одно поле контакта. Флаг
TELEGRAM_CONTACT_ENABLED включён по умолчанию: без ника в заказе модуль ничего не пишет.
"""

import asyncio
import logging
import re

import amo_service
import ms_client
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    MS_ATTR_COUNTERPARTY_TELEGRAM_ID,
    TELEGRAM_CONTACT_ENABLED,
    TELEGRAM_CONTACT_FIELD_ID,
    TELEGRAM_CONTACT_RETRY_DELAYS_S,
)

logger = logging.getLogger("uvicorn")

# Ник: 5-32 символа, латиница, цифры и «_», первый символ - буква (как в woocommerce-sklad)
_USERNAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{4,31}")
_LINK_PREFIX = re.compile(r"^(?:https?://)?(?:www\.)?(?:t|telegram)\.me/", re.IGNORECASE)

_bg_tasks: set = set()


def post_bg(lead_id) -> None:
    """На создании сделки - в фоне перенести ник в контакт. Ответ вебхука сеть не ждёт."""
    if not TELEGRAM_CONTACT_ENABLED or lead_id is None:
        return
    task = asyncio.create_task(apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def normalize(raw) -> str | None:
    """«t.me/name», «name», « @name » → «@name». Не ник (телефон, кириллица, пусто) → None."""
    value = _LINK_PREFIX.sub("", str(raw or "").strip())
    value = value.strip().strip("/").lstrip("@").strip()
    return f"@{value}" if _USERNAME.fullmatch(value) else None


async def _lead_with_order(lead_id):
    """Сделка с контактами и UUID заказа МС. amgroup сперва создаёт сделку, через секунды
    заполняет поле заказа и только потом привязывает контакт - поэтому ждём повторами и то,
    и другое. Поймано на заказе 19102: UUID был сразу, контакт появился позже, и ник было
    некуда писать. (None, "") - сделка не из заказа."""
    found = (None, "")
    for delay in [0.0] + list(TELEGRAM_CONTACT_RETRY_DELAYS_S):
        if delay:
            await asyncio.sleep(delay)
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        if not lead:
            continue
        uuid = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
        if not uuid:
            continue
        if _main_contact_id(lead):
            return lead, uuid
        found = (lead, uuid)
    return found


async def _counterparty_telegram(order_uuid: str) -> str | None:
    order = await ms_client.get(f"entity/customerorder/{order_uuid}", params={"expand": "agent"})
    agent = (order or {}).get("agent") or {}
    for attr in agent.get("attributes") or []:
        if attr.get("id") == MS_ATTR_COUNTERPARTY_TELEGRAM_ID:
            return normalize(attr.get("value"))
    return None


def _main_contact_id(lead: dict):
    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    for contact in contacts:
        if contact.get("is_main"):
            return contact.get("id")
    return contacts[0].get("id") if contacts else None


async def _nick_taken_by_other(nick: str, contact_id) -> bool | None:
    """Ник уже стоит у ДРУГОГО контакта. None - amo не ответила, судить не по чему."""
    rows = await amo_service.find_contacts_by_query(nick.lstrip("@"), limit=50)
    if rows is None:
        return None
    for contact in rows:
        if contact.get("id") == contact_id:
            continue
        other = normalize(amo_service.get_custom_field_value(contact, TELEGRAM_CONTACT_FIELD_ID))
        if other and other.lower() == nick.lower():
            return True
    return False


async def apply(lead_id) -> str:
    """Перенести ник в контакт сделки. Возвращает исход - для логов и тестов:
    written, no_order, no_nick, no_contact, filled, taken, amo_silent, error."""
    try:
        lead, order_uuid = await _lead_with_order(lead_id)
        if not order_uuid:
            return "no_order"
        nick = await _counterparty_telegram(order_uuid)
        if not nick:
            return "no_nick"
        contact_id = _main_contact_id(lead)
        if not contact_id:
            logger.info("telegram_contact: сделка %s - контакта нет, ник %s некуда писать", lead_id, nick)
            return "no_contact"
        contact = await amo_service.get_contact_by_id(contact_id)
        if not contact:
            return "amo_silent"
        current = str(amo_service.get_custom_field_value(contact, TELEGRAM_CONTACT_FIELD_ID) or "").strip()
        if current:
            logger.info(
                "telegram_contact: сделка %s - у контакта %s ник уже есть (%s), не трогаем",
                lead_id, contact_id, current,
            )
            return "filled"
        taken = await _nick_taken_by_other(nick, contact_id)
        if taken is None:
            return "amo_silent"
        if taken:
            logger.warning(
                "telegram_contact: сделка %s - ник %s уже у другого контакта, в %s не пишем (антидубль склеит)",
                lead_id, nick, contact_id,
            )
            return "taken"
        result = await amo_service.patch_contact(contact_id, custom_fields={TELEGRAM_CONTACT_FIELD_ID: nick})
        if result.get("ok"):
            logger.info("telegram_contact: сделка %s - ник %s записан в контакт %s", lead_id, nick, contact_id)
            return "written"
        logger.warning("telegram_contact: сделка %s - не записался ник в контакт %s: %s", lead_id, contact_id, result)
        return "error"
    except Exception:
        logger.exception("telegram_contact: ошибка на сделке %s", lead_id)
        return "error"
