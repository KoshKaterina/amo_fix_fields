"""Заявка с формы предзаказа получает имя «Заказ №<номер сделки amo>».

Решение Кати 21.09.2026. Раньше форма предзаказа называла сделку именем клиента
(«Коба», «Эльдар»): в списке не видно ни что это предзаказ, ни на что он. Свой
нумерации у формы нет - заказа на сайте за такой заявкой не стоит, - поэтому
номером берём автонумерацию amo, то есть id самой сделки.

Почему это живёт здесь, а не в форме. Заявки предзаказа возит сторонний плагин
WordPress `cf7-amocrm-lead-generation` (разбор - `knowledge/forma-predzakaza-kak-ustroena.md`),
и в момент отправки формы id будущей сделки ещё не существует. Узнать его можно
только после создания - на вебхуке.

Что делает: на СОЗДАНИИ сделки дочитывает её, проверяет три признака и, если все
сошлись, переименовывает в «Заказ №<id>». Ничего больше не трогает.

Признаки заявки с формы предзаказа:
  1. источник сделки - «Новый сайт (ContactForm)» (LEAD_SOURCE_SITE_CONTACT_FORM);
  2. «Тип заявки» (577671) = «Предзаказ» - его ставит сама форма с 21.09.2026;
  3. имя ещё не наше (не начинается с «Заказ №») - идемпотентность, amo повторяет
     вебхуки, а контейнер переживает деплой без памяти.

Второй признак - тот, что отделяет предзаказ от прочих форм сайта: источник у них
общий, а тип заявки ставит только форма предзаказа.

Выключен по умолчанию: PREORDER_LEAD_NAME_ENABLED=1 включает.
"""

import asyncio
import logging

import amo_service
from waybill_config import (
    APPLICATION_TYPE_PREORDER,
    FIELD_APPLICATION_TYPE,
    LEAD_SOURCE_SITE_CONTACT_FORM,
    PREORDER_LEAD_NAME_ENABLED,
    PREORDER_LEAD_NAME_RETRY_DELAYS_S,
)

logger = logging.getLogger("uvicorn")

# Префикс, по которому узнаём уже переименованную сделку. Менять только вместе с
# пониманием, что прежние имена перестанут считаться «уже нашими».
NAME_PREFIX = "Заказ №"

_bg_tasks: set = set()


def build_name(lead_id) -> str:
    return f"{NAME_PREFIX}{lead_id}"


def _source_id(lead: dict) -> int | None:
    """Источник сделки из `_embedded.source` (тот же приём, что в lead_distribution)."""
    src = (lead.get("_embedded") or {}).get("source")
    if not src or src.get("id") is None:
        return None
    try:
        return int(src["id"])
    except (TypeError, ValueError):
        return None


def is_preorder_form_lead(lead: dict) -> bool:
    """Заявка именно с формы предзаказа сайта - источник плюс тип заявки."""
    if _source_id(lead) != LEAD_SOURCE_SITE_CONTACT_FORM:
        return False
    enum_id = amo_service.get_custom_field_enum_id(lead, FIELD_APPLICATION_TYPE)
    return enum_id == APPLICATION_TYPE_PREORDER


def needs_rename(lead: dict) -> bool:
    return not str(lead.get("name") or "").startswith(NAME_PREFIX)


def rename_bg(lead_id) -> None:
    """На создании сделки - в фоне переименовать. Быстрый: планирует задачу и
    сразу возвращает, ответ вебхука сети не ждёт."""
    if not PREORDER_LEAD_NAME_ENABLED or lead_id is None:
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _read_preorder_lead(lead_id) -> dict | None:
    """Сделка, если это заявка с формы предзаказа. С повторами: «Тип заявки»
    приезжает тем же запросом, что и сама сделка, но вебхук о создании может
    обогнать запись полей - на первой попытке поле бывает пустым."""
    for delay in [0.0] + list(PREORDER_LEAD_NAME_RETRY_DELAYS_S):
        if delay:
            await asyncio.sleep(delay)
        lead = await amo_service.get_lead_full(lead_id, with_=("source_id",))
        if not lead:
            continue
        if is_preorder_form_lead(lead):
            return lead
    return None


async def _apply(lead_id) -> None:
    try:
        lead = await _read_preorder_lead(lead_id)
        if not lead:
            return
        if not needs_rename(lead):
            return
        name = build_name(lead_id)
        result = await amo_service.patch_lead(lead_id, name=name)
        if result.get("ok"):
            logger.info("preorder_lead_name: сделка %s переименована в «%s»", lead_id, name)
        else:
            logger.warning(
                "preorder_lead_name: сделка %s - переименовать не вышло: %s", lead_id, result)
    except Exception:
        logger.exception("preorder_lead_name: сделка %s - сбой", lead_id)
