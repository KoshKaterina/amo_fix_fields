"""Автоснятие тега «пропущенный» при дозвоне.

UIS (телефония) вешает тег «пропущенный» на СДЕЛКУ и КОНТАКТ при потерянном
(пропущенном) входящем звонке, а тег «Успешный звонок» — при успешном звонке
(входящем/исходящем). Когда до клиента ДОЗВОНИЛИСЬ (на сделке появился
«Успешный звонок»), снимаем «пропущенный» и со сделки, и с её контактов.

Вызов из вебхука /lead_change по тегам из пейлоада. Идемпотентно: снимаем только
если тег реально есть; эхо от собственного PATCH гасится (в следующем вебхуке
«пропущенный» уже нет → повторно не триггерит). Построено по образцу urgency_tag.py.

⚠️ Триггер = ДОЗВОН (успешный звонок), не просто набор: если перезвонили, но не
дозвонились, «Успешный звонок» не появляется → «пропущенный» остаётся (клиент ещё
не на связи). Так и задумано.
"""

import asyncio
import logging

import amo_service
from waybill_config import TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()


def _tag_names(tags_wh) -> set[str]:
    """tags_wh = leads.update.0.tags из вебхука amo. Формат может быть dict
    {"0": {"id":.., "name":..}, ...} (form-encoded) или list — сводим к set имён
    в casefold."""
    values = []
    if isinstance(tags_wh, dict):
        values = list(tags_wh.values())
    elif isinstance(tags_wh, list):
        values = tags_wh
    out: set[str] = set()
    for t in values:
        if isinstance(t, dict):
            name = t.get("name")
            if name:
                out.add(str(name).strip().casefold())
    return out


def maybe_remove_bg(tags_wh, lead_id) -> None:
    """Если в тегах сделки из вебхука есть И «Успешный звонок», И «пропущенный» —
    в фоне снимаем «пропущенный» со сделки и контактов. Быстрый, не блокирует ответ."""
    if lead_id is None:
        return
    names = _tag_names(tags_wh)
    if TAG_SUCCESS_CALL_NAME.casefold() not in names:
        return
    if TAG_MISSED_NAME.casefold() not in names:
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        if not lead:
            return
        # Сделка
        if amo_service.has_tag(lead, TAG_MISSED_NAME):
            await amo_service.remove_tag(lead_id, TAG_MISSED_NAME, lead=lead)
            logger.info("Автоснятие «пропущенный»: снят со сделки %s (дозвон)", lead_id)
        # Контакты сделки (в embed сделки тегов контакта нет → дозагружаем контакт
        # целиком, проверяем наличие тега, снимаем; идемпотентно)
        for contact in (lead.get("_embedded") or {}).get("contacts") or []:
            cid = contact.get("id")
            if cid is None:
                continue
            full = await amo_service.get_contact_by_id(cid)
            if full and amo_service.has_tag(full, TAG_MISSED_NAME):
                await amo_service.remove_contact_tag(cid, TAG_MISSED_NAME, contact=full)
                logger.info("Автоснятие «пропущенный»: снят с контакта %s (сделка %s)", cid, lead_id)
    except Exception:
        logger.exception("Автоснятие «пропущенный»: ошибка на сделке %s", lead_id)
