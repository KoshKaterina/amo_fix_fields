"""Автоснятие тега «пропущенный» при дозвоне (реконсиляция по дочитыванию).

UIS вешает «пропущенный» на СДЕЛКУ и КОНТАКТ при потерянном входящем звонке, а
«Успешный звонок» — при успешном (вх./исх.). Когда до клиента ДОЗВОНИЛИСЬ, снимаем
«пропущенный» со сделки и её контактов.

⚠️ Почему реконсиляция, а не разбор тегов из пейлоада: amo НЕ присылает теги в
вебхук /lead_change (только поля — проверено на тест-сделке 06.07: showroom по полю
сработал, снятие по тегам — нет), а UIS ставит теги как СИСТЕМА (created_by=0).
Поэтому на любом изменении сделки в фоне ДОЧИТЫВАЕМ её теги и сверяем.

Идемпотентно и без петли: PATCH только когда на сделке ОДНОВРЕМЕННО «Успешный
звонок» И «пропущенный»; после снятия «пропущенный» повторная сверка = no-op.
Фоновая работа не блокирует ответ вебхука. Дешёвый short-circuit: если нет
«Успешный звонок» — сразу выходим (один GET), PATCH не делаем.
"""

import asyncio
import logging

import amo_service
from waybill_config import TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()


def maybe_remove_bg(lead_id) -> None:
    """На любом изменении сделки — в фоне сверить теги и снять «пропущенный», если
    дозвонились. Быстрый: планирует фон и сразу возвращает."""
    if lead_id is None:
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        if not lead:
            return
        # Снимаем ТОЛЬКО когда дозвонились (есть «Успешный звонок») И ещё висит
        # «пропущенный». Иначе — выходим без записи (это и гасит эхо своего PATCH).
        if not amo_service.has_tag(lead, TAG_SUCCESS_CALL_NAME):
            return
        if not amo_service.has_tag(lead, TAG_MISSED_NAME):
            return
        await amo_service.remove_tag(lead_id, TAG_MISSED_NAME, lead=lead)
        logger.info("Автоснятие «пропущенный»: снят со сделки %s (дозвон)", lead_id)
        # Контакты сделки: в embed сделки тегов контакта нет → дочитываем контакт.
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
