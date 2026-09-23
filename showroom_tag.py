"""Автотег «Запись в шоурум».

Когда в заказе тип доставки = наш самовывоз - «Самовывоз из офиса Sunscrypt»
или «Самовывоз из шоурума Sunscrypt» (поле 577315,
парсится из корзины 576703 и им же пишется), вешаем на сделку тег
«Запись в шоурум» (533267). Вызов из вебхука /lead_change (должен быть быстрым)
→ реальная работа в фоне. Идемпотентно (has_tag) и НЕ трёт существующие теги
(patch_lead с _embedded.tags заменяет весь набор → сохраняем текущие + добавляем).

⚠️ Дискриминатор — подстроки «самовывоз из офиса» и «самовывоз из шоурума»: строго
ОТЛИЧАЮТ наш самовывоз от «CDEK: Самовывоз» (пункт выдачи СДЭК), который тегать НЕ надо.
Шоурум добавлен 23.09.2026: способ доставки на сайте переименовали, и тег перестал
вставать вовсе — «из офиса» в новом имени нет.
Построено по образцу urgency_tag.py.
"""

import asyncio
import logging

import amo_service
from waybill_config import (
    DELIVERY_PICKUP_MARKERS,
    TAG_SHOWROOM_ID,
    TAG_SHOWROOM_NAME,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()


def maybe_apply_bg(delivery_type, lead_id) -> None:
    """delivery_type = строка «Тип доставки» (поле 577315), распарсенная из корзины.
    Если это наш самовывоз (офис или шоурум) — в фоне вешаем тег. Быстрый, не блокирует
    ответ. «CDEK: Самовывоз» сюда НЕ попадает (нет ни «из офиса», ни «из шоурума»)."""
    if lead_id is None or not delivery_type:
        return
    text = str(delivery_type).casefold()
    if not any(marker in text for marker in DELIVERY_PICKUP_MARKERS):
        return
    task = asyncio.create_task(_apply(lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if not lead:
            return
        if amo_service.has_tag(lead, TAG_SHOWROOM_NAME):
            return  # уже есть — идемпотентно (и гасит эхо от нашего же PATCH)
        tags = amo_service.get_tags(lead) + [{"id": TAG_SHOWROOM_ID}]
        await amo_service.patch_lead(lead_id, tags=tags)
        logger.info("Автотег «Запись в шоурум»: повешен на сделку %s", lead_id)
    except Exception:
        logger.exception("Автотег «Запись в шоурум»: ошибка на сделке %s", lead_id)
