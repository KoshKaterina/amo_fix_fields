"""Юнит-тест guard'а «кто стёр трек-номер» в waybill_service (без сети/прода).

Защита от порчи поля 571657 (разбор 01.08.2026, сделка 36526319) не должна
откатывать НАМЕРЕННУЮ ручную очистку поля оператором (см. cdek.md: если поле
уже заполнено, накладная не пересоздаётся — очистка поля это осознанный способ
форсировать /retry). Проверяем, что _verify_trek_after_delay восстанавливает
поле ТОЛЬКО когда его обнулил бот/интеграция (created_by=0 в событии amoCRM),
а не человек (ненулевой created_by) и не «не разобрались» (событие не нашли).

asyncio.sleep подменён на no-op — реального ожидания TREK_VERIFY_DELAY_S нет.
"""

import asyncio

import waybill_service
from waybill_config import FIELD_CDEK_ORDER_NUMBER


async def _noop_sleep(_seconds):
    return None


waybill_service.asyncio.sleep = _noop_sleep

_patched: list = []
_alerted: list = []


def _lead(cdek_value=None):
    values = [{"field_id": FIELD_CDEK_ORDER_NUMBER, "values": [{"value": cdek_value}]}] if cdek_value else []
    return {"id": 36526319, "custom_fields_values": values}


def _event(created_by, value_after, created_at=100):
    return {"created_by": created_by, "value_after": value_after, "created_at": created_at}


def _stub(lead, events, patch_ok=True):
    _patched.clear()
    _alerted.clear()

    async def _fake_get_lead_full(lead_id, with_=()):
        return lead

    async def _fake_do_get(path, params=None):
        return {"_embedded": {"events": events}}

    async def _fake_patch(lead_id, **kw):
        _patched.append((lead_id, kw))
        return {"ok": patch_ok}

    async def _fake_alert(text):
        _alerted.append(text)

    waybill_service.amo_service.get_lead_full = _fake_get_lead_full
    waybill_service.amo_service._do_get = _fake_do_get
    waybill_service.amo_service.patch_lead = _fake_patch
    waybill_service._alert = _fake_alert


# поле стёр бот/интеграция (created_by=0) -> восстанавливаем ТОЛЬКО поле 571657
_stub(_lead(cdek_value=None), events=[_event(0, [])])
asyncio.run(waybill_service._verify_trek_after_delay(36526319, "10301814033"))
assert len(_patched) == 1, "бот стёр поле — должны восстановить"
lead_id, kw = _patched[0]
assert lead_id == 36526319
assert kw == {"custom_fields": {FIELD_CDEK_ORDER_NUMBER: "10301814033"}}, (
    f"PATCH должен трогать ТОЛЬКО поле 571657: {kw!r}"
)
assert len(_alerted) == 1

# поле стёр живой человек (ненулевой created_by, как поле 572499 в реальной
# истории сделки 36526319 — created_by=13929334) -> НЕ трогаем, похоже на /retry
_stub(_lead(cdek_value=None), events=[_event(13929334, [])])
asyncio.run(waybill_service._verify_trek_after_delay(36526319, "10301814033"))
assert _patched == [], "человека, намеренно очистившего поле, трогать нельзя"
assert _alerted == [], "штатный /retry не должен шуметь в TG"

# не нашли событие очистки (не должно случаться, но проверяем перестраховку)
# -> НЕ восстанавливаем автоматически, но алертим, что нужна ручная проверка
_stub(_lead(cdek_value=None), events=[])
asyncio.run(waybill_service._verify_trek_after_delay(36526319, "10301814033"))
assert _patched == [], "без понимания, кто стёр поле — не гадаем и не пишем"
assert len(_alerted) == 1, "но должны попросить проверить руками"

# поле на месте (никто не стирал) -> тишина, к events даже не обращаемся
_stub(_lead(cdek_value="10301814033"), events=[_event(0, [])])
asyncio.run(waybill_service._verify_trek_after_delay(36526319, "10301814033"))
assert _patched == []
assert _alerted == []

print("waybill_service trek guard (created_by бот/человек): все тесты прошли")
