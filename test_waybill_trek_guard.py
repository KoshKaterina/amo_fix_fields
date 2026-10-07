"""Юнит-тест guard'а «кто стёр трек-номер» в waybill_service (без сети/прода).

Защита от порчи поля 571657 (разбор 01.08.2026, сделка 36526319) не должна
откатывать НАМЕРЕННУЮ ручную очистку поля оператором (см. cdek.md: если поле
уже заполнено, накладная не пересоздаётся — очистка поля это осознанный способ
форсировать /retry). Проверяем, что _verify_trek_after_delay восстанавливает
поле ТОЛЬКО когда его обнулил бот/интеграция (created_by=0 в событии amoCRM),
а не человек (ненулевой created_by) и не «не разобрались» (событие не нашли).

asyncio.sleep подменён на no-op — реального ожидания TREK_VERIFY_DELAY_S нет.

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде четыре сценария шли подряд голыми
`assert` на уровне модуля, то есть выполнялись на ИМПОРТЕ: провал читался как ошибка СБОРА
и ронял сбор всего репозитория, а в сводке файл давал ноль тестов. Вторая беда того вида:
первый же упавший `assert` обрывал файл, и про остальные три сценария мы не узнавали ничего.
"""

import asyncio

import pytest
import waybill_service
from waybill_config import FIELD_CDEK_ORDER_NUMBER

LEAD_ID = 36526319
TREK = "10301814033"


async def _noop_sleep(_seconds):
    return None


@pytest.fixture(autouse=True)
def _skip_real_waiting():
    """Убрать ожидание TREK_VERIFY_DELAY_S - и ВЕРНУТЬ sleep на место.

    ⚠️ Возврат тут обязателен, и это дорого купленное знание. `waybill_service.asyncio` -
    это НЕ локальная копия, а сам модуль `asyncio`, общий на весь процесс. Пока файл был
    скриптом, подмена на уровне модуля никому не мешала: процесс заканчивался вместе с
    проверками. Под pytest все файлы живут в ОДНОМ процессе, и подменённый `sleep` достался
    соседям - `test_waybill_intl_recipient.py` на нём завис намертво, потому что его опрос
    номера СДЭК крутился без паузы до самого дедлайна (поймано 07.10.2026 сразу после
    перевода файлов в pytest-модули).
    """
    was = waybill_service.asyncio.sleep
    waybill_service.asyncio.sleep = _noop_sleep
    try:
        yield
    finally:
        waybill_service.asyncio.sleep = was


_patched: list = []
_alerted: list = []


def _lead(cdek_value=None):
    values = (
        [{"field_id": FIELD_CDEK_ORDER_NUMBER, "values": [{"value": cdek_value}]}]
        if cdek_value
        else []
    )
    return {"id": LEAD_ID, "custom_fields_values": values}


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

    async def _fake_alert(text, *args, **kwargs):  # ключ события и значения панели - мимо
        _alerted.append(text)

    waybill_service.amo_service.get_lead_full = _fake_get_lead_full
    waybill_service.amo_service._do_get = _fake_do_get
    waybill_service.amo_service.patch_lead = _fake_patch
    waybill_service._alert = _fake_alert


def test_pole_ster_bot_vosstanavlivaem_tolko_ego():
    _stub(_lead(cdek_value=None), events=[_event(0, [])])
    asyncio.run(waybill_service._verify_trek_after_delay(LEAD_ID, TREK))

    assert len(_patched) == 1, "бот стёр поле — должны восстановить"
    lead_id, kw = _patched[0]
    assert lead_id == LEAD_ID
    assert kw == {"custom_fields": {FIELD_CDEK_ORDER_NUMBER: TREK}}, (
        f"PATCH должен трогать ТОЛЬКО поле 571657: {kw!r}"
    )
    assert len(_alerted) == 1


def test_pole_ster_chelovek_ne_trogaem_pohozhe_na_retry():
    """Ненулевой created_by - как поле 572499 в реальной истории сделки (13929334)."""
    _stub(_lead(cdek_value=None), events=[_event(13929334, [])])
    asyncio.run(waybill_service._verify_trek_after_delay(LEAD_ID, TREK))

    assert _patched == [], "человека, намеренно очистившего поле, трогать нельзя"
    assert _alerted == [], "штатный /retry не должен шуметь в TG"


def test_sobytie_ochistki_ne_nashli_ne_gadaem_no_prosim_proverit():
    """Случаться не должно, но перестраховка проверяется."""
    _stub(_lead(cdek_value=None), events=[])
    asyncio.run(waybill_service._verify_trek_after_delay(LEAD_ID, TREK))

    assert _patched == [], "без понимания, кто стёр поле — не гадаем и не пишем"
    assert len(_alerted) == 1, "но должны попросить проверить руками"


def test_pole_na_meste_tishina():
    _stub(_lead(cdek_value=TREK), events=[_event(0, [])])
    asyncio.run(waybill_service._verify_trek_after_delay(LEAD_ID, TREK))

    assert _patched == []
    assert _alerted == []
