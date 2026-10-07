"""Юнит-тест dup_autoclose (без сети/прода).

Проверяем:
  1. гейт триггера maybe_close_bg — заводит фон только когда изменилась «Причина
     отказа» (577623), флаг включён и есть lead_id;
  2. ядро _maybe_close (реконсиляция) со стабами amo_service — переводит в 143
     открытую сделку с причиной «Дубль сделки», и НЕ трогает закрытую / с иной причиной.

asyncio.create_task подменён заглушкой; amo_service.get_lead_full / patch_lead — фейки.

⚠️ Переведено в pytest-модуль 07.10.2026. Прежде проверки стояли голыми `assert` на уровне
модуля, то есть выполнялись на ИМПОРТЕ: провал читался как ошибка СБОРА и ронял сбор всего
репозитория, а в сводке файл давал ноль тестов. Перебор причин отказа стал `parametrize` -
теперь при падении видно, на какой именно причине сломалось.

⚠️ Флаг `DUP_AUTOCLOSE_ENABLED` переключается фикстурой с возвратом прежнего значения.
Скрипт возвращал его вручную, и это работало, пока файл был скриптом; в pytest упавший
посередине тест оставил бы флаг выключенным всем следующим.
"""

import asyncio

import dup_autoclose
import pytest
from waybill_config import (
    DUP_CLOSE_STATUS_ID,
    DUP_REASON_ENUM_IDS,
    DUP_REASON_FIELD_ID,
)

LEAD_ID = 777
STATUS_OPEN = 83537714

_scheduled: list = []
_patched: list = []


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def _fake_create_task(coro):
    coro.close()  # не исполняем _maybe_close (там сеть)
    _scheduled.append(True)
    return _FakeTask()


@pytest.fixture(autouse=True)
def _no_real_background_tasks():
    """⚠️ `dup_autoclose.asyncio` - это сам модуль `asyncio`, общий на процесс: возвращаем."""
    was = dup_autoclose.asyncio.create_task
    dup_autoclose.asyncio.create_task = _fake_create_task
    try:
        yield
    finally:
        dup_autoclose.asyncio.create_task = was


@pytest.fixture()
def flag():
    """Переключатель флага, возвращающий прежнее значение."""
    was = dup_autoclose.DUP_AUTOCLOSE_ENABLED
    try:
        yield lambda value: setattr(dup_autoclose, "DUP_AUTOCLOSE_ENABLED", value)
    finally:
        dup_autoclose.DUP_AUTOCLOSE_ENABLED = was


def _reason_upd(field_id, enum_text="Дубль сделки"):
    return {"0": {"id": str(field_id), "values": {"0": {"value": enum_text}}}}


def _fires(updates, lead_id) -> bool:
    _scheduled.clear()
    dup_autoclose.maybe_close_bg(updates, lead_id)
    return len(_scheduled) == 1


# ── гейт триггера ──────────────────────────────────────────────────────────


def test_izmenilas_prichina_otkaza_fon_zavoditsya(flag):
    flag(True)
    assert _fires(_reason_upd(DUP_REASON_FIELD_ID), 123) is True


def test_izmenilos_drugoe_pole_ne_zavodim(flag):
    """Например 576703."""
    flag(True)
    assert _fires(_reason_upd(576703), 123) is False


def test_net_lead_id_ne_zavodim(flag):
    flag(True)
    assert _fires(_reason_upd(DUP_REASON_FIELD_ID), None) is False


def test_pustoy_apdeyt_ne_zavodim(flag):
    flag(True)
    assert _fires({}, 123) is False


def test_flag_vyklyuchen_ne_zavodim(flag):
    flag(False)
    assert _fires(_reason_upd(DUP_REASON_FIELD_ID), 123) is False


# ── ядро реконсиляции ──────────────────────────────────────────────────────


def _make_lead(status, enum_id, pipeline=901105):
    return {
        "id": LEAD_ID,
        "status_id": status,
        "pipeline_id": pipeline,
        "custom_fields_values": [
            {"field_id": DUP_REASON_FIELD_ID, "values": [{"enum_id": enum_id}]}
        ],
    }


def _run(lead):
    _patched.clear()

    async def _fake_get(lead_id, with_=()):
        return lead

    async def _fake_patch(lead_id, **kw):
        _patched.append(kw)
        return {}

    dup_autoclose.amo_service.get_lead_full = _fake_get
    dup_autoclose.amo_service.patch_lead = _fake_patch
    asyncio.run(dup_autoclose._maybe_close(LEAD_ID))


@pytest.mark.parametrize("enum_id", sorted(DUP_REASON_ENUM_IDS))
def test_otkrytaya_sdelka_s_musornoy_prichinoy_perevoditsya_v_143(enum_id):
    """Перевод идёт в 143 В ЕЁ ЖЕ воронке, а не в воронке по умолчанию."""
    _run(_make_lead(STATUS_OPEN, enum_id, pipeline=10593102))

    assert len(_patched) == 1, f"должен быть PATCH для причины {enum_id}"
    assert _patched[0]["status_id"] == DUP_CLOSE_STATUS_ID
    assert _patched[0]["pipeline_id"] == 10593102


def test_nabor_prichin_aktualen():
    """Дубль / Тест / Обменник / Тех поддержка / Не ЦА."""
    assert DUP_REASON_ENUM_IDS == {1041141, 1041163, 1041691, 1041159, 1041161}


def test_uzhe_zakrytuyu_143_ne_dvigaem():
    """Эхо-защита: наш же PATCH рождает вебхук, на который нельзя реагировать."""
    _run(_make_lead(143, next(iter(DUP_REASON_ENUM_IDS))))
    assert _patched == [], "закрытую сделку не двигаем"


def test_uspeshnuyu_142_ne_dvigaem():
    _run(_make_lead(142, next(iter(DUP_REASON_ENUM_IDS))))
    assert _patched == [], "успешную сделку не двигаем"


def test_prichina_inaya_ne_dvigaem():
    """1041143 - «Купил в другом месте»."""
    _run(_make_lead(STATUS_OPEN, 1041143))
    assert _patched == [], "не Дубль — не двигаем"


def test_prichina_pustaya_ne_dvigaem():
    _run(_make_lead(STATUS_OPEN, None))
    assert _patched == [], "пустая причина — не двигаем"
