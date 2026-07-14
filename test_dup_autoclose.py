"""Юнит-тест dup_autoclose (без сети/прода).

Проверяем:
  1. гейт триггера maybe_close_bg — заводит фон только когда изменилась «Причина
     отказа» (577623), флаг включён и есть lead_id;
  2. ядро _maybe_close (реконсиляция) со стабами amo_service — переводит в 143
     открытую сделку с причиной «Дубль сделки», и НЕ трогает закрытую / с иной причиной.

asyncio.create_task подменён заглушкой; amo_service.get_lead_full / patch_lead — фейки.
"""

import asyncio

import dup_autoclose
from waybill_config import (
    DUP_CLOSE_STATUS_ID,
    DUP_REASON_ENUM_ID,
    DUP_REASON_FIELD_ID,
)

# --- гейт триггера ------------------------------------------------------------
dup_autoclose.DUP_AUTOCLOSE_ENABLED = True
_scheduled: list = []


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def _fake_create_task(coro):
    coro.close()  # не исполняем _maybe_close (там сеть)
    _scheduled.append(True)
    return _FakeTask()


dup_autoclose.asyncio.create_task = _fake_create_task


def _reason_upd(field_id, enum_text="Дубль сделки"):
    return {"0": {"id": str(field_id), "values": {"0": {"value": enum_text}}}}


def _fires(updates, lead_id) -> bool:
    _scheduled.clear()
    dup_autoclose.maybe_close_bg(updates, lead_id)
    return len(_scheduled) == 1


# изменилась Причина отказа + валидный lead → фон заводится
assert _fires(_reason_upd(DUP_REASON_FIELD_ID), 123) is True
# изменилось другое поле (напр. 576703) → не заводим
assert _fires(_reason_upd(576703), 123) is False
# нет lead_id → не заводим
assert _fires(_reason_upd(DUP_REASON_FIELD_ID), None) is False
# пустой апдейт → не заводим
assert _fires({}, 123) is False
# флаг выключен → не заводим
dup_autoclose.DUP_AUTOCLOSE_ENABLED = False
assert _fires(_reason_upd(DUP_REASON_FIELD_ID), 123) is False
dup_autoclose.DUP_AUTOCLOSE_ENABLED = True

# --- ядро реконсиляции --------------------------------------------------------
_patched: list = []


def _make_lead(status, enum_id, pipeline=901105):
    return {
        "id": 777,
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
    asyncio.run(dup_autoclose._maybe_close(777))


# открытая сделка, причина «Дубль сделки» → перевод в 143 в её воронке
_run(_make_lead(83537714, DUP_REASON_ENUM_ID, pipeline=10593102))
assert len(_patched) == 1, "должен быть один PATCH"
assert _patched[0]["status_id"] == DUP_CLOSE_STATUS_ID
assert _patched[0]["pipeline_id"] == 10593102

# уже закрытая (143) → эхо-защита, не трогаем
_run(_make_lead(143, DUP_REASON_ENUM_ID))
assert _patched == [], "закрытую сделку не двигаем"

# уже закрытая (142) → тоже не трогаем
_run(_make_lead(142, DUP_REASON_ENUM_ID))
assert _patched == [], "успешную сделку не двигаем"

# открытая, но причина иная (не Дубль) → не трогаем
_run(_make_lead(83537714, 1041143))  # «Купил в другом месте»
assert _patched == [], "не Дубль — не двигаем"

# открытая, причина пустая → не трогаем
_run(_make_lead(83537714, None))
assert _patched == [], "пустая причина — не двигаем"

print("dup_autoclose: все тесты прошли (гейт триггера + реконсиляция)")
