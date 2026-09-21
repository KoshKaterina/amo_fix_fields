"""Юнит-тесты имени сделки для заявок с формы предзаказа (без сети и прода).

Проверяем три решающих места: узнаём ли заявку именно с формы предзаказа,
не переименовываем ли уже переименованное, заводится ли фоновая задача только
при включённом флаге.
"""

import os

os.environ["PREORDER_LEAD_NAME_ENABLED"] = "1"

import preorder_lead_name as pln
from waybill_config import (
    APPLICATION_TYPE_ORDER,
    APPLICATION_TYPE_PREORDER,
    FIELD_APPLICATION_TYPE,
    LEAD_SOURCE_SITE_CONTACT_FORM,
)


def _lead(*, source_id=LEAD_SOURCE_SITE_CONTACT_FORM, type_enum=APPLICATION_TYPE_PREORDER,
          name="Эльдар") -> dict:
    lead = {"id": 36559797, "name": name, "_embedded": {}}
    if source_id is not None:
        lead["_embedded"]["source"] = {"id": source_id}
    if type_enum is not None:
        lead["custom_fields_values"] = [
            {"field_id": FIELD_APPLICATION_TYPE, "values": [{"enum_id": type_enum}]}
        ]
    return lead


# ── узнаём заявку с формы предзаказа ──
assert pln.is_preorder_form_lead(_lead()) is True
# другой источник (звонок, чат, маркетплейс) — не наша заявка
assert pln.is_preorder_form_lead(_lead(source_id=23478413)) is False
# источника нет вовсе — не наша
assert pln.is_preorder_form_lead(_lead(source_id=None)) is False
# та же форма сайта, но обычный заказ — не переименовываем
assert pln.is_preorder_form_lead(_lead(type_enum=APPLICATION_TYPE_ORDER)) is False
# тип заявки ещё не проставлен — ждём повтора, а не переименовываем вслепую
assert pln.is_preorder_form_lead(_lead(type_enum=None)) is False

# ── идемпотентность ──
assert pln.needs_rename(_lead(name="Эльдар")) is True
assert pln.needs_rename(_lead(name="Заказ №36559797")) is False
assert pln.needs_rename(_lead(name="")) is True

# ── формат имени ──
assert pln.build_name(36559797) == "Заказ №36559797"

# ── планировщик: флаг и пустой id ──
_scheduled: list = []


class _FakeTask:
    def add_done_callback(self, cb):
        pass


def _fake_create_task(coro):
    coro.close()  # не исполняем _apply — в нём сеть
    _scheduled.append(True)
    return _FakeTask()


pln.asyncio.create_task = _fake_create_task


def _fires(lead_id) -> bool:
    _scheduled.clear()
    pln.rename_bg(lead_id)
    return len(_scheduled) == 1


pln.PREORDER_LEAD_NAME_ENABLED = True
assert _fires(36559797) is True
assert _fires(None) is False      # нет сделки — не планируем

pln.PREORDER_LEAD_NAME_ENABLED = False
assert _fires(36559797) is False  # флаг выключен — молчим

print("preorder_lead_name: тесты прошли")
