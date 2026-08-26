"""Юнит-тест международного получателя в create_waybill_for_lead (без сети).

Разбор 26.08.2026 (сделка 36519063): to_location для тарифа «дверь» (137)
всегда уходил в СДЭК с country_code="RU", даже когда получатель реально за
границей — СДЭК искал город получателя ТОЛЬКО в России и перепутал «Минск»
(Беларусь, код 9220) с одноимённым селом в Красноярском крае (код 1912192,
country_code=RU) → «Recipient location is not recognized», отправление не
создано. Фикс: страна получателя определяется по коду телефона контакта
(country_code_from_phone), и для не-RU адреса город получателя пинится явным
кодом СДЭК (cdek_client.find_city) вместо голого текста + захардкоженного RU.

Проверяем: RU-номер — поведение НЕ меняется (регрессия старого домашнего
пути недопустима); BY-номер с однозначным городом — to_location.code
проставляется, страна больше не RU; город не найден/неоднозначен (см. живой
кейс «Гомель» — два разных city_uuid в Беларуси) — накладная НЕ создаётся,
причина уходит в примечание, /retry не плодит дубль (тот же _fail, что и у
остальных гейтов).
"""

import asyncio

import cdek_client
import waybill_service
from waybill_config import FIELD_DELIVERY_ADDRESS, FIELD_ORDER_TOTAL, FIELD_PAYMENT_METHOD, FIELD_PHONE

_created_orders: list = []
_notes: list = []
_patched_tags: list = []


def _lead(delivery_address: str):
    return {
        "id": 999,
        "custom_fields_values": [
            {"field_id": FIELD_ORDER_TOTAL, "values": [{"value": "Посылка склад-дверь\nИтого: 1000.00 рублей"}]},
            {"field_id": FIELD_DELIVERY_ADDRESS, "values": [{"value": delivery_address}]},
            {"field_id": FIELD_PAYMENT_METHOD, "values": [{"value": "Крипта"}]},
        ],
        "_embedded": {
            "tags": [],
            "contacts": [{"id": 1, "is_main": True}],
        },
    }


def _contact(phone: str):
    return {
        "id": 1,
        "name": "Тест",
        "custom_fields_values": [
            {"field_id": FIELD_PHONE, "values": [{"value": phone}]},
        ],
    }


def _stub(find_city_result, create_order_uuid="test-uuid", cdek_number="10306104834"):
    _created_orders.clear()
    _notes.clear()
    _patched_tags.clear()

    async def _fake_find_city(name, country_code=None):
        return find_city_result

    async def _fake_create_order(order):
        _created_orders.append(order)
        return {"entity": {"uuid": create_order_uuid}}

    async def _fake_get_order(uuid):
        return {"entity": {"cdek_number": cdek_number}, "requests": [{"type": "CREATE", "state": "SUCCESSFUL"}]}

    async def _fake_add_note(lead_id, text):
        _notes.append(text)
        return {"ok": True}

    async def _fake_patch_lead(lead_id, **kw):
        _patched_tags.append(kw.get("tags"))
        return {"ok": True}

    async def _fake_commit_waybill(lead_id, cdek_value, current_tags, **kw):
        return {"ok": True}

    async def _fake_alert(text):
        pass

    waybill_service.cdek_client.find_city = _fake_find_city
    waybill_service.cdek_client.create_order = _fake_create_order
    waybill_service.cdek_client.get_order = _fake_get_order
    waybill_service.amo_service.add_note = _fake_add_note
    waybill_service.amo_service.patch_lead = _fake_patch_lead
    waybill_service.amo_service.commit_waybill = _fake_commit_waybill
    waybill_service._alert = _fake_alert


async def _run(lead, contact):
    async def _fake_get_lead_full(lead_id, with_=()):
        return lead

    async def _fake_get_contact_by_id(contact_id):
        return contact

    waybill_service.amo_service.get_lead_full = _fake_get_lead_full
    waybill_service.amo_service.get_contact_by_id = _fake_get_contact_by_id
    return await waybill_service.create_waybill_for_lead(lead.get("id"), source="webhook")


# --- сценарий А: RU-номер -> старое поведение НЕ меняется (полный адрес + country_code RU) ---
_stub(find_city_result=[])  # find_city не должен даже вызываться на RU-пути
res = asyncio.run(_run(_lead("Москва, ул. Ленина, 1"), _contact("+79161234567")))
assert res["ok"] is True, res
assert len(_created_orders) == 1
assert _created_orders[0]["to_location"] == {"address": "Москва, ул. Ленина, 1", "country_code": "RU"}, (
    f"RU-путь не должен меняться: {_created_orders[0]['to_location']!r}"
)

# --- сценарий Б: BY-номер, город однозначно найден -> to_location пинится по коду города ---
_stub(find_city_result=[{"code": 9220, "country_code": "BY", "city": "Минск"}])
res = asyncio.run(_run(_lead("Минск, ул. Иосифа Жиновича, 21"), _contact("+375298999999")))
assert res["ok"] is True, res
assert _created_orders[0]["to_location"] == {"code": 9220, "address": "Минск, ул. Иосифа Жиновича, 21"}, (
    f"BY-путь должен пинить город по коду: {_created_orders[0]['to_location']!r}"
)

# --- сценарий В: BY-номер, город не найден -> накладная НЕ создаётся, причина в примечании ---
_stub(find_city_result=[])
res = asyncio.run(_run(_lead("Несуществующгород, ул. Ленина, 1"), _contact("+375298999999")))
assert res["ok"] is False, res
assert _created_orders == [], "не найден город — заказ в СДЭК уходить не должен"
assert any("не найден в СДЭК" in n for n in _notes), f"причина должна попасть в примечание: {_notes!r}"

# --- сценарий Г: BY-номер, город неоднозначен (кейс «Гомель», два city_uuid) -> тоже отказ ---
_stub(find_city_result=[
    {"code": 6539, "country_code": "BY", "city": "Гомель"},
    {"code": 80390, "country_code": "BY", "city": "Гомель"},
])
res = asyncio.run(_run(_lead("Гомель, ул. Советская, 1"), _contact("+375298999999")))
assert res["ok"] is False, res
assert _created_orders == [], "неоднозначный город — заказ в СДЭК уходить не должен"
assert any("неоднозначен" in n for n in _notes), f"причина должна попасть в примечание: {_notes!r}"

print("waybill_service international recipient (страна получателя по телефону, город пинится по коду) — все тесты прошли")
