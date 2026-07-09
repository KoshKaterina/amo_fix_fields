"""Юнит-тест гейта КОНТРОЛЬ (process_kontrol_lead) без сети/прода.

Мокаем amo_service (get_lead_full/patch_lead/add_note) и ms_client (get/put);
чистые функции (get_tags/has_tag/filter_tags_excluding/blockers/auto_fix) — реальные.
Проверяем: релиз в «00», удержание с тегом «ошибка передачи» + причина, эхо-защиту
(повторный тот же провал не плодит примечаний/тег), и skip для не-КОНТРОЛЬ сделки.
"""
import asyncio

import kontrol_gate
from waybill_config import (
    PIPELINE_FULFILLMENT,
    STATUS_FF_KONTROL,
    STATUS_FF_PROCESSING,
    TAG_KONTROL_ERROR,
)

UU = "uu-123"
A_PM = kontrol_gate.A_PAYMENT_METHOD
A_PVZ = kontrol_gate.A_PVZ_CODE

_patches: list = []
_notes: list = []
_ms_puts: list = []


def _order(*, phone="+79001112233"):
    return {
        "id": UU,
        "agent": {"phone": phone} if phone else {},
        "salesChannel": None,
        "attributes": [
            {"id": A_PM, "value": "онлайн"},   # → prepaid
            {"id": A_PVZ, "value": "PVZ123"},  # код ПВЗ есть
        ],
    }


_POSITIONS = {"rows": [
    {"id": "p1", "price": 100000, "quantity": 1,
     "assortment": {"meta": {"type": "product"}, "name": "Keystone", "id": "g1"}},
    {"id": "p2", "price": 30000, "quantity": 1,
     "assortment": {"meta": {"type": "service"}, "name": "СДЭК ПВЗ", "id": "s1"}},
]}


def _install_mocks(order):
    async def fake_get(path, params=None, **kw):
        if path == f"entity/customerorder/{UU}":
            return order
        if path == f"entity/customerorder/{UU}/positions":
            return _POSITIONS
        if path == "report/stock/bystore/current":
            return [{"storeId": kontrol_gate.STOCK_STORE_ID, "assortmentId": "g1", "stock": 10}]
        if path == "entity/customerorder/metadata":
            return {"states": [{"name": "00. Обрабатывается", "id": "state00"}]}
        return None

    async def fake_put(path, body, **kw):
        _ms_puts.append((path, body))
        return {}

    async def fake_patch_lead(lead_id, **kw):
        _patches.append({"lead_id": lead_id, **kw})
        return {"ok": True, "status_code": 200}

    async def fake_add_note(lead_id, text):
        _notes.append((lead_id, text))
        return {"ok": True}

    kontrol_gate.ms_client.get = fake_get
    kontrol_gate.ms_client.put = fake_put
    kontrol_gate.amo_service.patch_lead = fake_patch_lead
    kontrol_gate.amo_service.add_note = fake_add_note
    kontrol_gate.amo_service.get_custom_field_value = lambda lead, fid: UU


def _reset():
    _patches.clear()
    _notes.clear()
    _ms_puts.clear()
    kontrol_gate._last_error_reason.clear()
    kontrol_gate._state00_uuid = None


def _lead(status=STATUS_FF_KONTROL, tags=None):
    return {"id": 555, "pipeline_id": PIPELINE_FULFILLMENT, "status_id": status,
            "_embedded": {"tags": tags or []}}


def run(coro):
    return asyncio.run(coro)


# ── 1) успех → релиз в «00» ────────────────────────────────────────────────
_reset()
lead = _lead()
_install_mocks(_order())

async def _get_lead_full(lid, with_=()):
    return lead
kontrol_gate.amo_service.get_lead_full = _get_lead_full

res = run(kontrol_gate.process_kontrol_lead(555, apply=True, source="webhook"))
assert res["action"] == "released", res
assert any(p.get("status_id") == STATUS_FF_PROCESSING for p in _patches), _patches
assert any("Гейт КОНТРОЛЬ пройден" in t for _, t in _notes), _notes
assert any(p[0] == f"entity/customerorder/{UU}" and "state" in p[1] for p in _ms_puts), _ms_puts
print("✓ успех → перенос в «00» + релиз МС + примечание")

# ── 2) провал (нет телефона) → удержание + тег «ошибка передачи» + причина ──
_reset()
lead = _lead()
_install_mocks(_order(phone=None))
res = run(kontrol_gate.process_kontrol_lead(555, apply=True, source="webhook"))
assert res["action"] == "held", res
assert "нет телефона" in res["reason"], res
tag_patches = [p for p in _patches if p.get("tags") is not None]
assert tag_patches and any(t.get("name") == TAG_KONTROL_ERROR for t in tag_patches[0]["tags"]), _patches
assert not any(p.get("status_id") == STATUS_FF_PROCESSING for p in _patches), "не должно двигать в 00"
assert any("Ошибка передачи в комплектацию" in t and "нет телефона" in t for _, t in _notes), _notes
print("✓ провал → тег «ошибка передачи» + причина примечанием, сделка не двинута")

# ── 3) эхо: тот же провал, тег уже стоит → без нового примечания и без тега ─
_reset()
lead = _lead()
_install_mocks(_order(phone=None))
run(kontrol_gate.process_kontrol_lead(555, apply=True, source="webhook"))  # 1-й провал
n_notes_1 = len(_notes)
# 2-й прогон: тег уже на сделке, причина та же
lead = _lead(tags=[{"name": TAG_KONTROL_ERROR}])
run(kontrol_gate.process_kontrol_lead(555, apply=True, source="webhook"))
assert len(_notes) == n_notes_1, f"эхо не должно плодить примечания: {_notes}"
# тег не переставляли повторно (has_tag → пропуск)
tag_patches = [p for p in _patches if p.get("tags") is not None]
assert len(tag_patches) == 1, f"тег ставим один раз: {tag_patches}"
print("✓ эхо-защита: повторный тот же провал не плодит примечания/тег")

# ── 4) skip: сделка уже не в КОНТРОЛЕ (человек увёл) ────────────────────────
_reset()
lead = _lead(status=STATUS_FF_PROCESSING)
_install_mocks(_order())
res = run(kontrol_gate.process_kontrol_lead(555, apply=True, source="webhook"))
assert res["action"] == "skip", res
assert not _patches and not _notes and not _ms_puts, "skip не должен ничего писать"
print("✓ skip: сделка вне КОНТРОЛЯ — ничего не пишем")

print("\nkontrol_gate: все тесты прошли")
