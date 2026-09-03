"""Тесты протеза отгрузок amgroup (amgroup_shipment) - создание отгрузки в
МойСкладе по сделке amoCRM на нужном этапе воронки «Офис» и тройная защита
от двойного списания.

Написаны по инциденту 02-03.09.2026 (см. докстринг amgroup_shipment.py):
amgroup встала, отгрузки в МойСкладе перестал создавать кто бы то ни было.

Запуск: python3 -m pytest test_amgroup_shipment.py -q
"""

import asyncio
import os
import sys

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import amgroup_shipment  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_MOYSKLAD_ORDER_UUID,
    PIPELINE_CLEVER,
    PIPELINE_OFFICE,
    STATUS_OFFICE_COURIER_OWN,
    STATUS_SUCCESS,
    STATUS_WAYBILL_READY,
)

ORDER_UUID = "order-uuid-0001"


def _lead(pipeline_id, status_id, *, order_uuid=ORDER_UUID, shipment_id=None, lead_id=1001):
    fields = []
    if order_uuid:
        fields.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": order_uuid}]})
    if shipment_id:
        fields.append({"field_id": amgroup_shipment.FIELD_SHIPMENT_ID, "values": [{"value": shipment_id}]})
    return {
        "id": lead_id,
        "pipeline_id": pipeline_id,
        "status_id": status_id,
        "custom_fields_values": fields,
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    monkeypatch.setattr(amgroup_shipment, "_STATE_PATH", str(tmp_path / "created.json"), raising=False)
    monkeypatch.setattr(amgroup_shipment, "_state_loaded", False, raising=False)
    amgroup_shipment._created.clear()
    yield
    amgroup_shipment._created.clear()


def _wire(
    monkeypatch,
    *,
    demand_search_rows=(),
    template=None,
    created_demand=None,
    patch_ok=True,
    store_name="Sunscrypt Основной",
):
    """Стабит ms_client (get/put/post) и amo_service.patch_lead. Возвращает
    списки вызовов для проверок."""
    calls = {"get": [], "put": [], "post": [], "patch": []}

    async def fake_get(path, params=None):
        calls["get"].append((path, params))
        if path == "entity/demand":
            return {"rows": list(demand_search_rows)}
        if path.startswith("entity/store/"):
            return {"name": store_name} if store_name else None
        return None

    async def fake_put(path, body):
        calls["put"].append((path, body))
        return template

    async def fake_post(path, body):
        calls["post"].append((path, body))
        return created_demand

    async def fake_patch_lead(lead_id, *, custom_fields=None, **kwargs):
        calls["patch"].append((lead_id, custom_fields))
        return {"ok": patch_ok}

    monkeypatch.setattr(amgroup_shipment.ms_client, "get", fake_get)
    monkeypatch.setattr(amgroup_shipment.ms_client, "put", fake_put)
    monkeypatch.setattr(amgroup_shipment.ms_client, "post", fake_post)
    monkeypatch.setattr(amgroup_shipment.amo_service, "patch_lead", fake_patch_lead)
    return calls


_DEMAND_TEMPLATE = {
    "customerOrder": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/customerorder/order-uuid-0001"}},
    "store": {"meta": {"href": "https://api.moysklad.ru/api/remap/1.2/entity/store/store-uuid-0001"}},
    "positions": {"rows": [{"quantity": 1}]},
}
_CREATED_DEMAND = {**_DEMAND_TEMPLATE, "id": "demand-uuid-0001", "name": "00007"}


def test_sozdaetsya_na_gotova_nakladnaya(monkeypatch):
    """Офис + «Готова накладная» (СДЭК) - отгрузка создаётся."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result == {"shipment_id": "demand-uuid-0001", "shipment_number": "00007", "warehouse": "Sunscrypt Основной"}
    assert len(calls["post"]) == 1
    assert calls["post"][0][0] == "entity/demand"
    # Поля сделки записаны одним PATCH.
    assert len(calls["patch"]) == 1
    _, fields = calls["patch"][0]
    assert fields == {
        amgroup_shipment.FIELD_SHIPMENT_ID: "demand-uuid-0001",
        amgroup_shipment.FIELD_SHIPMENT_NUMBER: "00007",
        amgroup_shipment.FIELD_SHIPMENT_WAREHOUSE: "Sunscrypt Основной",
    }


def test_sozdaetsya_na_dostavka_nash_kurer(monkeypatch):
    """Офис + «Доставка наш курьер» - отгрузка создаётся."""
    _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_OFFICE_COURIER_OWN)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is not None
    assert result["shipment_id"] == "demand-uuid-0001"


def test_sozdaetsya_na_uspeshno_realizovano_v_ofise(monkeypatch):
    """Офис + «Успешно реализовано» (142) - отгрузка создаётся."""
    _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_SUCCESS)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is not None


def test_status_142_v_chuzhoy_voronke_ne_sozdaet(monkeypatch):
    """142 - системный статус на все воронки. Сделка закрыта в [CLEVER]
    Основная (не Офис) - отгрузку создавать НЕЛЬЗЯ, иначе спишем товар зря
    на закрытие сделки в чужой воронке."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_CLEVER, STATUS_SUCCESS)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["post"] == []
    assert calls["patch"] == []


def test_ne_celevoy_etap_v_ofise_ne_sozdaet(monkeypatch):
    """Воронка Офис, но этап не из целевых трёх - не наша сделка."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, 999999)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["get"] == []
    assert calls["put"] == []


def test_pole_id_otgruzki_uzhe_zapolneno_ne_sozdaet(monkeypatch):
    """«ID Отгрузки» уже заполнено в сделке - выходим молча, до склада не
    достаём вовсе (гейт 1 - самый дешёвый, идёт первым)."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY, shipment_id="already-created")

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["get"] == []
    assert calls["put"] == []
    assert calls["post"] == []


def test_pustoe_pole_zakaza_ne_sozdaet(monkeypatch):
    """Нет «ID Заказа» (576689) - создавать отгрузку не по чему."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY, order_uuid=None)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["post"] == []


def test_sklad_ne_otvetil_na_proverku_ne_sozdaet(monkeypatch):
    """entity/demand (проверка существующих) вернул None - склад НЕ ОТВЕТИЛ,
    это не значит «отгрузки нет». Ничего не создаём."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)

    async def fake_get_none(path, params=None):
        calls["get"].append((path, params))
        return None

    monkeypatch.setattr(amgroup_shipment.ms_client, "get", fake_get_none)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["put"] == []
    assert calls["post"] == []


def test_otgruzka_uzhe_est_v_mojskladom_ne_sozdaet_vtoruyu(monkeypatch):
    """Проверка entity/demand по заказу вернула существующую отгрузку -
    выходим молча, вторую не создаём."""
    calls = _wire(
        monkeypatch,
        demand_search_rows=[{"id": "already-there", "name": "00003"}],
        template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND,
    )
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["put"] == []
    assert calls["post"] == []


def test_shablon_ne_otvetil_nichego_ne_sozdaet(monkeypatch):
    """PUT entity/demand/new вернул None - шаблон недоступен, POST не зовём."""
    calls = _wire(monkeypatch, template=None, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is None
    assert calls["post"] == []


def test_povtornyy_vyzov_ne_sozdaet_vtoruyu_otgruzku(monkeypatch):
    """Диск (гейт 3): вторая сделка того же лида по амо не приходит дважды в
    реальности, но состояние на диске обязано пережить процесс. Здесь -
    прямая проверка: второй вызов той же функции для той же сделки не должен
    создавать вторую отгрузку, даже если поле «ID Отгрузки» почему-то не
    записалось (симулируем неудачный PATCH на первом вызове)."""
    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND, patch_ok=False)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    first = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))
    second = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert first is not None
    assert second is None
    assert len(calls["post"]) == 1  # POST на создание отгрузки был только один раз


def test_diskovoe_sostoyanie_perezhivaet_perezagruzku_modulya(monkeypatch, tmp_path):
    """Состояние - на диске, а не только в памяти процесса: имитируем
    «перезапуск» явной очисткой in-memory кэша и повторной загрузкой файла."""
    state_path = tmp_path / "created.json"
    monkeypatch.setattr(amgroup_shipment, "_STATE_PATH", str(state_path), raising=False)
    monkeypatch.setattr(amgroup_shipment, "_state_loaded", False, raising=False)
    amgroup_shipment._created.clear()

    _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)
    first = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))
    assert first is not None
    assert state_path.exists()

    # «Перезапуск процесса»: чистим память, состояние читаем заново с диска.
    amgroup_shipment._created.clear()
    monkeypatch.setattr(amgroup_shipment, "_state_loaded", False, raising=False)

    calls = _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND)
    second = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert second is None
    assert calls["post"] == []


def test_zapis_polya_sklada_padaet_na_default_esli_sklad_ne_otvetil(monkeypatch):
    """Имя склада не удалось получить (entity/store/... вернул None) - в
    поле сделки идёт запасное имя по умолчанию, отгрузка при этом всё равно
    считается созданной (товар уже списан)."""
    _wire(monkeypatch, template=_DEMAND_TEMPLATE, created_demand=_CREATED_DEMAND, store_name=None)
    lead = _lead(PIPELINE_OFFICE, STATUS_WAYBILL_READY)

    result = asyncio.run(amgroup_shipment.create_shipment_for_lead(lead))

    assert result is not None
    assert result["warehouse"] == amgroup_shipment.DEFAULT_STORE_NAME
