"""Тесты сборки и создания сделки-протеза amgroup (amgroup_lead_builder).

Точка расширения amgroup_fallback.create_lead_for_order - здесь её реализация:
сборка полей заказа, поиск/создание контакта, дедуп по имени сделки,
отдельный шаг простановки ответственного. amgroup_fallback (сам отсев
Озона/TangemShop, чтение окна заказов) тестируется отдельно в
test_amgroup_fallback.py.

Запуск: python3 -m pytest test_amgroup_lead_builder.py -q
"""

import asyncio
import os
import sys

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import amgroup_lead_builder as builder  # noqa: E402


def _ms_order(uuid, number, *, site="12345", phone="+79991234567", agent_name="Иван Иванов"):
    """Полная карточка заказа МойСклад, как её отдаёт _fetch_full_order
    (с раскрытыми positions/agent/organization/store/salesChannel)."""
    return {
        "id": uuid,
        "name": number,
        "moment": "2026-09-03 12:00:00",
        "sum": 1399000,  # 13 990.00 руб, в рублях (не в копейках)
        "payedSum": 0,
        "shipmentAddress": "г. Москва, ул. Тестовая, 1",
        "description": "",
        "attributes": [
            {"name": "Номер заказа на сайте", "value": site},
            {"name": "Способ оплаты", "value": "Картой на сайте"},
        ],
        "agent": {"id": "agent-uuid-1", "name": agent_name, "phone": phone},
        "organization": {"name": "ИП Тест"},
        "store": {"name": "Sunscrypt Основной"},
        "salesChannel": {"name": "Магазин"},
        "positions": {
            "rows": [
                {
                    "quantity": 1,
                    "price": 1399000,
                    "assortment": {
                        "name": "Tangem 2.0 White, 3 карты",
                        "weight": 0.05,
                        "volume": 0.001,
                        "meta": {"type": "product"},
                    },
                },
            ]
        },
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    """Кэш контактов - состояние процесса, между тестами должен обнуляться,
    иначе один тест видит контакт, созданный в другом."""
    builder._contact_cache.clear()
    monkeypatch.setattr(builder, "AMGROUP_LEAD_RESPONSIBLE_USER_ID", 999, raising=False)
    yield
    builder._contact_cache.clear()


def _stub_ms_ok(monkeypatch, order):
    async def fake_get(path, params=None):
        return order

    monkeypatch.setattr(builder.ms_client, "get", fake_get)


def _stub_amo_empty(monkeypatch, *, contacts=None, leads=None):
    """По умолчанию amo не находит ни контактов, ни сделок."""
    async def fake_find_contacts(query, limit=10):
        return contacts or []

    async def fake_find_leads(query, with_=()):
        return leads or []

    monkeypatch.setattr(builder.amo_service, "find_contacts_by_query", fake_find_contacts)
    monkeypatch.setattr(builder.amo_service, "find_leads_by_query", fake_find_leads)


def _stub_create(monkeypatch, *, contact_id=555, lead_id=777):
    calls = {"contacts": 0, "leads": 0, "patch": []}

    async def fake_create_contact(name, phone, email):
        calls["contacts"] += 1
        return contact_id

    async def fake_create_lead_direct(**kwargs):
        calls["leads"] += 1
        calls["last_lead_kwargs"] = kwargs
        return lead_id

    async def fake_patch_lead(lead_id_, **kwargs):
        calls["patch"].append((lead_id_, kwargs))
        return {"ok": True, "status_code": 200}

    monkeypatch.setattr(builder.api, "create_contact", fake_create_contact)
    monkeypatch.setattr(builder.api, "create_lead_direct", fake_create_lead_direct)
    monkeypatch.setattr(builder.amo_service, "patch_lead", fake_patch_lead)
    return calls


def test_sklad_ne_otvetil_nichego_ne_sozdaem(monkeypatch, caplog):
    """МойСклад не ответил на карточку заказа (None) - сделку не создаём,
    ничего не пишем в amoCRM, причина видна в логе."""
    async def fake_get(path, params=None):
        return None

    monkeypatch.setattr(builder.ms_client, "get", fake_get)
    calls = _stub_create(monkeypatch)

    with caplog.at_level("WARNING", logger="uvicorn"):
        result = asyncio.run(builder.create_lead_for_order({"id": "uuid-x", "name": "07300"}))

    assert result is None
    assert calls["leads"] == 0
    assert calls["contacts"] == 0
    assert any("не ответил" in rec.message for rec in caplog.records)


def test_sobrannye_polya_sovpadayut_s_etalonom(monkeypatch):
    """Собранный набор custom_fields_values - эталонные значения (карта
    полей и ENUM перенесены из прототипа, здесь проверяем, что сборка не
    разошлась с ними)."""
    order = _ms_order("uuid-1", "07301")
    b = builder._build_fields(order)

    assert b["goods"] == ["Tangem 2.0 White, 3 карты, 1 шт, 13 990.00 рублей"]
    assert b["paystatus"] == "Не оплачен"
    assert b["weight"] == 0.05
    assert b["volume"] == 0.001
    assert "Заказ № 07301 от 03.09.2026:" in b["sostav"]
    assert "Итого: 13 990.00 рублей" in b["sostav"]

    cf = builder._custom_fields(order, b, "12345")
    by_field = {f["field_id"]: f["values"][0] for f in cf}

    assert by_field[builder.FIELD["site"]]["value"] == "12345"
    assert by_field[builder.FIELD["pay"]]["value"] == "Картой на сайте"
    assert by_field[builder.FIELD["addr"]]["value"] == "г. Москва, ул. Тестовая, 1"
    assert by_field[builder.FIELD["order_uuid"]]["value"] == "uuid-1"
    assert by_field[builder.FIELD["order_num"]]["value"] == "07301"
    assert by_field[builder.FIELD["agent"]]["value"] == "Иван Иванов"
    assert by_field[builder.FIELD["org"]]["value"] == "ИП Тест"
    assert by_field[builder.FIELD["basket"]]["value"] == "Tangem 2.0 White, 3 карты, 1 шт, 13 990.00 рублей"
    assert by_field[builder.FIELD["channel"]]["enum_id"] == builder.ENUM["channel"]["Магазин"]
    assert by_field[builder.FIELD["store"]]["enum_id"] == builder.ENUM["store"]["Sunscrypt Основной"]
    assert by_field[builder.FIELD["currency"]]["enum_id"] == builder.ENUM["currency"]["руб"]
    assert by_field[builder.FIELD["paystatus"]]["enum_id"] == builder.ENUM["paystatus"]["Не оплачен"]
    assert by_field[builder.FIELD["created_by"]]["enum_id"] == builder.ENUM["created_by"]["Из МойСклад"]
    assert by_field[builder.FIELD["type"]]["enum_id"] == builder.ENUM["type"]["Заказ"]


def test_sozdaet_sdelku_i_stavit_otvetstvennogo(monkeypatch):
    """Обычный путь: сделка создаётся, контакт создаётся, ответственный
    проставляется отдельным явным шагом (боты amo сами это не делают)."""
    order = _ms_order("uuid-2", "07302")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=777)

    result = asyncio.run(builder.create_lead_for_order({"id": "uuid-2", "name": "07302"}))

    assert result == 777
    assert calls["contacts"] == 1
    assert calls["leads"] == 1
    assert calls["last_lead_kwargs"]["contact_id"] == 555
    assert calls["last_lead_kwargs"]["pipeline_id"] == builder.PIPELINE_CLEVER
    assert calls["last_lead_kwargs"]["status_id"] == builder.STATUS_CLEVER_NEW_LEAD
    assert calls["last_lead_kwargs"]["tags"] == [builder.AMGROUP_FALLBACK_TAG]
    # ответственный - отдельный PATCH после создания, не часть тела создания
    assert "responsible_user_id" not in calls["last_lead_kwargs"]
    assert calls["patch"] == [(777, {"responsible_user_id": 999})]


def test_povtornyy_vyzov_na_tom_zhe_zakaze_ne_sozdaet_sdelku(monkeypatch):
    """Повторный вызов на том же заказе (сделка уже есть под тем же именем)
    сделку не создаёт - находит существующую и возвращает её id."""
    order = _ms_order("uuid-3", "07303", site="99999")
    _stub_ms_ok(monkeypatch, order)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=777)

    # первый вызов: сделки ещё нет
    _stub_amo_empty(monkeypatch)
    first = asyncio.run(builder.create_lead_for_order({"id": "uuid-3", "name": "07303"}))
    assert first == 777
    assert calls["leads"] == 1

    # второй вызов: amo теперь находит сделку с тем же именем при поиске
    existing_lead = {"id": 777, "name": "Заказ №99999"}
    _stub_amo_empty(monkeypatch, leads=[existing_lead])
    second = asyncio.run(builder.create_lead_for_order({"id": "uuid-3", "name": "07303"}))

    assert second == 777
    assert calls["leads"] == 1  # create_lead_direct не позвали второй раз
    assert calls["contacts"] == 1  # и контакт второй раз не создавали


def test_dva_zakaza_odnogo_klienta_odin_kontakt(monkeypatch):
    """У клиента два заказа подряд - контакт создаётся один раз, второй
    заказ берёт id из кэша процесса, а не создаёт дубль человека."""
    order1 = _ms_order("uuid-4a", "07304", site="10001", phone="+79997654321")
    order2 = _ms_order("uuid-4b", "07305", site="10002", phone="+79997654321")
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=None)
    # у каждого заказа своя сделка, но лидов создаём с новым id по очереди
    lead_ids = iter([701, 702])

    async def fake_create_lead_direct(**kwargs):
        calls["leads"] += 1
        return next(lead_ids)

    monkeypatch.setattr(builder.api, "create_lead_direct", fake_create_lead_direct)
    _stub_amo_empty(monkeypatch)

    async def fake_get(path, params=None):
        # id заказа - последний сегмент пути entity/customerorder/<id>
        uuid = path.rsplit("/", 1)[-1]
        return order1 if uuid == "uuid-4a" else order2

    monkeypatch.setattr(builder.ms_client, "get", fake_get)

    first = asyncio.run(builder.create_lead_for_order({"id": "uuid-4a", "name": "07304"}))
    second = asyncio.run(builder.create_lead_for_order({"id": "uuid-4b", "name": "07305"}))

    assert first == 701
    assert second == 702
    assert calls["contacts"] == 1  # контакт создан только один раз
    assert builder._contact_cache["79997654321"] == 555


def test_bez_otvetstvennogo_v_nastroykah_sdelka_vse_ravno_sozdaetsya(monkeypatch, caplog):
    """AMGROUP_LEAD_RESPONSIBLE_USER_ID не задан - сделка всё равно
    создаётся (не глотаем результат), но простановка ответственного не
    происходит и это видно в логе как явная недоделка, а не тишина."""
    monkeypatch.setattr(builder, "AMGROUP_LEAD_RESPONSIBLE_USER_ID", None, raising=False)
    order = _ms_order("uuid-5", "07306")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=777)

    with caplog.at_level("WARNING", logger="uvicorn"):
        result = asyncio.run(builder.create_lead_for_order({"id": "uuid-5", "name": "07306"}))

    assert result == 777
    assert calls["patch"] == []
    assert any("не проставлен ответственный" in rec.message for rec in caplog.records)


def test_ne_sozdalas_sdelka_vozvrashaet_none(monkeypatch, caplog):
    """create_lead_direct не смог создать сделку (None) - функция возвращает
    None, а не глотает ошибку молча."""
    order = _ms_order("uuid-6", "07307")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=None)

    with caplog.at_level("ERROR", logger="uvicorn"):
        result = asyncio.run(builder.create_lead_for_order({"id": "uuid-6", "name": "07307"}))

    assert result is None
    assert calls["patch"] == []
    assert any("не создалась сделка" in rec.message for rec in caplog.records)
