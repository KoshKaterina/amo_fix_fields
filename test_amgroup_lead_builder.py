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
import types

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def _install_stubs():
    # aiogram (зависимость telegram_bot.py, который тянет lead_distribution)
    # в тестовом окружении не стоит - подменяем модуль целиком, как
    # test_amgroup_duplicate_watch.py и test_order_watchdog.py.
    if "telegram_bot" not in sys.modules:
        tg = types.ModuleType("telegram_bot")

        async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
            return True

        tg.send_alert = send_alert
        sys.modules["telegram_bot"] = tg


_install_stubs()

import amgroup_lead_builder as builder  # noqa: E402
import lead_distribution  # noqa: E402

PREORDER_ATTRIBUTE_UUID = "aef73872-b202-11f1-0a80-00c30002599a"


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
    monkeypatch.setattr(builder, "AMGROUP_PREORDER_TYPE_ENABLED", False)
    monkeypatch.setattr(builder, "MS_ATTR_PREORDER_SUMMARY_ID", "")
    yield
    builder._contact_cache.clear()


def _stub_ms_ok(monkeypatch, order):
    async def fake_get(path, params=None):
        return order

    monkeypatch.setattr(builder.ms_client, "get", fake_get)


def _stub_amo_empty(monkeypatch, *, contacts=None, leads=None):
    """По умолчанию amo не находит ни контактов, ни сделок. contacts=None
    (не "не найдено") зовёт _AmoSearchFailed - для теста молчания amoCRM на
    пути поиска контакта. Так же leads=None - молчание на пути поиска сделки."""
    async def fake_find_contacts(query, limit=10):
        if contacts is None:
            return []
        return contacts

    async def fake_find_leads(query, with_=(), limit=50):
        if leads is None:
            return []
        return leads

    monkeypatch.setattr(builder.amo_service, "find_contacts_by_query", fake_find_contacts)
    monkeypatch.setattr(builder.amo_service, "find_leads_by_query", fake_find_leads)


def _stub_amo_leads_silent(monkeypatch, *, contacts=None):
    """amoCRM не отвечает на поиск СДЕЛКИ (find_leads_by_query -> None) -
    поиск контакта при этом отвечает как обычно."""
    async def fake_find_contacts(query, limit=10):
        return contacts or []

    async def fake_find_leads(query, with_=(), limit=50):
        return None

    monkeypatch.setattr(builder.amo_service, "find_contacts_by_query", fake_find_contacts)
    monkeypatch.setattr(builder.amo_service, "find_leads_by_query", fake_find_leads)


def _stub_amo_contacts_silent(monkeypatch):
    """Дедуп сделки отвечает честным «не найдено», а поиск КОНТАКТА молчит
    (find_contacts_by_query -> None)."""
    async def fake_find_contacts(query, limit=10):
        return None

    async def fake_find_leads(query, with_=(), limit=50):
        return []

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
    assert calls["last_lead_kwargs"]["pipeline_id"] == builder.PIPELINE_CLEVER_MAIN
    assert calls["last_lead_kwargs"]["status_id"] == builder.STATUS_CLEVER_NEW_LEAD
    assert calls["last_lead_kwargs"]["tags"] == [builder.AMGROUP_FALLBACK_TAG]
    # ответственный - отдельный PATCH после создания, не часть тела создания
    assert "responsible_user_id" not in calls["last_lead_kwargs"]
    # бюджет - тоже отдельный PATCH (у create_lead_direct нет параметра суммы,
    # api.py не трогаем), уходит ПЕРЕД проставлением ответственного
    assert calls["patch"] == [(777, {"price": 13990}), (777, {"responsible_user_id": 999})]


def _preorder_attribute(value, *, uuid=PREORDER_ATTRIBUTE_UUID):
    return {"id": uuid, "name": "Предзаказ: ожидаемые позиции", "value": value}


def test_disabled_gate_keeps_legacy_order_type_even_with_summary(monkeypatch):
    order = _ms_order("uuid-legacy", "08000", site="")
    order["attributes"].append(_preorder_attribute(
        "Тип: Предзаказ\nОжидаемые позиции:\n- Tangem 2.0 White [SKU: T-1] × 1"
    ))
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch)

    assert asyncio.run(builder.create_lead_for_order({"id": order["id"]})) == 777
    fields = {field["field_id"]: field["values"][0]
              for field in calls["last_lead_kwargs"]["custom_fields_values"]}
    assert fields[builder.FIELD["type"]] == {"enum_id": builder.ENUM["type"]["Заказ"]}


@pytest.mark.parametrize(
    ("summary", "expected_type", "mixed"),
    [
        ("Тип: Заказ\nОжидаемые позиции: нет", "Заказ", False),
        ("Тип: Предзаказ\nОжидаемые позиции:\n- Tangem 2.0 White [SKU: T-1] × 1", "Предзаказ", False),
        ("Тип: Предзаказ\nОжидаемые позиции:\n- Tangem 2.0 White [SKU: T-1] × 1", "Предзаказ", True),
    ],
)
def test_preorder_type_is_in_initial_create_payload(monkeypatch, summary, expected_type, mixed):
    """Новый признак меняет только enum при первоначальном POST, не связку/состав."""
    order = _ms_order("uuid-preorder", "08001", site="")
    order["attributes"].append(_preorder_attribute(summary))
    if mixed:
        order["positions"]["rows"].append({
            "quantity": 2, "price": 50000,
            "assortment": {"name": "Обычный товар", "meta": {"type": "product"}},
        })
    monkeypatch.setattr(builder, "AMGROUP_PREORDER_TYPE_ENABLED", True)
    monkeypatch.setattr(builder, "MS_ATTR_PREORDER_SUMMARY_ID", PREORDER_ATTRIBUTE_UUID)
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch)

    assert asyncio.run(builder.create_lead_for_order({"id": order["id"]})) == 777

    lead = calls["last_lead_kwargs"]
    fields = {field["field_id"]: field["values"][0] for field in lead["custom_fields_values"]}
    assert fields[builder.FIELD["type"]] == {"enum_id": builder.ENUM["type"][expected_type]}
    assert fields[builder.FIELD["order_uuid"]] == {"value": order["id"]}
    assert fields[builder.FIELD["order_num"]] == {"value": order["name"]}
    assert order["id"] in fields[builder.FIELD["order_url"]]["value"]
    assert "Tangem 2.0 White" in fields[builder.FIELD["sostav"]]["value"]
    assert ("Обычный товар" in fields[builder.FIELD["sostav"]]["value"]) == mixed
    assert lead["name"] == "Заказ МС 08001"  # тестовый заказ без номера сайта
    assert builder.FIELD["site"] not in fields
    assert all("custom_fields_values" not in patch for _, patch in calls["patch"])


@pytest.mark.parametrize(
    ("attribute", "expected_reason"),
    [
        (None, "attribute_not_found"),
        (_preorder_attribute("Тип: Заказ"), "invalid_summary"),
        (_preorder_attribute("Тип: Заказ\nОжидаемые позиции: нет", uuid="11111111-1111-1111-1111-111111111111"), "attribute_not_found"),
    ],
)
def test_enabled_unknown_type_holds_new_lead_before_any_write(monkeypatch, caplog, attribute, expected_reason):
    order = _ms_order("uuid-unknown", "08002", site="")
    if attribute:
        order["attributes"].append(attribute)
    monkeypatch.setattr(builder, "AMGROUP_PREORDER_TYPE_ENABLED", True)
    monkeypatch.setattr(builder, "MS_ATTR_PREORDER_SUMMARY_ID", PREORDER_ATTRIBUTE_UUID)
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch)

    with caplog.at_level("WARNING", logger="uvicorn"):
        assert asyncio.run(builder.create_lead_for_order({"id": order["id"]})) is None

    assert calls == {"contacts": 0, "leads": 0, "patch": []}
    assert any(expected_reason in record.message for record in caplog.records)
    assert all("Tangem" not in record.message for record in caplog.records)


def test_enabled_without_configured_uuid_holds_new_lead(monkeypatch, caplog):
    order = _ms_order("uuid-no-config", "08003", site="")
    order["attributes"].append(_preorder_attribute("Тип: Заказ\nОжидаемые позиции: нет"))
    monkeypatch.setattr(builder, "AMGROUP_PREORDER_TYPE_ENABLED", True)
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch)

    with caplog.at_level("WARNING", logger="uvicorn"):
        assert asyncio.run(builder.create_lead_for_order({"id": order["id"]})) is None
    assert calls["contacts"] == calls["leads"] == 0
    assert any("invalid_expected_uuid" in record.message for record in caplog.records)


@pytest.mark.parametrize("attributes", [[None], {"bad": "shape"}])
def test_malformed_attributes_reach_fail_closed_parser(monkeypatch, caplog, attributes):
    """Ранний lookup номера сайта не должен скрывать unknown исключением."""
    order = _ms_order("uuid-malformed", "08005", site="")
    order["attributes"] = attributes
    monkeypatch.setattr(builder, "AMGROUP_PREORDER_TYPE_ENABLED", True)
    monkeypatch.setattr(builder, "MS_ATTR_PREORDER_SUMMARY_ID", PREORDER_ATTRIBUTE_UUID)
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch)

    with caplog.at_level("WARNING", logger="uvicorn"):
        assert asyncio.run(builder.create_lead_for_order({"id": order["id"]})) is None

    assert calls == {"contacts": 0, "leads": 0, "patch": []}
    assert any("invalid_attributes" in record.message for record in caplog.records)


def test_existing_lead_not_reclassified_even_if_summary_is_unknown(monkeypatch):
    order = _ms_order("uuid-existing", "08004", site="")
    monkeypatch.setattr(builder, "AMGROUP_PREORDER_TYPE_ENABLED", True)
    monkeypatch.setattr(builder, "MS_ATTR_PREORDER_SUMMARY_ID", PREORDER_ATTRIBUTE_UUID)
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch, leads=[{"id": 771, "name": "Заказ МС 08004"}])
    calls = _stub_create(monkeypatch)

    assert asyncio.run(builder.create_lead_for_order({"id": order["id"]})) == 771
    assert calls == {"contacts": 0, "leads": 0, "patch": []}


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


def test_bez_otvetstvennogo_v_nastroykah_sdelka_ne_sozdaetsya(monkeypatch, caplog):
    """AMGROUP_LEAD_RESPONSIBLE_USER_ID не задан - сделка НЕ создаётся вовсе
    (находка приёмки безопасности 03.09.2026: без ответственного боты amoCRM
    на сделку не реагируют, распределения не будет, и она молча оседает на
    пользователе интеграции - раньше это была только WARNING в лог, теперь
    явный отказ от создания, видный как ERROR)."""
    monkeypatch.setattr(builder, "AMGROUP_LEAD_RESPONSIBLE_USER_ID", None, raising=False)
    order = _ms_order("uuid-5", "07306")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=777)

    with caplog.at_level("ERROR", logger="uvicorn"):
        result = asyncio.run(builder.create_lead_for_order({"id": "uuid-5", "name": "07306"}))

    assert result is None
    assert calls["leads"] == 0
    assert calls["contacts"] == 0
    assert calls["patch"] == []
    assert any("AMGROUP_LEAD_RESPONSIBLE_USER_ID не задан" in rec.message for rec in caplog.records)


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


def test_amo_ne_otvetil_pri_poiske_dublya_sdelku_ne_sozdaem(monkeypatch, caplog):
    """amoCRM не ответил на поиск дубля сделки (find_leads_by_query -> None) -
    создание прерывается ЦЕЛИКОМ: ни контакт, ни сделка не заводятся, лучше
    повтор на следующем проходе, чем риск дубля."""
    order = _ms_order("uuid-7", "07308")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_leads_silent(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=777)

    with caplog.at_level("WARNING", logger="uvicorn"):
        result = asyncio.run(builder.create_lead_for_order({"id": "uuid-7", "name": "07308"}))

    assert result is None
    assert calls["leads"] == 0
    assert calls["contacts"] == 0
    assert any("amoCRM не ответил" in rec.message and "дубля сделки" in rec.message for rec in caplog.records)


def test_amo_ne_otvetil_pri_poiske_kontakta_sdelku_ne_sozdaem(monkeypatch, caplog):
    """Дедуп сделки прошёл честно (сделки нет), а поиск КОНТАКТА молчит
    (find_contacts_by_query -> None) - сделка тоже не создаётся: не увидели
    существующий контакт из-за сбоя не значит, что его нет."""
    order = _ms_order("uuid-8", "07309")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_contacts_silent(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=777)

    with caplog.at_level("WARNING", logger="uvicorn"):
        result = asyncio.run(builder.create_lead_for_order({"id": "uuid-8", "name": "07309"}))

    assert result is None
    assert calls["leads"] == 0
    assert calls["contacts"] == 0
    assert any("amoCRM не ответил" in rec.message and "поиске контакта" in rec.message for rec in caplog.records)


def test_dlinnyy_sostav_zakaza_obrezaetsya_potolkom_polya(monkeypatch):
    """Заказ из многих позиций даёт длинный «Состав заказа» - t() обязана
    резать его sanitize_custom_field_value (потолок 256 символов), иначе
    amoCRM отклонит всю сделку целиком (находка приёмки безопасности
    03.09.2026)."""
    order = _ms_order("uuid-9", "07310")
    many_positions = [
        {
            "quantity": 1,
            "price": 100000,
            "assortment": {
                "name": f"Товар с очень длинным названием номер {i} для теста обрезки поля",
                "weight": 0.01,
                "volume": 0.001,
                "meta": {"type": "product"},
            },
        }
        for i in range(20)
    ]
    order["positions"] = {"rows": many_positions}

    b = builder._build_fields(order)
    assert len(b["sostav"]) > 256  # исходное значение точно длиннее потолка

    cf = builder._custom_fields(order, b, "12345")
    by_field = {f["field_id"]: f["values"][0] for f in cf}
    sostav_value = by_field[builder.FIELD["sostav"]]["value"]

    assert len(sostav_value) <= 256


def test_oshibka_sozdaniya_kontakta_ne_svetit_telefon_v_loge(monkeypatch, caplog):
    """Контакт не создался (api.create_contact вернул None) - сделка всё
    равно создаётся без привязки к контакту (contact_id=None, ничего не
    глотаем), но в логе про сбой контакта - только номер заказа, сырой
    телефон клиента светить нельзя (находка приёмки безопасности 03.09.2026:
    PII в логах)."""
    order = _ms_order("uuid-10", "07311", phone="+79261234567")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=None, lead_id=777)

    with caplog.at_level("ERROR", logger="uvicorn"):
        result = asyncio.run(builder.create_lead_for_order({"id": "uuid-10", "name": "07311"}))

    assert result == 777
    assert calls["leads"] == 1
    assert calls["last_lead_kwargs"]["contact_id"] is None
    error_messages = [rec.message for rec in caplog.records if rec.levelname == "ERROR"]
    assert any("не создался контакт" in m and "07311" in m for m in error_messages)
    assert not any("9261234567" in m or "Иван Иванов" in m for m in error_messages)


# ── выбор ответственного: самовывоз → офис-менеджер, иначе распределитель ──

def _amo_lead_for_pick(lead_id=777, *, delivery="CDEK: Курьер"):
    return {
        "id": lead_id,
        "pipeline_id": builder.PIPELINE_CLEVER_MAIN,
        "status_id": builder.STATUS_CLEVER_NEW_LEAD,
        "custom_fields_values": [
            {"field_id": lead_distribution.FIELD_DELIVERY_TYPE, "values": [{"value": delivery}]},
        ],
        "_embedded": {"contacts": [{"id": 555}], "tags": []},
    }


def _wire_distribution(monkeypatch, *, lead, decision, profile_enabled=True):
    """Распределитель включён, профиль на «Основная / Новый лид» есть,
    decide_and_record отдаёт decision (None - никого не выбрал)."""
    calls = {"decide": []}
    profile = lead_distribution.Profile(
        id="p1", name="Основное правило", enabled=profile_enabled,
        entry_points=[{"pipeline_id": builder.PIPELINE_CLEVER_MAIN,
                       "status_ids": [builder.STATUS_CLEVER_NEW_LEAD]}],
        source_ids=[23478413], participant_ids=[9291546, 13929334],
    )

    async def fake_get_lead_full(lead_id, with_=("contacts", "companies")):
        return lead

    async def fake_decide(lead_, profile_, *, meta=None):
        calls["decide"].append(profile_.id)
        if meta is not None:
            meta["rule"] = "load"
        return decision

    monkeypatch.setattr(builder, "LEAD_DISTRIBUTION_ENABLED", True, raising=False)
    monkeypatch.setattr(builder.amo_service, "get_lead_full", fake_get_lead_full)
    monkeypatch.setattr(builder.lead_distribution, "list_profiles", lambda: [profile])
    monkeypatch.setattr(builder.lead_distribution, "decide_and_record", fake_decide)
    return calls


def test_otvetstvennogo_vybiraet_raspredelitel_po_smene(monkeypatch):
    """Обычная доставка, распределитель включён - ответственный тот, кого
    выбрал распределитель по смене (как у сделок amgroup), а не константа."""
    order = _ms_order("uuid-r1", "07401")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=777)
    dist = _wire_distribution(monkeypatch, lead=_amo_lead_for_pick(), decision=9291546)

    result = asyncio.run(builder.create_lead_for_order({"id": "uuid-r1", "name": "07401"}))

    assert result == 777
    assert dist["decide"] == ["p1"]
    assert calls["patch"][-1] == (777, {"responsible_user_id": 9291546})


def test_samovyvoz_iz_ofisa_uhodit_ofis_menedzheru_bez_raspredelitelya(monkeypatch):
    """Самовывоз из офиса - офис-менеджер напрямую (правило Кати 03.09.2026),
    распределитель даже не спрашиваем."""
    order = _ms_order("uuid-r2", "07402")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=778)
    dist = _wire_distribution(
        monkeypatch, lead=_amo_lead_for_pick(778, delivery="Самовывоз из офиса Sunscrypt"), decision=9291546,
    )

    asyncio.run(builder.create_lead_for_order({"id": "uuid-r2", "name": "07402"}))

    assert dist["decide"] == []
    assert calls["patch"][-1] == (778, {"responsible_user_id": builder.RESPONSIBLE_OFFICE_MANAGER_USER_ID})


def test_raspredelitel_nikogo_ne_vybral_beryom_konstantu(monkeypatch):
    """Пул пуст, дежурного нет - запасной ход: константа из настроек."""
    order = _ms_order("uuid-r3", "07403")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=779)
    _wire_distribution(monkeypatch, lead=_amo_lead_for_pick(779), decision=None)

    asyncio.run(builder.create_lead_for_order({"id": "uuid-r3", "name": "07403"}))

    assert calls["patch"][-1] == (779, {"responsible_user_id": 999})


def test_bez_konstanty_no_s_raspredelitelem_sdelka_sozdaetsya(monkeypatch):
    """Константа не задана, но распределитель включён - сделка создаётся,
    ответственного даёт распределитель."""
    monkeypatch.setattr(builder, "AMGROUP_LEAD_RESPONSIBLE_USER_ID", None, raising=False)
    order = _ms_order("uuid-r4", "07404")
    _stub_ms_ok(monkeypatch, order)
    _stub_amo_empty(monkeypatch)
    calls = _stub_create(monkeypatch, contact_id=555, lead_id=780)
    _wire_distribution(monkeypatch, lead=_amo_lead_for_pick(780), decision=13929334)

    result = asyncio.run(builder.create_lead_for_order({"id": "uuid-r4", "name": "07404"}))

    assert result == 780
    assert calls["patch"][-1] == (780, {"responsible_user_id": 13929334})
