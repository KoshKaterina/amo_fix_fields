"""Isolated contract tests: no application startup, dotenv, socket or database."""

import socket
import sys
import types

import pytest


def _forbidden(*_args, **_kwargs):
    raise AssertionError("network/dotenv access is forbidden in parser tests")


# Install fail-closed guards before importing the unit under test.
socket.socket = _forbidden
socket.create_connection = _forbidden
socket.getaddrinfo = _forbidden
dotenv_stub = types.ModuleType("dotenv")
dotenv_stub.load_dotenv = _forbidden
dotenv_stub.find_dotenv = _forbidden
sys.modules["dotenv"] = dotenv_stub

from ms_preorder_type import ParsedOrderType, parse_ms_preorder_type  # noqa: E402


ATTR = "123e4567-e89b-42d3-a456-426614174000"
OTHER = "123e4567-e89b-42d3-a456-426614174001"
HREF = f"https://api.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/attributes/{ATTR}"
ORDINARY = "Тип: Заказ\nОжидаемые позиции: нет"
PREORDER = "Тип: Предзаказ\nОжидаемые позиции:\n- Tangem Red [SKU: TR] × 2"
MIXED = "Тип: Предзаказ\nОжидаемые позиции:\n- Tangem Blue [SKU: TB] × 1"


def _order(attribute):
    # Local amgroup_lead_builder fixture has this customerorder envelope and
    # name-only legacy attributes; order_watchdog's MS read fixture uses id.
    return {"id": "synthetic-order", "attributes": [
        {"name": "Номер заказа на сайте", "value": "12345"}, attribute,
    ]}


@pytest.mark.parametrize("value,kind", [
    (ORDINARY, "order"),
    (PREORDER, "preorder"),
    (MIXED, "preorder"),
    ("Тип: Предзаказ\nОжидаемые позиции:\n- Red [SKU: ] × 2\n- Blue [SKU: BB] × 3", "preorder"),
])
def test_exact_task6_values_from_ms_read_attribute_id(value, kind):
    result = parse_ms_preorder_type(_order({"id": ATTR, "name": "renamed", "value": value}), ATTR)
    assert result == ParsedOrderType(kind, "ok")


def test_task6_writer_meta_href_representation_is_also_recognized():
    result = parse_ms_preorder_type(_order({"meta": {
        "href": HREF, "type": "attributemetadata", "mediaType": "application/json",
    }, "value": PREORDER}), ATTR)
    assert result == ParsedOrderType("preorder", "ok")


def test_same_id_and_href_is_one_attribute_not_duplicate():
    result = parse_ms_preorder_type(_order({"id": ATTR, "meta": {"href": HREF}, "value": ORDINARY}), ATTR)
    assert result == ParsedOrderType("order", "ok")


@pytest.mark.parametrize("customerorder,expected,reason", [
    (None, ATTR, "invalid_order"),
    ({}, "", "invalid_expected_uuid"),
    ({"attributes": None}, ATTR, "invalid_attributes"),
    ({"attributes": {}}, ATTR, "invalid_attributes"),
    ({"attributes": [None]}, ATTR, "invalid_attributes"),
    ({"attributes": []}, ATTR, "attribute_not_found"),
    (_order({"id": OTHER, "value": ORDINARY}), ATTR, "attribute_not_found"),
    (_order({"name": "Предзаказ", "value": PREORDER}), ATTR, "attribute_not_found"),
    (_order({"id": ATTR, "value": None}), ATTR, "invalid_attribute_value"),
    (_order({"id": ATTR, "value": {"text": PREORDER}}), ATTR, "invalid_attribute_value"),
    (_order({"id": ATTR, "meta": {"href": HREF.replace(ATTR, OTHER)}, "value": PREORDER}),
     ATTR, "attribute_identity_conflict"),
    (_order({"id": OTHER, "meta": {"href": HREF}, "value": PREORDER}),
     ATTR, "attribute_identity_conflict"),
    (_order({"id": ATTR, "meta": {"type": "customentity"}, "value": PREORDER}),
     ATTR, "attribute_identity_conflict"),
    (_order({"id": ATTR, "meta": "broken", "value": PREORDER}),
     ATTR, "attribute_identity_conflict"),
    (_order({"meta": {"href": HREF + "?x=1"}, "value": PREORDER}),
     ATTR, "attribute_not_found"),
    (_order({"meta": {"href": HREF.replace("https://", "http://")}, "value": PREORDER}),
     ATTR, "attribute_not_found"),
    (_order({"meta": {"href": HREF.replace("/api/remap/1.2/", "/other/")}, "value": PREORDER}),
     ATTR, "attribute_not_found"),
])
def test_identity_and_shape_fail_closed(customerorder, expected, reason):
    result = parse_ms_preorder_type(customerorder, expected)
    assert result == ParsedOrderType("unknown", reason)


def test_duplicate_expected_attribute_is_unknown_even_if_values_agree():
    customerorder = {"attributes": [
        {"id": ATTR, "value": PREORDER},
        {"meta": {"href": HREF}, "value": PREORDER},
    ]}
    assert parse_ms_preorder_type(customerorder, ATTR) == ParsedOrderType("unknown", "duplicate_attribute")


@pytest.mark.parametrize("value", [
    "Тип: Заказ",  # no second line
    "Тип: Заказ\nОжидаемые позиции:\n- Red [SKU: R] × 1",
    "Тип: Предзаказ\nОжидаемые позиции: нет",
    "Тип: Предзаказ\nОжидаемые позиции:",  # no waiting lines
    "Тип: Предзаказ\nОжидаемые позиции:\n",
    "Тип: Предзаказ\nОжидаемые позиции:\n- Red [SKU: R] × 0",
    "Тип: Предзаказ\nОжидаемые позиции:\n- Red [SKU: R] × -1",
    "Тип: Предзаказ\nОжидаемые позиции:\n- Red [SKU: R] × 01",
    "Тип: Предзаказ\nОжидаемые позиции:\n- Red [SKU: R] × 2\n",
    "Тип: Предзаказ\nОжидаемые позиции:\nRed [SKU: R] × 2",
    "Тип: Предзаказ\nОжидаемые позиции:\n-  [SKU: R] × 2",
    "Тип: Предзаказ\nОжидаемые позиции:\n- Red  Blue [SKU: R] × 2",
    "Тип: Предзаказ\nОжидаемые позиции:\n- Red [SKU: R  X] × 2",
    "Тип: Предзаказ\r\nОжидаемые позиции:\r\n- Red [SKU: R] × 2",
    "prefix Тип: Предзаказ\nОжидаемые позиции:\n- Red [SKU: R] × 2",
])
def test_non_contract_text_is_unknown(value):
    result = parse_ms_preorder_type(_order({"id": ATTR, "value": value}), ATTR)
    assert result == ParsedOrderType("unknown", "invalid_summary")


def test_reason_is_diagnostic_code_without_customer_content():
    customerorder = _order({"id": ATTR, "value": "Иван Иванов +79990000000"})
    result = parse_ms_preorder_type(customerorder, ATTR)
    assert result == ParsedOrderType("unknown", "invalid_summary")
    assert "Иван" not in repr(result)
