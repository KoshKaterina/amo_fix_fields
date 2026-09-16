"""Isolated contract tests: no application startup, dotenv, socket or database."""

import os
from pathlib import Path
import socket
import subprocess
import sys
import types
from unittest.mock import patch

import pytest


def _forbidden(*_args, **_kwargs):
    raise AssertionError("network/dotenv access is forbidden in parser tests")


dotenv_stub = types.ModuleType("dotenv")
dotenv_stub.load_dotenv = _forbidden
dotenv_stub.find_dotenv = _forbidden

# Block before importing the unit under test, then restore collection state.
with (patch.object(socket, "socket", _forbidden),
      patch.object(socket, "create_connection", _forbidden),
      patch.object(socket, "getaddrinfo", _forbidden),
      patch.dict(sys.modules, {"dotenv": dotenv_stub})):
    from ms_preorder_type import ParsedOrderType, parse_ms_preorder_type  # noqa: E402


@pytest.fixture(autouse=True)
def _guard_each_test(monkeypatch):
    """Keep guards active only for this test; pytest restores every patch."""
    monkeypatch.setattr(socket, "socket", _forbidden)
    monkeypatch.setattr(socket, "create_connection", _forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", _forbidden)
    monkeypatch.setitem(sys.modules, "dotenv", dotenv_stub)


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
    (_order({"id": None, "meta": {"href": HREF}, "value": PREORDER}),
     ATTR, "attribute_identity_conflict"),
    (_order({"id": ATTR, "meta": {"href": None}, "value": PREORDER}),
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


@pytest.mark.parametrize("dotenv_present", [False, True])
def test_import_guard_restores_other_tests_in_safe_subprocess(dotenv_present):
    """Collection must not leave socket or dotenv changed for unrelated tests."""
    script = r'''
import socket
import sys
import types

class OuterSocket(socket.socket):
    def __new__(cls, *args, **kwargs):
        raise AssertionError("no real sockets")

def outer_connection(*args, **kwargs):
    raise AssertionError("no real connections")

def outer_dns(*args, **kwargs):
    raise AssertionError("no real DNS")

socket.socket = OuterSocket
socket.create_connection = outer_connection
socket.getaddrinfo = outer_dns
if sys.argv[1] == "present":
    old_dotenv = types.ModuleType("dotenv")
    old_dotenv.load_dotenv = lambda: "existing sentinel"
    sys.modules["dotenv"] = old_dotenv
else:
    sys.modules.pop("dotenv", None)
    old_dotenv = None

import test_ms_preorder_type
assert socket.socket is OuterSocket
assert socket.create_connection is outer_connection
assert socket.getaddrinfo is outer_dns
assert sys.modules.get("dotenv") is old_dotenv
'''
    result = subprocess.run(
        [sys.executable, "-B", "-c", script, "present" if dotenv_present else "absent"],
        cwd=Path(__file__).resolve().parent,
        env={"PATH": os.pathsep.join((str(Path(sys.executable).parent), "/usr/bin", "/bin")),
             "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1", "PYTHONNOUSERSITE": "1"},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
