"""Тесты переноса ника Телеграма из контрагента МойСклада в контакт amo (telegram_contact).

Держим то, на чём модуль стоит:
  • ник берём у контрагента заказа, привязанного к сделке (576689), и пишем в «TelegramUsername_WZ»;
  • заполненное поле не трогаем - его ставит Wazzup по живой переписке или менеджер;
  • ник уже у другого контакта - не пишем: по этому полю антидубль склеивает контакты;
  • amo не ответила на поиск - не пишем;
  • мусор вместо ника (телефон, кириллица) не пишем.

Запуск: python3 -m pytest test_telegram_contact.py -q
"""

import asyncio
import os
import sys
import types

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if "telegram_bot" not in sys.modules:
    _tg = types.ModuleType("telegram_bot")

    async def _send_alert(*a, **k):
        return True

    _tg.send_alert = _send_alert
    sys.modules["telegram_bot"] = _tg

import telegram_contact as T  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_MOYSKLAD_ORDER_UUID,
    MS_ATTR_COUNTERPARTY_TELEGRAM_ID,
    TELEGRAM_CONTACT_FIELD_ID,
)

LEAD = 36556393
CONTACT = 48594653
ORDER = "5a26d603-b113-11f1-0a80-1075003ce503"


def _lead(order_uuid=ORDER, contacts=((CONTACT, True),)):
    cfs = []
    if order_uuid:
        cfs.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": order_uuid}]})
    return {
        "id": LEAD,
        "custom_fields_values": cfs,
        "_embedded": {"contacts": [{"id": cid, "is_main": main} for cid, main in contacts]},
    }


def _contact(cid=CONTACT, nick=None):
    cfs = []
    if nick is not None:
        cfs.append({"field_id": TELEGRAM_CONTACT_FIELD_ID, "values": [{"value": nick}]})
    return {"id": cid, "custom_fields_values": cfs}


def _ms_order(nick="@good_ghost"):
    attrs = [] if nick is None else [{"id": MS_ATTR_COUNTERPARTY_TELEGRAM_ID, "value": nick}]
    return {"id": ORDER, "agent": {"id": "cp-1", "attributes": attrs}}


def _run(monkeypatch, *, lead=None, ms_order=None, contact=None, search=(), patch_ok=True):
    calls = {"patch": [], "ms": [], "lead_reads": 0}

    async def fake_lead(lead_id, with_=()):
        calls["lead_reads"] += 1
        return lead

    async def fake_ms_get(path, params=None):
        calls["ms"].append((path, params))
        return ms_order

    async def fake_contact(contact_id, with_=()):
        return contact

    async def fake_find(query, limit=10):
        return None if search is None else list(search)

    async def fake_patch(contact_id, *, tags=None, custom_fields=None):
        calls["patch"].append((contact_id, custom_fields))
        return {"ok": patch_ok, "status_code": 200 if patch_ok else 400}

    monkeypatch.setattr(T, "TELEGRAM_CONTACT_RETRY_DELAYS_S", [0.0, 0.0])
    monkeypatch.setattr(T.amo_service, "get_lead_full", fake_lead)
    monkeypatch.setattr(T.amo_service, "get_contact_by_id", fake_contact)
    monkeypatch.setattr(T.amo_service, "find_contacts_by_query", fake_find)
    monkeypatch.setattr(T.amo_service, "patch_contact", fake_patch)
    monkeypatch.setattr(T.ms_client, "get", fake_ms_get)
    outcome = asyncio.run(T.apply(LEAD))
    return outcome, calls


@pytest.mark.parametrize("raw, expected", [
    ("@good_ghost", "@good_ghost"),
    ("good_ghost", "@good_ghost"),
    ("https://t.me/good_ghost/", "@good_ghost"),
    ("@abcd", None),
    ("+7 999 123-45-67", None),
    ("@дуров", None),
    ("", None),
    (None, None),
])
def test_normalize(raw, expected):
    assert T.normalize(raw) == expected


def test_written_to_empty_field(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order(), contact=_contact(),
                          search=[_contact()])
    assert outcome == "written"
    assert calls["patch"] == [(CONTACT, {TELEGRAM_CONTACT_FIELD_ID: "@good_ghost"})]
    assert calls["ms"][0] == (f"entity/customerorder/{ORDER}", {"expand": "agent"})


def test_nick_from_counterparty_normalized(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order("t.me/good_ghost"),
                          contact=_contact(), search=[])
    assert outcome == "written"
    assert calls["patch"][0][1] == {TELEGRAM_CONTACT_FIELD_ID: "@good_ghost"}


def test_filled_field_not_touched(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order(),
                          contact=_contact(nick="@wazzup_nick"), search=[])
    assert outcome == "filled"
    assert calls["patch"] == []


def test_nick_taken_by_other_contact(monkeypatch):
    other = _contact(cid=11111, nick="@Good_Ghost")
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order(), contact=_contact(),
                          search=[_contact(), other])
    assert outcome == "taken"
    assert calls["patch"] == []


def test_amo_silent_on_search(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order(), contact=_contact(), search=None)
    assert outcome == "amo_silent"
    assert calls["patch"] == []


def test_no_nick_in_counterparty(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order(nick=None), contact=_contact())
    assert outcome == "no_nick"
    assert calls["patch"] == []


def test_garbage_nick_not_written(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order(nick="+79991234567"), contact=_contact())
    assert outcome == "no_nick"
    assert calls["patch"] == []


def test_lead_without_order_after_retries(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(order_uuid=None), ms_order=_ms_order(), contact=_contact())
    assert outcome == "no_order"
    assert calls["lead_reads"] == 3
    assert calls["ms"] == [] and calls["patch"] == []


def test_ms_silent(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=None, contact=_contact())
    assert outcome == "no_nick"
    assert calls["patch"] == []


def test_main_contact_chosen(monkeypatch):
    lead = _lead(contacts=((222, False), (CONTACT, True)))
    outcome, calls = _run(monkeypatch, lead=lead, ms_order=_ms_order(), contact=_contact(), search=[])
    assert outcome == "written"
    assert calls["patch"][0][0] == CONTACT


def test_patch_failure_reported(monkeypatch):
    outcome, calls = _run(monkeypatch, lead=_lead(), ms_order=_ms_order(), contact=_contact(),
                          search=[], patch_ok=False)
    assert outcome == "error"


def test_disabled_flag_does_nothing(monkeypatch):
    monkeypatch.setattr(T, "TELEGRAM_CONTACT_ENABLED", False)
    T.post_bg(LEAD)  # без event loop и без исключений - просто выход
    assert not T._bg_tasks
