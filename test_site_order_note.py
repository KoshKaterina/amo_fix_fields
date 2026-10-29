"""Тесты заметки в сделку с данными клиента с сайта (site_order_note).

Повод: контакт amo назван «Новый контакт по звонку …», а клиент ввёл на сайте настоящее имя.
Развилки: имя расходится / совпадает / контакта нет, заказ не с сайта, повтор не пишет второй
заметки, сухой режим ничего не пишет, а сбой МС или amo читается как «не знаю» (повторим).

Подмены - только через monkeypatch на объектах, которые держит код под тестом.

Запуск: python run_tests.py test_site_order_note.py
"""

import asyncio
import os
import sys
import time

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import site_order_note as son  # noqa: E402

ORDER = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
ATTR = "11111111-2222-3333-4444-555555555555"
LEAD_ID = 36590001
SITE_TEXT = "Имя: Ирина Фролова\nТелефон: +7 (916) 123-45-67\nПочта: irina@example.com"


def _lead(*, contacts=(7001,), age_sec=300, created_by_enum=son.ENUM_CREATED_BY_MS, order=ORDER, status_id=1):
    cf = [{"field_id": son.FIELD_ORDER_NUMBER, "values": [{"value": "09150"}]}]
    if created_by_enum is not None:
        cf.append({"field_id": son.FIELD_CREATED_BY, "values": [{"value": "Из МойСклад", "enum_id": created_by_enum}]})
    if order is not None:
        cf.append({"field_id": son.FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": order}]})
    return {
        "id": LEAD_ID,
        "status_id": status_id,
        "created_at": int(time.time() - age_sec),
        "custom_fields_values": cf,
        "_embedded": {"contacts": [{"id": c, "is_main": True} for c in contacts]},
    }


class World:
    def __init__(self, monkeypatch, *, lead=None, order="default", contact_name="Новый контакт по звонку с +79161234567 из UIS",
                 notes=(), add_result=None):
        self.lead = lead if lead is not None else _lead()
        self.order = ({"attributes": [{"id": ATTR, "value": SITE_TEXT}]} if order == "default" else order)
        self.contact_name = contact_name
        self.notes_in_amo = list(notes)
        self.add_result = add_result or {"ok": True, "status_code": 200, "retryable": False}
        self.written: list[str] = []
        self.ms_calls: list[str] = []

        async def get_lead_full(lead_id, with_=()):
            return self.lead

        async def get_contact_by_id(contact_id, with_=()):
            return {"id": contact_id, "name": self.contact_name} if self.contact_name is not None else None

        async def get_lead_notes(lead_id, limit=100):
            return self.notes_in_amo

        async def add_note(lead_id, text):
            self.written.append(text)
            return self.add_result

        async def ms_get(path, params=None):
            self.ms_calls.append(path)
            return self.order

        monkeypatch.setattr(son.amo_service, "get_lead_full", get_lead_full)
        monkeypatch.setattr(son.amo_service, "get_contact_by_id", get_contact_by_id)
        monkeypatch.setattr(son.amo_service, "get_lead_notes", get_lead_notes)
        monkeypatch.setattr(son.amo_service, "add_note", add_note)
        monkeypatch.setattr(son.ms_client, "get", ms_get)

    def run(self, source="scan"):
        return asyncio.run(son.note_lead(LEAD_ID, source=source))


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_ENABLED", True)
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_DRY_RUN", False)
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_MS_ATTR", ATTR)
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_MIN_AGE_SEC", 90)
    monkeypatch.setattr(son, "_STATE_PATH", str(tmp_path / "note.json"))
    monkeypatch.setattr(son, "_state_loaded", False)
    monkeypatch.setattr(son, "_PAUSE_SEC", 0)
    son._state.clear()
    son._dry_seen.clear()
    son._in_flight.clear()


def test_different_name_writes_note_with_warning(monkeypatch):
    w = World(monkeypatch)
    assert w.run() == son.WRITTEN
    assert len(w.written) == 1
    note = w.written[0]
    assert note.startswith("Данные клиента с сайта (заказ МС 09150):\nИмя: Ирина Фролова")
    assert "Имя в карточке контакта: Новый контакт по звонку" in note
    assert "отличается" in note


def test_same_name_says_so(monkeypatch):
    w = World(monkeypatch, contact_name="ирина  фролова")
    assert w.run() == son.WRITTEN
    assert "совпадает с сайтом" in w.written[0]
    assert "отличается" not in w.written[0]


def test_lead_without_contact_still_gets_note(monkeypatch):
    w = World(monkeypatch, lead=_lead(contacts=()))
    assert w.run() == son.WRITTEN
    assert "не привязан" in w.written[0]


def test_second_call_does_not_write_again(monkeypatch):
    w = World(monkeypatch)
    assert w.run() == son.WRITTEN
    assert w.run() == son.ALREADY_HANDLED
    assert len(w.written) == 1


def test_state_lost_but_note_already_in_lead(monkeypatch):
    w = World(monkeypatch, notes=[{"params": {"text": "Данные клиента с сайта (заказ МС 09150):\n..."}}])
    assert w.run() == son.ALREADY_NOTED
    assert w.written == []


def test_order_without_site_field_is_final_no_data(monkeypatch):
    w = World(monkeypatch, order={"attributes": [{"id": "other", "value": "x"}]})
    assert w.run() == son.NO_DATA
    assert w.written == []
    assert w.run() == son.ALREADY_HANDLED


def test_lead_not_from_ms_is_ignored(monkeypatch):
    w = World(monkeypatch, lead=_lead(created_by_enum=1040193))
    assert w.run() == son.NOT_MS_LEAD
    assert w.ms_calls == []


def test_lead_without_order_id_is_ignored(monkeypatch):
    w = World(monkeypatch, lead=_lead(order=None))
    assert w.run() == son.NOT_MS_LEAD


def test_young_lead_waits_for_amgroup(monkeypatch):
    w = World(monkeypatch, lead=_lead(age_sec=20))
    assert w.run() == son.TOO_YOUNG
    assert w.ms_calls == []


def test_closed_lead_is_skipped(monkeypatch):
    w = World(monkeypatch, lead=_lead(status_id=son.STATUS_SUCCESS))
    assert w.run() == son.CLOSED


def test_ms_silence_is_unknown_and_retried(monkeypatch):
    w = World(monkeypatch, order=None)
    assert w.run() == son.UNKNOWN_MS
    assert w.written == []
    w.order = {"attributes": [{"id": ATTR, "value": SITE_TEXT}]}
    assert w.run() == son.WRITTEN


def test_amo_lead_read_failure_is_unknown(monkeypatch):
    w = World(monkeypatch)
    w.lead = None
    monkeypatch.setattr(son.amo_service, "get_lead_full", lambda *a, **k: asyncio.sleep(0, result=None))
    assert w.run() == son.UNKNOWN_LEAD


def test_contact_read_failure_is_unknown(monkeypatch):
    w = World(monkeypatch, contact_name=None)
    assert w.run() == son.UNKNOWN_LEAD
    assert w.written == []


def test_failed_write_is_retried(monkeypatch):
    w = World(monkeypatch, add_result={"ok": False, "status_code": 500, "retryable": True})
    assert w.run() == son.UNKNOWN_WRITE
    w.add_result = {"ok": True, "status_code": 200, "retryable": False}
    assert w.run() == son.WRITTEN
    assert len(w.written) == 2


def test_dry_run_reads_everything_but_writes_nothing(monkeypatch):
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_DRY_RUN", True)
    w = World(monkeypatch)
    assert w.run() == son.DRY_RUN_WOULD_WRITE
    assert w.written == []
    assert not os.path.exists(son._STATE_PATH)
    assert w.run() == son.ALREADY_HANDLED


def test_disabled_and_missing_attr_do_nothing(monkeypatch):
    w = World(monkeypatch)
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_ENABLED", False)
    assert w.run() == son.DISABLED
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_ENABLED", True)
    monkeypatch.setattr(son, "SITE_ORDER_NOTE_MS_ATTR", "")
    assert w.run() == son.DISABLED
    assert w.written == [] and w.ms_calls == []


def test_check_once_amo_silence_skips_pass(monkeypatch):
    async def none(*a, **k):
        return None
    monkeypatch.setattr(son.amo_service, "get_leads_created_between", none)
    assert asyncio.run(son.check_once()) == {"amo_answered": False, "candidates": 0, "outcomes": {}}


def test_check_once_takes_only_ms_leads(monkeypatch):
    w = World(monkeypatch)
    other = _lead(created_by_enum=1040193)
    other["id"] = LEAD_ID + 1

    async def leads(*a, **k):
        return [w.lead, other]
    monkeypatch.setattr(son.amo_service, "get_leads_created_between", leads)
    result = asyncio.run(son.check_once())
    assert result["candidates"] == 1
    assert result["outcomes"] == {son.WRITTEN: 1}
