"""Тесты примечания с контактами из заказа (order_note).

Написаны по разбору сделки 36543929 / заказа №18712 (30.08.2026): email из
заказа доезжает в МойСклад, но amgroup затирает его при склейке с существующим
контактом amo. Проверяем, что примечание собирается из ЗАКАЗА, ставится один
раз, ждёт появления поля с номером и молчит там, где писать нечего.

Запуск: python3 -m pytest test_order_note.py -q
"""

import asyncio
import os
import sys
import types

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")
os.environ["ORDER_NOTE_ENABLED"] = "1"
os.environ["ORDER_NOTE_RETRY_DELAYS_S"] = "0,0,0"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import order_note  # noqa: E402
from waybill_config import FIELD_SITE_ORDER_NUMBER  # noqa: E402


def _lead(order_number=None):
    fields = []
    if order_number is not None:
        fields.append({"field_id": FIELD_SITE_ORDER_NUMBER,
                       "values": [{"value": order_number}]})
    return {"id": 36543929, "custom_fields_values": fields}


def _order(first="Сергей", last="Малько", email="seregeimalko@yandex.ru",
           phone="+79064975489"):
    return {"id": 18712, "billing": {
        "first_name": first, "last_name": last, "email": email, "phone": phone}}


class _Amo:
    """Заглушка amo_service: помнит, что у неё спрашивали и что записали."""

    def __init__(self, leads, notes=None):
        self._leads = list(leads)          # ответы get_lead_full по порядку
        self.notes = list(notes or [])     # уже стоящие примечания
        self.added = []                    # тексты, которые мы записали
        self.lead_reads = 0

    async def get_lead_full(self, lead_id, with_=()):
        self.lead_reads += 1
        if not self._leads:
            return None
        return self._leads.pop(0) if len(self._leads) > 1 else self._leads[0]

    async def get_lead_notes(self, lead_id, limit=100):
        return [{"params": {"text": t}} for t in self.notes]

    async def add_note(self, lead_id, text):
        self.added.append(text)
        return {"ok": True, "status_code": 200}

    @staticmethod
    def get_custom_field_value(entity, field_id):
        for f in entity.get("custom_fields_values") or []:
            if f.get("field_id") == field_id:
                values = f.get("values") or []
                if values:
                    return values[0].get("value")
        return None


class _Woo:
    def __init__(self, order):
        self._order = order
        self.asked = []

    async def get_order(self, order_id):
        self.asked.append(str(order_id))
        return self._order


@pytest.fixture
def wired(monkeypatch):
    """Подменяет amo_service и woo_client внутри order_note."""
    def _wire(leads, order, notes=None):
        amo, woo = _Amo(leads, notes), _Woo(order)
        monkeypatch.setattr(order_note, "amo_service", amo)
        monkeypatch.setattr(order_note, "woo_client", woo)
        return amo, woo
    return _wire


def test_note_text_matches_order():
    text = order_note.build_note("18712", _order())
    assert text == (
        "Данные из заказа №18712 (с сайта, до склейки контактов):\n"
        "Имя: Сергей Малько\n"
        "Email: seregeimalko@yandex.ru\n"
        "Телефон: +79064975489"
    )


def test_note_skips_empty_fields():
    text = order_note.build_note("18712", _order(last="", phone=""))
    assert "Телефон" not in text and "Имя: Сергей" in text


def test_no_note_when_order_has_no_contacts():
    assert order_note.build_note("18712", _order("", "", "", "")) is None


def test_posts_note_from_order(wired):
    amo, woo = wired([_lead("18712")], _order())
    asyncio.run(order_note._apply(36543929))
    assert woo.asked == ["18712"]
    assert len(amo.added) == 1
    assert "seregeimalko@yandex.ru" in amo.added[0]


def test_waits_until_amgroup_fills_the_field(wired):
    """Первые дочитывания — поле ещё пустое (amgroup не успел), потом появилось."""
    amo, woo = wired([_lead(None), _lead(None), _lead("18712")], _order())
    asyncio.run(order_note._apply(36543929))
    assert amo.lead_reads == 3
    assert len(amo.added) == 1


def test_silent_when_lead_is_not_from_site(wired):
    """Сделка со звонка/чата: номера заказа нет — ни запроса в WC, ни примечания."""
    amo, woo = wired([_lead(None)], _order())
    asyncio.run(order_note._apply(36543929))
    assert woo.asked == [] and amo.added == []


def test_does_not_duplicate_existing_note(wired):
    amo, woo = wired([_lead("18712")], _order(),
                     notes=["Данные из заказа №18712 (с сайта, до склейки контактов):\n..."])
    asyncio.run(order_note._apply(36543929))
    assert amo.added == [] and woo.asked == []


def test_insales_number_is_skipped(wired):
    """Номер с суффиксом « Tangemshop» — заказа с таким id в WooCommerce нет."""
    amo, woo = wired([_lead("12345 Tangemshop")], _order())
    asyncio.run(order_note._apply(36543929))
    assert woo.asked == [] and amo.added == []


def test_missing_order_in_wc_is_not_fatal(wired):
    amo, woo = wired([_lead("18712")], None)
    asyncio.run(order_note._apply(36543929))
    assert amo.added == []


def test_errors_do_not_escape(wired, monkeypatch):
    """Сбой amo не должен ронять фоновую задачу вебхука."""
    amo, woo = wired([_lead("18712")], _order())

    async def boom(*a, **kw):
        raise RuntimeError("amo упал")

    monkeypatch.setattr(amo, "add_note", boom)
    asyncio.run(order_note._apply(36543929))  # не бросает


def test_flag_off_does_nothing(monkeypatch):
    monkeypatch.setattr(order_note, "ORDER_NOTE_ENABLED", False)
    spawned = []
    monkeypatch.setattr(order_note.asyncio, "create_task", lambda c: spawned.append(c))
    order_note.post_bg(36543929)
    assert spawned == []
