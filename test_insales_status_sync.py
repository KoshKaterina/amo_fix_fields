"""Тесты статуса «Доставлен» в InSales (insales_status_sync, insales_client).

Задача Кати 02.11.2026: сделка Tangemshop закрылась в Офисе как успешная — заказу на
tangemshop.ru ставим статус «Доставлен». Проверяем, что пишем только по нашим сделкам,
только вперёд, один раз, и что клиент InSales ищет заказ перебором (фильтра по номеру
в API нет) и шлёт в PUT ровно одно поле.

Запуск: python3 -m pytest test_insales_status_sync.py -q
"""

import asyncio
import datetime
import json
import os
import sys

import httpx
import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import insales_client  # noqa: E402
import insales_status_sync as sync  # noqa: E402
import migration_freeze  # noqa: E402
from shared.timez import MSK  # noqa: E402
from waybill_config import (  # noqa: E402
    ENUM_SALES_CHANNEL_TANGEMSHOP,
    FIELD_SALES_CHANNEL,
    FIELD_SITE_ORDER_NUMBER,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_OFFICE,
    STATUS_SUCCESS,
)

_CREATED = int(datetime.datetime(2026, 11, 5, 12, 0, tzinfo=MSK).timestamp())
_CLOSED = int(datetime.datetime(2026, 11, 9, 15, 0, tzinfo=MSK).timestamp())


def _lead(number="18858 Tangemshop", name="Заказ №18858 Tangemshop", pipeline=PIPELINE_OFFICE,
          status=STATUS_SUCCESS, channel=True, closed=_CLOSED):
    fields = []
    if number is not None:
        fields.append({"field_id": FIELD_SITE_ORDER_NUMBER, "values": [{"value": number}]})
    if channel:
        fields.append({"field_id": FIELD_SALES_CHANNEL,
                       "values": [{"value": "TangemShop", "enum_id": ENUM_SALES_CHANNEL_TANGEMSHOP}]})
    return {"id": 36600001, "name": name, "pipeline_id": pipeline, "status_id": status,
            "created_at": _CREATED, "closed_at": closed, "custom_fields_values": fields}


def _order(permalink="novyy", system="new", number=18858, oid=1660000001, financial="pending",
           pay="Оплата онлайн"):
    return {"id": oid, "number": number, "fulfillment_status": system, "financial_status": financial,
            "payment_title": pay, "custom_status": {"permalink": permalink}}


class _InSales:
    """Заглушка insales_client: помнит поиски и записи."""

    InSalesError = insales_client.InSalesError

    def __init__(self, order=None, fail_on=None):
        self.order = order
        self.fail_on = fail_on
        self.finds = []
        self.sets = []      # (id, код статуса) — записи статуса
        self.paid = []      # id заказов, отмеченных оплаченными

    async def find_order_by_number(self, number, updated_since):
        self.finds.append((number, updated_since))
        if self.fail_on == "find":
            raise insales_client.InSalesError("InSales GET orders: 403", 403)
        return self.order

    async def update_order(self, order_id, *, permalink=None, mark_paid=False):
        if permalink:
            self.sets.append((order_id, permalink))
        if mark_paid:
            self.paid.append(order_id)
        if self.fail_on == "set":
            raise insales_client.InSalesError("InSales PUT orders/1: 403", 403)
        return {"id": order_id, "custom_status": {"permalink": permalink}}


@pytest.fixture
def env(monkeypatch, tmp_path):
    fake = _InSales(order=_order())
    alerts_sent = []

    async def _no_freeze(lead_id, where, lead=None):
        return False

    async def _alert(text, event=None, values=None):
        alerts_sent.append((event, values))

    monkeypatch.setattr(sync, "insales_client", fake)
    monkeypatch.setattr(migration_freeze, "skip", _no_freeze)
    monkeypatch.setattr(sync, "_alert", _alert)
    monkeypatch.setattr(sync, "INSALES_STATUS_SENT_PATH", str(tmp_path / "sent.json"))
    monkeypatch.setattr(sync, "_enabled", True)
    monkeypatch.setattr(sync, "_sent", {})
    monkeypatch.setattr(sync, "_not_found_alerted", set())
    fake.alerts = alerts_sent
    fake.sent_path = tmp_path / "sent.json"
    return fake


def _run(lead, lead_id=36600001):
    return asyncio.run(sync.process_lead(lead_id, lead=lead))


# ── какие сделки наши ──────────────────────────────────────────────────────────

def test_number_from_field_then_from_name():
    assert sync.order_number(_lead()) == 18858
    # поле затёрто мостом — номер берём из имени сделки
    assert sync.order_number(_lead(number=None)) == 18858
    # номер WooCommerce в поле и обычное имя — не Tangemshop
    assert sync.order_number(_lead(number="19003", name="Заказ №19003")) is None


def test_target_only_office_success_tangemshop_after_guard():
    assert sync.is_target(_lead())
    assert not sync.is_target(_lead(pipeline=PIPELINE_CLEVER_MAIN))
    assert not sync.is_target(_lead(status=143))
    # розничный заказ sunscrypt.ru в том же Офисе/142
    assert not sync.is_target(_lead(number="19003", name="Заказ №19003", channel=False))
    # канал затёрт, но имя Tangemshop — наш
    assert sync.is_target(_lead(number=None, channel=False))
    # закрыта до даты включения — не трогаем
    old = int(datetime.datetime(2026, 10, 1, tzinfo=MSK).timestamp())
    assert not sync.is_target(_lead(closed=old))


# ── запись ────────────────────────────────────────────────────────────────────

def test_sets_delivered_and_remembers(env):
    assert _run(_lead()) == "set"
    assert env.sets == [(1660000001, "dostavlen")]
    number, since = env.finds[0]
    assert number == 18858
    # окно поиска — от даты создания сделки минус три дня, с зоной МСК
    assert since == datetime.datetime(2026, 11, 2, 12, 0, tzinfo=MSK).isoformat()
    assert json.loads(env.sent_path.read_text(encoding="utf-8")) == {"18858": "dostavlen"}


def test_second_pass_is_deduplicated_without_api(env):
    assert _run(_lead()) == "set"
    assert _run(_lead()) == "dedup"
    assert len(env.finds) == 1 and len(env.sets) == 1


def test_already_delivered_is_not_rewritten(env):
    env.order = _order(permalink="dostavlen", system="delivered")
    assert _run(_lead()) == "already"
    assert env.sets == []
    assert sync._sent == {"18858": "dostavlen"}


@pytest.mark.parametrize("permalink,system", [
    ("otmenen", "declined"),
    ("vozvrat", "returned"),
    ("v-protsesse-vozvrata", "returning"),
])
@pytest.mark.parametrize("mode", ["", "prepaid", "all"])
def test_cancelled_and_returns_are_not_touched_at_all(env, monkeypatch, permalink, system, mode):
    monkeypatch.setattr(sync, "INSALES_MARK_PAID", mode)
    env.order = _order(permalink=permalink, system=system)
    assert _run(_lead()) == "hands_off"
    assert env.sets == [] and env.paid == []
    assert sync._sent == {}


def test_human_delivered_status_is_kept(env):
    # «Получена оплата от СД» поставил человек — точнее нашего «Доставлен», статус не пишем
    env.order = _order(permalink="poluchena-oplata-ot-sd", system="delivered")
    assert _run(_lead()) == "already"
    assert env.sets == [] and env.paid == []


# ── отметка оплаты (INSALES_MARK_PAID) ───────────────────────────────────────

@pytest.fixture
def paid_on(env, monkeypatch):
    monkeypatch.setattr(sync, "INSALES_MARK_PAID", "all")
    return env


@pytest.mark.parametrize("pay,marked", [
    ("Оплата онлайн", True),
    ("Оплата картой", True),
    ("Оплата при получении", False),   # наложку рефералка считает по «Доставлен»
])
def test_prepaid_mode_marks_only_prepayment(env, monkeypatch, pay, marked):
    monkeypatch.setattr(sync, "INSALES_MARK_PAID", "prepaid")
    env.order = _order(pay=pay)
    assert _run(_lead()) == "set"
    assert env.sets == [(1660000001, "dostavlen")]
    assert env.paid == ([1660000001] if marked else [])
    assert sync._sent == {"18858": "dostavlen+paid"}


def test_mark_paid_goes_in_the_same_put_as_status(paid_on):
    assert _run(_lead()) == "set"
    assert paid_on.sets == [(1660000001, "dostavlen")]
    assert paid_on.paid == [1660000001]
    assert sync._sent == {"18858": "dostavlen+paid"}


def test_delivered_but_unpaid_gets_only_payment(paid_on):
    # Заказ уже «Доставлен» (догон или руками), а предоплата прошла мимо InSales
    paid_on.order = _order(permalink="dostavlen", system="delivered")
    assert _run(_lead()) == "set"
    assert paid_on.sets == [] and paid_on.paid == [1660000001]


def test_human_delivered_status_kept_but_payment_marked(paid_on):
    paid_on.order = _order(permalink="poluchena-oplata-ot-sd", system="delivered")
    assert _run(_lead()) == "set"
    assert paid_on.sets == [] and paid_on.paid == [1660000001]


def test_already_paid_is_not_marked_again(paid_on):
    paid_on.order = _order(financial="paid")
    assert _run(_lead()) == "set"
    assert paid_on.sets == [(1660000001, "dostavlen")] and paid_on.paid == []


def test_delivered_and_paid_needs_nothing(paid_on):
    paid_on.order = _order(permalink="dostavlen", system="delivered", financial="paid")
    assert _run(_lead()) == "already"
    assert paid_on.sets == [] and paid_on.paid == []
    assert sync._sent == {"18858": "dostavlen+paid"}


def test_orders_done_before_the_flag_are_revisited_once(paid_on, monkeypatch):
    # До включения флага в дедупе лежит «dostavlen» — сверка один раз дойдёт до оплаты
    monkeypatch.setattr(sync, "_sent", {"18858": "dostavlen"})
    paid_on.order = _order(permalink="dostavlen", system="delivered")
    assert _run(_lead()) == "set"
    assert paid_on.paid == [1660000001]
    assert _run(_lead()) == "dedup"
    assert len(paid_on.finds) == 1


def test_not_found_alerts_once_and_retries_later(env):
    env.order = None
    assert _run(_lead()) == "not_found"
    assert _run(_lead()) == "not_found"
    assert len(env.finds) == 2          # следующий проход ищет снова
    assert len(env.alerts) == 1         # но в чат — один раз
    assert env.alerts[0][0] == "insales_status_sync_error"


def test_api_error_alerts_and_is_not_remembered(env):
    env.fail_on = "set"
    assert _run(_lead()) == "error"
    assert env.alerts and env.alerts[0][1]["номер"] == "18858"
    assert sync._sent == {}


def test_not_ours_does_not_touch_insales(env):
    assert _run(_lead(number="19003", name="Заказ №19003", channel=False)) == "not_ours"
    assert env.finds == [] and env.sets == []


def test_disabled_does_nothing(env, monkeypatch):
    monkeypatch.setattr(sync, "_enabled", False)
    assert _run(_lead()) == "disabled"
    assert env.finds == []


def test_lead_is_reread_from_api_not_taken_from_webhook(env, monkeypatch):
    reads = []

    async def _get(lead_id, with_=()):
        reads.append(lead_id)
        return _lead(status=75426882)   # по факту сделка «В пути», а не в УР

    monkeypatch.setattr(sync.amo_service, "get_lead_full", _get)
    assert asyncio.run(sync.process_lead(36600001)) == "not_ours"
    assert reads == [36600001] and env.finds == []


# ── вход из вебхука ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("pipeline,status,changed,expected", [
    (PIPELINE_OFFICE, STATUS_SUCCESS, True, True),
    (None, STATUS_SUCCESS, True, True),             # часть событий amo без воронки
    (PIPELINE_OFFICE, STATUS_SUCCESS, False, False),  # правка давно закрытой сделки
    (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS, True, False),
    (PIPELINE_OFFICE, 75426882, True, False),
])
def test_webhook_gate(env, monkeypatch, pipeline, status, changed, expected):
    calls = []

    async def _process(lead_id, lead=None, source="reconcile"):
        calls.append((lead_id, source))
        return "set"

    monkeypatch.setattr(sync, "process_lead", _process)

    async def _go():
        sync.on_lead_change(36600001, pipeline, status, changed)
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    asyncio.run(_go())
    assert calls == ([(36600001, "webhook")] if expected else [])


# ── клиент InSales ────────────────────────────────────────────────────────────

def _client(handler):
    return insales_client.InSalesClient("shop.example", "key", "pass",
                                        transport=httpx.MockTransport(handler))


def test_client_pages_with_cursor_until_number_found():
    seen = []
    page1 = [{"id": 1000 + i, "number": 18000 + i, "updated_at": f"2026-11-02T10:{i % 60:02d}:00.000+03:00"}
             for i in range(100)]
    page2 = [{"id": 2000, "number": 18858, "updated_at": "2026-11-03T09:00:00.000+03:00"}]

    def handler(request):
        seen.append(dict(request.url.params))
        assert request.url.path == "/admin/orders.json"
        return httpx.Response(200, json=page1 if len(seen) == 1 else page2)

    async def _go():
        c = _client(handler)
        await c.init()
        try:
            return await c.find_order_by_number(18858, "2026-11-02T00:00:00+03:00")
        finally:
            await c.aclose()

    order = asyncio.run(_go())
    assert order["id"] == 2000
    assert seen[0] == {"per_page": "100", "updated_since": "2026-11-02T00:00:00+03:00"}
    # вторая страница — курсором от последнего заказа первой
    assert seen[1]["updated_since"] == page1[-1]["updated_at"]
    assert seen[1]["from_id"] == str(page1[-1]["id"])


def test_client_short_page_without_number_is_not_found():
    def handler(request):
        return httpx.Response(200, json=[{"id": 1, "number": 1, "updated_at": "x"}])

    async def _go():
        c = _client(handler)
        await c.init()
        try:
            return await c.find_order_by_number(18858, "2026-11-02T00:00:00+03:00")
        finally:
            await c.aclose()

    assert asyncio.run(_go()) is None


def test_client_put_sends_only_custom_status():
    bodies = []

    def handler(request):
        assert request.method == "PUT" and request.url.path == "/admin/orders/1660000001.json"
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"id": 1660000001, "custom_status": {"permalink": "dostavlen"}})

    async def _go():
        c = _client(handler)
        await c.init()
        try:
            return await c.set_custom_status(1660000001, "dostavlen")
        finally:
            await c.aclose()

    asyncio.run(_go())
    assert bodies == [{"order": {"custom_status_permalink": "dostavlen"}}]


@pytest.mark.parametrize("response", [
    httpx.Response(403, text="Недостаточно прав для операции"),
    httpx.Response(200, json={"id": 1, "custom_status": {"permalink": "novyy"}}),
])
def test_client_put_failure_raises(response):
    async def _go():
        c = _client(lambda request: response)
        await c.init()
        try:
            await c.set_custom_status(1, "dostavlen")
        finally:
            await c.aclose()

    with pytest.raises(insales_client.InSalesError):
        asyncio.run(_go())


@pytest.mark.parametrize("permalink,expected", [
    ("dostavlen", {"order": {"custom_status_permalink": "dostavlen", "financial_status": "paid"}}),
    (None, {"order": {"financial_status": "paid"}}),
])
def test_client_mark_paid_body(permalink, expected):
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"id": 1, "financial_status": "paid",
                                         "custom_status": {"permalink": permalink or "dostavlen"}})

    async def _go():
        c = _client(handler)
        await c.init()
        try:
            return await c.update_order(1, permalink=permalink, mark_paid=True)
        finally:
            await c.aclose()

    asyncio.run(_go())
    assert bodies == [expected]


def test_client_mark_paid_not_applied_raises():
    response = httpx.Response(200, json={"id": 1, "financial_status": "pending",
                                         "custom_status": {"permalink": "dostavlen"}})

    async def _go():
        c = _client(lambda request: response)
        await c.init()
        try:
            await c.update_order(1, mark_paid=True)
        finally:
            await c.aclose()

    with pytest.raises(insales_client.InSalesError):
        asyncio.run(_go())
