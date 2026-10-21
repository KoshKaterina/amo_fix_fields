"""Клиенты интеграции GetCourse: как ответы amo и GetCourse превращаются в «повторить»,
«исход неизвестен» и «отказ». От этого зависит, создаст ли повтор вторую сделку."""

import asyncio
import base64
import json

import httpx
import pytest

import getcourse_amo_client as amo
import getcourse_api_client as gc


@pytest.fixture(autouse=True)
def _fast(monkeypatch):
    monkeypatch.setattr(amo, "MIN_INTERVAL_S", 0)
    monkeypatch.setattr(amo, "READ_BACKOFF_S", (0, 0))
    monkeypatch.setattr(amo, "_lock", None)
    monkeypatch.setattr(amo, "GETCOURSE_AMO_TOKEN", "t")
    monkeypatch.setattr(gc, "GETCOURSE_API_KEY", "k")
    yield
    amo._transport = None
    gc._transport = None


def run(coro):
    return asyncio.run(coro)


def transport(handler):
    return httpx.MockTransport(handler)


def test_get_404_and_204_mean_nothing():
    amo._transport = transport(lambda r: httpx.Response(404 if "contacts/1" in str(r.url) else 204))
    assert run(amo.get_contact(1)) is None
    assert run(amo.search_contacts("x")) == []


def test_get_retries_then_unavailable():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(503)

    amo._transport = transport(handler)
    with pytest.raises(amo.AmoUnavailable):
        run(amo.get_lead(5))
    assert len(calls) == amo.READ_ATTEMPTS


def test_get_4xx_is_refusal():
    amo._transport = transport(lambda r: httpx.Response(401, text="no"))
    with pytest.raises(amo.AmoError) as exc:
        run(amo.get_lead(5))
    assert exc.value.status == 401


def test_write_429_is_safe_to_retry():
    amo._transport = transport(lambda r: httpx.Response(429))
    with pytest.raises(amo.AmoUnavailable):
        run(amo.add_note(1, "x"))


@pytest.mark.parametrize("response", [
    lambda r: httpx.Response(502),
    lambda r: (_ for _ in ()).throw(httpx.ReadTimeout("slow", request=r)),
    lambda r: httpx.Response(200, json=[{}]),
])
def test_create_without_proof_is_uncertain(response):
    amo._transport = transport(response)
    with pytest.raises(amo.AmoUncertain):
        run(amo.create_lead_complex({"name": "x"}))


def test_connect_error_on_write_is_safe_to_retry():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    amo._transport = transport(handler)
    with pytest.raises(amo.AmoUnavailable):
        run(amo.create_lead_complex({"name": "x"}))


def test_create_returns_ids_and_sends_token():
    seen = {}

    def handler(request):
        seen["auth"] = request.headers["Authorization"]
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=[{"id": 11, "contact_id": 22, "request_id": ["0"]}])

    amo._transport = transport(handler)
    assert run(amo.create_lead_complex({"name": "x"})) == (11, 22)
    assert seen["auth"] == "Bearer t" and seen["body"] == [{"name": "x"}]


def test_get_leads_batches_ids():
    urls = []

    def handler(request):
        urls.append(request.url)
        ids = request.url.params.get_list("filter[id][]")
        return httpx.Response(200, json={"_embedded": {"leads": [{"id": int(i)} for i in ids]}})

    amo._transport = transport(handler)
    leads = run(amo.get_leads(list(range(1, 121))))
    assert len(leads) == 120 and len(urls) == 3
    assert run(amo.get_leads([])) == [] and len(urls) == 3


def test_gc_set_deal_field_sends_base64_params():
    seen = {}

    def handler(request):
        form = dict(httpx.QueryParams(request.content.decode()))
        seen.update(form)
        return httpx.Response(200, json={"success": True, "result": {"success": True}})

    gc._transport = transport(handler)
    assert run(gc.set_deal_field("20619", "a@b.ru", "ИД сделки amo", "36584201")) is True
    params = json.loads(base64.b64decode(seen["params"]))
    assert seen["action"] == "add" and seen["key"] == "k"
    assert params == {"user": {"email": "a@b.ru"}, "system": {"refresh_if_exists": 0},
                      "deal": {"deal_number": "20619", "addfields": {"ИД сделки amo": "36584201"}}}


def test_gc_export_busy_is_not_an_error():
    gc._transport = transport(lambda r: httpx.Response(
        200, json={"success": False, "error_message": "Уже запущен один экспорт", "info": None}))
    assert run(gc.start_deals_export("2026-10-21")) is None


def test_gc_export_not_ready_then_ready():
    answers = iter([
        {"success": False, "error_message": "Файл еще не создан"},
        {"success": True, "info": {"fields": ["ID заказа"], "items": [["1"]]}},
    ])
    gc._transport = transport(lambda r: httpx.Response(200, json=next(answers)))
    assert run(gc.fetch_export(7)) is None
    assert run(gc.fetch_export(7)) == (["ID заказа"], [["1"]])
