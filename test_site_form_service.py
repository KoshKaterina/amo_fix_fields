"""Тесты приёма форм сайта (site_form_service): карта форм, секрет, rate limit,
антидубль и путь заявки unsorted → accept с источником формы. Без сети — api.*
замокан."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
import time

import pytest

import api
import site_form_service as sf
import site_form_store as store


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    sf._rate.clear()
    sf._client_rate.clear()
    sf._seen.clear()
    monkeypatch.setattr(sf, "FORM_MAP", {
        "svyazatsya": {
            "source": "ContactForm_Связаться",
            "pipeline_id": 111,
            "status_id": 222,
            "tags": ["Форма сайта"],
        },
        "unsorted-only": {
            "source": "ContactForm_Академия",
            "pipeline_id": 111,
            "status_id": None,
            "tags": [],
        },
    })
    monkeypatch.setattr(sf, "SITE_FORM_SECRET", "s3cret")
    monkeypatch.setattr(sf, "SITE_FORM_ENABLED", True)
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", False)
    yield


# --- карта из env ---------------------------------------------------------

def test_load_map_valid(monkeypatch):
    monkeypatch.setenv("SITE_FORM_MAP", '{"a": {"source": "ContactForm_X", "pipeline_id": 5, "tags": ["T"]}}')
    m = sf._load_map()
    assert m["a"]["source"] == "ContactForm_X"
    assert m["a"]["pipeline_id"] == 5
    assert m["a"]["status_id"] is None
    assert m["a"]["tags"] == ["T"]


def test_load_map_broken_json(monkeypatch):
    monkeypatch.setenv("SITE_FORM_MAP", "{оборвано")
    assert sf._load_map() == {}


def test_load_map_skips_bad_entries(monkeypatch):
    monkeypatch.setenv(
        "SITE_FORM_MAP",
        '{"ok": {"source": "S", "pipeline_id": 1}, "no-pipe": {"source": "S"}, "no-src": {"pipeline_id": 2}}',
    )
    m = sf._load_map()
    assert set(m) == {"ok"}


def test_load_map_keeps_explicit_unavailable_type(monkeypatch):
    monkeypatch.setenv(
        "SITE_FORM_MAP",
        '{"out-of-stock": {"source": "Форма: нет в наличии", "pipeline_id": 1, '
        '"form_type": "unavailable"}, "wrong": {"source": "S", "pipeline_id": 1, '
        '"form_type": "preorder"}, "broken": {"source": "S", "pipeline_id": 1, '
        '"form_type": []}}',
    )
    m = sf._load_map()
    assert set(m) == {"out-of-stock"}
    assert m["out-of-stock"]["form_type"] == "unavailable"


# --- секрет и rate limit --------------------------------------------------

def test_secret_ok():
    assert sf.secret_ok("s3cret")
    assert not sf.secret_ok("wrong")
    assert not sf.secret_ok("")


def test_secret_empty_env(monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_SECRET", "")
    assert not sf.secret_ok("anything")


def test_rate_limit(monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_RATE_PER_MINUTE", 2)
    assert sf.allow_ip("1.2.3.4")
    assert sf.allow_ip("1.2.3.4")
    assert not sf.allow_ip("1.2.3.4")
    # другой IP — своя корзина
    assert sf.allow_ip("5.6.7.8")


def test_normalize_phone():
    assert sf._normalize_phone("8 (999) 000-00-00") == "+79990000000"
    assert sf._normalize_phone("+7 999 000 00 00") == "+79990000000"
    assert sf._normalize_phone("нет цифр") == ""


def test_fallback_phone_and_email_nonstandard_keys(monkeypatch):
    calls = _mock_api(monkeypatch)
    payload = {"form": "svyazatsya", "fields": {
        "imya-klienta-905": "Пётр",
        "tel-905": "8 (999) 000-00-00",
    }}
    assert asyncio.run(sf.process(payload)) == 201
    assert calls["create"]["contact"]["custom_fields_values"][0]["values"][0]["value"] == "+79990000000"

    sf._seen.clear()
    calls2 = _mock_api(monkeypatch)
    payload = {"form": "svyazatsya", "fields": {"pochta-123": "x@y.ru", "msg": "вопрос"}}
    assert asyncio.run(sf.process(payload)) == 201
    assert calls2["create"]["contact"]["custom_fields_values"][0]["field_code"] == "EMAIL"


def test_fallback_ignores_short_numbers(monkeypatch):
    calls = _mock_api(monkeypatch)
    payload = {"form": "svyazatsya", "fields": {"kolichestvo": "3", "msg": "хочу 25 штук"}}
    assert asyncio.run(sf.process(payload)) is None
    assert "create" not in calls


# --- process --------------------------------------------------------------

def _mock_api(monkeypatch, contact_id=None, lead_id=101, uid="u-1", accepted=201):
    calls = {}

    async def find_contact_id(query):
        calls.setdefault("find", []).append(query)
        return contact_id

    async def create_unsorted_lead_ex(**kwargs):
        calls["create"] = kwargs
        return {"lead_id": lead_id, "contact_id": 55, "uid": uid}

    async def accept_unsorted(u, status_id, user_id=None):
        calls["accept"] = (u, status_id)
        return accepted

    async def add_note_to_lead(lid, text, *, max_attempts=None):
        calls["note"] = (lid, text)
        return True

    async def set_lead_tags(lid, tags):
        calls["tags"] = (lid, tags)
        return True

    monkeypatch.setattr(api, "find_contact_id", find_contact_id)
    monkeypatch.setattr(api, "create_unsorted_lead_ex", create_unsorted_lead_ex)
    monkeypatch.setattr(api, "accept_unsorted", accept_unsorted)
    monkeypatch.setattr(api, "add_note_to_lead", add_note_to_lead)
    monkeypatch.setattr(api, "set_lead_tags", set_lead_tags)
    return calls


def _payload(slug="svyazatsya", **fields):
    base = {"your-name": "Иван", "your-tel": "8 999 000-00-00"}
    base.update(fields)
    return {"form": slug, "page_url": "https://sunscrypt.ru/contact/", "fields": base}


def test_process_full_path(monkeypatch):
    calls = _mock_api(monkeypatch)
    lead = asyncio.run(sf.process(_payload(), ip="9.9.9.9"))
    # accept вернул 201 — итоговая сделка принятая
    assert lead == 201
    c = calls["create"]
    assert c["source_name"] == "ContactForm_Связаться"
    assert c["pipeline_id"] == 111
    assert c["source_uid"] == "site_form_svyazatsya"
    assert c["form_id"] == "svyazatsya"
    # имя источника всегда идёт первым тегом (нативный источник закрыт без виджета)
    assert c["lead_tags"] == ["ContactForm_Связаться", "Форма сайта"]
    assert c["contact"]["custom_fields_values"][0]["values"][0]["value"] == "+79990000000"
    assert calls["accept"] == ("u-1", 222)
    # теги дожимаются PATCH-ем на ПРИНЯТУЮ сделку: источник первым, потом из карты
    assert calls["tags"] == (201, ["ContactForm_Связаться", "Форма сайта"])
    note_lead, note_text = calls["note"]
    assert note_lead == 201
    assert "Иван" in note_text and "svyazatsya" in note_text


def test_process_without_accept(monkeypatch):
    calls = _mock_api(monkeypatch)
    lead = asyncio.run(sf.process(_payload(slug="unsorted-only")))
    assert lead == 101
    assert "accept" not in calls


def test_process_existing_contact(monkeypatch):
    calls = _mock_api(monkeypatch, contact_id=777)
    asyncio.run(sf.process(_payload()))
    assert calls["create"]["contact"] == {"id": 777}


def test_process_unknown_form(monkeypatch):
    calls = _mock_api(monkeypatch)
    assert asyncio.run(sf.process(_payload(slug="left-form"))) is None
    assert "create" not in calls


def test_process_no_phone_no_email(monkeypatch):
    calls = _mock_api(monkeypatch)
    payload = {"form": "svyazatsya", "fields": {"your-name": "Аноним"}}
    assert asyncio.run(sf.process(payload)) is None
    assert "create" not in calls


def test_process_email_only(monkeypatch):
    calls = _mock_api(monkeypatch)
    payload = {"form": "svyazatsya", "fields": {"your-email": "a@b.ru"}}
    assert asyncio.run(sf.process(payload)) == 201
    assert calls["create"]["contact"]["custom_fields_values"][0]["field_code"] == "EMAIL"


def test_process_duplicate_dropped(monkeypatch):
    calls = _mock_api(monkeypatch)
    assert asyncio.run(sf.process(_payload())) == 201
    calls.pop("create")
    # тот же телефон в ту же форму сразу же — антидубль
    assert asyncio.run(sf.process(_payload())) is None
    assert "create" not in calls


def test_process_accept_failed_keeps_unsorted_lead(monkeypatch):
    calls = _mock_api(monkeypatch, accepted=None)
    lead = asyncio.run(sf.process(_payload()))
    assert lead == 101  # заявка осталась, примечание на неё
    assert calls["note"][0] == 101


# --- слой api: unsorted_ex, обёртка для Jivo, accept ------------------------

def test_api_unsorted_ex_and_jivo_wrapper(monkeypatch):
    captured = {}

    async def fake_request_json(method, url, body=None, what=""):
        captured["url"] = url
        captured["body"] = body
        return {"_embedded": {"unsorted": [
            {"uid": "U1", "_embedded": {"leads": [{"id": 7}], "contacts": [{"id": 8}]}}
        ]}}

    monkeypatch.setattr(api, "_request_json", fake_request_json)
    res = asyncio.run(api.create_unsorted_lead_ex(
        lead_name="N", pipeline_id=1, contact={"id": 3}, source_uid="sf",
        page_url="", created_ts=10, source_name="S", form_id="f",
        lead_tags=["T"], ip="1.1.1.1",
    ))
    assert res == {"lead_id": 7, "contact_id": 8, "uid": "U1"}
    form = captured["body"][0]
    assert captured["url"].endswith("/api/v4/leads/unsorted/forms")
    assert form["source_name"] == "S"
    assert form["source_uid"] == "sf"
    assert form["metadata"]["form_id"] == "f"
    assert form["metadata"]["ip"] == "1.1.1.1"
    assert form["_embedded"]["leads"][0]["_embedded"]["tags"] == [{"name": "T"}]

    # обёртка со старой сигнатурой (Jivo-мост): пара, метаданные jivo_chat, без тегов
    pair = asyncio.run(api.create_unsorted_lead(
        lead_name="N", pipeline_id=1, contact={"id": 3}, source_uid="sf",
        page_url="", created_ts=10,
    ))
    assert pair == (7, 8)
    form = captured["body"][0]
    assert form["metadata"]["form_id"] == "jivo_chat"
    assert "_embedded" not in form["_embedded"]["leads"][0]


@pytest.mark.parametrize("failure", ["timeout", "503"])
def test_schema2_unsorted_post_is_one_shot_then_uncertain(v2, monkeypatch, failure):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    real_create = api.create_unsorted_lead_ex
    _mock_api_v2(monkeypatch)
    monkeypatch.setattr(api, "create_unsorted_lead_ex", real_create)
    monkeypatch.setattr(api, "MAX_PATCH_RETRIES", 3)
    posts = []

    async def transient_post(method, url, _headers, json_body=None):
        posts.append((method, url, json_body))
        request = api.httpx.Request(method, url)
        if failure == "timeout":
            raise api.httpx.ReadTimeout("response lost", request=request)
        return api.httpx.Response(503, request=request, text="temporary")

    monkeypatch.setattr(api, "submit_request", transient_post)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    assert len(posts) == 1
    assert posts[0][0] == "POST"
    assert posts[0][1].endswith("/api/v4/leads/unsorted/forms")
    initial_form = posts[0][2][0]
    assert SID in initial_form["_embedded"]["leads"][0]["name"]
    assert initial_form["source_uid"] == "site_form_test-unavailable"
    assert initial_form["metadata"]["form_id"] == "test-unavailable"
    assert store.get(SID)["status"] == "uncertain"
    assert asyncio.run(sf.run_due(now=time.time() + 10 ** 6)) == 0
    assert len(posts) == 1


@pytest.mark.parametrize("failure", ["timeout", "503"])
def test_legacy_unsorted_post_keeps_default_retries(monkeypatch, failure):
    monkeypatch.setattr(api, "MAX_PATCH_RETRIES", 2)
    monkeypatch.setattr(api, "compute_retry_delay", lambda *_args: 0)
    posts = []

    async def transient_then_success(method, url, _headers, json_body=None):
        posts.append((method, url, json_body))
        request = api.httpx.Request(method, url)
        if len(posts) == 1:
            if failure == "timeout":
                raise api.httpx.ReadTimeout("temporary", request=request)
            return api.httpx.Response(503, request=request, text="temporary")
        return api.httpx.Response(200, request=request, json={"_embedded": {"unsorted": [
            {"uid": "U1", "_embedded": {"leads": [{"id": 7}], "contacts": [{"id": 8}]}}
        ]}})

    monkeypatch.setattr(api, "submit_request", transient_then_success)
    res = asyncio.run(api.create_unsorted_lead_ex(
        lead_name="N", pipeline_id=1, contact={"id": 3}, source_uid="legacy",
        page_url="", created_ts=10,
    ))
    assert res == {"lead_id": 7, "contact_id": 8, "uid": "U1"}
    assert len(posts) == 2


@pytest.mark.parametrize("failure", ["timeout", "503"])
def test_schema2_note_post_is_one_shot_then_uncertain(v2, monkeypatch, failure):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    real_note = api.add_note_to_lead
    _mock_api_v2(monkeypatch)
    monkeypatch.setattr(api, "add_note_to_lead", real_note)
    monkeypatch.setattr(api, "MAX_PATCH_RETRIES", 3)
    posts = []

    async def transient_note(method, url, _headers, json_body=None):
        posts.append((method, url, json_body))
        request = api.httpx.Request(method, url)
        if failure == "timeout":
            raise api.httpx.ReadTimeout("response lost after note commit", request=request)
        return api.httpx.Response(503, request=request, text="temporary")

    monkeypatch.setattr(api, "submit_request", transient_note)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    assert len(posts) == 1
    assert posts[0][0] == "POST"
    assert posts[0][1].endswith("/api/v4/leads/301/notes")
    assert store.get(SID)["status"] == "uncertain"
    assert store.get(SID)["lead_id"] == 301
    assert asyncio.run(sf.run_due(now=time.time() + 10 ** 6)) == 0
    assert len(posts) == 1


def test_cancel_during_schema2_one_shot_note_stays_uncertain(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    real_note = api.add_note_to_lead
    _mock_api_v2(monkeypatch)
    monkeypatch.setattr(api, "add_note_to_lead", real_note)
    posts = []

    async def scenario():
        started = asyncio.Event()

        async def blocked_submit(method, url, _headers, json_body=None):
            posts.append((method, url, json_body))
            started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(api, "submit_request", blocked_submit)
        assert (await sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
        worker = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        assert store.get(SID)["status"] == "finishing"
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert store.get(SID)["status"] == "uncertain"
        assert await sf.run_due(now=time.time() + 10 ** 6) == 0

    asyncio.run(scenario())
    assert len(posts) == 1
    assert posts[0][1].endswith("/api/v4/leads/301/notes")


@pytest.mark.parametrize("failure", ["timeout", "503"])
def test_legacy_note_post_keeps_default_retries(monkeypatch, failure):
    monkeypatch.setattr(api, "MAX_PATCH_RETRIES", 2)
    monkeypatch.setattr(api, "compute_retry_delay", lambda *_args: 0)
    posts = []

    async def transient_then_success(method, url, _headers, json_body=None):
        posts.append((method, url, json_body))
        request = api.httpx.Request(method, url)
        if len(posts) == 1:
            if failure == "timeout":
                raise api.httpx.ReadTimeout("temporary", request=request)
            return api.httpx.Response(503, request=request, text="temporary")
        return api.httpx.Response(200, request=request, json=[])

    monkeypatch.setattr(api, "submit_request", transient_then_success)
    assert asyncio.run(api.add_note_to_lead(301, "legacy note")) is True
    assert len(posts) == 2


def test_api_accept_unsorted(monkeypatch):
    async def fake_request_json(method, url, body=None, what=""):
        assert url.endswith("/api/v4/leads/unsorted/U1/accept")
        assert body == {"status_id": 42}
        return {"_embedded": {"leads": [{"id": 99}]}}

    monkeypatch.setattr(api, "_request_json", fake_request_json)
    assert asyncio.run(api.accept_unsorted("U1", 42)) == 99


def test_api_set_lead_utm(monkeypatch):
    captured = {}

    async def fake_request_json(method, url, body=None, what=""):
        captured.update(method=method, url=url, body=body)
        return {"id": 1}

    monkeypatch.setattr(api, "_request_json", fake_request_json)
    assert asyncio.run(api.set_lead_utm(1, {"utm_source": "ya", "utm_term": ""}))
    assert captured["method"] == "PATCH"
    assert captured["url"].endswith("/api/v4/leads/1")
    assert captured["body"] == {"custom_fields_values": [{"field_code": "UTM_SOURCE", "values": [{"value": "ya"}]}]}


# --- схема 2: новые формы (плагин sun-contact-forms) -----------------------

SID = "3f2b8c1e-5d4a-4b6f-9a7e-1c2d3e4f5a6b"

V2_MAP = {
    "test-callback": {"source": "Форма: обратный звонок", "pipeline_id": 8642414,
                      "status_id": 70070982, "tags": ["тест"]},
    "test-consultation": {"source": "Форма: консультация", "pipeline_id": 8642414,
                          "status_id": 70070982, "tags": ["тест"]},
    "test-unavailable": {"source": "Форма: нет в наличии", "pipeline_id": 8642414,
                         "status_id": 70070982, "tags": ["тест"], "form_type": "unavailable"},
}


@pytest.fixture
def v2(monkeypatch, tmp_path):
    monkeypatch.setattr(sf, "FORM_MAP", {**sf.FORM_MAP, **V2_MAP})
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "site_form.sqlite3"))
    monkeypatch.setattr(sf, "_wake", None)
    store.init_db()
    return store


def _v2_payload(form_type="callback", **over):
    payload = {
        "schema": 2,
        "form": f"test-{form_type}",
        "form_type": form_type,
        "submission_id": SID,
        "client_ip": "203.0.113.7",
        "contact": {"name": "Иван", "phone": "+79990000000", "telegram": "@ivan_test"},
        "comment": "Какой кошелёк выбрать?",
        "context": {
            "title": "Остались вопросы?",
            "entry": "test-page-callback",
            "page_url": "https://test.sunscrypt.ru/product/hardware-wallets/apparatnyj-koshelek-keystone-3-pro/?utm_source=ya",
            "page_title": "Keystone 3 Pro",
            "referrer": "https://yandex.ru/",
            "utm": {"utm_source": "ya", "utm_medium": "cpc", "evil": "x"},
            "product": {"id": 4899, "name": "Keystone 3 Pro", "sku": "HW-26",
                        "url": "https://test.sunscrypt.ru/product/hardware-wallets/apparatnyj-koshelek-keystone-3-pro/"},
            "service": None,
            "format": None,
        },
    }
    if form_type == "consultation":
        payload["context"]["service"] = {"id": None, "name": "Консультация по безопасности", "url": "", "verified": False}
        payload["context"]["format"] = {"code": "showroom", "label": "В шоуруме в Москве"}
    payload.update(over)
    return payload


def _mock_api_v2(monkeypatch, lead_id=301, uid="U-301", accepted=301):
    calls = _mock_api(monkeypatch, lead_id=lead_id, uid=uid, accepted=accepted)

    async def set_lead_utm(lid, utm):
        calls["utm"] = (lid, utm)
        return True

    monkeypatch.setattr(api, "set_lead_utm", set_lead_utm)
    return calls


def test_normalize_phone_v2():
    assert sf.normalize_phone_v2("8 (999) 000-00-00") == "+79990000000"
    assert sf.normalize_phone_v2("+7 999 000-00-00") == "+79990000000"
    assert sf.normalize_phone_v2("9990000000") == "+79990000000"
    assert sf.normalize_phone_v2("+375 29 123-45-67") == "+375291234567"
    # без «+» страну не угадываем - человек проверяет номер сам
    assert sf.normalize_phone_v2("375291234567") == ""
    assert sf.normalize_phone_v2("+7 999 000") == ""
    assert sf.normalize_phone_v2("+8 999 000-00-00") == ""
    assert sf.normalize_phone_v2("звоните вечером") == ""


def test_clean_v2_callback(v2):
    clean = sf.clean_v2(_v2_payload())
    assert clean["contact"]["phone"] == "+79990000000"
    assert clean["context"]["utm"] == {"utm_source": "ya", "utm_medium": "cpc"}
    assert clean["context"]["product"]["sku"] == "HW-26"
    assert clean["context"]["service"] is None
    assert clean["context"]["format"] is None


def test_unavailable_disabled_by_default(v2):
    with pytest.raises(sf.PayloadError, match="unavailable-disabled"):
        sf.clean_v2(_v2_payload("unavailable"))
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable"))) == (
        422, {"ok": False, "error": "unavailable-disabled"},
    )
    assert store.get(SID) is None


@pytest.mark.parametrize("change, error", [
    (lambda p: p.update(form="test-callback"), "form-type-mismatch"),
    (lambda p: p.update(form_type="callback"), "form-type-mismatch"),
    (lambda p: p["context"].update(product=None), "no-product"),
    (lambda p: p["context"]["product"].update(id=0), "no-product"),
    (lambda p: p["context"]["product"].update(name=""), "no-product"),
    (lambda p: p["context"]["product"].update(url="javascript:alert(1)"), "no-product"),
    (lambda p: p["context"].update(entry=""), "no-entry"),
])
def test_unavailable_requires_exact_map_and_product(v2, monkeypatch, change, error):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    payload = _v2_payload("unavailable")
    change(payload)
    with pytest.raises(sf.PayloadError, match=error):
        sf.clean_v2(payload)
    assert store.get(SID) is None


def test_unavailable_cannot_use_schema_1(v2, monkeypatch):
    calls = _mock_api(monkeypatch)
    assert asyncio.run(sf.process(_payload(slug="test-unavailable"))) is None
    payload = _payload()
    payload["form_type"] = "unavailable"
    assert asyncio.run(sf.process(payload)) is None
    assert "create" not in calls


def test_unavailable_queue_creates_one_incoming_without_order(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)
    payload = _v2_payload("unavailable")
    payload["context"]["entry"] = "product-out-of-stock"
    payload["comment"] = "Сообщите, когда появится"
    clean = sf.clean_v2(payload)
    assert clean["context"]["product"]["id"] == 4899
    assert clean["context"]["service"] is None
    assert clean["context"]["format"] is None
    assert asyncio.run(sf.accept_v2(payload)) == (200, {"ok": True, "status": "accepted"})
    assert asyncio.run(sf.accept_v2(payload)) == (200, {"ok": True, "status": "duplicate"})
    assert asyncio.run(sf.run_due()) == 1
    created = calls["create"]
    assert created["lead_name"] == f"Форма: нет в наличии: Иван [ID заявки: {SID}]"
    assert created["source_uid"] == "site_form_test-unavailable"
    assert created["form_id"] == "test-unavailable"
    assert created["lead_tags"] == ["Форма: нет в наличии", "тест"]
    assert "custom_fields_values" not in created
    note = calls["note"][1]
    assert "✉️ ЗАЯВКА С САЙТА: НЕТ В НАЛИЧИИ" in note
    assert "Товар: Keystone 3 Pro, артикул HW-26" in note
    assert "Запрос: Сообщите, когда появится" in note
    assert "Кнопка на сайте: product-out-of-stock" in note
    assert "Номер заявки: 3f2b8c1e" in note
    assert f"ID заявки: {SID}" in note
    assert "Заказ" not in note and "Предзаказ" not in note
    assert store.get(SID)["status"] == "done"
    calls.pop("create")
    assert asyncio.run(sf.accept_v2(payload))[1]["status"] == "duplicate"
    assert asyncio.run(sf.run_due()) == 0
    assert "create" not in calls


@pytest.mark.parametrize("form_type, expected", [
    ("callback", "Форма: обратный звонок: Иван"),
    ("consultation", "Форма: консультация: Иван"),
])
def test_existing_form_lead_names_keep_previous_format(v2, monkeypatch, form_type, expected):
    calls = _mock_api_v2(monkeypatch)
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    assert calls["create"]["lead_name"] == expected
    assert f"ID заявки: {SID}" not in calls["note"][1]


def test_unavailable_long_source_keeps_full_id_in_initial_name(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    long_source = "Форма: " + "А" * 300
    monkeypatch.setattr(sf, "FORM_MAP", {
        **sf.FORM_MAP,
        "test-unavailable": {**sf.FORM_MAP["test-unavailable"], "source": long_source},
    })
    calls = _mock_api_v2(monkeypatch)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    lead_name = calls["create"]["lead_name"]
    assert len(lead_name) == sf.MAX_NAME_LEN
    assert lead_name.endswith(f"[ID заявки: {SID}]")
    assert f"ID заявки: {SID}" in calls["note"][1]


@pytest.mark.parametrize("form_type", ["unavailable", "callback", "consultation"])
def test_pending_claim_has_one_winner_across_sqlite_connections(v2, monkeypatch, form_type):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    barrier = Barrier(2)

    def contend():
        barrier.wait(timeout=5)
        return store.claim_pending(SID)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: contend(), range(2)))
    assert sorted(results) == [False, True]
    assert store.get(SID)["status"] == "processing"
    assert store.due() == []


def test_two_workers_do_not_create_two_unavailable_incoming(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    creates = []

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_create(**kwargs):
            creates.append(kwargs)
            started.set()
            await release.wait()
            return {"lead_id": 301, "uid": "U-301"}

        monkeypatch.setattr(api, "create_unsorted_lead_ex", slow_create)
        assert (await sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
        first = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        assert store.get(SID)["status"] == "processing"
        second_count = await asyncio.wait_for(sf.run_due(), timeout=5)
        release.set()
        first_count = await asyncio.wait_for(first, timeout=5)
        return first_count, second_count

    assert asyncio.run(scenario()) == (1, 0)
    assert len(creates) == 1
    assert store.get(SID)["status"] == "done"


@pytest.mark.parametrize("form_type", ["unavailable", "callback", "consultation"])
def test_two_workers_created_row_add_one_note(v2, monkeypatch, form_type):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)
    notes = []

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_note(lead_id, note, *, max_attempts=None):
            notes.append((lead_id, note))
            started.set()
            await release.wait()
            return True

        monkeypatch.setattr(api, "add_note_to_lead", slow_note)
        assert (await sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
        store.mark_created(SID, 555, "U-555")
        first = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        assert store.get(SID)["status"] == "finishing"
        second_count = await asyncio.wait_for(sf.run_due(), timeout=5)
        release.set()
        first_count = await asyncio.wait_for(first, timeout=5)
        return first_count, second_count

    assert asyncio.run(scenario()) == (1, 0)
    assert len(notes) == 1
    assert "create" not in calls
    assert store.get(SID)["status"] == "done"


def test_new_lead_keeps_finish_claim_until_note_is_done(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    notes = []

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_note(lead_id, note, *, max_attempts=None):
            notes.append((lead_id, note))
            started.set()
            await release.wait()
            return True

        monkeypatch.setattr(api, "add_note_to_lead", slow_note)
        assert (await sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
        first = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        row = store.get(SID)
        assert row["status"] == "finishing"
        assert row["lead_id"] == 301
        assert await sf.run_due() == 0
        release.set()
        assert await first == 1

    asyncio.run(scenario())
    assert len(notes) == 1
    assert store.get(SID)["status"] == "done"


def test_created_row_note_timeout_is_uncertain_not_duplicated(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)
    notes = []

    async def committed_note_then_timeout(lead_id, note, *, max_attempts=None):
        notes.append((lead_id, note))
        raise TimeoutError("response lost after note")

    monkeypatch.setattr(api, "add_note_to_lead", committed_note_then_timeout)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    store.mark_created(SID, 555, "U-555")
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == "uncertain"
    assert asyncio.run(sf.run_due(now=time.time() + 10 ** 6)) == 0
    assert len(notes) == 1
    assert "create" not in calls


def test_cancel_created_row_before_note_recovers_without_create(v2, monkeypatch):
    calls = _mock_api_v2(monkeypatch)
    original_tags = api.set_lead_tags

    async def scenario():
        started = asyncio.Event()

        async def blocked_tags(_lead_id, _tags):
            started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(api, "set_lead_tags", blocked_tags)
        assert (await sf.accept_v2(_v2_payload("callback")))[1]["status"] == "accepted"
        store.mark_created(SID, 555, "U-555")
        worker = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert store.get(SID)["status"] == "created"
        monkeypatch.setattr(api, "set_lead_tags", original_tags)
        assert await sf.run_due() == 1

    asyncio.run(scenario())
    assert "create" not in calls
    assert store.get(SID)["status"] == "done"


def test_unavailable_pending_held_when_gate_closed(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", False)
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == "held"
    assert store.get(SID)["attempts"] == 0
    assert store.due() == []
    assert "create" not in calls


def test_unavailable_pending_held_when_gate_and_map_removed(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", False)
    monkeypatch.setattr(sf, "FORM_MAP", {})
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == "held"
    assert store.get(SID)["attempts"] == 0
    assert "create" not in calls


def test_unavailable_pending_held_on_map_drift(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    changed_map = {**sf.FORM_MAP, "test-unavailable": {**sf.FORM_MAP["test-unavailable"], "form_type": None}}
    monkeypatch.setattr(sf, "FORM_MAP", changed_map)
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == "held"
    assert store.get(SID)["attempts"] == 0
    assert store.get(SID)["last_error"] == "form-type-mismatch"
    assert asyncio.run(sf.run_due()) == 0
    assert "create" not in calls
    monkeypatch.setattr(sf, "FORM_MAP", {**sf.FORM_MAP, "test-unavailable": V2_MAP["test-unavailable"]})
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == "done"
    assert store.get(SID)["attempts"] == 0
    assert "create" in calls


def test_twenty_held_unavailable_do_not_starve_callback_and_resume(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_CLIENT_RATE_PER_MINUTE", 100)
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    creates = []

    async def create_unsorted_lead_ex(**kwargs):
        creates.append(kwargs)
        lead_id = 300 + len(creates)
        return {"lead_id": lead_id, "uid": f"U-{lead_id}"}

    async def accept_unsorted(uid, status_id, user_id=None):
        return int(uid[2:])

    monkeypatch.setattr(api, "create_unsorted_lead_ex", create_unsorted_lead_ex)
    monkeypatch.setattr(api, "accept_unsorted", accept_unsorted)
    unavailable_ids = []
    for n in range(20):
        submission_id = f"aaaaaaaa-0000-4000-8000-{n:012x}"
        unavailable_ids.append(submission_id)
        assert asyncio.run(sf.accept_v2(_v2_payload("unavailable", submission_id=submission_id)))[1]["status"] == "accepted"
    callback_id = "bbbbbbbb-0000-4000-8000-000000000000"
    assert asyncio.run(sf.accept_v2(_v2_payload("callback", submission_id=callback_id)))[1]["status"] == "accepted"

    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", False)
    assert asyncio.run(sf.run_due()) == 21
    assert store.get(callback_id)["status"] == "done"
    assert [call["form_id"] for call in creates] == ["test-callback"]
    assert all(store.get(submission_id)["status"] == "held" for submission_id in unavailable_ids)
    assert all(store.get(submission_id)["attempts"] == 0 for submission_id in unavailable_ids)
    assert asyncio.run(sf.run_due()) == 0

    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    assert asyncio.run(sf.run_due()) == 20
    assert len(creates) == 21
    assert all(call["form_id"] == "test-unavailable" for call in creates[1:])
    assert all(store.get(submission_id)["status"] == "done" for submission_id in unavailable_ids)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable", submission_id=unavailable_ids[0])))[1]["status"] == "duplicate"
    assert asyncio.run(sf.run_due()) == 0
    assert len(creates) == 21


def test_held_created_unavailable_resumes_without_second_lead(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    store.mark_created(SID, 555, "U-555")
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", False)
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == "held"
    assert store.get(SID)["lead_id"] == 555
    assert "create" not in calls
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == "done"
    assert "create" not in calls
    assert calls["accept"] == ("U-555", 70070982)


def test_unavailable_source_not_registered_when_disabled(v2, monkeypatch):
    calls = {}

    async def list_sources():
        calls["list"] = True
        return []

    async def create_sources(missing):
        calls["create"] = missing
        return missing

    monkeypatch.setattr(api, "list_sources", list_sources)
    monkeypatch.setattr(api, "create_sources", create_sources)
    monkeypatch.setattr(sf, "FORM_MAP", {"test-unavailable": V2_MAP["test-unavailable"]})
    asyncio.run(sf.ensure_sources())
    assert calls == {}


@pytest.mark.parametrize("mutate, error", [
    (lambda p: p.update(form="left-form"), "unknown-form"),
    (lambda p: p.update(submission_id="<script>"), "bad-submission-id"),
    (lambda p: p.update(form_type="preorder"), "bad-form-type"),
    (lambda p: p["contact"].update(name="  "), "no-name"),
    (lambda p: p["contact"].update(phone="12345"), "bad-phone"),
])
def test_clean_v2_rejects(v2, mutate, error):
    payload = _v2_payload()
    mutate(payload)
    with pytest.raises(sf.PayloadError, match=error):
        sf.clean_v2(payload)


def test_clean_v2_consultation_requires_format(v2):
    payload = _v2_payload("consultation")
    payload["context"]["format"] = None
    with pytest.raises(sf.PayloadError, match="no-format"):
        sf.clean_v2(payload)
    payload["context"]["format"] = {"code": "moon"}
    with pytest.raises(sf.PayloadError, match="no-format"):
        sf.clean_v2(payload)


def test_clean_v2_drops_bad_urls(v2):
    payload = _v2_payload()
    payload["context"]["page_url"] = "javascript:alert(1)"
    payload["context"]["referrer"] = "ftp://example.com/"
    payload.pop("page_url", None)
    clean = sf.clean_v2(payload)
    assert clean["context"]["page_url"] == ""
    assert clean["context"]["referrer"] == ""


def test_accept_v2_accepted_then_duplicate(v2):
    assert asyncio.run(sf.accept_v2(_v2_payload())) == (200, {"ok": True, "status": "accepted"})
    assert asyncio.run(sf.accept_v2(_v2_payload())) == (200, {"ok": True, "status": "duplicate"})
    row = store.get(SID)
    assert row["status"] == "pending"
    assert "+79990000000" in row["payload"]


def test_accept_v2_invalid_and_rate_limit(v2, monkeypatch):
    broken = _v2_payload()
    broken["contact"]["phone"] = ""
    assert asyncio.run(sf.accept_v2(broken)) == (422, {"ok": False, "error": "bad-phone"})
    monkeypatch.setattr(sf, "SITE_FORM_CLIENT_RATE_PER_MINUTE", 1)
    assert asyncio.run(sf.accept_v2(_v2_payload()))[0] == 200
    second = _v2_payload(submission_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
    assert asyncio.run(sf.accept_v2(second)) == (429, {"ok": False, "error": "rate-limit"})


def test_accept_v2_store_down(v2, monkeypatch):
    def boom(*args, **kwargs):
        raise OSError("disk")

    monkeypatch.setattr(store, "insert_pending", boom)
    assert asyncio.run(sf.accept_v2(_v2_payload())) == (503, {"ok": False, "error": "store"})


def test_run_due_creates_lead_and_wipes_payload(v2, monkeypatch):
    calls = _mock_api_v2(monkeypatch)
    asyncio.run(sf.accept_v2(_v2_payload()))
    assert asyncio.run(sf.run_due()) == 1

    created = calls["create"]
    assert created["pipeline_id"] == 8642414
    assert created["lead_name"] == "Форма: обратный звонок: Иван"
    assert created["lead_tags"] == ["Форма: обратный звонок", "тест"]
    assert created["ip"] == "203.0.113.7"
    assert created["page_url"].startswith("https://test.sunscrypt.ru/product/")
    assert created["contact"]["custom_fields_values"][0]["values"][0]["value"] == "+79990000000"
    assert calls["find"] == ["+79990000000"]
    assert calls["accept"] == ("U-301", 70070982)
    assert calls["tags"] == (301, ["Форма: обратный звонок", "тест"])
    assert calls["utm"] == (301, {"utm_source": "ya", "utm_medium": "cpc"})

    note = calls["note"][1]
    head, person, tech = note.split("\n\n")
    assert head.splitlines() == [
        "✉️ ЗАЯВКА С САЙТА: ОБРАТНЫЙ ЗВОНОК ИЛИ ВОПРОС",
        "Страница: Keystone 3 Pro",
        "Форма: Остались вопросы?",
        "Товар: Keystone 3 Pro, артикул HW-26",
        "Вопрос: Какой кошелёк выбрать?",
    ]
    assert person.splitlines() == ["☎️ КОНТАКТ", "Имя: Иван", "Телефон: +79990000000", "Telegram: @ivan_test"]
    assert tech.splitlines() == [
        "⚙️ ТЕХНИЧЕСКОЕ",
        "Источник: Форма: обратный звонок",
        "Адрес страницы: https://test.sunscrypt.ru/product/hardware-wallets/apparatnyj-koshelek-keystone-3-pro/?utm_source=ya",
        "Ссылка на товар: https://test.sunscrypt.ru/product/hardware-wallets/apparatnyj-koshelek-keystone-3-pro/",
        "Кнопка на сайте: test-page-callback",
        "UTM: utm_source=ya, utm_medium=cpc",
        "Пришёл с: https://yandex.ru/",
        "Номер заявки: 3f2b8c1e",
    ]
    assert "·" not in note
    # amo молча вырезает символы вне базовой плоскости Unicode (📩, 👤) - в примечании их быть не должно
    assert all(ord(ch) < 0x10000 for ch in note)

    row = store.get(SID)
    assert row["status"] == "done"
    assert row["lead_id"] == 301
    assert row["payload"] is None  # персональные данные после создания сделки не храним

    # повтор той же попытки после создания сделки - второй сделки нет
    calls.pop("create")
    assert asyncio.run(sf.accept_v2(_v2_payload()))[1]["status"] == "duplicate"
    assert asyncio.run(sf.run_due()) == 0
    assert "create" not in calls


def test_consultation_note_has_service_and_format(v2, monkeypatch):
    calls = _mock_api_v2(monkeypatch)
    asyncio.run(sf.accept_v2(_v2_payload("consultation")))
    asyncio.run(sf.run_due())
    note = calls["note"][1]
    head, _person, tech = (block.splitlines() for block in note.split("\n\n"))
    assert head[0] == "✉️ ЗАЯВКА С САЙТА: ЗАПИСЬ НА КОНСУЛЬТАЦИЮ"
    assert all(ord(ch) < 0x10000 for ch in note)
    assert "Консультация: Консультация по безопасности" in head
    assert "Формат: В шоуруме в Москве" in head
    assert head[-1] == "Запрос: Какой кошелёк выбрать?"
    # пометка о несверенной консультации - только в технической части
    assert "Консультация не сверена с сайтом: кнопка передала только название" in tech
    assert not any("не сверена" in line for line in head)


def test_consultation_verified_service_has_no_mark(v2):
    clean = sf.clean_v2(_v2_payload("consultation"))
    clean["context"]["service"] = {"id": 555, "name": "Консультация по Tangem",
                                   "url": "https://test.sunscrypt.ru/konsultaciya-tangem/", "verified": True}
    head, _person, tech = (b.splitlines() for b in sf.note_text_v2(clean, "Форма: консультация").split("\n\n"))
    assert "Консультация: Консультация по Tangem" in head
    assert "Ссылка на консультацию: https://test.sunscrypt.ru/konsultaciya-tangem/" in tech
    assert not any("не сверена" in line for line in tech)


def test_note_skips_empty_fields_and_site_suffix(v2):
    clean = sf.clean_v2(_v2_payload())
    clean["context"]["page_title"] = "Тест контактных форм (служебная) - Sunscrypt"
    clean["comment"] = ""
    clean["contact"]["telegram"] = ""
    head, person, tech = sf.note_text_v2(clean, "Форма: обратный звонок").split("\n\n")
    assert "Страница: Тест контактных форм (служебная)" in head.splitlines()
    assert not any(line.startswith("Вопрос:") for line in head.splitlines())
    assert not any(line.startswith("Telegram:") for line in person.splitlines())
    assert tech.splitlines()[-1] == "Номер заявки: 3f2b8c1e"


def test_v2_existing_contact_linked_by_phone(v2, monkeypatch):
    calls = _mock_api_v2(monkeypatch)

    async def find_contact_id(query):
        calls.setdefault("find", []).append(query)
        return 777

    monkeypatch.setattr(api, "find_contact_id", find_contact_id)
    asyncio.run(sf.accept_v2(_v2_payload()))
    asyncio.run(sf.run_due())
    assert calls["create"]["contact"] == {"id": 777}


def test_v2_no_lead_id_is_uncertain_not_retried(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch, lead_id=None)
    asyncio.run(sf.accept_v2(_v2_payload("unavailable")))
    asyncio.run(sf.run_due())
    row = store.get(SID)
    assert row["status"] == "uncertain"
    assert row["attempts"] == 0
    assert row["last_error"] == "amo-create-outcome-unknown"
    assert store.due(now=time.time() + 10 ** 6) == []


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
@pytest.mark.parametrize("failure", ["timeout", "503"])
def test_existing_forms_gate_off_keep_create_api_retries(v2, monkeypatch, form_type, failure):
    real_create = api.create_unsorted_lead_ex
    _mock_api_v2(monkeypatch)
    monkeypatch.setattr(api, "create_unsorted_lead_ex", real_create)
    monkeypatch.setattr(api, "MAX_PATCH_RETRIES", 2)
    monkeypatch.setattr(api, "compute_retry_delay", lambda *_args: 0)
    posts = []

    async def transient_then_success(method, url, _headers, json_body=None):
        posts.append((method, url, json_body))
        request = api.httpx.Request(method, url)
        if len(posts) == 1:
            if failure == "timeout":
                raise api.httpx.ReadTimeout("temporary", request=request)
            return api.httpx.Response(503, request=request, text="temporary")
        return api.httpx.Response(200, request=request, json={"_embedded": {"unsorted": [
            {"uid": "U-301", "_embedded": {"leads": [{"id": 301}], "contacts": [{"id": 302}]}}
        ]}})

    monkeypatch.setattr(api, "submit_request", transient_then_success)
    assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    assert len(posts) == 2
    assert all(method == "POST" and url.endswith("/api/v4/leads/unsorted/forms")
               for method, url, _body in posts)
    assert store.get(SID)["status"] == "done"


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
def test_existing_forms_gate_off_failed_create_returns_to_due(v2, monkeypatch, form_type):
    calls = _mock_api_v2(monkeypatch, lead_id=None)
    assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    row = store.get(SID)
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["last_error"] == "amo-create-failed"
    assert row["next_try_at"] > time.time()
    assert store.due(now=row["next_try_at"] + 1)[0]["submission_id"] == SID
    assert calls["create"]["lead_name"] == f"{V2_MAP[f'test-{form_type}']['source']}: Иван"


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
@pytest.mark.parametrize("failure", ["timeout", "503"])
def test_existing_forms_gate_off_keep_note_api_retries(v2, monkeypatch, form_type, failure):
    real_note = api.add_note_to_lead
    _mock_api_v2(monkeypatch)
    monkeypatch.setattr(api, "add_note_to_lead", real_note)
    monkeypatch.setattr(api, "MAX_PATCH_RETRIES", 2)
    monkeypatch.setattr(api, "compute_retry_delay", lambda *_args: 0)
    posts = []

    async def transient_then_success(method, url, _headers, json_body=None):
        posts.append((method, url, json_body))
        request = api.httpx.Request(method, url)
        if len(posts) == 1:
            if failure == "timeout":
                raise api.httpx.ReadTimeout("temporary", request=request)
            return api.httpx.Response(503, request=request, text="temporary")
        return api.httpx.Response(200, request=request, json=[])

    monkeypatch.setattr(api, "submit_request", transient_then_success)
    assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    assert len(posts) == 2
    assert all(method == "POST" and url.endswith("/api/v4/leads/301/notes")
               for method, url, _body in posts)
    assert store.get(SID)["status"] == "done"


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
def test_existing_forms_gate_off_keep_nonfatal_note_failure(v2, monkeypatch, form_type):
    _mock_api_v2(monkeypatch)
    notes = []

    async def failed_note(lead_id, note):
        notes.append((lead_id, note))
        return False

    monkeypatch.setattr(api, "add_note_to_lead", failed_note)
    assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    assert len(notes) == 1
    assert store.get(SID)["status"] == "done"


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
@pytest.mark.parametrize("phase, expected_status", [("create", "pending"), ("note", "created")])
def test_existing_forms_gate_off_exception_uses_retry_queue(v2, monkeypatch, form_type, phase, expected_status):
    _mock_api_v2(monkeypatch)

    async def failed_create(**_kwargs):
        raise TimeoutError("temporary create failure")

    async def failed_note(_lead_id, _note):
        raise TimeoutError("temporary note failure")

    if phase == "create":
        monkeypatch.setattr(api, "create_unsorted_lead_ex", failed_create)
    else:
        monkeypatch.setattr(api, "add_note_to_lead", failed_note)
    assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    row = store.get(SID)
    assert row["status"] == expected_status
    assert row["attempts"] == 1
    assert row["last_error"] == "TimeoutError"
    assert store.due(now=row["next_try_at"] + 1)[0]["submission_id"] == SID


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
def test_existing_forms_gate_off_processing_claim_excludes_second_worker(v2, monkeypatch, form_type):
    _mock_api_v2(monkeypatch)
    creates = []

    async def scenario():
        started = asyncio.Event()
        release = asyncio.Event()

        async def slow_create(**kwargs):
            creates.append(kwargs)
            started.set()
            await release.wait()
            return {"lead_id": 301, "uid": "U-301"}

        monkeypatch.setattr(api, "create_unsorted_lead_ex", slow_create)
        assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
        assert (await sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
        first = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        assert store.get(SID)["status"] == "processing"
        assert await asyncio.wait_for(sf.run_due(), timeout=5) == 0
        release.set()
        assert await asyncio.wait_for(first, timeout=5) == 1

    asyncio.run(scenario())
    assert len(creates) == 1
    assert store.get(SID)["status"] == "done"


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
@pytest.mark.parametrize("phase, expected_status", [("create", "pending"), ("note", "created")])
def test_existing_forms_gate_off_cancellation_releases_claim(v2, monkeypatch, form_type, phase, expected_status):
    _mock_api_v2(monkeypatch)

    async def scenario():
        started = asyncio.Event()

        async def blocked_create(**_kwargs):
            started.set()
            await asyncio.Event().wait()

        async def blocked_note(_lead_id, _note):
            started.set()
            await asyncio.Event().wait()

        if phase == "create":
            monkeypatch.setattr(api, "create_unsorted_lead_ex", blocked_create)
        else:
            monkeypatch.setattr(api, "add_note_to_lead", blocked_note)
        assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
        assert (await sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
        if phase == "note":
            store.mark_created(SID, 301, "U-301")
        worker = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        assert store.get(SID)["status"] == ("processing" if phase == "create" else "finishing")
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker

    asyncio.run(scenario())
    row = store.get(SID)
    assert row["status"] == expected_status
    assert row["attempts"] == 0
    assert store.due()[0]["submission_id"] == SID


def test_remote_committed_then_timeout_never_recreates(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    creates = []

    async def committed_then_timeout(**kwargs):
        creates.append(kwargs)
        raise TimeoutError("response lost after remote commit")

    monkeypatch.setattr(api, "create_unsorted_lead_ex", committed_then_timeout)
    payload = _v2_payload("unavailable")
    assert asyncio.run(sf.accept_v2(payload))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    row = store.get(SID)
    assert row["status"] == "uncertain"
    assert row["attempts"] == 0
    assert "TimeoutError" in row["last_error"]
    assert asyncio.run(sf.accept_v2(payload))[1]["status"] == "duplicate"
    assert asyncio.run(sf.run_due(now=time.time() + 10 ** 6)) == 0
    assert len(creates) == 1


def test_mark_created_write_error_never_recreates(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    creates = []

    async def created_remotely(**kwargs):
        creates.append(kwargs)
        return {"lead_id": 301, "uid": "U-301"}

    def failed_write(*_args, **_kwargs):
        raise OSError("local write failed")

    monkeypatch.setattr(api, "create_unsorted_lead_ex", created_remotely)
    monkeypatch.setattr(store, "mark_created", failed_write)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    row = store.get(SID)
    assert row["status"] == "uncertain"
    assert row["attempts"] == 0
    assert "OSError" in row["last_error"]
    assert asyncio.run(sf.run_due(now=time.time() + 10 ** 6)) == 0
    assert len(creates) == 1


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
@pytest.mark.parametrize("db_recovers", [True, False])
def test_known_legacy_lead_mark_created_error_never_recreates(v2, monkeypatch, form_type, db_recovers):
    _mock_api_v2(monkeypatch)
    original_create = api.create_unsorted_lead_ex
    original_mark = store.mark_created
    creates = []
    mark_calls = []

    async def counted_create(**kwargs):
        creates.append(kwargs)
        return await original_create(**kwargs)

    def failed_first_mark(*args, **kwargs):
        mark_calls.append((args, kwargs))
        if not db_recovers or len(mark_calls) == 1:
            raise OSError("SQLite write failed after amo confirmed lead")
        return original_mark(*args, **kwargs)

    monkeypatch.setattr(api, "create_unsorted_lead_ex", counted_create)
    monkeypatch.setattr(store, "mark_created", failed_first_mark)
    assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    row = store.get(SID)
    assert row["status"] == ("created" if db_recovers else "processing")
    assert row["attempts"] == 0
    assert len(creates) == 1
    if db_recovers:
        assert row["lead_id"] == 301
        assert asyncio.run(sf.run_due()) == 1
        assert store.get(SID)["status"] == "done"
    else:
        assert store.due(now=time.time() + 10 ** 6) == []
        assert asyncio.run(sf.run_due(now=time.time() + 10 ** 6)) == 0
    assert len(creates) == 1
    assert len(mark_calls) == 2


@pytest.mark.parametrize("phase, status", [("create", "processing"), ("note", "finishing")])
def test_uncertain_store_write_error_remains_outside_due(v2, monkeypatch, phase, status):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    external_writes = []

    if phase == "create":
        async def ambiguous_create(**kwargs):
            external_writes.append(kwargs)
            raise TimeoutError("create response lost")

        monkeypatch.setattr(api, "create_unsorted_lead_ex", ambiguous_create)
    else:
        async def ambiguous_note(lead_id, note, *, max_attempts=None):
            external_writes.append((lead_id, note, max_attempts))
            raise TimeoutError("note response lost")

        monkeypatch.setattr(api, "add_note_to_lead", ambiguous_note)

    def failed_uncertain_write(*_args, **_kwargs):
        raise OSError("SQLite unavailable")

    monkeypatch.setattr(store, "mark_uncertain", failed_uncertain_write)
    assert asyncio.run(sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    assert store.get(SID)["status"] == status
    assert store.due(now=time.time() + 10 ** 6) == []
    assert asyncio.run(sf.run_due(now=time.time() + 10 ** 6)) == 0
    assert len(external_writes) == 1


@pytest.mark.parametrize("form_type", ["unavailable", "callback", "consultation"])
def test_cancel_before_remote_create_releases_claim(v2, monkeypatch, form_type):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    calls = _mock_api_v2(monkeypatch)

    async def scenario():
        started = asyncio.Event()

        async def blocked_find(_phone):
            started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(api, "find_contact_id", blocked_find)
        assert (await sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
        worker = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        assert store.get(SID)["status"] == "processing"
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert store.get(SID)["status"] == "pending"

        async def no_contact(_phone):
            return None

        monkeypatch.setattr(api, "find_contact_id", no_contact)
        assert await sf.run_due() == 1

    asyncio.run(scenario())
    assert "create" in calls
    assert store.get(SID)["status"] == "done"


def test_cancel_during_remote_create_is_uncertain_not_retried(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    creates = []

    async def scenario():
        started = asyncio.Event()

        async def blocked_create(**kwargs):
            creates.append(kwargs)
            started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(api, "create_unsorted_lead_ex", blocked_create)
        assert (await sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
        worker = asyncio.create_task(sf.run_due())
        await asyncio.wait_for(started.wait(), timeout=5)
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert store.get(SID)["status"] == "uncertain"
        assert await sf.run_due(now=time.time() + 10 ** 6) == 0

    asyncio.run(scenario())
    assert len(creates) == 1


def test_late_mark_created_after_cancel_cannot_override_uncertain(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    original_mark = store.mark_created
    entered, release, finished = Event(), Event(), Event()

    def delayed_mark(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(5)
            return original_mark(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(store, "mark_created", delayed_mark)

    async def scenario():
        assert (await sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
        worker = asyncio.create_task(sf.run_due())
        assert await asyncio.to_thread(entered.wait, 5)
        assert store.get(SID)["status"] == "processing"
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert store.get(SID)["status"] == "uncertain"
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
        assert store.get(SID)["status"] == "uncertain"
        assert store.get(SID)["lead_id"] is None
        assert await sf.run_due(now=time.time() + 10 ** 6) == 0

    try:
        asyncio.run(scenario())
    finally:
        release.set()


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
def test_late_mark_created_after_cancel_keeps_confirmed_legacy_lead(v2, monkeypatch, form_type):
    _mock_api_v2(monkeypatch)
    original_create = api.create_unsorted_lead_ex
    original_mark = store.mark_created
    entered, release, finished = Event(), Event(), Event()
    creates = []

    async def counted_create(**kwargs):
        creates.append(kwargs)
        return await original_create(**kwargs)

    def delayed_mark(*args, **kwargs):
        # Первый вызов работает в to_thread; отмена запускает второй вызов
        # синхронно и сохраняет подтверждённый lead_id до освобождения claim.
        if not entered.is_set():
            entered.set()
            try:
                assert release.wait(5)
                return original_mark(*args, **kwargs)
            finally:
                finished.set()
        return original_mark(*args, **kwargs)

    monkeypatch.setattr(api, "create_unsorted_lead_ex", counted_create)
    monkeypatch.setattr(store, "mark_created", delayed_mark)

    async def scenario():
        assert sf.SITE_FORM_UNAVAILABLE_ENABLED is False
        assert (await sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
        worker = asyncio.create_task(sf.run_due())
        assert await asyncio.to_thread(entered.wait, 5)
        assert store.get(SID)["status"] == "processing"
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert store.get(SID)["status"] == "created"
        assert store.get(SID)["lead_id"] == 301
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
        assert store.get(SID)["status"] == "created"
        assert await sf.run_due() == 1

    try:
        asyncio.run(scenario())
    finally:
        release.set()
    assert len(creates) == 1
    assert store.get(SID)["status"] == "done"


def test_late_mark_done_after_cancel_cannot_override_uncertain(v2, monkeypatch):
    monkeypatch.setattr(sf, "SITE_FORM_UNAVAILABLE_ENABLED", True)
    _mock_api_v2(monkeypatch)
    original_mark = store.mark_done
    entered, release, finished = Event(), Event(), Event()

    def delayed_mark(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(5)
            return original_mark(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(store, "mark_done", delayed_mark)

    async def scenario():
        assert (await sf.accept_v2(_v2_payload("unavailable")))[1]["status"] == "accepted"
        worker = asyncio.create_task(sf.run_due())
        assert await asyncio.to_thread(entered.wait, 5)
        assert store.get(SID)["status"] == "finishing"
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert store.get(SID)["status"] == "uncertain"
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
        assert store.get(SID)["status"] == "uncertain"
        assert await sf.run_due(now=time.time() + 10 ** 6) == 0

    try:
        asyncio.run(scenario())
    finally:
        release.set()


@pytest.mark.parametrize("form_type", ["callback", "consultation"])
def test_precreate_lookup_error_keeps_legacy_retry(v2, monkeypatch, form_type):
    calls = _mock_api_v2(monkeypatch)

    async def failed_lookup(_phone):
        raise ConnectionError("lookup unavailable")

    monkeypatch.setattr(api, "find_contact_id", failed_lookup)
    assert asyncio.run(sf.accept_v2(_v2_payload(form_type)))[1]["status"] == "accepted"
    assert asyncio.run(sf.run_due()) == 1
    row = store.get(SID)
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["next_try_at"] > time.time() + 20
    assert "create" not in calls


def test_v2_created_row_is_finished_without_second_lead(v2, monkeypatch):
    calls = _mock_api_v2(monkeypatch)
    asyncio.run(sf.accept_v2(_v2_payload()))
    store.mark_created(SID, 555, "U-555")
    asyncio.run(sf.run_due())
    assert "create" not in calls
    assert calls["accept"] == ("U-555", 70070982)
    assert store.get(SID)["status"] == "done"


def test_store_purge_keeps_fresh_rows(v2):
    store.insert_pending("aaaaaaaa-0000-4000-8000-000000000001", "test-callback", "{}", now=1000.0)
    store.mark_done("aaaaaaaa-0000-4000-8000-000000000001", 1, now=1000.0)
    store.insert_pending(SID, "test-callback", "{}")
    assert store.purge(7, now=1000.0 + 8 * 86400) == 1
    assert store.get(SID) is not None
