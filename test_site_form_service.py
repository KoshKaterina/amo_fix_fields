"""Тесты приёма форм сайта (site_form_service): карта форм, секрет, rate limit,
антидубль и путь заявки unsorted → accept с источником формы. Без сети — api.*
замокан."""

import asyncio

import pytest

import api
import site_form_service as sf


@pytest.fixture(autouse=True)
def _clean_state(monkeypatch):
    sf._rate.clear()
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
    assert sf._normalize_phone("8 (909) 937-18-45") == "+79099371845"
    assert sf._normalize_phone("+7 909 937 18 45") == "+79099371845"
    assert sf._normalize_phone("нет цифр") == ""


def test_fallback_phone_and_email_nonstandard_keys(monkeypatch):
    calls = _mock_api(monkeypatch)
    payload = {"form": "svyazatsya", "fields": {
        "imya-klienta-905": "Пётр",
        "tel-905": "8 (909) 937-18-45",
    }}
    assert asyncio.run(sf.process(payload)) == 201
    assert calls["create"]["contact"]["custom_fields_values"][0]["values"][0]["value"] == "+79099371845"

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

    async def add_note_to_lead(lid, text):
        calls["note"] = (lid, text)
        return True

    monkeypatch.setattr(api, "find_contact_id", find_contact_id)
    monkeypatch.setattr(api, "create_unsorted_lead_ex", create_unsorted_lead_ex)
    monkeypatch.setattr(api, "accept_unsorted", accept_unsorted)
    monkeypatch.setattr(api, "add_note_to_lead", add_note_to_lead)
    return calls


def _payload(slug="svyazatsya", **fields):
    base = {"your-name": "Иван", "your-tel": "8 909 937-18-45"}
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
    assert c["contact"]["custom_fields_values"][0]["values"][0]["value"] == "+79099371845"
    assert calls["accept"] == ("u-1", 222)
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


def test_api_accept_unsorted(monkeypatch):
    async def fake_request_json(method, url, body=None, what=""):
        assert url.endswith("/api/v4/leads/unsorted/U1/accept")
        assert body == {"status_id": 42}
        return {"_embedded": {"leads": [{"id": 99}]}}

    monkeypatch.setattr(api, "_request_json", fake_request_json)
    assert asyncio.run(api.accept_unsorted("U1", 42)) == 99
