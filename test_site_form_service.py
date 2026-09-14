"""Тесты приёма форм сайта (site_form_service): карта форм, секрет, rate limit,
антидубль и путь заявки unsorted → accept с источником формы. Без сети — api.*
замокан."""

import asyncio
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
        "contact": {"name": "Иван", "phone": "+79099371845", "telegram": "@ivan_test"},
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
    assert sf.normalize_phone_v2("8 (909) 937-18-45") == "+79099371845"
    assert sf.normalize_phone_v2("+7 909 937-18-45") == "+79099371845"
    assert sf.normalize_phone_v2("9099371845") == "+79099371845"
    assert sf.normalize_phone_v2("+375 29 123-45-67") == "+375291234567"
    # без «+» страну не угадываем - человек проверяет номер сам
    assert sf.normalize_phone_v2("375291234567") == ""
    assert sf.normalize_phone_v2("+7 909 937") == ""
    assert sf.normalize_phone_v2("+8 909 937-18-45") == ""
    assert sf.normalize_phone_v2("звоните вечером") == ""


def test_clean_v2_callback(v2):
    clean = sf.clean_v2(_v2_payload())
    assert clean["contact"]["phone"] == "+79099371845"
    assert clean["context"]["utm"] == {"utm_source": "ya", "utm_medium": "cpc"}
    assert clean["context"]["product"]["sku"] == "HW-26"
    assert clean["context"]["service"] is None
    assert clean["context"]["format"] is None


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
    assert "+79099371845" in row["payload"]


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
    assert created["contact"]["custom_fields_values"][0]["values"][0]["value"] == "+79099371845"
    assert calls["find"] == ["+79099371845"]
    assert calls["accept"] == ("U-301", 70070982)
    assert calls["tags"] == (301, ["Форма: обратный звонок", "тест"])
    assert calls["utm"] == (301, {"utm_source": "ya", "utm_medium": "cpc"})

    note = calls["note"][1]
    for piece in (
        "Заявка с сайта: Обратный звонок или вопрос",
        "Заголовок окна: Остались вопросы?",
        "Имя: Иван",
        "Телефон: +79099371845",
        "Telegram: @ivan_test",
        "Вопрос: Какой кошелёк выбрать?",
        "Товар: Keystone 3 Pro, артикул HW-26 - https://test.sunscrypt.ru/product/",
        "Страница: Keystone 3 Pro - https://test.sunscrypt.ru/product/",
        "Кнопка на сайте: test-page-callback",
        "UTM: utm_source=ya, utm_medium=cpc",
        "Пришёл с: https://yandex.ru/",
        "Номер заявки: 3f2b8c1e",
    ):
        assert piece in note
    assert "·" not in note

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
    assert "Заявка с сайта: Запись на консультацию" in note
    assert "Запрос: Какой кошелёк выбрать?" in note
    assert "Консультация: Консультация по безопасности (название пришло со страницы" in note
    assert "Формат: В шоуруме в Москве" in note


def test_v2_existing_contact_linked_by_phone(v2, monkeypatch):
    calls = _mock_api_v2(monkeypatch)

    async def find_contact_id(query):
        calls.setdefault("find", []).append(query)
        return 777

    monkeypatch.setattr(api, "find_contact_id", find_contact_id)
    asyncio.run(sf.accept_v2(_v2_payload()))
    asyncio.run(sf.run_due())
    assert calls["create"]["contact"] == {"id": 777}


def test_v2_amo_down_retries_then_alerts(v2, monkeypatch):
    _mock_api_v2(monkeypatch, lead_id=None)
    sent = []

    async def fake_alert(row, error, attempts):
        sent.append((row["submission_id"][:8], error, attempts))

    monkeypatch.setattr(sf, "_alert_failed", fake_alert)
    asyncio.run(sf.accept_v2(_v2_payload()))
    asyncio.run(sf.run_due())
    row = store.get(SID)
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["last_error"] == "amo-create-failed"
    assert row["next_try_at"] > time.time() + 20

    for _ in range(len(sf.RETRY_DELAYS_S)):
        asyncio.run(sf.run_due(now=time.time() + 10 ** 6))
    row = store.get(SID)
    assert row["status"] == "failed"
    assert row["payload"] is not None  # для ручного разбора, до очистки
    assert sent == [("3f2b8c1e", "amo-create-failed", len(sf.RETRY_DELAYS_S) + 1)]


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
