"""Тесты чтения настроек уведомлений из панели (alerts.py, alert_templates.py,
alert_settings_client.py).

Что тут главное:
  • флаг off - сендер получает ровно то, что принёс: текст, чат, режим разметки. Это гарантия,
    что выкатка с выключенным флагом не меняет ни одного боевого уведомления;
  • флаг on - текст по шаблону панели, чат по ключу канала, получатели по режиму; выключенное
    событие не шлётся; неизвестная переменная - старый текст, а не дыра;
  • фикстура `test_fixtures/alert_settings_panel.json` - живой документ панели (снят с
    `settings_for_integration` 12.09.2026): каждая переменная каждого шаблона должна быть среди
    тех, что сендер реально передаёт (карта VALUES_BY_EVENT ниже). Разошлись - тест красный
    ДО того, как прод покажет старый текст вместо настроенного.

Запуск: python3 -m pytest test_alerts.py -q
"""

import json
import os
import sys

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")
os.environ.setdefault("ROP_ALERT_CHAT_ID", "-100777")
os.environ.setdefault("OZON_STALE_ESCALATE_CHAT_ID", "-100778")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import alert_settings_client as settings_client  # noqa: E402
import alert_templates  # noqa: E402
import alerts  # noqa: E402
import tg_recipients  # noqa: E402
import waybill_config  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_fixtures", "alert_settings_panel.json")

# Что каждый сендер кладёт в values (по коду на 12.09.2026). Шаблон панели не имеет права
# просить больше. Обновляй вместе с сендером.
VALUES_BY_EVENT = {
    "missed_call": {"теги", "телефон", "клиент", "ссылка_на_сделку"},
    "missed_call_no_callback": {"сколько_ждали", "ответственный", "клиент", "телефон", "ссылка_на_сделку"},
    "wazzup_no_reply": {"сколько_ждали", "теги", "канал", "клиент", "телефон", "сообщение", "ссылка_на_сделку"},
    "wazzup_no_reply_escalation": {"сколько_ждали", "ответственный", "клиент", "канал", "сообщение", "ссылка_на_сделку"},
    "academy_lead": {"теги", "телефон", "клиент", "сделка", "ссылка_на_сделку"},
    "academy_lead_burst": {"лимит"},
    "showroom_pickup": {"теги", "клиент", "состав", "доставка", "сумма", "ссылка_на_сделку"},
    "ozon_invoice_failed": {"причина", "сделка", "ссылка_на_сделку", "теги"},
    "ozon_invoice_stale": {"сколько_ждали", "статус_оплаты", "сделка", "ссылка_на_сделку", "теги"},
    "ozon_invoice_rejected": {"сколько_ждали", "статус_оплаты", "сделка", "ссылка_на_сделку", "теги"},
    "ozon_invoice_escalation": {"сколько_ждали", "сделка", "ссылка_на_сделку"},
    "office_transfer_stuck": {"сколько_ждали", "сделка", "ссылка_на_сделку", "теги"},
    "office_transfer_bad_fill": {"причина", "сделка", "ссылка_на_сделку", "теги"},
    "lead_not_distributed": {"сколько_ждали", "сделка", "ссылка_на_сделку", "теги"},
    "new_lead_untaken": {"сколько_ждали", "ответственный", "сделка", "ссылка_на_сделку"},
    "autopilot_event": {"текст_события", "теги"},
    "autopilot_failure": {"текст_поломки"},
    "wazzup_undelivered": {"канал", "клиент", "телефон", "отправитель", "тип_сообщения", "ошибка", "сообщение", "ссылка_на_сделку"},
    # Текст этих двух собирает код (keep_text): панель решает только выключатель и чат.
    "order_watchdog_digest": {"сколько_ещё"},
    "order_watchdog_restored": {"номер"},
    "amgroup_duplicate": set(),
    # Технические рапорты интеграции (13.09.2026), по событию на сообщение.
    "cdek_sync_no_pipeline": set(),
    "cdek_sync_stages_missing": {"этапы"},
    "cdek_sync_webhook_failed": {"ошибка"},
    "metrika_sync_counters_failed": {"ошибка"},
    "metrika_sync_counter_ambiguous": {"сколько"},
    "metrika_order_load_failed": {"номер", "ошибка"},
    "woo_status_sync_error": {"номер", "номер_сделки", "ошибка"},
    "queue_lane_depth": {"дорожка", "глубина", "порог", "подробности"},
    "queue_api_depth": {"глубина", "порог", "подробности"},
    "queue_task_waited": {"задача", "номер_сделки", "дорожка", "сколько_ждали", "порог"},
    "ozon_invoice_stages_missing": {"этапы"},
    "office_transfer_targets_missing": {"этапы"},
    "office_transfer_no_since": set(),
    "lead_distribution_no_since": set(),
    "wazzup_undelivered_burst": {"лимит", "окно"},
    # Накладные СДЭК (waybill_service), 13.09.2026.
    "waybill_amo_not_updated": {"номер_сделки", "трек", "ошибка"},
    "waybill_track_cleared_unknown": {"номер_сделки", "трек", "сколько_ждали"},
    "waybill_track_restored": {"номер_сделки", "трек", "сколько_ждали"},
    "waybill_track_not_restored": {"номер_сделки", "трек", "ошибка"},
    "waybill_create_failed": {"номер_сделки", "причина"},
}


@pytest.fixture
def panel_doc():
    with open(FIXTURE, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def mode(monkeypatch):
    def _set(value: str):
        monkeypatch.setattr(waybill_config, "ALERT_SETTINGS_FROM_PANEL", value)
        monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", value)
    yield _set
    settings_client.set_settings_for_tests(None)


def _doc_with(panel_doc, key, **over):
    doc = json.loads(json.dumps(panel_doc))
    doc["events"][key].update(over)
    return doc


LINK = alerts.lead_link(4417)
ACADEMY_VALUES = {
    "теги": "@gladkov_369", "телефон": "+79991234567", "клиент": "Пётр <Иванов>",
    "сделка": "Заказ №4417", "ссылка_на_сделку": LINK,
}


# ── рендер ──────────────────────────────────────────────────────────────────

def test_render_escapes_values_but_not_link():
    text = alert_templates.render("👤 {{клиент}}\n🔗 {{ссылка_на_сделку}}", ACADEMY_VALUES)
    assert "Пётр &lt;Иванов&gt;" in text
    assert LINK in text


def test_render_drops_line_when_all_its_variables_empty():
    text = alert_templates.render("🎓 Лид\n📞 {{телефон}}\n👤 {{клиент}}\n{{теги}}", {"телефон": "", "клиент": "Аня", "теги": None})
    assert text == "🎓 Лид\n👤 Аня"


def test_render_unknown_variable_raises():
    with pytest.raises(alert_templates.UnknownVariable):
        alert_templates.render("{{клиент}} {{погода}}", {"клиент": "Аня"})


def test_render_amo_field_from_lead():
    lead = {"custom_fields_values": [{"field_id": 578137, "values": [{"value": "Самовывоз"}]}]}
    text = alert_templates.render("🚚 {{amo.578137}}\n🏷 {{amo.999}}", {}, lead=lead)
    assert text == "🚚 Самовывоз"           # поля 999 у сделки нет - строка выпала
    assert alert_templates.render("🚚 {{amo.578137}}", {}, lead=None) == ""


def test_render_trims_trailing_separator():
    text = alert_templates.render("💬 {{канал}}, {{клиент}}", {"канал": "Telegram", "клиент": ""})
    assert text == "💬 Telegram"


# ── режимы ──────────────────────────────────────────────────────────────────

def test_off_returns_legacy_untouched(mode, panel_doc):
    mode("off")
    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", enabled=False))
    d = alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES,
                      chat_id=1, thread_id=2, parse_mode=None)
    assert d == alerts.Decision("старый", 1, 2, None, "legacy")


def test_shadow_returns_legacy_even_when_panel_disables(mode, panel_doc, caplog):
    mode("shadow")
    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", enabled=False))
    with caplog.at_level("INFO", logger="uvicorn"):
        d = alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES, chat_id=1, thread_id=2)
    assert d is not None and d.source == "legacy" and d.text == "старый"
    assert any("shadow" in r.getMessage() and "НЕ слать" in r.getMessage() for r in caplog.records)


def test_on_renders_panel_template_and_destination(mode, panel_doc):
    mode("on")
    settings_client.set_settings_for_tests(panel_doc)
    d = alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES, chat_id=1, thread_id=2)
    assert d is not None and d.source == "panel"
    assert d.parse_mode == "HTML"
    assert (d.chat_id, d.thread_id) == (tg_recipients.NOTIFY_CHAT_ID, tg_recipients.NOTIFY_THREAD_ID)
    assert d.text.startswith("🎓 Новый лид в Академии\n")
    assert "📞 +79991234567" in d.text and "👤 Пётр &lt;Иванов&gt;" in d.text and LINK in d.text


def test_on_disabled_event_is_not_sent(mode, panel_doc):
    mode("on")
    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", enabled=False))
    assert alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES) is None


def test_on_unknown_event_or_empty_cache_is_legacy(mode, panel_doc):
    mode("on")
    settings_client.set_settings_for_tests({})
    d = alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES, chat_id=1)
    assert d is not None and d.source == "legacy" and d.text == "старый"
    settings_client.set_settings_for_tests(panel_doc)
    d = alerts.decide("event_from_the_future", legacy_text="старый", values={}, chat_id=1)
    assert d is not None and d.source == "legacy"


def test_on_unknown_variable_falls_back_to_legacy_text_but_panel_chat(mode, panel_doc):
    mode("on")
    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", template="{{погода}} {{теги}}", channel="tech"))
    d = alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES, chat_id=1, thread_id=2, parse_mode="HTML")
    assert d is not None and d.text == "старый" and d.parse_mode == "HTML"
    assert (d.chat_id, d.thread_id) == (None, None)   # tech = адресат по умолчанию


def test_on_recipient_modes(mode, panel_doc):
    mode("on")
    tpl = "x\n{{теги}}\ny"
    doc = _doc_with(panel_doc, "academy_lead", template=tpl, recipients_mode="listed",
                    recipients=[{"name": "Саша", "handle": "@gladkov_369", "amo_user_id": None},
                                {"name": "Без ника", "handle": None, "amo_user_id": None}])
    settings_client.set_settings_for_tests(doc)
    d = alerts.decide("academy_lead", legacy_text="", values={"теги": "@legacy"})
    assert d.text == "x\n@gladkov_369\ny"

    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", template=tpl, recipients_mode="shift"))
    assert alerts.decide("academy_lead", legacy_text="", values={"теги": "@legacy"}).text == f"x\n{tg_recipients.MANAGERS_ON_SHIFT}\ny"

    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", template=tpl, recipients_mode="responsible"))
    assert alerts.decide("academy_lead", legacy_text="", values={"теги": "@legacy"}).text == "x\n@legacy\ny"

    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", template=tpl, recipients_mode="nobody"))
    assert alerts.decide("academy_lead", legacy_text="", values={"теги": "@legacy"}).text == "x\ny"


def test_on_responsible_handle_comes_from_panel_card(mode, panel_doc):
    """«Ответственному»: ник из карточки сотрудника панели; нет его там - теги сендера
    (с его же фолбэком на смену). Тег пустым не остаётся."""
    mode("on")
    tpl = "x\n{{теги}}\ny"
    doc = _doc_with(panel_doc, "missed_call", template=tpl, recipients_mode="responsible")
    doc["people"] = [{"name": "Екатерина Зубалий", "handle": "@kathrina_bistraya",
                      "tg_user_id": None, "amo_user_id": 13963494, "mention": "@kathrina_bistraya"}]
    settings_client.set_settings_for_tests(doc)
    legacy_tags = "@offf1cer @egorkonsss @kathrina_bistraya @thebarsa1"
    d = alerts.decide("missed_call", legacy_text="", values={"теги": legacy_tags}, responsible_id=13963494)
    assert d.text == "x\n@kathrina_bistraya\ny"
    d = alerts.decide("missed_call", legacy_text="", values={"теги": legacy_tags}, responsible_id="13963494")
    assert d.text == "x\n@kathrina_bistraya\ny"
    # Ответственный не из панели (дефолтный на новом лиде) - фолбэк сендера, как сегодня.
    d = alerts.decide("missed_call", legacy_text="", values={"теги": legacy_tags}, responsible_id=777)
    assert d.text == "x\n" + legacy_tags + "\ny"
    d = alerts.decide("missed_call", legacy_text="", values={"теги": legacy_tags}, responsible_id=None)
    assert d.text == "x\n" + legacy_tags + "\ny"


def test_on_listed_without_any_handle_falls_back_to_sender_tags(mode, panel_doc, caplog):
    mode("on")
    tpl = "x\n{{теги}}\ny"
    doc = _doc_with(panel_doc, "academy_lead", template=tpl, recipients_mode="listed",
                    recipients=[{"name": "Без ника", "handle": None, "amo_user_id": None}])
    settings_client.set_settings_for_tests(doc)
    with caplog.at_level("WARNING", logger="uvicorn"):
        d = alerts.decide("academy_lead", legacy_text="", values={"теги": "@gladkov_369"})
    assert d.text == "x\n@gladkov_369\ny"
    assert any("ни у кого нет ника" in r.getMessage() for r in caplog.records)


def test_on_channel_without_chat_is_silent(mode, panel_doc, monkeypatch):
    mode("on")
    monkeypatch.setattr(tg_recipients, "ROP_CHAT_ID", None)
    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", channel="rop"))
    assert alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES) is None
    monkeypatch.setattr(tg_recipients, "ROP_CHAT_ID", -100777)
    d = alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES)
    assert (d.chat_id, d.thread_id) == (-100777, None)


def test_on_keep_text_only_switch_and_channel(mode, panel_doc):
    mode("on")
    settings_client.set_settings_for_tests(_doc_with(panel_doc, "amgroup_duplicate", channel="op_notify"))
    d = alerts.decide("amgroup_duplicate", legacy_text="список пар", values={}, keep_text=True)
    assert d.text == "список пар" and d.chat_id == tg_recipients.NOTIFY_CHAT_ID and d.parse_mode is None


def test_with_line_after_head():
    d = alerts.Decision("⏳ Клиент ждёт\n@x\n🔗 ссылка", 1, 2, "HTML", "panel")
    assert alerts.with_line_after_head(d, "🏬 самовывоз").text == "⏳ Клиент ждёт\n🏬 самовывоз\n@x\n🔗 ссылка"


# ── контракт с панелью ──────────────────────────────────────────────────────

def test_fixture_has_no_chat_ids_and_every_variable_is_provided(panel_doc):
    raw = json.dumps(panel_doc, ensure_ascii=False)
    assert "-100" not in raw, "в документе панели не должно быть номеров чатов"
    missing_events = set(panel_doc["events"]) - set(VALUES_BY_EVENT)
    assert not missing_events, f"события панели без сендера в карте: {missing_events}"
    for key, cfg in panel_doc["events"].items():
        used = set(alert_templates.variables_in(cfg["template"]))
        extra = {v for v in used if not v.startswith("amo.")} - VALUES_BY_EVENT[key]
        assert not extra, f"{key}: шаблон просит {extra}, сендер не даёт"
        assert cfg["channel"] in ("op_notify", "op_showroom", "rop", "ozon_escalation", "tech"), key
        assert cfg["recipients_mode"] in ("responsible", "listed", "shift", "nobody"), key


def test_fixture_default_templates_render_for_every_event(panel_doc, mode):
    """Каждый шаблон по умолчанию собирается без ошибок на правдоподобных значениях и не
    оставляет сырых {{скобок}}."""
    mode("on")
    settings_client.set_settings_for_tests(panel_doc)
    sample = {
        "теги": "@offf1cer", "телефон": "+79991234567", "клиент": "Пётр Иванов", "сделка": "Заказ №4417",
        "ссылка_на_сделку": LINK, "сколько_ждали": "2 часа", "ответственный": "Игорь Оанча",
        "канал": "Telegram", "сообщение": "«есть в наличии?»", "лимит": 20, "состав": "Keystone 3 Pro, 1 шт",
        "доставка": "Самовывоз", "сумма": 15990, "причина": "не заполнен тип доставки",
        "статус_оплаты": "ожидает оплаты", "текст_события": "клиент ответил «да»",
        "текст_поломки": "не смог спросить остаток", "отправитель": "автоматика amo",
        "ошибка": "24_HOURS_EXCEEDED — окно в сутки закрылось", "сколько_ещё": "…и ещё 7",
        "номер": "19003", "этапы": "Оплата запрошена, Счёт выставлен", "сколько": 3,
        "номер_сделки": "36554593", "дорожка": "amo", "глубина": 120, "порог": 100,
        "подробности": "Все дорожки: amo=120, api_queue=3.", "задача": "waybill", "окно": 10,
        "тип_сообщения": "WABA-шаблон", "трек": "10320455561",
    }
    for key in panel_doc["events"]:
        values = {k: sample[k] for k in VALUES_BY_EVENT[key]}
        keep = key in ("order_watchdog_digest", "order_watchdog_restored", "amgroup_duplicate")
        d = alerts.decide(key, legacy_text="старый", values=values, keep_text=keep)
        assert d is not None, key
        if d.source == "panel" and not keep:
            assert "{{" not in d.text and d.text != "старый", (key, d.text)


# ── сквозной прогон сендера под флагом on ───────────────────────────────────

def test_academy_sender_end_to_end_with_panel_settings(mode, panel_doc, monkeypatch):
    """Настоящий сендер Академии с настройками панели: текст по шаблону, получатель из панели
    (а не константа ACADEMY_ALERT_TAG), чат по ключу канала. Телеграм и amo подменены."""
    import types
    sent = []
    tg = types.ModuleType("telegram_bot")

    async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
        sent.append({"text": text, "parse_mode": parse_mode, "chat_id": chat_id, "thread": message_thread_id})
        return True
    tg.send_alert = send_alert
    monkeypatch.setitem(sys.modules, "telegram_bot", tg)

    amo = types.ModuleType("amo_service")
    lead = {"id": 4417, "pipeline_id": None, "status_id": None, "name": "Заказ №4417",
            "_embedded": {"contacts": [{"id": 1, "is_main": True}]},
            "custom_fields_values": [{"field_id": 578137, "values": [{"value": "Самовывоз"}]}]}

    async def get_lead_full(lead_id, with_=()):
        return lead

    async def get_contact_by_id(cid):
        return {"name": "Пётр Иванов", "custom_fields_values": []}
    amo.get_lead_full = get_lead_full
    amo.get_contact_by_id = get_contact_by_id
    amo.get_custom_field_value = lambda entity, fid: "+79991234567"
    monkeypatch.setitem(sys.modules, "amo_service", amo)
    monkeypatch.setitem(sys.modules, "api", types.SimpleNamespace(BASE_URL=alerts.AMO_BASE_URL))

    import importlib
    import academy_lead_alert as academy
    importlib.reload(academy)
    lead["pipeline_id"] = academy.PIPELINE_ACADEMY
    lead["status_id"] = academy.STATUS_ACADEMY_INBOUND_LEAD
    monkeypatch.setattr(academy, "ACADEMY_LEAD_ALERT_DELAY_S", 0)
    monkeypatch.setattr(academy, "_SEEN_PATH", os.path.join(os.path.dirname(FIXTURE), "seen_tmp.json"))

    mode("on")
    doc = _doc_with(panel_doc, "academy_lead",
                    template="🎓 Лид: {{клиент}}, {{amo.578137}}\n{{теги}}\n{{ссылка_на_сделку}}",
                    recipients_mode="listed",
                    recipients=[{"name": "Саша", "handle": "@gladkov_369", "amo_user_id": None}])
    settings_client.set_settings_for_tests(doc)

    import asyncio
    asyncio.run(academy._apply(4417))
    try:
        os.remove(os.path.join(os.path.dirname(FIXTURE), "seen_tmp.json"))
    except OSError:
        pass
    assert len(sent) == 1, sent
    assert sent[0]["text"] == "🎓 Лид: Пётр Иванов, Самовывоз\n@gladkov_369\n" + LINK
    assert sent[0]["parse_mode"] == "HTML"
    assert (sent[0]["chat_id"], sent[0]["thread"]) == (tg_recipients.NOTIFY_CHAT_ID, tg_recipients.NOTIFY_THREAD_ID)


def test_tech_report_helper_under_on(mode, panel_doc, monkeypatch):
    """`_alert(text, event, values)` синка СДЭК: под `on` текст по шаблону панели и чат из
    панели; без ключа события - как раньше, в технический чат."""
    import types, asyncio
    sent = []
    tg = types.ModuleType("telegram_bot")

    async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
        sent.append({"text": text, "chat_id": chat_id, "thread": message_thread_id})
        return True
    tg.send_alert = send_alert
    monkeypatch.setitem(sys.modules, "telegram_bot", tg)
    for name in ("amo_service", "cdek_client"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    import importlib
    import cdek_status_sync
    importlib.reload(cdek_status_sync)

    mode("on")
    settings_client.set_settings_for_tests(_doc_with(
        panel_doc, "cdek_sync_stages_missing",
        template="СДЭК: нет этапов {{этапы}} — синк стоит", channel="op_notify"))
    asyncio.run(cdek_status_sync._alert("старый текст", "cdek_sync_stages_missing", {"этапы": "A, B"}))
    asyncio.run(cdek_status_sync._alert("без ключа - как раньше"))
    assert sent[0]["text"] == "СДЭК: нет этапов A, B — синк стоит"
    assert (sent[0]["chat_id"], sent[0]["thread"]) == (tg_recipients.NOTIFY_CHAT_ID, tg_recipients.NOTIFY_THREAD_ID)
    assert sent[1] == {"text": "без ключа - как раньше", "chat_id": None, "thread": None}


def test_shadow_writes_jsonl_record(mode, panel_doc, monkeypatch, tmp_path):
    """Решение тени дублируется в файл на томе: docker logs пропадает при пересоздании
    контейнера, а файл - нет."""
    import json as _json
    path = tmp_path / "alert_shadow.jsonl"
    monkeypatch.setattr(alerts, "SHADOW_LOG_PATH", path)
    mode("shadow")
    settings_client.set_settings_for_tests(panel_doc)
    alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES, chat_id=1, thread_id=2)
    settings_client.set_settings_for_tests(_doc_with(panel_doc, "academy_lead", enabled=False))
    alerts.decide("academy_lead", legacy_text="старый", values=ACADEMY_VALUES, chat_id=1, thread_id=2)
    rows = [_json.loads(l) for l in path.read_text(encoding="utf-8").splitlines()]
    assert [r["event"] for r in rows] == ["academy_lead", "academy_lead"]
    assert rows[0]["panel"]["chat_id"] == tg_recipients.NOTIFY_CHAT_ID and "Пётр" in rows[0]["panel"]["text"]
    assert rows[0]["legacy"] == {"chat_id": 1, "thread_id": 2, "text": "старый"}
    assert rows[1]["panel"] is None
