import asyncio
import sys
import types

tg = types.ModuleType("telegram_bot")


async def _send_alert(*args, **kwargs):
    return True


tg.send_alert = _send_alert
sys.modules.setdefault("telegram_bot", tg)

import academy_intent_alert as alert
from waybill_config import FIELD_ACADEMY_EVENT_REGISTRATION, FIELD_ACADEMY_MANAGER_ACTION


def run(coro):
    return asyncio.run(coro)


def test_scheduler_ignores_unrelated_field(monkeypatch):
    alert.ACADEMY_INTENT_ALERT_ENABLED = True
    called = []
    monkeypatch.setattr(alert.asyncio, "create_task", lambda coro: called.append(coro))
    alert.on_contact_change(1, {123})
    assert called == []


def test_process_sends_changed_action(monkeypatch):
    alert.ACADEMY_CUTOVER_TS = 100
    contact = {
        "id": 10,
        "name": "Анна",
        "custom_fields_values": [{
            "field_id": FIELD_ACADEMY_MANAGER_ACTION,
            "values": [{"value": "написать менеджеру"}],
        }],
        "_embedded": {"leads": [{"id": 20}]},
    }
    lead = {"id": 20, "pipeline_id": alert.PIPELINE_ACADEMY, "responsible_user_id": 11513202, "created_at": 101}

    async def get_contact(*args, **kwargs): return contact
    async def get_lead(*args, **kwargs): return lead
    sent = []
    async def send(text, **kwargs):
        sent.append((text, kwargs))
        return True

    monkeypatch.setattr(alert.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(alert.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(alert.telegram_bot, "send_alert", send)

    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "sent"
    assert "написать менеджеру" in sent[0][0]
    assert sent[0][0].count("<a href=") == 1
    assert alert.ACADEMY_ALERT_TAG in sent[0][0]
    assert sent[0][1]["chat_id"] == alert.NOTIFY_CHAT_ID
    assert sent[0][1]["message_thread_id"] == alert.NOTIFY_THREAD_ID


def test_empty_changed_value_sends_nothing(monkeypatch):
    alert.ACADEMY_CUTOVER_TS = 100
    contact = {"id": 10, "name": "", "_embedded": {"leads": [{"id": 20}]}}
    lead = {"id": 20, "pipeline_id": alert.PIPELINE_ACADEMY, "created_at": 101}
    async def get_contact(*args, **kwargs): return contact
    async def get_lead(*args, **kwargs): return lead
    sent = []
    async def send(*args, **kwargs): sent.append(1)
    monkeypatch.setattr(alert.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(alert.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(alert.telegram_bot, "send_alert", send)
    assert run(alert.process(10, {FIELD_ACADEMY_EVENT_REGISTRATION})) == "empty_or_failed"
    assert sent == []


def test_historical_lead_sends_nothing(monkeypatch):
    alert.ACADEMY_CUTOVER_TS = 100
    contact = {
        "id": 10,
        "custom_fields_values": [{
            "field_id": FIELD_ACADEMY_MANAGER_ACTION,
            "values": [{"value": "написать менеджеру"}],
        }],
        "_embedded": {"leads": [{"id": 20}]},
    }
    lead = {"id": 20, "pipeline_id": alert.PIPELINE_ACADEMY, "created_at": 99}
    async def get_contact(*args, **kwargs): return contact
    async def get_lead(*args, **kwargs): return lead
    sent = []
    async def send(*args, **kwargs): sent.append(1)
    monkeypatch.setattr(alert.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(alert.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(alert.telegram_bot, "send_alert", send)
    assert run(alert.process(10, {FIELD_ACADEMY_MANAGER_ACTION})) == "no_academy_lead"
    assert sent == []
