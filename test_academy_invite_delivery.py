import asyncio
import json

import httpx

import academy_invite_delivery as mod


def run(coro): return asyncio.run(coro)


def payload(**overrides):
    data = {"first_name": "Екатерина", "phone": "+79250833349",
            "Регистрация на мероприятие": "Практикум октябрь 2026",
            "действие менеджера": "записаться на практикум"}
    data.update(overrides)
    return data


def enable(monkeypatch, tmp_path):
    monkeypatch.setattr(mod, "ACADEMY_INVITE_SEND_ENABLED", True)
    monkeypatch.setattr(mod, "WAZZUP_API_KEY", "key")
    monkeypatch.setattr(mod, "ACADEMY_INVITE_WAZZUP_CHANNEL_ID", "exact")
    monkeypatch.setattr(mod, "ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID", "79250833349")
    monkeypatch.setattr(mod, "ACADEMY_INVITE_SENT_PATH", str(tmp_path / "sent.json"))


def test_message_uses_approved_copy(monkeypatch):
    monkeypatch.setattr(mod, "ACADEMY_MANAGER_FIRST_NAME", "Артем")
    assert "по ссылке: https://t.me/+one-use" in mod._message(payload(), "https://t.me/+one-use")


def test_registration_without_explicit_cta_is_fail_closed(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    called = []
    async def get_lead(*_a, **_k): called.append(1)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    assert run(mod.process(payload(**{"действие менеджера": "связаться с клиентом"}), 10)) == "not_explicit_request"
    assert called == []


def test_english_manager_action_alias_is_exact():
    assert mod._is_explicit_request({"manager_action": " Записаться на практикум "}) is True
    assert mod._is_explicit_request({"manager_action": "узнать об участии"}) is False


def test_process_rereads_then_sends_exact_channel_once(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    reads = iter([
        {"status_id": 88838386, "custom_fields_values": []},
        {"status_id": 88838386, "custom_fields_values": [{"field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK,
                                                             "values": [{"value": "https://t.me/+one-use"}]}]},
        {"status_id": mod.STATUS_ACADEMY_RECORDED_PRACTICUM,
         "custom_fields_values": [{"field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK,
                                     "values": [{"value": "https://t.me/+one-use"}]}]},
    ])
    async def get_lead(*_a, **_k): return next(reads)
    async def create(*_a, **_k): return "written"
    async def exact(): return True
    patched = []
    async def patch(*args, **kwargs): patched.append((args, kwargs)); return {"ok": True}
    calls = []
    async def request(method, path, *, body=None):
        calls.append((method, path, body))
        return httpx.Response(201, request=httpx.Request(method, "https://example.test"))
    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(mod.academy_invite_link, "process_lead", create)
    monkeypatch.setattr(mod.amo_service, "patch_lead", patch)
    monkeypatch.setattr(mod, "_channel_is_exact", exact)
    monkeypatch.setattr(mod, "_request", request)
    assert run(mod.process(payload(), 10)) == "sent"
    assert calls[0][2]["channelId"] == "exact"
    assert calls[0][2]["crmMessageId"] == "academy-practicum-10"
    assert patched[0][1]["status_id"] == mod.STATUS_ACADEMY_RECORDED_PRACTICUM
    assert run(mod.process(payload(), 10)) == "already_sent"
    assert json.loads((tmp_path / "sent.json").read_text()) == ["wazzup:10"]


def test_empty_readback_is_fail_closed(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    async def get_lead(*_a, **_k): return {"custom_fields_values": []}
    async def create(*_a, **_k): return "written"
    called = []
    async def send(*_a): called.append(1)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(mod.academy_invite_link, "process_lead", create)
    monkeypatch.setattr(mod, "_send", send)
    assert run(mod.process(payload(), 10)) == "link_missing"
    assert called == []


def test_final_stage_readback_is_fail_closed(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    lead = {"status_id": 88838386, "custom_fields_values": [
        {"field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK,
         "values": [{"value": "https://t.me/+one-use"}]},
    ]}
    async def get_lead(*_a, **_k): return lead
    async def patch(*_a, **_k): return {"ok": True}
    called = []
    async def send(*_a): called.append(1)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(mod.amo_service, "patch_lead", patch)
    monkeypatch.setattr(mod, "_send", send)
    assert run(mod.process(payload(), 10)) == "stage_or_link_unconfirmed"
    assert called == []


def test_stage_patch_failure_is_fail_closed(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    lead = {"status_id": 88838386, "custom_fields_values": [
        {"field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK,
         "values": [{"value": "https://t.me/+one-use"}]},
    ]}
    async def get_lead(*_a, **_k): return lead
    async def patch(*_a, **_k): return {"ok": False}
    called = []
    async def send(*_a): called.append(1)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(mod.amo_service, "patch_lead", patch)
    monkeypatch.setattr(mod, "_send", send)
    assert run(mod.process(payload(), 10)) == "stage_error"
    assert called == []


def test_changed_link_after_stage_is_fail_closed(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    before = {"status_id": 88838386, "custom_fields_values": [
        {"field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK,
         "values": [{"value": "https://t.me/+one-use"}]},
    ]}
    after = {"status_id": mod.STATUS_ACADEMY_RECORDED_PRACTICUM,
             "custom_fields_values": [{"field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK,
                                         "values": [{"value": "https://t.me/+changed"}]}]}
    reads = iter([before, after])
    async def get_lead(*_a, **_k): return next(reads)
    async def patch(*_a, **_k): return {"ok": True}
    called = []
    async def send(*_a): called.append(1)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(mod.amo_service, "patch_lead", patch)
    monkeypatch.setattr(mod, "_send", send)
    assert run(mod.process(payload(), 10)) == "stage_or_link_unconfirmed"
    assert called == []


def test_wrong_channel_fails_closed(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    async def request(method, path, *, body=None):
        data = [{"channelId": "other", "plainId": "79250833349", "state": "active", "transport": "tgapi"}]
        return httpx.Response(200, json=data, request=httpx.Request(method, "https://example.test"))
    monkeypatch.setattr(mod, "_request", request)
    assert run(mod._channel_is_exact()) is False
