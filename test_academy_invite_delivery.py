import asyncio
import json
import time

import httpx

import academy_invite_delivery as mod


def run(coro):
    return asyncio.run(coro)


def payload(**overrides):
    data = {
        "cuid": "7hw4.test", "name": "Екатерина", "phone": "+79250833349",
        "telegram_id": "123456789", "messenger_username": "katya_test",
        "Регистрация на мероприятие": "Практикум октябрь 2026",
        "действие менеджера": "записаться на практикум",
    }
    data.update(overrides)
    return data


def enable(monkeypatch, tmp_path):
    monkeypatch.setattr(mod, "ACADEMY_INVITE_SEND_ENABLED", True)
    monkeypatch.setattr(mod, "WAZZUP_API_KEY", "key")
    monkeypatch.setattr(mod, "ACADEMY_INVITE_WAZZUP_CHANNEL_ID", "exact")
    monkeypatch.setattr(mod, "ACADEMY_INVITE_WAZZUP_CHANNEL_PLAIN_ID", "79250833349")
    monkeypatch.setattr(mod, "ACADEMY_INVITE_OUTBOX_PATH", str(tmp_path / "outbox.sqlite3"))
    monkeypatch.setattr(mod, "ACADEMY_INVITE_HISTORY_REVIEW_PATH", str(tmp_path / "reviews.json"))
    monkeypatch.setattr(mod, "ACADEMY_INVITE_SENT_PATH", str(tmp_path / "legacy-sent.json"))
    monkeypatch.setattr(mod, "ACADEMY_WAZZUP_HISTORY_TOKEN", "")
    monkeypatch.setattr(
        mod.academy_invite_link, "verify_practicum_link", lambda *_a, **_k: _async(True),
    )


def lead(status=88838386, link="https://t.me/+one-use"):
    fields = [] if link is None else [{
        "field_id": mod.FIELD_ACADEMY_PRACTICUM_LINK, "values": [{"value": link}],
    }]
    return {
        "id": 10, "pipeline_id": mod.PIPELINE_ACADEMY, "status_id": status,
        "responsible_user_id": 13946318, "custom_fields_values": fields,
        "_embedded": {"contacts": [{"id": 7, "is_main": True}]},
    }


def test_message_uses_fresh_manager_first_name():
    text = mod._message(payload(), "https://t.me/+one-use", "Кирилл")
    assert "Меня зовут Кирилл" in text
    assert "по ссылке: https://t.me/+one-use" in text


def test_explicit_intent_requires_exact_action_or_trusted_old_ref():
    assert mod._is_explicit_request(payload())
    old = payload(**{"действие менеджера": "связаться с клиентом"})
    assert not mod._is_explicit_request(old)
    old["academy_intent_ref"] = mod._LEGACY_PRACTICUM_INTENT_REF
    assert mod._is_explicit_request(old)
    assert mod._is_explicit_request({"_intent_source": "manual_stage"})


def test_manual_review_is_exact_and_short_lived(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    review = {"reviews": [{
        "lead_id": 10, "recipient": "79250833349", "channel_id": "exact",
        "plain_id": "79250833349", "source": "wazzup_ui",
        "history_absent": True, "expires_at": time.time() + 60,
    }]}
    (tmp_path / "reviews.json").write_text(json.dumps(review))
    assert mod._manual_history_review(10, "79250833349")
    review["reviews"][0]["expires_at"] = time.time() - 1
    (tmp_path / "reviews.json").write_text(json.dumps(review))
    assert not mod._manual_history_review(10, "79250833349")


def test_csv_history_requires_known_recipient_schema():
    assert mod._csv_history_for_recipient("text,status\nhello,sent\n", {"phone:79250833349"}) is None
    csv = "recipient_phone,text\n79250833349,hello\n"
    assert mod._csv_history_for_recipient(csv, {"phone:79250833349"}) is True
    assert mod._csv_history_for_recipient(csv, {"phone:79990000000"}) is False
    numeric = "chatId,text\n123456789,hello\n"
    assert mod._csv_history_for_recipient(numeric, {"telegram:123456789"}) is True


def test_process_history_unverified_mutates_nothing(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", lambda *_a, **_k: _async(lead()))
    monkeypatch.setattr(mod, "_hydrate", lambda *_a, **_k: _async((payload(), "79250833349")))
    monkeypatch.setattr(mod, "_history_gate", lambda *_a, **_k: _async("unverified"))
    called = []
    monkeypatch.setattr(mod.academy_invite_link, "process_lead", lambda *_a, **_k: called.append(1))
    assert run(mod.process(payload(), 10)) == "history_unverified"
    assert called == []


def test_any_exact_channel_history_skips_before_link(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", lambda *_a, **_k: _async(lead()))
    monkeypatch.setattr(mod, "_hydrate", lambda *_a, **_k: _async((payload(), "79250833349")))
    monkeypatch.setattr(mod, "_history_gate", lambda *_a, **_k: _async("found"))
    assert run(mod.process(payload(), 10)) == "skipped_history"


def test_full_chain_accepts_then_webhook_delivers_without_duplicate(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    reads = iter([lead(), lead(status=mod.STATUS_ACADEMY_RECORDED_PRACTICUM)])
    monkeypatch.setattr(mod.amo_service, "get_lead_full", lambda *_a, **_k: _async(next(reads)))
    monkeypatch.setattr(mod, "_hydrate", lambda *_a, **_k: _async((payload(), "79250833349")))
    monkeypatch.setattr(mod, "_history_gate", lambda *_a, **_k: _async("clear"))
    patched = []
    async def patch(*args, **kwargs):
        patched.append((args, kwargs)); return {"ok": True}
    monkeypatch.setattr(mod.amo_service, "patch_lead", patch)
    sent = []
    async def send(data, lead_id, link, manager):
        sent.append((data, lead_id, link, manager)); return "accepted", "msg-1"
    monkeypatch.setattr(mod, "_send", send)
    assert run(mod.process(payload(), 10)) == "accepted"
    assert patched[0][1]["status_id"] == mod.STATUS_ACADEMY_RECORDED_PRACTICUM
    assert sent[0][3] == "Кирилл"
    assert run(mod.process(payload(), 10)) == "accepted"
    assert len(sent) == 1
    mod.record_webhook({"statuses": [{"messageId": "msg-1", "status": "delivered"}]})
    with mod._db() as conn:
        assert conn.execute("SELECT state FROM invite_job WHERE lead_id=10").fetchone()[0] == "delivered"


def test_stage_patch_failure_never_calls_send(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", lambda *_a, **_k: _async(lead()))
    monkeypatch.setattr(mod, "_hydrate", lambda *_a, **_k: _async((payload(), "79250833349")))
    monkeypatch.setattr(mod, "_history_gate", lambda *_a, **_k: _async("clear"))
    monkeypatch.setattr(mod.amo_service, "patch_lead", lambda *_a, **_k: _async({"ok": False}))
    called = []
    monkeypatch.setattr(mod, "_send", lambda *_a, **_k: called.append(1))
    assert run(mod.process(payload(), 10)) == "stage_error"
    assert called == []


def test_protected_stage_is_blocked_before_history_or_link(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", lambda *_a, **_k: _async(lead(status=88464042)))
    called = []
    monkeypatch.setattr(mod, "_history_gate", lambda *_a, **_k: called.append(1))
    assert run(mod.process(payload(), 10)) == "stage_guard"
    assert called == []


def test_accepted_error_is_terminal_no_automatic_retry(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    mod._enqueue(payload(), 10, delay=0)
    mod._mark_message(10, "msg-error")
    mod.record_webhook({"messages": [{"messageId": "msg-error", "status": "error", "error": {"code": "gone"}}]})
    with mod._db() as conn:
        row = conn.execute("SELECT state,last_error FROM invite_job WHERE lead_id=10").fetchone()
    assert row[0] == "delivery_error" and "gone" in row[1]
    assert run(mod.process(payload(), 10)) == "delivery_error"


def test_positive_wazzup_status_adds_sent_tag_idempotently(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    mod._enqueue(payload(), 10, delay=0)
    mod._mark_message(10, "msg-sent")
    calls = []
    async def add_tag(lead_id, tag):
        calls.append((lead_id, tag)); return {"ok": True}
    monkeypatch.setattr(mod.amo_service, "add_tag", add_tag)
    # HTTP 201 alone left the row accepted and must not add the business tag.
    assert calls == []
    mod.record_webhook({"statuses": [{"messageId": "msg-sent", "status": "sent"}]})
    run(mod._run_due_once())
    assert calls == [(10, "ссылка отправлена")]
    run(mod._run_due_once())
    assert calls == [(10, "ссылка отправлена")]


def test_failed_wazzup_status_never_adds_sent_tag(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    mod._enqueue(payload(), 10, delay=0)
    mod._mark_message(10, "msg-failed")
    calls = []
    monkeypatch.setattr(mod.amo_service, "add_tag", lambda *_a: calls.append(1))
    mod.record_webhook({"statuses": [{"messageId": "msg-failed", "status": "error"}]})
    run(mod._run_due_once())
    assert calls == []


def test_orphan_delivery_status_before_send_response_is_preserved(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    mod._enqueue(payload(), 10, delay=0)
    mod.record_webhook({"statuses": [{"messageId": "early", "status": "delivered"}]})
    mod._mark_message(10, "early")
    with mod._db() as conn:
        assert conn.execute("SELECT state FROM invite_job WHERE lead_id=10").fetchone()[0] == "delivered"


def test_legacy_sent_ledger_is_read_only_and_blocks_replay(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    legacy = tmp_path / "legacy-sent.json"
    legacy.write_text('["wazzup:10", "wazzup:11"]')
    before = legacy.read_bytes()
    assert run(mod.process(payload(), 10)) == "already_sent"
    assert legacy.read_bytes() == before


def test_duplicate_cards_share_recipient_claim(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path)
    mod._enqueue(payload(), 10, delay=0)
    mod._enqueue(payload(), 11, delay=0)
    assert mod._claim_recipient(10, "phone:79250833349")
    assert not mod._claim_recipient(11, "phone:79250833349")


def test_identity_never_overwrites_conflicting_wazzup_id(monkeypatch):
    contact = {"custom_fields_values": [
        {"field_id": mod.FIELD_PHONE, "values": [{"value": "+79250833349"}]},
        {"field_id": mod._FIELD_WAZZUP_TELEGRAM_ID, "values": [{"value": "111"}]},
    ]}
    assert run(mod._ensure_wazzup_identity(7, contact, payload(telegram_id="222"), "79250833349")) == "telegram_identity_conflict"


def test_identity_patch_requires_phone_and_cuid_then_readback(monkeypatch):
    contact = {"custom_fields_values": [
        {"field_id": mod.FIELD_PHONE, "values": [{"value": "+79250833349"}]},
        {"field_id": mod._FIELD_BOTHELP_TELEGRAM_ID, "values": [{"value": "123456789"}]},
    ]}
    patched = []
    async def patch(*_a, **kwargs): patched.append(kwargs); return {"ok": True}
    fresh = {"custom_fields_values": [{
        "field_id": mod._FIELD_WAZZUP_TELEGRAM_ID, "values": [{"value": "123456789"}],
    }]}
    monkeypatch.setattr(mod.amo_service, "patch_contact", patch)
    monkeypatch.setattr(mod.amo_service, "get_contact_by_id", lambda *_a, **_k: _async(fresh))
    assert run(mod._ensure_wazzup_identity(7, contact, payload(), "79250833349")) == "verified"
    assert patched == [{"custom_fields": {mod._FIELD_WAZZUP_TELEGRAM_ID: "123456789"}}]


def test_manual_stage_requires_recent_exact_event(monkeypatch):
    event = {"entity_id": 10, "value_after": [{"lead_status": {
        "id": mod.STATUS_ACADEMY_RECORDED_PRACTICUM, "pipeline_id": mod.PIPELINE_ACADEMY,
    }}]}
    monkeypatch.setattr(mod.amo_service, "_do_get", lambda *_a, **_k: _async({"_embedded": {"events": [event]}}))
    assert run(mod._manual_stage_event_confirmed(10, int(time.time())))
    assert not run(mod._manual_stage_event_confirmed(11, int(time.time())))


def test_send_prefers_username_and_201_is_only_accepted(monkeypatch):
    monkeypatch.setattr(mod, "_channel_is_exact", lambda: _async(True))
    captured = []
    async def request(method, path, *, body=None):
        captured.append(body)
        return httpx.Response(201, json={"messageId": "m1"}, request=httpx.Request(method, "https://x"))
    monkeypatch.setattr(mod, "_request", request)
    data = payload(_verified_username="katya_test")
    assert run(mod._send(data, 10, "https://t.me/+x", "Артём")) == ("accepted", "m1")
    assert captured[0]["username"] == "katya_test"
    assert "phone" not in captured[0] and "chatId" not in captured[0]


def test_send_never_uses_unverified_username_or_raw_telegram_id(monkeypatch):
    monkeypatch.setattr(mod, "_channel_is_exact", lambda: _async(True))
    captured = []
    async def request(method, path, *, body=None):
        captured.append(body)
        return httpx.Response(201, json={"messageId": "m2"}, request=httpx.Request(method, "https://x"))
    monkeypatch.setattr(mod, "_request", request)
    assert run(mod._send(payload(), 10, "https://t.me/+x", "Артём")) == ("accepted", "m2")
    assert captured[0]["phone"] == "79250833349"
    assert "username" not in captured[0] and "chatId" not in captured[0]


async def _async(value):
    return value
