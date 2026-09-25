import asyncio

import academy_bothelp_upsert as mod


def run(coro):
    return asyncio.run(coro)


def payload(**overrides):
    base = {
        "cuid": "7hw4.ddv", "name": "Тест Кат", "phone": "+79250833349",
        "email": "test@sunscrypt.ru", "pd_consent": "да", "marketing_consent": "да",
        "опыт_в_инвестициях": "хороший опыт", "размер_капитала": "10000000",
        "зачем_капитал": "хочу разобраться", "Регистрация на мероприятие": "Практикум октябрь 2026",
        "действие менеджера": "записаться на практикум",
    }
    base.update(overrides)
    return base


def test_target_status_progression_and_guard():
    no_event = {"Регистрация на мероприятие": "", "действие менеджера": ""}
    assert mod._target_status(payload(размер_капитала="", зачем_капитал="", **no_event), mod.STATUS_BOT_STARTED) == mod.STATUS_QUESTIONNAIRE
    assert mod._target_status(payload(**no_event), mod.STATUS_QUESTIONNAIRE) == mod.STATUS_QUESTIONNAIRE_DONE
    assert mod._target_status(payload(), mod.STATUS_QUESTIONNAIRE_DONE) is None
    assert mod._target_status(payload(размер_капитала="", зачем_капитал="", **no_event), mod.STATUS_QUESTIONNAIRE_DONE) is None
    assert mod._target_status(payload(), 88835666) is None


def test_payload_fields_maps_answers():
    fields = mod._payload_fields(payload())
    assert fields[mod.FIELD_CUID] == "7hw4.ddv"
    assert fields[mod.FIELD_EXPERIENCE] == "хороший опыт"
    assert fields[mod.FIELD_CAPITAL] == "10000000"
    assert fields[mod.FIELD_PURPOSE] == "хочу разобраться"


def test_process_updates_existing_contact_and_lead(monkeypatch):
    contact = {"id": 10, "custom_fields_values": [], "_embedded": {"leads": [{"id": 20}]}}
    lead = {"id": 20, "pipeline_id": mod.PIPELINE_ACADEMY, "status_id": mod.STATUS_BOT_STARTED, "created_at": 200}
    patched_contacts = []
    patched_leads = []

    async def candidates(_payload): return [contact]
    async def full(_id, with_=()): return contact
    async def leads(_ids): return [lead]
    async def update(cid, name=None, custom_fields_values=None):
        patched_contacts.append((cid, name, custom_fields_values)); return True
    async def patch(lid, **kwargs): patched_leads.append((lid, kwargs)); return {"ok": True}

    monkeypatch.setattr(mod, "configured", lambda: True)
    monkeypatch.setattr(mod, "_candidate_contacts", candidates)
    monkeypatch.setattr(mod, "_matches", lambda c, p: True)
    monkeypatch.setattr(mod.amo_service, "get_contact_by_id", full)
    monkeypatch.setattr(mod.amo_service, "get_leads_by_ids", leads)
    monkeypatch.setattr(mod.api, "update_contact", update)
    monkeypatch.setattr(mod.amo_service, "patch_lead", patch)

    result = run(mod.process(payload()))
    assert result["ok"] is True
    assert patched_contacts[0][0:2] == (10, "Тест Кат")
    assert patched_leads == [(20, {
        "status_id": mod.STATUS_QUESTIONNAIRE_DONE,
        "pipeline_id": mod.PIPELINE_ACADEMY,
    })]


def test_old_flow_registration_moves_stage_without_delivery(monkeypatch):
    stored_contact = {
        "id": 10,
        "custom_fields_values": [
            {"field_id": mod.FIELD_EVENT, "values": [{"value": "Практикум октябрь 2026"}]},
            {"field_id": mod.FIELD_ACTION, "values": [{"value": "связаться с клиентом"}]},
        ],
        "_embedded": {"leads": [{"id": 20}]},
    }
    lead = {"id": 20, "pipeline_id": mod.PIPELINE_ACADEMY,
            "status_id": mod.STATUS_QUESTIONNAIRE_DONE, "created_at": 200}
    lead_reads = iter([
        lead,
        {**lead, "status_id": mod.STATUS_RECORDED_PRACTICUM},
    ])
    patched = []
    scheduled = []

    async def candidates(_payload): return [stored_contact]
    async def contact_read(_id, with_=()): return stored_contact
    async def leads(_ids): return [lead]
    async def update(*_a, **_k): return True
    async def get_lead(*_a, **_k): return next(lead_reads)
    async def patch(lid, **kwargs): patched.append((lid, kwargs)); return {"ok": True}

    monkeypatch.setattr(mod, "configured", lambda: True)
    monkeypatch.setattr(mod, "_candidate_contacts", candidates)
    monkeypatch.setattr(mod, "_matches", lambda c, p: True)
    monkeypatch.setattr(mod.amo_service, "get_contact_by_id", contact_read)
    monkeypatch.setattr(mod.amo_service, "get_leads_by_ids", leads)
    monkeypatch.setattr(mod.api, "update_contact", update)
    monkeypatch.setattr(mod.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(mod.amo_service, "patch_lead", patch)
    monkeypatch.setattr(mod.academy_invite_delivery, "schedule", lambda *a: scheduled.append(a))

    old709 = payload(**{"действие менеджера": "связаться с клиентом"})
    result = run(mod.process(old709))
    assert result["progression"] == "recorded_practicum"
    assert patched == [(20, {
        "status_id": mod.STATUS_RECORDED_PRACTICUM,
        "pipeline_id": mod.PIPELINE_ACADEMY,
    })]
    assert scheduled == []


def test_old_flow_trusted_node_evidence_schedules_after_stage_readback(monkeypatch):
    stored_contact = {
        "id": 10,
        "custom_fields_values": [
            {"field_id": mod.FIELD_EVENT, "values": [{"value": "Практикум октябрь 2026"}]},
            {"field_id": mod.FIELD_ACTION, "values": [{"value": "связаться с клиентом"}]},
        ],
        "_embedded": {"leads": [{"id": 20}]},
    }
    lead = {"id": 20, "pipeline_id": mod.PIPELINE_ACADEMY,
            "status_id": mod.STATUS_QUESTIONNAIRE_DONE, "created_at": 200}
    lead_reads = iter([lead, {**lead, "status_id": mod.STATUS_RECORDED_PRACTICUM}])
    monkeypatch.setattr(mod, "configured", lambda: True)
    monkeypatch.setattr(mod, "_candidate_contacts", lambda _p: _async([stored_contact]))
    monkeypatch.setattr(mod, "_matches", lambda *_a: True)
    monkeypatch.setattr(mod.amo_service, "get_contact_by_id", lambda *_a, **_k: _async(stored_contact))
    monkeypatch.setattr(mod.amo_service, "get_leads_by_ids", lambda *_a: _async([lead]))
    monkeypatch.setattr(mod.api, "update_contact", lambda *_a, **_k: _async(True))
    monkeypatch.setattr(mod.amo_service, "get_lead_full", lambda *_a, **_k: _async(next(lead_reads)))
    monkeypatch.setattr(mod.amo_service, "patch_lead", lambda *_a, **_k: _async({"ok": True}))
    scheduled = []
    monkeypatch.setattr(mod.academy_invite_delivery, "schedule", lambda *a: scheduled.append(a))
    old = payload(**{
        "действие менеджера": "связаться с клиентом",
        "academy_intent_ref": mod._LEGACY_PRACTICUM_INTENT_REF,
    })
    assert run(mod.process(old))["progression"] == "recorded_practicum"
    assert scheduled == [(old, 20)]


def test_non_registration_keeps_questionnaire_progression():
    assert mod._is_forward_only_practicum(payload(**{
        "Регистрация на мероприятие": "",
        "действие менеджера": "",
    })) is False
    assert mod._target_status(payload(**{
        "Регистрация на мероприятие": "",
        "действие менеджера": "",
    }), mod.STATUS_QUESTIONNAIRE) == mod.STATUS_QUESTIONNAIRE_DONE


async def _async(value):
    return value


def test_unread_candidate_contact_never_creates_or_updates(monkeypatch):
    async def forbidden(*args, **kwargs):
        raise AssertionError("An uncertain lookup must not write")

    monkeypatch.setattr(mod, "configured", lambda: True)
    monkeypatch.setattr(mod, "_candidate_contacts", lambda _: _async([{"id": 10}]))
    monkeypatch.setattr(mod.amo_service, "get_contact_by_id", lambda *a, **k: _async(None))
    for name in ("create_contact", "create_lead_direct", "update_contact"):
        monkeypatch.setattr(mod.api, name, forbidden)
    assert run(mod.process(payload())) == {"ok": False, "reason": "search_failed"}


def test_incomplete_linked_leads_never_creates_or_updates(monkeypatch):
    contact = {"id": 10, "_embedded": {"leads": [{"id": 20}, {"id": 21}]}}

    async def forbidden(*args, **kwargs):
        raise AssertionError("An uncertain lookup must not write")

    monkeypatch.setattr(mod, "configured", lambda: True)
    monkeypatch.setattr(mod, "_candidate_contacts", lambda _: _async([contact]))
    monkeypatch.setattr(mod, "_matches", lambda *a: True)
    monkeypatch.setattr(mod.amo_service, "get_contact_by_id", lambda *a, **k: _async(contact))
    for name in ("create_contact", "create_lead_direct", "update_contact"):
        monkeypatch.setattr(mod.api, name, forbidden)
    # Both a failed batch and a partly returned batch must fail closed, even
    # when a visible row is closed (or an open match was already found).
    for rows in ([], None, [{"id": 20, "pipeline_id": mod.PIPELINE_ACADEMY, "status_id": 143}],
                 [{"id": 20, "pipeline_id": mod.PIPELINE_ACADEMY, "status_id": mod.STATUS_BOT_STARTED}]):
        monkeypatch.setattr(mod.amo_service, "get_leads_by_ids", lambda *a: _async(rows))
        assert run(mod.process(payload())) == {"ok": False, "reason": "search_failed"}


def test_simultaneous_deliveries_are_serialized(monkeypatch):
    active = 0
    max_active = 0

    async def unlocked(item):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return {"ok": True, "cuid": item["cuid"]}

    async def exercise():
        monkeypatch.setattr(mod, "_process_unlocked", unlocked)
        return await asyncio.gather(
            mod.process({"cuid": "first"}),
            mod.process({"cuid": "second"}),
        )

    assert run(exercise()) == [
        {"ok": True, "cuid": "first"},
        {"ok": True, "cuid": "second"},
    ]
    assert max_active == 1
