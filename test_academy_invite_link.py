import asyncio

import academy_invite_link as invite


def run(coro):
    return asyncio.run(coro)


def setup_function():
    invite.ACADEMY_INVITE_LINK_ENABLED = True
    invite.ACADEMY_INVITE_BOT_TOKEN = "test-token"
    invite.ACADEMY_PRACTICUM_CHAT_ID = "-100111"
    invite.ACADEMY_CONFERENCE_CHAT_ID = "-100222"
    invite.ACADEMY_CUTOVER_TS = 100
    invite._locks.clear()


def lead(*, pipeline=None, link=None):
    custom = []
    if link:
        custom.append({
            "field_id": invite.FIELD_ACADEMY_PRACTICUM_LINK,
            "values": [{"value": link}],
        })
    return {
        "id": 77,
        "created_at": 101,
        "pipeline_id": pipeline or invite.PIPELINE_ACADEMY,
        "custom_fields_values": custom,
        "_embedded": {"contacts": [{"id": 88, "is_main": True}]},
    }


def contact(registration="Практикум октябрь 2026"):
    return {
        "id": 88,
        "custom_fields_values": [{
            "field_id": invite.FIELD_ACADEMY_EVENT_REGISTRATION,
            "values": [{"value": registration}],
        }],
    }


def test_disabled_is_noop(monkeypatch):
    invite.ACADEMY_INVITE_LINK_ENABLED = False
    called = []
    monkeypatch.setattr(invite.amo_service, "get_lead_full", lambda *a, **k: called.append(1))
    assert run(invite.process_lead(77)) == "disabled"
    assert called == []


def test_practicum_writes_one_use_link(monkeypatch):
    async def get_lead(*args, **kwargs):
        return lead()

    async def get_contact(*args, **kwargs):
        return contact()

    created = []
    async def create(chat_id, lead_id):
        created.append((chat_id, lead_id))
        return "https://t.me/+one-use"

    patched = []
    async def patch(lead_id, **kwargs):
        patched.append((lead_id, kwargs))
        return {"ok": True}

    monkeypatch.setattr(invite.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(invite.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(invite.amo_service, "patch_lead", patch)
    monkeypatch.setattr(invite, "_create_link", create)

    assert run(invite.process_lead(77)) == "written"
    assert created == [("-100111", 77)]
    assert patched == [(77, {"custom_fields": {
        invite.FIELD_ACADEMY_PRACTICUM_LINK: "https://t.me/+one-use",
    }})]


def test_existing_link_does_not_create_another(monkeypatch):
    async def get_lead(*args, **kwargs):
        return lead(link="https://t.me/+existing")

    async def get_contact(*args, **kwargs):
        return contact()

    called = []
    async def create(*args, **kwargs):
        called.append(1)

    monkeypatch.setattr(invite.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(invite.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(invite, "_create_link", create)

    assert run(invite.process_lead(77)) == "already_filled"
    assert called == []


def test_historical_lead_does_not_create_link(monkeypatch):
    async def get_lead(*args, **kwargs):
        old = lead()
        old["created_at"] = 99
        return old

    created = []
    async def create(*args, **kwargs):
        created.append(1)

    monkeypatch.setattr(invite.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(invite, "_create_link", create)

    assert run(invite.process_lead(77)) == "before_cutover"
    assert created == []


def test_historical_lead_can_be_explicitly_backfilled(monkeypatch):
    old = lead()
    old["created_at"] = 99
    async def get_lead(*args, **kwargs): return old
    async def get_contact(*args, **kwargs): return contact()
    async def create(*args, **kwargs): return "https://t.me/+backfill"
    async def patch(*args, **kwargs): return {"ok": True}
    monkeypatch.setattr(invite.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(invite.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(invite.amo_service, "patch_lead", patch)
    monkeypatch.setattr(invite, "_create_link", create)
    assert run(invite.process_lead(77, allow_historical=True)) == "written"


def test_patch_failure_revokes_orphan(monkeypatch):
    async def get_lead(*args, **kwargs):
        return lead()

    async def get_contact(*args, **kwargs):
        return contact()

    async def create(*args, **kwargs):
        return "https://t.me/+orphan"

    async def patch(*args, **kwargs):
        return {"ok": False}

    revoked = []
    async def revoke(chat_id, link):
        revoked.append((chat_id, link))

    monkeypatch.setattr(invite.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(invite.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(invite.amo_service, "patch_lead", patch)
    monkeypatch.setattr(invite, "_create_link", create)
    monkeypatch.setattr(invite, "_revoke_link", revoke)

    assert run(invite.process_lead(77)) == "patch_error"
    assert revoked == [("-100111", "https://t.me/+orphan")]


def test_contact_change_follows_linked_leads(monkeypatch):
    linked = contact()
    linked["_embedded"] = {"leads": [{"id": 77}, {"id": 78}]}

    async def get_contact(*args, **kwargs):
        return linked

    processed = []
    async def process(lead_id, **kwargs):
        processed.append((lead_id, kwargs.get("contact", {}).get("id")))
        return "written" if lead_id == 77 else "already_filled"

    monkeypatch.setattr(invite.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(invite, "process_lead", process)

    assert run(invite.process_contact(88)) == "written"
    assert processed == [(77, 88), (78, 88)]


def test_conference_uses_separate_chat_and_field(monkeypatch):
    async def get_lead(*args, **kwargs):
        return lead()

    async def get_contact(*args, **kwargs):
        return contact("Конференция ноябрь 2026")

    created = []
    async def create(chat_id, lead_id):
        created.append(chat_id)
        return "https://t.me/+conference"

    patched = []
    async def patch(lead_id, **kwargs):
        patched.append(kwargs["custom_fields"])
        return {"ok": True}

    monkeypatch.setattr(invite.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(invite.amo_service, "get_contact_by_id", get_contact)
    monkeypatch.setattr(invite.amo_service, "patch_lead", patch)
    monkeypatch.setattr(invite, "_create_link", create)

    assert run(invite.process_lead(77)) == "written"
    assert created == ["-100222"]
    assert patched == [{invite.FIELD_ACADEMY_CONFERENCE_LINK: "https://t.me/+conference"}]
