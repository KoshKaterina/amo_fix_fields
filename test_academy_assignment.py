"""academy_assignment: сделки из источников со своим правилом распределения (GetCourse)
Артёму не переназначаются, остальные - как раньше."""

import asyncio

import pytest

import academy_assignment as aa

GETCOURSE_SOURCE = 23478671


@pytest.fixture
def lead_store(monkeypatch):
    state = {"lead": None, "patched": [], "with": None}

    async def get_lead_full(lead_id, with_=()):
        state["with"] = with_
        return state["lead"]

    async def patch_lead(lead_id, **kw):
        state["patched"].append(kw)
        return {"ok": True}

    monkeypatch.setattr(aa.amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(aa.amo_service, "patch_lead", patch_lead)
    monkeypatch.setattr(aa, "ACADEMY_CUTOVER_TS", 1)
    monkeypatch.setattr(aa, "ACADEMY_ASSIGNMENT_SKIP_SOURCE_IDS", {GETCOURSE_SOURCE})
    return state


def _lead(source_id):
    lead = {"id": 1, "pipeline_id": aa.PIPELINE_ACADEMY, "created_at": 100, "responsible_user_id": 11513202}
    if source_id:
        lead["_embedded"] = {"source": {"id": source_id, "name": "x"}}
    return lead


def test_getcourse_source_is_left_to_its_rule(lead_store):
    lead_store["lead"] = _lead(GETCOURSE_SOURCE)
    assert asyncio.run(aa.apply(1, expected_responsible_user_id=11513202)) == "skipped_source"
    assert lead_store["patched"] == []
    assert lead_store["with"] == ("source",)


def test_other_source_still_goes_to_artem(lead_store):
    lead_store["lead"] = _lead(23478555)
    assert asyncio.run(aa.apply(1, expected_responsible_user_id=11513202)) == "assigned"
    assert lead_store["patched"] == [{"responsible_user_id": aa.ACADEMY_RESPONSIBLE_USER_ID}]


def test_empty_skip_list_keeps_old_request(lead_store, monkeypatch):
    monkeypatch.setattr(aa, "ACADEMY_ASSIGNMENT_SKIP_SOURCE_IDS", set())
    lead_store["lead"] = _lead(GETCOURSE_SOURCE)
    assert asyncio.run(aa.apply(1, expected_responsible_user_id=11513202)) == "assigned"
    assert lead_store["with"] == ()
