import asyncio

import academy_assignment as assignment


def run(coro):
    return asyncio.run(coro)


def test_apply_assigns_artem(monkeypatch):
    assignment.ACADEMY_CUTOVER_TS = 100
    async def get_lead(*args, **kwargs):
        return {"id": 10, "pipeline_id": assignment.PIPELINE_ACADEMY, "responsible_user_id": 1, "created_at": 101}

    patches = []
    async def patch(lead_id, **kwargs):
        patches.append((lead_id, kwargs))
        return {"ok": True}

    monkeypatch.setattr(assignment.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(assignment.amo_service, "patch_lead", patch)
    assert run(assignment.apply(10)) == "assigned"
    assert patches == [(10, {"responsible_user_id": assignment.ACADEMY_RESPONSIBLE_USER_ID})]


def test_apply_does_not_rewrite_artem(monkeypatch):
    assignment.ACADEMY_CUTOVER_TS = 100
    async def get_lead(*args, **kwargs):
        return {
            "id": 10,
            "pipeline_id": assignment.PIPELINE_ACADEMY,
            "responsible_user_id": assignment.ACADEMY_RESPONSIBLE_USER_ID,
            "created_at": 101,
        }

    patches = []
    monkeypatch.setattr(assignment.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(assignment.amo_service, "patch_lead", lambda *a, **k: patches.append(1))
    assert run(assignment.apply(10)) == "already_assigned"
    assert patches == []


def test_apply_rejects_historical_lead(monkeypatch):
    assignment.ACADEMY_CUTOVER_TS = 100
    async def get_lead(*args, **kwargs):
        return {"id": 10, "pipeline_id": assignment.PIPELINE_ACADEMY, "responsible_user_id": 1, "created_at": 99}
    patches = []
    monkeypatch.setattr(assignment.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(assignment.amo_service, "patch_lead", lambda *a, **k: patches.append(1))
    assert run(assignment.apply(10)) == "before_cutover"
    assert patches == []


def test_scheduler_accepts_any_academy_stage(monkeypatch):
    assignment.ACADEMY_ASSIGNMENT_ENABLED = True
    assignment.ACADEMY_ASSIGNMENT_DELAY_S = 0
    created = []
    async def apply(lead_id, **kwargs):
        created.append((lead_id, kwargs))
    monkeypatch.setattr(assignment, "apply", apply)

    async def scenario():
        assignment.assign_bg(10, assignment.PIPELINE_ACADEMY, 123)
        await asyncio.gather(*assignment._bg_tasks)

    run(scenario())
    assert len(created) == 1
    assert created[0] == (10, {"delay": 0})
