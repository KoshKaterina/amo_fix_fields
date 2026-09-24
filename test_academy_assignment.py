import asyncio

import academy_assignment as assignment


def run(coro):
    return asyncio.run(coro)


def test_apply_assigns_artem(monkeypatch):
    async def get_lead(*args, **kwargs):
        return {"id": 10, "pipeline_id": assignment.PIPELINE_ACADEMY, "responsible_user_id": 1}

    patches = []
    async def patch(lead_id, **kwargs):
        patches.append((lead_id, kwargs))
        return {"ok": True}

    monkeypatch.setattr(assignment.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(assignment.amo_service, "patch_lead", patch)
    assert run(assignment.apply(10)) == "assigned"
    assert patches == [(10, {"responsible_user_id": assignment.ACADEMY_RESPONSIBLE_USER_ID})]


def test_apply_does_not_rewrite_artem(monkeypatch):
    async def get_lead(*args, **kwargs):
        return {
            "id": 10,
            "pipeline_id": assignment.PIPELINE_ACADEMY,
            "responsible_user_id": assignment.ACADEMY_RESPONSIBLE_USER_ID,
        }

    patches = []
    monkeypatch.setattr(assignment.amo_service, "get_lead_full", get_lead)
    monkeypatch.setattr(assignment.amo_service, "patch_lead", lambda *a, **k: patches.append(1))
    assert run(assignment.apply(10)) == "already_assigned"
    assert patches == []


def test_scheduler_ignores_other_stage(monkeypatch):
    assignment.ACADEMY_ASSIGNMENT_ENABLED = True
    created = []
    monkeypatch.setattr(assignment.asyncio, "create_task", lambda coro: created.append(coro))
    assignment.assign_bg(10, assignment.PIPELINE_ACADEMY, 123)
    assert created == []
