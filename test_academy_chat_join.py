import asyncio

import academy_chat_join as join
from waybill_config import PIPELINE_ACADEMY, STATUS_ACADEMY_JOINED_CHAT

CHAT = "-1004399451029"
USER = 1920391385


def run(coro):
    return asyncio.run(coro)


def enable(monkeypatch, *, dry_run=False, tmp_path=None):
    """Включить фичу и увести состояние в одноразовый файл."""
    monkeypatch.setattr(join, "ACADEMY_CHAT_JOIN_ENABLED", True)
    monkeypatch.setattr(join, "ACADEMY_INVITE_BOT_TOKEN", "token")
    monkeypatch.setattr(join, "ACADEMY_PRACTICUM_CHAT_ID", CHAT)
    monkeypatch.setattr(join, "ACADEMY_CHAT_JOIN_DRY_RUN", dry_run)
    if tmp_path is not None:
        monkeypatch.setattr(
            join, "ACADEMY_CHAT_JOIN_STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setattr(join, "_state", {"offset": 0, "seen": {}})
    monkeypatch.setattr(join, "_state_loaded", False)


def member_event(*, old="left", new="member", chat=CHAT, link_name=None, user_id=USER):
    event = {
        "chat": {"id": int(chat)},
        "old_chat_member": {"status": old, "user": {"id": user_id}},
        "new_chat_member": {"status": new, "user": {"id": user_id, "username": "somebody"}},
    }
    if link_name is not None:
        event["invite_link"] = {"name": link_name}
    return {"update_id": 1, "chat_member": event}


def lead(status_id, lead_id=500, pipeline=PIPELINE_ACADEMY, created_at=100):
    return {"id": lead_id, "pipeline_id": pipeline,
            "status_id": status_id, "created_at": created_at}


# --------------------------------------------------------------- разбор события


def test_is_member_status_reads_restricted_flag():
    assert join.is_member_status({"status": "member"}) is True
    assert join.is_member_status({"status": "administrator"}) is True
    assert join.is_member_status({"status": "left"}) is False
    assert join.is_member_status({"status": "kicked"}) is False
    # «Ограничен» - участник только с флагом is_member.
    assert join.is_member_status({"status": "restricted", "is_member": True}) is True
    assert join.is_member_status({"status": "restricted", "is_member": False}) is False
    assert join.is_member_status(None) is False


def test_lead_id_from_link_name():
    assert join.lead_id_from_link_name("academy lead 36566471") == 36566471
    assert join.lead_id_from_link_name("Academy  lead   42") == 42
    assert join.lead_id_from_link_name("приглашение в чат") is None
    assert join.lead_id_from_link_name(None) is None


def test_leaving_chat_is_not_a_join(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    called = []
    monkeypatch.setattr(join, "process_join", lambda *a, **k: called.append(a))
    assert run(join.handle_update(member_event(old="member", new="left"))) \
        == "ignored_not_a_join"
    # Смена прав внутри чата - тоже не вступление.
    assert run(join.handle_update(member_event(old="member", new="administrator"))) \
        == "ignored_not_a_join"
    assert called == []


def test_other_chat_ignored(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    assert run(join.handle_update(member_event(chat="-100999"))) == "ignored_other_chat"


def test_non_chat_member_update_ignored(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    assert run(join.handle_update({"update_id": 7, "message": {"text": "привет"}})) \
        == "ignored_not_chat_member"


# --------------------------------------------------------------- выбор сделки


def test_pick_lead_prefers_recorded_practicum():
    chosen = join.pick_lead([
        lead(87654850, lead_id=1),
        lead(88835666, lead_id=2),
        lead(88528190, lead_id=3),
    ])
    assert chosen["id"] == 2


def test_pick_lead_skips_forbidden_and_closed_stages():
    # Лист ожидания, «Не трогать», «Не купили DEFI-3», закрытые, и уже на цели.
    assert join.pick_lead([
        lead(70070966, lead_id=1),
        lead(88485802, lead_id=2),
        lead(88943002, lead_id=3),
        lead(143, lead_id=4),
        lead(142, lead_id=5),
        lead(STATUS_ACADEMY_JOINED_CHAT, lead_id=6),
        lead(88464042, lead_id=7),   # «Оплата запрошена» - уже дальше цели
    ]) is None


def test_pick_lead_ignores_other_pipeline():
    assert join.pick_lead([lead(88464034, lead_id=1, pipeline=10593102)]) is None


def test_pick_lead_takes_freshest_on_equal_stage():
    chosen = join.pick_lead([
        lead(88464034, lead_id=1, created_at=100),
        lead(88464034, lead_id=2, created_at=900),
    ])
    assert chosen["id"] == 2


# --------------------------------------------------------------- поиск по Telegram id


def _wire_search(monkeypatch, result, *, spy=None):
    async def find_contacts_by_query(query, limit=10):
        if spy is not None:
            spy.append(query)
        return result
    monkeypatch.setattr(
        join.amo_service, "find_contacts_by_query", find_contacts_by_query)


def test_find_by_telegram_id_requires_exact_field_match(monkeypatch):
    # Полнотекстовый поиск вытащил чужой контакт: цифры встретились в телефоне.
    _wire_search(monkeypatch, [{"id": 9, "custom_fields_values": [
        {"field_id": 271785, "values": [{"value": str(USER)}]}]}])
    found, why = run(join.find_lead_for_user(USER))
    assert found is None
    assert "не найден" in why


def test_find_by_telegram_id_reports_amo_silence(monkeypatch):
    # None от amo - это СБОЙ запроса, а не честный ноль: так и говорим.
    _wire_search(monkeypatch, None)
    found, why = run(join.find_lead_for_user(USER))
    assert found is None
    assert "не ответил" in why


def test_find_by_telegram_id_takes_matching_contact(monkeypatch):
    _wire_search(monkeypatch, [{"id": 9, "custom_fields_values": [
        {"field_id": join.FIELD_WAZZUP_TELEGRAM_ID,
         "values": [{"value": str(USER)}]}]}])

    async def get_contact_by_id(contact_id, with_=()):
        return {"id": contact_id, "_embedded": {"leads": [{"id": 500}]}}

    async def get_leads_by_ids(ids):
        return [lead(88835666, lead_id=i) for i in ids]

    monkeypatch.setattr(join.amo_service, "get_contact_by_id", get_contact_by_id)
    monkeypatch.setattr(join.amo_service, "get_leads_by_ids", get_leads_by_ids)
    found, why = run(join.find_lead_for_user(USER))
    assert found["id"] == 500
    assert "Telegram id" in why


def test_find_by_our_link_name_wins(monkeypatch):
    async def get_lead_full(lead_id, with_=()):
        return lead(88835666, lead_id=int(lead_id))
    monkeypatch.setattr(join.amo_service, "get_lead_full", get_lead_full)
    spy = []
    _wire_search(monkeypatch, [], spy=spy)
    found, why = run(join.find_lead_for_user(USER, "academy lead 777"))
    assert found["id"] == 777
    assert "ссылки" in why
    assert spy == []   # до поиска по контакту дело не дошло


def test_link_name_from_alien_pipeline_falls_back(monkeypatch):
    async def get_lead_full(lead_id, with_=()):
        return lead(88464034, lead_id=int(lead_id), pipeline=10593102)
    monkeypatch.setattr(join.amo_service, "get_lead_full", get_lead_full)
    _wire_search(monkeypatch, [])
    found, why = run(join.find_lead_for_user(USER, "academy lead 777"))
    assert found is None
    assert "не найден" in why


# --------------------------------------------------------------- перевод сделки


def _wire_found_lead(monkeypatch, status_id=88835666, lead_id=500):
    async def get_lead_full(lid, with_=()):
        return lead(status_id, lead_id=int(lid))
    monkeypatch.setattr(join.amo_service, "get_lead_full", get_lead_full)


def test_moves_lead_to_joined_stage(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    _wire_found_lead(monkeypatch)
    patched = {}

    async def patch_lead(lead_id, **kwargs):
        patched.update({"lead_id": lead_id, **kwargs})
        return {"ok": True}
    monkeypatch.setattr(join.amo_service, "patch_lead", patch_lead)

    assert run(join.handle_update(member_event(link_name="academy lead 500"))) == "moved"
    assert patched["lead_id"] == 500
    assert patched["status_id"] == int(STATUS_ACADEMY_JOINED_CHAT)
    assert patched["pipeline_id"] == int(PIPELINE_ACADEMY)


def test_second_event_for_same_person_does_nothing(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    _wire_found_lead(monkeypatch)
    calls = []

    async def patch_lead(lead_id, **kwargs):
        calls.append(lead_id)
        return {"ok": True}
    monkeypatch.setattr(join.amo_service, "patch_lead", patch_lead)

    assert run(join.handle_update(member_event(link_name="academy lead 500"))) == "moved"
    # Вышел и вернулся - сделку второй раз не двигаем.
    assert run(join.handle_update(member_event(link_name="academy lead 500"))) \
        == "already_handled"
    assert calls == [500]


def test_lead_already_on_stage_is_not_patched(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    _wire_found_lead(monkeypatch, status_id=int(STATUS_ACADEMY_JOINED_CHAT))
    called = []

    async def patch_lead(lead_id, **kwargs):
        called.append(lead_id)
        return {"ok": True}
    monkeypatch.setattr(join.amo_service, "patch_lead", patch_lead)

    assert run(join.handle_update(member_event(link_name="academy lead 500"))) \
        == "already_on_stage"
    assert called == []


def test_dry_run_decides_but_does_not_move(monkeypatch, tmp_path):
    enable(monkeypatch, dry_run=True, tmp_path=tmp_path)
    _wire_found_lead(monkeypatch)
    called = []

    async def patch_lead(lead_id, **kwargs):
        called.append(lead_id)
        return {"ok": True}
    monkeypatch.setattr(join.amo_service, "patch_lead", patch_lead)

    assert run(join.handle_update(member_event(link_name="academy lead 500"))) == "dry_run"
    assert called == []
    # Холостой ход не запоминает человека: реальный перевод потом должен состояться.
    assert join._state["seen"] == {}


def test_patch_failure_is_not_remembered(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    _wire_found_lead(monkeypatch)

    async def patch_lead(lead_id, **kwargs):
        return {"ok": False, "status_code": 500}
    monkeypatch.setattr(join.amo_service, "patch_lead", patch_lead)

    assert run(join.handle_update(member_event(link_name="academy lead 500"))) \
        == "patch_error"
    assert join._state["seen"] == {}


def test_disabled_does_nothing(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    monkeypatch.setattr(join, "ACADEMY_CHAT_JOIN_ENABLED", False)
    assert run(join.process_join(USER)) == "disabled"


# --------------------------------------------------------------- опрос


def test_poll_advances_offset_and_survives_bad_update(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    updates = [
        {"update_id": 10, "chat_member": {"chat": {"id": int(CHAT)}}},
        {"update_id": 11, "message": {"text": "не наше"}},
    ]

    async def fake_telegram(method, payload, timeout=20.0):
        assert method == "getUpdates"
        # Без явного allowed_updates Telegram не отдаёт chat_member вовсе.
        assert payload["allowed_updates"] == ["chat_member"]
        return updates

    monkeypatch.setattr(join, "_telegram", fake_telegram)
    assert run(join.poll_once()) == 2
    assert join._state["offset"] == 12


def test_poll_keeps_offset_when_nothing_came(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    join._state["offset"] = 5

    async def fake_telegram(method, payload, timeout=20.0):
        return []

    monkeypatch.setattr(join, "_telegram", fake_telegram)
    assert run(join.poll_once()) == 0
    assert join._state["offset"] == 5


def test_state_survives_restart(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    _wire_found_lead(monkeypatch)

    async def patch_lead(lead_id, **kwargs):
        return {"ok": True}
    monkeypatch.setattr(join.amo_service, "patch_lead", patch_lead)
    run(join.handle_update(member_event(link_name="academy lead 500")))

    # Новый процесс: состояние в памяти пустое, но на диске лежит.
    monkeypatch.setattr(join, "_state", {"offset": 0, "seen": {}})
    monkeypatch.setattr(join, "_state_loaded", False)
    assert run(join.handle_update(member_event(link_name="academy lead 500"))) \
        == "already_handled"


def test_stats_reports_disabled(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    monkeypatch.setattr(join, "ACADEMY_CHAT_JOIN_ENABLED", False)
    assert join.stats() == {"enabled": False}


def test_stats_reports_running(monkeypatch, tmp_path):
    enable(monkeypatch, tmp_path=tmp_path)
    data = join.stats()
    assert data["enabled"] is True
    assert data["dry_run"] is False
    assert data["offset"] == 0
