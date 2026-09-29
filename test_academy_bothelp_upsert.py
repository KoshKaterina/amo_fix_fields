"""Тесты приёмника BotHelp: сведённый набор обеих линий работы.

29.09.2026 две линии кода приёмника три дня жили порознь, и у каждой был свой набор тестов.
Здесь они сведены в один файл, потому что порознь каждый проверял только свою половину:

  • нижняя часть (боевая линия) - сопоставление по Telegram, строгий путь практикума для
    legacy-сценария, замок от одновременных доставок, отказ работать по непрочитанным данным;
  • верхняя часть (линия master) - выбор этапа во всех вариантах профиля: ранг бот-этапов,
    мягкий путь практикума с флагом новизны, защищённые этапы.

Два теста боевой линии адаптированы под сведённое поведение, каждый с объяснением на месте.

Запуск: python3 -m pytest test_academy_bothelp_upsert.py -q
"""

import os
import sys

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import academy_bothelp_upsert as up  # noqa: E402

INBOUND = up.STATUS_INBOUND
BOT = up.STATUS_BOT_STARTED
QUEST = up.STATUS_QUESTIONNAIRE
QUEST_DONE = up.STATUS_QUESTIONNAIRE_DONE
PRACTICUM = up.STATUS_RECORDED_PRACTICUM
WAITLIST = 70070966  # «Лист ожидания» — вне бот-этапов


def _profile(experience="", capital="", purpose="", event="", action=""):
    return {
        "опыт_в_инвестициях": experience,
        "размер_капитала": capital,
        "зачем_капитал": purpose,
        "Регистрация на мероприятие": event,
        "действие менеджера": action,
    }


# ── старт: создать и не двигать ──────────────────────────────────────────────

def test_sdelki_net_pustoy_profil_daet_bot_zapushchen():
    assert up._target_status(_profile(), None) == BOT


def test_tolko_chto_sozdannuyu_sdelku_ne_dvigaem():
    """Сделка родилась на «Боте запущен» — старый профиль не тащит её в середину воронки."""
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, BOT, just_created=True) is None


def test_start_po_sdelke_na_bote_zapushchennom_nichego_ne_menyaet():
    assert up._target_status(_profile(), BOT) is None


def test_vhodyashchiy_lid_podnimaetsya_do_bota():
    assert up._target_status(_profile(), INBOUND) == BOT


# ── только вперёд ────────────────────────────────────────────────────────────

def test_chastichnyy_profil_ne_tashchit_anketu_nazad():
    """Главный кейс: пришёл ОДИН ответ, а сделка уже на «Анкете пройденной»."""
    assert up._target_status(_profile(experience="с нуля"), QUEST_DONE) is None


def test_polnyy_profil_ne_otkatyvaet_s_ankety_proydennoy():
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, QUEST_DONE) is None


def test_vpered_po_ankete_rabotaet():
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, BOT) == QUEST_DONE


def test_odin_otvet_podnimaet_s_bota_na_prohodit_anketu():
    assert up._target_status(_profile(experience="с нуля"), BOT) == QUEST


# ── практикум ────────────────────────────────────────────────────────────────

def test_staryy_flag_praktikuma_ne_dvigaet_sdelku():
    """Человек записался на прошлой неделе, сегодня просто нажал /start."""
    payload = _profile(event="Практикум октябрь 2026")
    assert up._target_status(payload, BOT, practicum_is_new=False) is None


def test_novyy_flag_praktikuma_dvigaet():
    payload = _profile(event="Практикум октябрь 2026")
    assert up._target_status(payload, BOT, practicum_is_new=True) == PRACTICUM


def test_praktikum_iz_deystviya_klienta_tozhe_lovitsya():
    payload = _profile(action="записаться на практикум")
    assert up._target_status(payload, BOT, practicum_is_new=True) == PRACTICUM


def test_staryy_flag_praktikuma_ne_meshaet_ankete():
    """Флаг старый, но человек прямо сейчас отвечает на анкету — анкета едет."""
    payload = _profile(experience="с нуля", event="Практикум октябрь 2026")
    assert up._target_status(payload, BOT, practicum_is_new=False) == QUEST


# ── защищённые этапы ─────────────────────────────────────────────────────────

def test_zapisan_na_praktikum_ne_trogaem():
    payload = _profile(experience="с нуля", capital="50000", purpose="приумножить")
    assert up._target_status(payload, PRACTICUM) is None


def test_list_ozhidaniya_ne_trogaem():
    assert up._target_status(_profile(experience="с нуля"), WAITLIST) is None


def test_praktikum_s_zashchishchennogo_etapa_ne_stavitsya():
    payload = _profile(event="Практикум октябрь 2026")
    assert up._target_status(payload, WAITLIST, practicum_is_new=True) is None


# ── вспомогательное ──────────────────────────────────────────────────────────

def test_wants_practicum_chitaet_oba_polya_i_registr():
    assert up._wants_practicum("ПРАКТИКУМ октябрь", "") is True
    assert up._wants_practicum("", "Записаться на Практикум") is True
    assert up._wants_practicum("Конференция", "написать менеджеру") is False
    assert up._wants_practicum(None, None) is False


import asyncio  # noqa: E402

mod = up


def run(coro):
    return asyncio.run(coro)


# ── ниже: тесты боевой линии (Telegram, строгий практикум, замок) ──────────────

def payload(**overrides):
    base = {
        "cuid": "7hw4.ddv", "name": "Тест Кат", "phone": "+79250833349",
        "email": "test@sunscrypt.ru", "pd_consent": "да", "marketing_consent": "да",
        "user_id": "477157515", "messenger_username": "test_kat",
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
    # После сведения линий 29.09.2026 практикум ставится и мягким путём, поэтому
    # «анкета никуда не двигает» проверяется при снятом флаге новизны.
    assert mod._target_status(
        payload(), mod.STATUS_QUESTIONNAIRE_DONE, practicum_is_new=False) is None
    # А с новым флагом та же анкета уводит на «Записан на практикум».
    assert mod._target_status(
        payload(), mod.STATUS_QUESTIONNAIRE_DONE) == mod.STATUS_RECORDED_PRACTICUM
    assert mod._target_status(payload(размер_капитала="", зачем_капитал="", **no_event), mod.STATUS_QUESTIONNAIRE_DONE) is None
    assert mod._target_status(payload(), 88835666) is None


def test_payload_fields_maps_answers():
    fields = mod._payload_fields(payload())
    assert fields[mod.FIELD_CUID] == "7hw4.ddv"
    assert fields[mod.FIELD_EXPERIENCE] == "хороший опыт"
    assert fields[mod.FIELD_CAPITAL] == "10000000"
    assert fields[mod.FIELD_PURPOSE] == "хочу разобраться"
    assert fields[mod.FIELD_TELEGRAM_ID] == "477157515"
    assert fields[mod.FIELD_TELEGRAM_USERNAME] == "@test_kat"


def test_telegram_identity_queries_and_matches_wazzup_contact(monkeypatch):
    contact = {
        "id": 10,
        "custom_fields_values": [
            {"field_id": mod.FIELD_TELEGRAM_ID, "values": [{"value": "477157515"}]},
            {"field_id": mod.FIELD_TELEGRAM_USERNAME, "values": [{"value": "@liverpoolynwa1892"}]},
        ],
    }
    queries = []

    async def find(query, limit=25):
        queries.append(query)
        return [contact] if query == "477157515" else []

    monkeypatch.setattr(mod.amo_service, "find_contacts_by_query", find)
    item = payload(
        cuid="7hw4.dho", phone="", email="", user_id="477157515",
        messenger_username="liverpoolynwa1892",
    )
    assert run(mod._candidate_contacts(item)) == [contact]
    assert queries == ["7hw4.dho", "477157515", "@liverpoolynwa1892"]
    assert mod._matches(contact, item) is True


def test_process_reuses_wazzup_lead_found_only_by_telegram_id(monkeypatch):
    contact = {
        "id": 10,
        "custom_fields_values": [
            {"field_id": mod.FIELD_TELEGRAM_ID, "values": [{"value": "477157515"}]},
        ],
        "_embedded": {"leads": [{"id": 20}]},
    }
    lead = {
        "id": 20, "pipeline_id": mod.PIPELINE_ACADEMY,
        "status_id": mod.STATUS_INBOUND, "created_at": 200,
    }
    writes = []

    async def find(query, limit=25):
        return [contact] if query == "477157515" else []

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("Telegram identity must reuse the existing Wazzup pair")

    monkeypatch.setattr(mod, "configured", lambda: True)
    monkeypatch.setattr(mod.amo_service, "find_contacts_by_query", find)
    monkeypatch.setattr(mod.amo_service, "get_contact_by_id", lambda *_a, **_k: _async(contact))
    monkeypatch.setattr(mod.amo_service, "get_leads_by_ids", lambda *_a: _async([lead]))
    monkeypatch.setattr(mod.api, "create_contact", forbidden)
    monkeypatch.setattr(mod.api, "create_lead_direct", forbidden)
    monkeypatch.setattr(mod.api, "update_contact", lambda *_a, **_k: _async(True))
    monkeypatch.setattr(
        mod.amo_service, "patch_lead",
        lambda lid, **kwargs: writes.append((lid, kwargs)) or _async({"ok": True}),
    )
    monkeypatch.setattr(mod.academy_invite_delivery, "schedule", lambda *_a: None)

    item = payload(
        cuid="7hw4.dho", phone="", email="", user_id="477157515",
        messenger_username="liverpoolynwa1892",
        **{"Регистрация на мероприятие": "", "действие менеджера": ""},
    )
    result = run(mod.process(item))
    assert result == {"ok": True, "contact_id": 10, "lead_id": 20, "resolution": "existing"}
    assert writes == [(20, {
        "status_id": mod.STATUS_QUESTIONNAIRE_DONE,
        "pipeline_id": mod.PIPELINE_ACADEMY,
    })]


def test_numeric_bothelp_profile_id_is_not_used_as_telegram_id():
    item = payload(user_id="", bothelp_user_id="17012")
    assert mod._telegram_id(item) == ""


def test_user_id_username_matches_normalized_telegram_username():
    contact = {
        "custom_fields_values": [
            {"field_id": mod.FIELD_TELEGRAM_USERNAME, "values": [{"value": "@sergey_v29"}]},
        ],
    }
    item = payload(cuid="", phone="", email="", user_id="sergey_v29", messenger_username="")
    assert mod._telegram_username(item) == "@sergey_v29"
    assert mod._matches(contact, item) is True


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
    # Сведение линий 29.09.2026 изменило ожидание осознанно: карточка контакта пуста,
    # значит флаг практикума пришёл ИМЕННО этим запросом, и человека надо вести на
    # «Записан на практикум», а не оставлять на анкете. Прежнее ожидание писалось, когда
    # мягкого пути практикума в приёмнике не было вовсе.
    assert patched_leads == [(20, {
        "status_id": mod.STATUS_RECORDED_PRACTICUM,
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
