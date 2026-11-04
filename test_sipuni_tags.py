"""Теги звонка Sipuni, которые ставит наш код: «пропущенный» и «Успешный звонок» (SIPUNI_SET_TAGS).

У Sipuni тегов сделки нет, а на них завязаны unmiss_tag.py, боты Цифровой воронки и отчёты.
Распределение Sipuni теги НЕ читает (решает по потоку звонков, см. lead_distribution._sipuni_gate),
поэтому здесь проверяется только запись тегов и то, что она ничего не ломает.

Что защищаем:
  * по умолчанию выключено, и в режиме log запись в amo не идёт вообще (ни при каком флаге);
  * тегаем только НОВУЮ сделку (этап «Новый лид» или его буфер; решение Кати 03.11.2026);
    «пропущенный» ставится на неё и её контакты, один раз на звонок; такой сделки нет - тегов
    нет (контакт без сделки получил бы тег, который некому снять);
  * свежий пропуск сбрасывает «Успешный звонок» сделки, иначе unmiss_tag снял бы «пропущенный» сразу;
  * «Успешный звонок» ставится на отвеченный звонок, входящий и исходящий, один раз на call_id;
  * цепочка с unmiss_tag: после отвеченного звонка «пропущенный» снимается, «Успешный звонок» остаётся;
  * сбой amo не роняет учёт звонков и не отменяет алерт.

События те же, что в замере 03.11.2026 (см. раздел «РЕАЛЬНЫЕ события» в test_sipuni_events.py).
Запуск: python -m pytest test_sipuni_tags.py -q
"""
import copy
import logging

import pytest

import amo_service
import sipuni_events as S
import uis_missed_call as U
import unmiss_tag
from test_sipuni_events import (  # noqa: F401 - glue: фикстура, её нужно держать в этом модуле
    CALLER_DIGITS, OUT_TREE, PHONE_NORM, STAFF_207, e, feed, glue, idle, r_answer,
    r_greeting_drop, r_not_taken, r_taken, run,
)
from waybill_config import TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME

LEAD_ID = 36500001      # сделку отдаёт поиск по телефону
CONTACT_ID = 7001
NEW_LEAD = 83537714     # этап «Новый лид»
NEW_LEAD_BUFFER = 83915186
IN_WORK = 83537718      # любой другой этап: сделка уже в работе


def out_call(cid, ts=500, *, client=CALLER_DIGITS, answered=True):
    """Исходящий 03.11: сотрудник(2) -> клиент(1), два ev=1 и один ev=2, схема 000-4215210."""
    def o(kind, t, **kw):
        return e(kind, cid, src=STAFF_207, src_type=2, dst=client, dst_type=1, ts=t,
                 tree=OUT_TREE, **kw)
    if answered:
        return [o(1, ts), o(1, ts + 1), o(3, ts + 9),
                o(2, ts + 40, status="ANSWER", answer_ts=ts + 9)]
    return [o(1, ts), o(1, ts + 1), o(2, ts + 20, status="NOANSWER")]


# ═══════════════════════ отвеченный звонок в учёте звонков ═══════════════════════


@pytest.fixture
def trk():
    answered = []
    tracker = S.SipuniCallTracker(quiet_s=0.05, stuck_s=0.6, on_missed=lambda m: None,
                                  on_answered=answered.append)
    tracker.answered = answered
    yield tracker
    tracker.reset()


def test_answered_incoming_is_reported_once_after_quiet_window(trk):
    cid = "tg.1"

    async def go():
        feed(trk, r_taken(cid))
        assert trk.answered == [], "до тихого окна решения нет"
        await idle(trk)
        feed(trk, r_taken(cid))                     # повтор доставки
        await idle(trk)

    run(go())
    assert [(a.call_id, a.direction, a.phone) for a in trk.answered] == [
        (cid, S.DIR_IN, PHONE_NORM)]


def test_answered_outgoing_is_reported_with_the_client_phone(trk):
    async def go():
        feed(trk, out_call("tg.2"))
        await idle(trk)

    run(go())
    assert [(a.direction, a.phone) for a in trk.answered] == [(S.DIR_OUT, PHONE_NORM)]


@pytest.mark.parametrize("name, events", [
    ("сброс на приветствии", r_greeting_drop("tg.3")),
    ("ноги без ответа", r_not_taken("tg.4", legs=3)),
    ("исходящий без ответа", out_call("tg.5", answered=False)),
    ("внутренний с ответом", [
        e(1, "tg.6", src="012345102", src_type=2, dst="012345101", dst_type=2, is_inner_call="1"),
        e(3, "tg.6", src="012345102", src_type=2, dst="012345101", dst_type=2),
        e(2, "tg.6", src="012345102", src_type=2, dst="012345101", dst_type=2,
          status="ANSWER", answer_ts=1700000005)]),
])
def test_other_calls_are_not_reported_as_answered(trk, name, events):
    async def go():
        feed(trk, events)
        await idle(trk)

    run(go())
    assert trk.answered == [], name


def test_answer_after_missed_decision_is_reported_to_clear_the_tag(trk):
    """Решили «пропущенный», а потом ответ всё же пришёл: тег «Успешный звонок» снимет «пропущенный»."""
    cid = "tg.7"

    async def go():
        feed(trk, r_not_taken(cid, legs=2))
        await idle(trk)
        assert trk.answered == []
        feed(trk, [r_answer(cid, 2100)])
        await idle(trk)

    run(go())
    assert [a.call_id for a in trk.answered] == [cid]


def test_state_loss_does_not_repeat_the_answered_report(trk):
    cid = "tg.8"

    async def go():
        feed(trk, r_taken(cid))
        await idle(trk)
        trk._calls.clear()                          # состояние вытеснили из памяти
        feed(trk, r_taken(cid))
        await idle(trk)

    run(go())
    assert len(trk.answered) == 1


# ═══════════════════════ запись тегов в amo ═══════════════════════


class FakeAmo:
    """Минимальная сделка и контакт в памяти. get_lead_full, patch_lead, get_contact_by_id и
    patch_contact подменены, остальное (add_tag, remove_tag, add_contact_tag, remove_contact_tag,
    has_tag) настоящее: проверяем и их."""

    def __init__(self):
        self.leads = {LEAD_ID: {"id": LEAD_ID, "_embedded": {
            "tags": [], "contacts": [{"id": CONTACT_ID}]}}}
        self.contacts = {CONTACT_ID: {"id": CONTACT_ID, "_embedded": {"tags": []}}}
        self.lead_patches = []
        self.contact_patches = []
        self.fail_patch = False
        self.lead_status = NEW_LEAD       # этап сделки, которую отдаёт поиск по телефону
        self.extra_leads = []             # другие открытые сделки клиента

    def lead_tags(self):
        return sorted(t["name"] for t in self.leads[LEAD_ID]["_embedded"]["tags"])

    def contact_tags(self):
        return sorted(t["name"] for t in self.contacts[CONTACT_ID]["_embedded"]["tags"])

    def give_lead_tags(self, *names):
        self.leads[LEAD_ID]["_embedded"]["tags"] = [{"id": 900 + i, "name": n}
                                                    for i, n in enumerate(names)]


@pytest.fixture
def amo(glue, monkeypatch):
    fake = FakeAmo()

    async def get_lead_full(lead_id, with_=()):
        lead = fake.leads.get(int(lead_id))
        return copy.deepcopy(lead) if lead else None

    async def patch_lead(lead_id, *, tags=None, **kw):
        if fake.fail_patch:
            raise RuntimeError("amo недоступна")
        fake.lead_patches.append(copy.deepcopy(tags))
        fake.leads[int(lead_id)]["_embedded"]["tags"] = [dict(t) for t in tags]
        return {"ok": True, "status_code": 200, "retryable": False}

    async def get_contact_by_id(contact_id, with_=()):
        contact = fake.contacts.get(int(contact_id))
        return copy.deepcopy(contact) if contact else None

    async def patch_contact(contact_id, *, tags=None, custom_fields=None):
        fake.contact_patches.append(copy.deepcopy(tags))
        fake.contacts[int(contact_id)]["_embedded"]["tags"] = [dict(t) for t in tags]
        return {"ok": True, "status_code": 200, "retryable": False}

    async def find_leads_by_query(query, *a, **k):
        found = [{"id": LEAD_ID, "status_id": fake.lead_status, "responsible_user_id": 13929334,
                  "updated_at": 5}]
        return found + list(fake.extra_leads)

    monkeypatch.setattr(amo_service, "find_leads_by_query", find_leads_by_query)
    monkeypatch.setattr(amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(amo_service, "patch_lead", patch_lead)
    monkeypatch.setattr(amo_service, "get_contact_by_id", get_contact_by_id)
    monkeypatch.setattr(amo_service, "patch_contact", patch_contact)
    monkeypatch.setattr(S.tracker, "on_answered", None)   # вернётся после теста
    monkeypatch.setattr(U, "SIPUNI_ROUTING_WAIT_S", 0)    # здесь проверяем теги, а не ожидание
    monkeypatch.setattr(S.tracker, "_answered_done", S.collections.OrderedDict())
    return fake


def live(monkeypatch, *, tags=True):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    monkeypatch.setattr(U, "SIPUNI_SET_TAGS", tags)


def test_missed_tags_deal_and_contact(amo, glue, monkeypatch):
    live(monkeypatch)

    async def go():
        await U._sipuni_apply(r_not_taken("tg.20", legs=3))
        await idle(S.tracker)

    run(go())
    assert amo.lead_tags() == [TAG_MISSED_NAME]
    assert amo.contact_tags() == [TAG_MISSED_NAME]
    assert len(glue.sends) == 1, "алерт уходит, как и раньше"


def test_greeting_drop_with_a_deal_is_tagged_too(amo, glue, monkeypatch):
    live(monkeypatch)

    async def go():
        await U._sipuni_apply(r_greeting_drop("tg.21"))
        await idle(S.tracker)

    run(go())
    assert amo.lead_tags() == [TAG_MISSED_NAME] and amo.contact_tags() == [TAG_MISSED_NAME]


def test_fresh_miss_clears_old_success_tag_and_keeps_other_tags(amo, glue, monkeypatch):
    """Иначе unmiss_tag увидел бы оба тега и снял «пропущенный» сразу после постановки."""
    live(monkeypatch)
    amo.give_lead_tags("Горячий", TAG_SUCCESS_CALL_NAME)

    async def go():
        await U._sipuni_apply(r_not_taken("tg.22", legs=2))
        await idle(S.tracker)

    run(go())
    assert amo.lead_tags() == sorted(["Горячий", TAG_MISSED_NAME])
    assert len(amo.lead_patches) == 1, "одна запись на сделку"


def test_missed_tag_already_present_is_not_written_again(amo, glue, monkeypatch):
    live(monkeypatch)
    amo.give_lead_tags(TAG_MISSED_NAME)

    async def go():
        await U._sipuni_apply(r_not_taken("tg.23", legs=2))
        await idle(S.tracker)

    run(go())
    assert amo.lead_patches == [] and amo.contact_tags() == [TAG_MISSED_NAME]


def test_missed_without_open_deal_writes_nothing(amo, glue, monkeypatch):
    """Известный клиент без открытой сделки (как тест Т8): алерт уходит, тегов нет."""
    live(monkeypatch)

    async def no_leads(query, *a, **k):
        return []

    monkeypatch.setattr(amo_service, "find_leads_by_query", no_leads)

    async def go():
        await U._sipuni_apply(r_greeting_drop("tg.24"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1
    assert amo.lead_patches == [] and amo.contact_patches == []


@pytest.mark.parametrize("stage, tagged", [
    (NEW_LEAD, True), (NEW_LEAD_BUFFER, True), (IN_WORK, False), (142, False), (143, False),
])
def test_only_a_new_lead_deal_is_tagged(amo, glue, monkeypatch, stage, tagged):
    """Решение Кати 03.11.2026: тегаем новую сделку (этап «Новый лид» или его буфер), остальным всё равно."""
    live(monkeypatch)
    amo.lead_status = stage

    async def go():
        await U._sipuni_apply(r_not_taken("tg.70", legs=2))
        await U._sipuni_apply(r_taken("tg.71", client="79555555556"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1, "алерт не зависит от этапа сделки"
    if tagged:
        assert amo.lead_tags() == sorted([TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME])
        assert amo.contact_tags() == [TAG_MISSED_NAME]
    else:
        assert amo.lead_tags() == [] and amo.contact_tags() == []
        assert amo.lead_patches == [] and amo.contact_patches == []


def test_new_deal_is_chosen_over_a_fresher_deal_in_work(amo, glue, monkeypatch):
    """У клиента две открытые сделки: тег ложится на новую, даже если сделка в работе обновлялась позже."""
    live(monkeypatch)
    amo.extra_leads = [{"id": 36500002, "status_id": IN_WORK, "responsible_user_id": 1,
                        "updated_at": 99}]

    async def go():
        await U._sipuni_apply(r_not_taken("tg.72", legs=2))
        await idle(S.tracker)

    run(go())
    assert amo.lead_tags() == [TAG_MISSED_NAME]


def test_missed_with_hidden_number_writes_nothing(amo, glue, monkeypatch):
    live(monkeypatch)
    cid = "tg.25"

    async def go():
        await U._sipuni_apply([e(1, cid, src="", src_type=1), e(2, cid, src="", src_type=1)])
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and amo.lead_patches == [] and amo.contact_patches == []


@pytest.mark.parametrize("mode, flag", [("live", False), ("log", True), ("log", False)])
def test_tags_are_off_unless_live_and_flag_on(amo, glue, monkeypatch, mode, flag):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", mode)
    monkeypatch.setattr(U, "SIPUNI_SET_TAGS", flag)

    async def go():
        await U._sipuni_apply(r_not_taken("tg.26", legs=2))
        await U._sipuni_apply(r_taken("tg.27"))
        await idle(S.tracker)

    run(go())
    assert amo.lead_patches == [] and amo.contact_patches == []
    assert (len(glue.sends) == 1) == (mode == "live"), "алерт зависит только от режима"


def test_answered_call_sets_success_tag_once(amo, glue, monkeypatch):
    live(monkeypatch)

    async def go():
        await U._sipuni_apply(r_taken("tg.30"))
        await U._sipuni_apply(r_taken("tg.30"))             # повтор доставки
        await idle(S.tracker)

    run(go())
    assert amo.lead_tags() == [TAG_SUCCESS_CALL_NAME]
    assert len(amo.lead_patches) == 1
    assert amo.contact_tags() == [], "«Успешный звонок» на контакт не ставим"
    assert glue.sends == [], "по отвеченному алерта нет"


def test_answered_outgoing_sets_success_tag(amo, glue, monkeypatch):
    live(monkeypatch)

    async def go():
        await U._sipuni_apply(out_call("tg.31"))
        await idle(S.tracker)

    run(go())
    assert amo.lead_tags() == [TAG_SUCCESS_CALL_NAME]


def test_success_tag_already_there_is_not_written_again(amo, glue, monkeypatch):
    live(monkeypatch)
    amo.give_lead_tags(TAG_SUCCESS_CALL_NAME)

    async def go():
        await U._sipuni_apply(r_taken("tg.32"))
        await idle(S.tracker)

    run(go())
    assert amo.lead_patches == []


def test_answered_without_open_deal_writes_nothing(amo, glue, monkeypatch):
    live(monkeypatch)

    async def no_leads(query, *a, **k):
        return []

    monkeypatch.setattr(amo_service, "find_leads_by_query", no_leads)

    async def go():
        await U._sipuni_apply(r_taken("tg.33"))
        await idle(S.tracker)

    run(go())
    assert amo.lead_patches == []


def test_miss_then_callback_chain_with_unmiss_tag(amo, glue, monkeypatch):
    """Пропущенный -> перезвонили -> unmiss_tag снимает «пропущенный» со сделки и контакта."""
    live(monkeypatch)

    async def go():
        await U._sipuni_apply(r_not_taken("tg.40", legs=2))
        await idle(S.tracker)
        assert amo.lead_tags() == [TAG_MISSED_NAME] and amo.contact_tags() == [TAG_MISSED_NAME]
        await U._sipuni_apply(out_call("tg.41"))             # менеджер перезвонил, клиент ответил
        await idle(S.tracker)
        assert amo.lead_tags() == sorted([TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME])
        await unmiss_tag._apply(LEAD_ID)                     # эхо правки сделки

    run(go())
    assert amo.lead_tags() == [TAG_SUCCESS_CALL_NAME]
    assert amo.contact_tags() == []


def test_new_miss_after_success_is_not_undone_by_unmiss_tag(amo, glue, monkeypatch):
    live(monkeypatch)

    async def go():
        await U._sipuni_apply(r_taken("tg.42"))
        await idle(S.tracker)
        assert amo.lead_tags() == [TAG_SUCCESS_CALL_NAME]
        await U._sipuni_apply(r_not_taken("tg.43", legs=2, ts=5000))
        await idle(S.tracker)
        await unmiss_tag._apply(LEAD_ID)

    run(go())
    assert amo.lead_tags() == [TAG_MISSED_NAME], "пропуск после разговора остаётся пропуском"


def test_amo_failure_does_not_break_alert_or_call_tracking(amo, glue, monkeypatch, caplog):
    live(monkeypatch)
    amo.fail_patch = True
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        await U._sipuni_apply(r_not_taken("tg.50", legs=2))
        await U._sipuni_apply(r_taken("tg.51", client="79555555556"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1, "алерт ушёл до записи тегов"
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "ошибка постановки тега (call=tg.50)" in text
    assert "ошибка постановки тега (call=tg.51)" in text
    assert len(S.tracker) >= 2, "учёт звонков жив"


def test_log_mode_reports_answered_call_without_writing(amo, glue, monkeypatch, caplog):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "log")
    monkeypatch.setattr(U, "SIPUNI_SET_TAGS", True)
    caplog.set_level(logging.INFO, logger="uvicorn")

    async def go():
        await U._sipuni_apply(r_taken("tg.60"))
        await idle(S.tracker)

    run(go())
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "Sipuni отвеченный: call=tg.60 напр=in" in text and "режим log" in text
    assert amo.lead_patches == []


def test_flag_defaults_are_safe(monkeypatch):
    """Опечатка в значении не включает запись в amo: только точное on."""
    import importlib.util
    import pathlib
    import webhooks

    def probe(value):
        monkeypatch.delenv("SIPUNI_SET_TAGS", raising=False)
        if value is not None:
            monkeypatch.setenv("SIPUNI_SET_TAGS", value)
        path = pathlib.Path(webhooks.__file__).with_name("waybill_config.py")
        spec = importlib.util.spec_from_file_location("waybill_config_probe_tags", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.SIPUNI_SET_TAGS

    assert probe(None) is False
    for raw in ("", "1", "true", "yes", "onn", "off"):
        assert probe(raw) is False, raw
    assert probe("on") is True and probe(" ON ") is True
