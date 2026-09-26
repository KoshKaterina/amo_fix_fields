"""Тесты сторожа писем (mail_watch).

Главный из них — `test_one_mail_in_three_leads_creates_one`. Замер 26.09.2026 показал, что
amoCRM кладёт ОДНО письмо примечанием в несколько сделок контакта (13 писем из 289 попали в
две-три сделки). Наивный сторож завёл бы на такое письмо три сделки, и это увидел бы клиент.

Остальное — про молчание там, где молчать надо: письмо в открытой сделке, у клиента есть
другая открытая сделка, письмо от робота, повтор той же переписки. Плюс поведение при
недоступной amoCRM: письмо НЕ помечается разобранным и окно не двигается (на обратном
допущении сгорел order_watchdog 03.09.2026 — прочитал None как «ничего нет»).

Запуск: python3 -m pytest test_mail_watch.py -q
"""

import asyncio
import os
import sys
import tempfile
import time

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")
_DB = os.path.join(tempfile.mkdtemp(prefix="mail_watch_test_"), "mail_watch.sqlite3")
os.environ["MAIL_WATCH_DB_PATH"] = _DB
os.environ["MAIL_WATCH_ENABLED"] = "1"
os.environ["MAIL_WATCH_SINCE_TS"] = "1"

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_sent: list[str] = []

# ⚠️ Модули telegram_bot и alerts НЕ подменяем в sys.modules: такая подмена видна всем
# тестам процесса и ломает сборку соседей (ловушка поймана при первом прогоне — набор
# падал на test_office_transfer с «module alerts has no attribute …»). Оба импортируются
# в тестовом окружении честно, а их функции глушатся в фикстуре через monkeypatch.

import mail_watch  # noqa: E402
import mail_watch_store as store  # noqa: E402


# ---------------------------------------------------------------------------
# Фейковая amoCRM: события, примечания, сделки, контакты
# ---------------------------------------------------------------------------
class FakeAmo:
    def __init__(self):
        self.events: list[dict] = []
        self.notes: dict[int, dict] = {}       # note_id -> примечание
        self.leads: dict[int, dict] = {}       # lead_id -> сделка
        self.contact_leads: dict[int, list[int]] = {}
        self.created: list[dict] = []          # созданные сделки
        self.notes_added: list[tuple[int, str]] = []
        self.silent_lead_read = False

    # --- то, что подменяем в amo_service ---
    async def _do_get(self, path, params=None):
        if path == "/api/v4/events":
            return {"_embedded": {"events": list(self.events)}}
        if "/notes/" in path:
            note_id = int(path.rsplit("/", 1)[-1])
            note = self.notes.get(note_id)
            return note
        return None

    async def get_lead_full(self, lead_id, with_=()):
        if self.silent_lead_read:
            return None
        return self.leads.get(int(lead_id))

    async def get_contact_by_id(self, contact_id, with_=()):
        ids = self.contact_leads.get(int(contact_id), [])
        return {"id": int(contact_id), "_embedded": {"leads": [{"id": i} for i in ids]}}

    async def get_leads_by_ids(self, lead_ids):
        return [self.leads[i] for i in lead_ids if i in self.leads]

    async def add_note(self, lead_id, text):
        self.notes_added.append((int(lead_id), text))
        return {}

    # --- то, что подменяем в api ---
    async def create_lead_direct(self, **kw):
        lead_id = 900000 + len(self.created) + 1
        self.created.append(dict(kw, id=lead_id))
        self.leads[lead_id] = {"id": lead_id, "status_id": int(kw.get("status_id") or 0)}
        cid = kw.get("contact_id")
        if cid:
            self.contact_leads.setdefault(int(cid), []).append(lead_id)
        return lead_id


def _mail_note(note_id, *, message_id, thread_id, sender="client@example.com",
               subject="Вопрос по заказу", income=True, created_at=None):
    return {
        "id": note_id,
        "note_type": "amomail_message",
        "created_at": created_at or int(time.time()),
        "params": {
            "thread_id": str(thread_id),
            "message_id": str(message_id),
            "income": income,
            "from": {"email": sender, "name": "Клиент"},
            "to": {"email": "sales@sunscrypt.ru"},
            "subject": subject,
            "content_summary": "Здравствуйте, подскажите",
            "attach_cnt": 0,
        },
    }


def _event(note_id, entity_type="lead", entity_id=1, created_at=None):
    return {
        "id": f"ev{note_id}",
        "type": "incoming_mail",
        "entity_type": entity_type,
        "entity_id": entity_id,
        "created_at": created_at or int(time.time()),
        "value_after": [{"note": {"id": note_id}}],
    }


@pytest.fixture(autouse=True)
def fresh(monkeypatch):
    """Чистая база на каждый тест плюс подмена всех выходов в сеть."""
    if os.path.exists(_DB):
        os.remove(_DB)
    store.init_db()
    amo = FakeAmo()
    monkeypatch.setattr(mail_watch.amo_service, "_do_get", amo._do_get)
    monkeypatch.setattr(mail_watch.amo_service, "get_lead_full", amo.get_lead_full)
    monkeypatch.setattr(mail_watch.amo_service, "get_contact_by_id", amo.get_contact_by_id)
    monkeypatch.setattr(mail_watch.amo_service, "get_leads_by_ids", amo.get_leads_by_ids)
    monkeypatch.setattr(mail_watch.amo_service, "add_note", amo.add_note)
    monkeypatch.setattr(mail_watch.api, "create_lead_direct", amo.create_lead_direct)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_SINCE_TS", 1)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", False)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_ALERT_ENABLED", False)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_TARGET_PIPELINE_ID", 10593102)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_TARGET_STATUS_ID", 83537714)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_RESPONSIBLE_USER_ID", 13794378)

    async def _send_alert(text, **kw):
        _sent.append(text)
        return True

    monkeypatch.setattr(mail_watch.telegram_bot, "send_alert", _send_alert)
    _sent.clear()
    return amo


def _closed_lead(lead_id, contact_id, status=143):
    return {
        "id": lead_id,
        "status_id": status,
        "closed_at": int(time.time()) - 86400,
        "_embedded": {"contacts": [{"id": contact_id}]},
    }


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------------------


def test_report_mode_does_not_create(fresh):
    amo = fresh
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert amo.created == [], "в режиме отчёта сделок быть не должно"
    assert store.recent()[0]["decision"] == store.DECISION_REPORT_ONLY


def test_creates_lead_with_backlink(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert len(amo.created) == 1
    created = amo.created[0]
    assert created["contact_id"] == 55
    assert created["responsible_user_id"] == 13794378
    assert "Вопрос по заказу" in created["name"]
    # примечание в новой сделке и обратная ссылка в старой
    targets = {lead_id for lead_id, _ in amo.notes_added}
    assert targets == {created["id"], 1}
    assert store.recent()[0]["decision"] == store.DECISION_CREATED


def test_one_mail_in_three_leads_creates_one(fresh, monkeypatch):
    """Главный тест: одно письмо, три сделки, одна новая сделка."""
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    for lead_id in (1, 2, 3):
        amo.leads[lead_id] = _closed_lead(lead_id, 55)
    amo.contact_leads[55] = [1, 2, 3]
    for note_id, lead_id in ((101, 1), (102, 2), (103, 3)):
        amo.notes[note_id] = _mail_note(note_id, message_id="m-one", thread_id="t1")
    amo.events = [_event(101, entity_id=1), _event(102, entity_id=2), _event(103, entity_id=3)]

    run(mail_watch.reconcile_once())

    assert len(amo.created) == 1, "одно физическое письмо — одна сделка"
    decisions = [r["decision"] for r in store.recent()]
    assert decisions.count(store.DECISION_CREATED) == 1
    assert decisions.count(store.DECISION_DUPLICATE) == 2


def test_open_lead_is_left_alone(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    amo.leads[1] = _closed_lead(1, 55, status=83537714)  # открытая
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert amo.created == []
    assert store.recent()[0]["decision"] == store.DECISION_OPEN_LEAD


def test_contact_with_other_open_lead(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    amo.leads[1] = _closed_lead(1, 55)
    amo.leads[2] = {"id": 2, "status_id": 83537714, "pipeline_id": 10593102,
                    "_embedded": {"contacts": [{"id": 55}]}}
    amo.contact_leads[55] = [1, 2]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert amo.created == []
    assert store.recent()[0]["decision"] == store.DECISION_CONTACT_HAS_OPEN


def test_open_lead_in_ignored_pipeline_does_not_block(fresh, monkeypatch):
    """Открытая сделка в воронке «Тест» или в картотеке — не признак работы.

    Живой кейс 26.09.2026: письмо легло в закрытую сделку 36564831, а сторож промолчал,
    потому что у контакта висели три прогона авто-режима в воронке «Тест» с 9 и 12 сентября.
    """
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_IGNORE_PIPELINES", frozenset({8642414}))
    amo.leads[1] = _closed_lead(1, 55)
    # открытая сделка, но в воронке «Тест»
    amo.leads[2] = {"id": 2, "status_id": 70070986, "pipeline_id": 8642414,
                    "_embedded": {"contacts": [{"id": 55}]}}
    amo.contact_leads[55] = [1, 2]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert len(amo.created) == 1, "тестовая воронка не должна блокировать обращение"
    assert store.recent()[0]["decision"] == store.DECISION_CREATED


def test_open_lead_in_working_pipeline_still_blocks(fresh, monkeypatch):
    """А открытая сделка в рабочей воронке блокирует по-прежнему."""
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_IGNORE_PIPELINES", frozenset({8642414}))
    amo.leads[1] = _closed_lead(1, 55)
    amo.leads[2] = {"id": 2, "status_id": 83537714, "pipeline_id": 10593102,
                    "_embedded": {"contacts": [{"id": 55}]}}
    amo.contact_leads[55] = [1, 2]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert amo.created == []
    assert store.recent()[0]["decision"] == store.DECISION_CONTACT_HAS_OPEN


def test_service_sender_ignored(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1",
                               sender="mailer-daemon@yandex.ru", subject="Недоставленное сообщение")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert amo.created == []
    assert store.recent()[0]["decision"] == store.DECISION_SERVICE_SENDER


def test_second_mail_in_same_thread_no_second_lead(fresh, monkeypatch):
    """Клиент пишет второй раз в ту же тему — это тот же диалог."""
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]
    run(mail_watch.reconcile_once())
    assert len(amo.created) == 1
    our_lead = amo.created[0]["id"]

    # второе письмо той же цепочки, наша сделка ещё открыта
    amo.notes[102] = _mail_note(102, message_id="m2", thread_id="t1")
    amo.events = [_event(102, entity_id=1)]
    run(mail_watch.reconcile_once())

    assert len(amo.created) == 1, "второй сделки по той же переписке быть не должно"
    last = store.recent()[0]
    assert last["decision"] == store.DECISION_THREAD_ACTIVE
    assert last["lead_id"] == our_lead


def test_silent_amo_does_not_mark_or_advance_window(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    amo.silent_lead_read = True
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert amo.created == []
    assert store.recent() == [], "письмо не должно считаться разобранным"
    assert store.get_last_ts() == 0, "окно не должно двигаться, пока есть неразобранные"

    # amo ожила — следующий проход доводит дело до конца
    amo.silent_lead_read = False
    run(mail_watch.reconcile_once())
    assert len(amo.created) == 1
    assert store.get_last_ts() > 0


def test_old_mail_skipped(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_MAX_AGE_MIN", 60)
    old = int(time.time()) - 3 * 3600
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1", created_at=old)
    amo.events = [_event(101, entity_id=1, created_at=old)]

    run(mail_watch.reconcile_once())

    assert amo.created == []


def test_mail_before_cutover_skipped(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_SINCE_TS", int(time.time()) - 60)
    old = int(time.time()) - 3600
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1", created_at=old)
    amo.events = [_event(101, entity_id=1, created_at=old)]

    run(mail_watch.reconcile_once())

    assert amo.created == []


def test_no_cutover_means_no_work(fresh, monkeypatch):
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_SINCE_TS", 0)
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_id=1)]

    assert run(mail_watch.reconcile_once()) == "skipped-no-cutover"
    assert amo.created == []


def test_mail_attached_to_contact_without_leads(fresh, monkeypatch):
    """Больше четверти писем amo привязывает к контакту, а не к сделке."""
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    amo.contact_leads[55] = []
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1")
    amo.events = [_event(101, entity_type="contact", entity_id=55)]

    run(mail_watch.reconcile_once())

    assert len(amo.created) == 1
    assert amo.created[0]["contact_id"] == 55
    # обратной ссылки нет: прежней сделки не существует
    assert [lead_id for lead_id, _ in amo.notes_added] == [amo.created[0]["id"]]


def test_alert_names_sender_subject_and_both_links(fresh, monkeypatch):
    """Катя просила писать про каждую созданную сделку. Значит в уведомлении должны быть
    отправитель, тема и ДВЕ ссылки: на новую сделку и на ту, где лежит само письмо."""
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_ALERT_ENABLED", True)
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1",
                               sender="client@example.com", subject="Возврат")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert len(_sent) == 1, "на сработавшее письмо должно уйти ровно одно уведомление"
    msg = _sent[0]
    new_lead = amo.created[0]["id"]
    assert "client@example.com" in msg
    assert "Возврат" in msg
    assert f"/leads/detail/{new_lead}" in msg, "нет ссылки на новую сделку"
    assert "/leads/detail/1" in msg, "нет ссылки на сделку с письмом"


def test_no_alert_when_nothing_happened(fresh, monkeypatch):
    """Робот написал — уведомления быть не должно, иначе чат перестанут читать."""
    amo = fresh
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_CREATE_ENABLED", True)
    monkeypatch.setattr(mail_watch, "MAIL_WATCH_ALERT_ENABLED", True)
    amo.leads[1] = _closed_lead(1, 55)
    amo.contact_leads[55] = [1]
    amo.notes[101] = _mail_note(101, message_id="m1", thread_id="t1",
                               sender="mailer-daemon@yandex.ru")
    amo.events = [_event(101, entity_id=1)]

    run(mail_watch.reconcile_once())

    assert _sent == []


def test_module_does_not_move_or_delete_leads():
    """Сторож только создаёт и пишет примечания. Ни перевода этапов, ни удаления."""
    src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "mail_watch.py"),
               encoding="utf-8").read()
    for forbidden in ("patch_lead", "_do_patch", "_do_delete", "delete("):
        assert forbidden not in src, f"в модуле не должно быть {forbidden}"
