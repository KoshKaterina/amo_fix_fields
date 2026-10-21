"""Тесты интеграции GetCourse → amo (getcourse_service): приём, поиск человека, выбор сделки,
оформление, повторы. Без сети: amo подменена объектом в памяти, очередь - sqlite во временной
папке. Случаи - из приёмки ТЗ «GetCourse-amoCRM-Tiana-2026-10-21»."""

import asyncio
import json
from collections import defaultdict

import pytest

import getcourse_amo_client as amo_client
import getcourse_service as gs
import getcourse_store as store
from shared.phones import digits_only

ACADEMY = 8642410
ENTRY = 87654850
OTHER_PIPELINE = 10593102


# ─────────────────────────── amo в памяти ───────────────────────────

class FakeAmo:
    def __init__(self):
        self.contacts: dict[int, dict] = {}
        self.leads: dict[int, dict] = {}
        self.notes: dict[int, list[str]] = defaultdict(list)
        self.writes: list[tuple] = []
        self.next_id = 5000
        self.create_raises = None        # исключение после фактического создания
        self.search_raises = None

    def _id(self):
        self.next_id += 1
        return self.next_id

    def add_contact(self, *, email=None, phone=None, tg=None, name="Контакт"):
        cid = self._id()
        fields = []
        if email:
            fields.append({"field_code": "EMAIL", "values": [{"value": e} for e in ([email] if isinstance(email, str) else email)]})
        if phone:
            fields.append({"field_code": "PHONE", "values": [{"value": p, "enum_id": 1} for p in ([phone] if isinstance(phone, str) else phone)]})
        if tg:
            fields.append({"field_id": 577785, "values": [{"value": tg}]})
        self.contacts[cid] = {"id": cid, "name": name, "custom_fields_values": fields, "_embedded": {"leads": []}}
        return cid

    def add_lead(self, contact_id, *, pipeline=ACADEMY, status=ENTRY, tags=(), fields=None, responsible=777):
        lid = self._id()
        self.leads[lid] = {
            "id": lid, "pipeline_id": pipeline, "status_id": status, "responsible_user_id": responsible,
            "custom_fields_values": fields or [],
            "_embedded": {"tags": [{"id": i + 1, "name": t} for i, t in enumerate(tags)]},
        }
        if contact_id:
            self.contacts[contact_id]["_embedded"]["leads"].append({"id": lid})
        return lid

    # --- интерфейс getcourse_amo_client ---

    async def search_contacts(self, query):
        if self.search_raises:
            raise self.search_raises
        q = query.lower()
        found = []
        for c in self.contacts.values():
            for f in c["custom_fields_values"]:
                for v in f["values"]:
                    text = str(v["value"]).lower()
                    if q in text or (q.isdigit() and q in digits_only(text)):
                        found.append(json.loads(json.dumps(c)))
                        break
                else:
                    continue
                break
        return found

    async def get_contact(self, cid):
        c = self.contacts.get(int(cid))
        return json.loads(json.dumps(c)) if c else None

    async def get_leads(self, ids):
        return [json.loads(json.dumps(self.leads[i])) for i in ids if i in self.leads]

    async def get_lead(self, lid):
        lead = self.leads.get(int(lid))
        return json.loads(json.dumps(lead)) if lead else None

    async def lead_note_texts(self, lid):
        return list(self.notes[int(lid)])

    async def create_lead_complex(self, body):
        self.writes.append(("create", body))
        emb = body["_embedded"]
        contact = emb["contacts"][0]
        if "id" in contact:
            cid = contact["id"]
        else:
            cid = self._id()
            self.contacts[cid] = {"id": cid, "name": contact["name"],
                                  "custom_fields_values": contact["custom_fields_values"],
                                  "_embedded": {"leads": []}}
        lid = self.add_lead(cid, pipeline=body["pipeline_id"], status=body["status_id"],
                            tags=[t["name"] for t in emb["tags"]], fields=body.get("custom_fields_values"))
        if self.create_raises:
            exc, self.create_raises = self.create_raises, None
            raise exc
        return lid, cid

    async def add_note(self, lid, text):
        self.writes.append(("note", lid))
        self.notes[int(lid)].append(text)

    async def patch_lead(self, lid, body):
        self.writes.append(("patch_lead", lid, body))
        lead = self.leads[int(lid)]
        if "_embedded" in body:
            tags = []
            by_id = {t["id"]: t for t in lead["_embedded"]["tags"]}
            for t in body["_embedded"]["tags"]:
                tags.append(by_id[t["id"]] if "id" in t else {"id": 900 + len(tags), "name": t["name"]})
            lead["_embedded"]["tags"] = tags
        for f in body.get("custom_fields_values") or []:
            lead["custom_fields_values"] = [x for x in lead["custom_fields_values"] if x["field_id"] != f["field_id"]] + [f]
        assert "status_id" not in body and "responsible_user_id" not in body

    async def patch_contact(self, cid, body):
        self.writes.append(("patch_contact", cid, body))
        c = self.contacts[int(cid)]
        for f in body["custom_fields_values"]:
            key = f.get("field_code") or f.get("field_id")
            c["custom_fields_values"] = [x for x in c["custom_fields_values"]
                                         if (x.get("field_code") or x.get("field_id")) != key] + [f]


@pytest.fixture
def fake(monkeypatch, tmp_path):
    fake = FakeAmo()
    for name in ("search_contacts", "get_contact", "get_leads", "get_lead", "lead_note_texts",
                 "create_lead_complex", "add_note", "patch_lead", "patch_contact"):
        monkeypatch.setattr(gs.amo, name, getattr(fake, name))
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "gc.sqlite3"))
    store.init_db()
    alerts = []

    async def _alert(key, text):
        alerts.append((key, text))

    monkeypatch.setattr(gs, "_alert", _alert)
    monkeypatch.setattr(gs, "GETCOURSE_DRY_RUN", False)
    monkeypatch.setattr(gs, "GETCOURSE_ONLY_USER_IDS", set())
    monkeypatch.setattr(gs, "GETCOURSE_PIPELINE_ID", ACADEMY)
    monkeypatch.setattr(gs, "GETCOURSE_STATUS_ID", ENTRY)
    monkeypatch.setattr(gs, "GETCOURSE_LEAD_ORDER_FIELD_ID", 600001)
    monkeypatch.setattr(gs, "GETCOURSE_GC_LEAD_FIELD", "")
    fake.alerts = alerts
    return fake


def run(coro):
    return asyncio.run(coro)


def payload(**over):
    data = {
        "event": "order_created",
        "order_id": "910002423",
        "order_number": "20619",
        "status": "Завершен",
        "positions": "Живая онлайн-конференция Sunscrypt Academy",
        "cost": "0",
        "page_url": "https://edu.sunscrypt.ru/anketa/strategy",
        "user_id": "519862979",
        "email": "Ivan@Example.com",
        "phone": "+7 (927) 658-99-08",
        "first_name": "Иван",
        "telegram": "",
        "deal_utm_source": "yandex",
        "deal_utm_campaign": "{object.deal_utm_campaign}",
        "gc_utm_campaign": "strategy_oct",
        "a_experience": "Есть базовый опыт",
    }
    data.update(over)
    return data


def accept_and_process(data, now=None):
    status, body = run(gs.accept(data))
    assert status == 200, body
    run(gs.run_due(now))
    ev = gs.parse(data)
    return store.get(f"{ev['order_id']}:{ev['event']}")


# ─────────────────────────── разбор ───────────────────────────

def test_parse_drops_placeholders_and_keeps_answers():
    ev = gs.parse(payload(a_goals="{object.Цели}", answers={"level": "новичок"}))
    assert "deal_utm_campaign" not in ev                  # неподставленная переменная = пусто
    assert ev["a_experience"] == "Есть базовый опыт"
    assert ev["a_level"] == "новичок"
    assert "a_goals" not in ev
    assert gs.utm_of(ev) == {"utm_source": "yandex", "utm_campaign": "strategy_oct"}


@pytest.mark.parametrize("bad, code", [
    ({"event": "order_deleted"}, "unknown-event"),
    ({"order_id": ""}, "no-order-id"),
    ({"order_id": "{object.id}"}, "no-order-id"),
    ({"order_id": "12a"}, "no-order-id"),
])
def test_parse_rejects(bad, code):
    with pytest.raises(ValueError, match=code):
        gs.parse(payload(**bad))


def test_accept_validation_and_duplicate(fake):
    assert run(gs.accept({"order_id": "x"}))[0] == 422
    assert run(gs.accept(payload())) == (200, {"ok": True, "status": "accepted"})
    assert run(gs.accept(payload())) == (200, {"ok": True, "status": "duplicate"})


@pytest.mark.parametrize("raw, expected", [
    ("+7 (927) 658-99-08", "79276589908"),
    ("89276589908", "79276589908"),
    ("9276589908", "79276589908"),
    ("375296574574", "375296574574"),
    ("12345", ""),
    ("", ""),
])
def test_norm_phone(raw, expected):
    assert gs.norm_phone(raw) == expected


def test_telegram_field_with_phone_becomes_phone():
    keys = gs._keys({"telegram": "+7 927 658 99 08"})
    assert keys == {"phone": "79276589908"}


# ─────────────────────────── новый и известный человек ───────────────────────────

def test_new_person_gets_new_deal_and_contact(fake):
    row = accept_and_process(payload(telegram="@Ivan_Defi"))
    assert row["status"] == "done" and row["action"] == "created"
    lead = fake.leads[row["lead_id"]]
    assert (lead["pipeline_id"], lead["status_id"]) == (ACADEMY, ENTRY)
    assert [t["name"] for t in lead["_embedded"]["tags"]] == ["GetCourse", "реклама", "ГК: конференция"]
    fields = {f["field_id"]: f["values"][0]["value"] for f in lead["custom_fields_values"]}
    assert fields[559303] == "yandex" and fields[559307] == "strategy_oct" and fields[600001] == "20619"
    create = [w for w in fake.writes if w[0] == "create"][0][1]
    assert "responsible_user_id" not in create              # раздаёт распределитель по источнику
    contact = fake.contacts[row["contact_id"]]
    values = {f.get("field_code") or f.get("field_id"): f["values"][0]["value"] for f in contact["custom_fields_values"]}
    assert values == {"EMAIL": "ivan@example.com", "PHONE": "+79276589908", 577785: "@ivan_defi"}
    note = fake.notes[row["lead_id"]][0]
    assert note.startswith("[GC:910002423:order_created]")
    assert "/sales/control/deal/update/id/910002423" in note and "utm_source=yandex" in note
    assert "Какой у вас сейчас опыт в криптовалюте и DeFi?: Есть базовый опыт" in note
    assert store.contact_for("519862979") == row["contact_id"]
    assert store.get("910002423:order_created")["payload"] is None   # персональные данные стёрты


def test_existing_open_deal_is_augmented_not_moved(fake):
    cid = fake.add_contact(email="ivan@example.com")
    lid = fake.add_lead(cid, status=88882518, tags=["был на вебе 4"],
                        fields=[{"field_id": 559303, "values": [{"value": "telegram"}]}])
    row = accept_and_process(payload())
    assert row["action"] == "augmented" and row["lead_id"] == lid
    lead = fake.leads[lid]
    assert lead["status_id"] == 88882518 and lead["responsible_user_id"] == 777
    assert [t["name"] for t in lead["_embedded"]["tags"]] == ["был на вебе 4", "GetCourse", "реклама", "ГК: конференция"]
    fields = {f["field_id"]: f["values"][0]["value"] for f in lead["custom_fields_values"]}
    assert fields[559303] == "telegram"                     # первая атрибуция не затёрта
    assert fields[559307] == "strategy_oct"                 # пустое поле заполнено
    assert len(fake.notes[lid]) == 1
    # В контакт дописан телефон, почта сохранена.
    contact = fake.contacts[cid]
    by_code = {f.get("field_code"): f["values"] for f in contact["custom_fields_values"]}
    assert [v["value"] for v in by_code["PHONE"]] == ["+79276589908"]
    assert [v["value"] for v in by_code["EMAIL"]] == ["ivan@example.com"]
    assert not [w for w in fake.writes if w[0] == "create"]


def test_existing_phone_kept_when_new_phone_added(fake):
    cid = fake.add_contact(email="ivan@example.com", phone="+7 999 000-11-22")
    fake.add_lead(cid)
    accept_and_process(payload())
    phones = [f for f in fake.contacts[cid]["custom_fields_values"] if f.get("field_code") == "PHONE"][0]["values"]
    assert [p["value"] for p in phones] == ["+7 999 000-11-22", "+79276589908"]
    assert phones[0]["enum_id"] == 1


def test_closed_and_foreign_deals_lead_to_new_academy_deal_on_same_contact(fake):
    cid = fake.add_contact(email="ivan@example.com")
    closed = fake.add_lead(cid, status=142)
    foreign = fake.add_lead(cid, pipeline=OTHER_PIPELINE, status=1)
    row = accept_and_process(payload())
    assert row["action"] == "created" and row["contact_id"] == cid
    assert row["lead_id"] not in (closed, foreign)
    assert fake.leads[closed]["status_id"] == 142 and not fake.notes[closed]


def test_international_phone_is_not_matched_by_tail(fake):
    other = fake.add_contact(phone="+7 050 123-45-67")
    row = accept_and_process(payload(email="", phone="380501234567"))
    assert row["action"] == "created" and row["contact_id"] != other


def test_same_event_twice_does_not_duplicate_note(fake):
    cid = fake.add_contact(email="ivan@example.com")
    lid = fake.add_lead(cid)
    accept_and_process(payload())
    # Тот же заказ ещё раз (сверка, ручной перезапуск процесса) - строки нет, примечание одно.
    with store._connect() as conn:
        conn.execute("DELETE FROM getcourse_events")
    accept_and_process(payload())
    assert len(fake.notes[lid]) == 1


def test_gc_user_link_finds_contact_after_email_change(fake):
    cid = fake.add_contact(email="old@example.com")
    lid = fake.add_lead(cid)
    store.link_contact("519862979", cid)
    row = accept_and_process(payload(email="new@example.com", phone=""))
    assert row["lead_id"] == lid and row["action"] == "augmented"


# ─────────────────────────── неоднозначности ───────────────────────────

def test_several_open_deals_wait_for_dedup_then_review(fake):
    cid = fake.add_contact(email="ivan@example.com")
    fake.add_lead(cid)
    fake.add_lead(cid)
    row = accept_and_process(payload())
    assert row["status"] == "pending" and row["last_error"] == "several-open-deals"
    far = row["next_try_at"] + 1
    run(gs.run_due(far))
    row = store.get(row["event_id"])
    assert row["status"] == "pending"
    run(gs.run_due(row["next_try_at"] + 1))
    row = store.get(row["event_id"])
    assert row["status"] == "review" and row["action"] == "several-open-deals"
    assert fake.alerts and fake.alerts[-1][0] == "getcourse_review"
    assert "ivan" not in fake.alerts[-1][1].lower()        # в алерте нет персональных данных
    assert not fake.writes


def test_keys_pointing_to_different_people_go_to_review(fake):
    fake.add_contact(email="ivan@example.com", phone="+7 911 111-11-11")
    fake.add_contact(email="petr@example.com", phone="+7 927 658-99-08")
    row = accept_and_process(payload())
    assert row["status"] == "review" and row["action"] == "conflict-keys"
    assert not fake.writes


def test_duplicate_contacts_of_same_person_are_not_a_conflict(fake):
    fake.add_contact(email="ivan@example.com")             # без телефона
    b = fake.add_contact(phone="+7 927 658-99-08")         # без почты
    lid = fake.add_lead(b)
    row = accept_and_process(payload())
    assert row["action"] == "augmented" and row["lead_id"] == lid


def test_shared_telegram_without_other_keys_goes_to_review(fake):
    fake.add_contact(tg="@family_acc")
    fake.add_contact(tg="@family_acc")
    row = accept_and_process(payload(email="", phone="", telegram="family_acc"))
    assert row["action"] == "shared-telegram"


def test_no_keys_goes_to_review(fake):
    row = accept_and_process(payload(email="", phone="", telegram="Иван"))
    assert row["status"] == "review" and row["action"] == "no-keys"


# ─────────────────────────── оплата и заказы без формы ───────────────────────────

def test_payment_without_open_deal_creates_nothing(fake):
    row = accept_and_process(payload(event="order_paid", payed="10000"))
    assert row["status"] == "skipped" and row["action"] == "paid-without-open-deal"
    assert not fake.writes


def test_payment_with_open_deal_adds_note_and_tag(fake):
    cid = fake.add_contact(email="ivan@example.com")
    lid = fake.add_lead(cid, status=88464046)
    row = accept_and_process(payload(event="order_paid", payed="10000", payed_at="2026-10-21 12:00"))
    assert row["action"] == "augmented"
    assert "ГК: оплата" in [t["name"] for t in fake.leads[lid]["_embedded"]["tags"]]
    assert fake.leads[lid]["status_id"] == 88464046
    assert "оплачено 10000" in fake.notes[lid][0]


def test_manual_order_only_augments(fake):
    row = accept_and_process(payload(page_url=""))
    assert row["status"] == "skipped" and row["action"] == "manual-order-without-open-deal"
    cid = fake.add_contact(email="ivan@example.com")
    lid = fake.add_lead(cid)
    row = accept_and_process(payload(page_url="", order_id="910002424"))
    assert row["action"] == "augmented"
    assert "ГК: заказ без формы" in [t["name"] for t in fake.leads[lid]["_embedded"]["tags"]]


def test_new_form_alerts_once(fake):
    accept_and_process(payload(page_url="https://edu.sunscrypt.ru/anketa/new-form", positions="Новый курс"))
    accept_and_process(payload(page_url="https://edu.sunscrypt.ru/anketa/new-form?x=1", order_id="910002425",
                               email="other@example.com", phone="", positions="Новый курс"))
    new_form = [a for a in fake.alerts if a[0] == "getcourse_new_form"]
    assert len(new_form) == 1 and "/anketa/new-form" in new_form[0][1]


# 21.10.2026: заказы с формы пришли без deal_created_url - GetCourse пишет его не всегда.

def test_client_order_without_page_url_creates_deal(fake):
    row = accept_and_process(payload(page_url="", origin="client"))
    assert row["status"] == "done" and row["action"] == "created"
    lead = fake.leads[row["lead_id"]]
    # Форма узнана по предложению «Живая онлайн-конференция…».
    assert [t["name"] for t in lead["_embedded"]["tags"]] == ["GetCourse", "реклама", "ГК: конференция"]
    note = fake.notes[row["lead_id"]][0]
    assert "Форма: Анкета strategy (онлайн-конференция) (адрес страницы GetCourse не передал)" in note
    assert not [a for a in fake.alerts if a[0] == "getcourse_new_form"]


def test_defi_channel_offer_without_page_url(fake):
    row = accept_and_process(payload(page_url="", origin="client",
                                     positions="Анкета-заявка на доступ в закрытый DeFi-канал [10.26]"))
    names = [t["name"] for t in fake.leads[row["lead_id"]]["_embedded"]["tags"]]
    assert names == ["GetCourse", "ГК: анкета DeFi-канала"]
    assert "ГК #20619: Анкета-заявка на доступ в DeFi-канал" in fake.writes[0][1]["name"]


def test_client_order_with_unknown_offer_alerts_once(fake):
    accept_and_process(payload(page_url="", origin="client", positions="Совсем новый продукт"))
    accept_and_process(payload(page_url="", origin="client", positions="Совсем новый продукт",
                               order_id="910002426", email="other@example.com", phone=""))
    new_form = [a for a in fake.alerts if a[0] == "getcourse_new_form"]
    assert len(new_form) == 1 and "Совсем новый продукт" in new_form[0][1]


def test_order_without_page_url_and_origin_is_manual(fake):
    row = accept_and_process(payload(page_url="", origin=""))
    assert row["status"] == "skipped" and row["action"] == "manual-order-without-open-deal"


# ─────────────────────────── сбои ───────────────────────────

def test_lost_create_response_is_resolved_not_duplicated(fake):
    fake.create_raises = amo_client.AmoUncertain("ReadTimeout")
    row = accept_and_process(payload())
    assert row["status"] == "pending" and row["create_started"] == 1 and row["lead_id"] is None
    assert row["next_try_at"] - row["updated_at"] >= gs.UNCERTAIN_DELAY_S - 1
    assert len(fake.leads) == 1                             # сделка в amo на самом деле есть
    run(gs.run_due(row["next_try_at"] + 1))
    row = store.get(row["event_id"])
    assert row["status"] == "done" and row["action"] == "augmented"
    assert len(fake.leads) == 1 and len([w for w in fake.writes if w[0] == "create"]) == 1
    assert len(fake.notes[row["lead_id"]]) == 1


def test_amo_unavailable_retries_without_writes(fake):
    fake.search_raises = amo_client.AmoUnavailable("GET: 503")
    row = accept_and_process(payload())
    assert row["status"] == "pending" and row["attempts"] == 1
    assert row["last_error"].startswith("unavailable")
    assert not fake.writes


def test_amo_refusal_fails_with_alert(fake, monkeypatch):
    async def refuse(body):
        raise amo_client.AmoError(400, "bad field")

    monkeypatch.setattr(gs.amo, "create_lead_complex", refuse)
    row = accept_and_process(payload())
    assert row["status"] == "failed" and row["action"] == "amo-400"
    assert fake.alerts[-1][0] == "getcourse_failed"


def test_dry_run_writes_nothing(fake, monkeypatch):
    monkeypatch.setattr(gs, "GETCOURSE_DRY_RUN", True)
    row = accept_and_process(payload())
    assert row["status"] == "done" and row["action"] == "dry:create with new contact"
    assert not fake.writes


def test_test_filter_skips_other_users(fake, monkeypatch):
    monkeypatch.setattr(gs, "GETCOURSE_ONLY_USER_IDS", {"1"})
    row = accept_and_process(payload())
    assert row["status"] == "skipped" and row["action"] == "test-filter"


def test_mirror_writes_lead_id_into_getcourse(fake, monkeypatch):
    calls = []

    async def set_field(number, email, field, value):
        calls.append((number, email, field, value))
        return True

    monkeypatch.setattr(gs, "GETCOURSE_GC_LEAD_FIELD", "ИД сделки amo")
    monkeypatch.setattr(gs.gc, "configured", lambda: True)
    monkeypatch.setattr(gs.gc, "set_deal_field", set_field)
    row = accept_and_process(payload())
    assert calls == [("20619", "ivan@example.com", "ИД сделки amo", str(row["lead_id"]))]
    assert row["last_error"] is None


def test_mirror_failure_does_not_undo_deal(fake, monkeypatch):
    async def set_field(*args):
        raise gs.gc.GcUnavailable("deals add: ConnectError")

    monkeypatch.setattr(gs, "GETCOURSE_GC_LEAD_FIELD", "ИД сделки amo")
    monkeypatch.setattr(gs.gc, "configured", lambda: True)
    monkeypatch.setattr(gs.gc, "set_deal_field", set_field)
    row = accept_and_process(payload())
    assert row["status"] == "done" and row["last_error"].startswith("gc-mirror")


# ─────────────────────────── сверка ───────────────────────────

def test_reconcile_alerts_missing_orders_once(fake, monkeypatch):
    accept_and_process(payload())
    monkeypatch.setattr(gs, "GETCOURSE_RECONCILE_FROM", "2026-10-21")
    monkeypatch.setattr(gs.gc, "configured", lambda: True)

    async def start(since):
        return 1

    async def fetch(export_id):
        return (["ID заказа", "Номер", "Дата создания"],
                [["910002423", "20619", "2026-10-21 10:00:00"],
                 ["910002999", "20700", "2026-10-21 10:05:00"],
                 ["910003000", "20701", "2999-01-01 00:00:00"]])   # свежий - не трогаем

    monkeypatch.setattr(gs.gc, "start_deals_export", start)
    monkeypatch.setattr(gs.gc, "fetch_export", fetch)
    first = run(gs.reconcile_once(poll_interval_s=0, polls=1))
    second = run(gs.reconcile_once(poll_interval_s=0, polls=1))
    assert first == {"checked": 2, "missing": 1, "alerted": 1}
    assert second["alerted"] == 0
    missing = [a for a in fake.alerts if a[0] == "getcourse_missing"]
    assert len(missing) == 1 and "#20700" in missing[0][1]


def test_recover_stale_returns_processing_with_pause(fake):
    run(gs.accept(payload()))
    assert store.claim("910002423:order_created")
    assert store.recover_stale(0, 900, now=10**10) == 1
    row = store.get("910002423:order_created")
    assert row["status"] == "pending" and row["next_try_at"] == 10**10 + 900
