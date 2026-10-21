"""Тесты самоисцеления сделок amgroup без контакта (amgroup_contact_heal).

Написаны по сбоям 20-21.10.2026: amgroup создал сделку из заказа МС и не привязал
контакт, когда API amo отдавал 500, таймауты и 429. Развилки: один кандидат,
ноль, несколько, amgroup успел сам, и главное - сбой amo или склада читается
как «не знаю» (ничего не пишем, повторим), а не как «контакта нет».

Подмены - только через monkeypatch на объектах, которые держит код под тестом
(heal.amo_service, heal.ms_client, ...): соседям по процессу ничего не достаётся.

Запуск: python run_tests.py test_amgroup_contact_heal.py
"""

import asyncio
import os
import sys
import time

import httpx
import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import amgroup_contact_heal as heal  # noqa: E402
import amo_service  # noqa: E402
import lead_distribution as ld  # noqa: E402

AGENT = "0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"
LEAD_ID = 36583813


def _lead(*, contacts=(), age_min=10, created_by_enum=heal.ENUM_CREATED_BY_MS, agent=AGENT, status_id=1):
    cf = [{"field_id": heal.FIELD_ORDER_NUMBER, "values": [{"value": "19805"}]}]
    if created_by_enum is not None:
        cf.append({"field_id": heal.FIELD_CREATED_BY, "values": [{"value": "Из МойСклад", "enum_id": created_by_enum}]})
    if agent is not None:
        cf.append({"field_id": heal.FIELD_MS_AGENT_UUID, "values": [{"value": agent}]})
    return {
        "id": LEAD_ID,
        "status_id": status_id,
        "created_at": int(time.time() - age_min * 60),
        "custom_fields_values": cf,
        "_embedded": {"contacts": [{"id": c} for c in contacts]},
    }


def _contact(cid, *, phones=(), emails=()):
    cf = []
    if phones:
        cf.append({"field_code": "PHONE", "values": [{"value": p} for p in phones]})
    if emails:
        cf.append({"field_code": "EMAIL", "values": [{"value": e} for e in emails]})
    return {"id": cid, "custom_fields_values": cf}


class Fake:
    """Мир amo + МС для одного теста. leads - очередь снимков сделки: каждый
    get_lead_full забирает следующий (последний повторяется)."""

    def __init__(self, monkeypatch, *, leads, agent=None, search=None, link_result=None):
        self.leads = list(leads)
        self.agent = agent if agent is not None else {"phone": "+7 (912) 345-67-89", "email": "Client@Example.ru"}
        self.search = search if search is not None else {}
        self.link_result = link_result or {"ok": True, "status_code": 200, "retryable": False}
        self.reads = 0
        self.links: list[tuple] = []
        self.notes: list[str] = []
        self.alerts: list[str] = []
        self.queries: list[str] = []
        self.ms_calls: list[str] = []
        self.distributed: list[tuple] = []

        async def get_lead_full(lead_id, with_=()):
            self.reads += 1
            idx = min(self.reads - 1, len(self.leads) - 1)
            return self.leads[idx]

        async def find_contacts_by_query(query, limit=10):
            self.queries.append(query)
            value = self.search.get(query, [])
            return value(query) if callable(value) else value

        async def link_contact_to_lead(lead_id, contact_id, *, is_main=True):
            self.links.append((lead_id, contact_id, is_main))
            return self.link_result

        async def add_note(lead_id, text):
            self.notes.append(text)
            return {"ok": True}

        async def ms_get(path, params=None):
            self.ms_calls.append(path)
            return self.agent

        async def send_alert(text, **kw):
            self.alerts.append(text)
            return True

        async def process(lead_id, source="webhook", **kw):
            self.distributed.append((lead_id, source))
            return "routed"

        monkeypatch.setattr(heal.amo_service, "get_lead_full", get_lead_full)
        monkeypatch.setattr(heal.amo_service, "find_contacts_by_query", find_contacts_by_query)
        monkeypatch.setattr(heal.amo_service, "link_contact_to_lead", link_contact_to_lead)
        monkeypatch.setattr(heal.amo_service, "add_note", add_note)
        monkeypatch.setattr(heal.ms_client, "get", ms_get)
        monkeypatch.setattr(heal.telegram_bot, "send_alert", send_alert)
        monkeypatch.setattr(heal.alerts, "decide", lambda key, *, legacy_text, values, keep_text=False, **kw:
                            type("D", (), {"text": legacy_text, "send_kwargs": lambda self: {}})())
        monkeypatch.setattr(heal.lead_distribution, "process_lead_distribution", process)

    def run(self, source="scan"):
        return asyncio.run(heal.heal_lead(LEAD_ID, source=source))


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    monkeypatch.setattr(heal, "AMGROUP_CONTACT_HEAL_ENABLED", True)
    monkeypatch.setattr(heal, "AMGROUP_CONTACT_HEAL_DRY_RUN", False)
    monkeypatch.setattr(heal, "AMGROUP_CONTACT_HEAL_MIN_AGE_MIN", 5)
    monkeypatch.setattr(heal, "_STATE_PATH", str(tmp_path / "heal.json"))
    monkeypatch.setattr(heal, "_state_loaded", False)
    monkeypatch.setattr(heal, "_PAUSE_SEC", 0)
    monkeypatch.setattr(heal, "_SCHEDULE_MARGIN_S", 0)
    heal._state.clear()
    heal._dry_logged.clear()
    heal._in_flight.clear()
    yield
    heal._state.clear()
    heal._dry_logged.clear()


PHONE_Q = "79123456789"
EMAIL_Q = "client@example.ru"


# ════════════════ один кандидат ════════════════

def test_odin_kandidat_po_telefonu_privyazyvaem_i_raspredelyaem(monkeypatch):
    fake = Fake(monkeypatch,
                leads=[_lead(), _lead(), _lead(contacts=[48559931])],
                search={PHONE_Q: [_contact(48559931, phones=["+7 912 345-67-89"])]})

    assert fake.run() == heal.LINKED
    assert fake.links == [(LEAD_ID, 48559931, True)]
    assert len(fake.notes) == 1 and "привязан автоматически" in fake.notes[0]
    assert "48559931" in fake.notes[0]
    assert not fake.alerts
    assert fake.distributed == [(LEAD_ID, "contact_heal")]
    assert fake.ms_calls == [f"entity/counterparty/{AGENT}"]
    # Завершённый исход запомнен: повторный заход не делает ничего.
    assert fake.run() == heal.ALREADY_HANDLED
    assert len(fake.links) == 1


def test_telefon_sverka_po_poslednim_10_cifram(monkeypatch):
    """В МС «8 912…», у контакта «+7 912…» - один и тот же номер."""
    fake = Fake(monkeypatch,
                leads=[_lead(), _lead(), _lead(contacts=[777])],
                agent={"phone": "8 (912) 345-67-89", "email": ""},
                search={PHONE_Q: [_contact(777, phones=["+7 912 345 67 89"])]})
    assert fake.run() == heal.LINKED
    assert fake.queries == [PHONE_Q]


def test_odin_kontakt_sovpal_i_po_telefonu_i_po_pochte_eto_odin_kandidat(monkeypatch):
    c = _contact(44287675, phones=["+79123456789"], emails=["client@example.ru"])
    fake = Fake(monkeypatch, leads=[_lead(), _lead(), _lead(contacts=[44287675])],
                search={PHONE_Q: [c], EMAIL_Q: [c]})
    assert fake.run() == heal.LINKED
    assert fake.links == [(LEAD_ID, 44287675, True)]


def test_shum_polnotekstovogo_poiska_ne_kandidat(monkeypatch):
    """Полнотекстовый поиск вернул контакт, но значение поля не совпало - это не кандидат."""
    fake = Fake(monkeypatch, leads=[_lead()],
                agent={"phone": "+79123456789", "email": ""},
                search={PHONE_Q: [_contact(1, phones=["+79123456780"])]})
    assert fake.run() == heal.NO_CANDIDATE
    assert not fake.links


# ════════════════ ноль и несколько кандидатов: только позвать человека ════════════════

def test_nol_kandidatov_kontakt_ne_sozdaem_tolko_alert(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead()], search={PHONE_Q: [], EMAIL_Q: []})

    assert fake.run() == heal.NO_CANDIDATE
    assert not fake.links
    assert len(fake.notes) == 1 and "не найден" in fake.notes[0]
    assert len(fake.alerts) == 1 and str(LEAD_ID) in fake.alerts[0]
    assert "19805" in fake.alerts[0]
    assert not fake.distributed
    # Алерт один раз: следующий проход сделку уже не трогает.
    assert fake.run() == heal.ALREADY_HANDLED
    assert len(fake.alerts) == 1


def test_neskolko_kandidatov_tolko_alert(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead()],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])],
                        EMAIL_Q: [_contact(2, emails=["CLIENT@example.ru"])]})

    assert fake.run() == heal.AMBIGUOUS
    assert not fake.links
    assert len(fake.alerts) == 1
    assert "contacts/detail/1" in fake.alerts[0] and "contacts/detail/2" in fake.alerts[0]
    assert "несколько" in fake.notes[0]


def test_u_kontragenta_net_ni_telefona_ni_pochty(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead()], agent={"phone": "", "email": "нет"})
    assert fake.run() == heal.NO_IDENTIFIERS
    assert not fake.queries
    assert len(fake.alerts) == 1


# ════════════════ amgroup успел сам ════════════════

def test_amgroup_privyazal_do_nas(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(contacts=[5])])
    assert fake.run() == heal.ALREADY_LINKED
    assert not fake.ms_calls and not fake.links and not fake.notes
    assert not fake.distributed  # фоновый проход - распределение не зовём


def test_amgroup_privyazal_poka_my_iskali(monkeypatch):
    """Первое чтение - без контакта, перечитывание перед записью - контакт уже есть."""
    fake = Fake(monkeypatch, leads=[_lead(), _lead(contacts=[5])],
                search={PHONE_Q: [_contact(48559931, phones=["+79123456789"])]})
    assert fake.run() == heal.ALREADY_LINKED
    assert not fake.links and not fake.notes and not fake.alerts
    assert not heal._handled(LEAD_ID)


def test_amgroup_privyazal_drugoy_kontakt_odnovremenno_s_nami(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(), _lead(), _lead(contacts=[5])],
                search={PHONE_Q: [_contact(48559931, phones=["+79123456789"])]},
                link_result={"ok": False, "status_code": None, "retryable": True})
    assert fake.run() == heal.ALREADY_LINKED
    assert not fake.notes and not fake.alerts


def test_iz_ozhidaniya_raspredeleniya_amgroup_privyazal_pozdno_raspredelenie_zovem(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(contacts=[5])])
    assert fake.run(source="contact_wait") == heal.ALREADY_LINKED
    assert fake.distributed == [(LEAD_ID, "contact_heal")]


# ════════════════ сбои amo и склада = «не знаю» ════════════════

def test_poisk_kontakta_ne_otvetil_ne_znayu(monkeypatch):
    """find_contacts_by_query вернул None (5xx/таймаут/брейкер) - не «ноль кандидатов»."""
    fake = Fake(monkeypatch, leads=[_lead()], search={PHONE_Q: None})
    assert fake.run() == heal.UNKNOWN_SEARCH
    assert not fake.links and not fake.notes and not fake.alerts
    assert not heal._handled(LEAD_ID)


def test_poisk_po_pochte_ne_otvetil_dazhe_esli_po_telefonu_odin(monkeypatch):
    """Неполная выдача могла спрятать второго кандидата - «ровно один» не доказан."""
    fake = Fake(monkeypatch, leads=[_lead()],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])], EMAIL_Q: None})
    assert fake.run() == heal.UNKNOWN_SEARCH
    assert not fake.links


def test_sdelka_ne_prochitalas_ne_znayu(monkeypatch):
    fake = Fake(monkeypatch, leads=[None])
    assert fake.run() == heal.UNKNOWN_LEAD
    assert not fake.ms_calls and not fake.notes


def test_pustoy_otvet_vmesto_sdelki_ne_znayu(monkeypatch):
    """204 у _do_get превращается в {} - это не «сделка без контакта»."""
    fake = Fake(monkeypatch, leads=[{}])
    assert fake.run() == heal.UNKNOWN_LEAD


def test_pereschityvanie_pered_zapisyu_ne_prochitalos(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(), None],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])]})
    assert fake.run() == heal.UNKNOWN_LEAD
    assert not fake.links


def test_sklad_ne_otvetil_ne_znayu(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead()])
    fake.agent = None

    async def ms_get(path, params=None):
        return None

    monkeypatch.setattr(heal.ms_client, "get", ms_get)
    assert fake.run() == heal.UNKNOWN_MS
    assert not fake.queries and not fake.alerts
    assert not heal._handled(LEAD_ID)


def test_privyazka_po_taymautu_no_proshla_eto_uspeh(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(), _lead(), _lead(contacts=[1])],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])]},
                link_result={"ok": False, "status_code": None, "retryable": True})
    assert fake.run() == heal.LINKED
    assert len(fake.notes) == 1


def test_privyazka_po_taymautu_ne_proshla_povtorim(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead()],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])]},
                link_result={"ok": False, "status_code": 503, "retryable": True})
    assert fake.run() == heal.UNKNOWN_LINK
    assert not fake.notes and not fake.alerts
    assert not heal._handled(LEAD_ID)


def test_privyazka_otvergnuta_zovem_cheloveka(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead()],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])]},
                link_result={"ok": False, "status_code": 400, "retryable": False})
    assert fake.run() == heal.LINK_FAILED
    assert len(fake.alerts) == 1


def test_privyazka_ok_no_proverka_ne_prochitalas(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(), _lead(), None],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])]})
    assert fake.run() == heal.LINK_UNVERIFIED
    assert len(fake.alerts) == 1
    assert not fake.distributed


# ════════════════ гейты ════════════════

def test_suhoy_rezhim_nichego_ne_pishet(monkeypatch, caplog):
    monkeypatch.setattr(heal, "AMGROUP_CONTACT_HEAL_DRY_RUN", True)
    fake = Fake(monkeypatch, leads=[_lead()],
                search={PHONE_Q: [_contact(1, phones=["+79123456789"])]})
    with caplog.at_level("WARNING", logger="uvicorn"):
        assert fake.run() == heal.DRY_RUN_WOULD_LINK
    assert not fake.links and not fake.notes and not fake.alerts and not fake.distributed
    assert not heal._handled(LEAD_ID)
    assert any("привязали бы контакт 1" in r.getMessage() for r in caplog.records)


def test_suhoy_rezhim_nol_kandidatov_bez_alerta(monkeypatch):
    monkeypatch.setattr(heal, "AMGROUP_CONTACT_HEAL_DRY_RUN", True)
    fake = Fake(monkeypatch, leads=[_lead()])
    assert fake.run() == heal.NO_CANDIDATE
    assert not fake.notes and not fake.alerts
    assert not heal._handled(LEAD_ID)


def test_vyklyuchen_flag_nichego_ne_chitaet(monkeypatch):
    monkeypatch.setattr(heal, "AMGROUP_CONTACT_HEAL_ENABLED", False)
    fake = Fake(monkeypatch, leads=[_lead()])
    assert fake.run() == heal.DISABLED
    assert fake.reads == 0


def test_molodaya_sdelka_ne_trogaem(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(age_min=1)])
    assert fake.run() == heal.TOO_YOUNG
    assert not fake.ms_calls


def test_sdelka_ne_iz_moysklada_ne_trogaem(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(created_by_enum=1040193)])
    assert fake.run() == heal.NOT_MS_LEAD
    fake = Fake(monkeypatch, leads=[_lead(agent=None)])
    assert fake.run() == heal.NOT_MS_LEAD
    fake = Fake(monkeypatch, leads=[_lead(agent="../../entity/x")])
    assert fake.run() == heal.NOT_MS_LEAD
    assert not fake.ms_calls


def test_zakrytaya_sdelka_ne_trogaem(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead(status_id=143)])
    assert fake.run() == heal.CLOSED


def test_sostoyanie_perezhivaet_perezapusk(monkeypatch):
    fake = Fake(monkeypatch, leads=[_lead()], search={PHONE_Q: [], EMAIL_Q: []})
    assert fake.run() == heal.NO_CANDIDATE
    heal._state.clear()
    monkeypatch.setattr(heal, "_state_loaded", False)
    assert heal._handled(LEAD_ID)


# ════════════════ фоновый проход ════════════════

def test_prohod_amo_ne_otdal_sdelki(monkeypatch):
    async def created_between(ts_from, ts_to, with_=(), page_limit=250):
        return None

    async def boom(*a, **kw):
        pytest.fail("heal_lead не должен вызываться")

    monkeypatch.setattr(heal.amo_service, "get_leads_created_between", created_between)
    monkeypatch.setattr(heal, "heal_lead", boom)
    assert asyncio.run(heal.check_once()) == {"amo_answered": False, "candidates": 0, "outcomes": {}}


def test_prohod_vybiraet_tolko_sdelki_iz_ms_bez_kontakta(monkeypatch):
    def with_id(lead, lid):
        lead["id"] = lid
        return lead

    leads = [
        with_id(_lead(), 1),                          # кандидат
        with_id(_lead(contacts=[9]), 2),               # контакт есть
        with_id(_lead(created_by_enum=None), 3),       # не из МС
        with_id(_lead(age_min=1), 4),                  # моложе порога
        with_id(_lead(status_id=142), 5),              # закрыта
    ]
    windows = []

    async def created_between(ts_from, ts_to, with_=(), page_limit=250):
        windows.append((ts_from, ts_to, with_))
        return leads

    healed = []

    async def fake_heal(lead_id, *, source="scan"):
        healed.append((lead_id, source))
        return heal.NO_CANDIDATE

    monkeypatch.setattr(heal.amo_service, "get_leads_created_between", created_between)
    monkeypatch.setattr(heal, "heal_lead", fake_heal)
    result = asyncio.run(heal.check_once())
    assert healed == [(1, "scan")]
    assert result == {"amo_answered": True, "candidates": 1, "outcomes": {heal.NO_CANDIDATE: 1}}
    assert windows[0][2] == ("contacts",)
    assert windows[0][1] - windows[0][0] == (heal.AMGROUP_CONTACT_HEAL_LOOKBACK_H * 60 - 5) * 60


# ════════════════ вход из lead_distribution._contact_wait_loop ════════════════

def test_ozhidanie_kontakta_istekl0_zovet_samoiscelenie(monkeypatch):
    lead = _lead()

    async def get_lead_full(lead_id, with_=()):
        return lead

    async def fail(lead, reason):
        return None

    scheduled = []
    monkeypatch.setattr(ld.amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(ld, "_fail", fail)
    monkeypatch.setattr(ld, "match_profile", lambda *a: None)
    monkeypatch.setattr(ld, "LEAD_DISTRIBUTION_CONTACT_WAIT_S", 0.02)
    monkeypatch.setattr(ld, "LEAD_DISTRIBUTION_CONTACT_POLL_S", 0.005)
    monkeypatch.setattr(heal, "schedule_after_contact_wait", lambda l: scheduled.append(l["id"]))
    asyncio.run(ld._contact_wait_loop(LEAD_ID, "webhook"))
    assert scheduled == [LEAD_ID]


def test_otlozhennyy_vyzov_zhdet_porog_i_lechit(monkeypatch):
    calls = []

    async def fake_heal(lead_id, *, source="scan"):
        calls.append((lead_id, source))
        return heal.LINKED

    monkeypatch.setattr(heal, "heal_lead", fake_heal)

    async def main():
        heal.schedule_after_contact_wait(_lead(age_min=10))
        await asyncio.gather(*heal._bg_tasks)

    asyncio.run(main())
    assert calls == [(LEAD_ID, "contact_wait")]


def test_otlozhennyy_vyzov_ne_dlya_sdelok_ne_iz_ms(monkeypatch):
    async def main():
        heal.schedule_after_contact_wait(_lead(created_by_enum=None))
        return len(heal._bg_tasks)

    assert asyncio.run(main()) == 0


# ════════════════ amo_service: 5xx и таймауты - None, а не пустой список ════════════════

class _Resp:
    def __init__(self, status, body=None):
        self.status_code = status
        self._body = body
        self.headers = {}
        self.text = ""

    def json(self):
        return self._body


@pytest.fixture
def amo_http(monkeypatch):
    monkeypatch.setattr(amo_service, "is_circuit_open", lambda: False)
    monkeypatch.setattr(amo_service, "compute_retry_delay", lambda *a, **kw: 0)
    monkeypatch.setattr(amo_service, "_record_429", lambda *a, **kw: None)
    monkeypatch.setattr(amo_service, "_record_success", lambda *a, **kw: None)

    def install(handler):
        async def submit_request(method, url, headers, json_body=None, **kw):
            return handler(method, url)
        monkeypatch.setattr(amo_service, "submit_request", submit_request)
    return install


def test_amo_500_na_poiske_kontakta_eto_none(amo_http):
    amo_http(lambda m, u: _Resp(500))
    assert asyncio.run(amo_service.find_contacts_by_query("79123456789")) is None


def test_amo_taymaut_na_poiske_kontakta_eto_none(amo_http):
    def handler(m, u):
        raise httpx.ReadTimeout("timeout")
    amo_http(handler)
    assert asyncio.run(amo_service.find_contacts_by_query("79123456789")) is None


def test_amo_429_na_vyborke_sdelok_eto_none(amo_http):
    amo_http(lambda m, u: _Resp(429))
    assert asyncio.run(amo_service.get_leads_created_between(0, 1)) is None


def test_amo_chestnyy_nol_eto_pustoy_spisok(amo_http):
    amo_http(lambda m, u: _Resp(204))
    assert asyncio.run(amo_service.find_contacts_by_query("79123456789")) == []
    assert asyncio.run(amo_service.get_leads_created_between(0, 1)) == []


def test_amo_vtoraya_stranica_upala_eto_none(amo_http):
    full_page = {"_embedded": {"leads": [{"id": i} for i in range(250)]}}

    def handler(m, u):
        return _Resp(200, full_page) if "page=1" in u else _Resp(502)
    amo_http(handler)
    assert asyncio.run(amo_service.get_leads_created_between(0, 1)) is None


def test_link_shlet_is_main(amo_http, monkeypatch):
    sent = []

    async def submit_request(method, url, headers, json_body=None, **kw):
        sent.append((method, url, json_body))
        return _Resp(200, {})
    monkeypatch.setattr(amo_service, "submit_request", submit_request)
    result = asyncio.run(amo_service.link_contact_to_lead(LEAD_ID, 48559931))
    assert result["ok"]
    method, url, body = sent[0]
    assert method == "POST" and url.endswith(f"/api/v4/leads/{LEAD_ID}/link")
    assert body == [{"to_entity_id": 48559931, "to_entity_type": "contacts", "metadata": {"is_main": True}}]
