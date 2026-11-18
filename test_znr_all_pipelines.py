"""ЗНР по причине во всех рабочих воронках (решение Кати 18.11.2026).

Две части:
  1. dup_autoclose: любая причина закрывает сделку в 143 своей воронки - только в
     воронках из ZNR_AUTOCLOSE_ANY_REASON_PIPELINES и только если причину выбрали только
     что (событие журнала amo). Мусорные причины - как раньше, во всех воронках.
  2. office_transfer: перенос из 143 по причине из TangemShop, Листа ожидания, Работы с
     базой, ОПТ, ОПТ под реализацию; причина «Tangemshop»; без переноса «в себя».

Флаги ставим monkeypatch'ем: test_office_transfer.py меняет их на уровне модуля, порядок
файлов в прогоне не должен влиять.
"""

import asyncio
import os
import tempfile
import time

os.environ.setdefault(
    "LEAD_STATUS_DB_PATH",
    os.path.join(tempfile.mkdtemp(), "lead_status_test.sqlite3"),
)

import pytest  # noqa: E402

import dup_autoclose  # noqa: E402
import office_transfer  # noqa: E402
from waybill_config import (  # noqa: E402
    DUP_REASON_FIELD_ID,
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_DB_WORK,
    PIPELINE_OFFICE,
    PIPELINE_OPT,
    PIPELINE_OPT_CONSIGNMENT,
    PIPELINE_TANGEMSHOP,
    PIPELINE_WAITLIST,
    REASON_ACADEMY,
    REASON_OPT_CONSIGNMENT,
    REASON_RETAIL,
    STATUS_CLEVER_IN_PROGRESS,
    STATUS_OPT_CONSIGNMENT_NEGOTIATION,
    REASON_OPT,
    REASON_TANGEMSHOP,
    REASON_WAITLIST,
    RESPONSIBLE_OPT_MANAGER_USER_ID,
    STATUS_ACADEMY_FIRST_CONTACT,
    STATUS_CLOSED_LOST,
    STATUS_OPT_PRIMARY_CONTACT,
    STATUS_SUCCESS,
    STATUS_TANGEM_IN_PROGRESS,
    STATUS_WAITLIST,
)

REASON_LOST = 1041147   # «Пропал»
REASON_TEST = 1041163   # «Тест» (мусорная)
OPEN_STATUS = 12345
SIX = frozenset({PIPELINE_CLEVER_MAIN, PIPELINE_TANGEMSHOP, PIPELINE_WAITLIST, PIPELINE_DB_WORK,
                 PIPELINE_OPT, PIPELINE_OPT_CONSIGNMENT})
PIPELINE_TEST = 8642414


def run(coro):
    return asyncio.run(coro)


def _lead(pipeline_id, status_id, reason, *, responsible=999, lead_id=42):
    cfs = []
    if reason is not None:
        cfs.append({"field_id": DUP_REASON_FIELD_ID, "values": [{"enum_id": reason}]})
    return {"id": lead_id, "pipeline_id": pipeline_id, "status_id": status_id,
            "responsible_user_id": responsible, "name": "Тест", "custom_fields_values": cfs,
            "_embedded": {"contacts": []}}


def _event(enum_id):
    return {"type": f"custom_field_{DUP_REASON_FIELD_ID}_value_changed",
            "value_after": [{"custom_field_value": {"field_id": DUP_REASON_FIELD_ID,
                                                    "enum_id": enum_id}}]}


# ════════════════ 1. dup_autoclose ════════════════

@pytest.fixture()
def autoclose(monkeypatch):
    """Стабы amo: сделка, журнал событий, PATCH. Возвращает (state, запуск)."""
    state = {"lead": None, "events": [], "patches": [], "event_params": None, "events_fail": False}

    async def get_lead_full(lid, with_=()):
        return state["lead"]

    async def patch_lead(lid, **kw):
        state["patches"].append(kw)
        return {"ok": True}

    async def do_get(path, params=None):
        assert path == "/api/v4/events"
        state["event_params"] = dict(params)
        if state["events_fail"]:
            return None
        return {"_embedded": {"events": state["events"]}}

    monkeypatch.setattr(dup_autoclose.amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(dup_autoclose.amo_service, "patch_lead", patch_lead)
    monkeypatch.setattr(dup_autoclose.amo_service, "_do_get", do_get)
    monkeypatch.setattr(dup_autoclose, "ZNR_AUTOCLOSE_ANY_REASON_PIPELINES", SIX)

    def go(lead, events=()):
        state["lead"] = lead
        state["events"] = list(events)
        state["patches"].clear()
        state["event_params"] = None
        run(dup_autoclose._maybe_close(lead["id"]))
        return state["patches"]

    return state, go


@pytest.mark.parametrize("pipeline", sorted(SIX))
def test_svezhaya_obychnaya_prichina_zakryvaet_v_svoey_voronke(autoclose, pipeline):
    state, go = autoclose
    patches = go(_lead(pipeline, OPEN_STATUS, REASON_LOST), [_event(REASON_LOST)])
    assert patches == [{"status_id": STATUS_CLOSED_LOST, "pipeline_id": pipeline}]
    p = state["event_params"]
    assert p["filter[entity_id]"] == "42"
    assert p["filter[type]"] == f"custom_field_{DUP_REASON_FIELD_ID}_value_changed"
    assert int(p["filter[created_at][from]"]) >= int(time.time()) - 600 - 5


def test_staraya_prichina_posle_perenosa_ne_zakryvaet(autoclose):
    """Сделка Листа ожидания с причиной «Лист ожидания» от переноса - событий смены нет."""
    _, go = autoclose
    assert go(_lead(PIPELINE_WAITLIST, STATUS_WAITLIST, REASON_WAITLIST), []) == []


def test_sobytie_pro_drugoe_znachenie_ne_schitaetsya(autoclose):
    """Недавно ставили «Пропал», а сейчас в сделке «Лист ожидания» от старого переноса."""
    _, go = autoclose
    assert go(_lead(PIPELINE_DB_WORK, OPEN_STATUS, REASON_WAITLIST), [_event(REASON_LOST)]) == []


def test_zhurnal_ne_prochitalsya_ne_zakryvaem(autoclose):
    state, go = autoclose
    state["events_fail"] = True
    assert go(_lead(PIPELINE_OPT, OPEN_STATUS, REASON_LOST), [_event(REASON_LOST)]) == []


@pytest.mark.parametrize("pipeline", [PIPELINE_ACADEMY, PIPELINE_OFFICE, 10787898, PIPELINE_TEST])
def test_isklyuchennye_voronki_obychnuyu_prichinu_ne_zakryvayut(autoclose, pipeline):
    state, go = autoclose
    assert go(_lead(pipeline, OPEN_STATUS, REASON_LOST), [_event(REASON_LOST)]) == []
    assert state["event_params"] is None, "журнал не читаем зря"


@pytest.mark.parametrize("pipeline", [PIPELINE_ACADEMY, PIPELINE_WAITLIST])
def test_musornaya_prichina_kak_ran_she_bez_zhurnala(autoclose, pipeline):
    """«Тест» закрывает в 143 СВОЕЙ воронки (в «Тест» не переводит), журнал не нужен."""
    state, go = autoclose
    patches = go(_lead(pipeline, OPEN_STATUS, REASON_TEST), [])
    assert patches == [{"status_id": STATUS_CLOSED_LOST, "pipeline_id": pipeline}]
    assert state["event_params"] is None


def test_spisok_pust_vedet_sebya_kak_ran_she(autoclose, monkeypatch):
    _, go = autoclose
    monkeypatch.setattr(dup_autoclose, "ZNR_AUTOCLOSE_ANY_REASON_PIPELINES", frozenset())
    assert go(_lead(PIPELINE_CLEVER_MAIN, OPEN_STATUS, REASON_LOST), [_event(REASON_LOST)]) == []


@pytest.mark.parametrize("status", [STATUS_SUCCESS, STATUS_CLOSED_LOST])
def test_zakrytuyu_ne_trogaem(autoclose, status):
    _, go = autoclose
    assert go(_lead(PIPELINE_OPT, status, REASON_LOST), [_event(REASON_LOST)]) == []


def test_pustaya_prichina_ne_trogaem(autoclose):
    _, go = autoclose
    assert go(_lead(PIPELINE_OPT, OPEN_STATUS, None), []) == []


def test_env_razbor_spiska(monkeypatch):
    import importlib
    import waybill_config
    monkeypatch.setenv("ZNR_AUTOCLOSE_ANY_REASON_PIPELINES", " 10593102, 9822330,x,,")
    try:
        importlib.reload(waybill_config)
        assert waybill_config.ZNR_AUTOCLOSE_ANY_REASON_PIPELINES == frozenset({10593102, 9822330})
    finally:
        monkeypatch.delenv("ZNR_AUTOCLOSE_ANY_REASON_PIPELINES")
        importlib.reload(waybill_config)


# ════════════════ 2. office_transfer ════════════════

_ALL_ON = (
    "OFFICE_TRANSFER_ENABLED",
    "OFFICE_TRANSFER_RULE_ZNR_WAITLIST", "OFFICE_TRANSFER_RULE_ZNR_ACADEMY",
    "OFFICE_TRANSFER_RULE_ZNR_OPT", "OFFICE_TRANSFER_RULE_ZNR_TANGEMSHOP",
    "OFFICE_TRANSFER_RULE_ZNR_OPT_CONSIGNMENT", "OFFICE_TRANSFER_RULE_ZNR_RETAIL",
    "OFFICE_TRANSFER_SOURCE_OPT", "OFFICE_TRANSFER_SOURCE_DB_WORK",
    "OFFICE_TRANSFER_SOURCE_TANGEMSHOP", "OFFICE_TRANSFER_SOURCE_WAITLIST",
    "OFFICE_TRANSFER_SOURCE_OPT_CONSIGNMENT",
    "OFFICE_TRANSFER_ZNR_DB_WORK", "OFFICE_TRANSFER_ZNR_TANGEMSHOP",
)


@pytest.fixture()
def transfer(monkeypatch):
    for name in _ALL_ON:
        monkeypatch.setattr(office_transfer, name, True)
    monkeypatch.setattr(office_transfer, "OFFICE_TRANSFER_SINCE_TS", 0)
    st = {"lead": None, "patches": [], "alerts": [], "tags": []}

    async def get_lead_full(lid, with_=()):
        return st["lead"]

    async def patch_lead(lid, **kw):
        st["patches"].append(kw)
        return {"ok": True, "status_code": 200}

    async def get_contact_by_id(cid, with_=()):
        return {"id": cid, "_embedded": {"leads": []}}

    async def add_tag(lid, name):
        st["tags"].append(name)
        return {"ok": True}

    async def add_note(lid, text):
        return {"ok": True}

    async def send_alert(text, *a, **kw):
        st["alerts"].append(text)
        return True

    async def noop_sync(payload, lead=None):
        return None

    async def skip_freeze(*a, **kw):
        return False

    import metrika_sync
    import woo_status_sync
    monkeypatch.setattr(office_transfer.amo_service, "get_lead_full", get_lead_full)
    monkeypatch.setattr(office_transfer.amo_service, "patch_lead", patch_lead)
    monkeypatch.setattr(office_transfer.amo_service, "get_contact_by_id", get_contact_by_id)
    monkeypatch.setattr(office_transfer.amo_service, "add_tag", add_tag)
    monkeypatch.setattr(office_transfer.amo_service, "add_note", add_note)
    monkeypatch.setattr(office_transfer.telegram_bot, "send_alert", send_alert)
    monkeypatch.setattr(office_transfer.migration_freeze, "skip", skip_freeze)
    monkeypatch.setattr(metrika_sync, "process_sync", noop_sync)
    monkeypatch.setattr(woo_status_sync, "is_enabled", lambda: False)
    office_transfer._pending_fail.clear()
    office_transfer._fill_alerted.clear()

    def go(lead):
        st["lead"] = lead
        st["patches"].clear()
        st["alerts"].clear()
        st["tags"].clear()
        return run(office_transfer.process_office_transfer(lead["id"]))

    return st, go


_TARGETS = {
    REASON_WAITLIST: (PIPELINE_WAITLIST, STATUS_WAITLIST),
    REASON_ACADEMY: (PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT),
    REASON_OPT: (PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT),
    REASON_TANGEMSHOP: (PIPELINE_TANGEMSHOP, STATUS_TANGEM_IN_PROGRESS),
    REASON_OPT_CONSIGNMENT: (PIPELINE_OPT_CONSIGNMENT, STATUS_OPT_CONSIGNMENT_NEGOTIATION),
    REASON_RETAIL: (PIPELINE_CLEVER_MAIN, STATUS_CLEVER_IN_PROGRESS),
}

_CASES = [
    (src, reason)
    for src in sorted(SIX)
    for reason in _TARGETS
]


@pytest.mark.parametrize("src,reason", _CASES)
def test_tablica_pravil(transfer, src, reason):
    """Таблица Кати: причина ведёт в свою воронку, если сделка уже не в ней."""
    st, go = transfer
    res = go(_lead(src, STATUS_CLOSED_LOST, reason))
    target = _TARGETS[reason]
    if target[0] == src:
        assert res == "no-match" and st["patches"] == [], "в саму себя не переносим"
        return
    assert res == "moved", res
    p = st["patches"][0]
    assert (p["pipeline_id"], p["status_id"]) == target
    if reason in (REASON_OPT, REASON_OPT_CONSIGNMENT):
        assert p["responsible_user_id"] == RESPONSIBLE_OPT_MANAGER_USER_ID
    else:
        assert "responsible_user_id" not in p, "ответственный прежний"


@pytest.mark.parametrize("src", sorted(SIX))
def test_obychnaya_prichina_ostaetsya_v_143(transfer, src):
    st, go = transfer
    assert go(_lead(src, STATUS_CLOSED_LOST, REASON_LOST)) == "no-match"
    assert st["patches"] == [] and st["alerts"] == []


@pytest.mark.parametrize("src", [PIPELINE_WAITLIST, PIPELINE_OPT_CONSIGNMENT])
def test_novye_istochniki_uspeshnye_ne_trogaem_i_ne_alertim(transfer, src):
    st, go = transfer
    assert go(_lead(src, STATUS_SUCCESS, None)) == "no-match"
    assert st["patches"] == [] and st["alerts"] == [] and st["tags"] == []


def test_flagi_vyklyucheny_prezhnee_povedenie(transfer, monkeypatch):
    st, go = transfer
    for name in ("OFFICE_TRANSFER_ZNR_DB_WORK", "OFFICE_TRANSFER_ZNR_TANGEMSHOP",
                 "OFFICE_TRANSFER_SOURCE_WAITLIST", "OFFICE_TRANSFER_SOURCE_OPT_CONSIGNMENT",
                 "OFFICE_TRANSFER_RULE_ZNR_TANGEMSHOP"):
        monkeypatch.setattr(office_transfer, name, False)
    assert go(_lead(PIPELINE_DB_WORK, STATUS_CLOSED_LOST, REASON_WAITLIST)) == "no-match"
    assert go(_lead(PIPELINE_TANGEMSHOP, STATUS_CLOSED_LOST, REASON_ACADEMY)) == "no-match"
    assert go(_lead(PIPELINE_WAITLIST, STATUS_CLOSED_LOST, REASON_ACADEMY)) == "skipped-not-applicable"
    assert go(_lead(PIPELINE_CLEVER_MAIN, STATUS_CLOSED_LOST, REASON_TANGEMSHOP)) == "no-match"
    assert st["patches"] == []
    assert office_transfer.is_source_pipeline(PIPELINE_OPT_CONSIGNMENT) is False


def test_reconcile_ne_sprashivaet_lishnego(transfer, monkeypatch):
    """ЗНР-только источники - без запроса по 142; картотека и TangemShop - оба этапа."""
    asked = []

    async def do_get(path, params=None):
        d = dict(params)
        asked.append((int(d["filter[value_after][leads_statuses][0][pipeline_id]"]),
                      int(d["filter[value_after][leads_statuses][0][status_id]"])))
        return {"_embedded": {"events": []}}

    monkeypatch.setattr(office_transfer.amo_service, "_do_get", do_get)
    monkeypatch.setattr(office_transfer, "OFFICE_TRANSFER_SINCE_TS", int(time.time()) - 60)
    monkeypatch.setattr(office_transfer, "_last_reconcile_ts", 0)
    run(office_transfer._reconcile_once())
    assert (PIPELINE_WAITLIST, STATUS_SUCCESS) not in asked
    assert (PIPELINE_OPT_CONSIGNMENT, STATUS_SUCCESS) not in asked
    for src in SIX:
        assert (src, STATUS_CLOSED_LOST) in asked, src
    for src in (PIPELINE_CLEVER_MAIN, PIPELINE_OPT, PIPELINE_DB_WORK, PIPELINE_TANGEMSHOP):
        assert (src, STATUS_SUCCESS) in asked, src
