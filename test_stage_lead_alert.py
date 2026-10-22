"""Тесты уведомлений «новый лид в ОПТ / в листе ожидания (Клуб, Консультация Владимир)» (stage_lead_alert).

Что проверяется в первую очередь:
  • ЧУЖИЕ воронки и этапы не будят фон (вебхук ходит на каждое изменение любой сделки), в том
    числе этап «Товар» листа ожидания: по ТЗ Кати он уведомлений не даёт;
  • каждый из трёх этапов попадает в СВОЁ событие и в свою ветку;
  • пока номер ветки не задан - модуль молчит, а не падает в общий топик розницы;
  • дедуп с окном, лид, уведённый за паузу, часовой лимит на группу.

⚠️ Фон (asyncio.create_task) тут НЕ используется: соседние тест-модули подменяют
asyncio.create_task глобально. Гейт проверяем на notify_bg со своей заглушкой, отправку -
вызовом _apply.

Запуск: python3 -m pytest test_stage_lead_alert.py -q
"""

import asyncio
import os
import sys
import types

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_sent: list[dict] = []
_send_ok = [True]


def _install_stubs():
    tg = types.ModuleType("telegram_bot")

    async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
        _sent.append({"text": text, "chat_id": chat_id, "thread": message_thread_id})
        return _send_ok[0]

    tg.send_alert = send_alert
    sys.modules["telegram_bot"] = tg


_install_stubs()

import alerts  # noqa: E402
import stage_lead_alert  # noqa: E402
import tg_recipients  # noqa: E402
from waybill_config import (  # noqa: E402
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_OPT,
    PIPELINE_WAITLIST,
    STATUS_NEW_LEAD,
    STATUS_OPT_CONTACT_FOUND,
    STATUS_OPT_PRIMARY_CONTACT,
    STATUS_WAITLIST,
    STATUS_WAITLIST_CLUB,
    STATUS_WAITLIST_VLADIMIR,
)

LEAD_ID = 41300001
OPT_THREAD = 7001
WAITLIST_THREAD = 7002
STATUS_OPT_POTENTIAL = 86989422   # «Изучен потенциал» - этап ОПТ дальше по воронке
STATUS_WAITLIST_TAKEN = 83669954  # «Взят в работу» - этап листа ожидания дальше по воронке


def _lead(lead_id=LEAD_ID, pipeline=PIPELINE_OPT, status=STATUS_OPT_PRIMARY_CONTACT,
          name="Форма: оптовая закупка", contacts=({"id": 1, "is_main": True},)):
    return {
        "id": lead_id,
        "pipeline_id": pipeline,
        "status_id": status,
        "name": name,
        "_embedded": {"contacts": list(contacts)},
    }


def _rule(event):
    return next(r for r in stage_lead_alert.RULES if r.event == event)


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    _sent.clear()
    _send_ok[0] = True
    stage_lead_alert._seen.clear()
    for group in stage_lead_alert._sent_times:
        stage_lead_alert._sent_times[group].clear()
        stage_lead_alert._burst_notified[group] = False
    stage_lead_alert._warned_no_thread.clear()
    monkeypatch.setattr(stage_lead_alert, "_SEEN_PATH", str(tmp_path / "seen.json"))
    monkeypatch.setattr(stage_lead_alert, "_seen_loaded", False)
    monkeypatch.setattr(stage_lead_alert, "STAGE_LEAD_ALERT_ENABLED", True)
    monkeypatch.setattr(stage_lead_alert, "STAGE_LEAD_ALERT_DELAY_S", 0)
    monkeypatch.setattr(stage_lead_alert, "OPT_NOTIFY_THREAD", OPT_THREAD)
    monkeypatch.setattr(stage_lead_alert, "WAITLIST_NOTIFY_THREAD", WAITLIST_THREAD)
    # Адрес для панельного решения резолвит alerts._destination из tg_recipients.
    monkeypatch.setattr(tg_recipients, "OPT_NOTIFY_THREAD", OPT_THREAD)
    monkeypatch.setattr(tg_recipients, "WAITLIST_NOTIFY_THREAD", WAITLIST_THREAD)

    async def contact(contact_id, with_=()):
        return {
            "id": contact_id,
            "name": "Пётр Иванов",
            "custom_fields_values": [
                {"field_id": 413385, "values": [{"value": "+79991234567"}]},
            ],
        }

    monkeypatch.setattr(stage_lead_alert.amo_service, "get_contact_by_id", contact)
    yield
    _sent.clear()
    stage_lead_alert._seen.clear()


@pytest.fixture
def scheduled(monkeypatch):
    """Собирает правила, для которых notify_bg завёл фон (саму корутину не исполняем)."""
    fired: list = []

    class _Task:
        def add_done_callback(self, cb):
            pass

    def _fake_create_task(coro):
        fired.append(coro.cr_frame.f_locals["rule"].event)
        coro.close()
        return _Task()

    monkeypatch.setattr(stage_lead_alert.asyncio, "create_task", _fake_create_task)
    return fired


def _send(rule_event, lead, monkeypatch, lead_id=LEAD_ID):
    async def get_lead(lid, with_=()):
        return lead

    monkeypatch.setattr(stage_lead_alert.amo_service, "get_lead_full", get_lead)
    asyncio.run(stage_lead_alert._apply(_rule(rule_event), lead_id))


# ── гейт вебхука: кого будим, кого нет ───────────────────────────────────────

def test_pervichnyy_kontakt_opta_zavodit_opt_lead(scheduled):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == ["opt_lead"]


def test_nayden_kontakt_opta_tozhe_zavodit_opt_lead(scheduled):
    """Перенос ЗНР → ОПТ кладёт сделку сюда, если контакт уже встречался."""
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_CONTACT_FOUND)
    assert scheduled == ["opt_lead"]


def test_klub_zavodit_svoyo_sobytie(scheduled):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_WAITLIST, STATUS_WAITLIST_CLUB)
    assert scheduled == ["waitlist_club_lead"]


def test_konsultaciya_vladimir_zavodit_svoyo_sobytie(scheduled):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_WAITLIST, STATUS_WAITLIST_VLADIMIR)
    assert scheduled == ["waitlist_vladimir_lead"]


def test_tovar_ne_zavodit_fon(scheduled):
    """Этап «Товар» (132 сделки, сюда же идёт перенос ЗНР) уведомлений не даёт."""
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_WAITLIST, STATUS_WAITLIST)
    assert scheduled == []


def test_drugie_etapy_ne_zavodyat_fon(scheduled):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_POTENTIAL)
    stage_lead_alert.notify_bg(LEAD_ID + 1, PIPELINE_WAITLIST, STATUS_WAITLIST_TAKEN)
    assert scheduled == []


def test_chuzhie_voronki_ne_zavodyat_fon(scheduled):
    """Этап с тем же номером в чужой воронке - не наш: ключ правила это пара воронка + этап."""
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_CLEVER_MAIN, STATUS_NEW_LEAD)
    stage_lead_alert.notify_bg(LEAD_ID + 1, PIPELINE_ACADEMY, STATUS_WAITLIST_CLUB)
    stage_lead_alert.notify_bg(LEAD_ID + 2, PIPELINE_OPT, STATUS_WAITLIST_CLUB)
    assert scheduled == []


def test_stroki_iz_vebhuka_tozhe_prohodyat(scheduled):
    """amo шлёт значения формой, то есть строками - сравнение должно это переживать."""
    stage_lead_alert.notify_bg(str(LEAD_ID), str(PIPELINE_WAITLIST), str(STATUS_WAITLIST_CLUB))
    assert scheduled == ["waitlist_club_lead"]


def test_master_flag_glushit(scheduled, monkeypatch):
    monkeypatch.setattr(stage_lead_alert, "STAGE_LEAD_ALERT_ENABLED", False)
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == []


def test_pustoy_vebhuk_ne_padaet(scheduled):
    stage_lead_alert.notify_bg(None, None, None)
    stage_lead_alert.notify_bg(LEAD_ID, None, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == []


def test_bez_nomera_vetki_molchim_a_ne_padaem_v_obshchiy_topik(scheduled, monkeypatch):
    """Номер ветки приходит из окружения. Пока его нет - событие не шлётся вовсе."""
    monkeypatch.setattr(stage_lead_alert, "OPT_NOTIFY_THREAD", None)
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == []
    # А соседняя группа с номером продолжает работать.
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_WAITLIST, STATUS_WAITLIST_CLUB)
    assert scheduled == ["waitlist_club_lead"]


def test_bez_nomera_vetki_dedup_ne_otmechaetsya(scheduled, monkeypatch):
    """Не отправляли - значит, и «уже писали» ставить нельзя: появится номер, и тот же лид дойдёт."""
    monkeypatch.setattr(stage_lead_alert, "OPT_NOTIFY_THREAD", None)
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert stage_lead_alert._seen == {}


# ── дедуп с окном ────────────────────────────────────────────────────────────

def test_eho_vebhuka_molchit(scheduled):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == ["opt_lead"]


def test_pereezd_mezhdu_dvumya_vhodami_opta_daet_odno_soobshchenie(scheduled):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_CONTACT_FOUND)
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == ["opt_lead"]


def test_klub_potom_konsultaciya_dayut_dva_raznyh_soobshcheniya(scheduled):
    """Это разные события с разными заголовками - Артёму важно знать про оба."""
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_WAITLIST, STATUS_WAITLIST_CLUB)
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_WAITLIST, STATUS_WAITLIST_VLADIMIR)
    assert scheduled == ["waitlist_club_lead", "waitlist_vladimir_lead"]


def test_dedup_perezhivaet_restart(scheduled, monkeypatch):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    stage_lead_alert._seen.clear()
    monkeypatch.setattr(stage_lead_alert, "_seen_loaded", False)
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == ["opt_lead"]


def test_posle_okna_uvedomlyaem_snova(scheduled):
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    key = f"opt_lead:{LEAD_ID}"
    stage_lead_alert._seen[key] -= stage_lead_alert.STAGE_LEAD_ALERT_DEDUP_H * 3600 + 60
    stage_lead_alert.notify_bg(LEAD_ID, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)
    assert scheduled == ["opt_lead", "opt_lead"]


# ── гейт по сделке: пауза между вебхуком и отправкой ─────────────────────────

def test_sdelka_na_etape_prohodit():
    assert stage_lead_alert.is_alert_stage(_rule("opt_lead"), _lead()) is True
    assert stage_lead_alert.is_alert_stage(
        _rule("waitlist_club_lead"),
        _lead(pipeline=PIPELINE_WAITLIST, status=STATUS_WAITLIST_CLUB)) is True


def test_uehala_dalshe_ne_prohodit():
    assert stage_lead_alert.is_alert_stage(
        _rule("opt_lead"), _lead(status=STATUS_OPT_POTENTIAL)) is False


def test_uehala_v_drugoy_etap_lista_ozhidaniya_ne_prohodit():
    """Клуб → Консультация Владимир за паузу: сообщение про «Клуб» уже не про эту сделку."""
    assert stage_lead_alert.is_alert_stage(
        _rule("waitlist_club_lead"),
        _lead(pipeline=PIPELINE_WAITLIST, status=STATUS_WAITLIST_VLADIMIR)) is False


def test_net_sdelki_ne_prohodit():
    assert stage_lead_alert.is_alert_stage(_rule("opt_lead"), None) is False


def test_uveli_za_pauzu_molchim_i_otmetka_snyata(monkeypatch):
    stage_lead_alert._seen[f"opt_lead:{LEAD_ID}"] = 1.0
    _send("opt_lead", _lead(status=STATUS_OPT_POTENTIAL), monkeypatch)
    assert _sent == []
    assert f"opt_lead:{LEAD_ID}" not in stage_lead_alert._seen


def test_amo_molchit_otmetka_snyata(monkeypatch):
    async def boom(lid, with_=()):
        raise RuntimeError("amo 502")

    stage_lead_alert._seen[f"opt_lead:{LEAD_ID}"] = 1.0
    monkeypatch.setattr(stage_lead_alert.amo_service, "get_lead_full", boom)
    asyncio.run(stage_lead_alert._apply(_rule("opt_lead"), LEAD_ID))
    assert _sent == []
    assert f"opt_lead:{LEAD_ID}" not in stage_lead_alert._seen


def test_telegram_ne_prinyal_otmetka_snyata(monkeypatch):
    stage_lead_alert._seen[f"opt_lead:{LEAD_ID}"] = 1.0
    _send_ok[0] = False
    _send("opt_lead", _lead(), monkeypatch)
    assert len(_sent) == 1
    assert f"opt_lead:{LEAD_ID}" not in stage_lead_alert._seen


# ── сообщения: текст, адресат, ветка ─────────────────────────────────────────

def test_opt_soobshchenie_i_vetka(monkeypatch):
    _send("opt_lead", _lead(), monkeypatch)
    assert len(_sent) == 1
    text = _sent[0]["text"]
    assert "🤝 Новый лид в ОПТ" in text
    assert "@temossska" in text
    assert f"/leads/detail/{LEAD_ID}" in text
    assert "Открыть сделку" in text
    assert "+79991234567" in text
    assert "Пётр Иванов" in text
    assert "Форма: оптовая закупка" in text
    # Этап словами, не номером (правило Кати 03.08.2026).
    assert "Первичный контакт" in text
    assert str(STATUS_OPT_PRIMARY_CONTACT) not in text
    assert "·" not in text
    assert _sent[0]["chat_id"] == tg_recipients.NOTIFY_CHAT_ID
    assert _sent[0]["thread"] == OPT_THREAD


def test_opt_na_nayden_kontakt_pokazyvaet_etap(monkeypatch):
    _send("opt_lead", _lead(status=STATUS_OPT_CONTACT_FOUND), monkeypatch)
    assert "Найден контакт" in _sent[0]["text"]


def test_klub_soobshchenie_i_vetka(monkeypatch):
    lead = _lead(pipeline=PIPELINE_WAITLIST, status=STATUS_WAITLIST_CLUB, name="Лист ожидания клуба")
    _send("waitlist_club_lead", lead, monkeypatch)
    text = _sent[0]["text"]
    assert "⏳ Новый лид в листе ожидания: Клуб" in text
    assert "Консультация Владимир" not in text
    assert "@temossska" in text
    assert _sent[0]["thread"] == WAITLIST_THREAD


def test_konsultaciya_vladimir_soobshchenie_i_vetka(monkeypatch):
    lead = _lead(pipeline=PIPELINE_WAITLIST, status=STATUS_WAITLIST_VLADIMIR,
                 name="Консультация Владимира")
    _send("waitlist_vladimir_lead", lead, monkeypatch)
    text = _sent[0]["text"]
    assert "⏳ Новый лид в листе ожидания: Консультация Владимир" in text
    assert "@temossska" in text
    assert _sent[0]["thread"] == WAITLIST_THREAD


def test_bez_kontakta_soobshchenie_vse_ravno_uhodit(monkeypatch):
    _send("opt_lead", _lead(contacts=()), monkeypatch)
    assert len(_sent) == 1
    assert f"/leads/detail/{LEAD_ID}" in _sent[0]["text"]


def test_uglovye_skobki_ekraniruyutsya(monkeypatch):
    """parse_mode=HTML: имя с «<» иначе сломает разметку и сообщение не дойдёт."""
    _send("opt_lead", _lead(name="Заявка <опт>"), monkeypatch)
    assert "&lt;опт&gt;" in _sent[0]["text"]


# ── панель: события и каналы ─────────────────────────────────────────────────

def test_kanaly_op_opt_i_op_waitlist_razreshayutsya(monkeypatch):
    assert alerts._destination("op_opt") == (tg_recipients.NOTIFY_CHAT_ID, OPT_THREAD)
    assert alerts._destination("op_waitlist") == (tg_recipients.NOTIFY_CHAT_ID, WAITLIST_THREAD)


def test_kanal_bez_nomera_vetki_ne_razreshaetsya(monkeypatch):
    """Как op_showroom: нет ветки - панельное решение «не слать», а не общий топик."""
    monkeypatch.setattr(tg_recipients, "OPT_NOTIFY_THREAD", None)
    monkeypatch.setattr(tg_recipients, "WAITLIST_NOTIFY_THREAD", None)
    assert alerts._destination("op_opt") is None
    assert alerts._destination("op_waitlist") is None


def test_vyklyuchen_v_paneli_ne_shlem(monkeypatch):
    monkeypatch.setattr(alerts, "decide", lambda *a, **kw: None)
    _send("opt_lead", _lead(), monkeypatch)
    assert _sent == []


def test_panel_shablon_poluchaet_vse_peremennye(monkeypatch):
    """Переменные, которые сендер отдаёт панели, - ровно те, что объявлены в каталоге панели."""
    seen: dict = {}

    def decide(event_key, *, legacy_text, values, **kw):
        seen["event"] = event_key
        seen["values"] = values
        return None

    monkeypatch.setattr(alerts, "decide", decide)
    _send("opt_lead", _lead(), monkeypatch)
    assert seen["event"] == "opt_lead"
    assert set(seen["values"]) == {"теги", "телефон", "клиент", "сделка", "этап", "ссылка_на_сделку"}
    assert seen["values"]["теги"] == "@temossska"


# ── часовой лимит на группу ──────────────────────────────────────────────────

def test_perebor_limita_odno_preduprezhdenie(monkeypatch):
    monkeypatch.setattr(stage_lead_alert, "STAGE_LEAD_ALERT_HOUR_LIMIT", 3)
    for i in range(5):
        stage_lead_alert._seen[f"opt_lead:{LEAD_ID + i}"] = 1.0
        _send("opt_lead", _lead(lead_id=LEAD_ID + i), monkeypatch, lead_id=LEAD_ID + i)
    assert len(_sent) == 4
    assert sum("приглушены" in s["text"] for s in _sent) == 1
    burst = next(s for s in _sent if "приглушены" in s["text"])
    assert "ОПТ" in burst["text"]
    assert burst["thread"] == OPT_THREAD


def test_limit_obshchiy_dlya_kluba_i_konsultacii(monkeypatch):
    """Лимит у листа ожидания один на оба этапа, а предупреждение приходит в его ветку."""
    monkeypatch.setattr(stage_lead_alert, "STAGE_LEAD_ALERT_HOUR_LIMIT", 2)
    plan = [
        ("waitlist_club_lead", STATUS_WAITLIST_CLUB),
        ("waitlist_vladimir_lead", STATUS_WAITLIST_VLADIMIR),
        ("waitlist_club_lead", STATUS_WAITLIST_CLUB),
    ]
    for i, (event, status) in enumerate(plan):
        lead_id = LEAD_ID + i
        stage_lead_alert._seen[f"{event}:{lead_id}"] = 1.0
        _send(event, _lead(lead_id=lead_id, pipeline=PIPELINE_WAITLIST, status=status),
              monkeypatch, lead_id=lead_id)
    assert len(_sent) == 3   # два алерта + одно предупреждение
    assert "лист ожидания" in _sent[-1]["text"]
    assert _sent[-1]["thread"] == WAITLIST_THREAD


def test_limit_opta_ne_glushit_list_ozhidaniya(monkeypatch):
    monkeypatch.setattr(stage_lead_alert, "STAGE_LEAD_ALERT_HOUR_LIMIT", 1)
    stage_lead_alert._seen[f"opt_lead:{LEAD_ID}"] = 1.0
    _send("opt_lead", _lead(), monkeypatch)
    stage_lead_alert._seen[f"waitlist_club_lead:{LEAD_ID + 1}"] = 1.0
    _send("waitlist_club_lead",
          _lead(lead_id=LEAD_ID + 1, pipeline=PIPELINE_WAITLIST, status=STATUS_WAITLIST_CLUB),
          monkeypatch, lead_id=LEAD_ID + 1)
    assert len(_sent) == 2
    assert not any("приглушены" in s["text"] for s in _sent)


# ── адреса веток ─────────────────────────────────────────────────────────────

def test_nomera_vetok_snyaty_iz_ssylok_kati(monkeypatch):
    """Ссылки Кати 22.10.2026: t.me/c/3680811996/27598/27599 (опт) и .../27600/27601 (лист
    ожидания). Чат у обеих тот же, что у остальных веток отдела продаж."""
    import importlib
    import waybill_config

    monkeypatch.delenv("OPT_NOTIFY_THREAD_ID", raising=False)
    monkeypatch.delenv("WAITLIST_NOTIFY_THREAD_ID", raising=False)
    cfg = importlib.reload(waybill_config)
    try:
        assert cfg.OPT_NOTIFY_THREAD_ID == 27598
        assert cfg.WAITLIST_NOTIFY_THREAD_ID == 27600
    finally:
        importlib.reload(waybill_config)
    assert tg_recipients.NOTIFY_CHAT_ID == -1003680811996
