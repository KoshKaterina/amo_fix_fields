"""Подпись телефонии в уведомлениях о пропущенном: UIS это или Sipuni (просьба Кати 04.11.2026).

Оба входа, UIS (495 и 901) и Sipuni (905), шлют один и тот же алерт, и по тексту их было не отличить.
Теперь у «Пропущенного звонка» и у «Клиенту не перезвонили» есть переменная шаблона панели
`{{телефония}}` со значением UIS или Sipuni, а в тексте из кода строка «☎️ UIS» или «☎️ Sipuni».

Что защищаем:
  * текст из кода: подпись после телефона, у UIS и у Sipuni своя;
  * шаблон панели: переменная подставляется, шаблон без неё работает как раньше;
  * второй контур («Клиенту не перезвонили»): подпись в тексте руководству и в переменной.

Запуск: python -m pytest test_sipuni_alert_provider.py -q
"""
import time

import alert_settings_client as settings_client
import sipuni_events as S
import uis_missed_call as U
from test_sipuni_events import (  # noqa: F401 - glue: фикстура, её нужно держать в этом модуле
    CALLER_DIGITS, MISSED_CALL, _no_deal, glue, idle, r_not_taken, run,
)

TEMPLATE = (
    "🔴 ПРОПУЩЕННЫЙ звонок (панель) — срочно связаться с клиентом\n{{теги}}\n📞 {{телефон}}\n"
    "☎️ {{телефония}}\n👤 {{клиент}}\n{{ссылка_на_сделку}}"
)
TEMPLATE_OLD = (
    "🔴 ПРОПУЩЕННЫЙ звонок (панель) — срочно связаться с клиентом\n{{теги}}\n📞 {{телефон}}\n"
    "👤 {{клиент}}\n{{ссылка_на_сделку}}"
)
ESCALATION_TEMPLATE = (
    "🚨 Клиенту не перезвонили (панель) {{сколько_ждали}}\nМенеджер: {{ответственный}}\n"
    "Телефон: {{телефон}}\nТелефония: {{телефония}}\n\n{{ссылка_на_сделку}}"
)


def panel(monkeypatch, events):
    monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", "on")
    settings_client.set_settings_for_tests({"events": events, "people": []})


def teardown_panel():
    settings_client.set_settings_for_tests(None)


def missed_event(template):
    return {"enabled": True, "channel": "op_notify", "template": template, "recipients_mode": "shift"}


def test_code_text_names_the_telephony_for_both_providers(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")

    async def go():
        await U._apply({"contact_phone_number": CALLER_DIGITS, "contact_full_name": "",
                        "call_session_id": "uis-prov"})
        await U._sipuni_apply(MISSED_CALL)
        await idle(S.tracker)

    run(go())
    (uis_text, _), (sip_text, _) = glue.sends
    assert "☎️ UIS" in uis_text and "☎️ Sipuni" not in uis_text
    assert "☎️ Sipuni" in sip_text and "☎️ UIS" not in sip_text
    lines = sip_text.split("\n")
    assert lines[lines.index(f"📞 {CALLER_DIGITS}") + 1] == "☎️ Sipuni", "подпись сразу после телефона"


def test_panel_text_substitutes_the_telephony(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    panel(monkeypatch, {"missed_call": missed_event(TEMPLATE)})

    async def go():
        await U._apply({"contact_phone_number": CALLER_DIGITS, "contact_full_name": "",
                        "call_session_id": "uis-prov2"})
        await U._sipuni_apply(MISSED_CALL)
        await idle(S.tracker)

    try:
        run(go())
    finally:
        teardown_panel()
    (uis_text, _), (sip_text, _) = glue.sends
    assert "(панель)" in uis_text and "(панель)" in sip_text, "оба текста по шаблону панели"
    assert "☎️ UIS" in uis_text and "☎️ Sipuni" in sip_text


def test_old_panel_template_without_the_variable_still_works(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    panel(monkeypatch, {"missed_call": missed_event(TEMPLATE_OLD)})

    async def go():
        await U._sipuni_apply(MISSED_CALL)
        await idle(S.tracker)

    try:
        run(go())
    finally:
        teardown_panel()
    text = glue.sends[0][0]
    assert "(панель)" in text and "☎️" not in text


def test_dropped_call_alert_keeps_both_the_note_and_the_telephony(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    _no_deal(monkeypatch)
    from test_sipuni_events import r_greeting_drop

    async def go():
        await U._sipuni_apply(r_greeting_drop("pv.1", ts=int(time.time())))
        await idle(S.tracker)

    run(go())
    text = glue.sends[0][0]
    assert U._DROPPED_IN_MENU_NOTE in text and "☎️ Sipuni" in text


def test_escalation_names_the_telephony(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")

    async def scenario():
        await U._sipuni_apply(MISSED_CALL)
        await idle(S.tracker)

    run(scenario())
    st = U._callback_pending[36500001]
    st["since"] -= 61 * 60
    glue.sends.clear()
    run(U._sweep_callbacks())
    text = glue.sends[0][0]
    assert "не перезвонили" in text and "Телефония: Sipuni" in text


def test_escalation_for_uis_says_uis_and_panel_template_gets_the_variable(glue, monkeypatch):
    panel(monkeypatch, {"missed_call_no_callback": {
        "enabled": True, "channel": "rop", "template": ESCALATION_TEMPLATE, "recipients_mode": "nobody"}})

    async def never_called_back(lead_id, st):
        return False

    monkeypatch.setattr(U, "_check_called_back", never_called_back)
    monkeypatch.setattr(U.alerts.tg_recipients, "ROP_CHAT_ID", -5358037627)   # канал «rop» панели
    U._callback_pending[36500002] = {
        "since": time.monotonic() - 61 * 60, "phone": CALLER_DIGITS, "name": "", "responsible_id": None,
        "provider": "uis",
    }
    glue.sends.clear()
    try:
        run(U._sweep_callbacks())
    finally:
        teardown_panel()
    text = glue.sends[0][0]
    assert "(панель)" in text and "Телефония: UIS" in text


def test_build_escalation_code_text(monkeypatch):
    uis = U._build_escalation(1, {"phone": "79990000000", "provider": "uis", "responsible_id": None}, 61)
    sip = U._build_escalation(1, {"phone": "79990000000", "provider": "sipuni", "responsible_id": None}, 61)
    assert "Телефония: UIS" in uis and "Телефония: Sipuni" in sip
    assert "Телефон: 79990000000\nТелефония:" in sip, "подпись сразу после телефона"
