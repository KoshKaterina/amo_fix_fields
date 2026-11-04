"""Пометка «брошен на приветствии» в алерте по шаблону панели: переменная `{{пометка}}`.

Боевой алерт идёт по шаблону панели (ALERT_SETTINGS_FROM_PANEL=on), поэтому строку из кода в нём
не видно. Решение Кати 04.11.2026: добавить её в шаблон. Код отдаёт значение переменной, шаблон
панели решает, где её показать; у обычного пропущенного значение пустое, и строка выпадает.

Что защищаем:
  * сброс до сотрудников: строка с пометкой есть, при таймауте ожидания распределения к ней
    добавлена вторая фраза;
  * обычный пропущенный (звонок дошёл до сотрудников) и алерт UIS: строки нет, шаблон с
    `{{пометка}}` не падает в «неизвестную переменную»;
  * шаблон без `{{пометка}}` (панель ещё не обновили) работает как раньше;
  * режим off (без панели) по-прежнему показывает пометку из кода.

Запуск: python -m pytest test_sipuni_alert_note.py -q
"""
import time

import alert_settings_client as settings_client
import sipuni_events as S
import uis_missed_call as U
from test_sipuni_events import (  # noqa: F401 - glue: фикстура, её нужно держать в этом модуле
    CALLER_DIGITS, _no_deal, glue, idle, r_greeting_drop, r_not_taken, run,
)

TEMPLATE_WITH_NOTE = (
    "🔴 ПРОПУЩЕННЫЙ звонок (панель) — срочно связаться с клиентом\n{{теги}}\nℹ️ {{пометка}}\n"
    "📞 {{телефон}}\n👤 {{клиент}}\n{{ссылка_на_сделку}}"
)
TEMPLATE_OLD = (
    "🔴 ПРОПУЩЕННЫЙ звонок (панель) — срочно связаться с клиентом\n{{теги}}\n📞 {{телефон}}\n"
    "👤 {{клиент}}\n{{ссылка_на_сделку}}"
)
NEW_LEAD = 83537714


def panel(monkeypatch, template):
    """Включает режим панели и подкладывает документ с шаблоном missed_call."""
    monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", "on")
    settings_client.set_settings_for_tests({
        "events": {"missed_call": {"enabled": True, "channel": "op_notify", "template": template,
                                   "recipients_mode": "shift"}},
        "people": [],
    })


def teardown_panel():
    settings_client.set_settings_for_tests(None)


def drop(cid):
    return r_greeting_drop(cid, ts=int(time.time()))


def test_dropped_call_shows_the_note_line_in_the_panel_text(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    panel(monkeypatch, TEMPLATE_WITH_NOTE)
    _no_deal(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("an.1"))
        await idle(S.tracker)

    try:
        run(go())
    finally:
        teardown_panel()
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert "(панель)" in text, "текст по шаблону панели, а не запасной"
    assert "ℹ️ " + U._DROPPED_TEXT in text
    assert U._ROUTING_TIMEOUT_TEXT not in text and "·" not in text


def test_routing_timeout_adds_the_second_phrase(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    panel(monkeypatch, TEMPLATE_WITH_NOTE)

    async def find(query, *a, **k):
        return [{"id": 36500001, "status_id": NEW_LEAD, "responsible_user_id": 13929334, "updated_at": 5}]

    monkeypatch.setattr(U.amo_service, "find_leads_by_query_all", find)

    async def go():
        await U._sipuni_apply(drop("an.2"))      # ожидание 0.6 с из фикстуры glue, записи нет
        await idle(S.tracker)

    try:
        run(go())
    finally:
        teardown_panel()
    text = glue.sends[0][0]
    assert "(панель)" in text
    assert "ℹ️ " + U._DROPPED_TEXT + ". " + U._ROUTING_TIMEOUT_TEXT in text


def test_ordinary_missed_call_has_no_note_line(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    panel(monkeypatch, TEMPLATE_WITH_NOTE)

    async def go():
        await U._sipuni_apply(r_not_taken("an.3", ts=int(time.time()), legs=3))
        await idle(S.tracker)

    try:
        run(go())
    finally:
        teardown_panel()
    text = glue.sends[0][0]
    assert "(панель)" in text, "текст по шаблону панели"
    assert "ℹ️" not in text and U._DROPPED_TEXT not in text
    assert "ПРОПУЩЕННЫЙ звонок" in text and "📞" in text, "остальные строки на месте"


def test_uis_alert_with_the_note_template_does_not_fall_back(glue, monkeypatch):
    """Шаблон с {{пометка}} у UIS: переменная есть и пуста, строка выпадает, текст от панели."""
    panel(monkeypatch, TEMPLATE_WITH_NOTE)

    async def go():
        await U._apply({"contact_phone_number": CALLER_DIGITS, "contact_full_name": "",
                        "call_session_id": "uis-note"})

    try:
        run(go())
    finally:
        teardown_panel()
    text = glue.sends[0][0]
    assert "(панель)" in text and "ℹ️" not in text


def test_old_template_without_the_variable_still_works(glue, monkeypatch):
    """Панель ещё не обновили: лишний ключ в values шаблон не трогает."""
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    panel(monkeypatch, TEMPLATE_OLD)
    _no_deal(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("an.5"))
        await idle(S.tracker)

    try:
        run(go())
    finally:
        teardown_panel()
    text = glue.sends[0][0]
    assert "(панель)" in text and U._DROPPED_TEXT not in text


def test_without_panel_the_note_still_comes_from_the_code(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    monkeypatch.setattr(settings_client, "ALERT_SETTINGS_FROM_PANEL", "off")
    _no_deal(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("an.6"))
        await idle(S.tracker)

    run(go())
    assert U._DROPPED_IN_MENU_NOTE in glue.sends[0][0]
