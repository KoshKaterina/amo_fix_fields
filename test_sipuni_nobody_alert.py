"""Алерт «Некому адресовать звонок, проверьте работу телефонии» (решение Кати 23.11.2026).

Неделя 16-23.11: из 107 пропущенных в рабочее время 82 вообще не получили ног к сотрудникам, и в 39
из них клиент нажал кнопку меню, а звонить было некому (никто не онлайн или все заняты). Обычный алерт
«бросил на приветствии» тут вводит в заблуждение, поэтому такой звонок идёт с отдельной пометкой и с пингом
всего пула сразу, без ожидания распределения.

Что защищаем:
  * клиент дождался (>= SIPUNI_NOBODY_MIN_S), ног к сотрудникам нет, рабочее время: пометка «Некому
    адресовать звонок, проверьте работу телефонии», пинг всего пула, даже если у сделки есть ответственный;
  * быстрый сброс на приветствии, выключенный флаг, нерабочее время, звонок с ногами: как раньше;
  * граница по длительности: 19 секунд ещё «бросил на приветствии», 20 уже «некому»;
  * шаблон панели получает текст в переменной `{{пометка}}`;
  * режим log ничего не шлёт, но пишет признак в журнал; по умолчанию функция выключена.

Запуск: python -m pytest test_sipuni_nobody_alert.py -q
"""
import datetime
import time

import pytest

import sipuni_events as S
import uis_missed_call as U
from test_sipuni_alert_note import TEMPLATE_WITH_NOTE, panel, teardown_panel
from test_sipuni_events import (  # noqa: F401 - glue: фикстура, её нужно держать в этом модуле
    CALLER_DIGITS, _no_deal, _probe_config, _spy_decide_responsible, _spy_mentions, glue, idle,
    r_end, r_greeting_drop, r_not_taken, r_start, run,
)

NOBODY = "Некому адресовать звонок, проверьте работу телефонии"


def waited(cid, secs):
    """Клиент ждал `secs` секунд и положил трубку, ног к сотрудникам не было."""
    ts = int(time.time())
    return [r_start(cid, ts), r_end(cid, ts + secs, "NOANSWER")]


def live(monkeypatch, *, on=True, start=0, end=24, min_s=20.0):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    monkeypatch.setattr(U, "SIPUNI_NOBODY_ALERT", on)
    monkeypatch.setattr(U, "SIPUNI_NOBODY_MIN_S", min_s)
    monkeypatch.setattr(U, "SIPUNI_NOBODY_START_H", start)
    monkeypatch.setattr(U, "SIPUNI_NOBODY_END_H", end)


def feed_and_wait(events):
    async def go():
        await U._sipuni_apply(events)
        await idle(S.tracker)

    run(go())


def test_waited_call_without_staff_legs_says_nobody_and_pings_everyone(glue, monkeypatch):
    live(monkeypatch)
    _no_deal(monkeypatch)
    asked = _spy_mentions(monkeypatch)
    routed = _spy_decide_responsible(monkeypatch)
    feed_and_wait(waited("n.1", 30))
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert NOBODY in text
    assert asked == [None] and routed == [None], "весь пул, а не один ответственный"
    assert "@pool-ping" in text and "@only-responsible" not in text
    assert "на приветствии" not in text, "прежняя пометка не показывается вместе с новой"
    assert "ПРОПУЩЕННЫЙ звонок" in text and CALLER_DIGITS in text and "·" not in text


def test_existing_deal_with_responsible_does_not_narrow_the_ping(glue, monkeypatch):
    """У сделки есть ответственный (в заглушке amo он есть), но звонить было некому: пингуем всех,
    ссылка на сделку остаётся."""
    live(monkeypatch)
    asked = _spy_mentions(monkeypatch)
    routed = _spy_decide_responsible(monkeypatch)
    started = time.monotonic()
    feed_and_wait(waited("n.2", 40))
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert NOBODY in text and "36500001" in text
    assert asked == [None] and routed == [None]
    assert time.monotonic() - started < 0.5, "распределения не ждём (в тесте окно ожидания 0,6 с)"


def test_quick_greeting_drop_keeps_the_old_note_even_with_the_flag_on(glue, monkeypatch):
    live(monkeypatch)
    _no_deal(monkeypatch)
    feed_and_wait(r_greeting_drop("n.3", ts=int(time.time())))
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert U._DROPPED_IN_MENU_NOTE in text and NOBODY not in text


def test_flag_off_keeps_the_old_note_for_a_long_wait(glue, monkeypatch):
    live(monkeypatch, on=False)
    _no_deal(monkeypatch)
    feed_and_wait(waited("n.4", 45))
    text = glue.sends[0][0]
    assert U._DROPPED_IN_MENU_NOTE in text and NOBODY not in text


def test_outside_working_hours_keeps_the_old_note(glue, monkeypatch):
    hour = datetime.datetime.now(U._MSK).hour
    live(monkeypatch, start=0, end=hour)      # текущий час в окно не попадает
    _no_deal(monkeypatch)
    feed_and_wait(waited("n.5", 45))
    text = glue.sends[0][0]
    assert U._DROPPED_IN_MENU_NOTE in text and NOBODY not in text


def test_call_that_reached_staff_is_not_nobody(glue, monkeypatch):
    live(monkeypatch)
    asked = _spy_mentions(monkeypatch)
    feed_and_wait(r_not_taken("n.6", ts=int(time.time()), legs=4))
    text = glue.sends[0][0]
    assert NOBODY not in text and U._DROPPED_IN_MENU_NOTE not in text
    assert asked == [13929334], "звонили человеку, адресат прежний: ответственный"


@pytest.mark.parametrize("secs, nobody", [(19, False), (20, True), (21, True)])
def test_duration_boundary(glue, monkeypatch, secs, nobody):
    live(monkeypatch, min_s=20.0)
    _no_deal(monkeypatch)
    feed_and_wait(waited("n.7.%d" % secs, secs))
    text = glue.sends[0][0]
    assert (NOBODY in text) is nobody
    assert (U._DROPPED_IN_MENU_NOTE in text) is (not nobody)


def test_log_mode_sends_nothing_but_marks_the_decision(glue, monkeypatch, caplog):
    live(monkeypatch)
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "log")
    _no_deal(monkeypatch)
    caplog.set_level("INFO", logger="uvicorn")
    feed_and_wait(waited("n.8", 30))
    assert glue.sends == []
    assert "некому=да" in caplog.text and "режим log" in caplog.text


def test_panel_template_gets_the_text_in_the_note_variable(glue, monkeypatch):
    live(monkeypatch)
    panel(monkeypatch, TEMPLATE_WITH_NOTE)
    _no_deal(monkeypatch)
    try:
        feed_and_wait(waited("n.9", 30))
    finally:
        teardown_panel()
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert "(панель)" in text
    assert "ℹ️ " + NOBODY in text, "строка шаблона панели с пометкой показывает новый текст"
    assert "на приветствии" not in text


def test_helper_requires_all_conditions(monkeypatch):
    now = int(time.time())

    def missed(**kw):
        base = dict(call_id="x", phone="+79555555555", tree_name="", tree_number="000-4221010",
                    first_ts=now, last_ts=now + 30, legs=1, statuses=("NOANSWER",), staff_legs=0,
                    dropped_in_menu=True)
        base.update(kw)
        return S.MissedCall(**base)

    live(monkeypatch)
    assert U._is_nobody_to_ring(missed()) is True
    assert U._is_nobody_to_ring(missed(dropped_in_menu=False, staff_legs=2)) is False
    assert U._is_nobody_to_ring(missed(first_ts=None)) is False
    assert U._is_nobody_to_ring(missed(last_ts=None)) is False
    assert U._is_nobody_to_ring(missed(last_ts=now + 5)) is False
    monkeypatch.setattr(U, "SIPUNI_NOBODY_ALERT", False)
    assert U._is_nobody_to_ring(missed()) is False


_NOBODY_ENV = ("SIPUNI_NOBODY_ALERT", "SIPUNI_NOBODY_MIN_S", "SIPUNI_NOBODY_START_H", "SIPUNI_NOBODY_END_H")


def _cfg(monkeypatch, **env):
    for name in _NOBODY_ENV:
        monkeypatch.delenv(name, raising=False)
    return _probe_config(monkeypatch, **env)


def test_config_default_is_off_and_hours_are_10_to_19(monkeypatch):
    cfg = _cfg(monkeypatch)
    assert cfg.SIPUNI_NOBODY_ALERT is False
    assert cfg.SIPUNI_NOBODY_MIN_S == 20.0
    assert cfg.SIPUNI_NOBODY_START_H == 10 and cfg.SIPUNI_NOBODY_END_H == 19


@pytest.mark.parametrize("raw, expected", [
    ("on", True), (" ON ", True), ("off", False), ("", False), ("1", False), ("true", False), ("yes", False),
])
def test_config_only_exact_on_enables_the_alert(monkeypatch, raw, expected):
    assert _cfg(monkeypatch, SIPUNI_NOBODY_ALERT=raw).SIPUNI_NOBODY_ALERT is expected


def test_config_numbers_are_read(monkeypatch):
    cfg = _cfg(monkeypatch, SIPUNI_NOBODY_MIN_S="25.5", SIPUNI_NOBODY_START_H="9", SIPUNI_NOBODY_END_H="20")
    assert cfg.SIPUNI_NOBODY_MIN_S == 25.5 and cfg.SIPUNI_NOBODY_START_H == 9 and cfg.SIPUNI_NOBODY_END_H == 20
