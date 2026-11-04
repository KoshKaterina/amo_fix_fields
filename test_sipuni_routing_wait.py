"""Звонок, брошенный на приветствии: ждём, пока распределение назначит ответственного новой сделке,
и пингуем его (решение Кати 04.11.2026, вместо пинга всех сразу из решения 03.11.2026).

Что защищаем:
  * новая сделка ещё не распределена: алерт ждёт и уходит с тегом назначенного ответственного;
  * не дождались за SIPUNI_ROUTING_WAIT_S или приложение останавливается: пингуем весь пул и
    говорим об этом в тексте (алерт не теряется);
  * сделки по телефону нет: пул сразу (ответственного не будет);
  * сделка уже в работе, распределена раньше или ожидание выключено: ответственный из сделки
    сразу, без ожидания;
  * звонок, который дошёл до сотрудников и не был взят, ведёт себя как раньше (не ждёт).

События те же, что в замере 03.11.2026 (см. раздел «РЕАЛЬНЫЕ события» в test_sipuni_events.py).
Запуск: python -m pytest test_sipuni_routing_wait.py -q
"""
import asyncio
import time

import sipuni_events as S
import uis_missed_call as U
from test_sipuni_events import (  # noqa: F401 - glue: фикстура, её нужно держать в этом модуле
    _no_deal, _spy_decide_responsible, _spy_mentions, glue, idle, r_greeting_drop, r_not_taken, run,
)

NEW_LEAD = 83537714     # этап «Новый лид»
IN_WORK = 83537718      # сделка уже в работе
LEAD = 36500001
DUTY = 13929334         # ответственный, с которым сделку создал Sipuni (дежурный)
ASSIGNED = 9291546      # кого назначило распределение


def deal(monkeypatch, *, status, responsible=DUTY):
    async def find(query, *a, **k):
        return [{"id": LEAD, "status_id": status, "responsible_user_id": responsible, "updated_at": 5}]

    monkeypatch.setattr(U.amo_service, "find_leads_by_query", find)


def live(monkeypatch, *, wait=None):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    if wait is not None:
        monkeypatch.setattr(U, "SIPUNI_ROUTING_WAIT_S", wait)


def routed_record(*, ago=0.0, user=ASSIGNED):
    return {"lead_id": LEAD, "routed_at": int(time.time() - ago), "assigned_user_id": user, "profile_id": "p"}


def drop(cid):
    return r_greeting_drop(cid, ts=int(time.time()))


def test_waits_for_routing_then_pings_the_assigned_responsible(glue, monkeypatch):
    live(monkeypatch)                                # ожидание 0.6 с из фикстуры glue
    deal(monkeypatch, status=NEW_LEAD)
    asked = _spy_mentions(monkeypatch)
    routed = _spy_decide_responsible(monkeypatch)

    async def route_later():
        await asyncio.sleep(0.25)
        glue.routed[LEAD] = routed_record()

    async def go():
        later = asyncio.create_task(route_later())
        await U._sipuni_apply(drop("rw.1"))
        await asyncio.sleep(0.15)
        assert glue.sends == [], "пока сделку не распределили, алерт не уходит"
        await idle(S.tracker)
        await later

    run(go())
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert asked == [ASSIGNED] and routed == [ASSIGNED]
    assert "@only-responsible" in text and "@pool-ping" not in text
    assert U._DROPPED_IN_MENU_NOTE in text and U._ROUTING_TIMEOUT_NOTE not in text
    assert U._callback_pending[LEAD]["responsible_id"] == ASSIGNED, "перезвон сторожим за назначенным"


def test_timeout_pings_everyone_and_says_why(glue, monkeypatch):
    live(monkeypatch)
    deal(monkeypatch, status=NEW_LEAD)
    asked = _spy_mentions(monkeypatch)
    routed = _spy_decide_responsible(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("rw.2"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert asked == [None] and routed == [None]
    assert "@pool-ping" in text
    assert U._DROPPED_IN_MENU_NOTE in text and U._ROUTING_TIMEOUT_NOTE in text


def test_no_deal_pings_everyone_at_once(glue, monkeypatch):
    live(monkeypatch, wait=5)                        # если бы ждали, idle() не дождался бы
    _no_deal(monkeypatch)
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("rw.3"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and asked == [None]
    assert U._ROUTING_TIMEOUT_NOTE not in glue.sends[0][0]


def test_deal_in_work_pings_its_responsible_at_once(glue, monkeypatch):
    live(monkeypatch, wait=5)
    deal(monkeypatch, status=IN_WORK)
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("rw.4"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and asked == [DUTY]
    assert "@only-responsible" in glue.sends[0][0]


def test_deal_routed_before_the_call_does_not_wait(glue, monkeypatch):
    """Запись о распределении старше звонка: сделка уже чья-то, ответственного берём из amo."""
    live(monkeypatch, wait=5)
    deal(monkeypatch, status=NEW_LEAD)
    glue.routed[LEAD] = routed_record(ago=3600, user=ASSIGNED)
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("rw.5"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and asked == [DUTY]


def test_deal_already_routed_by_the_time_of_alert_pings_the_assigned(glue, monkeypatch):
    live(monkeypatch, wait=5)
    deal(monkeypatch, status=NEW_LEAD)
    glue.routed[LEAD] = routed_record()
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("rw.6"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and asked == [ASSIGNED]


def test_wait_switched_off_pings_the_current_responsible(glue, monkeypatch):
    live(monkeypatch, wait=0)
    deal(monkeypatch, status=NEW_LEAD)
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("rw.7"))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and asked == [DUTY]


def test_shutdown_releases_the_waiting_alert_instead_of_losing_it(glue, monkeypatch):
    live(monkeypatch, wait=30)
    deal(monkeypatch, status=NEW_LEAD)
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(drop("rw.8"))
        await asyncio.sleep(0.2)
        assert glue.sends == [] and len(U._routing_waiters) == 1, "ждёт назначения"
        await U.shutdown()

    run(go())
    assert len(glue.sends) == 1 and asked == [None], "при остановке алерт уходит, пингуем всех"
    assert U._routing_waiters == set()


def test_unanswered_queue_does_not_wait_even_for_a_new_deal(glue, monkeypatch):
    """Звонок дошёл до сотрудников и не был взят: как раньше, ответственному сразу."""
    live(monkeypatch, wait=5)
    deal(monkeypatch, status=NEW_LEAD)
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(r_not_taken("rw.9", ts=int(time.time()), legs=3))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1 and asked == [DUTY]
    assert U._DROPPED_IN_MENU_NOTE not in glue.sends[0][0]
