"""Поиск сделки по телефону для алерта «ПРОПУЩЕННЫЙ звонок» идёт по ВСЕМ страницам выдачи amo.

Почему: `amo_service.find_leads_by_query` отдаёт одну страницу в 50 сделок по возрастанию номера.
У контакта с сотнями сделок открытая оказывается за этой страницей, и код считает, что сделки нет:
алерт без ссылки, пинг всего пула вместо ответственного, ожидание распределения и теги не
срабатывают. Живой случай 04.11.2026: контакт 79259993699, 295 сделок, три открытые (воронка
«Работа с базой»), алерт в 16:15 ушёл без ссылки.

Что защищаем:
  * открытая сделка за пределами первых 50 находится (выбирается самая свежая по работе);
  * старая постраничная функция в этом поиске не вызывается;
  * сбой amo или слишком длинная выдача (None) - «сделку не нашли», а не падение;
  * то же для входа UIS: алерт получает ссылку и ответственного.

Запуск: python -m pytest test_sipuni_find_lead.py -q
"""
import amo_service
import uis_missed_call as U
from test_sipuni_events import (  # noqa: F401 - glue: фикстура, её нужно держать в этом модуле
    CALLER_DIGITS, _spy_mentions, glue, idle, r_greeting_drop, run,
)
from waybill_config import STATUS_NEW_LEAD_ALL
import sipuni_events as S
import time

CLOSED = 143
BASE_STAGE = 89434514        # «Работа с базой / Купили на МП»: не закрыта, но и не «Новый лид»
OPEN_IDS = (36588675, 36588677, 36588679)
FRESHEST = OPEN_IDS[-1]
RESPONSIBLE = 13946318


def heavy_contact_leads(closed=292):
    """292 старые закрытые (то, что видит первая страница) и три открытые сзади, 295 всего."""
    old = [{"id": i, "status_id": CLOSED, "responsible_user_id": 1, "updated_at": 1000 + i}
           for i in range(1, closed + 1)]
    new = [{"id": lid, "status_id": BASE_STAGE, "responsible_user_id": RESPONSIBLE,
            "updated_at": 5000 + n} for n, lid in enumerate(OPEN_IDS)]
    return old + new


def patch_search(monkeypatch, leads):
    async def find_all(query, *a, **k):
        return leads

    async def old_search(query, *a, **k):
        raise AssertionError("постраничный поиск find_leads_by_query здесь вызываться не должен")

    monkeypatch.setattr(amo_service, "find_leads_by_query_all", find_all)
    monkeypatch.setattr(amo_service, "find_leads_by_query", old_search)


def test_open_deal_beyond_the_first_page_is_found(monkeypatch):
    patch_search(monkeypatch, heavy_contact_leads())
    row = run(U._find_lead_row("79259993699"))
    assert row["id"] == FRESHEST and row["status_id"] == BASE_STAGE
    assert run(U._find_lead("79259993699")) == (FRESHEST, RESPONSIBLE)
    assert run(U._find_lead_row("79259993699", statuses=STATUS_NEW_LEAD_ALL)) is None, \
        "открытые сделки не на этапе «Новый лид»: для тегов и ожидания их нет"


def test_all_closed_means_no_deal(monkeypatch):
    patch_search(monkeypatch, heavy_contact_leads()[:292])
    assert run(U._find_lead_row("79259993699")) is None
    assert run(U._find_lead("79259993699")) == (None, None)


def test_search_failure_is_no_deal_not_a_crash(monkeypatch):
    patch_search(monkeypatch, None)
    assert run(U._find_lead_row("79259993699")) is None
    assert run(U._find_lead("79259993699")) == (None, None)


def test_dropped_call_of_a_heavy_contact_pings_the_responsible_with_a_link(glue, monkeypatch):
    monkeypatch.setattr(U, "SIPUNI_EVENTS_MODE", "live")
    patch_search(monkeypatch, heavy_contact_leads())
    asked = _spy_mentions(monkeypatch)

    async def go():
        await U._sipuni_apply(r_greeting_drop("fl.1", ts=int(time.time())))
        await idle(S.tracker)

    run(go())
    assert len(glue.sends) == 1
    text = glue.sends[0][0]
    assert asked == [RESPONSIBLE], "ответственный открытой сделки, а не весь пул"
    assert str(FRESHEST) in text, "в алерте ссылка на сделку"
    assert FRESHEST in U._callback_pending, "перезвон сторожим по найденной сделке"


def test_uis_alert_for_a_heavy_contact_has_the_link_too(glue, monkeypatch):
    patch_search(monkeypatch, heavy_contact_leads())

    async def go():
        await U._apply({"contact_phone_number": CALLER_DIGITS, "contact_full_name": "",
                        "call_session_id": "uis-heavy"})

    run(go())
    assert len(glue.sends) == 1 and str(FRESHEST) in glue.sends[0][0]
