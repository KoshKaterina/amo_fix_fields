"""Коллтрекинг сайта (call_click_service): приём событий сайта и склейка со звонком Sipuni.

Что защищаем:
  * мусор с сайта не принимается (вид, id, время, номер, метки у входа);
  * один клик и один звонящий в окне → «точно»: примечание и UTM в пустые поля (режим live);
  * метки в сделке уже есть → не перетираем, только примечание;
  * два клика или два звонящих в окне → «спорно»: примечание без полей;
  * клик по номеру не на Sipuni (495) и клик вне окна не склеиваются;
  * повторный клиент без клика: телефон из заявки → ClientID → визит с метками → примечание
    «по прошлому визиту» один раз на сделку;
  * режим log в amo не пишет; сбой примечания освобождает запись для повтора;
  * выключенный модуль (по умолчанию) ничего не делает в обработчиках Sipuni.
Сети нет: amo подменена. Запуск: python -m pytest test_call_click.py -q
"""
import asyncio
import time
import uuid

import pytest

import call_click_service as C
import call_click_store as store

NUMBER = "+79056063678"
CALLER = "+79991234567"
OTHER = "+79997654321"
UTM = {"utm_source": "yandex_direct", "utm_medium": "cpc", "utm_campaign": "keystone",
       "utm_content": "{ad_id}", "utm_term": "{keyword}"}
PAGE = "https://sunscrypt.ru/apparatnye-koshelki-keystone/?utm_source=yandex_direct"


@pytest.fixture
def amo(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "DB_PATH", str(tmp_path / "cc.sqlite3"))
    monkeypatch.setattr(C, "CALL_CLICK_MODE", "live")
    monkeypatch.setattr(C, "CALL_CLICK_SECRET", "s")
    monkeypatch.setattr(C, "LEAD_RETRIES", ())
    monkeypatch.setattr(C, "_last_cleanup", time.time())
    monkeypatch.setattr(C, "_rate", [])
    store.init_db()
    calls = {"notes": [], "utm": [], "lead": {"id": 501, "custom_fields_values": []}, "note_ok": True}

    async def add_note(lead_id, text, **kw):
        calls["notes"].append((lead_id, text))
        return calls["note_ok"]

    async def set_utm(lead_id, utm):
        calls["utm"].append((lead_id, utm))
        return True

    async def get_lead(lead_id, with_=()):
        return calls["lead"]

    monkeypatch.setattr(C.api, "add_note_to_lead", add_note)
    monkeypatch.setattr(C.api, "set_lead_utm", set_utm)
    monkeypatch.setattr(C.amo_service, "get_lead_full", get_lead)
    return calls


async def _find(phone):
    return {"id": 501}


async def _nothing(phone):
    return None


def ev(kind="tel_click", at=None, number=NUMBER, utm=None, client_id="", **kw):
    payload = {"schema": 1, "kind": kind, "event_id": kw.pop("event_id", None) or uuid.uuid4().hex,
               "at": time.time() if at is None else at, "number": number, "page_url": PAGE,
               "client_id": client_id, "device": "mobile", "utm": UTM if utm is None else utm}
    payload.update(kw)
    return payload


def accept(payload):
    return asyncio.run(C.accept(payload))


def attribute(call_id, phone, start, finder=_find):
    return asyncio.run(C.attribute_call(call_id, phone, start, finder))


# --- приём ---------------------------------------------------------------

def test_clean_event_rejects_garbage():
    now = 1_800_000_000
    good = ev(at=now)
    assert C.clean_event(good, now)["number"] == NUMBER
    for bad in ({"schema": 2}, {"kind": "x"}, {"event_id": "nope"}, {"at": now - 3600},
                {"number": "abc"}):
        with pytest.raises(C.PayloadError):
            C.clean_event({**good, **bad}, now)
    with pytest.raises(C.PayloadError):
        C.clean_event(ev(kind="utm_visit", at=now, utm={}), now)
    cleaned = C.clean_event({**good, "client_id": "<script>", "page_url": "javascript:1",
                             "utm": {"utm_source": "x", "evil": "y"}}, now)
    assert cleaned["client_id"] == "" and cleaned["page_url"] == "" and cleaned["utm"] == {"utm_source": "x"}


def test_accept_stores_once(amo):
    p = ev()
    assert accept(p) == (200, {"ok": True})
    assert accept(p) == (200, {"ok": True})
    assert len(store.clicks_between(0, time.time() + 10)) == 1
    assert accept({**p, "kind": "bad"})[0] == 422


# --- склейка -------------------------------------------------------------

def test_sure_match_writes_note_and_empty_utm(amo):
    t = time.time() - 100
    accept(ev(at=t))
    C.call_started("c1", CALLER, t + 9)
    assert attribute("c1", CALLER, t + 9) == "sure"
    assert len(amo["notes"]) == 1
    text = amo["notes"][0][1]
    assert "Звонок с сайта" in text and "через 9 с" in text
    assert "utm_source=yandex_direct" in text and "utm_term={keyword}" in text
    assert "Страница: https://sunscrypt.ru/apparatnye-koshelki-keystone/\n" in text + "\n"
    assert amo["utm"] == [(501, UTM)]
    # тот же звонок второй раз (отвеченный после пропущенного не бывает, но дубль события бывает)
    assert attribute("c1", CALLER, t + 9) in ("none", "skip")
    assert len(amo["notes"]) == 1


def test_existing_utm_not_overwritten(amo):
    amo["lead"] = {"id": 501, "custom_fields_values": [
        {"field_code": "UTM_SOURCE", "values": [{"value": "telegram"}]}]}
    t = time.time() - 100
    accept(ev(at=t))
    assert attribute("c2", CALLER, t + 5) == "sure"
    assert len(amo["notes"]) == 1 and amo["utm"] == []


def test_two_clicks_ambiguous(amo):
    t = time.time() - 100
    accept(ev(at=t))
    accept(ev(at=t + 20, utm={"utm_source": "vk"}))
    assert attribute("c3", CALLER, t + 30) == "ambiguous"
    text = amo["notes"][0][1]
    assert "точно не связать" in text and "utm_source=vk" in text
    assert amo["utm"] == []


def test_two_callers_in_window_ambiguous(amo):
    t = time.time() - 100
    accept(ev(at=t))
    C.call_started("c4", CALLER, t + 10)
    C.call_started("c5", OTHER, t + 15)
    assert attribute("c4", CALLER, t + 10) == "ambiguous"
    assert amo["utm"] == []


def test_same_caller_twice_still_sure(amo):
    t = time.time() - 100
    accept(ev(at=t))
    C.call_started("c6", CALLER, t + 10)
    C.call_started("c7", CALLER, t + 40)
    assert attribute("c6", CALLER, t + 10) == "sure"


def test_other_number_and_out_of_window_ignored(amo):
    t = time.time() - 1000
    accept(ev(at=t, number="+74953203206"))
    accept(ev(at=t - 500))
    assert attribute("c8", CALLER, t + 10) == "none"
    assert amo["notes"] == []


def test_repeat_client_by_form_phone(amo):
    now = time.time()
    asyncio.run(C.link_form_phone(CALLER, "1700000000123456789"))
    accept(ev(kind="utm_visit", at=now - 300, number="", client_id="1700000000123456789"))
    assert attribute("c9", CALLER, now) == "repeat"
    assert "по прошлому визиту" in amo["notes"][0][1]
    assert amo["utm"] == []
    assert attribute("c10", CALLER, now + 60) == "skip"
    assert len(amo["notes"]) == 1


def test_log_mode_does_not_touch_amo(amo, monkeypatch):
    monkeypatch.setattr(C, "CALL_CLICK_MODE", "log")
    t = time.time() - 100
    accept(ev(at=t))
    assert attribute("c11", CALLER, t + 9) == "sure"
    assert amo["notes"] == [] and amo["utm"] == []


def test_note_failure_releases(amo):
    amo["note_ok"] = False
    t = time.time() - 100
    accept(ev(at=t))
    attribute("c12", CALLER, t + 9)
    assert store.claim_attribution(501, "call:c12") is True  # запись освобождена для повтора


def test_no_lead(amo):
    t = time.time() - 100
    accept(ev(at=t))
    assert attribute("c13", CALLER, t + 9, _nothing) == "no-lead"
    assert amo["notes"] == []


def test_disabled_is_noop(monkeypatch):
    monkeypatch.setattr(C, "CALL_CLICK_MODE", "off")
    C.call_started("x", CALLER, 1)          # не трогает базу
    C.schedule_attribution("x", CALLER, 1, _find)
    assert C.is_enabled() is False


def test_place_in_note_and_referrer_field(amo):
    t = time.time() - 100
    accept(ev(at=t, place="header-phone", link_text="+7(905)606-36-78", referrer="https://yandex.ru/"))
    assert attribute("c20", CALLER, t + 7) == "sure"
    text = amo["notes"][0][1]
    assert "Где нажал номер: шапка сайта, номер в шапке\n" in text + "\n"
    assert "кнопка" not in text            # текст ссылки - сам номер, его не повторяем
    assert amo["utm"] == [(501, {**UTM, "referrer": "https://yandex.ru/"})]


def test_place_block_and_button_text(amo):
    t = time.time() - 100
    accept(ev(at=t, place="#sales", link_text="Позвонить   в отдел"))
    attribute("c21", CALLER, t + 7)
    assert "Где нажал номер: блок страницы #sales, кнопка «Позвонить в отдел»" in amo["notes"][0][1]


def test_place_garbage_dropped():
    now = 1_800_000_000
    cleaned = C.clean_event(ev(at=now, place="<b>x</b>", link_text="a" * 500), now)
    assert cleaned["place"] == "" and len(cleaned["link_text"]) == 60


def test_no_utm_no_fields_even_with_referrer(amo):
    t = time.time() - 100
    accept(ev(at=t, utm={}, referrer="https://google.com/"))
    assert attribute("c22", CALLER, t + 7) == "sure"
    assert amo["utm"] == []
    assert "Откуда пришёл: https://google.com/" in amo["notes"][0][1]


def test_same_visitor_double_tap_is_one_click(amo):
    """17.11.2026: Катя нажала номер дважды за 40 с и позвонила - было «спорно», должно быть «точно»."""
    t = time.time() - 200
    accept(ev(at=t))
    accept(ev(at=t + 41))
    assert attribute("c30", CALLER, t + 53) == "sure"
    assert "позвонил через 12 с" in amo["notes"][0][1]
    assert amo["utm"] == [(501, UTM)]
    # оба клика заняты этим звонком: следующий звонок их не подхватит
    assert attribute("c31", OTHER, t + 80) == "none"


def test_two_visitors_still_ambiguous(amo):
    t = time.time() - 200
    accept(ev(at=t))
    accept(ev(at=t + 10, device="desktop"))
    assert attribute("c32", CALLER, t + 30) == "ambiguous"


def test_lead_found_on_retry(amo, monkeypatch):
    monkeypatch.setattr(C, "LEAD_RETRIES", (0, 0))
    calls = []

    async def finder(phone):
        calls.append(phone)
        return {"id": 501} if len(calls) == 3 else None

    t = time.time() - 100
    accept(ev(at=t))
    assert attribute("c33", CALLER, t + 9, finder) == "sure"
    assert len(calls) == 3 and len(amo["notes"]) == 1
