"""Коллтрекинг сайта (call_click_service): приём событий сайта и склейка со звонком Sipuni.

Что защищаем:
  * мусор с сайта не принимается (вид, id, время, номер, метки у входа);
  * один клик и один звонящий в окне → «точно»: примечание и UTM в пустые поля (режим live);
  * метки в сделке уже есть → не перетираем, только примечание;
  * два клика или два звонящих в окне → «спорно»: примечание без полей;
  * клик по номеру не на Sipuni (495) и клик вне окна не склеиваются;
  * звонок без клика по номеру (набрал руками) не сопоставляется, ClientID не принимается и не
    хранится (решение Кати 17.11.2026);
  * открытой сделки нет → примечание в контакт;
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
    calls["contact_notes"] = []
    calls["contact_id"] = 777

    async def find_contact(q):
        return calls["contact_id"]

    async def contact_note(cid, text):
        calls["contact_notes"].append((cid, text))
        return True

    monkeypatch.setattr(C.api, "find_contact_id", find_contact)
    monkeypatch.setattr(C.api, "add_note_to_contact", contact_note)
    monkeypatch.setattr(C.amo_service, "get_lead_full", get_lead)
    return calls


async def _find(phone):
    return {"id": 501}


async def _nothing(phone):
    return None


def ev(kind="tel_click", at=None, number=NUMBER, utm=None, **kw):
    payload = {"schema": 1, "kind": kind, "event_id": kw.pop("event_id", None) or uuid.uuid4().hex,
               "at": time.time() if at is None else at, "number": number, "page_url": PAGE,
               "device": "mobile", "utm": UTM if utm is None else utm}
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
        C.clean_event(ev(kind="utm_visit", at=now), now)      # вход с метками больше не принимаем
    cleaned = C.clean_event({**good, "client_id": "1700000000123456789", "page_url": "javascript:1",
                             "utm": {"utm_source": "x", "evil": "y"}}, now)
    assert cleaned["client_id"] == "1700000000123456789"       # ClientID для Метрики (18.11)
    assert C.clean_event({**good, "client_id": "<x>"}, now)["client_id"] == ""
    assert cleaned["page_url"] == "" and cleaned["utm"] == {"utm_source": "x"}


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


def test_call_without_click_is_ignored(amo):
    """Набрал номер руками: клика нет - ничего не пишем ни в сделку, ни в контакт."""
    assert attribute("c9", CALLER, time.time()) == "none"
    assert amo["notes"] == [] and amo["contact_notes"] == [] and amo["utm"] == []


def test_stored_click_keeps_client_id(amo):
    accept({**ev(), "client_id": "1700000000123456789"})
    row = store.clicks_between(0, time.time() + 10)[0]
    assert row["client_id"] == "1700000000123456789" == row["payload"]["client_id"]


# --- Метрика (решение Кати 18.11.2026) ------------------------------------

import call_click_metrika as M

CID = "1700000000123456789"


@pytest.fixture
def ym(amo, monkeypatch):
    monkeypatch.setattr(M, "ENABLED", True)
    monkeypatch.setattr(M, "TOKEN", "t")
    monkeypatch.setattr(M, "COUNTER_ID", "93845803")
    sent = {"rows": [], "fail": False}

    async def post(body, comment):
        if sent["fail"]:
            raise RuntimeError("http 500")
        sent["rows"].append(body.decode())
        return {"id": len(sent["rows"]), "status": "UPLOADED"}

    monkeypatch.setattr(M, "_post", post)
    return sent


def test_metrika_sure_call_sent(ym):
    t = time.time() - 100
    accept({**ev(at=t), "client_id": CID})
    assert attribute("m1", CALLER, t + 9) == "sure"
    assert ym["rows"] == [f"ClientId,DateTime\n{CID},{int(t + 9)}\n"]


def test_metrika_same_client_same_source_once_in_3_days(ym):
    t = time.time() - 500
    accept({**ev(at=t), "client_id": CID})
    assert attribute("m2", CALLER, t + 9) == "sure"
    accept({**ev(at=t + 300), "client_id": CID})
    assert attribute("m3", CALLER, t + 309) == "sure"
    assert len(ym["rows"]) == 1


def test_metrika_same_client_other_source_counted(ym):
    t = time.time() - 500
    accept({**ev(at=t), "client_id": CID})
    assert attribute("m4", CALLER, t + 9) == "sure"
    accept({**ev(at=t + 300, utm={"utm_source": "vk", "utm_medium": "cpc"}), "client_id": CID})
    assert attribute("m5", CALLER, t + 309) == "sure"
    assert len(ym["rows"]) == 2


def test_metrika_repeat_after_3_days_counted(amo):
    import call_click_store as S
    assert S.metrika_claim("a", CID, "direct", 1000.0, 3 * 86400, 1000.0)
    assert not S.metrika_claim("b", CID, "direct", 1000.0 + 86400, 3 * 86400, 1000.0)
    assert S.metrika_claim("c", CID, "direct", 1000.0 + 3 * 86400 + 1, 3 * 86400, 1000.0)


def test_metrika_skips_without_client_id_and_ambiguous(ym):
    t = time.time() - 300
    accept(ev(at=t))
    attribute("m6", CALLER, t + 9)                      # без ClientID
    accept({**ev(at=t + 150, device="desktop"), "client_id": CID})
    accept({**ev(at=t + 160), "client_id": "1700000000999"})
    attribute("m7", CALLER, t + 170)                    # спорно
    assert ym["rows"] == []


def test_metrika_failure_releases_and_amo_still_written(ym, amo):
    ym["fail"] = True
    t = time.time() - 100
    accept({**ev(at=t), "client_id": CID})
    assert attribute("m8", CALLER, t + 9) == "sure"
    assert len(amo["notes"]) == 1
    import call_click_store as S
    assert S.metrika_claim("m8", CID, M.source_key({"utm": UTM}), t + 9, M.REPEAT_S, time.time())


def test_metrika_off_by_default(amo):
    t = time.time() - 100
    accept({**ev(at=t), "client_id": CID})
    assert attribute("m9", CALLER, t + 9) == "sure"     # выключено - ничего не шлём, склейка идёт


def test_source_key():
    assert M.source_key({"utm": {"utm_source": "yandex_direct", "utm_medium": "cpc", "utm_campaign": "k"}}) == "utm:yandex_direct|cpc|k"
    assert M.source_key({"utm": {}, "referrer": "https://www.google.com/"}) == "ref:google.com"
    assert M.source_key({"utm": {}, "referrer": ""}) == "direct"


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


def test_no_lead_note_goes_to_contact(amo):
    """17.11.2026: известный номер, все сделки закрыты, Sipuni новую не создал - пишем в контакт."""
    t = time.time() - 100
    accept(ev(at=t, place="header-phone"))
    assert attribute("c13", CALLER, t + 9, _nothing) == "contact"
    assert amo["notes"] == [] and amo["utm"] == []
    cid, text = amo["contact_notes"][0]
    assert cid == 777 and "Звонок с сайта" in text and "utm_campaign=keystone" in text
    assert text.endswith("Открытой сделки по номеру не было, поэтому примечание в контакте.")
    assert attribute("c13", CALLER, t + 9, _nothing) in ("none", "skip")
    assert len(amo["contact_notes"]) == 1


def test_no_lead_no_contact(amo):
    amo["contact_id"] = None
    t = time.time() - 100
    accept(ev(at=t))
    assert attribute("c14", CALLER, t + 9, _nothing) == "no-lead"
    assert amo["contact_notes"] == []


def test_contact_in_log_mode_not_written(amo, monkeypatch):
    monkeypatch.setattr(C, "CALL_CLICK_MODE", "log")
    t = time.time() - 100
    accept(ev(at=t))
    assert attribute("c15", CALLER, t + 9, _nothing) == "contact"
    assert amo["contact_notes"] == []


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


def test_referrer_written_without_utm(amo):
    """18.11.2026: органика без меток - источник перехода всё равно в поле referrer."""
    t = time.time() - 100
    accept(ev(at=t, utm={}, referrer="https://yandex.ru/"))
    assert attribute("c22", CALLER, t + 7) == "sure"
    assert amo["utm"] == [(501, {"referrer": "https://yandex.ru/"})]
    text = amo["notes"][0][1]
    assert "Откуда пришёл: https://yandex.ru/" in text
    assert "Метки визита: нет\n" in text and "прямой заход" not in text


def test_no_utm_no_referrer_no_fields(amo):
    t = time.time() - 100
    accept(ev(at=t, utm={}))
    assert attribute("c23", CALLER, t + 7) == "sure"
    assert amo["utm"] == []


def test_existing_referrer_not_overwritten(amo):
    amo["lead"] = {"id": 501, "custom_fields_values": [
        {"field_code": "REFERRER", "values": [{"value": "https://google.com/"}]}]}
    t = time.time() - 100
    accept(ev(at=t, referrer="https://yandex.ru/"))
    assert attribute("c24", CALLER, t + 7) == "sure"
    assert amo["utm"] == [] and len(amo["notes"]) == 1


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


def test_same_visitor_different_pages_is_one_click(amo):
    """18.11.2026 10:03/10:04: один мобильный нажал номер в каталоге и на главной - это один человек."""
    t = time.time() - 200
    accept(ev(at=t, page_url="https://sunscrypt.ru/product-category/hardware-wallets/", place="header"))
    accept(ev(at=t + 71, page_url="https://sunscrypt.ru/", place="header"))
    assert attribute("c40", CALLER, t + 82) == "sure"
    text = amo["notes"][0][1]
    assert "Страница: https://sunscrypt.ru/\n" in text and "позвонил через 11 с" in text


def test_different_source_still_two_visitors(amo):
    t = time.time() - 200
    accept(ev(at=t, referrer="https://yandex.ru/"))
    accept(ev(at=t + 20, referrer="https://google.com/"))
    assert attribute("c41", CALLER, t + 30) == "ambiguous"


def test_direct_visit_marked_in_note(amo):
    """18.11.2026: ни меток, ни источника - пометка «зашёл напрямую»."""
    t = time.time() - 100
    accept(ev(at=t, utm={}))
    assert attribute("d1", CALLER, t + 7) == "sure"
    assert "Потенциально знаком с компанией - зашёл на сайт напрямую" in amo["notes"][0][1]


def test_no_direct_mark_with_source(amo):
    t = time.time() - 300
    accept(ev(at=t, utm={}, referrer="https://yandex.ru/"))
    attribute("d2", CALLER, t + 7)
    accept(ev(at=t + 200))                      # с метками
    attribute("d3", OTHER, t + 207)
    assert all("напрямую" not in n[1] for n in amo["notes"]) and len(amo["notes"]) == 2
