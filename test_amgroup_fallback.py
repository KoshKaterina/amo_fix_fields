"""Тесты протеза amgroup (amgroup_fallback) - скелет: чтение МойСклада,
отсев Озона/TangemShop, поиск существующей сделки. Сборку и создание сделки
делает соседний срез (amgroup_lead_builder), здесь используем простую
async-заглушку и проверяем только контракт (когда её зовут, когда нет, когда
результат помечает заказ обработанным).

Написаны по инциденту 02-03.09.2026: сторонняя интеграция МойСклад -> amoCRM
встала, и по ложному алерту order_watchdog в тот же день - склад НЕ ОТВЕТИВШИЙ
нельзя читать как «заказов нет». Дополнены 03.09.2026 находками приёмки
безопасности: та же путаница на пути записи (поиск сделки в amoCRM), сухой
режим не должен отравлять состояние, сбой создания не должен хоронить заказ
навсегда.

Запуск: python3 -m pytest test_amgroup_fallback.py -q
"""

import asyncio
import datetime
import os
import sys

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import amgroup_fallback  # noqa: E402
from waybill_config import FIELD_MOYSKLAD_ORDER_UUID  # noqa: E402

UTC = datetime.timezone.utc


def _ms_order(uuid, number, *, channel=None, agent=None):
    order = {"id": uuid, "name": number}
    if channel:
        order["salesChannel"] = {"name": channel}
    if agent:
        order["agent"] = {"name": agent}
    return order


def _amo_lead(lead_id, order_uuid):
    return {
        "id": lead_id,
        "custom_fields_values": [
            {"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": order_uuid}]},
        ],
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    monkeypatch.setattr(amgroup_fallback, "_STATE_PATH", str(tmp_path / "logged.json"), raising=False)
    monkeypatch.setattr(amgroup_fallback, "_logged_loaded", False, raising=False)
    monkeypatch.setattr(amgroup_fallback, "_DEAL_STATE_PATH", str(tmp_path / "has_deal.json"), raising=False)
    monkeypatch.setattr(amgroup_fallback, "_deal_loaded", False, raising=False)
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", True, raising=False)
    amgroup_fallback._logged.clear()
    amgroup_fallback._known_with_deal.clear()
    yield
    amgroup_fallback._logged.clear()
    amgroup_fallback._known_with_deal.clear()


def _run(monkeypatch, ms_pages, amo_leads_by_query=None):
    """ms_pages: список страниц entity/customerorder (каждая - список rows,
    последняя короче limit=100 сигналит конец). amo_leads_by_query: функция
    query -> list[dict] | None для find_leads_by_query (None = amoCRM не
    ответил), по умолчанию всегда пусто (честный «не найдено»)."""
    async def fake_get(path, params=None):
        offset = (params or {}).get("offset", 0)
        idx = offset // 100
        if idx >= len(ms_pages):
            return {"rows": []}
        return {"rows": ms_pages[idx]}

    async def fake_find(query, with_=(), limit=50):
        if amo_leads_by_query is None:
            return []
        return amo_leads_by_query(query)

    monkeypatch.setattr(amgroup_fallback.ms_client, "get", fake_get)
    monkeypatch.setattr(amgroup_fallback.amo_service, "find_leads_by_query", fake_find)
    return asyncio.run(amgroup_fallback.check_once())


def test_sklad_ne_otvetil_nichego_ne_delaem(monkeypatch, caplog):
    """Склад не ответил (get вернул None) - проход прерывается, это НЕ
    читается как «заказов нет», и предупреждение видно в логе."""
    async def fake_get(path, params=None):
        return None

    monkeypatch.setattr(amgroup_fallback.ms_client, "get", fake_get)
    monkeypatch.setattr(amgroup_fallback.amo_service, "find_leads_by_query",
                         lambda query, with_=(), limit=50: pytest.fail("amo не должен опрашиваться"))

    with caplog.at_level("WARNING", logger="uvicorn"):
        result = asyncio.run(amgroup_fallback.check_once())

    assert result == {"ms_answered": False, "amo_answered": True, "total": 0, "excluded": 0, "too_young": 0, "with_deal": 0, "missing": 0}
    assert any("не ответил" in rec.message for rec in caplog.records)


def test_zakaz_ozona_po_kanalu_otseivaetsya(monkeypatch):
    """Канал продаж «Маркетплейс» - это Озон, в amo не ездит никогда."""
    order = _ms_order("uuid-1", "07200", channel="Маркетплейс")
    result = _run(monkeypatch, [[order]])

    assert result == {"ms_answered": True, "amo_answered": True, "total": 1, "excluded": 1, "too_young": 0, "with_deal": 0, "missing": 0}


def test_zakaz_ozona_po_kontragentu_otseivaetsya(monkeypatch):
    """Контрагент ООО «ИНТЕРНЕТ РЕШЕНИЯ» - тоже Озон, канал тут ни при чём."""
    order = _ms_order("uuid-2", "07201", agent='ООО "ИНТЕРНЕТ РЕШЕНИЯ"')
    result = _run(monkeypatch, [[order]])

    assert result["excluded"] == 1
    assert result["missing"] == 0


def test_zakaz_tangemshop_otseivaetsya(monkeypatch):
    """Канал «TangemShop» - другой магазин, в amo эту воронку не ведём."""
    order = _ms_order("uuid-3", "07202", channel="TangemShop")
    result = _run(monkeypatch, [[order]])

    assert result["excluded"] == 1
    assert result["missing"] == 0


def test_zakaz_so_sdelkoy_v_missing_ne_popadaet(monkeypatch):
    """По заказу уже есть сделка (найдена по UUID и подтверждена значением
    поля) - в список без сделки он попасть не должен."""
    order = _ms_order("uuid-4", "07203", channel="Магазин")

    def amo_leads(query):
        if query == "uuid-4":
            return [_amo_lead(111, "uuid-4")]
        return []

    result = _run(monkeypatch, [[order]], amo_leads_by_query=amo_leads)

    assert result == {"ms_answered": True, "amo_answered": True, "total": 1, "excluded": 0, "too_young": 0, "with_deal": 1, "missing": 0}


def test_polnotekstovyy_poisk_ne_obmanyvaet(monkeypatch):
    """find_leads_by_query цепляет соседнюю сделку с другим UUID в поле -
    значение поля не совпадает, значит сделки по ЭТОМУ заказу нет."""
    order = _ms_order("uuid-5", "07204", channel="Магазин")

    def amo_leads(query):
        return [_amo_lead(222, "uuid-5555-другая-сделка")]

    result = _run(monkeypatch, [[order]], amo_leads_by_query=amo_leads)

    assert result["with_deal"] == 0
    assert result["missing"] == 1


def test_zakaz_bez_sdelki_popadaet_v_missing_i_ne_sozdaetsya_v_suhom_rezhime(monkeypatch):
    """Обычный заказ без сделки - попадает в missing; в сухом режиме
    (по умолчанию в тестах) точка расширения не зовётся и заказ НЕ
    помечается обработанным (иначе переход в боевой режим его бы не поймал -
    находка приёмки безопасности 03.09.2026)."""
    called = []

    async def fake_create(order):
        called.append(order)
        return 1

    amgroup_fallback.create_lead_for_order = fake_create

    order = _ms_order("uuid-6", "07205", channel="Магазин")
    result = _run(monkeypatch, [[order]])

    amgroup_fallback.create_lead_for_order = None
    assert result["missing"] == 1
    assert called == []
    assert amgroup_fallback._logged == set()


def test_suhoy_rezhim_ne_otravlyaet_sostoyanie_boevoy_podhvatyvaet(monkeypatch):
    """Заказ увиден в сухом режиме (ничего не создалось) -> переключаем на
    боевой режим -> тот же заказ на следующем проходе ДОЛЖЕН создаться, а не
    считаться уже обработанным."""
    calls = []

    async def fake_create(order):
        calls.append(order)
        return 999

    amgroup_fallback.create_lead_for_order = fake_create
    order = _ms_order("uuid-dry2combat", "07210", channel="Магазин")

    # проход 1: сухой режим (по умолчанию в _clean)
    _run(monkeypatch, [[order]])
    assert calls == []
    assert amgroup_fallback._logged == set()

    # проход 2: боевой режим - тот же заказ должен подхватиться
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", False, raising=False)
    _run(monkeypatch, [[order]])

    amgroup_fallback.create_lead_for_order = None
    assert len(calls) == 1
    assert calls[0]["id"] == "uuid-dry2combat"
    assert "uuid-dry2combat" in amgroup_fallback._logged


def test_missing_ne_povtoryaetsya_kazhdyy_prohod(monkeypatch):
    """Дедуп на диске: заказ, по которому сделка УСПЕШНО создалась, не должен
    звать создание сделки повторно на следующем проходе."""
    calls = []

    async def fake_create(order):
        calls.append(order)
        return 555  # успех - вернули id созданной сделки

    amgroup_fallback.create_lead_for_order = fake_create
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", False, raising=False)

    order = _ms_order("uuid-7", "07206", channel="Магазин")
    _run(monkeypatch, [[order]])
    _run(monkeypatch, [[order]])

    amgroup_fallback.create_lead_for_order = None
    assert len(calls) == 1


def test_sboy_sozdaniya_ne_hooronit_zakaz_navsegda(monkeypatch):
    """create_lead_for_order вернул None (сбой, ничего не глотаем молча) -
    заказ НЕ помечается обработанным и на следующем проходе создание
    вызывается повторно, пока не получится (находка приёмки безопасности
    03.09.2026 - раньше любой сбой хоронил заказ навсегда)."""
    calls = []
    results = iter([None, 777])  # первый проход - сбой, второй - успех

    async def fake_create(order):
        calls.append(order)
        return next(results)

    amgroup_fallback.create_lead_for_order = fake_create
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", False, raising=False)

    order = _ms_order("uuid-8", "07207", channel="Магазин")
    _run(monkeypatch, [[order]])
    assert amgroup_fallback._logged == set()  # сбой - не помечен

    _run(monkeypatch, [[order]])
    amgroup_fallback.create_lead_for_order = None

    assert len(calls) == 2  # повтор случился
    assert "uuid-8" in amgroup_fallback._logged  # второй раз - успех, помечен


def test_ischeklyucheniye_pri_sozdanii_tozhe_ne_hooronit_zakaz(monkeypatch):
    """create_lead_for_order упал исключением - заказ тоже не помечается
    обработанным (не только «тихий None»)."""
    calls = []

    async def fake_create(order):
        calls.append(order)
        raise RuntimeError("boom")

    amgroup_fallback.create_lead_for_order = fake_create
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", False, raising=False)

    order = _ms_order("uuid-9", "07208", channel="Магазин")
    _run(monkeypatch, [[order]])

    amgroup_fallback.create_lead_for_order = None
    assert len(calls) == 1
    assert amgroup_fallback._logged == set()


def test_amo_ne_otvetil_pri_poiske_sdelki_prohod_preryvaetsya(monkeypatch, caplog):
    """amoCRM не ответил на поиск существующей сделки (find_leads_by_query
    вернул None, не пустой список) - проход прерывается ЦЕЛИКОМ, точка
    расширения не зовётся вовсе (лучше повтор, чем дубль сделки)."""
    called = []

    async def fake_create(order):
        called.append(order)
        return 1

    amgroup_fallback.create_lead_for_order = fake_create
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", False, raising=False)

    order = _ms_order("uuid-10", "07209", channel="Магазин")

    async def fake_get(path, params=None):
        return {"rows": [order]} if (params or {}).get("offset", 0) == 0 else {"rows": []}

    async def fake_find(query, with_=(), limit=50):
        return None  # amoCRM не ответил

    monkeypatch.setattr(amgroup_fallback.ms_client, "get", fake_get)
    monkeypatch.setattr(amgroup_fallback.amo_service, "find_leads_by_query", fake_find)

    with caplog.at_level("WARNING", logger="uvicorn"):
        result = asyncio.run(amgroup_fallback.check_once())

    amgroup_fallback.create_lead_for_order = None
    assert result["amo_answered"] is False
    assert called == []  # ни одной попытки создать сделку
    assert amgroup_fallback._logged == set()
    assert amgroup_fallback._known_with_deal == set()
    assert any("не ответил" in rec.message and "amoCRM" in rec.message for rec in caplog.records)


def test_podtverzhdennaya_sdelka_ne_pereproveryaetsya_na_sleduyushem_prohode(monkeypatch):
    """Заказ, для которого сделка уже НАЙДЕНА, на следующем проходе не должен
    снова спрашивать amoCRM (память «сделка уже есть» - иначе каждый заказ
    окна опрашивается пожизненно и это лишняя нагрузка, способная выбить
    предохранитель, см. докстринг модуля)."""
    order = _ms_order("uuid-11", "07211", channel="Магазин")
    find_calls = {"n": 0}

    def amo_leads(query):
        find_calls["n"] += 1
        return [_amo_lead(321, "uuid-11")]

    result1 = _run(monkeypatch, [[order]], amo_leads_by_query=amo_leads)
    assert result1["with_deal"] == 1
    assert find_calls["n"] == 1  # искали по UUID один раз

    result2 = _run(monkeypatch, [[order]], amo_leads_by_query=amo_leads)
    assert result2["with_deal"] == 1
    assert find_calls["n"] == 1  # второй раз amo вообще не спрашивали


def test_pustoy_sklad_eto_chestnyy_nol(monkeypatch):
    """Склад ответил и правда вернул пустой список - это НЕ ошибка, а
    легитимный «заказов за окно нет»."""
    result = _run(monkeypatch, [[]])
    assert result == {"ms_answered": True, "amo_answered": True, "total": 0, "excluded": 0, "too_young": 0, "with_deal": 0, "missing": 0}


# ── порог возраста: живой amgroup успевает за секунды, дублёр не лезет вперёд ──

def _created_min_ago(minutes):
    msk = datetime.timezone(datetime.timedelta(hours=3))
    t = datetime.datetime.now(msk) - datetime.timedelta(minutes=minutes)
    return t.strftime("%Y-%m-%d %H:%M:%S.000")


def test_zakaz_molozhe_poroga_ne_schitaetsya_bez_sdelki(monkeypatch):
    """Заказ создан 2 минуты назад, сделки нет - это ещё не «без сделки», amgroup
    просто не успел (у живого 4-6 секунд). В amo за ним не ходим, в missing
    не кладём, посмотрим на следующем проходе."""
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_MIN_AGE_MIN", 10, raising=False)
    asked = []

    def by_query(query):
        asked.append(query)
        return []

    order = _ms_order("uuid-y1", "07301", channel="Магазин")
    order["created"] = _created_min_ago(2)
    result = _run(monkeypatch, [[order]], amo_leads_by_query=by_query)

    assert result["missing"] == 0
    assert result["too_young"] == 1
    assert asked == []


def test_zakaz_starshe_poroga_bez_sdelki_popadaet_v_missing(monkeypatch):
    """Заказу 30 минут, сделки нет - amgroup за полчаса не пришёл, это уже сбой."""
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_MIN_AGE_MIN", 10, raising=False)
    order = _ms_order("uuid-y2", "07302", channel="Магазин")
    order["created"] = _created_min_ago(30)
    result = _run(monkeypatch, [[order]])

    assert result["missing"] == 1
    assert result["too_young"] == 0


def test_zakaz_bez_daty_sozdaniya_porog_ne_derzhit(monkeypatch):
    """Поле created пустое или кривое - порог не применяем: лучше лишняя
    проверка в amo, чем заказ, который порог прячет вечно."""
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_MIN_AGE_MIN", 10, raising=False)
    order = _ms_order("uuid-y3", "07303", channel="Магазин")
    order["created"] = "вчера"
    result = _run(monkeypatch, [[order]])

    assert result["missing"] == 1

