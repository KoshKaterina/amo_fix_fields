"""Тесты протеза amgroup (amgroup_fallback) - скелет: чтение МойСклада,
отсев Озона/TangemShop, поиск существующей сделки. Сборку и создание сделки
делает соседний срез, здесь не тестируем.

Написаны по инциденту 02-03.09.2026: сторонняя интеграция МойСклад -> amoCRM
встала, и по ложному алерту order_watchdog в тот же день - склад НЕ ОТВЕТИВШИЙ
нельзя читать как «заказов нет».

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
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", True, raising=False)
    amgroup_fallback._logged.clear()
    yield
    amgroup_fallback._logged.clear()


def _run(monkeypatch, ms_pages, amo_leads_by_query=None):
    """ms_pages: список страниц entity/customerorder (каждая - список rows,
    последняя короче limit=100 сигналит конец). amo_leads_by_query: функция
    query -> list[dict] для find_leads_by_query, по умолчанию всегда пусто."""
    calls = {"offset": 0}

    async def fake_get(path, params=None):
        offset = (params or {}).get("offset", 0)
        idx = offset // 100
        if idx >= len(ms_pages):
            return {"rows": []}
        return {"rows": ms_pages[idx]}

    async def fake_find(query, with_=()):
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
                         lambda query, with_=(): pytest.fail("amo не должен опрашиваться"))

    with caplog.at_level("WARNING", logger="uvicorn"):
        result = asyncio.run(amgroup_fallback.check_once())

    assert result == {"ms_answered": False, "total": 0, "excluded": 0, "with_deal": 0, "missing": 0}
    assert any("не ответил" in rec.message for rec in caplog.records)


def test_zakaz_ozona_po_kanalu_otseivaetsya(monkeypatch):
    """Канал продаж «Маркетплейс» - это Озон, в amo не ездит никогда."""
    order = _ms_order("uuid-1", "07200", channel="Маркетплейс")
    result = _run(monkeypatch, [[order]])

    assert result == {"ms_answered": True, "total": 1, "excluded": 1, "with_deal": 0, "missing": 0}


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

    assert result == {"ms_answered": True, "total": 1, "excluded": 0, "with_deal": 1, "missing": 0}


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
    (по умолчанию в тестах) точка расширения не зовётся."""
    called = []
    amgroup_fallback.create_lead_for_order = lambda order: called.append(order)

    order = _ms_order("uuid-6", "07205", channel="Магазин")
    result = _run(monkeypatch, [[order]])

    amgroup_fallback.create_lead_for_order = None
    assert result["missing"] == 1
    assert called == []


def test_missing_ne_povtoryaetsya_kazhdyy_prohod(monkeypatch):
    """Дедуп на диске: тот же заказ без сделки не должен звать создание
    сделки повторно на следующем проходе (пока список заказов не изменился)."""
    calls = []
    amgroup_fallback.create_lead_for_order = lambda order: calls.append(order)
    monkeypatch.setattr(amgroup_fallback, "AMGROUP_FALLBACK_DRY_RUN", False, raising=False)

    order = _ms_order("uuid-7", "07206", channel="Магазин")
    _run(monkeypatch, [[order]])
    _run(monkeypatch, [[order]])

    amgroup_fallback.create_lead_for_order = None
    assert len(calls) == 1


def test_pustoy_sklad_eto_chestnyy_nol(monkeypatch):
    """Склад ответил и правда вернул пустой список - это НЕ ошибка, а
    легитимный «заказов за окно нет»."""
    result = _run(monkeypatch, [[]])
    assert result == {"ms_answered": True, "total": 0, "excluded": 0, "with_deal": 0, "missing": 0}
