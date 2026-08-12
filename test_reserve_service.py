"""Резерв товара в МойСклад — решающая матрица и защита от лишней записи.

Модуль пишет в живой склад: неверное решение либо запирает товар под мёртвой
сделкой, либо отпускает уже проданный. Поэтому проверяем именно РЕШЕНИЕ (что
именно уйдёт в МС на каждом этапе каждой воронки), а не только «функция не
упала». Сеть и amo — заглушки, PUT'ы копятся в списке.
"""

import asyncio
import datetime

import pytest

import reserve_service
import reserve_store
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_OFFICE,
    PIPELINE_TANGEMSHOP,
    RESERVE_TIMEOUT_DAYS,
    STATUS_CLEVER_NEW_LEAD,
    STATUS_CLEVER_PRECLOSED,
    STATUS_CLEVER_TERMS_AGREED,
    STATUS_CLOSED_LOST,
    STATUS_OFFICE_DEFERRED_RESERVE,
    STATUS_OFFICE_PREORDER_PAID,
    STATUS_OFFICE_SHIPPED,
    STATUS_PAYMENT_RECEIVED,
    STATUS_SUCCESS,
    STATUS_WAYBILL_READY,
)

ORDER = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
PIPELINE_UNTRACKED = 10659946  # «Лист ожидания» — вне зоны действия сервиса


def _lead(pipeline_id, status_id, order_uuid=ORDER):
    return {
        "id": 1,
        "pipeline_id": pipeline_id,
        "status_id": status_id,
        "custom_fields_values": (
            [{"field_id": FIELD_MOYSKLAD_ORDER_UUID, "values": [{"value": order_uuid}]}]
            if order_uuid
            else []
        ),
    }


@pytest.fixture
def env(monkeypatch, tmp_path):
    """Свежая база на каждый тест + перехват всех обращений к МС и amo."""
    monkeypatch.setattr(reserve_store, "DB_PATH", str(tmp_path / "reserve.sqlite3"))
    monkeypatch.setattr(reserve_service, "RESERVE_SERVICE_ENABLED", True)
    reserve_store.init()

    puts: list[tuple[str, dict]] = []
    state = {"lead": None, "positions": None}

    async def fake_get_lead_full(lead_id, with_=()):
        return state["lead"]

    async def fake_ms_get(path, params=None):
        return state["positions"]

    async def fake_ms_put(path, body):
        puts.append((path, body))
        return {"id": "ok"}

    monkeypatch.setattr(reserve_service.amo_service, "get_lead_full", fake_get_lead_full)
    monkeypatch.setattr(reserve_service.ms_client, "get", fake_ms_get)
    monkeypatch.setattr(reserve_service.ms_client, "put", fake_ms_put)

    state["positions"] = {
        "rows": [
            {"id": "pos-1", "quantity": 2, "reserve": 0},
            {"id": "pos-2", "quantity": 1, "reserve": 0},
            {"id": "pos-delivery"},  # доставка: поля reserve нет вообще
        ]
    }
    state["puts"] = puts
    return state


def _run(state, pipeline_id, status_id, order_uuid=ORDER):
    state["lead"] = _lead(pipeline_id, status_id, order_uuid)
    asyncio.run(reserve_service._apply(1))
    return state["puts"]


# --- постановка резерва -------------------------------------------------


def test_stavim_rezerv_na_rabochem_etape(env):
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED)
    assert [b["reserve"] for _, b in puts] == [2, 1], "резерв = количеству в позиции"
    assert reserve_store.get(1) is not None, "таймер тайм-аута должен быть заведён"


def test_dostavka_bez_polya_reserve_ne_trogaetsya(env):
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED)
    assert all("pos-delivery" not in path for path, _ in puts)


def test_oplata_poluchena_derzhit_rezerv_bez_taymera(env):
    """Оплачено — резерв бессрочный. Запись остаётся, но помечена exempt: таймер
    по ней не сработает, а сверка отгрузок её видит."""
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_PAYMENT_RECEIVED)
    assert [b["reserve"] for _, b in puts] == [2, 1]
    row = reserve_store.get(1)
    assert row is not None and row["timeout_exempt"] is True
    assert reserve_store.list_expired("9999-01-01") == [], "бессрочный в тайм-аут не попадает"


def test_ur_v_osnovnoy_rezerv_derzhim(env):
    """УР в ОП рознице — не конец жизни сделки, она едет в Офис."""
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_SUCCESS)
    assert [b["reserve"] for _, b in puts] == [2, 1]


# --- снятие резерва -----------------------------------------------------


def test_snimaem_na_zakryto_i_ne_realizovano(env):
    env["positions"]["rows"][0]["reserve"] = 2
    env["positions"]["rows"][1]["reserve"] = 1
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLOSED_LOST)
    assert [b["reserve"] for _, b in puts] == [0, 0]
    assert reserve_store.get(1) is None


def test_snimaem_na_predvaritelno_zakryt(env):
    env["positions"]["rows"][0]["reserve"] = 2
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_PRECLOSED)
    assert [b["reserve"] for _, b in puts] == [0]


def test_otgruzka_snimaet_rezerv(env):
    env["positions"]["rows"][0]["reserve"] = 2
    puts = _run(env, PIPELINE_OFFICE, STATUS_OFFICE_SHIPPED)
    assert [b["reserve"] for _, b in puts] == [0]


def test_ur_v_ofise_zakryvaet_rezerv(env):
    """142 в Офисе — реально конец пути, в отличие от 142 в ОП рознице."""
    env["positions"]["rows"][0]["reserve"] = 2
    puts = _run(env, PIPELINE_OFFICE, STATUS_SUCCESS)
    assert [b["reserve"] for _, b in puts] == [0]


def test_gotova_nakladnaya_snimaet_rezerv(env):
    env["positions"]["rows"][0]["reserve"] = 2
    puts = _run(env, PIPELINE_OFFICE, STATUS_WAYBILL_READY)
    assert [b["reserve"] for _, b in puts] == [0]


# --- когда НЕ трогаем вообще -------------------------------------------


def test_bez_uuid_zakaza_ms_nichego_ne_delaem(env):
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED, order_uuid="")
    assert puts == []


def test_chuzhaya_voronka_ne_trogaetsya(env):
    puts = _run(env, PIPELINE_UNTRACKED, STATUS_SUCCESS)
    assert puts == []


def test_etap_vne_shemy_ne_trogaetsya(env):
    """«Первичка» в Офисе в схеме не описана — сервис проходит мимо, резерв
    остаётся тем, что поставил кто-то до него."""
    env["positions"]["rows"][0]["reserve"] = 2
    puts = _run(env, PIPELINE_OFFICE, 75428410)
    assert puts == []


# --- бессрочный резерв в Офисе (решение 04.08, в коде Тианы не было) ---------


def test_predzakaz_oplachen_derzhit_rezerv_bessrochno(env):
    puts = _run(env, PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID)
    assert [b["reserve"] for _, b in puts] == [2, 1]
    row = reserve_store.get(1)
    assert row is not None and row["timeout_exempt"] is True


def test_otlozhennyy_tovar_derzhit_rezerv_bessrochno(env):
    puts = _run(env, PIPELINE_OFFICE, STATUS_OFFICE_DEFERRED_RESERVE)
    assert [b["reserve"] for _, b in puts] == [2, 1]
    assert reserve_store.get(1)["timeout_exempt"] is True


def test_bessrochnyy_rezerv_taymautom_ne_snimaetsya(env):
    """Даже спустя месяц: «Предзаказ оплачен» тайм-аут не трогает."""
    _run(env, PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID)
    _make_stale()
    env["puts"].clear()
    asyncio.run(reserve_service._timeout_once())
    assert env["puts"] == []
    assert reserve_store.get(1) is not None


# --- снятие по факту отгрузки, независимо от статуса ------------------------


def test_rezerv_ne_stavim_na_uzhe_otgruzhennoe(env):
    """Позиция отгружена полностью — держать нечего, резерв в неё не ставим."""
    env["positions"]["rows"][0]["shipped"] = 2
    env["positions"]["rows"][1]["shipped"] = 1
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED)
    assert puts == [], "цели ноль, а резерв и так ноль — API дёргать незачем"
    assert reserve_store.get(1) is None, "держать нечего — запись не заводим"


def test_chastichnaya_otgruzka_umenshaet_rezerv(env):
    """Отгрузили 1 из 2 — в резерве должна остаться единица."""
    env["positions"]["rows"][0]["reserve"] = 2
    env["positions"]["rows"][0]["shipped"] = 1
    env["positions"]["rows"][1]["shipped"] = 1
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED)
    assert [b["reserve"] for _, b in puts] == [1]


def test_otgruzka_snimaet_rezerv_bez_smeny_statusa(env):
    """Главное новое правило: отгрузка статус сделки не меняет и вебхука не
    даёт. Фоновая сверка обязана снять резерв сама, даже с бессрочного."""
    _run(env, PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID)
    env["puts"].clear()
    # товар уехал со склада, статус сделки прежний
    env["positions"]["rows"][0]["reserve"] = 2
    env["positions"]["rows"][0]["shipped"] = 2
    env["positions"]["rows"][1]["reserve"] = 1
    env["positions"]["rows"][1]["shipped"] = 1
    asyncio.run(reserve_service._shipment_once())
    assert [b["reserve"] for _, b in env["puts"]] == [0, 0]
    assert reserve_store.get(1) is None, "держать больше нечего — запись убрана"


def test_sverka_otgruzok_ne_trogaet_neotgruzhennoe(env):
    """Ничего не отгружено — сверка проходит вхолостую, лишних PUT нет."""
    _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED)
    env["puts"].clear()
    env["positions"]["rows"][0]["reserve"] = 2
    env["positions"]["rows"][1]["reserve"] = 1
    asyncio.run(reserve_service._shipment_once())
    assert env["puts"] == []
    assert reserve_store.get(1) is not None


def test_ms_nedostupen_zapis_ne_teryaem(env):
    """МойСклад не ответил — состояние неизвестно, запись НЕ чистим, иначе
    потеряем заказ из-под наблюдения навсегда."""
    _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED)
    env["positions"] = None  # МС молчит
    asyncio.run(reserve_service._shipment_once())
    assert reserve_store.get(1) is not None


def test_uzhe_v_nuzhnom_sostoyanii_api_ne_dergaem(env):
    """Идемпотентность: резерв уже стоит — лишнего PUT в живой склад нет."""
    env["positions"]["rows"][0]["reserve"] = 2
    env["positions"]["rows"][1]["reserve"] = 1
    puts = _run(env, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_TERMS_AGREED)
    assert puts == []


def test_master_flag_vyklyuchaet_zapis(env, monkeypatch):
    monkeypatch.setattr(reserve_service, "RESERVE_SERVICE_ENABLED", False)
    reserve_service.maybe_apply_bg(1, PIPELINE_CLEVER_MAIN)
    assert env["puts"] == []


def test_tangemshop_rabotaet_po_svoim_etapam(env):
    puts = _run(env, PIPELINE_TANGEMSHOP, STATUS_CLOSED_LOST)
    assert puts == [] or all(b["reserve"] == 0 for _, b in puts)


# --- тайм-аут -----------------------------------------------------------


def _make_stale(lead_id=1):
    """Старит запись до возраста заведомо больше тайм-аута — чтобы гонять
    боевой _timeout_once() без подмены его логики."""
    old = (
        datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(days=RESERVE_TIMEOUT_DAYS + 1)
    ).isoformat()
    with reserve_store._connect() as conn:
        conn.execute(
            "UPDATE reserve_state SET reserved_at = ? WHERE lead_id = ?", (old, lead_id)
        )


def test_taymaut_snimaet_rezerv_po_prosrochennoy_sdelke(env):
    """Сделка простояла дольше тайм-аута и до оплаты не дошла — резерв снимаем."""
    reserve_store.mark_reserved(1, ORDER, PIPELINE_CLEVER_MAIN)
    _make_stale()
    env["positions"]["rows"][0]["reserve"] = 2
    env["lead"] = _lead(PIPELINE_CLEVER_MAIN, STATUS_CLEVER_NEW_LEAD)
    asyncio.run(reserve_service._timeout_once())
    assert [b["reserve"] for _, b in env["puts"]] == [0]
    assert reserve_store.get(1) is None


def test_svezhaya_sdelka_taymautom_ne_snimaetsya(env):
    """Не просрочена — фоновый цикл её не видит и в магазин не лезет."""
    reserve_store.mark_reserved(1, ORDER, PIPELINE_CLEVER_MAIN)
    env["positions"]["rows"][0]["reserve"] = 2
    env["lead"] = _lead(PIPELINE_CLEVER_MAIN, STATUS_CLEVER_NEW_LEAD)
    asyncio.run(reserve_service._timeout_once())
    assert env["puts"] == []
    assert reserve_store.get(1) is not None


def test_taymaut_ne_trogaet_oplachennuyu_sdelku(env):
    """Оплата пришла — просроченной записи быть не должно, а если хвост остался,
    резерв не снимаем, только чистим запись."""
    reserve_store.mark_reserved(1, ORDER, PIPELINE_CLEVER_MAIN)
    env["positions"]["rows"][0]["reserve"] = 2
    env["lead"] = _lead(PIPELINE_CLEVER_MAIN, STATUS_PAYMENT_RECEIVED)
    _make_stale()
    asyncio.run(reserve_service._timeout_once())
    assert env["puts"] == []
    assert reserve_store.get(1) is None


def test_povtornaya_postanovka_ne_perezapuskaet_taymer(env):
    reserve_store.mark_reserved(1, ORDER, PIPELINE_CLEVER_MAIN)
    first = reserve_store.get(1)["reserved_at"]
    reserve_store.mark_reserved(1, ORDER, PIPELINE_CLEVER_MAIN)
    assert reserve_store.get(1)["reserved_at"] == first
