"""Тесты сторожа заказов (order_watchdog).

Написаны по инциденту 07.08.2026: заказ №18287 оформили на сайте, а в МойСклад
он не попал. Сторож — третья линия защиты после вебхука и сверки; проверяем, что
он ловит потерю, не шумит на нормальных заказах и не повторяется.

Дополнены 13.09.2026 по ложной тревоге о заказе 19003: заказ в МС был, но мост
amgroup затёр атрибут «Номер заказа на сайте». Теперь сторож перед криком идёт
через сделку amo к самому заказу и возвращает затёртый номер вместо тревоги.

Запуск: python3 -m pytest test_order_watchdog.py -q
"""

import asyncio
import datetime
import os
import sys
import types

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_sent: list[dict] = []


def _install_stubs():
    tg = types.ModuleType("telegram_bot")

    async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
        _sent.append({"text": text, "chat_id": chat_id})
        return True

    tg.send_alert = send_alert
    sys.modules["telegram_bot"] = tg


_install_stubs()

import order_watchdog  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_SITE_ORDER_NUMBER,
    MS_ATTR_ORDER_NUMBER_ID,
)

UTC = datetime.timezone.utc


def _woo_order(order_id, age_min=60, total="15990", items=("Keystone 3 Pro",)):
    created = datetime.datetime.now(UTC) - datetime.timedelta(minutes=age_min)
    return {
        "id": order_id,
        "total": total,
        "status": "processing",
        "payment_method_title": "При получении",
        "date_created": created.astimezone().replace(tzinfo=None).isoformat(),
        "date_created_gmt": created.replace(tzinfo=None, microsecond=0).isoformat(),
        "line_items": [{"name": n} for n in items],
        "shipping_lines": [{"method_title": "Самовывоз из офиса Sunscrypt"}],
    }


def _ms_row(site_number):
    return {"attributes": [{"id": MS_ATTR_ORDER_NUMBER_ID, "value": site_number}]}


def _lead(lead_id, site_number=None, order_uuid=None):
    cfs = []
    if site_number is not None:
        cfs.append({"field_id": FIELD_SITE_ORDER_NUMBER,
                    "values": [{"value": str(site_number)}]})
    if order_uuid is not None:
        cfs.append({"field_id": FIELD_MOYSKLAD_ORDER_UUID,
                    "values": [{"value": order_uuid}]})
    return {"id": lead_id, "custom_fields_values": cfs}


def _ms_order(order_uuid, site_number="", name="07481"):
    return {
        "id": order_uuid,
        "name": name,
        "attributes": [{"id": MS_ATTR_ORDER_NUMBER_ID, "value": site_number}],
    }


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    _sent.clear()
    order_watchdog._reported.clear()

    # Подменяем отправку у САМОГО модуля: при общем прогоне telegram_bot в
    # sys.modules может быть уже подменён соседним тест-модулем, и тогда наш
    # стаб выше не увидит ни одного сообщения.
    async def fake_send(text, parse_mode=None, chat_id=None, message_thread_id=None):
        _sent.append({"text": text, "chat_id": chat_id})
        return True

    monkeypatch.setattr(order_watchdog.telegram_bot, "send_alert", fake_send,
                        raising=False)
    monkeypatch.setattr(order_watchdog, "_REPORTED_PATH",
                        str(tmp_path / "reported.json"), raising=False)
    monkeypatch.setattr(order_watchdog, "_reported_loaded", False, raising=False)
    yield
    _sent.clear()
    order_watchdog._reported.clear()


def _run(woo_orders, ms_rows, monkeypatch, leads=(), ms_orders=None, put_log=None):
    """Один проход сторожа на моках.

    ms_rows=None — «МойСклад не ответил» (листинг вернул None).
    leads=None — «amoCRM не ответила» (поиск сделок вернул None).
    ms_orders — {uuid: заказ МС} для точечного чтения через сделку.
    """
    ms_orders = ms_orders or {}
    if put_log is None:
        put_log = []

    async def fake_woo(since):
        return woo_orders

    async def fake_ms_get(path, params=None):
        if path.startswith("entity/customerorder/"):
            return ms_orders.get(path.rsplit("/", 1)[-1])
        if ms_rows is None:
            return None
        # Отдаём одну страницу: тест не про пагинацию.
        if params and params.get("offset", 0) > 0:
            return {"rows": []}
        return {"rows": ms_rows}

    async def fake_ms_put(path, body, retries=3):
        put_log.append((path, body))
        return {}

    async def fake_find(query, with_=(), limit=50):
        return None if leads is None else list(leads)

    monkeypatch.setattr(order_watchdog.woo_client, "list_orders_created_since", fake_woo)
    monkeypatch.setattr(order_watchdog.ms_client, "get", fake_ms_get)
    monkeypatch.setattr(order_watchdog.ms_client, "put", fake_ms_put)
    monkeypatch.setattr(order_watchdog.amo_service, "find_leads_by_query", fake_find)
    return asyncio.run(order_watchdog.check_once())


def test_poteryanniy_zakaz_lovitsya(monkeypatch):
    """Тот самый случай: заказ на сайте есть, в МойСкладе его нет, сделки тоже."""
    result = _run([_woo_order(18287)], [_ms_row("18286")], monkeypatch)

    assert result == {"woo": 1, "ms": 1, "lost": 1, "restored": 0}
    assert len(_sent) == 1
    assert "18287" in _sent[0]["text"]
    assert "не доехал" in _sent[0]["text"]
    assert "Keystone 3 Pro" in _sent[0]["text"]


def test_zakaz_doehal_molchim(monkeypatch):
    result = _run([_woo_order(18286)], [_ms_row("18286")], monkeypatch)

    assert result["lost"] == 0
    assert _sent == []


def test_svezhiy_zakaz_ne_trevozhit(monkeypatch):
    """Заказ пятиминутной давности ещё может ехать штатно."""
    result = _run([_woo_order(18290, age_min=5)], [], monkeypatch)

    assert result["lost"] == 0
    assert _sent == []


def test_o_zakaze_pishem_odin_raz(monkeypatch):
    """Сторож ходит раз в час — сообщение должно быть одно, а не каждый проход."""
    _run([_woo_order(18287)], [], monkeypatch)
    _run([_woo_order(18287)], [], monkeypatch)

    assert len(_sent) == 1


def test_dedup_perezhivaet_restart(monkeypatch):
    _run([_woo_order(18287)], [], monkeypatch)
    # «Рестарт»: память чистая, файл на месте.
    order_watchdog._reported.clear()
    monkeypatch.setattr(order_watchdog, "_reported_loaded", False, raising=False)
    _run([_woo_order(18287)], [], monkeypatch)

    assert len(_sent) == 1


def test_neskolko_poter_odnim_soobsheniem(monkeypatch):
    result = _run([_woo_order(1), _woo_order(2), _woo_order(3)], [], monkeypatch)

    assert result["lost"] == 3
    assert len(_sent) == 1
    assert "Заказы с сайта не доехали" in _sent[0]["text"]


def test_soobshenie_idyot_v_tehnicheskiy_chat(monkeypatch):
    from waybill_config import TG_ALLOWED_CHAT_ID

    _run([_woo_order(18287)], [], monkeypatch)
    assert _sent[0]["chat_id"] == TG_ALLOWED_CHAT_ID


def test_zakaz_bez_daty_ne_sudim(monkeypatch):
    """Кривая дата — не повод для тревоги, лучше пропустить."""
    bad = _woo_order(18287)
    bad["date_created_gmt"] = ""
    result = _run([bad], [], monkeypatch)

    assert result["lost"] == 0
    assert _sent == []


# --- восстановление затёртого номера (инцидент 13.09.2026, заказ 19003) ---


def test_zatertyy_nomer_vosstanavlivaetsya(monkeypatch):
    """Заказ в МС жив, но атрибут пуст: возвращаем номер и НЕ кричим «потерян»."""
    put_log = []
    result = _run(
        [_woo_order(19003)], [], monkeypatch,
        leads=[_lead(36554541, 19003, "u-1")],
        ms_orders={"u-1": _ms_order("u-1", site_number="")},
        put_log=put_log,
    )

    assert result == {"woo": 1, "ms": 0, "lost": 0, "restored": 1}
    assert len(put_log) == 1
    path, body = put_log[0]
    assert path.endswith("customerorder/u-1")
    assert body["attributes"][0]["value"] == "19003"
    assert len(_sent) == 1
    assert "возвращён" in _sent[0]["text"]
    assert "не доехал" not in _sent[0]["text"]
    # Не потерян — в дедуп не попадает: следующий раз снова проверим по-настоящему.
    assert "19003" not in order_watchdog._reported


def test_vosstanovlenniy_zatirayut_snova_soobshaem_snova(monkeypatch):
    """Повторное затирание — повторное сообщение: это сигнал, что затиратель ходит."""
    common = dict(leads=[_lead(1, 19003, "u-1")],
                  ms_orders={"u-1": _ms_order("u-1", site_number="")})
    _run([_woo_order(19003)], [], monkeypatch, **common)
    _run([_woo_order(19003)], [], monkeypatch, **common)

    assert len(_sent) == 2


def test_nomer_uzhe_na_meste_molchim(monkeypatch):
    """Окна листинга разошлись, а заказ в порядке: ни PUT, ни сообщений."""
    put_log = []
    result = _run(
        [_woo_order(19003)], [], monkeypatch,
        leads=[_lead(1, 19003, "u-1")],
        ms_orders={"u-1": _ms_order("u-1", site_number="19003")},
        put_log=put_log,
    )

    assert result["lost"] == 0 and result["restored"] == 0
    assert put_log == []
    assert _sent == []


def test_chuzhoy_nomer_ne_trogaem(monkeypatch):
    """В заказе стоит ДРУГОЙ номер — не перетираем, а тревожим по-старому."""
    put_log = []
    result = _run(
        [_woo_order(19003)], [], monkeypatch,
        leads=[_lead(1, 19003, "u-1")],
        ms_orders={"u-1": _ms_order("u-1", site_number="18000")},
        put_log=put_log,
    )

    assert result["lost"] == 1 and result["restored"] == 0
    assert put_log == []
    assert "не доехал" in _sent[0]["text"]


def test_sdelka_bez_uuid_eto_poterya(monkeypatch):
    """Сделка есть, но заказа МС из неё не выудить — честная потеря."""
    result = _run(
        [_woo_order(19003)], [], monkeypatch,
        leads=[_lead(1, 19003)],
    )

    assert result["lost"] == 1
    assert "не доехал" in _sent[0]["text"]


def test_sboy_amo_ne_sudim(monkeypatch):
    """amoCRM молчит — это не «сделки нет»: ни алерта, ни дедупа до след. прохода."""
    result = _run([_woo_order(19003)], [], monkeypatch, leads=None)

    assert result["lost"] == 0
    assert _sent == []
    assert "19003" not in order_watchdog._reported


def test_ms_ne_otvetil_prohod_propuskaetsya(monkeypatch):
    """МойСклад не ответил на листинг: судить некого (03.09.2026 такое молчание
    прочиталось как «пусто», и сторож объявил потерянными 22 живых заказа)."""
    result = _run([_woo_order(19003)], None, monkeypatch)

    assert result["lost"] == 0 and result["ms"] == -1
    assert _sent == []
    assert "19003" not in order_watchdog._reported
