"""Тесты сторожа заказов (order_watchdog).

Написаны по инциденту 07.08.2026: заказ №18287 оформили на сайте, а в МойСклад
он не попал. Сторож — третья линия защиты после вебхука и сверки; проверяем, что
он ловит потерю, не шумит на нормальных заказах и не повторяется.

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
from waybill_config import MS_ATTR_ORDER_NUMBER_ID  # noqa: E402

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


def _run(woo_orders, ms_rows, monkeypatch):
    async def fake_woo(since):
        return woo_orders

    async def fake_ms(path, params=None):
        # Отдаём одну страницу: тест не про пагинацию.
        if params and params.get("offset", 0) > 0:
            return {"rows": []}
        return {"rows": ms_rows}

    monkeypatch.setattr(order_watchdog.woo_client, "list_orders_created_since", fake_woo)
    monkeypatch.setattr(order_watchdog.ms_client, "get", fake_ms)
    return asyncio.run(order_watchdog.check_once())


def test_poteryanniy_zakaz_lovitsya(monkeypatch):
    """Тот самый случай: заказ на сайте есть, в МойСкладе его нет."""
    result = _run([_woo_order(18287)], [_ms_row("18286")], monkeypatch)

    assert result == {"woo": 1, "ms": 1, "lost": 1}
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
