"""Тесты сторожа задвоения сделок amgroup (amgroup_duplicate_watch).

Написаны на случай 02.09.2026: amgroup встал, мы включили протез
(amgroup_fallback), и есть риск, что amgroup оживёт и заведёт сделку поверх
нашей. Проверяем, что сторож ловит пару, не шумит на одиночке, молчит при
сбое amoCRM (а не читает None как «дублей нет» — на этом сгорел
order_watchdog 03.09.2026) и не повторяется. Отдельно — что в модуле вообще
нет функций, которые что-то удаляют или двигают: сторож только смотрит.

Запуск: python3 -m pytest test_amgroup_duplicate_watch.py -q
"""

import asyncio
import inspect
import os
import sys
import types

import pytest

os.environ.setdefault("TOKEN", "test")
os.environ.setdefault("PUBLIC_BASE_URL", "https://example.invalid")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

_sent: list[str] = []


def _install_stubs():
    # aiogram (зависимость telegram_bot.py) в тестовом окружении не стоит —
    # подменяем модуль целиком, как это делает test_order_watchdog.py.
    if "telegram_bot" not in sys.modules:
        tg = types.ModuleType("telegram_bot")

        async def send_alert(text, parse_mode=None, chat_id=None, message_thread_id=None):
            _sent.append(text)
            return True

        tg.send_alert = send_alert
        sys.modules["telegram_bot"] = tg


_install_stubs()

import amgroup_duplicate_watch as watch  # noqa: E402


@pytest.fixture(autouse=True)
def _clean(monkeypatch, tmp_path):
    _sent.clear()
    watch._reported.clear()

    async def fake_send(text, parse_mode=None, chat_id=None, message_thread_id=None):
        _sent.append(text)
        return True

    monkeypatch.setattr(watch.telegram_bot, "send_alert", fake_send, raising=False)
    monkeypatch.setattr(watch, "_REPORTED_PATH", str(tmp_path / "reported.json"), raising=False)
    monkeypatch.setattr(watch, "_reported_loaded", False, raising=False)
    yield
    _sent.clear()
    watch._reported.clear()


def _cf(field_id, value):
    return {"field_id": field_id, "values": [{"value": value}]}


def _lead(lead_id, *, uuid=None, number=None, tagged=False, created_by=1,
          created_at=1_000_000, contact_id=None, name="Заказ"):
    cfv = []
    if uuid is not None:
        cfv.append(_cf(watch.FIELD_MOYSKLAD_ORDER_UUID, uuid))
    if number is not None:
        cfv.append(_cf(watch.FIELD_ORDER_NUMBER, number))
    embedded = {}
    if tagged:
        embedded["tags"] = [{"name": watch.DUP_TAG}]
    if contact_id is not None:
        embedded["contacts"] = [{"id": contact_id, "is_main": True}]
    return {
        "id": lead_id,
        "name": name,
        "created_by": created_by,
        "created_at": created_at,
        "custom_fields_values": cfv,
        "_embedded": embedded,
    }


def _run(leads, monkeypatch, *, fail=False, contacts=None, users=None):
    """leads — сделки, которые «отдаёт» первая воронка из watch._PIPELINES;
    остальные воронки на проходе всегда пустые (сеть за них не отвечает)."""
    first_pipeline = watch._PIPELINES[0]

    async def fake_leads(pipeline_id, since_ts, with_=(), page_limit=250):
        if fail:
            return None
        if pipeline_id == first_pipeline:
            return leads
        return []

    async def fake_contacts(contact_ids):
        contacts_map = contacts or {}
        return {cid: contacts_map[cid] for cid in contact_ids if cid in contacts_map}

    async def fake_user_name(uid):
        return (users or {}).get(int(uid))

    monkeypatch.setattr(watch.amo_service, "get_leads_updated_since", fake_leads)
    monkeypatch.setattr(watch.amo_service, "get_contacts_by_ids", fake_contacts)
    monkeypatch.setattr(watch.amo_service, "get_user_name", fake_user_name)
    return asyncio.run(watch.check_once())


def test_para_odinakovym_zakazom_lovitsya(monkeypatch):
    """Основной случай: два лида с одним и тем же «ID Заказа» — дубль."""
    leads = [
        _lead(111, uuid="ms-uuid-1", number="07182", tagged=True,
              created_by=1, contact_id=501, name="Заказ 07182"),
        _lead(222, uuid="ms-uuid-1", number="07182", tagged=False,
              created_by=2, contact_id=501, name="Заказ 07182"),
    ]
    contacts = {501: {"id": 501, "name": "Иван Иванов"}}
    users = {1: "Мария Петрова", 2: "amoBot"}

    result = _run(leads, monkeypatch, contacts=contacts, users=users)

    assert result == {"leads": 2, "groups": 1, "new": 1}
    assert len(_sent) == 1
    text = _sent[0]
    assert "07182" in text
    assert "Иван Иванов" in text
    assert "Мария Петрова" in text
    assert "amoBot" in text
    assert "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/111" in text
    assert "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/222" in text
    assert watch.DUP_TAG in text
    # длинное тире в переписке с людьми запрещено правилом папки
    assert "—" not in text
    assert "·" not in text


def test_odinochnaya_sdelka_ne_trevozhit(monkeypatch):
    leads = [_lead(111, uuid="ms-uuid-1", number="07182")]

    result = _run(leads, monkeypatch)

    assert result == {"leads": 1, "groups": 0, "new": 0}
    assert _sent == []


def test_zapasnoy_klyuch_svyazyvaet_paru(monkeypatch):
    """У одной сделки заполнен только UUID, у другой — только номер заказа,
    но это один и тот же заказ (номер совпадает с тем, что был бы у первой)."""
    leads = [
        _lead(111, uuid="ms-uuid-9", number=None),
        _lead(222, uuid=None, number="07999"),
        _lead(333, uuid="ms-uuid-9", number="07999"),  # мост между двумя выше
    ]

    result = _run(leads, monkeypatch)

    assert result["groups"] == 1
    assert len(_sent) == 1


def test_amocrm_ne_otvetil_molchim(monkeypatch):
    """None от amo_service — сбой выборки, а не «дублей нет». Молчим."""
    result = _run([_lead(111, uuid="x")], monkeypatch, fail=True)

    assert result is None
    assert _sent == []


def test_povtorniy_prohod_po_toy_zhe_pare_ne_pishet(monkeypatch):
    leads = [
        _lead(111, uuid="ms-uuid-1"),
        _lead(222, uuid="ms-uuid-1"),
    ]

    _run(leads, monkeypatch)
    result2 = _run(leads, monkeypatch)

    assert len(_sent) == 1
    assert result2["new"] == 0


def test_dedup_perezhivaet_restart(monkeypatch, tmp_path):
    leads = [
        _lead(111, uuid="ms-uuid-1"),
        _lead(222, uuid="ms-uuid-1"),
    ]

    _run(leads, monkeypatch)
    # «Рестарт»: память чистая, файл на месте (fixture не трогает _REPORTED_PATH второй раз).
    watch._reported.clear()
    monkeypatch.setattr(watch, "_reported_loaded", False, raising=False)
    _run(leads, monkeypatch)

    assert len(_sent) == 1


def test_tretya_sdelka_v_gruppe_eto_novaya_situaciya(monkeypatch):
    """Пара уже отчитана — появилась третья сделка по тому же заказу: набор
    id другой, это новая ситуация, молчать нельзя."""
    pair = [_lead(111, uuid="ms-uuid-1"), _lead(222, uuid="ms-uuid-1")]
    _run(pair, monkeypatch)
    assert len(_sent) == 1

    trio = pair + [_lead(333, uuid="ms-uuid-1")]
    _run(trio, monkeypatch)

    assert len(_sent) == 2


def test_nikakih_udaleniy_v_module():
    """Жёсткое требование: сторож не удаляет, не сливает и не двигает сделки.
    Проверяем и по факту (нет функций с такими именами), и по вызовам API
    (модуль не дёргает patch/delete методы amo_service)."""
    source = inspect.getsource(watch)
    forbidden_calls = ["amo_service.patch_lead", "amo_service.remove_", "delete", ".merge("]
    for token in forbidden_calls:
        assert token not in source, f"в модуле не должно быть {token!r}"

    names = [name for name, _ in inspect.getmembers(watch, inspect.isfunction)]
    for name in names:
        low = name.lower()
        assert "delete" not in low
        assert "merge" not in low
        assert "close" not in low
        assert not low.startswith("patch")
