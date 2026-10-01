"""Строка продажи для рабочей таблицы отдела: сбор, маршрутизация, отправка.

Проверяем ровно то, ошибка в чём означает строку, которую лист отклонит или примет НЕ ТУ:

  • «CDEK: Самовывоз» это сдэк, а не самовывоз - в названии есть оба слова, и порядок
    проверок решает всё;
  • наложка определяется СТРОГО по «При получении»: «наличные» и «Эвотор» это шоурум,
    где наложки не бывает, и уехав на лист «Наложка» они испортили бы оба листа;
  • «Другая сумма» ПЕРЕБИВАЕТ бюджет сделки - она для того и заведена, и когда заполнена,
    бюджет врёт;
  • выключенный флаг означает ПОЛНОЕ молчание в сеть: заглушка пишет только в журнал;
  • неизвестное поле отправку отменяет - лучше пустая строка в листе, чем неверная.

Запуск без сети и без базы:  python -m pytest test_sales_sheet_feed.py -q
"""

import asyncio
import sys
import types


def _stub(name, **attrs):
    m = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(m, k, v)
    sys.modules[name] = m
    return m


if "dotenv" not in sys.modules:
    _stub("dotenv", load_dotenv=lambda *a, **k: None)

import sales_sheet_feed as F  # noqa: E402
from waybill_config import (  # noqa: E402
    FIELD_APPLICATION_TYPE,
    FIELD_CDEK_ORDER_NUMBER,
    FIELD_DELIVERY_TYPE,
    FIELD_INVOICE_OTHER_AMOUNT,
    FIELD_PAYMENT_METHOD,
    FIELD_PHONE,
)


def _cf(*pairs):
    """⚠️ `field_id` обязан быть ЧИСЛОМ: `get_custom_field_value` сверяет его с числовой
    константой, и строковый ключ молча даёт пустое значение по всем полям."""
    return [{"field_id": int(fid), "values": [{"value": val}]} for fid, val in pairs]


def _lead(payment="Онлайн-оплата", delivery="СДЭК: Курьерская доставка", price=9029,
          other_amount="", track="", app_type="Заказ", lead_id=777, responsible=9291546):
    return {
        "id": lead_id,
        "price": price,
        "responsible_user_id": responsible,
        "custom_fields_values": _cf(
            (FIELD_PAYMENT_METHOD, payment),
            (FIELD_DELIVERY_TYPE, delivery),
            (FIELD_INVOICE_OTHER_AMOUNT, other_amount),
            (FIELD_CDEK_ORDER_NUMBER, track),
            (FIELD_APPLICATION_TYPE, app_type),
        ),
    }


def _contact(name="Илья", phone="+79254488485"):
    return {"id": 1, "name": name, "custom_fields_values": _cf((FIELD_PHONE, phone))}


# ── вид доставки: пять значений листа ───────────────────────────────────────────

def test_cdek_pickup_is_cdek_not_pickup():
    """⚠️ Главная ловушка. «CDEK: Самовывоз» это пункт выдачи СДЭКа, а не наш офис. В строке
    есть и «сдэк», и «самовывоз» - победить обязан сдэк, иначе доставка в листе соврёт."""
    assert F.delivery_for_sheet("CDEK: Самовывоз") == "сдэк"
    assert F.delivery_for_sheet("Самовывоз СДЭК") == "сдэк"


def test_cdek_courier_is_cdek_not_our_courier():
    assert F.delivery_for_sheet("СДЭК: Курьерская доставка") == "сдэк"
    assert F.delivery_for_sheet("Курьер СДЭК") == "сдэк"


def test_our_own_courier_is_courier():
    assert F.delivery_for_sheet("Курьерская доставка") == "курьер"
    assert F.delivery_for_sheet("Доставка курьером по Москве") == "курьер"


def test_pickup_both_kinds_and_dostavista():
    assert F.delivery_for_sheet("Самовывоз из офиса Sunscrypt") == "самовывоз"
    assert F.delivery_for_sheet("Самовывоз из шоурума") == "самовывоз"
    assert F.delivery_for_sheet("Достависта МСК") == "достависта"


def test_empty_delivery_is_absent_not_unknown():
    """«Отсутствует» - это значение листа «доставки нет вовсе», и оно НЕ равно «не знаем»."""
    assert F.delivery_for_sheet("") == "отсутствует"
    assert F.delivery_for_sheet(None) == "отсутствует"


def test_unknown_delivery_is_not_guessed():
    assert F.delivery_for_sheet("Почта России") == F.UNKNOWN


def test_every_mapped_value_is_allowed_by_the_sheet():
    for said in ("CDEK: Самовывоз", "СДЭК: Курьерская доставка", "Курьерская доставка",
                 "Самовывоз из офиса Sunscrypt", "Достависта МСК", ""):
        assert F.delivery_for_sheet(said) in F.DELIVERY_VALUES


# ── в какой лист и каким методом ────────────────────────────────────────────────

def test_cod_only_on_payment_on_delivery():
    """Наложка - строго «При получении». Широкое определение увело бы на этот лист шоурум."""
    assert F.pick_sheet("При получении") == F.SHEET_COD
    assert F.pick_sheet("при получении (наложенный платёж)") == F.SHEET_COD
    for other in ("Онлайн-оплата", "Наличные", "Эвотор", "Другой способ", ""):
        assert F.pick_sheet(other) == F.SHEET_SALES, other


# ── сумма: «Другая сумма» перебивает бюджет ─────────────────────────────────────

def test_amount_is_the_lead_budget_by_default():
    assert F.amount_from_lead(_lead(price=9029)) == 9029


def test_other_amount_overrides_the_budget():
    """⚠️ «Другая сумма» заведена Катей как перебивающая: счёт выставляется на неё вместо суммы
    заказа. Значит и получено столько же, а бюджет в этом случае врёт."""
    assert F.amount_from_lead(_lead(price=9029, other_amount="16093")) == "16093"
    assert F.amount_from_lead(_lead(price=9029, other_amount="47169,50")) == "47169,50"


def test_amount_is_marked_unknown_when_there_is_nothing():
    assert F.amount_from_lead(_lead(price=0)) == F.UNKNOWN
    assert F.amount_from_lead(_lead(price=None)) == F.UNKNOWN


# ── клиент и трек ───────────────────────────────────────────────────────────────

def test_client_cell_is_name_comma_phone():
    assert F.client_cell(_contact()) == "Илья, +79254488485"
    assert F.client_cell(_contact(phone="")) == "Илья"
    assert F.client_cell(None) == F.UNKNOWN


def test_track_keeps_only_digits_and_empty_is_allowed():
    """Пустой трек панель разрешает: накладной может ещё не быть."""
    assert F.track_from_lead(_lead(track=" 10262950411 ")) == "10262950411"
    assert F.track_from_lead(_lead(track="")) == ""


# ── собранная строка ────────────────────────────────────────────────────────────

def test_cod_row_has_its_own_five_fields():
    sheet, body = F.collect(
        _lead(payment="При получении", delivery="CDEK: Самовывоз", price=16093,
              track="10262950411"),
        _contact("Марат", "+79643924444"))
    assert sheet == F.SHEET_COD
    assert body["client"] == "Марат, +79643924444"
    assert body["delivery"] == "сдэк"
    assert body["payment"] == "наложка"
    assert body["paid"] == "не оплачено"
    assert body["amount"] == 16093
    assert body["track"] == "10262950411"
    assert body["status"] == "Не вручен"
    assert body["preorder"] is False
    assert body["amo_status"] == "ОП розница / Успешно реализовано"


def test_ordinary_sale_is_paid_and_goes_to_the_named_cash_register():
    """Касса обычной продажи - решение Кати: всегда «счет Озон ИП Перфилов»."""
    sheet, body = F.collect(_lead(payment="Онлайн-оплата"), _contact())
    assert sheet == F.SHEET_SALES
    assert body["paid"] == "оплачено"
    assert body["payment"] == "счет Озон ИП Перфилов"
    assert "track" not in body and "status" not in body and "preorder" not in body


def test_manager_goes_as_the_amo_id_not_as_a_name():
    """Решение Кати 01.10.2026: отдаём числовой id, алиас по нему находит сама панель."""
    _, body = F.collect(_lead(responsible=9291546), _contact())
    assert body["manager"] == 9291546


def test_preorder_flag_is_raised_only_for_preorder():
    _, body = F.collect(_lead(payment="При получении", app_type="Предзаказ"), _contact())
    assert body["preorder"] is True
    _, plain = F.collect(_lead(payment="При получении", app_type="Заказ"), _contact())
    assert plain["preorder"] is False


def test_nothing_is_unresolved_on_a_normal_deal():
    """Все поля, которых раньше не знали, теперь определены - значит строку можно отправлять."""
    _, body = F.collect(_lead(), _contact())
    assert F.unresolved(body) == []


def test_unresolved_names_the_broken_fields():
    _, body = F.collect(_lead(price=0, delivery="Почта России"), None)
    assert set(F.unresolved(body)) == {"client", "delivery", "amount"}


# ── заглушка и отправка ─────────────────────────────────────────────────────────

def test_disabled_flag_means_total_silence_to_the_network(monkeypatch, caplog):
    """Флаг выключен - в сеть не идём вовсе, только журнал. Это и есть заглушка."""
    monkeypatch.setattr(F, "ENABLED", False)
    sent: list = []
    monkeypatch.setattr(F, "send", lambda *a, **k: sent.append(a))
    with caplog.at_level("INFO", logger="uvicorn"):
        sheet, _ = asyncio.run(F.report(_lead(payment="При получении", track="10262950411"),
                                        _contact("Марат", "+79643924444")))
    assert sent == []
    assert sheet == F.SHEET_COD
    said = "\n".join(r.getMessage() for r in caplog.records)
    assert "ЗАГЛУШКА" in said and "не отправляю" in said
    assert "Наложка" in said and "cod-row" in said and "Марат" in said


def test_enabled_flag_sends_and_names_the_right_endpoint(monkeypatch):
    monkeypatch.setattr(F, "ENABLED", True)
    seen: list = []

    async def fake_send(sheet, body):
        seen.append((sheet, body))
        return {"status": "written", "row": 15288}

    monkeypatch.setattr(F, "send", fake_send)
    asyncio.run(F.report(_lead(payment="Онлайн-оплата"), _contact()))
    assert seen and seen[0][0] == F.SHEET_SALES


def test_unknown_field_cancels_the_send(monkeypatch, caplog):
    """⚠️ Лучше не записать строку, чем записать неверную: лист кормит цифру выручки."""
    monkeypatch.setattr(F, "ENABLED", True)
    sent: list = []

    async def fake_send(sheet, body):
        sent.append(body)
        return {}

    monkeypatch.setattr(F, "send", fake_send)
    with caplog.at_level("ERROR", logger="uvicorn"):
        asyncio.run(F.report(_lead(delivery="Почта России"), _contact()))
    assert sent == []
    assert "не отправляю" in "\n".join(r.getMessage() for r in caplog.records)
