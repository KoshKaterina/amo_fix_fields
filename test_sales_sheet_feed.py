"""Заглушка строки продажи: сбор данных для листов «Продажи» и «Наложка».

Проверяем ровно то, ошибка в чём означает строку, которую лист отклонит или примет НЕ ТУ:

  • «CDEK: Самовывоз» это сдэк, а не самовывоз - в названии есть оба слова, и порядок
    проверок решает всё;
  • «СДЭК: Курьерская доставка» это тоже сдэк, а не курьер - по той же причине;
  • наложка определяется СТРОГО по «При получении»: «наличные» и «Эвотор» это шоурум,
    где наложки не бывает, и уехав на лист «Наложка» они испортили бы оба листа;
  • чего не знаем, то помечаем, а не угадываем: лист проверяет значения по справочнику
    и чужое отклоняет, поэтому выдумка дороже честного пробела.

Запуск без сети и без базы:  python -m pytest test_sales_sheet_feed.py -q
"""

import sys
import types

import pytest


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
    FIELD_ORDER_TOTAL,
    FIELD_PAYMENT_METHOD,
    FIELD_PHONE,
)


def _cf(*pairs):
    """⚠️ `field_id` обязан быть ЧИСЛОМ: `get_custom_field_value` сверяет его с числовой
    константой, и строковый ключ молча даёт пустое значение по всем полям."""
    return [{"field_id": int(fid), "values": [{"value": val}]} for fid, val in pairs]


def _lead(payment="Онлайн-оплата", delivery="СДЭК: Курьерская доставка", total="9 029",
          track="", app_type="Заказ", lead_id=777, responsible=9291546):
    return {
        "id": lead_id,
        "responsible_user_id": responsible,
        "custom_fields_values": _cf(
            (FIELD_PAYMENT_METHOD, payment),
            (FIELD_DELIVERY_TYPE, delivery),
            (FIELD_ORDER_TOTAL, total),
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
    assert F.delivery_for_sheet("Посылка склад-склад") == F.UNKNOWN  # имени перевозчика нет


def test_cdek_courier_is_cdek_not_courier():
    """«СДЭК: Курьерская доставка» - перевозчик, а не наша курьерка по Москве."""
    assert F.delivery_for_sheet("СДЭК: Курьерская доставка") == "сдэк"
    assert F.delivery_for_sheet("Курьер СДЭК") == "сдэк"


def test_our_own_courier_is_courier():
    assert F.delivery_for_sheet("Курьерская доставка") == "курьер"
    assert F.delivery_for_sheet("Доставка курьером по Москве") == "курьер"


def test_pickup_both_kinds():
    assert F.delivery_for_sheet("Самовывоз из офиса Sunscrypt") == "самовывоз"
    assert F.delivery_for_sheet("Самовывоз из шоурума") == "самовывоз"


def test_dostavista_and_empty_and_unknown():
    assert F.delivery_for_sheet("Достависта МСК") == "достависта"
    assert F.delivery_for_sheet("") == "отсутствует"
    assert F.delivery_for_sheet(None) == "отсутствует"
    assert F.delivery_for_sheet("Почта России") == F.UNKNOWN


def test_every_mapped_value_is_allowed_by_the_sheet():
    """Всё, что мы отдаём, обязано быть из пяти значений листа - иначе лист отклонит строку."""
    for said in ("CDEK: Самовывоз", "СДЭК: Курьерская доставка", "Курьерская доставка",
                 "Самовывоз из офиса Sunscrypt", "Достависта МСК", ""):
        assert F.delivery_for_sheet(said) in F.DELIVERY_VALUES


# ── в какой лист ────────────────────────────────────────────────────────────────

def test_cod_only_on_payment_on_delivery():
    """Наложка - строго «При получении». Широкое определение увело бы на этот лист шоурум."""
    assert F.pick_sheet("При получении") == F.SHEET_COD
    assert F.pick_sheet("при получении (наложенный платёж)") == F.SHEET_COD
    assert F.pick_sheet("Онлайн-оплата") == F.SHEET_SALES
    assert F.pick_sheet("Наличные") == F.SHEET_SALES
    assert F.pick_sheet("Эвотор") == F.SHEET_SALES
    assert F.pick_sheet("") == F.SHEET_SALES


# ── колонка клиента ─────────────────────────────────────────────────────────────

def test_client_cell_is_name_comma_phone():
    assert F.client_cell(_contact()) == "Илья, +79254488485"


def test_client_cell_survives_missing_parts():
    assert F.client_cell(_contact(phone="")) == "Илья"
    assert F.client_cell({"id": 1, "name": "", "custom_fields_values": []}) == F.UNKNOWN
    assert F.client_cell(None) == F.UNKNOWN


# ── сумма и трек ────────────────────────────────────────────────────────────────

def test_amount_reads_spaces_as_thousands():
    """В поле сделки сумма лежит строкой с пробелами, а лист требует ЧИСЛО больше нуля."""
    assert F.amount_from_lead(_lead(total="9 029")) == 9029.0
    assert F.amount_from_lead(_lead(total="16093")) == 16093.0
    assert F.amount_from_lead(_lead(total="")) == F.UNKNOWN


def test_track_goes_as_number():
    """У соседей по колонке трек лежит числом, значит и наш обязан быть числом."""
    assert F.track_from_lead(_lead(track="10262950411")) == 10262950411
    assert F.track_from_lead(_lead(track=" 10262950411 ")) == 10262950411
    assert F.track_from_lead(_lead(track="")) == F.UNKNOWN


# ── собранная строка ────────────────────────────────────────────────────────────

def test_cod_row_has_its_five_own_fields():
    sheet, body = F.collect(_lead(payment="При получении", delivery="CDEK: Самовывоз",
                                  total="16 093", track="10262950411"),
                            _contact("Марат", "+79643924444"), "Успешно реализовано")
    assert sheet == F.SHEET_COD
    assert body["client"] == "Марат, +79643924444"
    assert body["delivery"] == "сдэк"
    assert body["payment"] == "наложка"        # лист про наложку по определению
    assert body["paid"] == "не оплачено"       # и про неоплаченное тоже
    assert body["amount"] == 16093.0
    assert body["track"] == 10262950411
    assert body["status"] == "Не вручен"       # накладная только создаётся, вручения нет
    assert body["preorder"] is False
    assert body["amo_status"] == "Успешно реализовано"


def test_preorder_flag_is_raised_only_for_preorder():
    _, body = F.collect(_lead(payment="При получении", app_type="Предзаказ"), _contact(), "УР")
    assert body["preorder"] is True


def test_online_row_is_paid_and_has_no_cod_fields():
    sheet, body = F.collect(_lead(payment="Онлайн-оплата"), _contact(), "Успешно реализовано")
    assert sheet == F.SHEET_SALES
    assert body["paid"] == "оплачено"
    assert "track" not in body and "status" not in body and "preorder" not in body


def test_what_we_do_not_know_is_marked_not_guessed():
    """Два поля в сделке не лежат вовсе: короткий алиас менеджера и касса. Угадать их нельзя -
    лист проверяет оба по справочнику «Списки» строго и чужое отклоняет."""
    sheet, body = F.collect(_lead(payment="Онлайн-оплата"), _contact(), "УР")
    gaps = F.unresolved(sheet, body)
    assert "manager" in gaps and "payment" in gaps
    # id ответственного при этом отдаём - по нему алиас и разрешат на стороне панели.
    assert body["manager_amo_id"] == 9291546


def test_cod_knows_its_cash_register_so_it_is_not_a_gap():
    sheet, body = F.collect(_lead(payment="При получении", track="10262950411"), _contact(), "УР")
    assert "payment" not in F.unresolved(sheet, body)


# ── сама заглушка ───────────────────────────────────────────────────────────────

def test_report_logs_and_sends_nothing(caplog):
    """Заглушка обязана МОЛЧАТЬ в сеть и ГОВОРИТЬ в журнал: по этой строке и будет разговор
    о незаполненных полях."""
    with caplog.at_level("INFO", logger="uvicorn"):
        sheet, body = F.report(_lead(payment="При получении", track="10262950411"),
                               _contact("Марат", "+79643924444"), "Успешно реализовано")
    assert sheet == F.SHEET_COD
    said = "\n".join(r.getMessage() for r in caplog.records)
    assert "ЗАГЛУШКА" in said and "не отправляю" in said
    assert "Наложка" in said and "cod-row" in said
    assert "Марат" in said


def test_report_names_the_right_endpoint_for_ordinary_sale(caplog):
    with caplog.at_level("INFO", logger="uvicorn"):
        sheet, _ = F.report(_lead(payment="Онлайн-оплата"), _contact(), "Успешно реализовано")
    said = "\n".join(r.getMessage() for r in caplog.records)
    assert sheet == F.SHEET_SALES
    assert "sales-sheet/row" in said and "cod-row" not in said
