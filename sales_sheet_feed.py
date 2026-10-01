"""Строка продажи для рабочей Google-таблицы: сбор данных. ПОКА ТОЛЬКО В ЖУРНАЛ.

Постановка Кати 01.10.2026: когда робот переводит сделку в «Успешно реализовано», он должен
записать продажу в рабочую таблицу отдела - обычную в лист «Продажи», наложенный платёж в лист
«Наложка». Методы панели для этого уже написаны (ветка `feat/sales-sheet-write`):

    POST /api/ingest/sales-sheet/row       - лист «Продажи», колонки A:G
    POST /api/ingest/sales-sheet/cod-row   - лист «Наложка», колонки A:L

⚠️ **Этот модуль НИЧЕГО НЕ ОТПРАВЛЯЕТ.** Прямая просьба Кати на этом шаге: собрать данные,
написать в журнал, что собрали и в какой лист ушло бы, - и остановиться. Отправку включаем
отдельным решением, когда поля сойдутся на живых сделках.

Зачем вообще заглушка, а не сразу отправка. Часть полей таблицы в сделке amoCRM НЕ лежит, и
какие именно - видно только на живом потоке. Заглушка за день работы даёт список «вот эти поля
мы знаем, а эти не знаем», и разговор про них идёт с цифрами, а не с предположениями. Лист
проверяет значения строго по справочнику «Списки» и чужое отклоняет - значит промах мы увидим
не в журнале, а отказом Google, и разбирать его будет уже дороже.

Раскладка листов, справочники и границы проверок разобраны живьём:
`knowledge/google-tablica-prodazh-kuda-panel-mozhet-pisat.md` рабочей папки.
"""
from __future__ import annotations

import datetime
import json
import logging
from typing import Any

import amo_service
from waybill_config import (
    FIELD_APPLICATION_TYPE,
    FIELD_CDEK_ORDER_NUMBER,
    FIELD_DELIVERY_TYPE,
    FIELD_ORDER_TOTAL,
    FIELD_PAYMENT_METHOD,
    FIELD_PHONE,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))

SHEET_SALES = "Продажи"
SHEET_COD = "Наложка"

# Значение, которым помечаем поле, которого в сделке нет. В журнале его видно глазом, а при
# включении отправки по нему же легко найти все места, где ещё нужно решение.
UNKNOWN = "НЕ ЗНАЕМ"

# Вид доставки: лист принимает ровно пять значений («Списки» D), а в сделке лежит название
# услуги перевозчика. ⚠️ Порядок проверок здесь ЗНАЧИМ: «CDEK: Самовывоз» содержит и «сдэк»,
# и «самовывоз», и это пункт выдачи СДЭКа, а не наш офис. Поэтому сдэк проверяется первым.
# Та же ловушка уже описана в `knowledge/imena-dostavki-gde-zashity.md`.
_DELIVERY_RULES = (
    (("cdek", "сдэк"), "сдэк"),
    (("достависта",), "достависта"),
    (("самовывоз",), "самовывоз"),
    (("курьер",), "курьер"),
)
DELIVERY_VALUES = ("сдэк", "курьер", "достависта", "самовывоз", "отсутствует")


def delivery_for_sheet(raw: Any) -> str:
    """Название услуги доставки из сделки -> одно из пяти значений листа.

    Пусто - «отсутствует»: так лист называет случай «доставки нет вовсе», и это НЕ то же самое,
    что «не знаем». Непонятное название не угадываем: отдаём `UNKNOWN`, и в журнале рядом видно
    исходную строку.
    """
    text = str(raw or "").strip().lower()
    if not text:
        return "отсутствует"
    for markers, value in _DELIVERY_RULES:
        if any(m in text for m in markers):
            return value
    return UNKNOWN


def is_cod(payment_method: Any) -> bool:
    """Наложка строго по «При получении» - то же узкое правило, что у развилки оплаты робота.

    Широкое определение (`waybill_config.is_cod_payment`) брать НЕЛЬЗЯ: в нём есть «наличные» и
    «Эвотор», а это шоурум, где наложки не бывает вовсе. Разошлись бы лист и развилка.
    """
    return "при получении" in str(payment_method or "").lower()


def pick_sheet(payment_method: Any) -> str:
    """В какой лист поехала бы строка. Наложка - свой лист со своими колонками."""
    return SHEET_COD if is_cod(payment_method) else SHEET_SALES


def client_cell(contact: dict | None) -> str:
    """Колонка «Контрагент»: «Имя, +79162200866» - так лежит у соседей по листу.

    ⚠️ Это персональные данные. В журнал пишем, потому что журнал робота и так полон имён и
    ссылок на сделки, и строка без клиента не проверяема. Наружу этот модуль ничего не отдаёт.
    """
    if not contact:
        return UNKNOWN
    name = str(contact.get("name") or "").strip()
    phone = str(amo_service.get_custom_field_value(contact, FIELD_PHONE) or "").strip()
    if name and phone:
        return f"{name}, {phone}"
    return name or phone or UNKNOWN


def amount_from_lead(lead: dict) -> Any:
    """Сумма: берём поле «Сумма заказа» сделки, а не бюджет.

    ⚠️ Открытый вопрос, и он назван вслух в журнале: Катя просила «деньги, полученные со
    сделки». У наложки это сумма заказа и есть - деньги возьмут при вручении. У онлайна
    полученное знает МойСклад (`payedSum`), и развилка оплаты робота его уже спрашивала. Пока
    отправки нет, разницу просто показываем.
    """
    raw = amo_service.get_custom_field_value(lead, FIELD_ORDER_TOTAL)
    if raw in (None, ""):
        return UNKNOWN
    try:
        return float(str(raw).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return str(raw)


def track_from_lead(lead: dict) -> Any:
    """Трек СДЭК. В лист уходит ЧИСЛОМ - так лежит у соседей по колонке."""
    raw = str(amo_service.get_custom_field_value(lead, FIELD_CDEK_ORDER_NUMBER) or "").strip()
    if not raw:
        return UNKNOWN
    digits = "".join(c for c in raw if c.isdigit())
    return int(digits) if digits else raw


def is_preorder(lead: dict) -> bool:
    """Предзаказ - колонка XERO листа «Наложка», одно слово «предзаказ».

    ⚠️ Робот ведёт только сделки с типом заявки «Заказ», то есть сюда предзаказ по идее не
    доходит вовсе. Флаг собираем ради проверки этой самой идеи: если он хоть раз окажется
    поднят, значит предзаказ попадает в маршрут, и это отдельная находка.
    """
    value = str(amo_service.get_custom_field_value(lead, FIELD_APPLICATION_TYPE) or "").strip()
    return value.casefold() == "предзаказ"


def collect(lead: dict, contact: dict | None, stage_name: str = "") -> tuple[str, dict[str, Any]]:
    """Собрать то, что ушло бы в панель. Возвращает (имя листа, тело запроса).

    Имена полей - ровно те, что принимают методы панели (`SalesRowIn` / `CodRowIn`), чтобы при
    включении отправки тело не переделывать.
    """
    method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
    sheet = pick_sheet(method)
    raw_delivery = amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE)

    body: dict[str, Any] = {
        # ⚠️ Лист принимает КОРОТКИЙ АЛИАС менеджера («Кирилл П») и проверяет его по справочнику
        # «Списки» B строго. В сделке лежит только числовой id ответственного, а соответствие
        # «id в amoCRM -> алиас» знает ПАНЕЛЬ (поле `User.roster_alias`). Отсюда самый крупный
        # открытый вопрос: кто переводит одно в другое. Пока отдаём id и честно говорим, что
        # алиаса не знаем.
        "manager": UNKNOWN,
        "manager_amo_id": lead.get("responsible_user_id"),
        "date": datetime.datetime.now(_MSK).strftime("%d.%m.%Y"),
        "client": client_cell(contact),
        "delivery": delivery_for_sheet(raw_delivery),
        "payment": UNKNOWN,   # «касса» - см. ниже, в сделке её нет
        "amount": amount_from_lead(lead),
        "paid": "не оплачено" if sheet == SHEET_COD else "оплачено",
        "lead_id": int(lead.get("id") or 0),
    }
    if sheet == SHEET_COD:
        body.update({
            "payment": "наложка",       # на этом листе так в 230 строках из 230
            "track": track_from_lead(lead),
            # Статуса доставки мы в этот момент не знаем: накладная только создаётся. Решение
            # Кати - считать «Не вручен», и это честно: груз действительно ещё не вручён.
            "status": "Не вручен",
            # ⚠️ Расхождение, которое надо решить ДО включения отправки. В листе у соседей по
            # колонке стоит «Офис / Успешно реализовано» - 180 строк из 230. Мы же в этот момент
            # переводим сделку в успех СВОЕЙ воронки (ОП розница), а в Офис её уводит уже
            # офисная автоматика, после нас. То есть наше значение честное, но не такое, как у
            # соседей, и лист на эту колонку проверки НЕ имеет - значит разнобой никто не
            # остановит. Пока пишем свой этап и говорим об этом вслух.
            "amo_status": str(stage_name or UNKNOWN),
            "preorder": is_preorder(lead),
        })
    return sheet, body


def unresolved(sheet: str, body: dict[str, Any]) -> list[str]:
    """Чего мы не знаем - списком, чтобы в журнале это было одной строкой, а не поиском глазами."""
    gaps = [k for k, v in body.items() if v == UNKNOWN]
    if sheet == SHEET_SALES and body.get("payment") == UNKNOWN:
        pass  # уже попало в gaps, отдельной записи не нужно
    return gaps


def report(lead: dict, contact: dict | None, stage_name: str = "") -> tuple[str, dict[str, Any]]:
    """ЗАГЛУШКА: собрать строку и написать в журнал, куда и что ушло бы. Ничего не отправляет.

    Пишем ОДНОЙ строкой в формате JSON: такую строку легко выбрать из логов контейнера за день
    и сложить в таблицу, чтобы разговор про незаполненные поля шёл на числах.
    """
    sheet, body = collect(lead, contact, stage_name)
    gaps = unresolved(sheet, body)
    logger.info(
        "sales_sheet (ЗАГЛУШКА, не отправляю): лист «%s», метод %s, не знаем: %s, тело %s",
        sheet,
        "/api/ingest/sales-sheet/cod-row" if sheet == SHEET_COD
        else "/api/ingest/sales-sheet/row",
        ", ".join(gaps) or "ничего",
        json.dumps(body, ensure_ascii=False, default=str),
    )
    return sheet, body
