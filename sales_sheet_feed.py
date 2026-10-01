"""Строка продажи в рабочую таблицу отдела: сбор, журнал и отправка за флагом.

Постановка Кати 01.10.2026: когда робот уводит сделку в «Успешно реализовано», продажа должна
лечь в рабочую таблицу отдела - обычная в лист «Продажи», наложенный платёж в лист «Наложка».
Методы панели в бою с 01.10.2026, инструкция - `features/zapis-v-tablicu-prodazh/API.md`:

    POST /api/ingest/sales-sheet/row       - лист «Продажи», оплата уже прошла
    POST /api/ingest/sales-sheet/cod-row   - лист «Наложка», оплата при получении

⚠️ **ОТПРАВКА ПО УМОЛЧАНИЮ ВЫКЛЮЧЕНА.** Без `SALES_SHEET_FEED_ENABLED=1` модуль только собирает
строку и пишет её в журнал - ровно та заглушка, которую просила Катя первым шагом. Так выкатка
ничего не записывает в лист, а в журнале за день видно настоящие строки с живых сделок.

⚠️ **Включать отправку НЕЛЬЗЯ, пока панель не примет числовой id в поле `manager`.** Лист хочет
короткий алиас («Кирилл П») и проверяет его по справочнику строго; соответствие «id в amoCRM →
алиас» знает только панель, полем `User.roster_alias`. Решение Кати 01.10.2026 - менять контракт
панели, чтобы `manager` принимал id и сам находил алиас. До этой правки включённый флаг даст 400.

⚠️ **Лист кормит цифру выручки, которую смотрит руководство.** Лишняя строка двигает её. Поэтому
повтор здесь безопасен по построению: защита от дубля живёт в самой панели и смотрит на лист -
у «Продаж» по дате, сумме и последним цифрам телефона, у «Наложки» по треку. Ответ `duplicate` -
это УСПЕХ, а не ошибка.

Раскладка листов, справочники и границы проверок - `knowledge/google-tablica-prodazh-kuda-panel-mozhet-pisat.md`.
"""
from __future__ import annotations

import datetime
import json
import logging
import os
from typing import Any

import httpx

import amo_service
from waybill_config import (
    FIELD_APPLICATION_TYPE,
    FIELD_CDEK_ORDER_NUMBER,
    FIELD_DELIVERY_TYPE,
    FIELD_INVOICE_OTHER_AMOUNT,
    FIELD_PAYMENT_METHOD,
    FIELD_PHONE,
    TEAM_PANEL_BASE_URL,
    TEAM_PANEL_INGEST_TOKEN,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))

SHEET_SALES = "Продажи"
SHEET_COD = "Наложка"

PATH_SALES = "/api/ingest/sales-sheet/row"
PATH_COD = "/api/ingest/sales-sheet/cod-row"

# ⚠️ Флаг и таймаут живут ЗДЕСЬ, а не в общем `waybill_config`, намеренно: общий файл правят
# несколько сессий разом, и ради двух значений рисковать чужой работой не стоит. Тот же довод
# стоит у соседней константы `PAY_RECHECK_PAUSE_S` в `autopilot.py`.
ENABLED = os.getenv("SALES_SHEET_FEED_ENABLED", "").strip() == "1"
# Запись в Google идёт 3-6 секунд, в инструкции панели сказано «таймаут не меньше 30, лучше 60».
TIMEOUT_S = float(os.getenv("SALES_SHEET_FEED_TIMEOUT_S", "60"))

# Значение-пометка для поля, которого мы не знаем. Осталось ради честности журнала: если
# когда-нибудь появится поле без источника, в логе это будет видно глазом, а не угадано.
UNKNOWN = "НЕ ЗНАЕМ"

# Касса для обычной продажи. Решение Кати 01.10.2026 дословно: «всегда счет Озон ИП Перфилов».
# Значение из справочника «Списки» A; регистр панель не проверяет, канонический вид вернёт сама.
CASH_REGISTER_DEFAULT = "счет Озон ИП Перфилов"

# Воронка и этап строкой - колонка «статус амо» листа «Наложка». Решение Кати: от авто-режима это
# всегда успешная реализация розницы. Форма «Воронка / Этап» взята у соседей по колонке, там
# «Офис / Успешно реализовано». Имя воронки 10593102 - «ОП розница» (`knowledge/glossary.md`).
AMO_STATUS_DEFAULT = "ОП розница / Успешно реализовано"

# Вид доставки: лист принимает ровно пять значений («Списки» D), а в сделке лежит название
# услуги перевозчика. ⚠️ Порядок проверок здесь ЗНАЧИМ: «CDEK: Самовывоз» содержит и «сдэк»,
# и «самовывоз», и это пункт выдачи СДЭКа, а не наш офис. Поэтому сдэк проверяется первым.
# Та же ловушка описана в `knowledge/imena-dostavki-gde-zashity.md`.
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
    что «не знаем». Непонятное название не угадываем: отдаём `UNKNOWN`, и панель ответит 400 с
    перечислением допустимых значений - лучше внятный отказ, чем тихо не та доставка в листе.
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
    """В какой лист поедет строка. У наложки свой лист со своими колонками и своим методом."""
    return SHEET_COD if is_cod(payment_method) else SHEET_SALES


def client_cell(contact: dict | None) -> str:
    """Колонка «Контрагент»: «Имя, +79162200866» - так лежит у соседей по листу.

    ⚠️ Это персональные данные. В журнал пишем, потому что журнал робота и так полон имён и
    ссылок на сделки, и строка без клиента не проверяема.
    """
    if not contact:
        return UNKNOWN
    name = str(contact.get("name") or "").strip()
    phone = str(amo_service.get_custom_field_value(contact, FIELD_PHONE) or "").strip()
    if name and phone:
        return f"{name}, {phone}"
    return name or phone or UNKNOWN


def amount_from_lead(lead: dict) -> Any:
    """Сумма: «Другая сумма», если заполнена, иначе бюджет сделки (решение Кати 01.10.2026).

    ⚠️ Порядок именно такой, и он не произвольный. «Другая сумма» (578141) заведена Катей как
    ПЕРЕБИВАЮЩАЯ: когда она заполнена, счёт СБП выставляется на неё вместо суммы заказа. Значит
    и денег со сделки получено столько же, а бюджет в этом случае врёт.

    Сумму отдаём как есть - панель принимает и число, и строку с пробелами или запятой, и сама
    приводит к числу. Своё приведение было бы второй реализацией того же правила.
    """
    other = amo_service.get_custom_field_value(lead, FIELD_INVOICE_OTHER_AMOUNT)
    if str(other or "").strip():
        return other
    price = lead.get("price")
    if price in (None, "", 0):
        return UNKNOWN
    return price


def track_from_lead(lead: dict) -> Any:
    """Трек СДЭК. Пустой допустим - накладной может ещё не быть, панель это разрешает."""
    raw = str(amo_service.get_custom_field_value(lead, FIELD_CDEK_ORDER_NUMBER) or "").strip()
    if not raw:
        return ""
    digits = "".join(c for c in raw if c.isdigit())
    return digits or raw


def is_preorder(lead: dict) -> bool:
    """Предзаказ - колонка XERO листа «Наложка», одно слово «предзаказ».

    ⚠️ Робот ведёт только сделки с типом заявки «Заказ», то есть сюда предзаказ по идее не
    доходит вовсе. Флаг собираем ради проверки этой самой идеи: окажется он хоть раз поднят -
    значит предзаказ попадает в маршрут, и это отдельная находка.
    """
    value = str(amo_service.get_custom_field_value(lead, FIELD_APPLICATION_TYPE) or "").strip()
    return value.casefold() == "предзаказ"


def collect(lead: dict, contact: dict | None) -> tuple[str, dict[str, Any]]:
    """Собрать тело запроса. Возвращает (имя листа, тело).

    Имена полей - ровно те, что принимают методы панели, чтобы тело не переделывать.
    """
    method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
    sheet = pick_sheet(method)

    body: dict[str, Any] = {
        # ⚠️ Числовой id ответственного, а НЕ имя. Решение Кати 01.10.2026: алиас по id найдёт
        # сама панель - у неё для этого есть поле `User.roster_alias`, у нас его нет.
        "manager": lead.get("responsible_user_id"),
        "date": datetime.datetime.now(_MSK).strftime("%d.%m.%Y"),
        "client": client_cell(contact),
        "delivery": delivery_for_sheet(
            amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE)),
        "payment": CASH_REGISTER_DEFAULT,
        "amount": amount_from_lead(lead),
        "paid": "оплачено",
        "lead_id": int(lead.get("id") or 0),
    }
    if sheet == SHEET_COD:
        body.update({
            # Лист про наложку по определению: в 230 его строках из 230 касса «наложка», а
            # оплата «не оплачено». Панель поставила бы эти умолчания и сама, но присылаем явно -
            # так в журнале видно, что именно уехало, без чтения чужого кода.
            "payment": "наложка",
            "paid": "не оплачено",
            "track": track_from_lead(lead),
            # Статуса доставки в этот момент не знаем: накладная только создаётся. Решение Кати -
            # считать «Не вручен», и это честно: груз действительно ещё не вручён.
            "status": "Не вручен",
            "amo_status": AMO_STATUS_DEFAULT,
            "preorder": is_preorder(lead),
        })
    return sheet, body


def unresolved(body: dict[str, Any]) -> list[str]:
    """Поля, которых мы не знаем. Пусто - строку можно отправлять."""
    return [k for k, v in body.items() if v == UNKNOWN]


async def send(sheet: str, body: dict[str, Any]) -> dict[str, Any] | None:
    """Отправить строку в панель. `None` - отправить не удалось или не настроено.

    Ошибки разбираем по смыслу, потому что реакция на них РАЗНАЯ: 400 повторять бессмысленно,
    502 повторить можно, 503 это не наша поломка. Сами повторы не делаем - следующий перевод
    сделки в успех придёт своим ходом, а дубля панель не допустит.
    """
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        logger.error("sales_sheet: адрес панели или токен не заданы, строку не отправил")
        return None
    url = TEAM_PANEL_BASE_URL.rstrip("/") + (PATH_COD if sheet == SHEET_COD else PATH_SALES)
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S) as client:
            resp = await client.post(
                url, json=body, headers={"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN})
    except Exception:  # noqa: BLE001 - сеть легла; сделка уже переведена, ронять нечего
        logger.exception("sales_sheet: не смог отправить строку по сделке %s", body.get("lead_id"))
        return None

    if resp.status_code == 400:
        try:
            errors = resp.json()["detail"]["errors"]
        except Exception:  # noqa: BLE001
            errors = resp.text[:400]
        logger.error("sales_sheet: панель отклонила строку по сделке %s, поля: %s",
                     body.get("lead_id"), json.dumps(errors, ensure_ascii=False, default=str))
        return None
    if resp.status_code == 503:
        logger.error("sales_sheet: запись в таблицу выключена в панели (503), сделка %s",
                     body.get("lead_id"))
        return None
    if resp.status_code >= 400:
        logger.error("sales_sheet: панель ответила %s по сделке %s, тело %s",
                     resp.status_code, body.get("lead_id"), resp.text[:300])
        return None

    data = resp.json()
    status = str(data.get("status") or "")
    # `duplicate` - успех: такая строка уже есть, второй раз её писать и не надо.
    logger.info("sales_sheet: лист «%s», сделка %s, итог %s, строка %s, сверено %s",
                data.get("sheet") or sheet, body.get("lead_id"), status or "?",
                data.get("row"), data.get("verified"))
    if data.get("verified") is False:
        logger.warning("sales_sheet: строка записана, но сверка не сошлась: %s",
                       json.dumps(data.get("warnings") or [], ensure_ascii=False, default=str))
    return data


async def report(lead: dict, contact: dict | None) -> tuple[str, dict[str, Any]]:
    """Собрать строку, написать в журнал и - если флаг включён - отправить.

    В журнал пишем ОДНОЙ строкой в формате JSON: такую строку легко выбрать из логов контейнера
    за день и сложить в таблицу, чтобы разговор про поля шёл на числах, а не на предположениях.
    """
    sheet, body = collect(lead, contact)
    gaps = unresolved(body)
    logger.info(
        "sales_sheet (%s): лист «%s», метод %s, не знаем: %s, тело %s",
        "отправляю" if ENABLED else "ЗАГЛУШКА, не отправляю",
        sheet, PATH_COD if sheet == SHEET_COD else PATH_SALES,
        ", ".join(gaps) or "ничего",
        json.dumps(body, ensure_ascii=False, default=str),
    )
    if not ENABLED:
        return sheet, body
    if gaps:
        logger.error("sales_sheet: не отправляю, в строке неизвестные поля: %s", ", ".join(gaps))
        return sheet, body
    await send(sheet, body)
    return sheet, body
