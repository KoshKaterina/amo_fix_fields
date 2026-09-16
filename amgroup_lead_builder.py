# -*- coding: utf-8 -*-
"""Сборка и создание сделки-протеза amgroup (МойСклад -> amoCRM).

Точка расширения amgroup_fallback.create_lead_for_order - этот модуль её
реализует. amgroup_fallback находит заказ покупателя МойСклад без сделки и
зовёт create_lead_for_order(order); эта функция собирает набор полей, находит
или заводит контакт, создаёт сделку и проставляет ответственного (боты amo
на такую сделку не реагируют - см. ниже), возвращает id сделки или None.

Логика и карта полей перенесены из боевого прототипа backfill_leads.py
(Катя, 03.09.2026) - им в тот же день созданы десять боевых сделок, набор
полей сверен с живыми сделками amgroup. Формулы/ID полей ниже не придуманы
заново, а перенесены оттуда.

⚠️ Факт, проверенный 03.09.2026 на двенадцати сделках: боты amoCRM не
реагируют на сделки, созданные не через amgroup - распределение на дежурного
и шаблон сообщения клиенту НЕ запускаются. Поэтому assign_responsible ниже -
ОТДЕЛЬНЫЙ явный шаг после создания сделки, а не часть тела создания.

⚠️ Мина, на которой Катя подорвалась 03.09.2026: у одного клиента бывает два
заказа подряд, и без кэша созданных контактов в рамках одного запуска
получается дубль человека в amoCRM. _contact_cache ниже - защита от этого.

⚠️ Побочный эффект, который эта правка НЕ выключает (находка приёмки
безопасности 03.09.2026): сделка протеза создаётся сразу с заполненным полем
«Состав заказа» (FIELD["sostav"]), а webhooks.py на изменение этого поля
ставит резерв в МойСкладе - и воронка/этап, куда протез кладёт сделку
(PIPELINE_CLEVER_MAIN/STATUS_CLEVER_NEW_LEAD), входит в зону резервирования.
Значит включение протеза сделок ОДНОВРЕМЕННО включает простановку резервов по
заказам, которым на момент находки может быть уже до AMGROUP_FALLBACK_LOOKBACK_HOURS
часов и которые к этому моменту могли быть уже отгружены. Код здесь не
трогаем (webhooks.py - чужой файл), гасить резерв или нет - решает Катя.

Пишем в amoCRM только через существующие функции сервиса (amo_service.py,
api.py) - они уже идут через общую очередь с ограничением частоты и
circuit breaker, свой HTTP-клиент здесь не заводим. Читаем МойСклад только
через ms_client.get - и, как и в amgroup_fallback, None от него значит
«склад не ответил», а не «данных нет» (см. докстринг amgroup_fallback.py).

Константы полей/воронки ниже свести в общий конфиг на сшивке - часть уже
живёт в waybill_config.py (PIPELINE_CLEVER_MAIN, STATUS_CLEVER_NEW_LEAD,
FIELD_MOYSKLAD_ORDER_UUID, AMGROUP_FALLBACK_TAG) и импортируется оттуда,
остальное (карта полей сделки, ENUM выпадающих списков) - своё, локальное.
"""

import logging
import re
from collections.abc import Mapping
from typing import Any

import amo_service
import api
import lead_distribution
import ms_client
from api_helpers import sanitize_custom_field_value
from ms_preorder_type import parse_ms_preorder_type
from waybill_config import (
    AMGROUP_FALLBACK_TAG,
    AMGROUP_LEAD_RESPONSIBLE_USER_ID,
    AMGROUP_PREORDER_TYPE_ENABLED,
    FIELD_MOYSKLAD_ORDER_UUID,
    LEAD_DISTRIBUTION_ENABLED,
    MS_ATTR_PREORDER_SUMMARY_ID,
    PIPELINE_CLEVER_MAIN,
    RESPONSIBLE_OFFICE_MANAGER_USER_ID,
    STATUS_CLEVER_NEW_LEAD,
)

logger = logging.getLogger("uvicorn")


class _AmoSearchFailed(Exception):
    """amoCRM не ответил на поиск (сделки или контакта) - вызывающий код
    обязан прервать создание, а не читать сбой как «дубля нет» (см. докстринг
    модуля и amo_service.find_leads_by_query/find_contacts_by_query)."""


# «№ Заказа» - второе поле связки, дублирует amgroup_fallback.FIELD_MOYSKLAD_ORDER_NUMBER
# (там не экспортировано под этим именем - своя копия, свести на сшивке).
FIELD_MOYSKLAD_ORDER_NUMBER = 576697

# Лимит полнотекстового поиска amo при дедупе. Больше дефолтных 50/10:
# короткий номер заказа и цифры телефона цепляют много постороннего, и без
# запаса нужная сделка/контакт может не попасть в первую страницу выдачи
# (находка приёмки безопасности 03.09.2026).
_LEAD_SEARCH_LIMIT = 250
_CONTACT_SEARCH_LIMIT = 100

# Карта полей сделки (field_id) - точь-в-точь из прототипа, сверена на живых
# сделках amgroup 03.09.2026.
FIELD: dict[str, int] = {
    "pvz": 572209, "pay": 577373, "site": 577415, "ym": 578015,
    "comment": 576711, "addr": 576719, "channel": 576725, "store": 576723,
    "currency": 576729, "order_uuid": FIELD_MOYSKLAD_ORDER_UUID,
    "order_num": FIELD_MOYSKLAD_ORDER_NUMBER, "agent_uuid": 576695,
    "order_url": 576721, "sostav": 576703, "weight": 576705, "volume": 576707,
    "paystatus": 576669, "agent": 576671, "org": 576673, "created_by": 576683,
    "msinfo": 576717, "type": 577671, "basket": 577313, "delivery": 577315,
    "promo": 570661, "points": 576667, "basket_old": 570641,
}

# Значения выпадающих списков (enum_id) - тоже из прототипа.
ENUM: dict[str, dict[str, int]] = {
    "channel": {"Магазин": 1040221, "Маркетплейс": 1040217, "ОПТ": 1040219, "TangemShop": 1041663},
    "store": {
        "Sunscrypt Основной": 1040201, "Sunscrypt Шоурум": 1041885,
        "Sunscrypt Вскрытые": 1040207, "Sunscrypt контроль": 1041779,
        "Sunscrypt временный": 1041419, "ЭРМС_Основной": 1041653,
    },
    "currency": {"руб": 1040233, "доллар": 1040235},
    "paystatus": {"Не оплачен": 1040147, "Частично оплачен": 1040149, "Оплачен": 1040151},
    "created_by": {"Через виджет": 1040193, "Из МойСклад": 1040195},
    "type": {"Заказ": 1041237, "Предзаказ": 1041239, "Резерв": 1041903},
}

# «Инфо по МС» - служебное поле, amgroup всегда пишет туда курсы валют на
# момент создания; для протеза достаточно константы (сами мы курсы не считаем).
MSINFO = '{"currenciesRates":{"0e5aa71e-c413-11ee-0a80-13fd002f63fe":1,"593c92c3-dd4f-11ef-0a80-04160006e47a":89.268837}}'

_EXPAND = "positions.assortment,agent,organization,store,salesChannel"


def _rub(kop: float) -> str:
    """1399000 (В КОПЕЙКАХ - МойСклад отдаёт суммы entity/customerorder в
    копейках, не в рублях) -> '13 990.00 рублей' со склонением, как у amgroup.
    ⚠️ Код ниже (v = kop / 100) прав, деление не трогать - более ранняя версия
    этого докстринга утверждала обратное («сумма уже в рублях») и приглашала
    убрать деление, отчего все суммы протеза уехали бы в сто раз. Перенесено
    из прототипа, сама формула без изменений."""
    v = kop / 100
    n = int(v)
    last = n % 10
    last2 = n % 100
    if last == 1 and last2 != 11:
        word = "рубль"
    elif last in (2, 3, 4) and last2 not in (12, 13, 14):
        word = "рубля"
    else:
        word = "рублей"
    return f"{n:,}".replace(",", " ") + f".{int(round((v - n) * 100)):02d} {word}"


def _attr(order: dict, name: str) -> Any:
    """Значение доп. поля заказа МойСклад по человекочитаемому имени.
    Доп. поля приходят в ответе всегда, expand для них не нужен (в отличие
    от positions/agent/store — это ссылочные поля)."""
    attributes = order.get("attributes")
    if not isinstance(attributes, list):
        return None
    for a in attributes:
        if isinstance(a, Mapping) and a.get("name") == name:
            return a.get("value")
    return None


def _build_fields(order: dict) -> dict:
    """Собирает состав заказа, вес/объём и статус оплаты из позиций. Позиции
    заказа читаются из positions.rows - в прототипе получены через expand
    прямо на entity/customerorder, здесь так же (см. _fetch_full_order)."""
    pos = (order.get("positions") or {}).get("rows") or []
    goods: list[str] = []
    services: list[tuple[str, float, str]] = []
    weight = 0.0
    volume = 0.0
    for p in pos:
        a = p.get("assortment") or {}
        nm = a.get("name") or "?"
        qty = int(p.get("quantity") or 0)
        line = f"{nm}, {qty} шт, {_rub(p.get('price') or 0)}"
        if (a.get("meta") or {}).get("type") == "service":
            services.append((nm, p.get("price") or 0, line))
        else:
            goods.append(line)
            weight += (a.get("weight") or 0) * qty
            volume += (a.get("volume") or 0) * qty
    moment = (order.get("moment") or "")[:10]
    dt = ".".join(reversed(moment.split("-"))) if moment else ""
    lines = [f"{i}. {t}" for i, t in enumerate(goods + [s[2] for s in services], 1)]
    sostav = (
        f"Заказ № {order.get('name')} от {dt}:\n" + "\n".join(lines)
        + f"\nНДС: 0.00 рублей\nИтого: {_rub(order.get('sum') or 0)}"
    )
    paid = order.get("payedSum") or 0
    total = order.get("sum") or 0
    paystatus = "Оплачен" if paid >= total > 0 else ("Частично оплачен" if paid > 0 else "Не оплачен")
    return {
        "goods": goods, "services": services, "sostav": sostav,
        "weight": round(weight, 3), "volume": round(volume, 4), "paystatus": paystatus,
    }


def _normalize_phone_digits(phone: str | None) -> str:
    digits = re.sub(r"\D", "", phone or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return digits


async def _fetch_full_order(order_uuid: str) -> dict | None:
    """Полная карточка заказа с раскрытыми позициями, агентом, организацией,
    складом и каналом продаж. amgroup_fallback отдаёт заказ с раскрытыми
    только agent/salesChannel (свой набор под свою задачу) - для сборки
    сделки нужно больше, поэтому здесь отдельный запрос по id.

    None - склад не ответил (ms_client.get так и возвращает при обрыве или
    ошибке), это НЕ значит «заказа нет» - вызывающий код обязан прерваться,
    не создавать сделку и не считать это фактом об отсутствии заказа."""
    return await ms_client.get(f"entity/customerorder/{order_uuid}", params={"expand": _EXPAND})


# Кэш «цифры телефона -> id контакта» в рамках процесса. Без него у клиента с
# двумя заказами подряд второй заказ не находит контакт (amoCRM не успевает
# проиндексировать только что созданный) и заводит дубль человека.
_contact_cache: dict[str, int] = {}


async def _find_or_create_contact(
    phone: str | None, name: str | None, *, order_number: str = ""
) -> int | None:
    """Ищет контакт по последним 10 цифрам телефона среди значений поля
    PHONE (полнотекстовый поиск amo цепляет и посторонние совпадения, поэтому
    сверяем само значение поля - как в amgroup_fallback._find_existing_lead).
    Не нашёл - создаёт новый контакт через api.create_contact (общая очередь).
    Кэш процесса - защита от дубля при двух заказах одного клиента подряд.

    Может raise _AmoSearchFailed, если amoCRM не ответил на поиск - тогда
    нельзя молча переходить к созданию нового контакта: не увидели
    существующий из-за сбоя - не значит, что его нет (см. докстринг модуля).
    order_number - только для лога, персональные данные (телефон, имя) в лог
    не пишем."""
    digits = _normalize_phone_digits(phone)
    if not digits:
        return None
    if digits in _contact_cache:
        return _contact_cache[digits]

    contacts = await amo_service.find_contacts_by_query(digits, limit=_CONTACT_SEARCH_LIMIT)
    if contacts is None:
        raise _AmoSearchFailed("find_contacts_by_query")

    for contact in contacts:
        for f in contact.get("custom_fields_values") or []:
            if f.get("field_code") != "PHONE":
                continue
            for v in f.get("values") or []:
                if re.sub(r"\D", "", str(v.get("value") or ""))[-10:] == digits[-10:]:
                    cid = contact.get("id")
                    if cid:
                        _contact_cache[digits] = cid
                        return cid

    contact_id = await api.create_contact(name or digits, "+" + digits, None)
    if contact_id:
        _contact_cache[digits] = contact_id
    else:
        logger.error("amgroup_lead_builder: не создался контакт по заказу %s", order_number)
    return contact_id


async def _find_existing_lead(
    order_uuid: str, order_number: str, lead_name: str, name_query: str
) -> dict | None:
    """Защита от дубля перед созданием. Основной ключ - поле «ID Заказа»
    (order_uuid, FIELD_MOYSKLAD_ORDER_UUID) и «№ Заказа» (order_number,
    FIELD_MOYSKLAD_ORDER_NUMBER) - тот же надёжный приём, что уже используется
    в amgroup_fallback._find_existing_lead: найденное в полнотекстовом поиске
    всегда сверяем со значением самого поля, а не доверяем факту попадания в
    выдачу. Имя сделки (name_query, полное совпадение) - запасной путь на
    случай, если у сделки ещё не проставлено ни одно из полей связки.

    Может raise _AmoSearchFailed - см. докстринг модуля, вызывающий код
    обязан прервать создание, а не считать сбой поиска отсутствием дубля."""
    for value, field_id in ((order_uuid, FIELD_MOYSKLAD_ORDER_UUID), (order_number, FIELD_MOYSKLAD_ORDER_NUMBER)):
        if not value:
            continue
        leads = await amo_service.find_leads_by_query(value, limit=_LEAD_SEARCH_LIMIT)
        if leads is None:
            raise _AmoSearchFailed("find_leads_by_query")
        for lead in leads:
            found = str(amo_service.get_custom_field_value(lead, field_id) or "").strip()
            if found.casefold() == str(value).strip().casefold():
                return lead

    if not name_query:
        return None
    leads = await amo_service.find_leads_by_query(name_query, limit=_LEAD_SEARCH_LIMIT)
    if leads is None:
        raise _AmoSearchFailed("find_leads_by_query")
    for lead in leads:
        if lead.get("name") == lead_name:
            return lead
    return None


def _new_lead_profile() -> "lead_distribution.Profile | None":
    """Включённый профиль распределителя с точкой входа «Основная / Новый лид» -
    тот же, что раздаёт сделки amgroup. Матчинг распределителя (match_profile)
    обойти приходится сознательно: он узнаёт сделку по источнику (source_id),
    а сделка, созданная нашим ключом, источника не несёт - через вебхук
    распределитель её не увидит никогда (проверено 03.09.2026: у сделок
    ручного переноса источник пуст, правило их пропустило)."""
    for p in lead_distribution.list_profiles():
        if p.enabled and lead_distribution._matches_entry(p, PIPELINE_CLEVER_MAIN, STATUS_CLEVER_NEW_LEAD):
            return p
    return None


async def pick_responsible(lead_id: int) -> tuple[int | None, str]:
    """Кого ставить ответственным на сделку протеза (03.09.2026, после того как
    amgroup ожил и стало видно, откуда у живых сделок берётся ответственный).
    Порядок:
      1. самовывоз из офиса - офис-менеджер (правило Кати 03.09: Екатерине
         самовывоз, остальное дежурному);
      2. иначе - распределитель лидов, тот же, что раздаёт сделки amgroup: по
         смене из ростера панели, после конца дня - тому, кто на смене завтра;
      3. распределитель выключен, профиля нет или он никого не выбрал -
         константа AMGROUP_LEAD_RESPONSIBLE_USER_ID, запасной ход.
    Возвращает (id пользователя или None, откуда взяли - для лога)."""
    if not LEAD_DISTRIBUTION_ENABLED:
        return AMGROUP_LEAD_RESPONSIBLE_USER_ID, "константа (распределитель выключен)"
    lead = await amo_service.get_lead_full(lead_id, with_=("contacts", "tags"))
    if not lead:
        return AMGROUP_LEAD_RESPONSIBLE_USER_ID, "константа (сделка не прочиталась)"
    if lead_distribution._is_office_delivery(lead):
        return RESPONSIBLE_OFFICE_MANAGER_USER_ID, "самовывоз из офиса - офис-менеджер"
    profile = _new_lead_profile()
    if profile is None:
        return AMGROUP_LEAD_RESPONSIBLE_USER_ID, "константа (нет включённого профиля на «Новый лид»)"
    meta: dict = {}
    try:
        target = await lead_distribution.decide_and_record(lead, profile, meta=meta)
    except Exception:
        logger.exception(
            "amgroup_lead_builder: распределитель упал на сделке %s - берём запасного", lead_id,
        )
        target = None
    if target:
        return int(target), f"распределитель, профиль «{profile.name}», правило {meta.get('rule')}"
    return AMGROUP_LEAD_RESPONSIBLE_USER_ID, "константа (распределитель никого не выбрал)"


async def assign_responsible(lead_id: int, responsible_user_id: int | None = None) -> bool:
    """Отдельный явный шаг - боты amoCRM не реагируют на сделки, созданные не
    через amgroup (проверено 03.09.2026 на двенадцати сделках), а распределитель
    лидов такую сделку через вебхук не узнаёт (у неё нет источника). Значит
    ответственного проставляем сами: responsible_user_id не передан - выбор
    делает pick_responsible (самовывоз → офис-менеджер, иначе распределитель
    по смене, иначе константа). Возвращает True при успехе."""
    if responsible_user_id:
        uid, origin = responsible_user_id, "передан явно"
    else:
        uid, origin = await pick_responsible(lead_id)
    if not uid:
        logger.warning(
            "amgroup_lead_builder: не проставлен ответственный на сделке %s - "
            "AMGROUP_LEAD_RESPONSIBLE_USER_ID не задан в настройках модуля (%s)",
            lead_id, origin,
        )
        return False
    logger.info("amgroup_lead_builder: ответственный на сделке %s - %s (%s)", lead_id, uid, origin)
    result = await amo_service.patch_lead(lead_id, responsible_user_id=int(uid))
    ok = bool(result and result.get("ok"))
    if not ok:
        logger.error(
            "amgroup_lead_builder: не проставился ответственный %s на сделке %s: %s",
            uid, lead_id, result,
        )
    return ok


def _custom_fields(order: dict, b: dict, site: str, *, order_type: str = "Заказ") -> list[dict]:
    """Собирает custom_fields_values для POST /leads. t() - текстовые поля,
    e() - select/enum. Пустые значения не добавляются (как в прототипе).
    t() режет значение через sanitize_custom_field_value (потолок 256
    символов, как и у остальных записей сервиса) - без этого «Состав заказа»
    на заказе из нескольких позиций легко перевалит за потолок amoCRM, и вся
    сделка не запишется (находка приёмки безопасности 03.09.2026)."""
    ag = order.get("agent") or {}
    cf: list[dict] = []

    def t(key: str, val: Any) -> None:
        if val not in (None, "", []):
            cf.append({"field_id": FIELD[key], "values": [{"value": sanitize_custom_field_value(val)}]})

    def e(key: str, val: Any) -> None:
        eid = ENUM[key].get(val)
        if eid:
            cf.append({"field_id": FIELD[key], "values": [{"enum_id": eid}]})

    t("site", site)
    t("pay", _attr(order, "Способ оплаты"))
    t("pvz", _attr(order, "Код ПВЗ"))
    t("promo", _attr(order, "Промокод"))
    t("ym", _attr(order, "ClientID Яндекс.Метрики"))
    t("addr", order.get("shipmentAddress"))
    t("comment", order.get("description"))
    e("channel", (order.get("salesChannel") or {}).get("name"))
    e("store", (order.get("store") or {}).get("name"))
    e("currency", "руб")
    e("paystatus", b["paystatus"])
    e("created_by", "Из МойСклад")
    e("type", order_type)
    t("order_uuid", order.get("id"))
    t("order_num", order.get("name"))
    t("agent_uuid", ag.get("id"))
    t("order_url", f"https://online.moysklad.ru/app/#customerorder/edit?id={order.get('id')}")
    t("sostav", b["sostav"])
    t("weight", b["weight"])
    t("volume", b["volume"])
    t("agent", ag.get("name"))
    t("org", (order.get("organization") or {}).get("name"))
    t("msinfo", MSINFO)
    basket = "\n".join(b["goods"])
    t("basket", basket)
    t("basket_old", basket)
    deliv = "; ".join(f"{s[0]}, {_rub(s[1])}" for s in b["services"])
    t("delivery", deliv)
    t("points", "0")
    return cf


async def create_lead_for_order(order: dict) -> int | None:
    """Точка расширения amgroup_fallback.create_lead_for_order. order -
    заказ покупателя МойСклад, как отдаёт amgroup_fallback._fetch_orders
    (раскрыты agent, salesChannel). Возвращает id созданной (или уже
    существующей - см. дедуп) сделки, либо None при любой неудаче -
    ничего не глотаем молча, каждая причина уходит в лог. Персональные
    данные заказа (имя, телефон, адрес, комментарий клиента) в лог не
    пишем нигде в этой функции - только номер заказа и id сделки."""
    order_uuid = str(order.get("id") or "")
    if not order_uuid:
        logger.error("amgroup_lead_builder: у заказа нет id, пропускаю (номер %s)", order.get("name"))
        return None

    full = await _fetch_full_order(order_uuid)
    if full is None:
        logger.warning(
            "amgroup_lead_builder: МойСклад не ответил на карточку заказа %s "
            "(id %s) - сделку не создаю, это не значит «заказа нет»",
            order.get("name"), order_uuid,
        )
        return None

    site = str(_attr(full, "Номер заказа на сайте") or "").strip()
    order_number = str(full.get("name") or "").strip()
    lead_name = f"Заказ №{site}" if site else f"Заказ МС {order_number}"

    try:
        dup = await _find_existing_lead(order_uuid, order_number, lead_name, site or order_number)
    except _AmoSearchFailed:
        logger.warning(
            "amgroup_lead_builder: amoCRM не ответил при поиске дубля сделки по "
            "заказу %s - сделку не создаю в этом проходе, лучше повтор, чем дубль",
            order_number,
        )
        return None
    if dup is not None:
        logger.info(
            "amgroup_lead_builder: сделка по заказу %s уже есть (id %s), не создаю повторно",
            order_number, dup.get("id"),
        )
        return dup.get("id")

    if not AMGROUP_LEAD_RESPONSIBLE_USER_ID and not LEAD_DISTRIBUTION_ENABLED:
        logger.error(
            "amgroup_lead_builder: AMGROUP_LEAD_RESPONSIBLE_USER_ID не задан и "
            "распределитель лидов выключен - сделка по заказу %s НЕ создаётся "
            "(без ответственного её никто не увидит: боты amoCRM на сделки "
            "протеза не реагируют)",
            order_number,
        )
        return None

    # Классифицируем только новую сделку, после дедупа, но до каких-либо
    # записей в amoCRM. Неизвестный/испорченный снимок удерживаем для разбора:
    # отсутствие поля у старого заказа не доказывает обычный заказ.
    order_type = "Заказ"
    if AMGROUP_PREORDER_TYPE_ENABLED:
        parsed = parse_ms_preorder_type(full, MS_ATTR_PREORDER_SUMMARY_ID)
        if parsed.kind == "unknown":
            logger.warning(
                "amgroup_lead_builder: тип заказа МС %s неизвестен (%s); "
                "новую сделку не создаю до разбора",
                order_number, parsed.reason,
            )
            return None
        order_type = "Предзаказ" if parsed.kind == "preorder" else "Заказ"

    b = _build_fields(full)
    cf = _custom_fields(full, b, site, order_type=order_type)

    ag = full.get("agent") or {}
    try:
        contact_id = await _find_or_create_contact(ag.get("phone"), ag.get("name"), order_number=order_number)
    except _AmoSearchFailed:
        logger.warning(
            "amgroup_lead_builder: amoCRM не ответил при поиске контакта по заказу "
            "%s - сделку не создаю в этом проходе, лучше повтор, чем дубль контакта",
            order_number,
        )
        return None

    price = int((full.get("sum") or 0) / 100)
    lead_id = await api.create_lead_direct(
        name=lead_name,
        pipeline_id=PIPELINE_CLEVER_MAIN,
        status_id=STATUS_CLEVER_NEW_LEAD,
        custom_fields_values=cf,
        contact_id=contact_id,
        tags=[AMGROUP_FALLBACK_TAG],
    )
    if lead_id is None:
        logger.error(
            "amgroup_lead_builder: не создалась сделка по заказу %s (сайт %s, %s ₽)",
            order_number, site, price,
        )
        return None

    logger.info(
        "amgroup_lead_builder: создана сделка %s по заказу %s (сайт %s, %s ₽, контакт %s)",
        lead_id, order_number, site, price, contact_id,
    )

    # Бюджет - отдельным PATCH, тем же приёмом, что и ответственный ниже:
    # у api.create_lead_direct нет параметра суммы (api.py не трогаем), а без
    # него все сделки протеза уезжали бы с ценой 0 (находка приёмки
    # безопасности 03.09.2026) - amgroup эту сумму проставлял, наш протез
    # обязан делать то же самое, иначе ломается отчётность отдела продаж.
    budget_result = await amo_service.patch_lead(lead_id, price=price)
    if not (budget_result and budget_result.get("ok")):
        logger.error(
            "amgroup_lead_builder: не проставился бюджет на сделке %s (заказ %s): %s",
            lead_id, order_number, budget_result,
        )

    # Отдельный явный шаг - см. докстринг assign_responsible.
    await assign_responsible(lead_id)

    return lead_id
