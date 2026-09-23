"""Перенос УР(142)/ЗНР(143) сделок из воронок-источников в целевую воронку/этап
вместо нативного копирования (F5-виджет / «Создать сделку»).

Воронки-источники: [CLEVER] Основная (розница) — всегда; ОПТ — за флагом
OFFICE_TRANSFER_SOURCE_OPT (09.08.2026, решение Кати). Правила для опта те же
пять, что для розницы: этапы 142/143 у ОПТ те же, поля («Тип заявки», «Склад
заказа», «Тип доставки») заполняются так же, поэтому опт-заказ едет в тот же
этап Офиса, что розничный с такой же доставкой — ЗА ИСКЛЮЧЕНИЕМ самовывоза из
шоурума (см. правило 2 ниже, решение Кати 10.08.2026). Список источников — в
_source_pipelines(), гейт для вебхука — is_source_pipeline().

Третий источник — картотека «Работа с базой» за флагом
OFFICE_TRANSFER_SOURCE_DB_WORK (07.09.2026, постановка Кати: «при УР должно
происходить всё то же, что в ОП»). ⚠️ Ей разрешена ТОЛЬКО ветка УР: сделка,
закрытая в картотеке как «не реализовано», обязана остаться там — это карточка
обзвона, а не брак заказа. Разводку держит _allowed_branches().

Правила (условия читаются по СВЕЖЕЙ дочитанной сделке, не по телу вебхука —
select-поля сверяются по enum_id, не по тексту, чтобы не зависеть от того, как
менеджер видит подпись значения):

  УР (142), источник [CLEVER] Основная:
    1. Тип доставки содержит «курьером по москве» ИЛИ «курьерская доставка»
       (новое имя с 23.09.2026, перевозчик при этом НЕ упомянут) + Тип заявки=Заказ +
       Склад∈{Основной,Вскрытые} → Офис/«Оформить доставку»
    2. Тип доставки содержит «самовывоз из офиса» + Тип заявки=Заказ +
       Склад∈{Основной,Вскрытые} → Офис/УР(142). Исключение (10.08.2026,
       решение Кати): для ОПТ самовывоз ИЗ ШОУРУМА конкретно + Тип заявки=Заказ
       → не в УР(142), а в Офис/«Отложенный/резерв товар» — товар опту ещё не
       выдан физически на этот момент (в отличие от розницы). Розница с любым
       самовывозом — как раньше, в УР(142).
    3. Тип заявки=Заказ + Тип доставки содержит CDEK/СДЭК + Склад∈{Основной,
       Вскрытые} → Офис/«Сделать накладную». Схлопнуты правила 3+6+7 исходного
       списка Тианы — все три вели в один и тот же этап (#6 сама Тиана
       пометила дублем #3, #7 добавлял избыточный тег «тест» поверх того же
       условия).
    4. Тип заявки=Предзаказ → Офис/«Предзаказ оплачен»
    5. (УБРАНО 05.08.2026) Тип заявки=Заказ + Склад=ЭРМС_Основной → Фулфилмент/
       «КОНТРОЛЬ». Воронка Фулфилмент разобрана, правила в _UR_RULES больше нет:
       заказ с ЭРМС-склада теперь никуда не переносится и уходит в алерт
       заполнения — если такие заказы ещё появляются, это сигнал, а не маршрут.
    6. Тип заявки=Резерв (Тип доставки не смотрим) → Офис/«Отложенный/резерв
       товар» (25.08.2026, постановка Кати): товар отложен под клиента, но ещё
       не выдан — тот же целевой этап, что у правила 2 для ОПТ+самовывоз из
       шоурума (STATUS_OFFICE_RESERVE), маршрут туда же, вход другой.

  ЗНР (143), поле «Причина ЗИН» (577623, он же DUP_REASON_FIELD_ID):
    1. Причина ЗИН=Лист ожидания → воронка «Лист ожидания»/«Лист ожидания»
    2. Причина ЗИН=Академия → воронка «Академия»/«Первичный контакт»
       (переносим РЕАЛЬНУЮ сделку со всей историей — подтверждено Катей, а не
       создаём пустую копию, как делала прежняя нативная автоматика)
    3. Причина ЗИН=Опт → воронка ОПТ (постановка Тианы 25.08.2026): контакт
       БЕЗ других сделок (новый человек) → «Первичный контакт»; у контакта
       ЕСТЬ другие сделки (в любом статусе/воронке, тот же permissive-принцип
       учёта, что у amo_service.find_other_deal_responsible) → «Найден
       контакт». Единственное ЗНР-правило с асинхронным матчером
       (_match_znr_opt/_contact_has_other_leads) — эмбед контактов на самой
       сделке (with_=("contacts",)) списка ИХ сделок не содержит, нужен
       отдельный GET /api/v4/contacts/{id}?with=leads на каждый контакт.

Смена ответственного — ТОЛЬКО при переносе в Офис (на Екатерину Зубалий,
RESPONSIBLE_OFFICE_MANAGER_USER_ID, + прежний ответственный в поле 578151) и
при переносе в ОПТ по причине «Опт» (на Артёма Коннова,
RESPONSIBLE_OPT_MANAGER_USER_ID, без записи прежнего в 578151 — поле хранит
прежнего МОПа именно для Офиса). Остальные
воронки (Фулфилмент/Лист ожидания/Академия) — ответственный не меняется.

«Онлайн чат» (реактивация клиента, пишущего в закрытую УР/ЗНР сделку) — ВНЕ
ОБЪЁМА этой доработки, не реализовано здесь (см. план: требует отдельного
спайка по обнаружению триггера).

Каждое правило — за собственным флагом (OFFICE_TRANSFER_RULE_*) поверх общего
OFFICE_TRANSFER_ENABLED: Тиана включает правило только после того, как
отключит соответствующую нативную автоматику в Digital Funnel.

Надёжность (первый приоритет по требованию задачи): быстрый путь — вебхук
(LANE_AMO, PRIORITY_NEW) → enqueue_office_transfer → process_office_transfer.
Страховка — периодическая reconciliation по ОКНУ ВРЕМЕНИ через /api/v4/events
(НЕ «все сделки, сейчас сидящие в статусе» — иначе задело бы сделки, висевшие
в УР/ЗНР ДО включения фичи, что прямо запрещено). Провал — тег + примечание +
Telegram-алерт; ретраи продолжаются каждый reconciliation-проход, тег не
блокирует повторные попытки (сделка никогда не теряется молча).

⚠️ ИЗВЕСТНЫЙ РИСК, обнаруженный при реализации (вне согласованного объёма
правок — ПРОВЕРЕНО, но НЕ исправлено здесь, нужно отдельное решение Кати):
metrika_sync.py._resolve_clever() резолвит «оригинал в CLEVER» для дубля в
Офисе/Фулфилменте по УСЛОВИЮ, что оригинал и дубль — РАЗНЫЕ сделки, связанные
полем 576689. После переноса (а не копирования) это больше не так — сделка
одна и та же. Для COD-заказов («наложка», see metrika_sync._classify: Офис/УР
и Фулфилмент/09-09.2 требуют need_resolve_clever=True) резолв не найдёт
сиблинга и заказ молча выпадет из отправки в Яндекс.Метрику (в логе будет
«не нашёл оригинал в CLEVER» — сегодня это ожидаемо для АРХИВНЫХ дублей,
после этой фичи станет систематическим для КАЖДОГО перенесённого COD-заказа).
остальные обработчики — ПРОВЕРЕНЫ, у них нет такого допущения
(читают 576689 с самой сделки / ищут по воронке+UUID без требования отдельного
оригинала), их трогать не нужно. metrika_sync.py — нужно трогать, но это
отдельное решение (влияет на согласованную бизнес-логику аналитики), не
включено в этот PR.
"""

import asyncio
import logging
import time

import amo_service
import alerts
import migration_freeze
import tg_recipients
import telegram_bot
from waybill_config import (
    APPLICATION_TYPE_ORDER,
    APPLICATION_TYPE_PREORDER,
    APPLICATION_TYPE_RESERVE,
    DELIVERY_CDEK_MARKERS,
    DELIVERY_COURIER_OWN_MARKERS,
    DELIVERY_RUSSIAN_POST_MARKER,
    DELIVERY_PICKUP_MARKERS,
    DELIVERY_SHOWROOM_PICKUP_MARKER,
    DUP_REASON_FIELD_ID,
    FIELD_APPLICATION_TYPE,
    FIELD_DELIVERY_TYPE,
    FIELD_FORMER_RESPONSIBLE,
    FIELD_ORDER_WAREHOUSE,
    OFFICE_TRANSFER_ENABLED,
    OFFICE_TRANSFER_RECONCILE_INTERVAL_S,
    OFFICE_TRANSFER_RULE_UR_DELIVERY,
    OFFICE_TRANSFER_RULE_UR_PICKUP,
    OFFICE_TRANSFER_RULE_UR_PREORDER,
    OFFICE_TRANSFER_RULE_UR_RESERVE,
    OFFICE_TRANSFER_RULE_UR_POST,
    OFFICE_TRANSFER_RULE_UR_WAYBILL,
    OFFICE_TRANSFER_RULE_ZNR_ACADEMY,
    OFFICE_TRANSFER_RULE_ZNR_OPT,
    OFFICE_TRANSFER_RULE_ZNR_WAITLIST,
    OFFICE_TRANSFER_SINCE_TS,
    OFFICE_TRANSFER_SOURCE_DB_WORK,
    OFFICE_TRANSFER_SOURCE_OPT,
    OFFICE_TRANSFER_STALE_ALERT_MIN,
    OFFICE_TRANSFER_WAREHOUSES,
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_DB_WORK,
    PIPELINE_OFFICE,
    PIPELINE_OPT,
    PIPELINE_WAITLIST,
    REASON_ACADEMY,
    REASON_OPT,
    REASON_WAITLIST,
    RESPONSIBLE_OFFICE_MANAGER_USER_ID,
    RESPONSIBLE_OPT_MANAGER_USER_ID,
    STATUS_ACADEMY_FIRST_CONTACT,
    STATUS_CLOSED_LOST,
    STATUS_CREATE_WAYBILL,
    STATUS_OFFICE_DELIVERY,
    STATUS_OFFICE_PREORDER_PAID,
    STATUS_OFFICE_RESERVE,
    STATUS_OPT_CONTACT_FOUND,
    STATUS_OPT_PRIMARY_CONTACT,
    STATUS_SUCCESS,
    STATUS_WAITLIST,
    TAG_OFFICE_TRANSFER_ERROR,
    TAG_BAD_FILL,
    TAG_NO_DELIVERY,
    WAREHOUSE_ERMS_MAIN,
)

logger = logging.getLogger("uvicorn")

AMO_LEAD_URL = "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{}"

# Сколько сделок за проход reconciliation считать нормой. Больше — в лог
# предупреждение: скорее всего, воронку-источник миграции забыли внести в
# MIGRATION_SOURCE_PIPELINES, и её поток пошёл в обычную обработку.
RECONCILE_VOLUME_ALERT = 50

# Насколько глубоко проход заглядывает назад. При штатной работе окно = интервал
# между проходами (2 минуты), но после рестарта _last_reconcile_ts обнуляется, и
# окно раскрывается от cutover — на прогоне миграции это 23 000+ событий и сотни
# страниц за один проход (поймано 05.08.2026 сразу после выката). Час назад —
# запас на любой разумный перезапуск; более долгий простой добираем руками.
RECONCILE_MAX_LOOKBACK_S = 3600


# ════════════════ чтение условий по свежей сделке ════════════════

def _application_type(lead: dict) -> int | None:
    return amo_service.get_custom_field_enum_id(lead, FIELD_APPLICATION_TYPE)


def _warehouse(lead: dict) -> int | None:
    return amo_service.get_custom_field_enum_id(lead, FIELD_ORDER_WAREHOUSE)


def _delivery_text(lead: dict) -> str:
    return str(amo_service.get_custom_field_value(lead, FIELD_DELIVERY_TYPE) or "").casefold()


def _reason_enum(lead: dict) -> int | None:
    return amo_service.get_custom_field_enum_id(lead, DUP_REASON_FIELD_ID)


def _pipeline_id(lead: dict) -> int:
    return int(lead.get("pipeline_id") or 0)


# ════════════════ матчеры правил — каждый: сделка → (pipeline_id, status_id) | None ════════════════
# ignore_flags=True — «теневой» матчинг без учёта флагов правил: нужен _no_match_ur,
# чтобы в переходный период (правила включаются по одному) не алертить «заказ
# заполнен некорректно» по нормальным сделкам, чьё правило просто ещё выключено
# (их пока ведёт нативная автоматика Digital Funnel).

def _match_ur_delivery(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    if not ignore_flags and not OFFICE_TRANSFER_RULE_UR_DELIVERY:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) not in OFFICE_TRANSFER_WAREHOUSES:
        return None
    text = _delivery_text(lead)
    # ⚠️ Перевозчика отбрасываем ПЕРВЫМ: «СДЭК: Курьерская доставка» содержит ту же
    # подстроку «курьерская доставка», что и наша курьерка, а ехать ей надо на
    # «Сделать накладную» (_match_ur_waybill), не на «Оформить доставку». Этот матчер
    # в _UR_RULES стоит раньше, поэтому без отсева он перехватил бы СДЭК-заказы.
    if any(marker in text for marker in DELIVERY_CDEK_MARKERS):
        return None
    if not any(marker in text for marker in DELIVERY_COURIER_OWN_MARKERS):
        return None
    return (PIPELINE_OFFICE, STATUS_OFFICE_DELIVERY)


def _match_ur_pickup(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    """Самовывоз → сразу УР Офиса, НЕ рабочий этап «Самовывоз» (решение Кати
    31.07.2026): МОП бросает сделку в УР ОП только когда клиент уже пришёл в
    офис, оплатил и забрал товар — выдача состоялась, в Офисе делать нечего,
    сделка закрывается. Этап «Самовывоз» при нативном копировании был
    формальностью (копии закрывались в УР той же минутой).

    Исключение — ОПТ + самовывоз конкретно ИЗ ШОУРУМА (решение Кати 10.08.2026):
    в отличие от розницы, опт-заказ на этот момент физически ещё не выдан
    клиенту — уходит не в УР(142), а в «Отложенный/резерв товар»
    (STATUS_OFFICE_RESERVE), товар числится в резерве до фактической выдачи.
    Самовывоз из ОФИСА и розница с любым самовывозом — как раньше, в УР(142)."""
    if not ignore_flags and not OFFICE_TRANSFER_RULE_UR_PICKUP:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) not in OFFICE_TRANSFER_WAREHOUSES:
        return None
    text = _delivery_text(lead)
    if DELIVERY_SHOWROOM_PICKUP_MARKER in text and _pipeline_id(lead) == PIPELINE_OPT:
        return (PIPELINE_OFFICE, STATUS_OFFICE_RESERVE)
    # С 06.08.2026 самовывоз бывает из офиса и из шоурума (у шоурума свой склад).
    if not any(marker in text for marker in DELIVERY_PICKUP_MARKERS):
        return None
    return (PIPELINE_OFFICE, STATUS_SUCCESS)


def _match_ur_waybill(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    if not ignore_flags and not OFFICE_TRANSFER_RULE_UR_WAYBILL:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) not in OFFICE_TRANSFER_WAREHOUSES:
        return None
    text = _delivery_text(lead)
    if not any(marker in text for marker in DELIVERY_CDEK_MARKERS):
        return None
    return (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL)


def _match_ur_post(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    """Почта России → Офис/«Сделать накладную» (решение Кати 31.07.2026: пока в
    общий этап отправки, воронку переработают позже). Вебхук автонакладной на
    этом этапе для почты отработает БЕЗОПАСНО: parse_tariff не найдёт тариф СДЭК
    в поле 576703 → тег + алерт «не определён тариф» — ожидаемый сигнал офису
    оформить почтовую отправку руками, а не баг (сверено по коду waybill_service
    31.07.2026: нераспознанный тариф = _fail без создания накладной)."""
    if not ignore_flags and not OFFICE_TRANSFER_RULE_UR_POST:
        return None
    if _application_type(lead) != APPLICATION_TYPE_ORDER:
        return None
    if _warehouse(lead) not in OFFICE_TRANSFER_WAREHOUSES:
        return None
    if DELIVERY_RUSSIAN_POST_MARKER not in _delivery_text(lead):
        return None
    return (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL)


def _match_ur_preorder(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    if not ignore_flags and not OFFICE_TRANSFER_RULE_UR_PREORDER:
        return None
    if _application_type(lead) != APPLICATION_TYPE_PREORDER:
        return None
    return (PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID)


def _match_ur_reserve(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    """Тип заявки=Резерв → Офис/«Отложенный/резерв товар», тип доставки не
    смотрим (постановка Кати 25.08.2026): товар отложен под клиента, но ещё не
    выдан — тот же целевой этап, что у ОПТ+самовывоз из шоурума в
    _match_ur_pickup, только вход по другому значению «Типа заявки»."""
    if not ignore_flags and not OFFICE_TRANSFER_RULE_UR_RESERVE:
        return None
    if _application_type(lead) != APPLICATION_TYPE_RESERVE:
        return None
    return (PIPELINE_OFFICE, STATUS_OFFICE_RESERVE)


_UR_RULES = (
    _match_ur_delivery,
    _match_ur_pickup,
    _match_ur_waybill,
    _match_ur_post,
    _match_ur_preorder,
    _match_ur_reserve,
)


def _match_znr_waitlist(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    if not ignore_flags and not OFFICE_TRANSFER_RULE_ZNR_WAITLIST:
        return None
    if _reason_enum(lead) != REASON_WAITLIST:
        return None
    return (PIPELINE_WAITLIST, STATUS_WAITLIST)


def _match_znr_academy(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    if not ignore_flags and not OFFICE_TRANSFER_RULE_ZNR_ACADEMY:
        return None
    if _reason_enum(lead) != REASON_ACADEMY:
        return None
    return (PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT)


async def _contact_has_other_leads(lead: dict) -> bool:
    """Хотя бы у одного контакта сделки есть ДРУГИЕ сделки (кроме текущей).
    Эмбед контактов на самой сделке (with_=("contacts",), как читает
    process_office_transfer) содержит только id/ссылки — без списка ИХ
    сделок, поэтому каждый контакт дочитывается отдельно
    (with_=("leads",)), по образцу amo_service.find_other_deal_responsible.
    Статус/воронка сделок не фильтруются — тот же permissive-принцип, что и
    там. Контакт не дочитался (get_contact_by_id вернул None) — пропускаем
    его, не роняем матчинг."""
    lead_id = int(lead.get("id") or 0)
    contacts = (lead.get("_embedded") or {}).get("contacts") or []
    for c in contacts:
        cid = c.get("id")
        if not cid:
            continue
        full = await amo_service.get_contact_by_id(cid, with_=("leads",))
        if not full:
            continue
        other_leads = (full.get("_embedded") or {}).get("leads") or []
        if any(l.get("id") is not None and int(l["id"]) != lead_id for l in other_leads):
            return True
    return False


async def _match_znr_opt(lead: dict, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    """Причина ЗИН=Опт → воронка ОПТ (постановка Тианы 25.08.2026): новый
    контакт (других сделок нет) → «Первичный контакт»; контакт уже
    встречался (есть другие сделки) → «Найден контакт». Единственный
    асинхронный матчер в _ZNR_RULES/_UR_RULES — см. _match_rules ниже,
    которая умеет и синхронные, и асинхронные правила вперемешку."""
    if not ignore_flags and not OFFICE_TRANSFER_RULE_ZNR_OPT:
        return None
    if _reason_enum(lead) != REASON_OPT:
        return None
    if await _contact_has_other_leads(lead):
        return (PIPELINE_OPT, STATUS_OPT_CONTACT_FOUND)
    return (PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT)


_ZNR_RULES = (
    _match_znr_waitlist,
    _match_znr_academy,
    _match_znr_opt,
)


async def _match_rules(lead: dict, status_id: int, *, ignore_flags: bool = False) -> tuple[int, int] | None:
    """Матчеры бывают синхронные (все _UR_RULES, большинство _ZNR_RULES) и
    асинхронные (_match_znr_opt — нужен I/O за контактом). Вызываем каждый
    БЕЗ await, и если результат — корутина (асинхронный матчер), дожидаемся
    её отдельно: так не пришлось переписывать сигнатуры уже существующих
    синхронных правил ради одного нового."""
    allowed = _allowed_branches(_pipeline_id(lead))
    if status_id == STATUS_SUCCESS and _BRANCH_UR in allowed:
        rules = _UR_RULES
    elif status_id == STATUS_CLOSED_LOST and _BRANCH_ZNR in allowed:
        rules = _ZNR_RULES
    else:
        rules = ()
    for fn in rules:
        target = fn(lead, ignore_flags=ignore_flags)
        if asyncio.iscoroutine(target):
            target = await target
        if target is not None:
            return target
    return None


def _source_pipelines() -> tuple[int, ...]:
    """Воронки, ИЗ которых переносим. Розница — всегда, ОПТ — за флагом
    OFFICE_TRANSFER_SOURCE_OPT (09.08.2026), картотека «Работа с базой» — за
    OFFICE_TRANSFER_SOURCE_DB_WORK (07.09.2026).

    Читаем флаг на КАЖДОМ вызове, а не собираем кортеж на импорте: иначе флаг,
    подменённый в тестах (и в консоли при разборе инцидента), не подействовал бы.

    Правила у опта те же пять, что у розницы, — отдельного матчера нет по
    построению. Условия правил читаются с полей сделки («Тип заявки», «Склад
    заказа», «Тип доставки»), а они у опта заполняются так же, поэтому опт-заказ
    едет в тот же этап Офиса, что и розничный с такой же доставкой. С картотекой
    так же — но ей разрешена только ветка УР, см. _allowed_branches()."""
    out = [PIPELINE_CLEVER_MAIN]
    if OFFICE_TRANSFER_SOURCE_OPT:
        out.append(PIPELINE_OPT)
    if OFFICE_TRANSFER_SOURCE_DB_WORK:
        out.append(PIPELINE_DB_WORK)
    return tuple(out)


_BRANCH_UR = "ur"
_BRANCH_ZNR = "znr"


def _allowed_branches(pipeline_id) -> frozenset[str]:
    """Какие ветки правил разрешены воронке-источнику.

    Картотека «Работа с базой» — ТОЛЬКО УР. Сделка, закрытая там как «не
    реализовано», обязана ОСТАТЬСЯ в картотеке: это карточка обзвона менеджера,
    а не брак заказа. ЗНР-правила увезли бы её в Лист ожидания / Академию / ОПТ,
    и человек потерял бы её из своего списка (постановка Кати 07.09.2026).

    Ограничение бизнесовое, а не переходное, поэтому оно НЕ снимается
    ignore_flags: теневой матчинг в _no_match_ur гасит флаги правил, но ветку
    ЗНР картотеке не открывает."""
    try:
        pid = int(pipeline_id)
    except (TypeError, ValueError):
        return frozenset({_BRANCH_UR, _BRANCH_ZNR})
    if pid == PIPELINE_DB_WORK:
        return frozenset({_BRANCH_UR})
    return frozenset({_BRANCH_UR, _BRANCH_ZNR})


def is_source_pipeline(pipeline_id) -> bool:
    """Публичный гейт для webhooks.py: воронка годится как источник переноса."""
    try:
        return int(pipeline_id) in _source_pipelines()
    except (TypeError, ValueError):
        return False


# ════════════════ диспетчер ════════════════

# lead_id → {"since": monotonic-независимый unix ts первой неудачи, "alerted": bool}
_pending_fail: dict[int, dict] = {}


def _clear_fail(lead_id: int) -> None:
    _pending_fail.pop(int(lead_id), None)


async def _stale_alert(lead: dict, state: dict) -> None:
    if OFFICE_TRANSFER_STALE_ALERT_MIN <= 0 or state["alerted"]:
        return
    age_min = (time.time() - state["since"]) / 60
    if age_min < OFFICE_TRANSFER_STALE_ALERT_MIN:
        return
    state["alerted"] = True
    lead_id = lead.get("id")
    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    d = alerts.decide(
        "office_transfer_stuck",
        legacy_text=(
            f"🚨 Сделка {lead_id} застряла в УР/ЗНР дольше {int(age_min)} мин, "
            f"автоперенос не удался — нужна ручная проверка.\n"
            f"{lead.get('name') or ''}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}"
        ),
        chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID, lead=lead,
        responsible_id=lead.get("responsible_user_id"),
        values={
            "сколько_ждали": f"{int(age_min)} мин",
            "сделка": lead.get("name") or "",
            "ссылка_на_сделку": alerts.lead_link(lead_id),
            "теги": mentions,
        },
    )
    if d is not None:
        await telegram_bot.send_alert(d.text, **d.send_kwargs())


async def _fail(lead: dict, reason: str) -> None:
    """Перенос не удался: тег + примечание + (по истечении порога) один
    Telegram-алерт. НЕ блокирует повторные попытки reconciliation — тег
    только информирует, что сделка требует внимания."""
    lead_id = int(lead.get("id"))
    logger.warning("office_transfer %s: %s", lead_id, reason)
    state = _pending_fail.setdefault(lead_id, {"since": time.time(), "alerted": False})
    await amo_service.add_tag(lead_id, TAG_OFFICE_TRANSFER_ERROR)
    await amo_service.add_note(
        lead_id,
        f"⚠️ Автоперенос в целевую воронку не выполнен: {reason}. "
        f"Повторные попытки продолжаются автоматически.",
    )
    await _stale_alert(lead, state)


# УР-сделки без правила: дедуп ТГ-алертов в памяти процесса (поверх тега
# на сделке, который переживает рестарт).
_fill_alerted: set[int] = set()


async def _notify_fill_problem(lead: dict, tag: str, note: str, alert: str, outcome: str) -> str:
    """УР-сделка не подошла ни под одно правило — автоперенос невозможен, дело в
    заполнении полей, само не рассосётся (решение Кати 31.07.2026: сигналим
    человеку, НЕ переносим наугад). Тег + примечание + ТГ-алерт СРАЗУ, без
    порога ожидания. Дедуп: тег на сделке + set в памяти. Менеджер дозаполнит
    поля → amo пришлёт вебхук на обновление сделки (условие в webhooks.py
    срабатывает и по ТЕКУЩЕМУ статусу 142 в leads[update]) → матчинг
    повторится, сделка переносится штатно, тег снимается в диспетчере."""
    lead_id = int(lead.get("id"))
    if lead_id in _fill_alerted or any(
        (t.get("name") or "") == tag for t in amo_service.get_tags(lead)
    ):
        return "skipped-already-alerted"
    _fill_alerted.add(lead_id)
    logger.warning("office_transfer %s: %s", lead_id, outcome)
    await amo_service.add_tag(lead_id, tag)
    await amo_service.add_note(lead_id, note)
    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    d = alerts.decide(
        "office_transfer_bad_fill",
        legacy_text=f"⚠️ {alert}\n{lead.get('name') or ''}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}",
        chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID, lead=lead,
        responsible_id=lead.get("responsible_user_id"),
        values={
            "причина": alert,
            "сделка": lead.get("name") or "",
            "ссылка_на_сделку": alerts.lead_link(lead_id),
            "теги": mentions,
        },
    )
    if d is not None:
        await telegram_bot.send_alert(d.text, **d.send_kwargs())
    return outcome


async def _no_match_ur(lead: dict) -> str:
    """Классификация УР-сделки без правила (паттерны из анализа 90 дней,
    решения Кати 31.07.2026). Сначала «теневой» матчинг без флагов: сделка,
    которая подошла бы под ещё выключенное правило, — не проблема заполнения,
    её пока ведёт нативная автоматика — молчим. Дальше два случая:
    «Заказ + склад на месте, а Тип доставки пуст» (менеджеру достаточно
    дозаполнить одно поле) и «всё остальное» (нет типа заявки/склада,
    чужой склад, нераспознанная доставка вроде «мэйлру»)."""
    if await _match_rules(lead, STATUS_SUCCESS, ignore_flags=True) is not None:
        logger.info(
            "office_transfer %s: подошла бы под выключенное правило — ведёт нативка, молчим",
            lead.get("id"),
        )
        return "no-match-rule-disabled"
    lead_id = lead.get("id")
    app = _application_type(lead)
    wh = _warehouse(lead)
    if (app == APPLICATION_TYPE_ORDER
            and wh in OFFICE_TRANSFER_WAREHOUSES
            and not _delivery_text(lead).strip()):
        return await _notify_fill_problem(
            lead, TAG_NO_DELIVERY,
            "⚠️ Автоперенос: в сделке не заполнен «Тип доставки» — она осталась в УР. "
            "Заполните поле — перенос отработает сам.",
            f"Сделка {lead_id} в УР без «Типа доставки» — автоперенос не знает, куда "
            f"её вести. Дозаполните поле.",
            "no-match-no-delivery",
        )
    return await _notify_fill_problem(
        lead, TAG_BAD_FILL,
        "⚠️ Автоперенос: заказ заполнен некорректно (тип заявки/склад/доставка не "
        "дают определить маршрут) — сделка осталась в УР. Поправьте поля — "
        "перенос отработает сам.",
        f"Сделка {lead_id} в УР заполнена некорректно (тип заявки/склад/доставка) — "
        f"автоперенос стоит. Поправьте поля.",
        "no-match-bad-fill",
    )


async def process_office_transfer(lead_id, source: str = "webhook") -> str:
    """Обработчик очереди (LANE_AMO) / reconciliation. Возвращает исход
    строкой (лог/тесты). Всегда дочитывает сделку заново — состояние могло
    смениться, пока задача ждала в очереди, или между reconciliation-проходами."""
    if not OFFICE_TRANSFER_ENABLED:
        return "disabled"

    lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
    if not lead:
        logger.warning("office_transfer %s: сделка не прочиталась", lead_id)
        return "failed-lead-read"

    # Окно миграции воронок: перенесённую сделку дальше не тащим. Проверка тут,
    # а не только в вебхуке, — иначе reconciliation (раз в 2 минуты) подберёт её
    # по событию входа в 142/143. Сделка уже на руках, лишнего запроса нет.
    if await migration_freeze.skip(lead_id, "office_transfer", lead=lead):
        return "skipped-migration-freeze"

    status_id = int(lead.get("status_id") or 0)
    pipeline_id = int(lead.get("pipeline_id") or 0)
    if pipeline_id not in _source_pipelines() or status_id not in (STATUS_SUCCESS, STATUS_CLOSED_LOST):
        # Уже перенесена (нами или вручную), либо это не тот случай — идемпотентный
        # no-op. Гасит и эхо от нашего же PATCH, и повторную доставку вебхука.
        # Сюда же попадает ОПТ при выключенном OFFICE_TRANSFER_SOURCE_OPT.
        logger.info(
            "office_transfer %s: воронка не источник или этап не {142,143} "
            "(pipeline=%s status=%s, источники=%s) — скип",
            lead_id, pipeline_id, status_id, _source_pipelines(),
        )
        _clear_fail(lead_id)
        return "skipped-not-applicable"

    if OFFICE_TRANSFER_SINCE_TS:
        closed_at = int(lead.get("closed_at") or 0)
        if closed_at and closed_at < OFFICE_TRANSFER_SINCE_TS:
            # «Без ретроактивности» и для ВЕБХУК-пути (31.07.2026): reconciliation
            # фильтрует по окну событий, а вебхук прилетает на ЛЮБОЕ обновление
            # старой закрытой сделки (примечание, правка поля) — без этого гейта
            # УР/ЗНР-архив уезжал бы в Офис при первом же касании. closed_at
            # обновляется при переоткрытии → свежий вход в 142/143 легитимно
            # пройдёт (новый closed_at уже после cutover).
            logger.info(
                "office_transfer %s: closed_at=%s < cutover=%s — скип (ретро)",
                lead_id, closed_at, OFFICE_TRANSFER_SINCE_TS,
            )
            _clear_fail(lead_id)
            return "skipped-pre-cutover"

    # Гейта «578151 заполнен = уже переносили» больше нет (решение Кати 15.09.2026).
    # Поле правят люди: сделку 36556353 офис-менеджер завела с собой в «Ответственном
    # МОПе», и перенос молча скипал её четыре раза. А защищать было нечего: сюда
    # доходит только сделка в УР/ЗНР воронки-источника, то есть вне Офиса, и с 08.08
    # по 15.09 ни одну сделку не вернули из Офиса в УР. Снова закрыли успешно после
    # возврата - значит, новый заказ, ему правильно ехать в Офис. Архив до cutover
    # держит гейт по closed_at выше.

    target = await _match_rules(lead, status_id)
    if target is None:
        if status_id == STATUS_SUCCESS:
            return await _no_match_ur(lead)
        # ЗНР без «Листа ожидания»/«Академии» остаётся в CLEVER — норма, молчим.
        return "no-match"
    target_pipeline_id, target_status_id = target

    # PAID/CANCELLED ДО переноса (мина «копирование→перенос», фикс 31.07.2026):
    # Метрика и Woo классифицируют предоплату по состоянию «CLEVER/142», а
    # после PATCH сделка в нём больше не появится — сверка по расписанию его
    # уже не застанет. Прогоняем оба синка по ещё-старому состоянию сейчас.
    # Ошибка синка НЕ блокирует перенос: в metrika_sync теперь есть страховочная
    # сетка (Офис/142 и ФФ дают PAID и для предоплаты, резолв умеет «сделка
    # сама себе оригинал») — доловит при закрытии целевой воронки.
    # Импорты ленивые — по образцу reconcile_window (кольцо woo↔metrika).
    import metrika_sync
    import woo_status_sync
    try:
        await metrika_sync.process_sync({"lead_id": lead_id}, lead=lead)
    except Exception:
        logger.exception("office_transfer %s: Метрика-синк до переноса упал", lead_id)
    if woo_status_sync.is_enabled():
        try:
            await woo_status_sync.process_sync({"lead_id": lead_id}, lead=lead)
        except Exception:
            logger.exception("office_transfer %s: Woo-синк до переноса упал", lead_id)

    patch_kwargs: dict = {"pipeline_id": target_pipeline_id, "status_id": target_status_id}
    if target_pipeline_id == PIPELINE_OFFICE:
        current_responsible = lead.get("responsible_user_id")
        if current_responsible != RESPONSIBLE_OFFICE_MANAGER_USER_ID:
            former_name = None
            if current_responsible:
                former_name = await amo_service.get_user_name(current_responsible)
            patch_kwargs["responsible_user_id"] = RESPONSIBLE_OFFICE_MANAGER_USER_ID
            patch_kwargs["custom_fields"] = {
                FIELD_FORMER_RESPONSIBLE: former_name or (str(current_responsible) if current_responsible else ""),
            }
    elif target_pipeline_id == PIPELINE_OPT:
        # Причина ЗИН=Опт (решение Тианы 25.08.2026): распределение лидов не
        # смотрит на воронку ОПТ, без явной смены сделка осталась бы на
        # прежнем розничном МОПе. Прежнего сюда не пишем — 578151 хранит
        # прежнего МОПа именно для Офиса.
        if lead.get("responsible_user_id") != RESPONSIBLE_OPT_MANAGER_USER_ID:
            patch_kwargs["responsible_user_id"] = RESPONSIBLE_OPT_MANAGER_USER_ID

    result = await amo_service.patch_lead(lead_id, **patch_kwargs)
    if not result.get("ok"):
        await _fail(lead, f"PATCH не прошёл (status_code={result.get('status_code')})")
        return "failed-patch"

    logger.info(
        "office_transfer %s: перенесена CLEVER/%s → воронка %s / этап %s (source=%s)",
        lead_id, status_id, target_pipeline_id, target_status_id, source,
    )
    _clear_fail(lead_id)
    # Сделка уехала штатно — снимаем сигналы проблем заполнения, если висели
    # (менеджер дозаполнил поля после алерта — тег больше не актуален).
    _fill_alerted.discard(int(lead.get("id")))
    for _tag in (TAG_NO_DELIVERY, TAG_BAD_FILL):
        if any((t.get("name") or "") == _tag for t in amo_service.get_tags(lead)):
            await amo_service.remove_tag(lead_id, _tag, lead=lead)

    return "moved"


# ════════════════ reconciliation (окно по времени, НЕ «текущий статус») ════════════════

async def _entered_status_leads(pipeline_id: int, status_id: int, ts_from: int, ts_to: int) -> set[int]:
    """ID сделок, ПЕРЕШЕДШИХ в pipeline_id/status_id за окно [ts_from, ts_to) —
    по событию lead_status_changed.
    обобщённый на произвольные pipeline/status. НЕ используем
    amo_service.get_leads_by_status() здесь — та возвращает ВСЁ, что сейчас
    сидит в статусе, независимо от времени входа, что нарушило бы требование
    «без ретроактивности» (задело бы сделки, висевшие в УР/ЗНР до cutover).

    Во время массового прогона миграции отсеиваем его собственные события по
    `value_before` — сделка приехала из воронки-источника, а не закрылась у
    менеджера. Признак лежит в этом же ответе, дочитывать сделки не нужно."""
    leads: set[int] = set()
    skipped_bulk = 0
    page = 1
    while True:
        params = [
            ("filter[type]", "lead_status_changed"),
            ("filter[created_at][from]", str(ts_from)),
            ("filter[created_at][to]", str(ts_to)),
            ("filter[value_after][leads_statuses][0][pipeline_id]", str(pipeline_id)),
            ("filter[value_after][leads_statuses][0][status_id]", str(status_id)),
            ("limit", "100"), ("page", str(page)),
        ]
        d = await amo_service._do_get("/api/v4/events", params)
        evs = ((d or {}).get("_embedded") or {}).get("events") or []
        for e in evs:
            va = e.get("value_after") or []
            ls = (va[0].get("lead_status") if va else None) or {}
            if ls.get("id") == status_id and ls.get("pipeline_id") == pipeline_id:
                lid = e.get("entity_id")
                if lid is None:
                    continue
                vb = e.get("value_before") or []
                before = (vb[0].get("lead_status") if vb else None) or {}
                if migration_freeze.is_bulk_move_event(before.get("pipeline_id")):
                    skipped_bulk += 1
                    continue
                leads.add(int(lid))
        if len(evs) < 100:
            break
        page += 1
    if skipped_bulk:
        logger.info(
            "office_transfer reconcile: событий миграции отсеяно %s (статус %s), сделок к работе %s",
            skipped_bulk, status_id, len(leads),
        )
    return leads


_last_reconcile_ts: int = 0
_reconcile_task: asyncio.Task | None = None


async def _reconcile_once() -> str:
    global _last_reconcile_ts
    now = int(time.time())

    # Массовый прогон миграции проход НЕ отключает (05.08.2026). Раньше здесь
    # стоял выход по bulk_skip: reconcile дочитывал каждую перенесённую сделку
    # ради тега — 537 запросов за 5 минут на прогоне 04.08. Но вместе с
    # прогоном глохла и БОЕВАЯ обработка: вебхуки о закрытии гасит bulk_skip, а
    # страховкой был как раз этот проход. 05.08 так встали заказы №18235 и
    # №18245 — уехали бы в Офис только после конца окна, ночью.
    # Теперь миграционные события отсеивает _entered_status_leads по
    # value_before (видно в самом событии), сделки не дочитываются, лимит цел.
    window_from = max(_last_reconcile_ts, OFFICE_TRANSFER_SINCE_TS)
    if window_from <= 0:
        logger.warning("office_transfer reconcile: OFFICE_TRANSFER_SINCE_TS не задан — проход пропущен")
        return "skipped-no-cutover"
    # Потолок оглядки ставим ПОСЛЕ проверки cutover: иначе окно всегда выглядело
    # бы заданным и защита «без границы не запускаться» перестала бы работать.
    window_from = max(window_from, now - RECONCILE_MAX_LOOKBACK_S)

    # Обе воронки-источника за один проход: розница всегда, ОПТ — если включён
    # флаг. По воронке два запроса (142 и 143), то есть с ОПТ проход стоит
    # четыре запроса вместо двух — на интервале 2 минуты это в лимиты влезает
    # с запасом (замер лимитов amo: 50 на аккаунт, 7 на интеграцию, 03.08.2026).
    leads: set[int] = set()
    for _src in _source_pipelines():
        leads |= await _entered_status_leads(_src, STATUS_SUCCESS, window_from, now)
        leads |= await _entered_status_leads(_src, STATUS_CLOSED_LOST, window_from, now)

    # Предохранитель: в норме за проход набегают единицы сделок. Много — значит
    # в перенос завели воронку, которой нет в MIGRATION_SOURCE_PIPELINES, и её
    # поток пошёл в дочитывание. Дальше работаем (терять боевые нельзя), но в
    # логе это видно сразу, а не по отказам amo.
    if len(leads) > RECONCILE_VOLUME_ALERT:
        logger.warning(
            "office_transfer reconcile: в окне %s сделок — сверьте MIGRATION_SOURCE_PIPELINES, "
            "похоже, воронку-источник не внесли в список",
            len(leads),
        )

    processed = 0
    for lead_id in leads:
        await process_office_transfer(lead_id, source="reconcile")
        processed += 1
    _last_reconcile_ts = now
    logger.info("office_transfer reconcile: окно [%s, %s), сделок в окне %s", window_from, now, processed)
    return f"processed={processed}"


async def _reconcile_loop() -> None:
    while True:
        await asyncio.sleep(OFFICE_TRANSFER_RECONCILE_INTERVAL_S)
        try:
            await _reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("office_transfer reconcile: ошибка прохода")


# ════════════════ config-drift проверка + запуск/остановка из lifespan ════════════════

# (флаг правила, pipeline_id, status_id, метка для лога) — сверяется с прогретым
# кэшем воронок ТОЛЬКО для включённых правил, чтобы не шуметь про ещё не
# скроенные (флаг=off) цели.
_RULE_TARGETS = (
    (OFFICE_TRANSFER_RULE_UR_DELIVERY, PIPELINE_OFFICE, STATUS_OFFICE_DELIVERY, "УР→Офис/Оформить доставку"),
    (OFFICE_TRANSFER_RULE_UR_PICKUP, PIPELINE_OFFICE, STATUS_SUCCESS, "УР→Офис/УР (самовывоз: выдан на месте)"),
    (OFFICE_TRANSFER_RULE_UR_PICKUP, PIPELINE_OFFICE, STATUS_OFFICE_RESERVE, "УР→Офис/Отложенный резерв (ОПТ+шоурум)"),
    (OFFICE_TRANSFER_RULE_UR_WAYBILL, PIPELINE_OFFICE, STATUS_CREATE_WAYBILL, "УР→Офис/Сделать накладную"),
    (OFFICE_TRANSFER_RULE_UR_PREORDER, PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID, "УР→Офис/Предзаказ оплачен"),
    (OFFICE_TRANSFER_RULE_UR_RESERVE, PIPELINE_OFFICE, STATUS_OFFICE_RESERVE, "УР→Офис/Отложенный резерв (Резерв)"),
    (OFFICE_TRANSFER_RULE_ZNR_WAITLIST, PIPELINE_WAITLIST, STATUS_WAITLIST, "ЗНР→Лист ожидания"),
    (OFFICE_TRANSFER_RULE_ZNR_ACADEMY, PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT, "ЗНР→Академия"),
    (OFFICE_TRANSFER_RULE_ZNR_OPT, PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT, "ЗНР→ОПТ/Первичный контакт (новый)"),
    (OFFICE_TRANSFER_RULE_ZNR_OPT, PIPELINE_OPT, STATUS_OPT_CONTACT_FOUND, "ЗНР→ОПТ/Найден контакт (повторный)"),
)


async def _alert(text: str, event: str | None = None, values: dict | None = None) -> None:
    """Технический рапорт. `event` - ключ события в каталоге панели: панель может выключить
    его или перенаправить; без ключа - как раньше, прямо в технический чат."""
    try:
        body, kw = text, {}
        if event:
            d = alerts.decide(event, legacy_text=text, values=values or {})
            if d is None:
                logger.info("%s: уведомление выключено в панели", event)
                return
            body, kw = d.text, d.send_kwargs()
        await telegram_bot.send_alert(body, **kw)
    except Exception:
        logger.exception("office_transfer alert failed: %s", text)


async def _validate_enabled_targets() -> None:
    missing = [
        label for enabled, pid, sid, label in _RULE_TARGETS
        if enabled and amo_service.get_status_sort(sid, pid) is None
    ]
    if missing:
        msg = (
            "office_transfer: не найдены в прогретом кэше воронок целевые этапы: "
            f"{', '.join(missing)} — проверьте ID в waybill_config.py (переименовали/удалили этап?)"
        )
        logger.error(msg)
        await _alert(msg, "office_transfer_targets_missing", {"этапы": ", ".join(missing)})


async def init() -> None:
    """Вызывается из lifespan ПОСЛЕ amo_service.warm_pipeline_cache(). Проверяет
    целевые этапы включённых правил, запускает reconciliation."""
    global _last_reconcile_ts
    if not OFFICE_TRANSFER_ENABLED:
        logger.info("office_transfer: ВЫКЛЮЧЕН (OFFICE_TRANSFER_ENABLED)")
        return

    await _validate_enabled_targets()

    if OFFICE_TRANSFER_SINCE_TS <= 0:
        msg = (
            "office_transfer: OFFICE_TRANSFER_ENABLED=1, но OFFICE_TRANSFER_SINCE_TS не задан — "
            "reconciliation НЕ запущена (иначе задело бы сделки, висевшие в УР/ЗНР до включения). "
            "Вебхук-путь при этом работает."
        )
        logger.error(msg)
        await _alert(msg, "office_transfer_no_since")
        return

    _last_reconcile_ts = OFFICE_TRANSFER_SINCE_TS
    start_reconcile()


def start_reconcile() -> None:
    global _reconcile_task
    if OFFICE_TRANSFER_RECONCILE_INTERVAL_S <= 0:
        logger.info("office_transfer: reconciliation выключена (OFFICE_TRANSFER_RECONCILE_INTERVAL_S=0)")
        return
    _reconcile_task = asyncio.create_task(_reconcile_loop())
    logger.info(
        "office_transfer: reconciliation каждые %s сек, cutover=%s",
        OFFICE_TRANSFER_RECONCILE_INTERVAL_S, OFFICE_TRANSFER_SINCE_TS,
    )


async def stop_reconcile() -> None:
    global _reconcile_task
    if _reconcile_task is not None:
        _reconcile_task.cancel()
        try:
            await _reconcile_task
        except asyncio.CancelledError:
            pass
        _reconcile_task = None
