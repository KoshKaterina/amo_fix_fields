"""Юнит-тест office_transfer (без сети/прода).

Проверяем: матчеры правил (позитив/негатив по каждому условию, схлопнутое
правило СДЭК/3+6+7), guard'ы диспетчера (вне CLEVER / вне {142,143} — skip,
идемпотентно), смена ответственного ТОЛЬКО при переносе в Офис (+ пропуск
повторной записи, если ответственный уже Зубалий, + отсутствие смены для
остальных воронок), путь отказа (тег+примечание, алерт по порогу с дедупом),
cutover-окно reconciliation (без заданной границы — проход не идёт; с
границей — окно не уходит раньше неё, без ретроактивности).

amo_service.get_lead_full/patch_lead/add_tag/add_note/get_user_name/_do_get и
telegram_bot.send_alert — фейки. Чтение полей сделки (get_custom_field_value/
get_custom_field_enum_id) — реальные функции, сделки собираем как настоящий
payload amo (custom_fields_values).
"""
import asyncio
import time

import office_transfer
from waybill_config import (
    APPLICATION_TYPE_ORDER,
    APPLICATION_TYPE_PREORDER,
    APPLICATION_TYPE_RESERVE,
    DUP_REASON_FIELD_ID,
    FIELD_APPLICATION_TYPE,
    FIELD_DELIVERY_TYPE,
    FIELD_FORMER_RESPONSIBLE,
    FIELD_ORDER_WAREHOUSE,
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
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
    WAREHOUSE_SUNSCRYPT_MAIN,
    WAREHOUSE_SUNSCRYPT_OPENED,
)


def run(coro):
    return asyncio.run(coro)


def _cf(field_id, *, value=None, enum_id=None):
    v = {}
    if value is not None:
        v["value"] = value
    if enum_id is not None:
        v["enum_id"] = enum_id
    return {"field_id": field_id, "values": [v]}


def _lead(*, status_id=STATUS_SUCCESS, pipeline_id=PIPELINE_CLEVER_MAIN,
          application_type=None, warehouse=None, delivery_text=None,
          reason=None, responsible_user_id=999, lead_id=42):
    cfs = []
    if application_type is not None:
        cfs.append(_cf(FIELD_APPLICATION_TYPE, enum_id=application_type))
    if warehouse is not None:
        cfs.append(_cf(FIELD_ORDER_WAREHOUSE, enum_id=warehouse))
    if delivery_text is not None:
        cfs.append(_cf(FIELD_DELIVERY_TYPE, value=delivery_text))
    if reason is not None:
        cfs.append(_cf(DUP_REASON_FIELD_ID, enum_id=reason))
    return {
        "id": lead_id,
        "status_id": status_id,
        "pipeline_id": pipeline_id,
        "responsible_user_id": responsible_user_id,
        "name": "Тестовая сделка",
        "custom_fields_values": cfs,
    }


# ── включаем ВСЕ флаги правил + мастер-флаг для тестов матчеров/диспетчера ──
office_transfer.OFFICE_TRANSFER_ENABLED = True
for _flag in (
    "OFFICE_TRANSFER_RULE_UR_DELIVERY", "OFFICE_TRANSFER_RULE_UR_PICKUP",
    "OFFICE_TRANSFER_RULE_UR_WAYBILL", "OFFICE_TRANSFER_RULE_UR_PREORDER",
    "OFFICE_TRANSFER_RULE_UR_RESERVE",
    "OFFICE_TRANSFER_RULE_ZNR_WAITLIST",
    "OFFICE_TRANSFER_RULE_ZNR_ACADEMY", "OFFICE_TRANSFER_RULE_UR_POST",
    "OFFICE_TRANSFER_RULE_ZNR_OPT",
):
    setattr(office_transfer, _flag, True)


# ── 1) матчеры правил: позитив + негатив по каждому условию ─────────────────

# УР-1 Достависта (курьер по Москве)
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")


async def _fake_contact_no_leads(cid, with_=()):
    return {"id": cid, "_embedded": {"leads": []}}


async def _fake_contact_only_current(cid, with_=()):
    return {"id": cid, "_embedded": {"leads": [{"id": 42}]}}  # только сама текущая сделка


async def _fake_contact_other_lead(cid, with_=()):
    return {"id": cid, "_embedded": {"leads": [{"id": 42}, {"id": 999}]}}


# ── 2) диспетчер: guard'ы + идемпотентность ─────────────────────────────────

_patches: list = []
_tags: list = []
_notes: list = []
_alerts: list = []
_removed_tags: list = []
_user_names = {999: "Иван Иванов"}


def _install_dispatcher_mocks(lead):
    async def fake_get_lead_full(lid, with_=()):
        return lead

    async def fake_patch_lead(lid, **kw):
        _patches.append({"lead_id": lid, **kw})
        return {"ok": True, "status_code": 200}

    async def fake_add_tag(lid, name):
        _tags.append((lid, name))
        return {"ok": True}

    async def fake_add_note(lid, text):
        _notes.append((lid, text))
        return {"ok": True}

    async def fake_get_user_name(uid):
        return _user_names.get(uid)

    async def fake_send_alert(text, *a, **kw):
        _alerts.append(text)
        return True

    async def fake_remove_tag(lid, name, *, lead=None):
        _removed_tags.append((lid, name))
        return {"ok": True}

    office_transfer.amo_service.get_lead_full = fake_get_lead_full
    office_transfer.amo_service.patch_lead = fake_patch_lead
    office_transfer.amo_service.add_tag = fake_add_tag
    office_transfer.amo_service.add_note = fake_add_note
    office_transfer.amo_service.get_user_name = fake_get_user_name
    office_transfer.amo_service.remove_tag = fake_remove_tag
    office_transfer.telegram_bot.send_alert = fake_send_alert


def _reset():
    _patches.clear()
    _tags.clear()
    _notes.clear()
    _alerts.clear()
    _removed_tags.clear()
    office_transfer._pending_fail.clear()
    office_transfer._fill_alerted.clear()


async def _fake_get_lead_full(lid, with_=()):
    return lead


async def _failing_patch(lid, **kw):
    return {"ok": False, "status_code": 500, "retryable": True}


# ── 5) reconciliation: cutover-окно (без ретроактивности) ───────────────────

_events_requests: list = []


async def _fake_do_get(path, params=None):
    _events_requests.append((path, dict(params or [])))
    return {"_embedded": {"events": []}}

# ── 5б) reconciliation в окне миграции: поток прогона отсеян, боевое живо ──
# (05.08.2026: раньше проход целиком выключался флагом, и заказы стояли до ночи)

_LEGACY_PIPELINE = 901105


def _status_event(entity_id, before_pipeline, after_status):
    return {
        "entity_id": entity_id,
        "value_before": [{"lead_status": {"id": 12345, "pipeline_id": before_pipeline}}],
        "value_after": [{"lead_status": {"id": after_status, "pipeline_id": PIPELINE_CLEVER_MAIN}}],
    }


async def _fake_do_get_mixed(path, params=None):
    _events_requests.append((path, dict(params or [])))
    after = int(dict(params or [])["filter[value_after][leads_statuses][0][status_id]"])
    if after != STATUS_SUCCESS:
        return {"_embedded": {"events": []}}
    return {"_embedded": {"events": [
        _status_event(101, _LEGACY_PIPELINE, STATUS_SUCCESS),      # привёз прогон
        _status_event(102, PIPELINE_CLEVER_MAIN, STATUS_SUCCESS),  # закрыл менеджер
        _status_event(103, _LEGACY_PIPELINE, STATUS_SUCCESS),      # привёз прогон
    ]}}


_processed_by_reconcile: list = []


async def _fake_process(lead_id, source="webhook"):
    _processed_by_reconcile.append(int(lead_id))
    return "moved"

# ── 5в) потолок оглядки: после рестарта окно не разворачивается на неделю ──
# (05.08.2026: первый же проход после выката перебрал 23 224 события миграции)

import time as _time  # noqa: E402

# reconciliation обходит ОБЕ воронки: 4 запроса (2 воронки × 2 статуса)
_opt_reconcile_requests: list = []


async def _fake_do_get_two_sources(path, params=None):
    p = dict(params or [])
    _opt_reconcile_requests.append((
        int(p["filter[value_after][leads_statuses][0][pipeline_id]"]),
        int(p["filter[value_after][leads_statuses][0][status_id]"]),
    ))
    return {"_embedded": {"events": []}}


# ── 9) PAID до переноса: синки зовутся по ещё-CLEVER состоянию, до PATCH ──

import metrika_sync
import woo_status_sync

_call_order: list = []


async def _fake_metrika_ps(payload, lead=None):
    _call_order.append(("metrika", lead.get("pipeline_id"), lead.get("status_id")))


async def _fake_woo_ps(payload, lead=None):
    _call_order.append(("woo", lead.get("pipeline_id"), lead.get("status_id")))


async def _patch_marks(lid, **kw):
    _call_order.append(("patch", None, None))
    _patches.append({"lead_id": lid, **kw})
    return {"ok": True, "status_code": 200}


async def _boom(payload, lead=None):
    raise RuntimeError("метрика упала")


async def _fake_find_empty(q, with_=()):
    return []


async def _fake_find_hit(q, with_=()):
    return [clever_orig]

# ══════════ картотека «Работа с базой»: перенос только на УР ══════════
# Постановка Кати 07.09.2026: «при УР должно происходить всё то же, что в ОП».
# А вот ЗНР картотеке запрещён — карточка обзвона обязана остаться на месте.
from waybill_config import PIPELINE_DB_WORK  # noqa: E402
_contact_calls: list = []

async def _count_contact(cid, with_=()):
    _contact_calls.append(cid)
    return {"id": cid, "_embedded": {"leads": [{"id": 777}]}}

# ⚠️ Сторож: расширение _classify НЕ включает отправку в Метрику — у неё свой
# гейт воронок раньше по коду. Катя просила Метрику не трогать.
_ym_rows: list = []

async def _catch_upload(counter_id, row, **kw):
    _ym_rows.append(row)
    return True


async def _fake_contact_info(lead):
    return 5001, "a@b.c", "+79990000000"


def _ym_lead(pipeline_id):
    lead = _lead(pipeline_id=pipeline_id, status_id=STATUS_SUCCESS)
    lead["custom_fields_values"].append(_cf(metrika_sync.FIELD_PAYMENT_METHOD, value="Онлайн-оплата"))
    lead["custom_fields_values"].append(_cf(metrika_sync.FIELD_YM_CLIENT_ID, value="1700000000000000000"))
    lead["created_at"] = int(time.time())
    return lead

# ══════════ TangemShop: перенос только на УР ══════════
# ТЗ 29.09.2026 по запуску заказов магазина tangemshop.ru через amoCRM. Прямые слова
# Кати: «УР переводит сделку в офис, ЗИН закрывает её без перехода в офис».
from waybill_config import (  # noqa: E402
    ENUM_SALES_CHANNEL_TANGEMSHOP,
    FIELD_SALES_CHANNEL,
    FIELD_SITE_ORDER_NUMBER,
    PIPELINE_TANGEMSHOP,
    is_tangemshop_lead,
)


async def _find_no_sibling(query, with_=()):
    """Оригинала в рознице нет — штатный случай после перехода на ПЕРЕНОС.
    Ровно тут _resolve_clever и возвращает саму сделку, из-за чего заказ чужого
    магазина доезжал бы до отправки."""
    return []


def _office_lead(lead_id: int, site_number: str) -> dict:
    """Заказ, закрытый как УР и уехавший в Офис.

    ⚠️ Две обязательные мелочи, на каждой из которых сторож молча становится
    ложно-зелёным:
      • UUID заказа МойСклада - без него `_resolve_clever` выходит РАНЬШЕ признака
        магазина, и пустой результат означал бы совсем другую причину;
      • свой id у каждой сделки - у Метрики есть дедуп по состоянию заказа, и
        вторая отправка того же id не уходит вовсе.
    """
    lead = _ym_lead(PIPELINE_OFFICE)
    lead["id"] = lead_id
    lead["custom_fields_values"].append(
        _cf(metrika_sync.FIELD_MOYSKLAD_ORDER_UUID, value=f"0e5aa71e-c413-11ee-0a80-{lead_id:012d}"))
    lead["custom_fields_values"].append(_cf(FIELD_SITE_ORDER_NUMBER, value=site_number))
    return lead

# ── мина 2: заказ Tangemshop не стучится в WooCommerce ──────────────────────
# На статусе completed висит начисление реферальной комиссии Easy Affiliate.
import woo_status_sync  # noqa: E402





def test_ur_1_dostavista_matching_vernyy():
    """УР-1 Достависта: матчинг верный"""
    assert office_transfer._match_ur_delivery(lead) == (PIPELINE_OFFICE, STATUS_OFFICE_DELIVERY)
    lead2 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="CDEK: Посылка склад-дверь")
    assert office_transfer._match_ur_delivery(lead2) is None, "другой тип доставки — не матчит"
    lead3 = _lead(application_type=APPLICATION_TYPE_PREORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="Доставка курьером по Москве")
    assert office_transfer._match_ur_delivery(lead3) is None, "предзаказ — не матчит (нужен Заказ)"
    lead4 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_ERMS_MAIN,
                  delivery_text="Доставка курьером по Москве")
    assert office_transfer._match_ur_delivery(lead4) is None, "чужой склад — не матчит"
    # новое имя своей курьерки (переименование 23.09.2026)
    lead5 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="Курьерская доставка")
    assert office_transfer._match_ur_delivery(lead5) == (PIPELINE_OFFICE, STATUS_OFFICE_DELIVERY)
    # ⚠️ мина: у курьерки СДЭК та же подстрока «курьерская доставка», но ехать ей на
    # «Сделать накладную», а не на «Оформить доставку»
    lead6 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="СДЭК: Курьерская доставка")
    assert office_transfer._match_ur_delivery(lead6) is None, "курьерка СДЭК — не наша доставка"
    assert office_transfer._match_ur_waybill(lead6) == (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL),     "курьерка СДЭК должна ехать на «Сделать накладную»"
    lead7 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="СДЭК: Доставка в постамат")
    assert office_transfer._match_ur_delivery(lead7) is None, "постамат СДЭК — не наша доставка"


def test_ur_2_samovyvoz_diskriminator_iz_ofisa_protiv_cdek_samovyvo():
    """УР-2 Самовывоз: дискриминатор «из офиса» против CDEK: Самовывоз работает"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # УР-2 Самовывоз (дискриминатор «из офиса» против «CDEK: Самовывоз»)
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_OPENED,
                 delivery_text="Самовывоз из офиса Sunscrypt")
    assert office_transfer._match_ur_pickup(lead) == (PIPELINE_OFFICE, STATUS_SUCCESS), "самовывоз = сразу УР Офиса (Катя 31.07)"
    lead2 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_OPENED,
                  delivery_text="CDEK: Самовывоз")
    assert office_transfer._match_ur_pickup(lead2) is None


def test_ur_2_isklyuchenie_opt_shourum_rezerv_tolko_dlya_opt_roznic():
    """УР-2 исключение ОПТ+шоурум: резерв только для ОПТ, розница и «самовывоз из офиса» — без изменений"""
    # УР-2 исключение: ОПТ + самовывоз из ШОУРУМА + Заказ → «Отложенный/резерв товар», не УР(142)
    lead_opt_showroom = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
                               warehouse=WAREHOUSE_SUNSCRYPT_OPENED, delivery_text="Самовывоз из шоурума")
    assert office_transfer._match_ur_pickup(lead_opt_showroom) == (PIPELINE_OFFICE, STATUS_OFFICE_RESERVE), \
        "ОПТ + самовывоз из шоурума = резерв, а не УР (Катя 10.08)"
    # розница с тем же маркером «из шоурума» — как раньше, УР(142) (правило только для ОПТ)
    lead_retail_showroom = _lead(pipeline_id=PIPELINE_CLEVER_MAIN, application_type=APPLICATION_TYPE_ORDER,
                                  warehouse=WAREHOUSE_SUNSCRYPT_OPENED, delivery_text="Самовывоз из шоурума")
    assert office_transfer._match_ur_pickup(lead_retail_showroom) == (PIPELINE_OFFICE, STATUS_SUCCESS), \
        "розница + самовывоз из шоурума — без изменений, сразу УР"
    # ОПТ + самовывоз ИЗ ОФИСА (не шоурум) — тоже без изменений, сразу УР
    lead_opt_office = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
                             warehouse=WAREHOUSE_SUNSCRYPT_OPENED, delivery_text="Самовывоз из офиса Sunscrypt")
    assert office_transfer._match_ur_pickup(lead_opt_office) == (PIPELINE_OFFICE, STATUS_SUCCESS), \
        "ОПТ + самовывоз из офиса (не шоурум) — исключение не применяется"


def test_ur_3_sdek_shlopnutye_3_6_7_registronezavisimo_obe_formy_cd():
    """УР-3 СДЭК (схлопнутые 3+6+7): регистронезависимо, обе формы CDEK/СДЭК"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead, text
    # УР-3 СДЭК (схлопнутые правила 3+6+7 исходного списка) — регистронезависимо, CDEK/СДЭК
    for text in ("CDEK: Посылка склад-дверь", "сдэк: самовывоз", "Доставка СДЭК курьером"):
        lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                     delivery_text=text)
        assert office_transfer._match_ur_waybill(lead) == (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL), text
    lead_no = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                    delivery_text="Достависта курьер")
    assert office_transfer._match_ur_waybill(lead_no) is None


def test_ur_4_predzakaz():
    """УР-4 Предзаказ"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # УР-4 Предзаказ — условие только по Тип заявки
    lead = _lead(application_type=APPLICATION_TYPE_PREORDER)
    assert office_transfer._match_ur_preorder(lead) == (PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID)
    lead2 = _lead(application_type=APPLICATION_TYPE_ORDER)
    assert office_transfer._match_ur_preorder(lead2) is None


def test_ur_6_rezerv_tip_dostavki_ne_smotrim_vedet_v_otlozhennyy_re():
    """УР-6 Резерв: тип доставки не смотрим, ведёт в «Отложенный/резерв товар»"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # УР-6 Резерв — условие только по Тип заявки, тип доставки НЕ смотрим (Катя 25.08.2026)
    lead = _lead(application_type=APPLICATION_TYPE_RESERVE)
    assert office_transfer._match_ur_reserve(lead) == (PIPELINE_OFFICE, STATUS_OFFICE_RESERVE)
    lead2 = _lead(application_type=APPLICATION_TYPE_RESERVE, delivery_text="CDEK: Самовывоз")
    assert office_transfer._match_ur_reserve(lead2) == (PIPELINE_OFFICE, STATUS_OFFICE_RESERVE), (
        "тип доставки любой — не влияет на маршрут")
    lead3 = _lead(application_type=APPLICATION_TYPE_ORDER)
    assert office_transfer._match_ur_reserve(lead3) is None, "Заказ — не Резерв, не матчит"


def test_ur_erms_ne_matchitsya_ni_odnim_pravilom_roznichnyy_sklad_m():
    """УР(ЭРМС): не матчится ни одним правилом, розничный склад матчится"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # УР(ЭРМС): маршрута в Фулфилмент БОЛЬШЕ НЕТ (воронка разобрана 05.08.2026).
    # ⚠️ Этот блок был выпотрошен вместе с правилом: остались две присвоенные сделки
    # и печать «✓», а проверок — НИ ОДНОЙ, то есть галочка врала. Возвращаем смысл:
    # ЭРМС-склад не должен матчиться ни одним правилом, розничный склад — должен.
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_ERMS_MAIN,
                 delivery_text="СДЭК до ПВЗ")
    assert run(office_transfer._match_rules(lead, STATUS_SUCCESS)) is None, (
        "ЭРМС больше не маршрут: после выпила Фулфилмента правила его не берут")
    lead2 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="СДЭК до ПВЗ")
    assert run(office_transfer._match_rules(lead2, STATUS_SUCCESS)) == (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL), (
        "склад — единственное отличие от предыдущей сделки, розничный обязан матчиться")


def test_znr_list_ozhidaniya():
    """ЗНР Лист ожидания"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # ЗНР Лист ожидания
    lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_WAITLIST)
    assert office_transfer._match_znr_waitlist(lead) == (PIPELINE_WAITLIST, STATUS_WAITLIST)
    lead2 = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_ACADEMY)
    assert office_transfer._match_znr_waitlist(lead2) is None


def test_znr_akademiya():
    """ЗНР Академия"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # ЗНР Академия
    lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_ACADEMY)
    assert office_transfer._match_znr_academy(lead) == (PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT)
    lead2 = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_WAITLIST)
    assert office_transfer._match_znr_academy(lead2) is None


def test_znr_opt_novyy_kontakt_pervichnyy_kontakt_povtornyy_nayden():
    """ЗНР Опт: новый контакт → Первичный контакт, повторный → Найден контакт"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # ЗНР Опт — новый контакт → Первичный контакт, повторный → Найден контакт
    _orig_get_contact = office_transfer.amo_service.get_contact_by_id
    lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_OPT)
    lead["_embedded"] = {"contacts": [{"id": 501}]}
    office_transfer.amo_service.get_contact_by_id = _fake_contact_no_leads
    assert run(office_transfer._match_znr_opt(lead)) == (PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT), (
        "новый контакт, других сделок нет — Первичный контакт")
    office_transfer.amo_service.get_contact_by_id = _fake_contact_only_current
    assert run(office_transfer._match_znr_opt(lead)) == (PIPELINE_OPT, STATUS_OPT_PRIMARY_CONTACT), (
        "у контакта в списке только ТЕКУЩАЯ сделка (id=42) — не считается «другой», Первичный контакт")
    office_transfer.amo_service.get_contact_by_id = _fake_contact_other_lead
    assert run(office_transfer._match_znr_opt(lead)) == (PIPELINE_OPT, STATUS_OPT_CONTACT_FOUND), (
        "есть ДРУГАЯ сделка (id=999) — Найден контакт")
    lead_wrong_reason = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_WAITLIST)
    lead_wrong_reason["_embedded"] = {"contacts": [{"id": 501}]}
    assert run(office_transfer._match_znr_opt(lead_wrong_reason)) is None, "причина не Опт — не матчит"
    # правило выключено флагом — не матчит, даже если условия подходят
    office_transfer.OFFICE_TRANSFER_RULE_UR_DELIVERY = False
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве")
    assert office_transfer._match_ur_delivery(lead) is None
    office_transfer.OFFICE_TRANSFER_RULE_UR_DELIVERY = True
    # guard: сделка не в CLEVER — skip, ничего не пишем (идемпотентность: уже
    # перенесённая сделка при повторном вызове не PATCH-ится второй раз)
    _reset()
    lead = _lead(pipeline_id=PIPELINE_OFFICE, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="Доставка курьером по Москве")
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-not-applicable", res
    assert not _patches
    # guard: статус не в {142,143} — skip
    _reset()
    lead = _lead(status_id=83537714, pipeline_id=PIPELINE_CLEVER_MAIN)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-not-applicable", res
    assert not _patches
    # подходящая сделка переносится одним PATCH
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве", responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert len(_patches) == 1, _patches
    assert _patches[0]["pipeline_id"] == PIPELINE_OFFICE
    assert _patches[0]["status_id"] == STATUS_OFFICE_DELIVERY
    # Резерв: сквозной прогон через диспетчер, склад/доставка не заданы (не участвуют)
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_RESERVE, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert len(_patches) == 1, _patches
    assert _patches[0]["pipeline_id"] == PIPELINE_OFFICE
    assert _patches[0]["status_id"] == STATUS_OFFICE_RESERVE
    assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OFFICE_MANAGER_USER_ID, (
        "Резерв едет в Офис — ответственный меняется на Зубалий, как у остальных правил Офиса")
    # нет ни одного правила (чужой склад + нераспознанная доставка) — с 31.07.2026
    # это алерт «заказ заполнен некорректно»: тег + примечание + ТГ, PATCH не шлём
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=999999, delivery_text="непонятно что")
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match-bad-fill", res
    assert not _patches
    assert (42, TAG_BAD_FILL) in _tags, _tags
    assert len(_notes) == 1 and len(_alerts) == 1, (_notes, _alerts)
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве", responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    run(office_transfer.process_office_transfer(42))
    assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OFFICE_MANAGER_USER_ID, _patches
    assert _patches[0]["custom_fields"][FIELD_FORMER_RESPONSIBLE] == "Иван Иванов", _patches
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве",
                 responsible_user_id=RESPONSIBLE_OFFICE_MANAGER_USER_ID)
    _install_dispatcher_mocks(lead)
    run(office_transfer.process_office_transfer(42))
    assert "responsible_user_id" not in _patches[0], _patches
    assert "custom_fields" not in _patches[0], _patches
    _reset()
    # ЭРМС БОЛЬШЕ НЕ МАРШРУТ (Фулфилмент разобран 05.08.2026, правило убрано из
    # _UR_RULES). Раньше здесь проверялось «перенос в ФФ не меняет ответственного»,
    # но переноса не стало — блок ждал _patches[0] и падал с IndexError, а весь файл
    # был красным начиная с выпила ФФ. Фиксируем ФАКТИЧЕСКОЕ поведение: заказ с
    # ЭРМС-склада не подходит ни под одно правило и уходит в алерт заполнения.
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_ERMS_MAIN, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match-bad-fill", res
    assert not _patches, "ЭРМС больше никуда не переносится — PATCH быть не должно"
    assert (42, TAG_BAD_FILL) in _tags, _tags
    _reset()
    lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_WAITLIST, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    run(office_transfer.process_office_transfer(42))
    assert "responsible_user_id" not in _patches[0], _patches
    _reset()
    lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_ACADEMY, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    run(office_transfer.process_office_transfer(42))
    assert "responsible_user_id" not in _patches[0], _patches
    _reset()
    lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_OPT, responsible_user_id=999)
    lead["_embedded"] = {"contacts": [{"id": 501}]}
    _install_dispatcher_mocks(lead)
    office_transfer.amo_service.get_contact_by_id = _fake_contact_no_leads
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches[0]["pipeline_id"] == PIPELINE_OPT
    assert _patches[0]["status_id"] == STATUS_OPT_PRIMARY_CONTACT
    assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OPT_MANAGER_USER_ID, (
        "перенос в ОПТ по причине Опт — ответственный меняется на Артёма Коннова")
    assert "custom_fields" not in _patches[0], "578151 не трогаем при переносе в ОПТ (только для Офиса)"
    office_transfer.amo_service.get_contact_by_id = _orig_get_contact
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве")
    office_transfer.amo_service.get_lead_full = _fake_get_lead_full
    office_transfer.amo_service.patch_lead = _failing_patch
    res = run(office_transfer.process_office_transfer(42))
    assert res == "failed-patch", res
    assert _tags and _tags[0][1] == TAG_OFFICE_TRANSFER_ERROR, _tags
    assert _notes and "не выполнен" in _notes[0][1], _notes
    assert not _alerts, "порог ещё не наступил — алерта быть не должно"


def test_zavisshaya_sdelka_odin_alert_po_istechenii_poroga_dedup_na():
    """зависшая сделка: один алерт по истечении порога, дедуп на повторных проходах"""
    # состарили неудачу за порог (default OFFICE_TRANSFER_STALE_ALERT_MIN=30 мин) → один алерт
    office_transfer._pending_fail[42]["since"] -= 3600
    run(office_transfer.process_office_transfer(42))
    assert len(_alerts) == 1, _alerts
    run(office_transfer.process_office_transfer(42))
    assert len(_alerts) == 1, "повторный алерт по той же сделке быть не должен (дедуп)"


def test_bez_office_transfer_since_ts_reconciliation_ne_zapuskaetsy():
    """без OFFICE_TRANSFER_SINCE_TS reconciliation не запускается"""
    _reset()
    office_transfer.amo_service._do_get = _fake_do_get
    # без заданного cutover — проход пропускается целиком (защита от случайного
    # запуска reconciliation без границы — задело бы старые досделочные сделки)
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 0
    office_transfer._last_reconcile_ts = 0
    res = run(office_transfer._reconcile_once())
    assert res == "skipped-no-cutover", res
    assert not _events_requests


def test_reconciliation_okno_ne_ranshe_office_transfer_since_ts_bez():
    """reconciliation: окно не раньше OFFICE_TRANSFER_SINCE_TS (без ретроактивности)"""
    # с заданным cutover — окно уходит в /api/v4/events не раньше границы
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 1000
    office_transfer._last_reconcile_ts = 0
    run(office_transfer._reconcile_once())
    assert _events_requests, "reconcile должен был сходить в /api/v4/events"
    for path, params in _events_requests:
        assert path == "/api/v4/events"
        assert int(params["filter[created_at][from]"]) >= 1000, params


def test_reconciliation_v_okne_migracii_sdelki_progona_otseyany_boe():
    """reconciliation в окне миграции: сделки прогона отсеяны, боевая обработана"""
    _orig_process = office_transfer.process_office_transfer
    office_transfer.amo_service._do_get = _fake_do_get_mixed
    office_transfer.process_office_transfer = _fake_process
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 1000
    office_transfer._last_reconcile_ts = 0
    # окно миграции ОТКРЫТО, воронка-источник в списке
    _mf = office_transfer.migration_freeze
    _mf_backup = (_mf.MIGRATION_BULK_PAUSE, _mf.MIGRATION_FREEZE_TAG, _mf.MIGRATION_FREEZE_FROM_TS,
                  _mf.MIGRATION_FREEZE_TO_TS, _mf.MIGRATION_SOURCE_PIPELINES)
    _mf.MIGRATION_BULK_PAUSE = True
    _mf.MIGRATION_FREEZE_TAG = "перенесено из старой воронки"
    _mf.MIGRATION_FREEZE_FROM_TS = 1
    _mf.MIGRATION_FREEZE_TO_TS = 99_999_999_999
    _mf.MIGRATION_SOURCE_PIPELINES = {_LEGACY_PIPELINE}
    res = run(office_transfer._reconcile_once())
    assert res == "processed=1", res
    assert _processed_by_reconcile == [102], _processed_by_reconcile
    # то же окно, но воронки-источника в списке НЕТ — обрабатываем всех,
    # лучше лишняя работа, чем потерянный заказ
    _mf.MIGRATION_SOURCE_PIPELINES = set()
    _processed_by_reconcile.clear()
    office_transfer._last_reconcile_ts = 0
    res = run(office_transfer._reconcile_once())
    assert res == "processed=3", res
    assert sorted(_processed_by_reconcile) == [101, 102, 103], _processed_by_reconcile
    (_mf.MIGRATION_BULK_PAUSE, _mf.MIGRATION_FREEZE_TAG, _mf.MIGRATION_FREEZE_FROM_TS,
     _mf.MIGRATION_FREEZE_TO_TS, _mf.MIGRATION_SOURCE_PIPELINES) = _mf_backup
    office_transfer.process_office_transfer = _orig_process
    office_transfer.amo_service._do_get = _fake_do_get
    office_transfer._last_reconcile_ts = 0
    _events_requests.clear()
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 1000  # cutover глубоко в прошлом
    office_transfer._last_reconcile_ts = 0           # как после рестарта
    run(office_transfer._reconcile_once())
    _now = int(_time.time())
    for path, params in _events_requests:
        _from = int(params["filter[created_at][from]"])
        assert _from >= _now - office_transfer.RECONCILE_MAX_LOOKBACK_S - 5, (_from, _now)


def test_potolok_oglyadki_ne_otmenyaet_proverku_cutover():
    """потолок оглядки не отменяет проверку cutover"""
    # защита «без cutover не запускаться» потолок не сломал
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 0
    office_transfer._last_reconcile_ts = 0
    _events_requests.clear()
    res = run(office_transfer._reconcile_once())
    assert res == "skipped-no-cutover", res
    assert not _events_requests


def test_cutover_geyt_zakrytaya_do_vklyucheniya_sdelka_ne_perenosit():
    """cutover-гейт: закрытая до включения сделка не переносится (даже вебхуком)"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    office_transfer._last_reconcile_ts = 0
    _events_requests.clear()
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 0
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 1_700_000_000
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве")
    lead["closed_at"] = 1_699_999_999  # вход в УР ДО cutover
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-pre-cutover", res
    assert not _patches and not _tags and not _alerts


def test_cutover_geyt_vhod_posle_vklyucheniya_perenositsya_shtatno():
    """cutover-гейт: вход после включения переносится штатно"""
    lead["closed_at"] = 1_700_000_001  # свежий вход ПОСЛЕ cutover
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res


def test_cutover_geyt_since_ts_0_ne_zadan_geyt_vyklyuchen():
    """cutover-гейт: SINCE_TS=0 (не задан) — гейт выключен"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    office_transfer.OFFICE_TRANSFER_SINCE_TS = 0
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве")
    lead["closed_at"] = 1_600_000_000
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res


def test_578151_zapolnen_rukami_sdelka_vse_ravno_uezzhaet_v_ofis():
    """578151 заполнен руками → сделка всё равно уезжает в Офис"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве")
    lead["custom_fields_values"].append(_cf(FIELD_FORMER_RESPONSIBLE, value="Оанча Игорь"))
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches and _patches[0]["pipeline_id"] == PIPELINE_OFFICE, _patches


def test_pochta_rossii_ofis_sdelat_nakladnuyu_registr_hvosty_negati():
    """Почта России → Офис/Сделать накладную (регистр, хвосты, негативы)"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Почта России")
    assert office_transfer._match_ur_post(lead) == (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL)
    lead2 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_OPENED,
                  delivery_text="почта россии, 1 шт, 350.00 рублей")
    assert office_transfer._match_ur_post(lead2) == (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL), "регистр/хвост"
    lead3 = _lead(application_type=APPLICATION_TYPE_PREORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="Почта России")
    assert office_transfer._match_ur_post(lead3) is None, "предзаказ — не матчит"
    lead4 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_ERMS_MAIN,
                  delivery_text="Почта России")
    assert office_transfer._match_ur_post(lead4) is None, "чужой склад — не матчит"
    lead5 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                  delivery_text="CDEK: Посылка склад-дверь")
    assert office_transfer._match_ur_post(lead5) is None, "СДЭК — не почта"


def test_zakaz_sklad_bez_tipa_dostavki_teg_dostavka_ne_zapolnena_al():
    """Заказ+склад без «Типа доставки» → тег «доставка не заполнена» + алерт"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # Заказ + склад на месте, доставка пустая → «доставка не заполнена»
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match-no-delivery", res
    assert (42, TAG_NO_DELIVERY) in _tags, _tags
    assert len(_alerts) == 1 and "Типа доставки" in _alerts[0], _alerts
    assert not _patches


def test_povtornyy_prohod_po_toy_zhe_sdelke_alert_ne_dubliruetsya():
    """повторный проход по той же сделке → алерт не дублируется"""
    # повторный вызов по той же сделке → дедуп, второго алерта нет
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-already-alerted", res
    assert len(_alerts) == 1, _alerts


def test_dedup_po_tegu_na_sdelke_perezhivaet_restart_processa():
    """дедуп по тегу на сделке (переживает рестарт процесса)"""
    # дедуп переживает рестарт: set пуст, но тег уже на сделке
    office_transfer._fill_alerted.clear()
    lead_tagged = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN)
    lead_tagged["_embedded"] = {"tags": [{"id": 1, "name": TAG_NO_DELIVERY}]}
    _install_dispatcher_mocks(lead_tagged)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-already-alerted", res
    assert len(_alerts) == 1, _alerts


def test_musornaya_ur_sdelka_vse_pusto_teg_zakaz_zapolnen_nekorrekt():
    """мусорная УР-сделка (всё пусто) → тег «заказ заполнен некорректно» + алерт"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # пустая доставка, но ЗАКАЗА нет (мусор) → «заказ заполнен некорректно»
    _reset()
    lead = _lead()  # ни типа заявки, ни склада, ни доставки
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match-bad-fill", res
    assert (42, TAG_BAD_FILL) in _tags, _tags
    assert len(_alerts) == 1, _alerts


def test_sdelka_pod_vyklyuchennym_pravilom_tihiy_skip_bez_lozhnogo():
    """сделка под выключенным правилом → тихий скип, без ложного алерта"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # сделка подошла бы под ВЫКЛЮЧЕННОЕ правило → молчим (её ведёт нативка)
    _reset()
    office_transfer.OFFICE_TRANSFER_RULE_UR_DELIVERY = False
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве")
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match-rule-disabled", res
    assert not _tags and not _alerts and not _patches, (_tags, _alerts)
    office_transfer.OFFICE_TRANSFER_RULE_UR_DELIVERY = True


def test_znr_s_obychnoy_prichinoy_tiho_ostaetsya_v_clever():
    """ЗНР с обычной причиной → тихо остаётся в CLEVER"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # ЗНР без спец-причины → тихий no-match, без тегов/алертов
    _reset()
    lead = _lead(status_id=STATUS_CLOSED_LOST, reason=1041147)  # «Пропал»
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match", res
    assert not _tags and not _alerts, (_tags, _alerts)


def test_polya_dozapolneny_perenos_teg_dostavka_ne_zapolnena_snyat():
    """поля дозаполнены → перенос + тег «доставка не заполнена» снят"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # менеджер дозаполнил поля после алерта → перенос + снятие тега
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Доставка курьером по Москве")
    lead["_embedded"] = {"tags": [{"id": 1, "name": TAG_NO_DELIVERY}]}
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert (42, TAG_NO_DELIVERY) in _removed_tags, _removed_tags


def test_opt_flag_vyklyuchen_sdelka_ne_trogaetsya_i_ne_alertit():
    """ОПТ: флаг выключен → сделка не трогается и не алертит"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    assert office_transfer.OFFICE_TRANSFER_SOURCE_OPT is False, (
        "флаг ОПТ обязан быть выключен по умолчанию — включается только руками, "
        "после снятия ручного копирования в ОПТ")
    # флаг ВЫКЛЮЧЕН → опт-сделка не наша, даже если по полям подошла бы идеально
    _reset()
    lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ")
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-not-applicable", res
    assert not _patches, _patches
    assert not _tags, "выключенный источник не должен алертить: копии ведёт нативка"


def test_opt_geyt_istochnika_po_flagu_stroka_i_musor_obrabotany():
    """ОПТ: гейт источника по флагу, строка и мусор обработаны"""
    assert office_transfer.is_source_pipeline(PIPELINE_CLEVER_MAIN) is True
    assert office_transfer.is_source_pipeline(PIPELINE_OPT) is False
    assert office_transfer.is_source_pipeline(None) is False, "мусор на входе гейта — не источник"
    office_transfer.OFFICE_TRANSFER_SOURCE_OPT = True
    assert office_transfer.is_source_pipeline(PIPELINE_OPT) is True
    assert office_transfer.is_source_pipeline(str(PIPELINE_OPT)) is True, (
        "вебхук отдаёт pipeline_id строкой — гейт обязан её понимать")
    assert office_transfer.is_source_pipeline(PIPELINE_OFFICE) is False, (
        "Офис — ЦЕЛЬ переноса, не источник: иначе перенесённая сделка поехала бы по кругу")


def test_opt_142_sdek_ofis_sdelat_nakladnuyu_otvetstvennyy_zubaliy():
    """ОПТ/142 СДЭК → Офис/«Сделать накладную», ответственный → Зубалий, прежний в 578151"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # флаг ВКЛЮЧЁН → опт-заказ едет тем же маршрутом, что розничный с той же доставкой
    _reset()
    lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ",
                 responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert len(_patches) == 1, _patches
    assert _patches[0]["pipeline_id"] == PIPELINE_OFFICE
    assert _patches[0]["status_id"] == STATUS_CREATE_WAYBILL, (
        "опт-СДЭК обязан ехать в тот же этап, что розничный СДЭК")
    # ответственный: как в рознице — Зубалий, прежний в 578151
    assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OFFICE_MANAGER_USER_ID
    assert _patches[0]["custom_fields"][FIELD_FORMER_RESPONSIBLE] == "Иван Иванов"


def test_opt_142_samovyvoz_iz_shouruma_ofis_otlozhennyy_rezerv_tova():
    """ОПТ/142 самовывоз из шоурума → Офис/«Отложенный/резерв товар» (исключение из общего маршрута)"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # опт-самовывоз из ШОУРУМА → «Отложенный/резерв товар», НЕ тот же маршрут, что розница (Катя 10.08)
    _reset()
    lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="Самовывоз из шоурума",
                 responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches[0]["pipeline_id"] == PIPELINE_OFFICE
    assert _patches[0]["status_id"] == STATUS_OFFICE_RESERVE, (
        "ОПТ+самовывоз из шоурума обязан ехать в резерв, а не в УР(142)")
    assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OFFICE_MANAGER_USER_ID


def test_opt_142_predzakaz_ofis_predzakaz_oplachen():
    """ОПТ/142 предзаказ → Офис/«Предзаказ оплачен»"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # опт-предзаказ → «Предзаказ оплачен» (ровно то, что делала ручная копия 03.08)
    _reset()
    lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_PREORDER,
                 responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches[0]["status_id"] == STATUS_OFFICE_PREORDER_PAID, _patches


def test_opt_143_list_ozhidaniya_akademiya_otvetstvennyy_ne_menyaet():
    """ОПТ/143 → Лист ожидания / Академия, ответственный не меняется"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # ЗНР из ОПТ: Лист ожидания и Академия — как в рознице, ответственный НЕ меняется
    for _reason, _pipe, _stat, _label in (
        (REASON_WAITLIST, PIPELINE_WAITLIST, STATUS_WAITLIST, "Лист ожидания"),
        (REASON_ACADEMY, PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT, "Академия"),
    ):
        _reset()
        lead = _lead(pipeline_id=PIPELINE_OPT, status_id=STATUS_CLOSED_LOST,
                     reason=_reason, responsible_user_id=999)
        _install_dispatcher_mocks(lead)
        res = run(office_transfer.process_office_transfer(42))
        assert res == "moved", (res, _label)
        assert _patches[0]["pipeline_id"] == _pipe, (_patches, _label)
        assert _patches[0]["status_id"] == _stat, (_patches, _label)
        assert "responsible_user_id" not in _patches[0], (
            f"перенос в «{_label}» не должен менять ответственного")


def test_opt_dostavka_vne_pyati_pravil_alert_naugad_ne_perenosim():
    """ОПТ: доставка вне пяти правил → алерт, наугад не переносим"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # опт-сделка мимо всех правил (у опта своя логистика — например фура) → алерт,
    # PATCH не шлём. Это принятое следствие решения «те же пять правил».
    _reset()
    lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="транспортная компания")
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match-bad-fill", res
    assert not _patches, "наугад не переносим"
    assert (42, TAG_BAD_FILL) in _tags, _tags


def test_opt_578151_zapolnen_sdelka_vse_ravno_uezzhaet_v_ofis():
    """ОПТ: 578151 заполнен → сделка всё равно уезжает в Офис"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    # и для опта заполненный 578151 перенос не останавливает
    _reset()
    lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ")
    lead["custom_fields_values"].append(_cf(FIELD_FORMER_RESPONSIBLE, value="Иван Иванов"))
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches and _patches[0]["status_id"] == STATUS_CREATE_WAYBILL, _patches


def test_opt_reconciliation_obhodit_obe_voronki_istochnika_4_zapros():
    """ОПТ: reconciliation обходит обе воронки-источника (4 запроса)"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    _saved_do_get = office_transfer.amo_service._do_get
    _saved_process = office_transfer.process_office_transfer
    office_transfer.amo_service._do_get = _fake_do_get_two_sources
    office_transfer.process_office_transfer = _fake_process
    office_transfer.OFFICE_TRANSFER_SINCE_TS = int(time.time()) - 60
    office_transfer._last_reconcile_ts = 0
    run(office_transfer._reconcile_once())
    assert set(_opt_reconcile_requests) == {
        (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS), (PIPELINE_CLEVER_MAIN, STATUS_CLOSED_LOST),
        (PIPELINE_OPT, STATUS_SUCCESS), (PIPELINE_OPT, STATUS_CLOSED_LOST),
    }, _opt_reconcile_requests
    # ...а с выключенным флагом — только розницу, лишних запросов в лимит нет
    office_transfer.OFFICE_TRANSFER_SOURCE_OPT = False
    _opt_reconcile_requests.clear()
    office_transfer._last_reconcile_ts = 0
    run(office_transfer._reconcile_once())
    assert set(_opt_reconcile_requests) == {
        (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS), (PIPELINE_CLEVER_MAIN, STATUS_CLOSED_LOST),
    }, _opt_reconcile_requests
    office_transfer.amo_service._do_get = _saved_do_get
    office_transfer.process_office_transfer = _saved_process
    office_transfer._last_reconcile_ts = 0
    _orig_metrika_ps = metrika_sync.process_sync
    _orig_woo_ps = woo_status_sync.process_sync
    _orig_woo_enabled = woo_status_sync.is_enabled
    metrika_sync.process_sync = _fake_metrika_ps
    woo_status_sync.process_sync = _fake_woo_ps
    woo_status_sync.is_enabled = lambda: True
    _reset()
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="CDEK: Посылка склад-дверь")
    _install_dispatcher_mocks(lead)
    office_transfer.amo_service.patch_lead = _patch_marks
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    kinds = [c[0] for c in _call_order]
    assert kinds == ["metrika", "woo", "patch"], _call_order
    # оба синка видели сделку ЕЩЁ в CLEVER/142 (старое состояние)
    assert _call_order[0][1:] == (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS)
    assert _call_order[1][1:] == (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS)
    # ошибка синка не блокирует перенос
    _call_order.clear()
    metrika_sync.process_sync = _boom
    _reset()
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    metrika_sync.process_sync = _orig_metrika_ps
    woo_status_sync.process_sync = _orig_woo_ps
    woo_status_sync.is_enabled = _orig_woo_enabled
    # Офис/142 и ФФ(доставлено/переведено) теперь PAID для ЛЮБОЙ оплаты (не только наложки)
    assert metrika_sync._classify(metrika_sync.PIPELINE_OFFICE, STATUS_SUCCESS, False) == ("PAID", True)
    assert metrika_sync._classify(metrika_sync.PIPELINE_OFFICE, STATUS_SUCCESS, True) == ("PAID", True)
    # CLEVER-логика не тронута: предоплата PAID, наложка в CLEVER — нет
    assert metrika_sync._classify(metrika_sync.PIPELINE_CLEVER_MAIN, STATUS_SUCCESS, False) == ("PAID", False)
    assert metrika_sync._classify(metrika_sync.PIPELINE_CLEVER_MAIN, STATUS_SUCCESS, True) == (None, False)
    # CANCELLED как был
    assert metrika_sync._classify(metrika_sync.PIPELINE_CLEVER_MAIN, STATUS_CLOSED_LOST, False) == ("CANCELLED", False)
    assert metrika_sync._classify(metrika_sync.PIPELINE_OFFICE, STATUS_CLOSED_LOST, True) == ("CANCELLED", True)


def test_resolve_clever_folbek_sama_sebe_original_starye_kopii_ne_t():
    """_resolve_clever: фолбэк «сама себе оригинал», старые копии не тронуты"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global clever_orig
    # _resolve_clever: сиблинга нет → сделка сама себе оригинал (перенесена, UUID на ней)
    _orig_find = metrika_sync.amo_service.find_leads_by_query
    metrika_sync.amo_service.find_leads_by_query = _fake_find_empty
    moved_lead = {"id": 77, "pipeline_id": metrika_sync.PIPELINE_OFFICE,
                  "custom_fields_values": [
                      {"field_id": metrika_sync.FIELD_MOYSKLAD_ORDER_UUID,
                       "values": [{"value": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}]}]}
    res = run(metrika_sync._resolve_clever(moved_lead))
    assert res is moved_lead, "перенесённая сделка — сама себе canonical"
    # ...а сделка ВООБЩЕ без UUID по-прежнему не резолвится
    res = run(metrika_sync._resolve_clever({"id": 78, "custom_fields_values": []}))
    assert res is None
    # ...и если сиблинг в CLEVER существует (старая копия) — возвращается именно он
    clever_orig = {"id": 79, "pipeline_id": metrika_sync.PIPELINE_CLEVER_MAIN,
                   "custom_fields_values": [
                       {"field_id": metrika_sync.FIELD_MOYSKLAD_ORDER_UUID,
                        "values": [{"value": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}]}]}
    metrika_sync.amo_service.find_leads_by_query = _fake_find_hit
    res = run(metrika_sync._resolve_clever(moved_lead))
    assert res is clever_orig, "старая копия по-прежнему резолвится в оригинал"
    metrika_sync.amo_service.find_leads_by_query = _orig_find


def test_kartoteka_flag_vyklyuchen_sdelka_ne_trogaetsya_i_ne_alerti():
    """картотека: флаг выключен → сделка не трогается и не алертит"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    _DB_FLAG_WAS = office_transfer.OFFICE_TRANSFER_SOURCE_DB_WORK
    assert _DB_FLAG_WAS is False, "OFFICE_TRANSFER_SOURCE_DB_WORK должен быть выключен по умолчанию"
    # флаг выключен → сделку не трогаем и НЕ алертим (её пока ведёт человек)
    _reset()
    lead = _lead(pipeline_id=PIPELINE_DB_WORK, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ",
                 responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-not-applicable", res
    assert not _patches and not _tags, (_patches, _tags)
    assert office_transfer.is_source_pipeline(PIPELINE_DB_WORK) is False
    office_transfer.OFFICE_TRANSFER_SOURCE_DB_WORK = True
    assert office_transfer.is_source_pipeline(PIPELINE_DB_WORK) is True
    assert office_transfer.is_source_pipeline(str(PIPELINE_DB_WORK)) is True
    # УР: тот же маршрут, что у розницы с такой же доставкой
    _reset()
    lead = _lead(pipeline_id=PIPELINE_DB_WORK, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ",
                 responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches[0]["pipeline_id"] == PIPELINE_OFFICE
    assert _patches[0]["status_id"] == STATUS_CREATE_WAYBILL, (
        "заказ из картотеки обязан ехать в тот же этап, что розничный с той же доставкой")
    assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OFFICE_MANAGER_USER_ID
    assert _patches[0]["custom_fields"][FIELD_FORMER_RESPONSIBLE] == "Иван Иванов"
    # ⚠️ ГЛАВНЫЙ тест задачи: ЗНР картотеки НИКУДА не едет
    _reset()
    lead = _lead(pipeline_id=PIPELINE_DB_WORK, status_id=STATUS_CLOSED_LOST,
                 reason=REASON_WAITLIST, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match", res
    assert not _patches, ("карточка обзвона обязана остаться в картотеке", _patches)
    # контроль: та же причина из РОЗНИЦЫ по-прежнему уезжает
    _reset()
    lead = _lead(pipeline_id=PIPELINE_CLEVER_MAIN, status_id=STATUS_CLOSED_LOST,
                 reason=REASON_WAITLIST, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches[0]["pipeline_id"] == PIPELINE_WAITLIST, _patches[0]
    # ЗНР=Опт для картотеки: асинхронный матчер даже не должен вызываться —
    # иначе на каждую закрытую карточку уходил бы лишний GET по контактам
    _reset()
    _saved_get_contact = office_transfer.amo_service.get_contact_by_id
    office_transfer.amo_service.get_contact_by_id = _count_contact
    lead = _lead(pipeline_id=PIPELINE_DB_WORK, status_id=STATUS_CLOSED_LOST,
                 reason=REASON_OPT, responsible_user_id=999)
    lead["_embedded"] = {"contacts": [{"id": 5001}]}
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match", res
    assert not _contact_calls, ("ЗНР-матчеры картотеке не даны — лишних GET по контактам быть не должно",
                                _contact_calls)
    # контроль: та же сделка из розницы контакт дочитывает и едет в ОПТ
    _reset()
    _contact_calls.clear()
    lead = _lead(pipeline_id=PIPELINE_CLEVER_MAIN, status_id=STATUS_CLOSED_LOST,
                 reason=REASON_OPT, responsible_user_id=999)
    lead["_embedded"] = {"contacts": [{"id": 5001}]}
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _contact_calls == [5001], _contact_calls
    office_transfer.amo_service.get_contact_by_id = _saved_get_contact
    # _no_match_ur жив: пустые поля дают понятный алерт, а не тишину
    _reset()
    lead = _lead(pipeline_id=PIPELINE_DB_WORK, application_type=None,
                 warehouse=None, delivery_text="", responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match-bad-fill", res
    assert _tags and _tags[0][1] == TAG_BAD_FILL, _tags
    office_transfer.OFFICE_TRANSFER_SOURCE_DB_WORK = _DB_FLAG_WAS
    # ── картотека и заказ на сайте: комиссия рефералки должна начисляться ───────
    # Woo спрашивает «оплачено?» у metrika_sync._classify, поэтому ветка живёт там.
    assert metrika_sync._classify(PIPELINE_DB_WORK, STATUS_SUCCESS, False) == ("PAID", False)
    assert metrika_sync._classify(PIPELINE_DB_WORK, STATUS_SUCCESS, True) == (None, False), (
        "наложку картотеки ждём закрытой в Офисе, как и розничную")
    # CANCELLED у картотеки резолвит оригинал (как любая не-розничная воронка) —
    # на Woo это не влияет, он реагирует только на PAID.
    assert metrika_sync._classify(PIPELINE_DB_WORK, STATUS_CLOSED_LOST, False) == ("CANCELLED", True)


def test_metrika_kartoteku_ne_vidit_a_roznicu_otpravlyaet_geyt_voro():
    """Метрика картотеку не видит, а розницу отправляет — гейт воронок работает"""
    # Эти имена читают заглушки, объявленные на уровне модуля, - значит и присваивать
    # их надо туда же, иначе заглушка упадёт NameError при вызове.
    global lead
    _saved_upload = metrika_sync.metrika_client.upload_simple_order
    metrika_sync.metrika_client.upload_simple_order = _catch_upload
    _saved_enabled = metrika_sync._enabled
    metrika_sync._enabled = True
    _saved_contact_info = metrika_sync._contact_info
    metrika_sync._contact_info = _fake_contact_info
    _db_lead = _ym_lead(PIPELINE_DB_WORK)
    run(metrika_sync.process_sync({"lead_id": 42}, lead=_db_lead))
    assert not _ym_rows, ("картотека не должна уезжать в Метрику — Катя просила не трогать", _ym_rows)
    # Контроль, чтобы сторож выше не был ложно-зелёным: та же сделка из РОЗНИЦЫ
    # доходит до отправки. Если бы process_sync выходил раньше по другой причине
    # (флаг, заморозка миграции), пустым оказался бы и этот случай.
    _ym_rows.clear()
    _retail_lead = _ym_lead(PIPELINE_CLEVER_MAIN)
    run(metrika_sync.process_sync({"lead_id": 42}, lead=_retail_lead))
    assert _ym_rows, "розница обязана уезжать в Метрику — иначе сторож выше ничего не проверяет"
    metrika_sync.metrika_client.upload_simple_order = _saved_upload
    metrika_sync._contact_info = _saved_contact_info
    metrika_sync._enabled = _saved_enabled
    _TG_FLAG_WAS = office_transfer.OFFICE_TRANSFER_SOURCE_TANGEMSHOP
    assert _TG_FLAG_WAS is False, "OFFICE_TRANSFER_SOURCE_TANGEMSHOP должен быть выключен по умолчанию"
    # флаг выключен → сделку не трогаем и НЕ алертим
    _reset()
    lead = _lead(pipeline_id=PIPELINE_TANGEMSHOP, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ",
                 responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "skipped-not-applicable", res
    assert not _patches and not _tags, (_patches, _tags)
    assert office_transfer.is_source_pipeline(PIPELINE_TANGEMSHOP) is False
    office_transfer.OFFICE_TRANSFER_SOURCE_TANGEMSHOP = True
    assert office_transfer.is_source_pipeline(PIPELINE_TANGEMSHOP) is True
    # УР со СДЭКом: тот же этап Офиса, что у розницы с такой же доставкой
    _reset()
    lead = _lead(pipeline_id=PIPELINE_TANGEMSHOP, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ",
                 responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert _patches[0]["pipeline_id"] == PIPELINE_OFFICE
    assert _patches[0]["status_id"] == STATUS_CREATE_WAYBILL, (
        "заказ Tangemshop обязан ехать в тот же этап, что розничный с той же доставкой")
    assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OFFICE_MANAGER_USER_ID
    assert _patches[0]["custom_fields"][FIELD_FORMER_RESPONSIBLE] == "Иван Иванов"
    # УР с самовывозом: в УР Офиса, тоже как розница
    _reset()
    lead = _lead(pipeline_id=PIPELINE_TANGEMSHOP, application_type=APPLICATION_TYPE_ORDER,
                 warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text="Самовывоз из офиса Sunscrypt", responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "moved", res
    assert (_patches[0]["pipeline_id"], _patches[0]["status_id"]) == (PIPELINE_OFFICE, STATUS_SUCCESS), _patches[0]
    # ⚠️ ГЛАВНЫЙ тест задачи: ЗИН закрывает сделку НА МЕСТЕ
    _reset()
    lead = _lead(pipeline_id=PIPELINE_TANGEMSHOP, status_id=STATUS_CLOSED_LOST,
                 reason=REASON_WAITLIST, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match", res
    assert not _patches, ("ЗИН обязан оставить сделку в воронке Tangemshop", _patches)
    # та же причина ЗИН, но «Академия»: тоже никуда
    _reset()
    lead = _lead(pipeline_id=PIPELINE_TANGEMSHOP, status_id=STATUS_CLOSED_LOST,
                 reason=REASON_ACADEMY, responsible_user_id=999)
    _install_dispatcher_mocks(lead)
    res = run(office_transfer.process_office_transfer(42))
    assert res == "no-match", res
    assert not _patches, _patches
    office_transfer.OFFICE_TRANSFER_SOURCE_TANGEMSHOP = _TG_FLAG_WAS
    # ── признак магазина: два независимых источника ──────────────────────────────
    assert is_tangemshop_lead(
        {"custom_fields_values": [_cf(FIELD_SALES_CHANNEL, value="TangemShop")]}) is True
    # по enum_id тоже: подпись значения в интерфейсе заказчик может переименовать
    assert is_tangemshop_lead({"custom_fields_values": [
        {"field_id": FIELD_SALES_CHANNEL,
         "values": [{"value": "как угодно", "enum_id": ENUM_SALES_CHANNEL_TANGEMSHOP}]}]}) is True
    assert is_tangemshop_lead(
        {"custom_fields_values": [_cf(FIELD_SITE_ORDER_NUMBER, value="17665 Tangemshop")]}) is True
    assert is_tangemshop_lead(
        {"custom_fields_values": [_cf(FIELD_SITE_ORDER_NUMBER, value="19003")]}) is False
    assert is_tangemshop_lead({"custom_fields_values": []}) is False
    assert is_tangemshop_lead({}) is False
    # ── мина 1: выручка Tangemshop не уезжает в счётчик Метрики Sunscrypt ────────
    # Сделка, закрытая как УР, живёт в ОФИСЕ и по воронке от розничной неотличима.
    metrika_sync.metrika_client.upload_simple_order = _catch_upload
    metrika_sync._contact_info = _fake_contact_info
    metrika_sync._enabled = True
    _saved_find = metrika_sync.amo_service.find_leads_by_query
    metrika_sync.amo_service.find_leads_by_query = _find_no_sibling
    _ym_rows.clear()
    run(metrika_sync.process_sync({"lead_id": 8101}, lead=_office_lead(8101, "17665 Tangemshop")))
    assert not _ym_rows, ("выручка чужого магазина не должна уезжать в счётчик Sunscrypt", _ym_rows)
    # Контроль, чтобы сторож не был ложно-зелёным: та же сделка БЕЗ признака магазина
    # доходит до отправки.
    _ym_rows.clear()
    run(metrika_sync.process_sync({"lead_id": 8102}, lead=_office_lead(8102, "19003")))
    assert _ym_rows, "розничный заказ в Офисе обязан уезжать в Метрику — иначе сторож ничего не проверяет"
    metrika_sync.metrika_client.upload_simple_order = _saved_upload
    metrika_sync._contact_info = _saved_contact_info
    metrika_sync._enabled = _saved_enabled
    metrika_sync.amo_service.find_leads_by_query = _find_no_sibling
    assert run(woo_status_sync.resolve_target(
        {"lead_id": 8201}, lead=_office_lead(8201, "17665 Tangemshop"))) is None, (
        "номер с суффиксом магазина в WooCommerce искать нечего")
    # Контроль: розничный заказ по-прежнему доходит до Woo.
    _target = run(woo_status_sync.resolve_target({"lead_id": 8202}, lead=_office_lead(8202, "19003")))
    assert _target is not None and _target["site"] == "19003", _target
    metrika_sync.amo_service.find_leads_by_query = _saved_find

