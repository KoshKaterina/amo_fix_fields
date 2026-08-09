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
    REASON_WAITLIST,
    RESPONSIBLE_OFFICE_MANAGER_USER_ID,
    STATUS_ACADEMY_FIRST_CONTACT,
    STATUS_CLOSED_LOST,
    STATUS_CREATE_WAYBILL,
    STATUS_OFFICE_DELIVERY,
    STATUS_OFFICE_PREORDER_PAID,
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
    "OFFICE_TRANSFER_RULE_ZNR_WAITLIST",
    "OFFICE_TRANSFER_RULE_ZNR_ACADEMY", "OFFICE_TRANSFER_RULE_UR_POST",
):
    setattr(office_transfer, _flag, True)


# ── 1) матчеры правил: позитив + негатив по каждому условию ─────────────────

# УР-1 Достависта (курьер по Москве)
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")
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
print("✓ УР-1 Достависта: матчинг верный")

# УР-2 Самовывоз (дискриминатор «из офиса» против «CDEK: Самовывоз»)
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_OPENED,
             delivery_text="Самовывоз из офиса Sunscrypt")
assert office_transfer._match_ur_pickup(lead) == (PIPELINE_OFFICE, STATUS_SUCCESS), "самовывоз = сразу УР Офиса (Катя 31.07)"
lead2 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_OPENED,
              delivery_text="CDEK: Самовывоз")
assert office_transfer._match_ur_pickup(lead2) is None
print("✓ УР-2 Самовывоз: дискриминатор «из офиса» против CDEK: Самовывоз работает")

# УР-3 СДЭК (схлопнутые правила 3+6+7 исходного списка) — регистронезависимо, CDEK/СДЭК
for text in ("CDEK: Посылка склад-дверь", "сдэк: самовывоз", "Доставка СДЭК курьером"):
    lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                 delivery_text=text)
    assert office_transfer._match_ur_waybill(lead) == (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL), text
lead_no = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
                delivery_text="Достависта курьер")
assert office_transfer._match_ur_waybill(lead_no) is None
print("✓ УР-3 СДЭК (схлопнутые 3+6+7): регистронезависимо, обе формы CDEK/СДЭК")

# УР-4 Предзаказ — условие только по Тип заявки
lead = _lead(application_type=APPLICATION_TYPE_PREORDER)
assert office_transfer._match_ur_preorder(lead) == (PIPELINE_OFFICE, STATUS_OFFICE_PREORDER_PAID)
lead2 = _lead(application_type=APPLICATION_TYPE_ORDER)
assert office_transfer._match_ur_preorder(lead2) is None
print("✓ УР-4 Предзаказ")

# УР(ЭРМС): маршрута в Фулфилмент БОЛЬШЕ НЕТ (воронка разобрана 05.08.2026).
# ⚠️ Этот блок был выпотрошен вместе с правилом: остались две присвоенные сделки
# и печать «✓», а проверок — НИ ОДНОЙ, то есть галочка врала. Возвращаем смысл:
# ЭРМС-склад не должен матчиться ни одним правилом, розничный склад — должен.
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_ERMS_MAIN,
             delivery_text="СДЭК до ПВЗ")
assert office_transfer._match_rules(lead, STATUS_SUCCESS) is None, (
    "ЭРМС больше не маршрут: после выпила Фулфилмента правила его не берут")
lead2 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
              delivery_text="СДЭК до ПВЗ")
assert office_transfer._match_rules(lead2, STATUS_SUCCESS) == (PIPELINE_OFFICE, STATUS_CREATE_WAYBILL), (
    "склад — единственное отличие от предыдущей сделки, розничный обязан матчиться")
print("✓ УР(ЭРМС): не матчится ни одним правилом, розничный склад матчится")

# ЗНР Лист ожидания
lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_WAITLIST)
assert office_transfer._match_znr_waitlist(lead) == (PIPELINE_WAITLIST, STATUS_WAITLIST)
lead2 = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_ACADEMY)
assert office_transfer._match_znr_waitlist(lead2) is None
print("✓ ЗНР Лист ожидания")

# ЗНР Академия
lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_ACADEMY)
assert office_transfer._match_znr_academy(lead) == (PIPELINE_ACADEMY, STATUS_ACADEMY_FIRST_CONTACT)
lead2 = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_WAITLIST)
assert office_transfer._match_znr_academy(lead2) is None
print("✓ ЗНР Академия")

# правило выключено флагом — не матчит, даже если условия подходят
office_transfer.OFFICE_TRANSFER_RULE_UR_DELIVERY = False
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")
assert office_transfer._match_ur_delivery(lead) is None
office_transfer.OFFICE_TRANSFER_RULE_UR_DELIVERY = True
print("✓ флаг правила выключен → не матчит")


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


# guard: сделка не в CLEVER — skip, ничего не пишем (идемпотентность: уже
# перенесённая сделка при повторном вызове не PATCH-ится второй раз)
_reset()
lead = _lead(pipeline_id=PIPELINE_OFFICE, application_type=APPLICATION_TYPE_ORDER,
             warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="Доставка курьером по Москве")
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "skipped-not-applicable", res
assert not _patches
print("✓ guard: сделка вне CLEVER (уже перенесена) → skip, PATCH не шлём")

# guard: статус не в {142,143} — skip
_reset()
lead = _lead(status_id=83537714, pipeline_id=PIPELINE_CLEVER_MAIN)
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "skipped-not-applicable", res
assert not _patches
print("✓ guard: сделка вне {142,143} → skip")

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
print("✓ подходящая сделка переносится одним PATCH")

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
print("✓ ни одно правило не подошло → тег «заказ заполнен некорректно» + алерт, без PATCH")


# ── 3) смена ответственного — ТОЛЬКО при переносе в Офис ────────────────────

_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве", responsible_user_id=999)
_install_dispatcher_mocks(lead)
run(office_transfer.process_office_transfer(42))
assert _patches[0]["responsible_user_id"] == RESPONSIBLE_OFFICE_MANAGER_USER_ID, _patches
assert _patches[0]["custom_fields"][FIELD_FORMER_RESPONSIBLE] == "Иван Иванов", _patches
print("✓ перенос в Офис: ответственный → Зубалий, прежний → 578151")

_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве",
             responsible_user_id=RESPONSIBLE_OFFICE_MANAGER_USER_ID)
_install_dispatcher_mocks(lead)
run(office_transfer.process_office_transfer(42))
assert "responsible_user_id" not in _patches[0], _patches
assert "custom_fields" not in _patches[0], _patches
print("✓ ответственный уже Зубалий → без повторной записи")

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
print("✓ ЭРМС после выпила Фулфилмента: не переносится, уходит в алерт заполнения")

_reset()
lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_WAITLIST, responsible_user_id=999)
_install_dispatcher_mocks(lead)
run(office_transfer.process_office_transfer(42))
assert "responsible_user_id" not in _patches[0], _patches
print("✓ перенос в Лист ожидания: ответственный не меняется")

_reset()
lead = _lead(status_id=STATUS_CLOSED_LOST, reason=REASON_ACADEMY, responsible_user_id=999)
_install_dispatcher_mocks(lead)
run(office_transfer.process_office_transfer(42))
assert "responsible_user_id" not in _patches[0], _patches
print("✓ перенос в Академию: ответственный не меняется")


# ── 4) путь отказа: тег + примечание сразу, алерт по порогу с дедупом ───────

_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")


async def _fake_get_lead_full(lid, with_=()):
    return lead


async def _failing_patch(lid, **kw):
    return {"ok": False, "status_code": 500, "retryable": True}


office_transfer.amo_service.get_lead_full = _fake_get_lead_full
office_transfer.amo_service.patch_lead = _failing_patch

res = run(office_transfer.process_office_transfer(42))
assert res == "failed-patch", res
assert _tags and _tags[0][1] == TAG_OFFICE_TRANSFER_ERROR, _tags
assert _notes and "не выполнен" in _notes[0][1], _notes
assert not _alerts, "порог ещё не наступил — алерта быть не должно"
print("✓ провал PATCH: тег + примечание сразу, алерта пока нет (порог не наступил)")

# состарили неудачу за порог (default OFFICE_TRANSFER_STALE_ALERT_MIN=30 мин) → один алерт
office_transfer._pending_fail[42]["since"] -= 3600
run(office_transfer.process_office_transfer(42))
assert len(_alerts) == 1, _alerts
run(office_transfer.process_office_transfer(42))
assert len(_alerts) == 1, "повторный алерт по той же сделке быть не должен (дедуп)"
print("✓ зависшая сделка: один алерт по истечении порога, дедуп на повторных проходах")

_reset()


# ── 5) reconciliation: cutover-окно (без ретроактивности) ───────────────────

_events_requests: list = []


async def _fake_do_get(path, params=None):
    _events_requests.append((path, dict(params or [])))
    return {"_embedded": {"events": []}}


office_transfer.amo_service._do_get = _fake_do_get

# без заданного cutover — проход пропускается целиком (защита от случайного
# запуска reconciliation без границы — задело бы старые досделочные сделки)
office_transfer.OFFICE_TRANSFER_SINCE_TS = 0
office_transfer._last_reconcile_ts = 0
res = run(office_transfer._reconcile_once())
assert res == "skipped-no-cutover", res
assert not _events_requests
print("✓ без OFFICE_TRANSFER_SINCE_TS reconciliation не запускается")

# с заданным cutover — окно уходит в /api/v4/events не раньше границы
office_transfer.OFFICE_TRANSFER_SINCE_TS = 1000
office_transfer._last_reconcile_ts = 0
run(office_transfer._reconcile_once())
assert _events_requests, "reconcile должен был сходить в /api/v4/events"
for path, params in _events_requests:
    assert path == "/api/v4/events"
    assert int(params["filter[created_at][from]"]) >= 1000, params
print("✓ reconciliation: окно не раньше OFFICE_TRANSFER_SINCE_TS (без ретроактивности)")

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
print("✓ reconciliation в окне миграции: сделки прогона отсеяны, боевая обработана")

# то же окно, но воронки-источника в списке НЕТ — обрабатываем всех,
# лучше лишняя работа, чем потерянный заказ
_mf.MIGRATION_SOURCE_PIPELINES = set()
_processed_by_reconcile.clear()
office_transfer._last_reconcile_ts = 0
res = run(office_transfer._reconcile_once())
assert res == "processed=3", res
assert sorted(_processed_by_reconcile) == [101, 102, 103], _processed_by_reconcile
print("✓ reconciliation: воронка не в списке источников — ничего не теряем")

(_mf.MIGRATION_BULK_PAUSE, _mf.MIGRATION_FREEZE_TAG, _mf.MIGRATION_FREEZE_FROM_TS,
 _mf.MIGRATION_FREEZE_TO_TS, _mf.MIGRATION_SOURCE_PIPELINES) = _mf_backup
office_transfer.process_office_transfer = _orig_process
office_transfer.amo_service._do_get = _fake_do_get
office_transfer._last_reconcile_ts = 0
_events_requests.clear()

# ── 5в) потолок оглядки: после рестарта окно не разворачивается на неделю ──
# (05.08.2026: первый же проход после выката перебрал 23 224 события миграции)

import time as _time  # noqa: E402

office_transfer.OFFICE_TRANSFER_SINCE_TS = 1000  # cutover глубоко в прошлом
office_transfer._last_reconcile_ts = 0           # как после рестарта
run(office_transfer._reconcile_once())
_now = int(_time.time())
for path, params in _events_requests:
    _from = int(params["filter[created_at][from]"])
    assert _from >= _now - office_transfer.RECONCILE_MAX_LOOKBACK_S - 5, (_from, _now)
print("✓ reconciliation: окно назад ограничено RECONCILE_MAX_LOOKBACK_S")

# защита «без cutover не запускаться» потолок не сломал
office_transfer.OFFICE_TRANSFER_SINCE_TS = 0
office_transfer._last_reconcile_ts = 0
_events_requests.clear()
res = run(office_transfer._reconcile_once())
assert res == "skipped-no-cutover", res
assert not _events_requests
print("✓ потолок оглядки не отменяет проверку cutover")

office_transfer._last_reconcile_ts = 0
_events_requests.clear()

office_transfer.OFFICE_TRANSFER_SINCE_TS = 0


# ── 6б) cutover-гейт ВЕБХУК-пути: старые закрытые сделки не трогаем (31.07.2026) ──

office_transfer.OFFICE_TRANSFER_SINCE_TS = 1_700_000_000
_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")
lead["closed_at"] = 1_699_999_999  # вход в УР ДО cutover
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "skipped-pre-cutover", res
assert not _patches and not _tags and not _alerts
print("✓ cutover-гейт: закрытая до включения сделка не переносится (даже вебхуком)")

lead["closed_at"] = 1_700_000_001  # свежий вход ПОСЛЕ cutover
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "moved", res
print("✓ cutover-гейт: вход после включения переносится штатно")

office_transfer.OFFICE_TRANSFER_SINCE_TS = 0
_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")
lead["closed_at"] = 1_600_000_000
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "moved", res
print("✓ cutover-гейт: SINCE_TS=0 (не задан) — гейт выключен")


# ── 6в) гейт «уже переносилась»: 578151 заполнен → повторный вход в УР не трогаем ──

_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")
lead["custom_fields_values"].append(_cf(FIELD_FORMER_RESPONSIBLE, value="Оанча Игорь"))
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "skipped-already-transferred", res
assert not _patches and not _tags and not _alerts
print("✓ гейт повторного переноса: 578151 заполнен → сделка стоит в УР, не трогаем")


# ── 7) правило «Почта России» → Офис/«Сделать накладную» (Катя 31.07.2026) ──

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
print("✓ Почта России → Офис/Сделать накладную (регистр, хвосты, негативы)")


# ── 8) no-match алерты: «доставка не заполнена» / «заказ заполнен некорректно» ──

# Заказ + склад на месте, доставка пустая → «доставка не заполнена»
_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN)
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "no-match-no-delivery", res
assert (42, TAG_NO_DELIVERY) in _tags, _tags
assert len(_alerts) == 1 and "Типа доставки" in _alerts[0], _alerts
assert not _patches
print("✓ Заказ+склад без «Типа доставки» → тег «доставка не заполнена» + алерт")

# повторный вызов по той же сделке → дедуп, второго алерта нет
res = run(office_transfer.process_office_transfer(42))
assert res == "skipped-already-alerted", res
assert len(_alerts) == 1, _alerts
print("✓ повторный проход по той же сделке → алерт не дублируется")

# дедуп переживает рестарт: set пуст, но тег уже на сделке
office_transfer._fill_alerted.clear()
lead_tagged = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN)
lead_tagged["_embedded"] = {"tags": [{"id": 1, "name": TAG_NO_DELIVERY}]}
_install_dispatcher_mocks(lead_tagged)
res = run(office_transfer.process_office_transfer(42))
assert res == "skipped-already-alerted", res
assert len(_alerts) == 1, _alerts
print("✓ дедуп по тегу на сделке (переживает рестарт процесса)")

# пустая доставка, но ЗАКАЗА нет (мусор) → «заказ заполнен некорректно»
_reset()
lead = _lead()  # ни типа заявки, ни склада, ни доставки
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "no-match-bad-fill", res
assert (42, TAG_BAD_FILL) in _tags, _tags
assert len(_alerts) == 1, _alerts
print("✓ мусорная УР-сделка (всё пусто) → тег «заказ заполнен некорректно» + алерт")

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
print("✓ сделка под выключенным правилом → тихий скип, без ложного алерта")

# ЗНР без спец-причины → тихий no-match, без тегов/алертов
_reset()
lead = _lead(status_id=STATUS_CLOSED_LOST, reason=1041147)  # «Пропал»
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "no-match", res
assert not _tags and not _alerts, (_tags, _alerts)
print("✓ ЗНР с обычной причиной → тихо остаётся в CLEVER")

# менеджер дозаполнил поля после алерта → перенос + снятие тега
_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="Доставка курьером по Москве")
lead["_embedded"] = {"tags": [{"id": 1, "name": TAG_NO_DELIVERY}]}
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "moved", res
assert (42, TAG_NO_DELIVERY) in _removed_tags, _removed_tags
print("✓ поля дозаполнены → перенос + тег «доставка не заполнена» снят")


# ── 8б) ОПТ как вторая воронка-источник (Катя 09.08.2026) ───────────────────
# Правила те же пять, новый только вход. Проверяем: гейт по флагу в обе стороны,
# что опт-сделка едет тем же маршрутом, что розничная с такой же доставкой,
# смену ответственного, ЗНР-ветку и что reconciliation обходит обе воронки.

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
print("✓ ОПТ: флаг выключен → сделка не трогается и не алертит")

assert office_transfer.is_source_pipeline(PIPELINE_CLEVER_MAIN) is True
assert office_transfer.is_source_pipeline(PIPELINE_OPT) is False
assert office_transfer.is_source_pipeline(None) is False, "мусор на входе гейта — не источник"

office_transfer.OFFICE_TRANSFER_SOURCE_OPT = True
assert office_transfer.is_source_pipeline(PIPELINE_OPT) is True
assert office_transfer.is_source_pipeline(str(PIPELINE_OPT)) is True, (
    "вебхук отдаёт pipeline_id строкой — гейт обязан её понимать")
assert office_transfer.is_source_pipeline(PIPELINE_OFFICE) is False, (
    "Офис — ЦЕЛЬ переноса, не источник: иначе перенесённая сделка поехала бы по кругу")
print("✓ ОПТ: гейт источника по флагу, строка и мусор обработаны")

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
print("✓ ОПТ/142 СДЭК → Офис/«Сделать накладную», ответственный → Зубалий, прежний в 578151")

# опт-предзаказ → «Предзаказ оплачен» (ровно то, что делала ручная копия 03.08)
_reset()
lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_PREORDER,
             responsible_user_id=999)
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "moved", res
assert _patches[0]["status_id"] == STATUS_OFFICE_PREORDER_PAID, _patches
print("✓ ОПТ/142 предзаказ → Офис/«Предзаказ оплачен»")

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
print("✓ ОПТ/143 → Лист ожидания / Академия, ответственный не меняется")

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
print("✓ ОПТ: доставка вне пяти правил → алерт, наугад не переносим")

# гейт «уже переносилась» действует и для опта
_reset()
lead = _lead(pipeline_id=PIPELINE_OPT, application_type=APPLICATION_TYPE_ORDER,
             warehouse=WAREHOUSE_SUNSCRYPT_MAIN, delivery_text="СДЭК до ПВЗ")
lead["custom_fields_values"].append(_cf(FIELD_FORMER_RESPONSIBLE, value="Иван Иванов"))
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "skipped-already-transferred", res
assert not _patches
print("✓ ОПТ: гейт «578151 заполнен → уже переносилась» работает и здесь")

# reconciliation обходит ОБЕ воронки: 4 запроса (2 воронки × 2 статуса)
_opt_reconcile_requests: list = []


async def _fake_do_get_two_sources(path, params=None):
    p = dict(params or [])
    _opt_reconcile_requests.append((
        int(p["filter[value_after][leads_statuses][0][pipeline_id]"]),
        int(p["filter[value_after][leads_statuses][0][status_id]"]),
    ))
    return {"_embedded": {"events": []}}


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
print("✓ ОПТ: reconciliation обходит обе воронки-источника (4 запроса)")

# ...а с выключенным флагом — только розницу, лишних запросов в лимит нет
office_transfer.OFFICE_TRANSFER_SOURCE_OPT = False
_opt_reconcile_requests.clear()
office_transfer._last_reconcile_ts = 0
run(office_transfer._reconcile_once())
assert set(_opt_reconcile_requests) == {
    (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS), (PIPELINE_CLEVER_MAIN, STATUS_CLOSED_LOST),
}, _opt_reconcile_requests
print("✓ ОПТ: флаг выключен → reconciliation не тратит запросы на ОПТ")

office_transfer.amo_service._do_get = _saved_do_get
office_transfer.process_office_transfer = _saved_process
office_transfer._last_reconcile_ts = 0


# ── 9) PAID до переноса: синки зовутся по ещё-CLEVER состоянию, до PATCH ──

import metrika_sync
import woo_status_sync

_call_order: list = []
_orig_metrika_ps = metrika_sync.process_sync
_orig_woo_ps = woo_status_sync.process_sync
_orig_woo_enabled = woo_status_sync.is_enabled


async def _fake_metrika_ps(payload, lead=None):
    _call_order.append(("metrika", lead.get("pipeline_id"), lead.get("status_id")))


async def _fake_woo_ps(payload, lead=None):
    _call_order.append(("woo", lead.get("pipeline_id"), lead.get("status_id")))


metrika_sync.process_sync = _fake_metrika_ps
woo_status_sync.process_sync = _fake_woo_ps
woo_status_sync.is_enabled = lambda: True

_reset()
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN,
             delivery_text="CDEK: Посылка склад-дверь")
_install_dispatcher_mocks(lead)


async def _patch_marks(lid, **kw):
    _call_order.append(("patch", None, None))
    _patches.append({"lead_id": lid, **kw})
    return {"ok": True, "status_code": 200}

office_transfer.amo_service.patch_lead = _patch_marks
res = run(office_transfer.process_office_transfer(42))
assert res == "moved", res
kinds = [c[0] for c in _call_order]
assert kinds == ["metrika", "woo", "patch"], _call_order
# оба синка видели сделку ЕЩЁ в CLEVER/142 (старое состояние)
assert _call_order[0][1:] == (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS)
assert _call_order[1][1:] == (PIPELINE_CLEVER_MAIN, STATUS_SUCCESS)
print("✓ Метрика и Woo прогоняются ДО PATCH, по состоянию CLEVER/142")

# ошибка синка не блокирует перенос
_call_order.clear()


async def _boom(payload, lead=None):
    raise RuntimeError("метрика упала")

metrika_sync.process_sync = _boom
_reset()
_install_dispatcher_mocks(lead)
res = run(office_transfer.process_office_transfer(42))
assert res == "moved", res
print("✓ упавший синк не блокирует перенос")

metrika_sync.process_sync = _orig_metrika_ps
woo_status_sync.process_sync = _orig_woo_ps
woo_status_sync.is_enabled = _orig_woo_enabled


# ── 10) страховочная сетка metrika_sync после переноса ──

# Офис/142 и ФФ(доставлено/переведено) теперь PAID для ЛЮБОЙ оплаты (не только наложки)
assert metrika_sync._classify(metrika_sync.PIPELINE_OFFICE, STATUS_SUCCESS, False) == ("PAID", True)
assert metrika_sync._classify(metrika_sync.PIPELINE_OFFICE, STATUS_SUCCESS, True) == ("PAID", True)
# CLEVER-логика не тронута: предоплата PAID, наложка в CLEVER — нет
assert metrika_sync._classify(metrika_sync.PIPELINE_CLEVER, STATUS_SUCCESS, False) == ("PAID", False)
assert metrika_sync._classify(metrika_sync.PIPELINE_CLEVER, STATUS_SUCCESS, True) == (None, False)
# CANCELLED как был
assert metrika_sync._classify(metrika_sync.PIPELINE_CLEVER, STATUS_CLOSED_LOST, False) == ("CANCELLED", False)
assert metrika_sync._classify(metrika_sync.PIPELINE_OFFICE, STATUS_CLOSED_LOST, True) == ("CANCELLED", True)
print("✓ _classify: Офис/ФФ дают PAID для любой оплаты, CLEVER/CANCELLED не тронуты")

# _resolve_clever: сиблинга нет → сделка сама себе оригинал (перенесена, UUID на ней)
_orig_find = metrika_sync.amo_service.find_leads_by_query


async def _fake_find_empty(q, with_=()):
    return []

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
clever_orig = {"id": 79, "pipeline_id": metrika_sync.PIPELINE_CLEVER,
               "custom_fields_values": [
                   {"field_id": metrika_sync.FIELD_MOYSKLAD_ORDER_UUID,
                    "values": [{"value": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"}]}]}


async def _fake_find_hit(q, with_=()):
    return [clever_orig]

metrika_sync.amo_service.find_leads_by_query = _fake_find_hit
res = run(metrika_sync._resolve_clever(moved_lead))
assert res is clever_orig, "старая копия по-прежнему резолвится в оригинал"
metrika_sync.amo_service.find_leads_by_query = _orig_find
print("✓ _resolve_clever: фолбэк «сама себе оригинал», старые копии не тронуты")

print("\noffice_transfer: все тесты прошли")
