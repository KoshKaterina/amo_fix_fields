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
    PIPELINE_FULFILLMENT,
    PIPELINE_OFFICE,
    PIPELINE_WAITLIST,
    REASON_ACADEMY,
    REASON_WAITLIST,
    RESPONSIBLE_OFFICE_MANAGER_USER_ID,
    STATUS_ACADEMY_FIRST_CONTACT,
    STATUS_CLOSED_LOST,
    STATUS_CREATE_WAYBILL,
    STATUS_FF_KONTROL,
    STATUS_OFFICE_DELIVERY,
    STATUS_OFFICE_PICKUP,
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
    "OFFICE_TRANSFER_RULE_UR_FULFILLMENT", "OFFICE_TRANSFER_RULE_ZNR_WAITLIST",
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
assert office_transfer._match_ur_pickup(lead) == (PIPELINE_OFFICE, STATUS_OFFICE_PICKUP)
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

# УР(ЭРМС) → Фулфилмент/КОНТРОЛЬ
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_ERMS_MAIN)
assert office_transfer._match_ur_fulfillment(lead) == (PIPELINE_FULFILLMENT, STATUS_FF_KONTROL)
lead2 = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_SUNSCRYPT_MAIN)
assert office_transfer._match_ur_fulfillment(lead2) is None
print("✓ УР(ЭРМС→Фулфилмент)")

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
lead = _lead(application_type=APPLICATION_TYPE_ORDER, warehouse=WAREHOUSE_ERMS_MAIN, responsible_user_id=999)
_install_dispatcher_mocks(lead)
run(office_transfer.process_office_transfer(42))
assert "responsible_user_id" not in _patches[0], _patches
assert "custom_fields" not in _patches[0], _patches
print("✓ перенос в Фулфилмент(ЭРМС): ответственный не меняется")

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

office_transfer.OFFICE_TRANSFER_SINCE_TS = 0


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
assert metrika_sync._classify(metrika_sync.PIPELINE_FULFILLMENT, metrika_sync.FULFILLMENT_DELIVERED, False) == ("PAID", True)
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
