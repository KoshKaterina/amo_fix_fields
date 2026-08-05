"""Ручной QA-гейт Фулфилмента + аудит копий продаж.

Две задачи (запуск вручную, по умолчанию DRY-RUN — ничего не пишет):

  python kontrol_gate.py              # all, СРАЗУ применяет
  python kontrol_gate.py audit        # Часть 1: УР продаж → есть ли копия (только отчёт)
  python kontrol_gate.py gate         # Часть 2: гейт КОНТРОЛЬ — СРАЗУ переносит готовые
  python kontrol_gate.py gate --dry-run   # прогон без записи (показать план)

ЧАСТЬ 1 (audit, только отчёт): каждая сделка, ПЕРЕШЕДШАЯ в УР воронки продаж CLEVER
за 3 дня — фильтр ИМЕННО по событию смены статуса (lead_status_changed → 142),
а НЕ по дате создания/изменения/закрытия — ДОЛЖНА иметь живую или завершённую
копию (статус != 143) в воронке Офис или Фулфилмент. Иначе — флаг. Окно жёстко 3 дня.

ЧАСТЬ 2 (gate): для каждой сделки ФФ на этапе «КОНТРОЛЬ»:
  1. проверяем стоп-поля (если не заполнить нечем — оставляем в КОНТРОЛЕ):
     телефон контрагента; ≥1 товарная позиция; распознаваемый способ оплаты;
     для курьера — адрес доставки, для ПВЗ/постамата — Код ПВЗ;
  2. проверяем физ. остаток товаров по складу Sunscrypt Основной (≥ заказанного);
  3. если стоп-полей и дефицита нет — переносим копию в «00. Обрабатывается»
     и ставим заказу в МС статус «00» (релиз в комплектацию).

⚠️ 05.08.2026 из гейта убран автоподгон производных полей заказа МС (оценочная,
доставка, итого, тип платежа, вид доставки) — сами поля удалены как наследие
фулфилмента. Гейт теперь только проверяет и переносит.

По умолчанию СРАЗУ ПРИМЕНЯЕТ (переносит готовые в «00» + релиз МС).
--dry-run — только показать план, ничего не писать.
"""
import argparse
import asyncio
import datetime
import time

import logging

import amo_service
import migration_freeze
import ms_client
from api import init_api_pipeline, shutdown_api_pipeline
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    MS_API_URL,
    PIPELINE_CLEVER,
    PIPELINE_FULFILLMENT,
    PIPELINE_OFFICE,
    STATUS_CLOSED_LOST,
    STATUS_FF_KONTROL,
    STATUS_FF_PROCESSING,
    STATUS_SUCCESS,
    TAG_KONTROL_ERROR,
)

logger = logging.getLogger("uvicorn")

MSK = datetime.timezone(datetime.timedelta(hours=3))

# ── этапы ФФ ─────────────────────────────────────────────────
KONTROL_STATUS = STATUS_FF_KONTROL       # «КОНТРОЛЬ (ПРОВЕРИТЬ ВРУЧНУЮ КАЖДЫЙ ЗАКАЗ»
PROCESSING_STATUS = STATUS_FF_PROCESSING  # «00. Обрабатывается»

# Эхо-защита: последняя записанная причина ошибки по сделке. Повторный прогон с той
# же причиной (напр. эхо-вебхук от простановки тега) не плодит дубли примечаний.
_last_error_reason: dict[str, str] = {}

# ── склад для проверки наличия (Sunscrypt Основной; до ухода с ФФ 07.2026 — ЭРМС_Основной) ──
STOCK_STORE_ID = "0e5a2b05-c413-11ee-0a80-13fd002f63f9"

# ── доп-поля заказа МС (из entity/customerorder/metadata/attributes) ──
A_PAYMENT_METHOD = "33735877-ba29-11f0-0a80-1737003bc63e"   # «Способ оплаты» (string)
A_PVZ_CODE = "308100c4-2aa3-11f1-0a80-01a9002fcf77"         # «Код ПВЗ» (string)
# ⚠️ 05.08.2026 удалены поля фулфилмента: «Вид доставки», «Стоимость доставки»,
# «Оценочная стоимость», «Итого к оплате получателем», «Прием платежа». Вместе с
# ними ушёл автоподгон производных полей — гейт теперь только проверяет заказ.

_state00_uuid = None  # uuid статуса МС «00. Обрабатывается», резолвим в init


# ════════════════ ПОРТ ЛОГИКИ woo field_resync ════════════════
def categorize_payment(s):
    if not s:
        return None
    s = s.lower()
    if "получ" in s or "налож" in s:
        return "cod"
    # Всё остальное распознаваемое — предоплата (клиент платит вперёд): онлайн-
    # эквайринг, перевод на карту/банк, криптовалюта. Раньше «перевод на карту»
    # выделялся в manual_prepaid ради обнуления доставки СДЭК (промо-акция) — акция
    # завершена (Катя, 09.07.2026), обнуление убрано → все предоплаты одинаковы.
    prepaid_markers = ("крипт", "crypto", "usdt", "usdc", "tether", "wallet",
                       "на карт", "перевод", "банк", "онлайн", "картой")
    if any(t in s for t in prepaid_markers):
        return "prepaid"
    return None


def detect_delivery_num(positions):
    """Вид доставки из имён услуг: 1=ПВЗ, 2=курьер, 3=постамат, None=не определить."""
    for p in positions:
        if p["type"] != "service":
            continue
        n = (p["name"] or "").lower()
        if "постамат" in n:
            return 3
        is_cdek = "сдэк" in n or "cdek" in n
        if "пвз" in n or ("самовывоз" in n and is_cdek):
            return 1
        if "курьер" in n or "достависта" in n:
            return 2
    return None


# ════════════════ чтение атрибутов МС ════════════════
def attr_val(order, uuid):
    for a in order.get("attributes", []):
        if a.get("id") == uuid:
            return a.get("value")
    return None


# ════════════════ доступ к МС ════════════════
async def fetch_positions(order_id):
    resp = await ms_client.get(f"entity/customerorder/{order_id}/positions",
                               {"expand": "assortment", "limit": 1000})
    out = []
    for p in (resp or {}).get("rows", []):
        a = p.get("assortment") or {}
        a_type = a.get("meta", {}).get("type", "")
        out.append({"id": p["id"], "type": "service" if a_type == "service" else "goods",
                    "name": a.get("name", ""), "price": p.get("price", 0),
                    "quantity": p.get("quantity", 1), "discount": p.get("discount", 0),
                    "assortment_id": a.get("id"), "assortment_type": a_type})
    return out


async def stock_map(assortment_ids):
    """Физ. остаток по складу STOCK_STORE_ID для списка товаров → {id: stock}."""
    if not assortment_ids:
        return {}
    rows = await ms_client.get("report/stock/bystore/current",
                               {"assortmentId": list(assortment_ids)})
    res = {}
    for r in (rows or []):
        if r.get("storeId") == STOCK_STORE_ID:
            res[r.get("assortmentId")] = r.get("stock", 0)
    return res



# ════════════════ стоп-поля ════════════════
def blockers(order, positions, stock):
    """Список причин, по которым заказ НЕ готов к релизу (остаётся в КОНТРОЛЕ)."""
    out = []
    # 1) телефон контрагента
    agent = order.get("agent") or {}
    phone = agent.get("phone") if isinstance(agent, dict) else None
    if not (phone and str(phone).strip()):
        out.append("нет телефона контрагента")
    # 2) ≥1 товарная позиция
    goods = [p for p in positions if p["type"] == "goods"]
    if not goods:
        out.append("нет товарных позиций")
    # 3) распознаваемый способ оплаты
    pm = attr_val(order, A_PAYMENT_METHOD)
    if categorize_payment(pm if isinstance(pm, str) else None) is None:
        out.append(f"способ оплаты не распознан ({pm!r})")
    # 4) адрес / код ПВЗ по виду доставки (определяем по именам услуг в заказе;
    #    доп.поле «Вид доставки» удалено 05.08.2026 вместе с обвязкой фулфилмента)
    dt = detect_delivery_num(positions)
    if dt == 2:  # курьер
        if not (order.get("shipmentAddress") or order.get("shipmentAddressFull")):
            out.append("курьер, но нет адреса доставки")
    elif dt in (1, 3):  # ПВЗ / постамат
        code = attr_val(order, A_PVZ_CODE)
        if not (code and str(code).strip()):
            out.append("ПВЗ/постамат, но нет Кода ПВЗ")
    # 5) наличие
    for p in goods:
        have = stock.get(p["assortment_id"], 0)
        if have < p["quantity"]:
            out.append(f"нет на складе: {p['name']} (нужно {p['quantity']}, есть {have})")
    return out


# ════════════════ ЧАСТЬ 1: аудит копий ════════════════
UR_AUDIT_DAYS = 3  # окно жёстко 3 дня


async def _entered_ur_leads(since, now):
    """ID сделок, ПЕРЕШЕДШИХ в УР(142) воронки CLEVER за окно — ИМЕННО по событию
    смены статуса (lead_status_changed, value_after=CLEVER/142), а не по дате
    создания/изменения/закрытия. Серверный фильтр value_after + клиентская проверка
    каждого события (результат верный, даже если серверный фильтр проигнорируется)."""
    leads, page = set(), 1
    while True:
        params = [
            ("filter[type]", "lead_status_changed"),
            ("filter[created_at][from]", str(since)),
            ("filter[created_at][to]", str(now)),
            ("filter[value_after][leads_statuses][0][pipeline_id]", str(PIPELINE_CLEVER)),
            ("filter[value_after][leads_statuses][0][status_id]", str(STATUS_SUCCESS)),
            ("limit", "100"), ("page", str(page)),
        ]
        d = await amo_service._do_get("/api/v4/events", params)
        evs = ((d or {}).get("_embedded") or {}).get("events") or []
        for e in evs:
            va = e.get("value_after") or []
            ls = (va[0].get("lead_status") if va else None) or {}
            if ls.get("id") == STATUS_SUCCESS and ls.get("pipeline_id") == PIPELINE_CLEVER:
                leads.add(e.get("entity_id"))
        if len(evs) < 100:
            break
        page += 1
    return leads


async def part1_audit():
    now = int(time.time())
    since = now - UR_AUDIT_DAYS * 86400
    entered = await _entered_ur_leads(since, now)
    print(f"\n══ ЧАСТЬ 1: сделки CLEVER, ПЕРЕШЕДШИЕ в УР за {UR_AUDIT_DAYS}д (по событию смены статуса) = {len(entered)} ══")
    bad = []
    for lid in entered:
        l = await amo_service.get_lead_full(lid, with_=())
        if not l:
            continue
        uu = str(amo_service.get_custom_field_value(l, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
        if not uu:
            bad.append((lid, l.get("name"), "нет привязки к МС (576689)")); continue
        sibs = [s for s in await amo_service.find_leads_by_query(uu)
                if str(amo_service.get_custom_field_value(s, FIELD_MOYSKLAD_ORDER_UUID) or "").strip() == uu]
        copies = [s for s in sibs if s.get("pipeline_id") in (PIPELINE_OFFICE, PIPELINE_FULFILLMENT)
                  and s.get("status_id") != STATUS_CLOSED_LOST]
        if not copies:
            bad.append((lid, l.get("name"), "нет живой/завершённой копии в Офис/ФФ"))
    print(f"✅ с копией: {len(entered) - len(bad)} | ⚠️ без копии: {len(bad)}")
    for lid, name, why in bad:
        print(f"   ⚠️ {lid} «{name}» — {why}")
    return bad


# ════════════════ ЧАСТЬ 2: гейт КОНТРОЛЬ ════════════════
async def _set_ms_state00(uuid):
    """Проставить заказу МС статус «00. Обрабатывается» (идемпотентно на стороне МС)."""
    if not _state00_uuid:
        await _resolve_state00()  # ленивое разрешение для событийного пути (без явного init)
    if not _state00_uuid:
        logger.warning("КОНТРОЛЬ: статус МС «00. Обрабатывается» не найден — релиз МС пропущен")
        return
    await ms_client.put(f"entity/customerorder/{uuid}", {"state": {"meta": {
        "href": f"{MS_API_URL}/entity/customerorder/metadata/states/{_state00_uuid}",
        "type": "state", "mediaType": "application/json"}}})


async def _hold_with_error(lead_id, current_tags, reason, apply):
    """Оставить сделку в КОНТРОЛЕ: тег «ошибка передачи» + причина примечанием.
    Эхо-safe: тег ставим только если его нет; примечание — только если причина
    изменилась с прошлого прогона (иначе повторный вебхук плодил бы дубли)."""
    key = str(lead_id)
    if apply:
        if not amo_service.has_tag({"_embedded": {"tags": current_tags}}, TAG_KONTROL_ERROR):
            new_tags = list(current_tags) + [{"name": TAG_KONTROL_ERROR}]
            res = await amo_service.patch_lead(lead_id, tags=new_tags)
            if not res.get("ok"):
                logger.error("КОНТРОЛЬ %s: не удалось поставить тег «%s»: %s", lead_id, TAG_KONTROL_ERROR, res)
        if _last_error_reason.get(key) != reason:
            await amo_service.add_note(lead_id, f"⚠️ Ошибка передачи в комплектацию: {reason}")
            _last_error_reason[key] = reason


async def process_kontrol_lead(lead_id, *, apply=True, source="webhook") -> dict:
    """Гейт КОНТРОЛЬ для ОДНОЙ сделки ФФ (событийный вход из вебхука + батч).

    Успех  → перенос в «00. Обрабатывается» (+ статус МС «00», + снятие тега ошибки).
    Провал → сделка остаётся в КОНТРОЛЕ, тег «ошибка передачи», причина примечанием.

    Возвращает {"action": released|held|skip, "reason": str|None}.
    """
    lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        logger.warning("КОНТРОЛЬ %s: сделка не получена", lead_id)
        return {"action": "skip", "reason": "сделка не получена"}

    # Окно миграции воронок: перенесённую сделку через гейт не гоняем — она
    # попала на этап переносом, а не боевым путём заказа.
    if await migration_freeze.skip(lead_id, "КОНТРОЛЬ", lead=lead):
        return {"action": "skip", "reason": "окно миграции"}

    # Событийный вход: сверяемся, что сделка ВСЁ ЕЩЁ в КОНТРОЛЕ ФФ (человек мог увести).
    # Это же гасит петлю: перенос в «00» и простановка тега не возвращают сюда.
    if source == "webhook" and not (
        lead.get("pipeline_id") == PIPELINE_FULFILLMENT and lead.get("status_id") == KONTROL_STATUS
    ):
        return {"action": "skip", "reason": "сделка уже не в этапе КОНТРОЛЬ"}

    current_tags = amo_service.get_tags(lead)
    key = str(lead_id)
    uu = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    if not uu:
        await _hold_with_error(lead_id, current_tags, "нет привязки к заказу МойСклад (поле 576689)", apply)
        return {"action": "held", "reason": "нет привязки к заказу МС"}

    order = await ms_client.get(f"entity/customerorder/{uu}", {"expand": "agent,state,salesChannel"})
    if not order:
        await _hold_with_error(lead_id, current_tags, "заказ в МойСклад не найден по привязке", apply)
        return {"action": "held", "reason": "заказ МС не найден"}
    positions = await fetch_positions(uu)

    # 1-2) стоп-поля + наличие на складе Sunscrypt Основной
    stock = await stock_map([p["assortment_id"] for p in positions if p["type"] == "goods" and p["assortment_id"]])
    blk = blockers(order, positions, stock)
    if blk:
        reason = "; ".join(blk)
        await _hold_with_error(lead_id, current_tags, reason, apply)
        logger.info("КОНТРОЛЬ %s: ДЕРЖИМ (%d): %s", lead_id, len(blk), reason)
        return {"action": "held", "reason": reason}

    # 4) готов → перенос в «00» (снимаем тег ошибки в том же PATCH) + статус МС «00»
    logger.info("КОНТРОЛЬ %s: ✅ ГОТОВ → «00. Обрабатывается» + релиз заказа МС %s", lead_id, uu)
    if apply:
        new_tags = amo_service.filter_tags_excluding(current_tags, TAG_KONTROL_ERROR)
        await amo_service.patch_lead(lead_id, status_id=PROCESSING_STATUS,
                                     pipeline_id=PIPELINE_FULFILLMENT, tags=new_tags)
        await _set_ms_state00(uu)
        await amo_service.add_note(lead_id, "✅ Гейт КОНТРОЛЬ пройден автоматически: заказ заполнен, "
                                            "товары в наличии (Sunscrypt Основной) → «00. Обрабатывается».")
        _last_error_reason.pop(key, None)
    return {"action": "released", "reason": None}


async def part2_gate(apply):
    leads = [l for l in await amo_service.get_leads_by_status(KONTROL_STATUS, with_=())
             if l.get("pipeline_id") == PIPELINE_FULFILLMENT]
    print(f"\n══ ЧАСТЬ 2: гейт КОНТРОЛЬ — сделок {len(leads)} | режим: {'ПРИМЕНЯЮ' if apply else 'DRY-RUN'} ══")
    released = held = 0
    for l in leads:
        lid = l.get("id")
        # source="batch" — без сверки «ещё в КОНТРОЛЕ» (список уже отфильтрован).
        res = await process_kontrol_lead(lid, apply=apply, source="batch")
        act = res["action"]
        mark = "✅ РЕЛИЗ" if act == "released" else ("⚠️ ДЕРЖИМ" if act == "held" else "— пропуск")
        print(f"— amo {lid} «{l.get('name')}»: {mark}" + (f" — {res['reason']}" if res["reason"] else ""))
        if act == "released":
            released += 1
        elif act == "held":
            held += 1
    print(f"\n══ Итог гейта: готово к релизу {released}, оставлено в КОНТРОЛЕ {held} ══")


async def _resolve_state00():
    global _state00_uuid
    meta = await ms_client.get("entity/customerorder/metadata")
    for s in (meta or {}).get("states", []):
        if s.get("name") == "00. Обрабатывается":
            _state00_uuid = s.get("id")
    return _state00_uuid


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", nargs="?", default="all", choices=["audit", "gate", "all"],
                    help="что делать (по умолчанию all)")
    ap.add_argument("--dry-run", action="store_true", help="только показать план, ничего не писать")
    args = ap.parse_args()

    init_api_pipeline()
    ms_client.init()
    try:
        await amo_service.warm_pipeline_cache()
        await _resolve_state00()
        if not _state00_uuid:
            print("⚠️ не нашёл статус МС «00. Обрабатывается» — релиз в МС будет пропущен")
        if args.cmd in ("audit", "all"):
            await part1_audit()
        if args.cmd in ("gate", "all"):
            await part2_gate(not args.dry_run)
    finally:
        await ms_client.aclose()
        await shutdown_api_pipeline()


if __name__ == "__main__":
    asyncio.run(main())
