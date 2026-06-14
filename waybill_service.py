"""Бизнес-логика создания накладных СДЭК и Telegram-команд /print, /retry."""

import asyncio
import logging
from typing import Awaitable, Callable

import cdek_client
import picking_pdf
import amo_service
from waybill_config import (
    FIELD_CDEK_ORDER_NUMBER,
    FIELD_COMPOSITION,
    FIELD_DELIVERY_ADDRESS,
    FIELD_EMAIL,
    FIELD_ORDER_TOTAL,
    FIELD_PACKAGE_NUMBER,
    FIELD_PAYMENT_METHOD,
    FIELD_PHONE,
    FIELD_PVZ_CODE,
    FIELD_PVZ_CODE_FALLBACK,
    FIELD_SENDER_COMPANY,
    PUBLIC_BASE_URL,
    SENDER,
    STATUS_CREATE_WAYBILL,
    STATUS_WAYBILL_READY,
    TAG_ERROR,
    TAG_PACKED,
    TARIFF_DOOR,
    TARIFFS_PVZ,
    extract_pvz_code,
    looks_like_uuid,
    parse_tariff,
    parse_total,
)

logger = logging.getLogger("uvicorn")

_alert_callback: Callable[[str], Awaitable[None]] | None = None


def set_alert_callback(fn: Callable[[str], Awaitable[None]]) -> None:
    global _alert_callback
    _alert_callback = fn


async def _alert(text: str) -> None:
    if _alert_callback is None:
        logger.warning("alert callback not set, suppressing: %s", text)
        return
    try:
        await _alert_callback(text)
    except Exception:
        logger.exception("alert callback failed")


# ---------------------------------------------------------------------------
# Создание накладной для одной сделки
# ---------------------------------------------------------------------------

async def create_waybill_for_lead(lead_id: int | str, *, source: str = "webhook") -> dict:
    """Возвращает dict {"ok": bool, "lead_id": ..., "reason": str | None, "cdek_number": ..., "skipped": bool}.

    source: "webhook" → ошибки сразу шлются в TG.
            "retry"   → ошибки возвращаются в результате (агрегатор отправит сводно).
    """
    lead = await amo_service.get_lead_full(lead_id, with_=("contacts", "companies"))
    if not lead:
        reason = "не удалось получить сделку из amoCRM"
        return await _fail(lead_id, reason, source, current_tags=[])

    current_tags = amo_service.get_tags(lead)

    existing_cdek = amo_service.get_custom_field_value(lead, FIELD_CDEK_ORDER_NUMBER)
    if existing_cdek:
        logger.info("Lead %s already has CDEK number %s — moving to ready, no re-creation", lead_id, existing_cdek)
        await amo_service.move_to_ready_and_clear_error(
            lead_id, current_tags, error_tag=TAG_ERROR, target_status=STATUS_WAYBILL_READY,
        )
        return {"ok": True, "lead_id": lead_id, "reason": None, "cdek_number": existing_cdek, "skipped": True}

    # 1. Парс полей
    order_text = amo_service.get_custom_field_value(lead, FIELD_ORDER_TOTAL)
    pvz_code_raw = amo_service.get_custom_field_value(lead, FIELD_PVZ_CODE)
    delivery_address = amo_service.get_custom_field_value(lead, FIELD_DELIVERY_ADDRESS)
    payment_method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
    sender_company = amo_service.get_custom_field_value(lead, FIELD_SENDER_COMPANY)
    package_number = amo_service.get_custom_field_value(lead, FIELD_PACKAGE_NUMBER)

    tariff = parse_tariff(order_text)
    if not tariff:
        return await _fail(lead_id, "не определён тариф СДЭК (поле 576703)", source, current_tags)

    total = parse_total(order_text)
    if total <= 0:
        return await _fail(lead_id, "не распарсена сумма (поле 576703)", source, current_tags)

    # 2. Контакт + (опционально) компания
    embedded = lead.get("_embedded") or {}
    contact_links = embedded.get("contacts") or []
    if not contact_links:
        return await _fail(lead_id, "у сделки нет контакта", source, current_tags)

    main_contact_id = None
    for cl in contact_links:
        if cl.get("is_main"):
            main_contact_id = cl.get("id")
            break
    if main_contact_id is None:
        main_contact_id = contact_links[0].get("id")
    if main_contact_id is None:
        return await _fail(lead_id, "не удалось определить id контакта", source, current_tags)

    contact = await amo_service.get_contact_by_id(main_contact_id)
    if not contact:
        return await _fail(lead_id, f"не удалось загрузить контакт {main_contact_id}", source, current_tags)

    recipient_name = contact.get("name") or ""
    recipient_phone = amo_service.get_custom_field_value(contact, FIELD_PHONE)
    recipient_email = amo_service.get_custom_field_value(contact, FIELD_EMAIL)

    if not recipient_phone:
        return await _fail(lead_id, "у контакта нет телефона", source, current_tags)

    recipient: dict = {
        "name": recipient_name,
        "phones": [{"number": str(recipient_phone)}],
    }
    if recipient_email:
        recipient["email"] = recipient_email

    company_links = embedded.get("companies") or []
    if company_links:
        company_id = company_links[0].get("id")
        if company_id is not None:
            company = await amo_service.get_company_by_id(company_id)
            if company:
                recipient["company"] = company.get("name") or ""

    # 3. Наложенный платёж
    cod_amount = 0
    if payment_method and "при получении" in str(payment_method).lower():
        cod_amount = total

    # 4. Сборка тела заказа
    lead_name = lead.get("name") or f"#{lead_id}"
    order: dict = {
        "tariff_code": tariff,
        "sender": {
            "company": sender_company or SENDER["company"],
            "name": SENDER["name"],
            "phones": SENDER["phones"],
        },
        "recipient": recipient,
        "from_location": {
            "address": SENDER["address"],
            "country_code": SENDER["country_code"],
            "city": SENDER["city"],
        },
        "packages": [
            {
                "number": str(package_number or lead_id),
                "weight": 300,
                "length": 15,
                "width": 15,
                "height": 5,
                "items": [
                    {
                        "ware_key": "-",
                        "name": lead_name,
                        "payment": {"value": cod_amount},
                        "cost": total,
                        "weight": 200,
                        "amount": 1,
                    }
                ],
            }
        ],
    }

    # 5. Точка доставки
    if tariff in TARIFFS_PVZ:
        pvz = extract_pvz_code(pvz_code_raw)
        if not pvz:
            fallback = amo_service.get_custom_field_value(lead, FIELD_PVZ_CODE_FALLBACK)
            pvz = extract_pvz_code(fallback)
        if not pvz:
            return await _fail(
                lead_id,
                f"не распознан код ПВЗ (поле 576719: {pvz_code_raw!r})",
                source, current_tags,
            )
        order["delivery_point"] = pvz
    elif tariff == TARIFF_DOOR:
        if not delivery_address:
            return await _fail(lead_id, "пустой адрес доставки (поле 577311)", source, current_tags)
        order["to_location"] = {
            "address": delivery_address,
            "country_code": "RU",
        }

    # 6. СДЭК API: создание заказа
    try:
        cdek_resp = await cdek_client.create_order(order)
    except cdek_client.CdekError as exc:
        body_excerpt = exc.body or ""
        return await _fail(
            lead_id,
            f"СДЭК {exc.status or ''}: {exc} {body_excerpt[:200]}".strip(),
            source, current_tags,
        )
    except Exception as exc:
        logger.exception("Unexpected CDEK error for lead %s", lead_id)
        return await _fail(lead_id, f"СДЭК неожиданная ошибка: {exc}", source, current_tags)

    order_uuid = (cdek_resp.get("entity") or {}).get("uuid")
    if not order_uuid:
        return await _fail(lead_id, f"СДЭК не вернул UUID: {cdek_resp}", source, current_tags)

    # 7. Polling cdek_number (до 60 секунд)
    cdek_number = None
    for _ in range(20):
        await asyncio.sleep(3)
        try:
            order_info = await cdek_client.get_order(order_uuid)
        except cdek_client.CdekError:
            continue
        cdek_number = (order_info.get("entity") or {}).get("cdek_number")
        if cdek_number:
            break

    if not cdek_number:
        return await _fail(
            lead_id,
            f"СДЭК не вернул cdek_number за 60с. UUID заказа: {order_uuid}. "
            f"Проверьте кабинет СДЭК и впишите номер вручную, либо удалите дубль перед /retry.",
            source, current_tags,
        )

    cdek_value = str(cdek_number)

    # 8. Записать в AMO + перевести этап + снять тег ошибки
    result = await amo_service.commit_waybill(
        lead_id, cdek_value, current_tags,
        error_tag=TAG_ERROR, target_status=STATUS_WAYBILL_READY,
    )
    if not result.get("ok"):
        critical = (
            f"КРИТИЧНО: сделка {lead_id}, СДЭК UUID={order_uuid} #{cdek_number} создан, "
            f"но AMO не обновлён (status={result.get('status_code')}). Внеси номер вручную."
        )
        logger.error(critical)
        await _alert(critical)
        return {"ok": False, "lead_id": lead_id, "reason": "AMO PATCH failed", "cdek_number": cdek_value, "skipped": False}

    # 9. Примечание со ссылкой на скачивание штрихкода СДЭК
    barcode_url = f"{PUBLIC_BASE_URL}/barcode/{cdek_value}"
    note_res = await amo_service.add_note(
        lead_id, f"Штрихкод СДЭК (№{cdek_value}) — скачать/распечатать: {barcode_url}"
    )
    if not note_res.get("ok"):
        logger.warning("Lead %s: не удалось добавить примечание со ссылкой на штрихкод: %s", lead_id, note_res)

    logger.info("Lead %s waybill created: cdek=%s uuid=%s", lead_id, cdek_number, order_uuid)
    return {"ok": True, "lead_id": lead_id, "reason": None, "cdek_number": cdek_value, "skipped": False}


async def _fail(lead_id, reason: str, source: str, current_tags: list[dict]) -> dict:
    logger.warning("Lead %s waybill creation failed: %s", lead_id, reason)
    # Тег "ошибка накладной" ставим всегда, независимо от source
    if current_tags is not None:
        if not any((t.get("name") or "").strip().lower() == TAG_ERROR.lower() for t in current_tags):
            new_tags = list(current_tags) + [{"name": TAG_ERROR}]
            tag_res = await amo_service.patch_lead(lead_id, tags=new_tags)
            if not tag_res.get("ok"):
                logger.error("Не удалось пометить сделку %s тегом '%s': %s", lead_id, TAG_ERROR, tag_res)
    if source != "retry":
        await _alert(f"Сделка {lead_id}: {reason}")
    return {"ok": False, "lead_id": lead_id, "reason": reason, "cdek_number": None, "skipped": False}


# ---------------------------------------------------------------------------
# Telegram команды
# ---------------------------------------------------------------------------

async def handle_print_command() -> dict:
    """Возвращает dict с ключами:
        ok: bool
        barcodes_pdf: bytes | None
        picking_pdf: bytes | None
        packed_lead_ids: list[int]
        summary: str
        warning: str | None
    """
    leads = await amo_service.get_leads_by_status(STATUS_WAYBILL_READY, with_=("contacts",))
    if not leads:
        return {
            "ok": True,
            "barcodes_pdf": None,
            "picking_pdf": None,
            "packed_lead_ids": [],
            "summary": "В этапе «Готова накладная» сделок нет.",
            "warning": None,
        }

    candidates = [lead for lead in leads if not amo_service.has_tag(lead, TAG_PACKED)]
    if not candidates:
        return {
            "ok": True,
            "barcodes_pdf": None,
            "picking_pdf": None,
            "packed_lead_ids": [],
            "summary": "Все сделки в «Готова накладная» уже помечены как упакованные.",
            "warning": None,
        }

    contact_ids: list[int] = []
    for lead in candidates:
        for cl in (lead.get("_embedded") or {}).get("contacts") or []:
            cid = cl.get("id")
            if cid is not None:
                contact_ids.append(int(cid))
    contacts_map = await amo_service.get_contacts_by_ids(contact_ids) if contact_ids else {}

    picking_data: list[dict] = []
    uuids: list[str] = []
    lead_ids_for_uuids: list[int] = []
    skipped: list[tuple[int, str]] = []
    skipped_with_pdf: list[int] = []
    # candidates с successfully retrieved uuid → пойдут в "уже упакованные" после успешной отправки штрихкодов
    # Те, у кого нет cdek_number, — попадают в picking_data, но в uuids не идут.

    for lead in candidates:
        lead_id = lead.get("id")
        cdek_value = amo_service.get_custom_field_value(lead, FIELD_CDEK_ORDER_NUMBER)
        composition = amo_service.get_custom_field_value(lead, FIELD_COMPOSITION) or ""
        contact_name = "—"
        cl = (lead.get("_embedded") or {}).get("contacts") or []
        if cl:
            cid = cl[0].get("id")
            if cid is not None:
                c = contacts_map.get(int(cid))
                if c:
                    contact_name = c.get("name") or "—"

        picking_data.append({
            "contact_name": contact_name,
            "cdek_number": str(cdek_value) if cdek_value else "—",
            "composition": str(composition).strip(),
        })

        if not cdek_value:
            skipped.append((lead_id, "нет номера СДЭК"))
            continue

        if looks_like_uuid(str(cdek_value)):
            uuids.append(str(cdek_value))
            lead_ids_for_uuids.append(int(lead_id))
        else:
            try:
                uuid = await cdek_client.find_uuid_by_cdek_number(str(cdek_value))
            except cdek_client.CdekError as exc:
                logger.warning("Не удалось резолвить cdek_number=%s в uuid: %s", cdek_value, exc)
                uuid = None
            if uuid:
                uuids.append(uuid)
                lead_ids_for_uuids.append(int(lead_id))
            else:
                skipped.append((lead_id, f"не найден UUID по номеру {cdek_value}"))

    # Лист подбора собираем всегда
    picking_bytes = await asyncio.to_thread(picking_pdf.build_pdf_bytes, picking_data)

    # Штрихкоды — bulk
    barcodes_bytes: bytes | None = None
    barcode_warning: str | None = None
    if uuids:
        try:
            barcodes_bytes = await cdek_client.get_barcodes_batch_pdf(uuids)
        except cdek_client.CdekError as exc:
            logger.error("get_barcodes_batch_pdf failed: %s", exc)
            barcode_warning = f"Штрихкоды СДЭК недоступны: {exc}"
    else:
        barcode_warning = "Нет сделок с резолвенным UUID — штрихкоды не запрашивал."

    summary_parts = [
        f"Готовлю печать: {len(candidates)} сделок.",
        f"С UUID для штрихкодов: {len(uuids)}.",
    ]
    if skipped:
        summary_parts.append("Пропущены (нет в штрихкодах):")
        for lid, reason in skipped[:20]:
            summary_parts.append(f"• {lid}: {reason}")
        if len(skipped) > 20:
            summary_parts.append(f"… и ещё {len(skipped) - 20}")

    return {
        "ok": True,
        "barcodes_pdf": barcodes_bytes,
        "picking_pdf": picking_bytes,
        "packed_lead_ids": lead_ids_for_uuids if barcodes_bytes else [],
        "summary": "\n".join(summary_parts),
        "warning": barcode_warning,
    }


async def mark_leads_packed(lead_ids: list[int]) -> tuple[int, list[int]]:
    """Ставит TAG_PACKED. Возвращает (success_count, failed_lead_ids)."""
    success = 0
    failed: list[int] = []
    for lid in lead_ids:
        res = await amo_service.add_tag(lid, TAG_PACKED)
        if res.get("ok"):
            success += 1
        else:
            failed.append(lid)
    return success, failed


async def handle_retry_command() -> dict:
    """Сводное выполнение /retry. Возвращает dict с summary."""
    leads = await amo_service.get_leads_by_status(STATUS_CREATE_WAYBILL, with_=("contacts",))
    candidates = [lead for lead in leads if amo_service.has_tag(lead, TAG_ERROR)]

    if not candidates:
        return {
            "ok": True,
            "summary": f"Сделок с тегом «{TAG_ERROR}» в этапе «Сделать накладную» нет.",
            "successes": 0,
            "failures": [],
        }

    successes = 0
    failures: list[tuple[int, str]] = []
    for lead in candidates:
        lid = lead.get("id")
        try:
            res = await create_waybill_for_lead(lid, source="retry")
        except Exception as exc:
            logger.exception("retry: unexpected error for lead %s", lid)
            failures.append((lid, f"неожиданная ошибка: {exc}"))
            continue
        if res.get("ok"):
            successes += 1
        else:
            failures.append((lid, res.get("reason") or "неизвестная ошибка"))

    parts = [f"/retry завершён. Успешно: {successes}, с ошибками: {len(failures)}."]
    if failures:
        parts.append("Сделки с ошибками:")
        for lid, reason in failures[:30]:
            parts.append(f"• {lid}: {reason}")
        if len(failures) > 30:
            parts.append(f"… и ещё {len(failures) - 30}")
    return {
        "ok": True,
        "summary": "\n".join(parts),
        "successes": successes,
        "failures": failures,
    }
