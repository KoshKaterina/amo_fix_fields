"""Бизнес-логика создания накладных СДЭК и Telegram-команд /print, /retry."""

import asyncio
import logging
import os
import time
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
    WAYBILL_ZERO_COST_PLACEHOLDER,
    build_cdek_item_name,
    extract_pvz_code,
    is_cod_payment,
    is_prepaid_payment,
    looks_like_uuid,
    parse_tariff,
    parse_total,
)

logger = logging.getLogger("uvicorn")

_alert_callback: Callable[[str], Awaitable[None]] | None = None

# Кулдаун на одинаковые waybill-алерты. Без него каждая неудачная попытка создать
# накладную (сделка застряла в «Сделать накладную» и правится/пересохраняется —
# триггер стреляет на каждое изменение) слала отдельное сообщение в TG → спам.
# Ключ = сам текст алерта (= «Сделка N: причина»), поэтому один и тот же затык по
# одной сделке шумит не чаще раза в кулдаун; критичный алерт с уникальным UUID
# под дедуп фактически не попадает. По аналогии с queue_manager._alert_bg.
WAYBILL_ALERT_COOLDOWN_SECONDS = float(os.getenv("WAYBILL_ALERT_COOLDOWN_SECONDS", "1800"))
_alert_last_sent: dict[str, float] = {}

# Защита ТОЛЬКО от initial-стирания поля 571657 сторонним МойСклад-виджетом
# amgroup сразу после создания накладной (разбор 01.08.2026, сделка 36526319:
# поле стёрлось через 16с после записи). Никакой периодической/часовой
# проверки намеренно нет — поле 571657, пустое ПОЗЖЕ (не сразу после этой
# записи), может быть намеренно очищено оператором для пересоздания накладной
# (см. cdek.md: «если поле уже заполнено — накладная не пересоздаётся») —
# такое трогать нельзя, это не баг, а осознанный /retry.
TREK_VERIFY_DELAY_S = float(os.getenv("TREK_VERIFY_DELAY_S", "60"))

# Сколько ещё терпеливо спрашивать ТОТ ЖЕ заказ СДЭК в фоне, если за первые
# 60с (цикл ниже) он не ответил ни номером, ни явным отказом. Разбор
# 12.08.2026 (сделка 36532789, заказ 06193): 60с — не гарантия ничего, СДЭК
# иногда проставляет cdek_number позже — оба «протаймаутивших» заказа в этом
# разборе на самом деле были приняты СДЭК как валидные отправления. Раньше
# такой таймаут сразу считался отказом и приглашал человека/(эхо-вебхук)
# создать ВТОРОЙ заказ — получался настоящий дубль на реальную посылку.
# Теперь вместо второго заказа просто дольше спрашиваем первый.
WAYBILL_BACKGROUND_POLL_SECONDS = float(os.getenv("WAYBILL_BACKGROUND_POLL_SECONDS", "600"))
WAYBILL_BACKGROUND_POLL_INTERVAL_S = float(os.getenv("WAYBILL_BACKGROUND_POLL_INTERVAL_S", "15"))


def set_alert_callback(fn: Callable[[str], Awaitable[None]]) -> None:
    global _alert_callback
    _alert_callback = fn


async def _alert(text: str) -> None:
    if _alert_callback is None:
        logger.warning("alert callback not set, suppressing: %s", text)
        return
    now = time.monotonic()
    last = _alert_last_sent.get(text)
    if last is not None and now - last < WAYBILL_ALERT_COOLDOWN_SECONDS:
        logger.info("waybill alert cooldown, suppressing duplicate: %s", text)
        return
    # Помечаем ДО отправки (защита от параллельных дублей), но при неудачной
    # доставке откатываем — иначе сбой TG-шлюза заглушил бы алерт на весь кулдаун.
    _alert_last_sent[text] = now
    try:
        ok = await _alert_callback(text)
    except Exception:
        logger.exception("alert callback failed")
        ok = False
    if ok is False and _alert_last_sent.get(text) == now:
        del _alert_last_sent[text]


# ---------------------------------------------------------------------------
# Разбор отказа СДЭК
# ---------------------------------------------------------------------------

def _extract_reject_reason(order_info: dict) -> str | None:
    """Причина отказа СДЭК по заказу, если запрос на создание отклонён (state=INVALID).

    Пример: 'Некорректный телефон получателя' — заказ в кабинете остаётся пустышкой
    без cdek_number, реального отправления нет, дубль удалять не нужно.
    Возвращает None, пока заказ ещё обрабатывается или создан успешно.

    Принимает ВЕСЬ ответ GET /orders/{uuid}: requests лежат рядом с entity,
    а не внутри неё (проверено на живых заказах 29.07.2026).
    """
    requests = order_info.get("requests") or (order_info.get("entity") or {}).get("requests") or []
    for req in requests:
        if (req.get("type") or "").upper() != "CREATE":
            continue
        if (req.get("state") or "").upper() != "INVALID":
            continue
        messages = []
        for err in req.get("errors") or []:
            msg = (err.get("message") or "").strip() or (err.get("code") or "").strip()
            if msg and msg not in messages:
                messages.append(msg)
        return "; ".join(messages) or "причина не указана"
    return None


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
    composition = amo_service.get_custom_field_value(lead, FIELD_COMPOSITION)

    tariff = parse_tariff(order_text)
    if not tariff:
        return await _fail(lead_id, "не определён тариф СДЭК (поле 576703)", source, current_tags)

    total = parse_total(order_text)
    used_cost_placeholder = False
    if total <= 0:
        # Сумма заказа распарсилась в 0: нет строки «Итого» в поле «Заказ» (576703)
        # или она нулевая (замена/гарантия/подарок). Раньше здесь был безусловный
        # отказ («защита от кривых заказов») — менеджер руками ставил стоимость 1.
        # Теперь решаем по способу оплаты (поле 577373):
        if is_cod_payment(payment_method):
            # Наложенный платёж + 0 ₽ — реально кривой заказ: курьер получит с
            # клиента 0. Накладную НЕ создаём, сигналим менеджеру (как раньше).
            return await _fail(
                lead_id,
                "сумма заказа 0 при наложенном платеже — курьер получит 0 ₽ с клиента; "
                "впишите корректную сумму в поле «Заказ» (576703)",
                source, current_tags,
            )
        if is_prepaid_payment(payment_method):
            # Предоплата: сумма для СДЭК — лишь объявленная ценность (товар уже
            # оплачен). Подставляем заглушку, чтобы накладная создалась без ручной
            # правки. Наложки нет → cod_amount останется 0.
            total = WAYBILL_ZERO_COST_PLACEHOLDER
            used_cost_placeholder = True
            logger.warning(
                "Lead %s: сумма заказа 0 при предоплате (%r) — объявленная ценность СДЭК = заглушка %s",
                lead_id, payment_method, WAYBILL_ZERO_COST_PLACEHOLDER,
            )
        else:
            # Способ оплаты пустой/не распознан — не считаем заказ предоплаченным
            # по умолчанию. Держим прежнюю защиту: отказ + сигнал.
            return await _fail(
                lead_id,
                f"сумма заказа 0 и способ оплаты не распознан ({payment_method!r}) — "
                "укажите способ оплаты и/или сумму в поле «Заказ» (576703)",
                source, current_tags,
            )

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
    # Наименование товара — из состава заказа (577313), а НЕ имя сделки: СДЭК требует
    # описание того, что в отгрузке (Катя, 31.07.2026). Количество живёт в тексте
    # наименования, а не в amount: у СДЭК cost и payment считаются ЗА ЕДИНИЦУ, а мы
    # кладём всю сумму заказа одной позицией — amount > 1 умножил бы и наложку, и
    # объявленную ценность.
    item_name = build_cdek_item_name(composition)
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
                        "name": item_name,
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
            return await _fail(lead_id, "пустой адрес доставки (поле 576719)", source, current_tags)
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

    # 7. Polling cdek_number (до 60 секунд).
    #    СДЭК валидирует заказ асинхронно: сразу после POST он висит ACCEPTED, а через
    #    пару секунд либо получает номер, либо падает в INVALID с причиной в requests[].errors.
    #    Отказ ловим сразу — иначе офис минуту ждёт и получает бесполезное "не вернул номер".
    cdek_number = None
    reject_reason = None
    for _ in range(20):
        await asyncio.sleep(3)
        try:
            order_info = await cdek_client.get_order(order_uuid)
        except cdek_client.CdekError:
            continue
        entity = order_info.get("entity") or {}
        cdek_number = entity.get("cdek_number")
        if cdek_number:
            break
        reject_reason = _extract_reject_reason(order_info)
        if reject_reason:
            break

    if not cdek_number:
        if reject_reason:
            return await _fail(
                lead_id,
                f"СДЭК отклонил заказ: {reject_reason}. Отправление НЕ создано, "
                f"в кабинете удалять нечего — исправьте данные и повторите /retry. "
                f"UUID заказа: {order_uuid}.",
                source, current_tags,
            )
        # СДЭК не отказал явно — просто не ответил за 60с. Заказ почти наверняка
        # существует и обрабатывается дольше обычного (разбор 12.08.2026: оба
        # «протаймаутивших» заказа в итоге оказались валидными, принятыми СДЭК).
        # Тег ошибки НЕ ставим (иначе /retry подхватит сделку и создаст поверх
        # ещё живого заказа настоящий дубль) — вместо этого дальше спрашиваем
        # ТОТ ЖЕ order_uuid в фоне.
        note_res = await amo_service.add_note(
            lead_id,
            f"СДЭК не подтвердил заказ за 60с (UUID {order_uuid}) — похоже, просто "
            f"обрабатывает дольше обычного. Ничего создавать/удалять вручную не нужно, "
            f"проверяю в фоне ещё до {WAYBILL_BACKGROUND_POLL_SECONDS:.0f}с — номер "
            f"впишется сам, как только СДЭК ответит.",
        )
        if not note_res.get("ok"):
            logger.warning("Lead %s: не удалось добавить примечание о фоновой проверке: %s", lead_id, note_res)
        logger.warning(
            "Lead %s: СДЭК не ответил за 60с (uuid=%s) — продолжаю спрашивать этот же заказ в фоне вместо retry",
            lead_id, order_uuid,
        )
        asyncio.create_task(
            _resolve_pending_order(lead_id, order_uuid, source, current_tags, used_cost_placeholder)
        )
        return {"ok": False, "lead_id": lead_id, "reason": "pending", "cdek_number": None, "skipped": False}

    cdek_value = str(cdek_number)
    if not await _commit_success(lead_id, order_uuid, cdek_value, current_tags, used_cost_placeholder):
        return {"ok": False, "lead_id": lead_id, "reason": "AMO PATCH failed", "cdek_number": cdek_value, "skipped": False}
    return {"ok": True, "lead_id": lead_id, "reason": None, "cdek_number": cdek_value, "skipped": False}


async def _commit_success(
    lead_id, order_uuid: str, cdek_value: str, current_tags: list[dict], used_cost_placeholder: bool,
) -> bool:
    """Общий финал успешного создания накладной: запись трек-номера в AMO,
    перевод этапа, снятие тега ошибки, примечания (заглушка цены + штрихкод).
    Используется и основным путём (номер пришёл за первые 60с), и фоновым
    дожиданием _resolve_pending_order (номер пришёл позже) — оба ведут себя
    идентично, разница только в том, кто и когда позвал."""
    result = await amo_service.commit_waybill(
        lead_id, cdek_value, current_tags,
        error_tag=TAG_ERROR, target_status=STATUS_WAYBILL_READY,
    )
    if not result.get("ok"):
        critical = (
            f"КРИТИЧНО: сделка {lead_id}, СДЭК UUID={order_uuid} #{cdek_value} создан, "
            f"но AMO не обновлён (status={result.get('status_code')}). Внеси номер вручную."
        )
        logger.error(critical)
        await _alert(critical)
        return False

    asyncio.create_task(_verify_trek_after_delay(lead_id, cdek_value))

    # Прозрачность: если объявленная ценность СДЭК проставлена заглушкой —
    # примечание в сделку, чтобы офис видел (сумма заказа была 0).
    if used_cost_placeholder:
        ph_note = await amo_service.add_note(
            lead_id,
            f"Сумма заказа была 0 (предоплата) — объявленная ценность СДЭК проставлена "
            f"автоматически: {WAYBILL_ZERO_COST_PLACEHOLDER} ₽. Если нужна другая ценность, "
            f"впишите сумму в поле «Заказ» и пересоздайте накладную.",
        )
        if not ph_note.get("ok"):
            logger.warning("Lead %s: не удалось добавить примечание о заглушке цены: %s", lead_id, ph_note)

    # Примечание со ссылкой на скачивание штрихкода СДЭК
    barcode_url = f"{PUBLIC_BASE_URL}/barcode/{cdek_value}"
    note_res = await amo_service.add_note(
        lead_id, f"Штрихкод СДЭК (№{cdek_value}) — скачать/распечатать: {barcode_url}"
    )
    if not note_res.get("ok"):
        logger.warning("Lead %s: не удалось добавить примечание со ссылкой на штрихкод: %s", lead_id, note_res)

    logger.info("Lead %s waybill committed: cdek=%s uuid=%s", lead_id, cdek_value, order_uuid)
    return True


async def _resolve_pending_order(
    lead_id, order_uuid: str, source: str, current_tags: list[dict], used_cost_placeholder: bool,
) -> None:
    """Продолжает спрашивать ТОТ ЖЕ заказ СДЭК после первых 60с — вместо того
    чтобы (как раньше) сдаться и тем самым пригласить создание второго заказа.
    Три исхода: номер пришёл → коммитим как обычный успех (_commit_success);
    СДЭК явно отклонил → ТЕПЕРЬ ставим тег ошибки с настоящей причиной (раньше
    этого шанса просто не было — код сдавался на первой минуте); СДЭК так и не
    ответил за WAYBILL_BACKGROUND_POLL_SECONDS → сдаёмся и алертим человека, но
    без намёка на «удалите дубль» — раз мы не создавали второй заказ, дубля и
    нет, есть один непонятный uuid, который нужно посмотреть в кабинете."""
    deadline = time.monotonic() + WAYBILL_BACKGROUND_POLL_SECONDS
    while time.monotonic() < deadline:
        await asyncio.sleep(WAYBILL_BACKGROUND_POLL_INTERVAL_S)
        try:
            order_info = await cdek_client.get_order(order_uuid)
        except cdek_client.CdekError:
            continue
        entity = order_info.get("entity") or {}
        cdek_number = entity.get("cdek_number")
        if cdek_number:
            await _commit_success(lead_id, order_uuid, str(cdek_number), current_tags, used_cost_placeholder)
            return
        reject_reason = _extract_reject_reason(order_info)
        if reject_reason:
            await _fail(
                lead_id,
                f"СДЭК отклонил заказ: {reject_reason}. Отправление НЕ создано, "
                f"в кабинете удалять нечего — исправьте данные и повторите /retry. "
                f"UUID заказа: {order_uuid}.",
                source, current_tags,
            )
            return

    total_wait = WAYBILL_BACKGROUND_POLL_SECONDS + 60
    await _fail(
        lead_id,
        f"СДЭК так и не ответил за {total_wait:.0f}с (UUID {order_uuid}). Второй заказ "
        f"НЕ создавался — проверьте этот UUID в кабинете СДЭК: если заказ там валиден, "
        f"впишите номер в поле вручную; если заказа нет вообще, тогда уже можно /retry.",
        source, current_tags,
    )


async def _last_field_clear_actor(lead_id, field_id: int) -> int | None:
    """created_by последнего события «поле обнулено» (value_after=[]) для этого
    поля на сделке. amoCRM кодирует роботов/API как created_by=0 и живых
    пользователей — их реальным numeric id (проверено эмпирически на этой же
    сделке 36526319: ручная правка поля 572499 показала created_by=13929334,
    а не 0). None — если подходящее событие не нашлось (не должно случаться,
    раз поле реально пусто, но перестраховываемся, а не гадаем)."""
    params = [
        ("filter[entity]", "lead"),
        ("filter[entity_id]", str(lead_id)),
        ("filter[type]", f"custom_field_{field_id}_value_changed"),
        ("limit", "100"),
    ]
    d = await amo_service._do_get("/api/v4/events", params)
    events = ((d or {}).get("_embedded") or {}).get("events") or []
    cleared = [e for e in events if not (e.get("value_after") or [])]
    if not cleared:
        return None
    latest = max(cleared, key=lambda e: e.get("created_at") or 0)
    return latest.get("created_by")


async def _verify_trek_after_delay(lead_id, cdek_value: str) -> None:
    """Перечитывает сделку через TREK_VERIFY_DELAY_S после записи трек-номера;
    если поле 571657 снова пусто — сверяет, КТО его обнулил (см.
    _last_field_clear_actor), и восстанавливает ТОЛЬКО его (без статуса/тегов),
    только если это сделал робот/интеграция (created_by=0), а не человек. Гард
    против намеренной ручной очистки под /retry (см. cdek.md: «если поле уже
    заполнено — накладная не пересоздаётся» — оператор мог специально обнулить
    поле, чтобы форсировать пересоздание, и это восстанавливать нельзя).
    Значение для восстановления уже известно (мы сами его записали секунды
    назад) — не гадаем по примечаниям/CDEK API. Разбор 01.08.2026, сделка
    36526319: поле стёрлось через 16с после записи — сторонний МойСклад-виджет
    (amgroup), не наш код."""
    await asyncio.sleep(TREK_VERIFY_DELAY_S)
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
    except Exception:
        logger.exception("Lead %s: post-commit trek verify failed to fetch lead", lead_id)
        return
    if not lead:
        return
    current = amo_service.get_custom_field_value(lead, FIELD_CDEK_ORDER_NUMBER)
    if current:
        return

    try:
        actor = await _last_field_clear_actor(lead_id, FIELD_CDEK_ORDER_NUMBER)
    except Exception:
        logger.exception("Lead %s: не удалось определить, кто очистил поле 571657", lead_id)
        actor = None

    if actor is None:
        await _alert(
            f"⚠️ Сделка {lead_id}: трек-номер СДЭК {cdek_value} пропал из поля 571657 через "
            f"{TREK_VERIFY_DELAY_S:.0f}с после записи, но не удалось определить, кто его очистил — "
            f"НЕ восстанавливаю автоматически, проверь вручную."
        )
        return
    if actor != 0:
        logger.info(
            "Lead %s: поле 571657 очистил пользователь %s (не бот) — не трогаю, похоже на намеренный /retry",
            lead_id, actor,
        )
        return

    logger.warning(
        "Lead %s: трек %s пропал из поля 571657 в течение %.0fс после записи (стёр бот/интеграция) — восстанавливаю",
        lead_id, cdek_value, TREK_VERIFY_DELAY_S,
    )
    res = await amo_service.patch_lead(lead_id, custom_fields={FIELD_CDEK_ORDER_NUMBER: cdek_value})
    if res.get("ok"):
        await _alert(
            f"⚠️ Сделка {lead_id}: трек-номер СДЭК {cdek_value} пропал из amo через "
            f"{TREK_VERIFY_DELAY_S:.0f}с после создания накладной (стёрто ботом/интеграцией) — "
            f"восстановлен автоматически."
        )
    else:
        await _alert(
            f"КРИТИЧНО: сделка {lead_id}, трек {cdek_value} пропал из amo и НЕ восстановился "
            f"автоматически ({res}). Внеси номер вручную."
        )


async def _fail(lead_id, reason: str, source: str, current_tags: list[dict]) -> dict:
    logger.warning("Lead %s waybill creation failed: %s", lead_id, reason)
    # Тег "ошибка накладной" ставим всегда, независимо от source
    if current_tags is not None:
        if not any((t.get("name") or "").strip().lower() == TAG_ERROR.lower() for t in current_tags):
            new_tags = list(current_tags) + [{"name": TAG_ERROR}]
            tag_res = await amo_service.patch_lead(lead_id, tags=new_tags)
            if not tag_res.get("ok"):
                logger.error("Не удалось пометить сделку %s тегом '%s': %s", lead_id, TAG_ERROR, tag_res)
    # Причину пишем примечанием в сделку — чтобы менеджер видел, что именно не так,
    # прямо в карточке (не только тег/алерт в TG). Пишем при любом source; каждая
    # попытка оставляет свою запись → в карточке остаётся история причин.
    note_res = await amo_service.add_note(lead_id, f"⚠️ Ошибка создания накладной СДЭК: {reason}")
    if not note_res.get("ok"):
        logger.warning("Lead %s: не удалось добавить примечание с причиной ошибки: %s", lead_id, note_res)
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
