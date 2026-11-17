"""amoCRM → Яндекс.Метрика CDP (сквозная аналитика).

Логика (согласовано):
  - id заказа в Метрике = id ОРИГИНАЛЬНОЙ сделки из воронки CLEVER Основная.
    Дубликат (Офис/Фулфилмент) → оригинал резолвится по полю 576689 (UUID МойСклад).
  - Определение типа оплаты по полю 577373 «Способ оплаты»:
        наложка (COD) = содержит «при получении / наличны / эвотор / наложен».
    Пустой способ оплаты → пропускаем, кроме отмены.
  - Маппинг статусов:
        CANCELLED  — этап 143 в рознице/Офисе и подтверждённый розничный
                     заказ в Листе ожидания (даже поверх PAID).
        IN_PROGRESS — CLEVER, любой не-терминальный этап.
        PAID (предоплата) — CLEVER «Успешно реализовано» (142).
        PAID (наложка)   — Офис «Успешно реализовано» (142)
                           ИЛИ Фулфилмент «09. Доставлено» / «09.2 Платёж отправлен владельцу».
  - Сумма (revenue) пишется только при PAID.

С 08.07.2026 вебхук НЕ триггерит синк (решение Кати: Метрике/Woo реальное время
не нужно — Метрика принимает офлайн-конверсии задним числом, Woo-комиссия
некритична к часам; вебхучный путь давал 51% задач очереди и по 2 GET amo на
событие). Вместо этого — сверка по расписанию:
  - интрадей: в METRIKA_INTRADAY_HOURS (МСК, по умолчанию 3,9,15,21),
    окно — с прошлого успешного прохода с перехлёстом 15 мин;
  - ночная (01:00 МСК): полное окно RECONCILE_DAYS — источник истины.
Оба прохода гонят Метрику И Woo одним набором сделок (reconcile_window). Тем же
проходом идёт страховка статусов InSales для Tangemshop (insales_status_sync).
"""

import asyncio
import datetime
import logging
import hashlib
from pathlib import Path
from collections import Counter

from shared.metrika_delivery import DeliveryStore
import os
import time

import alerts
import amo_service
import migration_freeze
import metrika_client
import metrika_payment_evidence
from waybill_config import (
    FIELD_EMAIL,
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_METHOD,
    FIELD_SITE_ORDER_NUMBER,
    FIELD_PHONE,
    FIELD_YM_CLIENT_ID,
    METRIKA_COUNTER_ID,
    METRIKA_SINCE_TS,
    METRIKA_TOKEN,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_DB_WORK,
    PIPELINE_OFFICE,
    PIPELINE_WAITLIST,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
    is_cod_payment,
    is_tangemshop_lead,
)

logger = logging.getLogger("uvicorn")

# Ночная сверка: окно и время запуска.
RECONCILE_DAYS = 14
NIGHTLY_HOUR_MSK = 1  # 01:00 МСК

# Интрадей-сверка (замена вебхучного пути, 08.07.2026): часы запуска МСК.
# 3,9,15,21 — выбор Кати (равномерно каждые 6ч; ночная полная в 01:00 отдельно).
_intraday_raw = os.getenv("METRIKA_INTRADAY_HOURS", "3,9,15,21")
INTRADAY_HOURS_MSK = sorted(
    {int(h) % 24 for h in _intraday_raw.split(",") if h.strip().isdigit()}
)
# Перехлёст окна: страхует правки, случившиеся во время прошлого прохода.
INTRADAY_OVERLAP_SECONDS = 900

# Метрика ждёт даты как LocalDateTime (НЕ unix). Счётчик в МСК.
_MSK = datetime.timezone(datetime.timedelta(hours=3))


def _fmt_dt(ts) -> str | None:
    try:
        return datetime.datetime.fromtimestamp(int(ts), tz=_MSK).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return None


_enabled = False
_counter_id: int | None = None
_nightly_task: asyncio.Task | None = None
_intraday_task: asyncio.Task | None = None
# Unix-время старта последнего УСПЕШНОГО прохода сверки (любого). Интрадей
# берёт окно отсюда: упавший/пропущенный проход растягивает следующее окно —
# дыр не остаётся. В памяти: после рестарта первый проход берёт дефолтное окно.
_last_reconcile_ts: int | None = None
# Сверки не должны пересекаться (ночная и интрадей на границе суток и т.п.).
_reconcile_lock = asyncio.Lock()

# Only metadata and hashes persist; fresh CRM reads reconstruct every retry.
_delivery = DeliveryStore(Path(__file__).parent / "var" / "metrika_delivery.sqlite3")
# Do not flood API while Yandex is validating a file or after a transport failure.
_RETRY_AFTER_SECONDS = 3600
_last_run = {}


def delivery_health() -> dict:
    result = {"enabled": _enabled, "last_run": dict(_last_run)}
    try:
        result["validation"] = _delivery.counts(_counter_id) if _enabled else {}
    except Exception as exc:
        result["store_error"] = type(exc).__name__
    return result


def _validated(receipt: dict | None, uploading_id: str) -> bool:
    return bool(receipt and uploading_id
                and str(receipt.get("uploading_id") or "") == uploading_id
                and receipt.get("api_validation_status") == "PASSED"
                and receipt.get("elements_count") == 1)


async def _retail_origin(lead: dict) -> bool:
    """Allow a moved website order, never every lead in the waitlist.

    A previously attempted order or a status event in retail proves lineage.
    A numeric Woo order ID is required for historical, unregistered orders.
    """
    if _delivery.get(_counter_id, lead["id"]):
        return True
    if not str(_cf(lead, FIELD_SITE_ORDER_NUMBER) or "").strip().isdigit():
        return False
    for page in range(1, 6):
        data = await amo_service._do_get("/api/v4/events", [
            ("filter[entity]", "lead"), ("filter[entity_id][]", str(lead["id"])),
            ("filter[type]", "lead_status_changed"), ("limit", "100"), ("page", str(page))])
        if data is None:
            raise metrika_client.MetrikaError("CRM order origin unavailable")
        events = (data.get("_embedded") or {}).get("events") or []
        for event in events:
            if str(event.get("entity_id")) != str(lead["id"]):
                continue
            for field in ("value_before", "value_after"):
                for value in event.get(field) or []:
                    status = value.get("lead_status") or {}
                    if status.get("pipeline_id") == PIPELINE_CLEVER_MAIN:
                        return True
        if len(events) < 100 and not (data.get("_links") or {}).get("next"):
            return False
    raise metrika_client.MetrikaError("CRM order origin history incomplete")


async def _deliver(row: dict, lead_id: int) -> str:
    oid = row["id"]
    # Include timestamps: correcting a conversion date is a meaningful update.
    signature = hashlib.sha256((str(_counter_id) + "|" + _row_signature(row) + "|"
        + str(row.get("create_date_time")) + "|" + str(row.get("update_date_time"))
        + "|" + str(row.get("finish_date_time"))).encode()).hexdigest()
    previous = _delivery.get(_counter_id, oid)
    now = time.time()
    if previous:
        if previous["state"] == "validated" and previous["signature"] == signature:
            return "unchanged"
        if previous["state"] != "validated" and now - previous["attempted_at"] < _RETRY_AFTER_SECONDS:
            # Wait for an older in-flight upload even if CRM changed meanwhile.
            return "pending"
    # Persist BEFORE the network request: crashes and HTTP failures remain retryable.
    _delivery.put(_counter_id, oid, lead_id, signature, "pending", None, now)
    response = await metrika_client.upload_simple_order(_counter_id, row)
    receipt = response.get("uploading") or {}
    uid = str(receipt.get("uploading_id") or "")
    if not uid:
        raise metrika_client.MetrikaError("Metrika upload missing receipt ID")
    state = "validated" if _validated(receipt, uid) else "pending"
    if receipt.get("api_validation_status") == "FAILED":
        state = "failed"
    _delivery.put(_counter_id, oid, lead_id, signature, state, uid, now)
    if state == "failed":
        raise metrika_client.MetrikaError("Metrika upload validation FAILED")
    return state


async def _check_pending_receipts() -> None:
    pending = _delivery.pending(_counter_id)
    ids = {r["uploading_id"] for r in pending if r["uploading_id"]}
    if not ids:
        return
    receipts = await metrika_client.get_upload_receipts(_counter_id, ids)
    for row in pending:
        uid = row["uploading_id"]
        receipt = receipts.get(uid)
        if _validated(receipt, uid):
            _delivery.confirm(_counter_id, row["order_id"], uid)
        elif receipt and receipt.get("api_validation_status") == "FAILED":
            await _alert(f"Metrika: validation FAILED for upload {uid}")



def is_enabled() -> bool:
    return _enabled


def _row_signature(row: dict) -> str:
    """Подпись значимого состояния заказа (без волатильных дат) — ключ дедупа отправок."""
    parts = (
        row.get("order_status"),
        row.get("revenue"),
        row.get("client_ids"),
        row.get("emails"),
        row.get("phones"),
        row.get("client_uniq_id"),
    )
    return "|".join("" if p is None else str(p) for p in parts)


async def init() -> None:
    """Проверка токена + определение counter id. Требует amo-кэш не нужен."""
    global _enabled, _counter_id
    if not METRIKA_TOKEN:
        logger.warning("Metrika sync: METRIKA_TOKEN не задан — синхронизация ВЫКЛЮЧЕНА")
        return

    await metrika_client.init()

    counter = METRIKA_COUNTER_ID
    if counter is None:
        try:
            counters = await metrika_client.get_counters()
        except Exception as exc:
            await _alert(
                f"Metrika sync: не удалось получить список счётчиков: {exc} — ВЫКЛЮЧЕНА",
                "metrika_sync_counters_failed", {"ошибка": str(exc)},
            )
            await metrika_client.aclose()
            return
        if len(counters) == 1:
            counter = counters[0].get("id")
            logger.info("Metrika sync: счётчик определён автоматически: %s", counter)
        else:
            await _alert(
                f"Metrika sync: в аккаунте {len(counters)} счётчиков — задай METRIKA_COUNTER_ID. ВЫКЛЮЧЕНА",
                "metrika_sync_counter_ambiguous", {"сколько": len(counters)},
            )
            await metrika_client.aclose()
            return

    _counter_id = counter
    _enabled = True
    logger.info("Metrika sync включён (counter=%s)", _counter_id)

    global _nightly_task, _intraday_task
    _nightly_task = asyncio.create_task(_nightly_loop())
    logger.info("Metrika: ночная сверка в %02d:00 МСК, окно %s дн.", NIGHTLY_HOUR_MSK, RECONCILE_DAYS)
    if INTRADAY_HOURS_MSK:
        _intraday_task = asyncio.create_task(_intraday_loop())
        logger.info(
            "Metrika: интрадей-сверка (Метрика+Woo) в %s МСК — вебхучный путь отключён",
            ",".join(f"{h:02d}:00" for h in INTRADAY_HOURS_MSK),
        )
    else:
        logger.warning("Metrika: METRIKA_INTRADAY_HOURS пуст — интрадей-сверка выключена, только ночная")


async def shutdown() -> None:
    global _enabled, _nightly_task, _intraday_task
    _enabled = False
    for task in (_nightly_task, _intraday_task):
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    _nightly_task = None
    _intraday_task = None
    await metrika_client.aclose()
    logger.info("Metrika sync stopped")


async def _alert(text: str, event: str | None = None, values: dict | None = None) -> None:
    logger.error(text)
    try:
        from telegram_bot import send_alert
        body, kw = text, {}
        if event:
            d = alerts.decide(event, legacy_text=text, values=values or {})
            if d is None:
                logger.info("%s: уведомление выключено в панели", event)
                return
            body, kw = d.text, d.send_kwargs()
        await send_alert(body, **kw)
    except Exception:
        logger.exception("%s", "Metrika alert failed: " + text)


def _cf(entity: dict, field_id: int):
    return amo_service.get_custom_field_value(entity, field_id)


def _classify(pipeline_id, status_id, cod: bool) -> tuple[str | None, bool]:
    """Возвращает (order_status, need_resolve_clever).

    need_resolve_clever=True → событие пришло из дубликата, id заказа надо взять
    из оригинала в CLEVER.
    """
    if status_id == STATUS_CLOSED_LOST:
        return "CANCELLED", pipeline_id != PIPELINE_CLEVER_MAIN

    # Картотека «Работа с базой» (07.09.2026) считается так же, как розница:
    # менеджер обзвона доводит продажу на месте, и закрытие там — та же продажа.
    # ⚠️ На Метрику это НЕ влияет: у неё свой гейт воронок в process_sync ниже,
    # и картотеки там нет — конверсии продолжают уходить только из розницы и
    # Офиса. Ветка нужна Woo: он спрашивает «оплачено?» именно здесь, а от
    # статуса заказа на сайте зависит начисление реферальной комиссии.
    if pipeline_id in (PIPELINE_CLEVER_MAIN, PIPELINE_DB_WORK):
        if status_id == STATUS_SUCCESS and not cod:
            return "PAID", False
        if status_id not in (STATUS_SUCCESS, STATUS_CLOSED_LOST):
            return "IN_PROGRESS", False
        return None, False

    # До 31.07.2026 два условия ниже требовали cod=True: предоплата закрывалась
    # PAID'ом в CLEVER/142 (копия оставалась там навсегда), а в Офисе/ФФ жили
    # только копии, где ждать надо было именно наложку. С переходом на ПЕРЕНОС
    # (office_transfer) предоплаченная сделка тоже уезжает из CLEVER и закрывается
    # в Офисе/ФФ — PAID на закрытии нужен для всех способов оплаты. Основной
    # захват предоплаты — синк ДО переноса в office_transfer; это страховочная
    # сетка (поля заполнили позже / синк в момент переноса упал). Для старых
    # не-cod копий даст повторный PAID по оригиналу — безопасно: Метрика делает
    # upsert по id заказа, Woo completed→completed идемпотентен.
    if pipeline_id == PIPELINE_OFFICE and status_id == STATUS_SUCCESS:
        return "PAID", True

    return None, False


async def process_sync(payload: dict, lead: dict | None = None, *, preview: bool = False) -> str | dict | None:
    if not _enabled:
        return
    lead_id = payload["lead_id"]
    if lead is None:
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
    if not lead:
        return

    # Окно миграции воронок: перенос старой сделки в 142 — не конверсия.
    # Проверка нужна и здесь: сверка по расписанию берёт сделки по updated_at,
    # мимо вебхука.
    if await migration_freeze.skip(lead_id, "Metrika", lead=lead):
        return

    pipeline_id = lead.get("pipeline_id")
    status_id = lead.get("status_id")

    moved_cancellation = (pipeline_id == PIPELINE_WAITLIST
                          and status_id == STATUS_CLOSED_LOST)
    if pipeline_id not in (PIPELINE_CLEVER_MAIN, PIPELINE_OFFICE) and not moved_cancellation:
        return

    # ⚠️ Заказы магазина tangemshop.ru (29.09.2026). Гейта по воронке выше тут
    # НЕ хватает: сделка Tangemshop, закрытая как УР, уезжает в Офис (см.
    # office_transfer, источник за флагом OFFICE_TRANSFER_SOURCE_TANGEMSHOP) и
    # с этого момента от розничной по воронке неотличима. Дальше _resolve_clever
    # не находит оригинал в рознице и с 31.07.2026 в этом случае считает
    # канонической саму сделку, а идентификатор клиента ей не нужен - хватает
    # id контакта. Без этого отсева выручка чужого магазина ушла бы офлайн-
    # конверсией в счётчик Метрики Sunscrypt. У tangemshop.ru свой счётчик,
    # и заводить его здесь никто не просил.
    if is_tangemshop_lead(lead):
        logger.info("Metrika: сделка %s — заказ Tangemshop, в счётчик Sunscrypt не шлём", lead_id)
        return

    if moved_cancellation and not await _retail_origin(lead):
        return
    payment = _cf(lead, FIELD_PAYMENT_METHOD)
    # Cancellation of a known order must propagate even if payment was cleared.
    if status_id != STATUS_CLOSED_LOST and not str(payment or "").strip():
        logger.info("Metrika: order %s has no payment method, skipped", lead_id)
        return
    cod = is_cod_payment(payment)
    order_status, need_resolve = _classify(pipeline_id, status_id, cod)
    if moved_cancellation:
        # History/registry proves this very ID was the retail original.
        order_status, need_resolve = "CANCELLED", False
    elif not order_status:
        need_resolve = pipeline_id != PIPELINE_CLEVER_MAIN

    # Каноническая сделка — всегда оригинал в CLEVER (источник id, ym, суммы, дат).
    if need_resolve:
        canonical = await _resolve_clever(lead)
        if not canonical:
            # Для старых/архивных дублей оригинал в CLEVER может не находиться —
            # это ожидаемый пропуск, не ошибка. Без алерта, чтобы не спамить TG.
            logger.warning(
                "Metrika: не нашёл оригинал в CLEVER для сделки %s "
                "(UUID МойСклад=%r, статус %s) — пропуск",
                lead_id, _cf(lead, FIELD_MOYSKLAD_ORDER_UUID), order_status,
            )
            return
    else:
        canonical = lead

    metrika_order_id = canonical.get("id")

    # Гард по дате старта интеграции: исторические заказы и массовые правки
    # старья (созданные до METRIKA_SINCE) в Метрику не тащим.
    if METRIKA_SINCE_TS is not None:
        created = canonical.get("created_at")
        if created is not None and int(created) < METRIKA_SINCE_TS:
            logger.info(
                "Metrika: заказ %s создан до старта интеграции (created_at=%s) — пропуск",
                metrika_order_id, created,
            )
            return

    payment_evidence = (metrika_payment_evidence.cached(canonical)
                        if order_status != "CANCELLED" else None)
    if order_status != "CANCELLED" and (payment_evidence or order_status != "PAID"):
        payment_evidence = await metrika_payment_evidence.for_lead(canonical)
        if payment_evidence:
            order_status = "PAID"
            logger.info("Metrika: original order %s payment confirmed (%s)",
                        metrika_order_id, payment_evidence.source)
    if not order_status:
        return

    ym = str(_cf(canonical, FIELD_YM_CLIENT_ID) or "").strip()
    contact_id, email, phone = await _contact_info(canonical)
    # Метрике нужен хотя бы один идентификатор клиента (client_uniq_id тоже годится).
    if not (ym or email or phone or contact_id):
        logger.warning("Metrika: заказ %s без идентификаторов клиента — пропуск", metrika_order_id)
        return

    now = int(time.time())
    # Время события — у триггерящей сделки (для COD это момент доставки в дубликате).
    # Для терминальных статусов берём closed_at (amo ставит его при входе в 142/143):
    # сверка идёт с задержкой до ~3ч, и updated_at к её моменту может быть сдвинут
    # более поздней правкой сделки — время конверсии уезжало бы (вплоть до соседних
    # суток). ФФ-этапы 09/09.2 (COD PAID) сделку не закрывают → closed_at там нет,
    # фолбэк на updated_at — задокументированное ограничение.
    event_ts = lead.get("updated_at") or now
    if order_status in ("PAID", "CANCELLED") and lead.get("closed_at"):
        event_ts = lead["closed_at"]
    if payment_evidence:
        event_ts = payment_evidence.timestamp
    event_dt = _fmt_dt(event_ts)

    row: dict = {
        "id": metrika_order_id,
        "create_date_time": _fmt_dt(canonical.get("created_at") or now),
        "update_date_time": event_dt,
        "order_status": order_status,
        "currency": "RUB",
    }
    if ym:
        row["client_ids"] = ym
    if email:
        row["emails"] = email
    if phone:
        row["phones"] = phone
    if contact_id:
        row["client_uniq_id"] = contact_id
    if order_status in ("PAID", "CANCELLED"):
        row["finish_date_time"] = event_dt
    # revenue пишем ВСЕГДА явно: при PAID — сумма заказа, иначе 0. Это
    # (а) обнуляет выручку при CANCELLED (правило согласовано), (б) при
    # merge_mode=UPDATE гарантированно перезаписывает прежнее значение, а не
    # оставляет «прилипшую» сумму от старого PAID.
    row["revenue"] = (canonical.get("price") or 0) if order_status == "PAID" else 0
    if payment_evidence:
        # Confirm the original order, not the manager's additional basket/sum.
        row["revenue"] = payment_evidence.original_total

    if preview:
        return row
    try:
        outcome = await _deliver(row, lead_id)
        logger.info("Metrika: order %s %s (%s)", metrika_order_id, order_status, outcome)
        return outcome
    except metrika_client.MetrikaError as exc:
        await _alert(
            f"Metrika: ошибка загрузки заказа {metrika_order_id}: {exc}",
            "metrika_order_load_failed", {"номер": metrika_order_id, "ошибка": str(exc)},
        )
        return "failed"


async def _resolve_clever(dup_lead: dict) -> dict | None:
    """Находит оригинал в CLEVER Основной по UUID заказа МойСклад (576689)."""
    uuid = str(_cf(dup_lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    if not uuid:
        return None
    # Не путать сбой поиска с подтверждённым отсутствием оригинала.
    candidates = await amo_service.find_leads_by_query(uuid, with_=("contacts",))
    if candidates is None:
        raise metrika_client.MetrikaError("CRM canonical order lookup unavailable")
    for cand in candidates:
        if cand.get("pipeline_id") == PIPELINE_CLEVER_MAIN and str(
            _cf(cand, FIELD_MOYSKLAD_ORDER_UUID) or ""
        ).strip() == uuid:
            return cand
    # Сиблинга-оригинала в CLEVER нет. До 31.07.2026 здесь был return None
    # («старьё/архив — пропуск»). С переходом на ПЕРЕНОС (office_transfer)
    # это штатный случай: сделка САМА и есть оригинал, переехавший из CLEVER, —
    # UUID МойСклад у неё на руках, она canonical. Побочно чинит старых
    # сирот-копий (оригинал в архиве): раньше молча выпадали из Метрики/Woo.
    return dup_lead


async def _contact_info(lead: dict) -> tuple[int | None, str | None, str | None]:
    """Возвращает (contact_id, email, phone) основного контакта сделки."""
    embedded = (lead.get("_embedded") or {}).get("contacts") or []
    cid = None
    for c in embedded:
        if c.get("is_main"):
            cid = c.get("id")
            break
    if cid is None and embedded:
        cid = embedded[0].get("id")
    if cid is None:
        return None, None, None

    contact = await amo_service.get_contact_by_id(cid)
    if not contact:
        return cid, None, None

    email = amo_service.get_custom_field_value(contact, FIELD_EMAIL)
    phone = amo_service.get_custom_field_value(contact, FIELD_PHONE)
    email = str(email).strip().lower() if email else None
    # Базовая валидация: битый контакт не должен ронять весь заказ. CDP отклоняет
    # ВСЮ строку с 400, если телефон/email невалидны (напр. тестовое «111»).
    # Невалидное значение опускаем — заказ уйдёт по остальным идентификаторам.
    if email and ("@" not in email or "." not in email.rsplit("@", 1)[-1]):
        email = None
    phone = "".join(ch for ch in str(phone) if ch.isdigit()) if phone else None
    if phone and not (10 <= len(phone) <= 15):
        phone = None
    return cid, email, phone


# ---------------------------------------------------------------------------
# Ночная сверка — источник истины. Догоняет пропущенные/задебаунсенные вебхуки.
# ---------------------------------------------------------------------------

async def reconcile_window(days: int = RECONCILE_DAYS, since_ts: int | None = None) -> None:
    """Пересинкивает все сделки 3 воронок, изменённые с since_ts (unix) либо
    за последние `days` дней, если since_ts не задан.

    Идемпотентно (upsert по id). Переиспользует process_sync — статусы и резолв
    считаются той же логикой, что и раньше в realtime. Проходы не пересекаются
    (_reconcile_lock); успешный проход двигает _last_reconcile_ts — от него
    интрадей берёт следующее окно.
    """
    global _last_reconcile_ts
    if not _enabled:
        return
    async with _reconcile_lock:
        run_started = int(time.time())
        since = since_ts if since_ts is not None else run_started - days * 86400
        leads_by_id: dict[int, dict] = {}
        fetch_failed = False
        retry_ids = {row["lead_id"] for row in _delivery.pending(_counter_id)}
        _last_run.clear()
        _last_run.update(started_at=run_started, running=True)
        try:
            await _check_pending_receipts()
        except metrika_client.MetrikaError:
            logger.exception("Metrika: could not check pending upload receipts")
            fetch_failed = True
        # Durable retries survive both the moving window and process restart.
        for pipeline in (PIPELINE_CLEVER_MAIN, PIPELINE_OFFICE):
            try:
                batch = await amo_service.get_leads_updated_since(pipeline, since, with_=("contacts",))
            except Exception:
                logger.exception("Metrika reconcile: ошибка выборки воронки %s", pipeline)
                fetch_failed = True
                continue
            # None = сбой выборки/обрыв пагинации (сеть/429/брейкер): amo_service
            # НЕ бросает исключений, а возвращает None — иначе fetch_failed была бы
            # мёртвой веткой и метка окна двигалась бы по непрочитанным данным.
            if batch is None:
                logger.warning("Metrika reconcile: выборка воронки %s не удалась — окно не сдвинем", pipeline)
                fetch_failed = True
                continue
            for ld in batch:
                lid = ld.get("id")
                if lid is not None:
                    leads_by_id[lid] = ld

        for lid in retry_ids - leads_by_id.keys():
            try:
                lead = await amo_service.get_lead_full(lid, with_=("contacts",))
            except Exception:
                logger.exception("Metrika: retry source unavailable for lead %s", lid)
                fetch_failed = True
                continue
            if lead:
                leads_by_id[lid] = lead
            else:
                fetch_failed = True

        # Only cancellations in this follow-up pipeline; no Woo/InSales side effects.
        extra_ids = set()
        try:
            batch = await amo_service.get_leads_updated_since(PIPELINE_WAITLIST, since, with_=("contacts",))
        except Exception:
            logger.exception("Metrika: waitlist source unavailable")
            batch = None
        if batch is None:
            fetch_failed = True
        else:
            for lead in batch:
                if lead.get("status_id") == STATUS_CLOSED_LOST:
                    extra_ids.add(lead["id"])
                    leads_by_id[lead["id"]] = lead

        window_h = (run_started - since) / 3600
        logger.info(
            "Metrika reconcile: %s сделок за окно %.1f ч — старт", len(leads_by_id), window_h
        )
        # Ленивый импорт: woo_status_sync импортирует metrika_sync на уровне модуля,
        # поэтому здесь — внутри функции, чтобы не было кольцевого импорта.
        import woo_status_sync
        import insales_status_sync
        outcomes = Counter()
        for lid, ld in leads_by_id.items():
            try:
                outcome = await process_sync({"lead_id": lid}, lead=ld)
                outcomes[outcome or "skipped"] += 1
                if outcome == "failed":
                    fetch_failed = True
            except Exception:
                logger.exception("Metrika reconcile: ошибка по сделке %s", lid)
                outcomes["failed"] += 1
                fetch_failed = True
            # Woo — тем же проходом, последовательно после Метрики (та же сделка
            # уже в памяти, повторного запроса в amo нет). Своя обработка ошибок.
            if lid not in extra_ids and ld.get("pipeline_id") in (PIPELINE_CLEVER_MAIN, PIPELINE_OFFICE) and woo_status_sync.is_enabled():
                try:
                    await woo_status_sync.process_sync({"lead_id": lid}, lead=ld)
                except Exception:
                    logger.exception("Woo reconcile: ошибка по сделке %s", lid)
            # InSales — страховка вебхука: заказ Tangemshop, чья сделка закрылась в
            # Офисе, пока контейнер пересобирался. Сделки не наши модуль отсеет сам.
            if lid not in extra_ids and ld.get("pipeline_id") in (PIPELINE_CLEVER_MAIN, PIPELINE_OFFICE) and insales_status_sync.is_enabled():
                try:
                    await insales_status_sync.process_lead(lid, lead=ld, source="reconcile")
                except Exception:
                    logger.exception("InSales reconcile: ошибка по сделке %s", lid)
        _last_run.clear()
        _last_run.update(at=run_started, orders=len(leads_by_id), outcomes=dict(outcomes),
                         fetch_failed=fetch_failed, running=False)
        logger.info("Metrika reconcile outcomes: %s", dict(outcomes))
        # Двигаем метку только если выборка прошла по всем воронкам: упавшая
        # воронка не должна выпасть из следующего окна.
        if fetch_failed:
            logger.warning(
                "Metrika reconcile: были сбои выборки — окно НЕ сдвинуто, "
                "следующий проход покроет этот интервал повторно"
            )
        else:
            _last_reconcile_ts = run_started


def _seconds_until_next(hour_msk: int) -> float:
    now = datetime.datetime.now(_MSK)
    nxt = now.replace(hour=hour_msk, minute=0, second=0, microsecond=0)
    if nxt <= now:
        nxt += datetime.timedelta(days=1)
    return (nxt - now).total_seconds()


def _seconds_until_next_of(hours_msk: list[int]) -> float:
    """До ближайшего из списка часов (МСК)."""
    return min(_seconds_until_next(h) for h in hours_msk)


def _prev_slot_ts() -> int:
    """Unix-время ПРЕДЫДУЩЕГО слота общего расписания (интрадей + ночной).

    Окно первого прохода после рестарта: «с предыдущего слота», а не фиксированные
    N часов — иначе рестарт в длинном ночном зазоре (01:00 → 10:00) оставлял бы
    дыру покрытия до следующей ночной сверки."""
    hours = sorted(set(INTRADAY_HOURS_MSK) | {NIGHTLY_HOUR_MSK})
    now = datetime.datetime.now(_MSK)
    for days_back in (0, 1):
        day = now - datetime.timedelta(days=days_back)
        for h in reversed(hours):
            cand = day.replace(hour=h, minute=0, second=0, microsecond=0)
            if cand < now:
                return int(cand.timestamp())
    return int((now - datetime.timedelta(days=1)).timestamp())


async def _nightly_loop() -> None:
    while True:
        try:
            await asyncio.sleep(_seconds_until_next(NIGHTLY_HOUR_MSK))
        except asyncio.CancelledError:
            raise
        try:
            await reconcile_window(RECONCILE_DAYS)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Metrika: ночная сверка упала")


async def _intraday_loop() -> None:
    """Интрадей-сверка Метрика+Woo — замена вебхучного пути (08.07.2026).

    Окно — с прошлого успешного прохода минус перехлёст; упавший или
    пропущенный запуск просто растягивает следующее окно. После рестарта
    последний проход неизвестен → берём предыдущий слот расписания
    (_prev_slot_ts; плюс ночная полная сверка добирает всё в пределах
    RECONCILE_DAYS)."""
    import api
    while True:
        try:
            await asyncio.sleep(_seconds_until_next_of(INTRADAY_HOURS_MSK))
        except asyncio.CancelledError:
            raise
        # Аналитике реальное время не нужно (08.07.2026): при забитом пайплайне
        # пропускаем слот целиком. Окно следующего прохода просто растянется —
        # механика «пропущенный запуск растягивает окно» описана в докстринге,
        # а ночная полная сверка добирает всё в пределах RECONCILE_DAYS.
        if api.skip_if_congested("metrika intraday"):
            continue
        try:
            base = _last_reconcile_ts if _last_reconcile_ts is not None else _prev_slot_ts()
            await reconcile_window(since_ts=base - INTRADAY_OVERLAP_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Metrika: интрадей-сверка упала")
