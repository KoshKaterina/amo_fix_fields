"""Счёт СБП (Ozon Pay) из amoCRM — замена виджета int2_ozonpay (MAG-285).

Схема «тех-этап» (запуска Salesbot по API в amo не существует — проверено
17.07.2026, см. JOURNAL ozon-pay): менеджер двигает сделку в «Оплата запрошена»
(тех-этап 87280230 без автоматики) → формируем платёжную ссылку «только СБП»
через Ozon Acquiring API (createPayment, payType=SBP — та же «самостоятельная
интеграция» и те же ключи, что у плагина сайта sunscrypt-sbp), сумма — из
связанного заказа МойСклад (поле 576689 = UUID заказа; sum МС и amount.value
Ozon оба в копейках, 1:1) → ОДНИМ атомарным PATCH пишем ссылку в поле 577617 и
переводим сделку в «Ссылка отправлена» (83537866) — там штатные DP-боты (7173)
шлют клиенту шаблон с уже заполненным полем.

Любая ошибка (нет 576689 / МС недоступен / sum=0 / Ozon отказал / PATCH не
прошёл) → сделка ОСТАЁТСЯ на тех-этапе (видно в воронке): тег «ошибка счёта» +
примечание с причиной + алерт в ТГ ОП с @ответственного менеджера. Клиент в
этом случае не получает ничего — менеджер выставляет счёт вручную и двигает
сделку сам (старый путь остаётся фолбэком).

Идемпотентность: TTL-дедуп по lead_id (amo шлёт add/update пачкой — второй
вебхук в окне не создаёт второй счёт). Повторный вход в тех-этап позже окна —
осознанно новая ссылка (поле перезаписывается, старая протухнет по ttl;
отменять её в Ozon не нужно).
"""

import asyncio
import hashlib
import hmac
import logging
import re
import time

import httpx

import amo_service
import ms_client
import telegram_bot
import tg_recipients
from waybill_config import (
    FIELD_INVOICE_BY_CARD,
    FIELD_INVOICE_OTHER_AMOUNT,
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_LINK,
    OZON_INVOICE_ENABLED,
    OZON_INVOICE_REDIRECT_URL,
    OZON_INVOICE_TTL_S,
    OZON_PAY_ACCESS_KEY,
    OZON_PAY_API_URL,
    OZON_PAY_NOTIFICATION_SECRET_KEY,
    OZON_PAY_SECRET_KEY,
    OZON_RECONCILE_INTERVAL_S,
    OZON_STALE_ALERT_MIN,
    PIPELINE_CLEVER_MAIN,
    PUBLIC_BASE_URL,
    STATUS_LINK_SENT,
    STATUS_PAYMENT_RECEIVED,
    STATUS_PAYMENT_REQUESTED,
    TAG_INVOICE_ERROR,
    looks_like_uuid,
)

logger = logging.getLogger("uvicorn")

AMO_LEAD_URL = "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{}"

# Дедуп-окно: повторные вебхуки одной смены этапа схлопываются, повторный
# вход в этап позже окна — легитимный новый счёт.
RECENT_TTL_S = 120.0
_recent: dict[str, float] = {}

_client: httpx.AsyncClient | None = None


def is_enabled() -> bool:
    """Гейт для вебхука: флаг включён И ключи Ozon заданы."""
    return OZON_INVOICE_ENABLED and bool(OZON_PAY_ACCESS_KEY and OZON_PAY_SECRET_KEY)


def init() -> None:
    """Вызывается из lifespan (async-контекст) — поэтому здесь же поднимаем
    фоновую сверку оплат: ей нужен работающий event loop."""
    global _client
    _client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=30.0))
    start_reconcile()


async def aclose() -> None:
    global _client
    await stop_reconcile()
    if _client is not None:
        await _client.aclose()
        _client = None


def _parse_other_amount(raw) -> tuple[int | None, str]:
    """Поле «Другая сумма» (578141, text): пусто → override нет; иначе число
    В РУБЛЯХ («15 398,50», «15398.5», «15398 ₽») → копейки. Мусор или сумма
    меньше 1 ₽ — ошибка: менеджер явно хотел другую сумму, молча игнорировать
    и выставить счёт на сумму заказа нельзя."""
    s = str(raw or "").strip()
    if not s:
        return None, ""
    cleaned = (
        s.replace("\xa0", "").replace(" ", "")
        .replace("₽", "").replace("р.", "").replace("руб.", "").replace("руб", "")
        .replace(",", ".")
    )
    try:
        value = float(cleaned)
    except ValueError:
        return None, f"не разобрать число из {s!r}"
    kopecks = int(round(value * 100))
    if kopecks < 100:
        return None, f"сумма меньше 1 ₽: {s!r}"
    return kopecks, ""


def _is_checked(raw) -> bool:
    """amo checkbox: заполнено → True/"1"/"on"/"true"; пусто/False/0 → нет."""
    if raw is True:
        return True
    return str(raw or "").strip().lower() in ("1", "on", "true", "yes")


def _sign_create_payment(ext_id: str, access_key: str, secret_key: str) -> str:
    """Подпись createPayment: SHA-256 hex от extId+accessKey+secretKey без
    разделителей (формула подтверждена боевым плагином sunscrypt-sbp)."""
    return hashlib.sha256(f"{ext_id}{access_key}{secret_key}".encode()).hexdigest()


async def _create_payment(
    ext_id: str, amount_kopecks: int, by_card: bool = False, ms_order_name: str = "",
) -> tuple[str | None, str, str]:
    """POST /v1/createPayment. Возвращает (payLink, paymentId, err).

    by_card=False (СБП): payType=SBP без заказа → прямая ссылка qr.nspk.ru.
    by_card=True (оплата картой): создаём платёж ВМЕСТЕ с заказом Ozon → ссылка
    order.item.payLink ведёт на checkout.ozon.ru (страница выбора: карта/СБП/
    Ozon Карта). У Ozon нет «прямой только-карты» ссылки — карта только через
    эту страницу (проверено доке + живой ссылкой 21.07.2026).

    ms_order_name — номер заказа МС (05740). Идёт в НАЧАЛО order.extId, чтобы в
    ЛК Ozon Pay строка читалась как «Заказ 05740_amo-…» и офис матчил
    поступление с заказом (заметила Катя 28.07.2026: прежний «ord-36522883-…»
    нёс id сделки amo и офису не говорил ничего). Ту же грабку прошёл плагин
    сайта в v0.6.0 — там extId заказа начинается с номера Woo.

    Без автоповторов: повторный POST после неясного сбоя может создать второй
    счёт клиенту — при ошибке честно отдаём её менеджеру (fail-путь)."""
    if _client is None:
        return None, "", "httpx-клиент Ozon не инициализирован"
    amount = {"currencyCode": "643", "value": str(amount_kopecks)}
    body = {
        "accessKey": OZON_PAY_ACCESS_KEY,
        "payType": "SBP",
        "amount": amount,
        "extId": ext_id,
        "redirectUrl": OZON_INVOICE_REDIRECT_URL,
        "ttl": OZON_INVOICE_TTL_S,
        "requestSign": _sign_create_payment(ext_id, OZON_PAY_ACCESS_KEY, OZON_PAY_SECRET_KEY),
    }
    if OZON_PAY_NOTIFICATION_SECRET_KEY:
        # Вебхук факта оплаты (этап 2): Ozon пришлёт Completed на наш эндпоинт,
        # и сделка сама уедет в «Оплата получена». notificationUrl per-payment —
        # сайтовые платежи продолжают ходить на URL сайта, не пересекаемся.
        body["notificationUrl"] = f"{PUBLIC_BASE_URL}/ozon_notify"
    if by_card:
        # Сокращённый заказ (формат смока v0.6.1, боевой). order в подпись НЕ
        # входит. expiresAt = ttl, чек не формируем (состава нет).
        expires_at = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(time.time() + OZON_INVOICE_TTL_S))
        # extId заказа: «<номер МС>_<extId платежа>». Префикс — для глаз офиса,
        # хвост «amo-<lead>-<ts>» оставляем как есть: по нему вебхук и находит
        # сделку (_LEAD_IN_EXT_RE ищет вхождение, а не совпадение целиком).
        order_ext_id = ext_id.replace("amo-", "ord-", 1)
        if ms_order_name:
            order_ext_id = f"{ms_order_name}_{ext_id}"
        body["order"] = {
            "extId": order_ext_id,
            "amount": amount,
            "paymentAlgorithm": "PAY_ALGO_SMS",
            "mode": "MODE_SHORTENED",
            "expiresAt": expires_at,
            "successUrl": OZON_INVOICE_REDIRECT_URL,
            "failUrl": OZON_INVOICE_REDIRECT_URL,
            "enableFiscalization": False,
        }
    try:
        resp = await _client.post(f"{OZON_PAY_API_URL}/v1/createPayment", json=body)
    except httpx.RequestError as exc:
        return None, "", f"сеть/таймаут Ozon: {exc.__class__.__name__}"
    if resp.status_code >= 400:
        return None, "", f"Ozon HTTP {resp.status_code}: {resp.text[:200]}"
    try:
        data = resp.json()
    except ValueError:
        return None, "", "Ozon вернул невалидный JSON"
    details = data.get("paymentDetails") or {}
    payment_id = str(details.get("paymentId") or "")
    order = data.get("order") or {}
    if by_card:
        # Оплата картой: ссылка на checkout.ozon.ru (order.item.payLink).
        pay_link = (order.get("item") or {}).get("payLink") or order.get("payLink")
        if not pay_link:
            return None, payment_id, f"оплата картой: в ответе Ozon нет order.item.payLink: {str(data)[:300]}"
        return pay_link, payment_id, ""
    # СБП: order=None, готовая ссылка в paymentDetails.sbp.payload (qr.nspk.ru).
    # Подтверждено боевым ответом 20.07.2026 (сделка 36515681).
    pay_link = order.get("payLink")
    if not pay_link:
        payload = (details.get("sbp") or {}).get("payload")
        if isinstance(payload, str) and payload.startswith("http"):
            pay_link = payload
    if not pay_link:
        return None, payment_id, f"в ответе Ozon нет ни order.payLink, ни sbp.payload: {str(data)[:300]}"
    return pay_link, payment_id, ""


async def _fail(lead: dict, reason: str, detail: str = "") -> None:
    """Счёт не создан/не доставлен: тег + примечание + алерт в ТГ ОП с
    @ответственного (формат и текст «Нет заказа в МС…» — требование Кати)."""
    lead_id = lead.get("id")
    name = lead.get("name") or f"сделка {lead_id}"
    logger.warning("Lead %s: СБП-счёт: %s (%s)", lead_id, reason, detail)
    note = f"⚠️ Счёт СБП: {reason}"
    if detail:
        note += f"\n{detail}"
    await amo_service.add_tag(lead_id, TAG_INVOICE_ERROR)
    await amo_service.add_note(lead_id, note)
    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    await telegram_bot.send_alert(
        f"⚠️ {reason}\n{name}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}",
        chat_id=tg_recipients.NOTIFY_CHAT_ID,
        message_thread_id=tg_recipients.NOTIFY_THREAD_ID,
    )


async def process_invoice_lead(lead_id, source: str = "webhook") -> str:
    """Обработчик очереди (LANE_AMO). Возвращает исход строкой (лог/тесты)."""
    key = str(lead_id)
    now = time.monotonic()
    for k, ts in list(_recent.items()):
        if now - ts > RECENT_TTL_S:
            _recent.pop(k, None)
    if key in _recent:
        logger.info("Lead %s: счёт уже создавался в последние %.0fс — скип (дедуп)", lead_id, RECENT_TTL_S)
        return "skipped-recent"
    _recent[key] = now

    lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        # Сделку не прочитать (amo недоступен?) — молчать нельзя: клиент ждёт
        # ссылку. Алерт на всю смену (ответственного не знаем).
        await telegram_bot.send_alert(
            f"⚠️ Не удалось создать СБП-счёт: сделка {lead_id} не прочиталась из amo\n"
            f"{AMO_LEAD_URL.format(lead_id)}\n{tg_recipients.MANAGERS_ON_SHIFT}",
            chat_id=tg_recipients.NOTIFY_CHAT_ID,
            message_thread_id=tg_recipients.NOTIFY_THREAD_ID,
        )
        return "failed-lead-read"

    # Сделка могла уехать с этапа, пока задача ждала в очереди — не слать.
    if int(lead.get("status_id") or 0) != STATUS_PAYMENT_REQUESTED or \
       int(lead.get("pipeline_id") or 0) != PIPELINE_CLEVER_MAIN:
        logger.info(
            "Lead %s: уже не на «Оплата запрошена» CLEVER (status=%s pipeline=%s) — скип",
            lead_id, lead.get("status_id"), lead.get("pipeline_id"),
        )
        return "skipped-moved"

    # Гейт от дублей: вебхук подписан на update_lead и приходит на ЛЮБОЕ изменение
    # сделки (не только смену этапа), а status_id в нём — просто текущий этап.
    # Стоило Гладкову 20.07 вписать ссылку в поле руками — код создал второй
    # платёж. Правило: 577617 уже заполнено → счёт НЕ создаём (уважаем и ручную
    # ссылку переходного периода). Нужна новая ссылка → очистить поле, любое
    # изменение сделки на тех-этапе создаст свежую.
    existing_link = str(amo_service.get_custom_field_value(lead, FIELD_PAYMENT_LINK) or "").strip()
    if existing_link:
        logger.info("Lead %s: 577617 уже заполнено (%.40s…) — счёт не создаём", lead_id, existing_link)
        return "skipped-link-present"

    ms_uuid = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    if not looks_like_uuid(ms_uuid):
        await _fail(lead, "Нет заказа в МС - невозможно создать оплату",
                    detail=f"поле «ID Заказа» (576689) пусто или не UUID: {ms_uuid!r}")
        return "failed-no-ms-order"

    # «Другая сумма» (578141): заполнено → счёт на неё, а не на сумму заказа.
    # Нечитаемое значение — честная ошибка менеджеру (не молчать и не подменять).
    other_kopecks, other_err = _parse_other_amount(
        amo_service.get_custom_field_value(lead, FIELD_INVOICE_OTHER_AMOUNT)
    )
    if other_err:
        await _fail(lead, "Поле «Другая сумма» заполнено, но не читается - счёт не создан",
                    detail=f"{other_err}. Исправьте сумму или очистите поле.")
        return "failed-other-amount"

    order = await ms_client.get(f"entity/customerorder/{ms_uuid}")
    if not order:
        await _fail(lead, "МойСклад не отдал заказ - счёт не создан",
                    detail=f"customerorder/{ms_uuid}")
        return "failed-ms-fetch"

    if other_kopecks is not None:
        kopecks = other_kopecks
    else:
        kopecks = int(round(float(order.get("sum") or 0)))
    if kopecks <= 0:
        await _fail(lead, "Сумма заказа МС = 0 - счёт не создан",
                    detail=f"заказ МС {order.get('name')}. Либо укажите сумму в поле «Другая сумма».")
        return "failed-zero-sum"

    # «Оплата картой» (578145): галочка → ссылка на checkout.ozon.ru (выбор
    # способа с картой); пусто → прямой СБП.
    by_card = _is_checked(amo_service.get_custom_field_value(lead, FIELD_INVOICE_BY_CARD))

    ext_id = f"amo-{lead_id}-{int(time.time())}"
    pay_link, payment_id, err = await _create_payment(
        ext_id, kopecks, by_card=by_card, ms_order_name=str(order.get("name") or "").strip(),
    )
    if not pay_link:
        await _fail(lead, "Ozon не создал счёт - ссылки нет", detail=err)
        return "failed-ozon"

    # Атомарно: ссылка в 577617 + перевод в «Ссылка отправлена» одним PATCH.
    # DP-боты этапа сработают на переход и прочитают сделку с уже заполненным
    # полем. Упал PATCH → сделка осталась на тех-этапе, ссылка — менеджеру.
    patched = await amo_service.patch_lead(
        lead_id,
        custom_fields={FIELD_PAYMENT_LINK: pay_link},
        status_id=STATUS_LINK_SENT,
        pipeline_id=PIPELINE_CLEVER_MAIN,
    )
    if not patched.get("ok"):
        await _fail(lead, "Ссылка создана, но не записалась в сделку - отправьте клиенту вручную",
                    detail=f"{pay_link}\nextId {ext_id}")
        return "failed-patch"

    rub = kopecks / 100
    rub_str = f"{rub:.2f}".rstrip("0").rstrip(".")
    src = "поле «Другая сумма»" if other_kopecks is not None else f"заказ МС {order.get('name')}"
    kind = "Счёт (оплата картой, страница выбора Ozon)" if by_card else "Счёт СБП"
    await amo_service.add_note(
        lead_id,
        f"{kind} создан автоматически: {rub_str} ₽ ({src}), действителен "
        f"{OZON_INVOICE_TTL_S // 3600} ч.\n{pay_link}\nextId {ext_id}"
        + (f"\npaymentId {payment_id}" if payment_id else ""),
    )
    logger.info("Lead %s: счёт создан (%s коп., by_card=%s, extId %s), сделка → «Ссылка отправлена»",
                lead_id, kopecks, by_card, ext_id)
    return "created"


# ---------------------------------------------------------------------------
# Этап 2: факт оплаты → сделка едет в «Оплата получена» (боты этапа шлют
# «спасибо» как при ручном переводе). ДВА независимых пути:
#
#   1) вебхук /ozon_notify — быстрый, но НЕнадёжный. Живой тест 28.07.2026:
#      при оплате КАРТОЙ (checkout.ozon.ru, order-флоу) уведомление не приходит
#      вовсе — счёт в ЛК «Оплачен», сделка молча висит на «Ссылка отправлена»
#      (карта 0/2, СБП 6/6 за те же сутки).
#   2) реконсиляция — фоновый опрос Ozon по висящим счетам (getPaymentDetails).
#      Основной путь: не зависит ни от причуд order-флоу, ни от потерянных
#      вебхуков СБП (сеть моргнула, рестарт контейнера).
#
# Идемпотентность общая: _paid_recent + естественная (после перевода сделка
# уходит с этапа и в выборку сверки больше не попадает).
# ---------------------------------------------------------------------------

# lead_id из extId. ИЩЕТ ВХОЖДЕНИЕ, не совпадение целиком: extId заказа теперь
# начинается с номера МС («05740_amo-36523057-1785242820»), а у старых счетов
# встречается «ord-<lead>-<ts>». Обе формы находятся одной регуляркой.
_LEAD_IN_EXT_RE = re.compile(r"(?:amo|ord)-(\d+)-(\d+)")

# paymentId и extId из примечания «Счёт … создан автоматически» — другого места,
# где живёт paymentId, у нас нет (поле в amo под него не заводили).
_NOTE_PAYMENT_RE = re.compile(r"paymentId\s+(\S+)")
_NOTE_EXT_RE = re.compile(r"extId\s+(\S+)")

# Идемпотентность вебхука: Ozon может ретраить уведомление — повторный
# Completed по тому же extId в окне не должен дублировать перевод/примечания.
PAID_TTL_S = 3600.0
_paid_recent: dict[str, float] = {}

_notify_tasks: set = set()

# Сделки, по которым уже кричали «оплаты нет» — чтобы алерт был один, а не
# каждый цикл. Живёт в памяти процесса: после рестарта допустимо повторить.
_stale_alerted: set[int] = set()

_reconcile_task: asyncio.Task | None = None


def _lead_and_ts_from_ext(*values) -> tuple[int | None, int | None]:
    """(lead_id, ts) из первого подходящего extId — платежа или заказа."""
    for value in values:
        match = _LEAD_IN_EXT_RE.search(str(value or ""))
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None


def verify_notification(data: dict) -> bool:
    """Подпись уведомления Ozon — обе боевые формулы плагина sunscrypt-sbp
    (подтверждены на реальных payload 09.07.2026), сравнение constant-time."""
    if not OZON_PAY_NOTIFICATION_SECRET_KEY:
        return False
    received = str(data.get("requestSign") or "")
    if not received:
        return False
    amount = str(data.get("amount") if data.get("amount") is not None else "")
    currency = str(data.get("currencyCode") or "")
    ext_tx = str(data.get("extTransactionID") or "")
    ext_ord = str(data.get("extOrderID") or data.get("extOrderId") or "")
    order_id = str(data.get("orderID") or data.get("orderId") or "")
    tx_id = str(data.get("transactionID") or "")
    secret = OZON_PAY_NOTIFICATION_SECRET_KEY
    sig_self = hashlib.sha256(
        f"{OZON_PAY_ACCESS_KEY}|||{ext_tx}|{amount}|{currency}|{secret}".encode()
    ).hexdigest()
    sig_attempt = hashlib.sha256(
        f"{OZON_PAY_ACCESS_KEY}|{order_id}|{tx_id}|{ext_ord}|{amount}|{currency}|{secret}".encode()
    ).hexdigest()
    return hmac.compare_digest(sig_self, received) or hmac.compare_digest(sig_attempt, received)


def handle_notification_bg(payload: dict) -> None:
    """Из вебхука: быстрый ответ, обработка фоном (PATCH идёт через api-пайплайн)."""
    task = asyncio.create_task(_handle_notification(payload))
    _notify_tasks.add(task)
    task.add_done_callback(_notify_tasks.discard)


async def _mark_paid(lead: dict, rub_str: str, marker: str, source: str) -> str:
    """Общий путь «оплата подтверждена» для вебхука и сверки: перевести сделку
    в «Оплата получена» + примечание. Сделка уже дальше — только примечание."""
    lead_id = lead.get("id")
    cur_status = int(lead.get("status_id") or 0)
    if cur_status in (STATUS_PAYMENT_REQUESTED, STATUS_LINK_SENT) and \
       int(lead.get("pipeline_id") or 0) == PIPELINE_CLEVER_MAIN:
        patched = await amo_service.patch_lead(
            lead_id, status_id=STATUS_PAYMENT_RECEIVED, pipeline_id=PIPELINE_CLEVER_MAIN,
        )
        if patched.get("ok"):
            await amo_service.add_note(
                lead_id,
                f"Оплата подтверждена Ozon ({source}): {rub_str} ₽. "
                f"Сделка переведена в «Оплата получена» автоматически.\n{marker}",
            )
            logger.info("ozon %s: lead %s оплачен (%s ₽) → «Оплата получена»", source, lead_id, rub_str)
            return "moved"
        await amo_service.add_note(
            lead_id,
            f"Оплата подтверждена Ozon ({source}): {rub_str} ₽, "
            f"но перевести сделку не вышло - переведите вручную.\n{marker}",
        )
        logger.error("ozon %s: lead %s оплачен, но PATCH не прошёл", source, lead_id)
        return "failed-patch"

    await amo_service.add_note(
        lead_id,
        f"Оплата подтверждена Ozon ({source}): {rub_str} ₽. "
        f"Сделка уже не на этапе оплаты (status {cur_status}) - не двигаю.\n{marker}",
    )
    logger.info("ozon %s: lead %s оплачен (%s ₽), сделка на %s — только примечание",
                source, lead_id, rub_str, cur_status)
    return "noted"


def _seen_paid(key: str) -> bool:
    """TTL-дедуп по extId/paymentId: True — уже отрабатывали недавно."""
    now = time.monotonic()
    for k, ts in list(_paid_recent.items()):
        if now - ts > PAID_TTL_S:
            _paid_recent.pop(k, None)
    if key in _paid_recent:
        return True
    _paid_recent[key] = now
    return False


async def _handle_notification(data: dict) -> str:
    if not isinstance(data, dict):
        return "ignored-shape"
    if not verify_notification(data):
        logger.warning("ozon_notify: невалидная подпись, игнорирую: %s", str(data)[:200])
        return "ignored-bad-sign"

    ext_tx = str(data.get("extTransactionID") or "")
    ext_ord = str(data.get("extOrderID") or data.get("extOrderId") or "")
    # Ищем сделку и по платежу, и по ЗАКАЗУ: при оплате картой наш идентификатор
    # уезжает в extOrderID (плагин сайта прошёл это же в v0.6.0).
    lead_id, _ = _lead_and_ts_from_ext(ext_tx, ext_ord)
    if lead_id is None:
        # Не наш extId (например, платёж сайта, если вебхук укажут глобально).
        logger.info("ozon_notify: extId %r/%r не наш формат — скип", ext_tx, ext_ord)
        return "ignored-foreign"

    status = str(data.get("status") or "")
    if status != "Completed":
        logger.info("ozon_notify: lead %s extId %s статус %r — не Completed, скип", lead_id, ext_tx, status)
        return "ignored-status"

    if _seen_paid(ext_tx or ext_ord):
        logger.info("ozon_notify: повторный Completed по %s — скип (дедуп)", ext_tx or ext_ord)
        return "skipped-duplicate"

    try:
        rub = int(round(float(data.get("amount") or 0))) / 100
    except (TypeError, ValueError):
        rub = 0
    rub_str = f"{rub:.2f}".rstrip("0").rstrip(".")

    lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        logger.error("ozon_notify: оплата Completed по %s, но сделка %s не прочиталась", ext_tx, lead_id)
        return "failed-lead-read"

    return await _mark_paid(lead, rub_str, f"extId {ext_tx or ext_ord}", "вебхук")


# ---------------------------------------------------------------------------
# Реконсиляция: сами спрашиваем Ozon по висящим счетам.
# ---------------------------------------------------------------------------

def _sign_get_details(payment_id: str) -> str:
    """Подпись getPaymentDetails: id + accessKey + secretKey (ТЗ §6.1, та же
    формула в боевом плагине сайта — active_resolve)."""
    return hashlib.sha256(f"{payment_id}{OZON_PAY_ACCESS_KEY}{OZON_PAY_SECRET_KEY}".encode()).hexdigest()


async def get_payment_status(payment_id: str) -> tuple[str, int | None, str]:
    """POST /v1/getPaymentDetails → (status, amount_kopecks|None, err).

    Схема ответа у Ozon плавает — статус ищем в тех же местах, что и плагин
    сайта. Не нашли → возвращаем ошибку И логируем сырой ответ: при карте это
    единственный способ увидеть, как выглядит оплаченный order-флоу."""
    if _client is None:
        return "", None, "httpx-клиент Ozon не инициализирован"
    body = {
        "id": payment_id,
        "accessKey": OZON_PAY_ACCESS_KEY,
        "requestSign": _sign_get_details(payment_id),
    }
    try:
        resp = await _client.post(f"{OZON_PAY_API_URL}/v1/getPaymentDetails", json=body)
    except httpx.RequestError as exc:
        return "", None, f"сеть/таймаут Ozon: {exc.__class__.__name__}"
    if resp.status_code >= 400:
        return "", None, f"Ozon HTTP {resp.status_code}: {resp.text[:200]}"
    try:
        data = resp.json()
    except ValueError:
        return "", None, "Ozon вернул невалидный JSON"

    details = data.get("paymentDetails") or {}
    payment = data.get("payment") or {}
    order = data.get("order") or {}
    status = (data.get("status") or details.get("status")
              or payment.get("status") or order.get("status") or "")
    amount_raw = (data.get("amount") or details.get("amount")
                  or payment.get("amount") or order.get("amount"))
    if isinstance(amount_raw, dict):
        amount_raw = amount_raw.get("value")
    try:
        kopecks = int(round(float(amount_raw))) if amount_raw is not None else None
    except (TypeError, ValueError):
        kopecks = None

    if not status:
        logger.warning("ozon сверка: статус не распознан в ответе getPaymentDetails: %s", str(data)[:400])
        return "", kopecks, "статус не распознан"
    return str(status), kopecks, ""


async def _payment_ref(lead_id) -> tuple[str, str, int | None]:
    """(payment_id, ext_id, когда выставлен счёт) из свежего примечания сделки."""
    notes = await amo_service.get_lead_notes(lead_id)
    payment_id = ext_id = ""
    created_at = None
    for note in notes:  # свежие в конце — побеждает последнее совпадение
        text = ((note.get("params") or {}).get("text")) or ""
        if "создан автоматически" not in text:
            continue
        match_payment = _NOTE_PAYMENT_RE.search(text)
        match_ext = _NOTE_EXT_RE.search(text)
        if match_payment:
            payment_id = match_payment.group(1)
        if match_ext:
            ext_id = match_ext.group(1)
        created_at = note.get("created_at")
    return payment_id, ext_id, created_at


async def _stale_alert(lead: dict, created_at: int | None, status: str) -> None:
    """Счёт выставлен, оплаты нет дольше порога → один алерт в ТГ ОП."""
    if OZON_STALE_ALERT_MIN <= 0 or not created_at:
        return
    age_min = (time.time() - float(created_at)) / 60
    if age_min < OZON_STALE_ALERT_MIN:
        return
    lead_id = lead.get("id")
    if lead_id in _stale_alerted:
        return
    _stale_alerted.add(lead_id)
    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    await telegram_bot.send_alert(
        f"⏳ Счёт висит без оплаты {int(age_min)} мин (статус Ozon: {status or 'неизвестен'})\n"
        f"{lead.get('name') or f'сделка {lead_id}'}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}",
        chat_id=tg_recipients.NOTIFY_CHAT_ID,
        message_thread_id=tg_recipients.NOTIFY_THREAD_ID,
    )


async def _reconcile_once() -> str:
    """Один проход: сделки с выставленным счётом на этапах оплаты → спросить
    Ozon → Completed двигаем, зависшие подсвечиваем алертом."""
    checked = moved = 0
    for status_id in (STATUS_LINK_SENT, STATUS_PAYMENT_REQUESTED):
        for lead in await amo_service.get_leads_by_status(status_id, with_=()):
            if int(lead.get("pipeline_id") or 0) != PIPELINE_CLEVER_MAIN:
                continue
            link = str(amo_service.get_custom_field_value(lead, FIELD_PAYMENT_LINK) or "").strip()
            if not link:
                continue  # счёта нет — сверять нечего (менеджер ещё не запросил оплату)

            lead_id = lead.get("id")
            payment_id, ext_id, created_at = await _payment_ref(lead_id)
            if not payment_id:
                # Счёт выставлен руками/старым виджетом — paymentId неизвестен.
                continue
            if (ext_id or payment_id) in _paid_recent:
                continue

            checked += 1
            status, kopecks, err = await get_payment_status(payment_id)
            if err:
                logger.warning("ozon сверка: lead %s paymentId %s — %s", lead_id, payment_id, err)
                continue

            if status != "Completed":
                await _stale_alert(lead, created_at, status)
                continue

            _seen_paid(ext_id or payment_id)
            rub = (kopecks or 0) / 100
            rub_str = f"{rub:.2f}".rstrip("0").rstrip(".") if kopecks else "?"
            marker = f"extId {ext_id}" if ext_id else f"paymentId {payment_id}"
            if await _mark_paid(lead, rub_str, marker, "сверка") == "moved":
                moved += 1
                _stale_alerted.discard(lead_id)

    logger.info("Ozon сверка: проверено счетов %s, переведено сделок %s", checked, moved)
    return f"checked={checked} moved={moved}"


async def _reconcile_loop() -> None:
    while True:
        await asyncio.sleep(OZON_RECONCILE_INTERVAL_S)
        try:
            await _reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Ozon сверка: ошибка прохода")


def start_reconcile() -> None:
    global _reconcile_task
    if not is_enabled() or OZON_RECONCILE_INTERVAL_S <= 0:
        logger.info("Ozon сверка оплат ВЫКЛЮЧЕНА (enabled=%s, interval=%s)",
                    is_enabled(), OZON_RECONCILE_INTERVAL_S)
        return
    _reconcile_task = asyncio.create_task(_reconcile_loop())
    logger.info("Ozon сверка оплат: каждые %s сек, алерт о зависших через %s мин",
                OZON_RECONCILE_INTERVAL_S, OZON_STALE_ALERT_MIN or "—")


async def stop_reconcile() -> None:
    global _reconcile_task
    if _reconcile_task is not None:
        _reconcile_task.cancel()
        try:
            await _reconcile_task
        except asyncio.CancelledError:
            pass
        _reconcile_task = None
