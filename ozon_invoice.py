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

import hashlib
import logging
import time

import httpx

import amo_service
import ms_client
import telegram_bot
import tg_recipients
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_LINK,
    OZON_INVOICE_ENABLED,
    OZON_INVOICE_REDIRECT_URL,
    OZON_INVOICE_TTL_S,
    OZON_PAY_ACCESS_KEY,
    OZON_PAY_API_URL,
    OZON_PAY_SECRET_KEY,
    PIPELINE_CLEVER_MAIN,
    STATUS_LINK_SENT,
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
    global _client
    _client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=30.0))


async def aclose() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _sign_create_payment(ext_id: str, access_key: str, secret_key: str) -> str:
    """Подпись createPayment: SHA-256 hex от extId+accessKey+secretKey без
    разделителей (формула подтверждена боевым плагином sunscrypt-sbp)."""
    return hashlib.sha256(f"{ext_id}{access_key}{secret_key}".encode()).hexdigest()


async def _create_payment(ext_id: str, amount_kopecks: int) -> tuple[str | None, str, str]:
    """POST /v1/createPayment (payType=SBP). Возвращает (payLink, paymentId, err).

    Без автоповторов: повторный POST после неясного сбоя может создать второй
    счёт клиенту — при ошибке честно отдаём её менеджеру (fail-путь)."""
    if _client is None:
        return None, "", "httpx-клиент Ozon не инициализирован"
    body = {
        "accessKey": OZON_PAY_ACCESS_KEY,
        "payType": "SBP",
        "amount": {"currencyCode": "643", "value": str(amount_kopecks)},
        "extId": ext_id,
        "redirectUrl": OZON_INVOICE_REDIRECT_URL,
        "ttl": OZON_INVOICE_TTL_S,
        "requestSign": _sign_create_payment(ext_id, OZON_PAY_ACCESS_KEY, OZON_PAY_SECRET_KEY),
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
    pay_link = (data.get("order") or {}).get("payLink")
    if not pay_link:
        # Платёж «без заказа» (наш случай): Ozon отдаёт order=None, а готовая
        # ссылка лежит в paymentDetails.sbp.payload (https://qr.nspk.ru/…) —
        # на телефоне открывает приложение банка, на десктопе QR. Подтверждено
        # боевым ответом 20.07.2026 (сделка 36515681).
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

    order = await ms_client.get(f"entity/customerorder/{ms_uuid}")
    if not order:
        await _fail(lead, "МойСклад не отдал заказ - счёт не создан",
                    detail=f"customerorder/{ms_uuid}")
        return "failed-ms-fetch"

    kopecks = int(round(float(order.get("sum") or 0)))
    if kopecks <= 0:
        await _fail(lead, "Сумма заказа МС = 0 - счёт не создан",
                    detail=f"заказ МС {order.get('name')}")
        return "failed-zero-sum"

    ext_id = f"amo-{lead_id}-{int(time.time())}"
    pay_link, payment_id, err = await _create_payment(ext_id, kopecks)
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
    await amo_service.add_note(
        lead_id,
        f"Счёт СБП создан автоматически: {rub_str} ₽ (заказ МС {order.get('name')}), действителен "
        f"{OZON_INVOICE_TTL_S // 3600} ч.\n{pay_link}\nextId {ext_id}"
        + (f"\npaymentId {payment_id}" if payment_id else ""),
    )
    logger.info("Lead %s: СБП-счёт создан (%s коп., extId %s), сделка → «Ссылка отправлена»",
                lead_id, kopecks, ext_id)
    return "created"
