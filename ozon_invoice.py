"""Счёт СБП (Ozon Pay) из amoCRM — замена виджета int2_ozonpay (MAG-285).

Схема «тех-этап» (запуска Salesbot по API в amo не существует — проверено
17.07.2026, см. JOURNAL ozon-pay): менеджер двигает сделку в «Оплата запрошена»
(тех-этап 87280230 без автоматики) → формируем платёжную ссылку «только СБП»
через Ozon Acquiring API (createPayment, payType=SBP — та же «самостоятельная
интеграция» и те же ключи, что у плагина сайта sunscrypt-sbp), сумма — из
связанного заказа МойСклад (поле 576689 = UUID заказа; sum МС и amount.value
Ozon оба в копейках, 1:1) или из поля «Другая сумма» (578141), и тогда заказ МС
вообще не нужен (Катя 28.09.2026) → ОДНИМ атомарным PATCH пишем ссылку в поле 577617 и
переводим сделку в «Ссылка отправлена» (83537866) — там штатные DP-боты (7173)
шлют клиенту шаблон с уже заполненным полем.

Способ оплаты (577373) со словом «крипт», «usdt»/«ustd», «trc» или «wallet» —
ссылку НЕ создаём вовсе (Катя 28.09.2026): такие оплаты идут мимо эквайринга.
Это не ошибка, поэтому без тега и алерта — только примечание в карточке, и
сторож зависших счетов такие сделки тоже пропускает.

Сделка не заполнена — счёт не выставляем и МОЛЧИМ (Катя 28.09.2026): пуст способ
оплаты, либо разом пусты бюджет, «Другая сумма» и заказ МС. Это не сбой, а работа
менеджера, которая ещё не сделана. Раньше такие сделки падали в ошибку, а наша же
запись двигала updated_at, сторож видел «новую версию» и пробовал снова — петля
на восемь одинаковых примечаний за два часа (сделка 36389097).

Любая ошибка (нет 576689 и пуста «Другая сумма» / МС недоступен / sum=0 / Ozon отказал / PATCH не
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
import datetime
import hashlib
import hmac
import logging
import re
import time

import httpx

import amo_service
import alerts
import ms_client
import telegram_bot
import tg_recipients
from waybill_config import (
    FIELD_INVOICE_BY_CARD,
    FIELD_INVOICE_OTHER_AMOUNT,
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_LINK,
    FIELD_PAYMENT_METHOD,
    OZON_INVOICE_ENABLED,
    OZON_INVOICE_REDIRECT_URL,
    OZON_INVOICE_TTL_S,
    OZON_PAY_ACCESS_KEY,
    OZON_PAY_API_URL,
    OZON_PAY_NOTIFICATION_SECRET_KEY,
    OZON_PAY_SECRET_KEY,
    OZON_RECONCILE_INTERVAL_S,
    OZON_ALERT_WINDOW_END_H,
    OZON_ALERT_WINDOW_START_H,
    OZON_NO_LINK_ALERT_MIN,
    OZON_NO_LINK_MAX_QUIET_MIN,
    OZON_NO_LINK_RETRY_MIN,
    OZON_STALE_ALERT_MIN,
    OZON_INVOICE_ACADEMY,
    OZON_INVOICE_DB_WORK,
    OZON_PAYMENT_STAGES,
    OZON_STALE_ESCALATE_CHAT_ID,
    OZON_STALE_ESCALATE_DAYS,
    OZON_STALE_EVENING_H,
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    PIPELINE_DB_WORK,
    PUBLIC_BASE_URL,
    TAG_INVOICE_ERROR,
    blocked_invoice_payment_token,
    looks_like_uuid,
)

logger = logging.getLogger("uvicorn")

AMO_LEAD_URL = "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{}"

_MSK = datetime.timezone(datetime.timedelta(hours=3))

# Дедуп-окно: повторные вебхуки одной смены этапа схлопываются, повторный
# вход в этап позже окна — легитимный новый счёт.
RECENT_TTL_S = 120.0
_recent: dict[str, float] = {}

_client: httpx.AsyncClient | None = None

# Разовые фоновые задачи старта (валидация этапов). Держим ссылки, иначе
# asyncio может собрать задачу сборщиком мусора на полпути.
_init_tasks: set = set()


def is_enabled() -> bool:
    """Гейт для вебхука: флаг включён И ключи Ozon заданы."""
    return OZON_INVOICE_ENABLED and bool(OZON_PAY_ACCESS_KEY and OZON_PAY_SECRET_KEY)


def _invoice_pipelines() -> tuple[int, ...]:
    """Воронки, где выставляем счёт. Розница всегда, картотека «Работа с базой» —
    за флагом OZON_INVOICE_DB_WORK (07.09.2026), Академия — за OZON_INVOICE_ACADEMY
    (09.09.2026, обучение продаётся по той же схеме).

    Флаги читаем на КАЖДОМ вызове, а не собираем кортеж на импорте: иначе флаг,
    подменённый в тестах (и в консоли при разборе инцидента), не подействовал бы.
    Тот же приём, что в office_transfer._source_pipelines()."""
    out = [PIPELINE_CLEVER_MAIN]
    if OZON_INVOICE_DB_WORK:
        out.append(PIPELINE_DB_WORK)
    if OZON_INVOICE_ACADEMY:
        out.append(PIPELINE_ACADEMY)
    return tuple(out)


def _stages(pipeline_id, *, flagged: bool = True) -> tuple[int, int, int] | None:
    """Тройка этапов оплаты воронки: (тех-этап входа, ссылка отправлена, оплата
    получена). None — воронка не наша.

    flagged=False снимает проверку флага и оставляет только «воронка вообще
    умеет в оплату». Нужно там, где деньги клиента уже списаны: выключенный
    флаг не должен мешать довести оплаченную сделку до конца."""
    try:
        pid = int(pipeline_id)
    except (TypeError, ValueError):
        return None
    if flagged and pid not in _invoice_pipelines():
        return None
    return OZON_PAYMENT_STAGES.get(pid)


def is_invoice_entry(pipeline_id, status_id) -> bool:
    """Публичный гейт для webhooks.py: сделка вошла в тех-этап воронки, где мы
    выставляем счёт. Воронки в вебхуке может не быть — тех-этапы у всех воронок
    свои, по одному этапу решение однозначно, а process_invoice_lead всё равно
    перечитает сделку и проверит пару целиком."""
    try:
        sid = int(status_id)
    except (TypeError, ValueError):
        return False
    if pipeline_id is None:
        return any(
            (stages := OZON_PAYMENT_STAGES.get(pid)) and sid == stages[0]
            for pid in _invoice_pipelines()
        )
    stages = _stages(pipeline_id)
    return stages is not None and sid == stages[0]


async def _validate_stages() -> None:
    """Этапы из карты существуют в amo? Кэш воронок к этому моменту прогрет
    (lifespan зовёт warm_pipeline_cache раньше), запросов не стоит.

    Ловит опечатку в ID и переименование этапа заказчиком до того, как это
    заметит менеджер по молчащему счёту: без этапа гейт просто никогда не
    совпадёт, и фича будет тихо мертва."""
    missing = []
    for pipeline_id in _invoice_pipelines():
        for status_id in OZON_PAYMENT_STAGES.get(pipeline_id) or ():
            if amo_service.get_status_sort(status_id, pipeline_id) is None:
                missing.append(f"{pipeline_id}/{status_id}")
    if missing:
        msg = (
            "ozon_invoice: не найдены в прогретом кэше воронок этапы оплаты: "
            f"{', '.join(missing)} — проверьте ID в waybill_config.py "
            "(переименовали/пересоздали этап?)"
        )
        logger.error(msg)
        d = alerts.decide(
            "ozon_invoice_stages_missing", legacy_text=f"⚠️ {msg}",
            chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID,
            values={"этапы": ", ".join(missing)},
        )
        if d is not None:
            await telegram_bot.send_alert(d.text, **d.send_kwargs())


def init() -> None:
    """Вызывается из lifespan (async-контекст) — поэтому здесь же поднимаем
    фоновую сверку оплат: ей нужен работающий event loop."""
    global _client
    _client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=30.0))
    if is_enabled():
        task = asyncio.create_task(_validate_stages())
        _init_tasks.add(task)
        task.add_done_callback(_init_tasks.discard)
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


def build_order_ext_id(ext_id: str, ms_order_name: str) -> str:
    """extId заказа Ozon для оплаты картой. Одна формула на два места: тело
    createPayment и примечание в сделке (оттуда его берёт сверка)."""
    return f"{ms_order_name}_{ext_id}" if ms_order_name else ext_id.replace("amo-", "ord-", 1)


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
        order_ext_id = build_order_ext_id(ext_id, ms_order_name)
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


# Одна и та же причина отказа по одной сделке — не чаще раза в час. Страховка от
# петли «наш же _fail меняет сделку → сторож видит новую версию → новая попытка →
# новый _fail» (сделка 36389097, 28.09.2026: восемь одинаковых примечаний за два
# часа). Память процесса: после пересборки контейнера отсчёт начинается заново.
_failed_recent: dict[tuple, float] = {}
_FAIL_REPEAT_GAP_S = 3600.0


async def _fail(lead: dict, reason: str, detail: str = "") -> None:
    """Счёт не создан/не доставлен: тег + примечание + алерт в ТГ ОП с
    @ответственного (формат и текст «Нет заказа в МС…» — требование Кати).

    Повтор той же причины по той же сделке в течение часа — только строка в лог:
    менеджеру от десятого одинакового примечания пользы нет, а карточку оно
    забивает так, что живую переписку в ней не найти."""
    lead_id = lead.get("id")
    name = lead.get("name") or f"сделка {lead_id}"
    logger.warning("Lead %s: СБП-счёт: %s (%s)", lead_id, reason, detail)
    now = time.time()
    for k, ts in list(_failed_recent.items()):
        if now - ts > _FAIL_REPEAT_GAP_S:
            _failed_recent.pop(k, None)
    key = (int(lead_id or 0), reason)
    if now - _failed_recent.get(key, 0.0) < _FAIL_REPEAT_GAP_S:
        logger.info("Lead %s: та же причина отказа в течение часа — примечание и алерт не дублируем", lead_id)
        return
    _failed_recent[key] = now
    note = f"⚠️ Счёт СБП: {reason}"
    if detail:
        note += f"\n{detail}"
    await amo_service.add_tag(lead_id, TAG_INVOICE_ERROR)
    await amo_service.add_note(lead_id, note)
    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    d = alerts.decide(
        "ozon_invoice_failed",
        legacy_text=f"⚠️ {reason}\n{name}\n{AMO_LEAD_URL.format(lead_id)}\n{mentions}",
        chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID, lead=lead,
        responsible_id=lead.get("responsible_user_id"),
        values={
            "причина": reason,
            "сделка": lead.get("name") or "",
            "ссылка_на_сделку": alerts.lead_link(lead_id),
            "теги": mentions,
        },
    )
    if d is not None:
        await telegram_bot.send_alert(d.text, **d.send_kwargs())


# Кому уже объяснили, что по этому способу оплаты ссылки не будет. Вебхук
# update_lead прилетает на ЛЮБУЮ правку сделки, а сделка с криптой может стоять
# на этапе долго — без этого окна в карточку насыпалась бы пачка одинаковых
# примечаний. Память процесса: после пересборки контейнера объясним ещё раз, и
# это дешевле, чем лишний запрос примечаний на каждый вебхук.
_blocked_noted: dict[int, float] = {}
_BLOCKED_NOTE_GAP_S = 6 * 3600


async def _note_blocked_once(lead_id, payment_method: str) -> None:
    """Объяснить менеджеру в карточке, почему ссылки не будет — не чаще раза в
    шесть часов на сделку."""
    key = int(lead_id)
    now = time.time()
    for k, ts in list(_blocked_noted.items()):
        if now - ts > _BLOCKED_NOTE_GAP_S:
            _blocked_noted.pop(k, None)
    if now - _blocked_noted.get(key, 0.0) < _BLOCKED_NOTE_GAP_S:
        return
    _blocked_noted[key] = now
    await amo_service.add_note(
        lead_id,
        f"Ссылка на оплату не создаётся: способ оплаты «{payment_method}».\n"
        "По криптовалютным способам счёт Ozon Pay не выставляем — оплата идёт мимо "
        "эквайринга. Нужна ссылка Ozon Pay — поменяйте способ оплаты и заведите "
        "сделку на этап «Оплата запрошена» заново.",
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
    # ⚠️ Ключ дедупа НЕ ставим здесь (23.09.2026, разбор сделки 36553383).
    # Раньше окно занимал любой заход, включая отказной: «поле уже заполнено»,
    # «уехала с этапа», «сделку не прочитать». Выходило хуже всего для того, кто
    # старается: менеджер дёргал этап раз в полминуты, каждый заход занимал окно
    # заново, и ссылка не создавалась восемь минут подряд. Теперь окно занимает
    # только настоящая попытка создания - см. _recent[key] перед _create_payment.

    lead = await amo_service.get_lead_full(lead_id, with_=())
    if not lead:
        # Сделку не прочитать (amo недоступен?) — молчать нельзя: клиент ждёт
        # ссылку. Алерт на всю смену (ответственного не знаем).
        d = alerts.decide(
            "ozon_invoice_failed",
            legacy_text=(
                f"⚠️ Не удалось создать СБП-счёт: сделка {lead_id} не прочиталась из amo\n"
                f"{AMO_LEAD_URL.format(lead_id)}\n{tg_recipients.MANAGERS_ON_SHIFT}"
            ),
            chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID,
            values={
                "причина": "Не удалось создать СБП-счёт: сделка не прочиталась из amo",
                "сделка": "",
                "ссылка_на_сделку": alerts.lead_link(lead_id),
                "теги": tg_recipients.MANAGERS_ON_SHIFT,
            },
        )
        if d is not None:
            await telegram_bot.send_alert(d.text, **d.send_kwargs())
        return "failed-lead-read"

    # Сделка могла уехать с этапа, пока задача ждала в очереди — не слать.
    # Воронку берём со сделки: тех-этапы розницы и картотеки разные, но обе наши.
    stages = _stages(lead.get("pipeline_id"))
    if stages is None or int(lead.get("status_id") or 0) != stages[0]:
        logger.info(
            "Lead %s: уже не на тех-этапе «Оплата запрошена» (status=%s pipeline=%s) — скип",
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

    # Способ оплаты (577373) из стоп-списка — крипта, USDT/USTD, TRC, wallet
    # (Катя 28.09.2026). Такие оплаты идут мимо эквайринга, ссылка Ozon Pay там
    # не нужна: создать её — значит отправить клиенту счёт, который он оплатит
    # не туда. Это НЕ ошибка менеджера, поэтому без тега «ошибка счёта» и без
    # алерта в ТГ: тихо пропускаем и один раз объясняем в карточке.
    payment_method = str(amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD) or "").strip()
    blocked_token = blocked_invoice_payment_token(payment_method)
    if blocked_token:
        logger.info("Lead %s: способ оплаты «%s» (стоп-слово «%s») — ссылку не создаём",
                    lead_id, payment_method, blocked_token)
        await _note_blocked_once(lead_id, payment_method)
        return "skipped-payment-method"

    # Сделка ещё не заполнена — выставлять нечего (Катя 28.09.2026). Два случая:
    #   • способ оплаты пуст: чем платит человек, неизвестно;
    #   • пусты И бюджет, И «Другая сумма», И заказа МС нет: суммы счёта взять
    #     неоткуда. Если заказ МС есть — сумму возьмём из него, это не наш случай.
    # Раньше такие сделки падали в _fail «Нет заказа в МС»: тег, примечание, алерт.
    # А наша же запись двигала updated_at, сторож видел «новую версию» сделки и
    # пробовал снова — петля. По сделке 36389097 вышло восемь одинаковых
    # примечаний за два часа. Теперь молча пропускаем: это не сбой, а незаполненная
    # сделка, и чинить её менеджеру, а не нам.
    other_raw = str(amo_service.get_custom_field_value(lead, FIELD_INVOICE_OTHER_AMOUNT) or "").strip()
    ms_uuid_raw = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    try:
        budget = float(lead.get("price") or 0)
    except (TypeError, ValueError):
        budget = 0.0
    if not payment_method:
        logger.info("Lead %s: способ оплаты не заполнен — ссылку не создаём", lead_id)
        return "skipped-not-ready"
    if budget <= 0 and not other_raw and not looks_like_uuid(ms_uuid_raw):
        logger.info("Lead %s: бюджет 0, «Другая сумма» пуста, заказа МС нет — ссылку не создаём", lead_id)
        return "skipped-not-ready"

    # «Другая сумма» (578141): заполнено → счёт на неё, а не на сумму заказа.
    # Нечитаемое значение — честная ошибка менеджеру (не молчать и не подменять).
    other_kopecks, other_err = _parse_other_amount(
        amo_service.get_custom_field_value(lead, FIELD_INVOICE_OTHER_AMOUNT)
    )
    if other_err:
        await _fail(lead, "Поле «Другая сумма» заполнено, но не читается - счёт не создан",
                    detail=f"{other_err}. Исправьте сумму или очистите поле.")
        return "failed-other-amount"

    # Заказ МС нужен только как ИСТОЧНИК СУММЫ. Заполнена «Другая сумма» — счёт
    # выставляем и без заказа (Катя 28.09.2026): сумму менеджер назвал сам, ждать
    # заказ не за чем. Это общий путь всех воронок из _invoice_pipelines().
    # Заказ при этом всё равно читаем, если он есть: его номер идёт в начало
    # extId заказа Ozon, по нему офис матчит поступление. Не отдался МС — с
    # «Другой суммой» не падаем, просто остаёмся без номера в extId.
    ms_uuid = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    order: dict = {}
    if looks_like_uuid(ms_uuid):
        order = (await ms_client.get(f"entity/customerorder/{ms_uuid}")) or {}
        if not order and other_kopecks is None:
            await _fail(lead, "МойСклад не отдал заказ - счёт не создан",
                        detail=f"customerorder/{ms_uuid}")
            return "failed-ms-fetch"
    elif other_kopecks is None:
        await _fail(lead, "Нет заказа в МС - невозможно создать оплату",
                    detail=f"поле «ID Заказа» (576689) пусто или не UUID: {ms_uuid!r}")
        return "failed-no-ms-order"
    ms_order_name = str(order.get("name") or "").strip()

    if other_kopecks is not None:
        kopecks = other_kopecks
    else:
        kopecks = int(round(float(order.get("sum") or 0)))
    if kopecks <= 0:
        await _fail(lead, "Сумма заказа МС = 0 - счёт не создан",
                    detail=f"заказ МС {ms_order_name}. Либо укажите сумму в поле «Другая сумма».")
        return "failed-zero-sum"

    # «Оплата картой» (578145): галочка → ссылка на checkout.ozon.ru (выбор
    # способа с картой); пусто → прямой СБП.
    by_card = _is_checked(amo_service.get_custom_field_value(lead, FIELD_INVOICE_BY_CARD))

    ext_id = f"amo-{lead_id}-{int(time.time())}"
    # Вот она, «настоящая попытка»: дальше идём в Ozon за платежом. Окно дедупа
    # занимаем ЗДЕСЬ, до сетевого вызова - иначе повторный вебхук, пришедший
    # пока мы ждём ответ, создал бы второй платёж на ту же сделку.
    _recent[key] = time.monotonic()
    pay_link, payment_id, err = await _create_payment(
        ext_id, kopecks, by_card=by_card, ms_order_name=ms_order_name,
    )
    if not pay_link:
        await _fail(lead, "Ozon не создал счёт - ссылки нет", detail=err)
        return "failed-ozon"

    # Атомарно: ссылка в 577617 + перевод в «Ссылка отправлена» одним PATCH.
    # DP-боты этапа сработают на переход и прочитают сделку с уже заполненным
    # полем. Упал PATCH → сделка осталась на тех-этапе, ссылка — менеджеру.
    # Воронка — СО СДЕЛКИ (она уже провалидирована гейтом выше): в картотеке
    # сделка обязана остаться в картотеке, а не уехать в розницу.
    patched = await amo_service.patch_lead(
        lead_id,
        custom_fields={FIELD_PAYMENT_LINK: pay_link},
        status_id=stages[1],
        pipeline_id=int(lead.get("pipeline_id")),
    )
    if not patched.get("ok"):
        await _fail(lead, "Ссылка создана, но не записалась в сделку - отправьте клиенту вручную",
                    detail=f"{pay_link}\nextId {ext_id}")
        return "failed-patch"

    rub = kopecks / 100
    rub_str = f"{rub:.2f}".rstrip("0").rstrip(".")
    src = "поле «Другая сумма»" if other_kopecks is not None else f"заказ МС {ms_order_name}"
    kind = "Счёт (оплата картой, страница выбора Ozon)" if by_card else "Счёт СБП"
    # orderExtId пишем только для карты: по нему сверка ищет оплату, когда Ozon
    # не знает нашего платежа (order-флоу заводит внутри заказа свой).
    order_ext_id = build_order_ext_id(ext_id, ms_order_name) if by_card else ""
    await amo_service.add_note(
        lead_id,
        f"{kind} создан автоматически: {rub_str} ₽ ({src}), действителен "
        f"{OZON_INVOICE_TTL_S // 3600} ч.\n{pay_link}\nextId {ext_id}"
        + (f"\npaymentId {payment_id}" if payment_id else "")
        + (f"\norderExtId {order_ext_id}" if order_ext_id else ""),
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
# «extId» ловим ТОЛЬКО как отдельное слово: иначе оно же совпадёт внутри
# «orderExtId» и подменит extId платежа номером заказа.
_NOTE_EXT_RE = re.compile(r"(?<![A-Za-z])extId\s+(\S+)")
_NOTE_ORDER_EXT_RE = re.compile(r"orderExtId\s+(\S+)")

# Идемпотентность вебхука: Ozon может ретраить уведомление — повторный
# Completed по тому же extId в окне не должен дублировать перевод/примечания.
PAID_TTL_S = 3600.0
_paid_recent: dict[str, float] = {}

_notify_tasks: set = set()

# Сделки, по которым уже кричали «оплаты нет» — чтобы алерт был один, а не
# каждый цикл. Память процесса — только быстрый кэш: после пересборки контейнера
# она пуста, и раньше это давало повторный алерт по тому же счёту (инцидент
# 31.07.2026: два одинаковых сообщения с разницей в один такт цикла). Настоящая
# отметка живёт в примечании сделки — см. _NOTE_STALE_RE.
# Здесь: сделка → когда ей писали в этом процессе. Страхует те несколько минут,
# пока свежее примечание ещё не видно в выдаче /notes.
_stale_alerted: dict[int, float] = {}
# Не писать по одной сделке чаще, чем раз в этот срок, что бы ни решила лесенка.
_STALE_MIN_GAP_S = 3600.0

# Маркер отправленного напоминания в примечании сделки. Переживает рестарт,
# виден менеджеру в карточке и не требует ни тома, ни своей базы.
_STALE_NOTE_MARK = "Напоминание о неоплаченном счёте отправлено"
_NOTE_STALE_RE = re.compile(re.escape(_STALE_NOTE_MARK))

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
    в «Оплата получена» + примечание. Сделка уже дальше — только примечание.

    ⚠️ Флаг воронки здесь НЕ проверяем (flagged=False) осознанно: сюда попадают
    только по нашему же extId, а его порождает единственный путь — создание счёта,
    который под флагом. Зато при выключении флага уже выставленные счета доводятся
    до конца: деньги клиент списал, оставить сделку висеть на «ссылка отправлена»
    с одним примечанием нельзя."""
    lead_id = lead.get("id")
    cur_status = int(lead.get("status_id") or 0)
    stages = _stages(lead.get("pipeline_id"), flagged=False)
    if stages is not None and cur_status in (stages[0], stages[1]):
        patched = await amo_service.patch_lead(
            lead_id, status_id=stages[2], pipeline_id=int(lead.get("pipeline_id")),
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

def _sign_get_details(value: str) -> str:
    """Подпись getPaymentDetails: <искомый id> + accessKey + secretKey
    (ТЗ §6.1, та же формула в боевом плагине сайта — active_resolve)."""
    return hashlib.sha256(f"{value}{OZON_PAY_ACCESS_KEY}{OZON_PAY_SECRET_KEY}".encode()).hexdigest()


async def _details_request(key: str, value: str) -> tuple[dict | None, str]:
    """Один POST /v1/getPaymentDetails по ключу «id» или «extId»."""
    if _client is None:
        return None, "httpx-клиент Ozon не инициализирован"
    body = {key: value, "accessKey": OZON_PAY_ACCESS_KEY, "requestSign": _sign_get_details(value)}
    try:
        resp = await _client.post(f"{OZON_PAY_API_URL}/v1/getPaymentDetails", json=body)
    except httpx.RequestError as exc:
        return None, f"сеть/таймаут Ozon: {exc.__class__.__name__}"
    if resp.status_code >= 400:
        return None, f"Ozon HTTP {resp.status_code}: {resp.text[:200]}"
    try:
        return resp.json(), ""
    except ValueError:
        return None, "Ozon вернул невалидный JSON"


def _extract_status(data: dict) -> tuple[str, int | None]:
    """(status, amount_kopecks) из ответа. Боевая форма (лог прода 28.07.2026):
    {"items":[{"status":"PAYMENT_REJECTED","amount":{"value":"1297900"},…}]}.
    Пустой items = такого платежа Ozon не знает. Прочие формы (плоский status,
    paymentDetails) оставлены фолбэком — схема у Ozon плавает."""
    items = data.get("items")
    node = items[0] if isinstance(items, list) and items else {}
    if not node:
        details = data.get("paymentDetails") or {}
        payment = data.get("payment") or {}
        order = data.get("order") or {}
        node = details or payment or order or data

    status = node.get("status") or ""
    amount_raw = node.get("amount")
    if isinstance(amount_raw, dict):
        amount_raw = amount_raw.get("value")
    try:
        kopecks = int(round(float(amount_raw))) if amount_raw is not None else None
    except (TypeError, ValueError):
        kopecks = None
    return str(status), kopecks


def is_paid_status(status: str) -> bool:
    """Оплачен ли. Словарь статусов getPaymentDetails отличается от вебхука
    (там «Completed», здесь «PAYMENT_*»), поэтому принимаем обе формы. Всё
    незнакомое считаем НЕоплаченным и логируем — лучше не двинуть сделку, чем
    объявить оплаченной неоплаченную."""
    return status.strip().upper() in {
        "COMPLETED", "PAYMENT_COMPLETED", "PAYMENT_COMPLETE",
        "SUCCESS", "PAYMENT_SUCCESS", "PAID", "PAYMENT_PAID",
    }


async def get_payment_status(
    payment_id: str, ext_id: str = "", order_ext_id: str = "",
) -> tuple[str, int | None, str]:
    """POST /v1/getPaymentDetails → (status, amount_kopecks|None, err).

    Спрашиваем по очереди: paymentId → extId платежа → extId ЗАКАЗА. Так надо
    из-за карты: живой тест 28.07.2026 показал, что по нашему paymentId Ozon
    отвечает пустым items — «карточный» платёж он заводит внутри заказа сам, а
    наш остаётся пустышкой. Первый ответ со статусом побеждает; что именно
    сработало — видно в логе (это и есть разведка по order-флоу)."""
    tried: list[str] = []
    last_err = ""
    for key, value in (("id", payment_id), ("extId", ext_id), ("extId", order_ext_id)):
        if not value or value in tried:
            continue
        tried.append(value)
        data, err = await _details_request(key, value)
        if err:
            # Отказ по ОДНОМУ ключу не повод бросать поиск. Боевой случай
            # 29.08.2026: по paymentId Ozon отдал пустоту (карта, штатно), по
            # extId платежа — «недостаточно прав», и каскад обрывался, не дойдя
            # до extId ЗАКАЗА — единственного ключа, по которому карточный
            # платёж вообще находится. Сделка висела на «Ссылка отправлена», а
            # сверка каждые три минуты ломилась в ту же стену.
            last_err = err
            logger.info("ozon сверка: по %s=%s спросить не вышло (%s) — пробую следующий ключ",
                        key, value, err)
            continue
        status, kopecks = _extract_status(data or {})
        if status:
            logger.info("ozon сверка: статус %r по %s=%s", status, key, value)
            return status, kopecks, ""
        logger.info("ozon сверка: по %s=%s платёж не найден: %s", key, value, str(data)[:300])
    if last_err:
        return "", None, last_err
    return "", None, "платёж не найден ни по paymentId, ни по extId"


async def _payment_ref(lead_id) -> tuple[str, str, str, int | None, int | None]:
    """(payment_id, ext_id платежа, ext_id ЗАКАЗА, когда выставлен счёт, когда
    последний раз напоминали) из примечаний сделки. extId заказа нужен для карты:
    по paymentId Ozon там отвечает пустотой. У старых счетов его в примечании нет —
    восстанавливаем прежнюю форму «ord-<lead>-<ts>» из extId платежа."""
    notes = await amo_service.get_lead_notes(lead_id)
    payment_id = ext_id = order_ext_id = ""
    created_at = None
    last_alert_at = None
    for note in notes:  # свежие в конце — побеждает последнее совпадение
        text = ((note.get("params") or {}).get("text")) or ""
        if _NOTE_STALE_RE.search(text):
            last_alert_at = note.get("created_at")
            continue
        if "создан автоматически" not in text:
            continue
        match_payment = _NOTE_PAYMENT_RE.search(text)
        match_ext = _NOTE_EXT_RE.search(text)
        match_order = _NOTE_ORDER_EXT_RE.search(text)
        if match_payment:
            payment_id = match_payment.group(1)
        if match_ext:
            ext_id = match_ext.group(1)
        order_ext_id = match_order.group(1) if match_order else ""
        created_at = note.get("created_at")
    if not order_ext_id and ext_id.startswith("amo-"):
        order_ext_id = ext_id.replace("amo-", "ord-", 1)
    return payment_id, ext_id, order_ext_id, created_at, last_alert_at


def _human_age(minutes: float) -> str:
    """«9157 мин» человек не читает. Даём «6 дн 8 ч», «3 ч 20 мин», «45 мин».

    Требование Саши со встречи 30.07.2026: возраст счёта в человеческих часах.
    """
    total = int(minutes)
    if total < 60:
        return f"{total} мин"
    hours, mins = divmod(total, 60)
    if hours < 24:
        return f"{hours} ч {mins} мин" if mins else f"{hours} ч"
    days, hours = divmod(hours, 24)
    return f"{days} дн {hours} ч" if hours else f"{days} дн"


def _in_alert_window(now: datetime.datetime | None = None) -> bool:
    """Рабочее окно алертов, МСК. Без него напоминания приходили ночью
    (инцидент 31.07.2026: сообщения в 00:11 и 00:14)."""
    now = now or datetime.datetime.now(_MSK)
    return OZON_ALERT_WINDOW_START_H <= now.hour < OZON_ALERT_WINDOW_END_H


def _stale_due(age_min: float, last_alert_at: int | None,
               now: datetime.datetime | None = None) -> str:
    """Пора ли напоминать и какой ступенью. '' = не пора.

    Лесенка со встречи 30.07.2026 + правило Кати от 31.07.2026: первое
    напоминание через час, дальше НЕ ЧАЩЕ РАЗА В СУТКИ и только вечером.
    Раньше в день выставления счёта могло уйти два сообщения (первое + вечернее);
    Катя: «слишком много прилетает, не чаще одного раза в день».
    """
    now = now or datetime.datetime.now(_MSK)
    if age_min < OZON_STALE_ALERT_MIN:
        return ""
    if last_alert_at is None:
        return "first"       # первое — как только счёт перевисел порог
    last = datetime.datetime.fromtimestamp(float(last_alert_at), _MSK)
    if last.date() == now.date():
        return ""            # сегодня по этой сделке уже писали — сутки молчим
    if now.hour < OZON_STALE_EVENING_H:
        return ""            # повторы уходят вечером, днём не дёргаем
    return "evening"


async def _stale_alert(lead: dict, created_at: int | None, status: str,
                       last_alert_at: int | None = None) -> None:
    """Счёт выставлен, оплаты нет дольше порога → напоминание в ТГ ОП.

    Отметка об отправке кладётся примечанием в сделку, поэтому переживает
    пересборку контейнера. Вне рабочего окна молчим и ждём следующего прохода.
    """
    if OZON_STALE_ALERT_MIN <= 0 or not created_at:
        return
    age_min = (time.time() - float(created_at)) / 60
    lead_id = lead.get("id")
    step = _stale_due(age_min, last_alert_at)
    if not step:
        return
    if time.time() - _stale_alerted.get(lead_id, 0.0) < _STALE_MIN_GAP_S:
        return               # уже слали в этом процессе, примечание ещё не осело
    if not _in_alert_window():
        return

    rejected = str(status or "").upper() == "PAYMENT_REJECTED"
    head = ("❌ Оплата отклонена, счёт не оплачен" if rejected
            else "⏳ Счёт висит без оплаты")
    age_days = age_min / 1440
    escalate = age_days >= OZON_STALE_ESCALATE_DAYS

    mentions = tg_recipients.mentions_for(lead.get("responsible_user_id"))
    tail = "" if rejected else " (статус Ozon: {})".format(status or "неизвестен")
    title = lead.get("name") or "сделка {}".format(lead_id)
    text = (
        f"{head} {_human_age(age_min)}{tail}\n"
        f"{title}\n"
        f"{AMO_LEAD_URL.format(lead_id)}\n{mentions}"
    )
    d = alerts.decide(
        "ozon_invoice_rejected" if rejected else "ozon_invoice_stale",
        legacy_text=text,
        chat_id=tg_recipients.NOTIFY_CHAT_ID, thread_id=tg_recipients.NOTIFY_THREAD_ID, lead=lead,
        responsible_id=lead.get("responsible_user_id"),
        values={
            "сколько_ждали": _human_age(age_min),
            "статус_оплаты": status or "неизвестен",
            "сделка": lead.get("name") or "",
            "ссылка_на_сделку": alerts.lead_link(lead_id),
            "теги": mentions,
        },
    )
    if d is not None:
        await telegram_bot.send_alert(d.text, **d.send_kwargs())
    else:
        logger.info("ozon: напоминание по сделке %s выключено в панели", lead_id)
    _stale_alerted[lead_id] = time.time()
    try:
        await amo_service.add_note(
            lead_id, f"{_STALE_NOTE_MARK} ({_human_age(age_min)} без оплаты)")
    except Exception:
        logger.exception("ozon: не удалось записать отметку о напоминании (lead %s)", lead_id)

    # Трое суток без оплаты → отдельно руководителю (решение встречи 30.07.2026).
    # Пока чат Саши не заведён, OZON_STALE_ESCALATE_CHAT_ID пуст и эскалация молчит:
    # тегать его в общем чате нельзя, это прямо оговорено.
    if escalate:
        d = alerts.decide(
            "ozon_invoice_escalation",
            legacy_text=(
                f"🚨 Счёт без оплаты {_human_age(age_min)}\n"
                f"{title}\n{AMO_LEAD_URL.format(lead_id)}"
            ),
            chat_id=OZON_STALE_ESCALATE_CHAT_ID or None, lead=lead,
            responsible_id=lead.get("responsible_user_id"),
            values={
                "сколько_ждали": _human_age(age_min),
                "сделка": lead.get("name") or "",
                "ссылка_на_сделку": alerts.lead_link(lead_id),
            },
        )
        # Как и раньше: без своего чата эскалация молчит, а не падает в технический.
        if d is None or (d.source == "legacy" and not OZON_STALE_ESCALATE_CHAT_ID):
            logger.info("ozon: сделка %s висит %s — эскалация не настроена или выключена",
                        lead_id, _human_age(age_min))
        else:
            await telegram_bot.send_alert(d.text, **d.send_kwargs())


# Сделки, по которым уже звали человека из-за отсутствующей ссылки: второй раз
# в тот же день не зовём. Живёт в процессе - после пересборки контейнера
# напоминание может повториться, и это дешевле, чем тащить ради него примечание.
_no_link_alerted: dict[int, float] = {}
_NO_LINK_ALERT_GAP_S = 6 * 3600
# На каком updated_at сделки мы уже пробовали создать счёт. Пока сделка не
# изменилась, повторять бессмысленно: входные данные те же, ответ будет тот же.
# Без этого сторож ходил в amo и МойСклад по одним и тем же сделкам каждые
# три минуты (Катя 23.09.2026: «это плохо для сервера»).
_no_link_tried_at: dict[int, int] = {}


async def _retry_missing_link(lead: dict) -> int:
    """Сделка стоит на «Оплата запрошена», ссылки нет — пробуем создать заново.

    Зачем это вообще нужно (23.09.2026, разбор сделки 36553383). Счёт создаётся
    ТОЛЬКО по вебхуку об изменении сделки. Значит достаточно один раз промахнуться
    - потерялся вебхук, попытка попала в дедуп-окно, менеджер очистил поле и
    больше сделку не трогал, - и сделка стоит без ссылки, пока кто-нибудь её не
    тронет руками. Клиент в этот момент ждёт оплату и ничего не получает.

    Ждём OZON_NO_LINK_RETRY_MIN минут покоя, чтобы не наступать на пятки живой
    работе менеджера: сделку могли только что перевести, и вебхук ещё в очереди.
    Отсчёт - от updated_at: пока сделку крутят, счётчик сбрасывается сам.

    Возвращает 1, если ссылка создалась, иначе 0.
    """
    if OZON_NO_LINK_RETRY_MIN <= 0:
        return 0
    # Академия под сторож не идёт (решение Кати 23.09.2026): счёт там выставляют
    # иначе, заказа МС у таких сделок нет и не будет, и сделка месяцами стоит на
    # «Оплата запрошена», пока клиент решается. Повтор заведомо холостой, а тег
    # «ошибка счёта» и алерт менеджеру - ложные. Сверку оплат по Академии это не
    # трогает: она ходит по выставленным счетам и работает как раньше.
    if int(lead.get("pipeline_id") or 0) == PIPELINE_ACADEMY:
        return 0
    lead_id = lead.get("id")
    updated_at = int(lead.get("updated_at") or 0)
    quiet_min = (time.time() - float(updated_at)) / 60
    if quiet_min < OZON_NO_LINK_RETRY_MIN:
        return 0
    # Слишком давно не трогали — это не застрявший счёт, а живая работа: клиент
    # думает, менеджер переписывается, сделка стоит на этапе оплаты неделями.
    if 0 < OZON_NO_LINK_MAX_QUIET_MIN < quiet_min:
        return 0
    # Уже пробовали на этой же версии сделки — входные данные не изменились,
    # ответ будет тот же. Ждём, пока сделку тронут: тогда updated_at сдвинется.
    if _no_link_tried_at.get(lead_id) == updated_at:
        return 0
    _no_link_tried_at[lead_id] = updated_at

    outcome = await process_invoice_lead(lead_id, source="reconcile")
    if outcome == "created":
        logger.info("Ozon сверка: сделка %s висела без ссылки %.0f мин — счёт создан заново",
                    lead_id, quiet_min)
        _no_link_alerted.pop(lead_id, None)
        _no_link_tried_at.pop(lead_id, None)
        return 1

    logger.info("Ozon сверка: сделка %s без ссылки %.0f мин, повтор дал «%s»",
                lead_id, quiet_min, outcome)

    # Ссылки нет ПО ЗАМЫСЛУ, а не из-за сбоя: способ оплаты из стоп-списка
    # (крипта и прочее), сделка не заполнена, уехала с этапа, попытка была только
    # что. Любой наш «скип» — не повод звать человека: та же логика, что с
    # Академией выше. Именно на этом месте раньше начиналась петля примечаний.
    if outcome.startswith("skipped-"):
        return 0

    # Повтор не помог и сделка висит давно — зовём человека. Порог отдельный от
    # OZON_STALE_ALERT_MIN: там «клиент не платит по выставленному счёту», а
    # здесь счёта нет вовсе, и это чинить нам, а не клиенту.
    if OZON_NO_LINK_ALERT_MIN <= 0 or quiet_min < OZON_NO_LINK_ALERT_MIN:
        return 0
    if time.time() - _no_link_alerted.get(lead_id, 0.0) < _NO_LINK_ALERT_GAP_S:
        return 0
    if not _in_alert_window():
        return 0
    _no_link_alerted[lead_id] = time.time()
    await _fail(
        lead,
        f"Сделка {int(quiet_min)} мин на «Оплата запрошена», а ссылки нет - выставьте счёт вручную",
        detail=f"автоповтор вернул «{outcome}»",
    )
    return 0


async def _reconcile_once() -> str:
    """Один проход: сделки с выставленным счётом на этапах оплаты → спросить
    Ozon → Completed двигаем, зависшие подсвечиваем алертом."""
    checked = moved = retried = 0
    # Пары «воронка + этап»: сверка ходит только по включённым воронкам, иначе
    # выключенный флаг всё равно стоил бы двух лишних запросов на проход.
    for pipeline_id in _invoice_pipelines():
        stages = OZON_PAYMENT_STAGES.get(pipeline_id)
        if not stages:
            continue
        for status_id in (stages[1], stages[0]):
            for lead in await amo_service.get_leads_by_status(status_id, with_=()):
                # Перестраховка: get_leads_by_status резолвит воронку сама по кэшу
                # этапов, но если константа этапа разъедется с amo, отрезолвит
                # чужую — и мы двинем чужую сделку.
                if int(lead.get("pipeline_id") or 0) != pipeline_id:
                    continue
                link = str(amo_service.get_custom_field_value(lead, FIELD_PAYMENT_LINK) or "").strip()
                if not link:
                    # Ссылки нет. На «Ссылка отправлена» это и правда нечего
                    # сверять, а вот на тех-этапе «Оплата запрошена» — застрявшая
                    # сделка: счёт не создался, и сам он уже не создастся, потому
                    # что повторных попыток по расписанию у модуля не было.
                    if status_id == stages[0]:
                        retried += await _retry_missing_link(lead)
                    continue

                lead_id = lead.get("id")
                payment_id, ext_id, order_ext_id, created_at, last_alert_at = await _payment_ref(lead_id)
                if not payment_id:
                    # Счёт выставлен руками/старым виджетом — paymentId неизвестен.
                    continue
                if (ext_id or payment_id) in _paid_recent:
                    continue

                checked += 1
                status, kopecks, err = await get_payment_status(payment_id, ext_id, order_ext_id)
                if err:
                    logger.warning("ozon сверка: lead %s paymentId %s — %s", lead_id, payment_id, err)
                    continue

                if not is_paid_status(status):
                    await _stale_alert(lead, created_at, status, last_alert_at)
                    continue

                _seen_paid(ext_id or payment_id)
                rub = (kopecks or 0) / 100
                rub_str = f"{rub:.2f}".rstrip("0").rstrip(".") if kopecks else "?"
                marker = f"extId {ext_id}" if ext_id else f"paymentId {payment_id}"
                if await _mark_paid(lead, rub_str, marker, "сверка") == "moved":
                    moved += 1
                    _stale_alerted.pop(lead_id, None)

    logger.info("Ozon сверка: проверено счетов %s, переведено сделок %s, пересозданных ссылок %s",
                checked, moved, retried)
    return f"checked={checked} moved={moved} retried={retried}"


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
