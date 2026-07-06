import datetime
import os
import re

from dotenv import load_dotenv

load_dotenv()

# amoCRM статусы (этапы воронки)
STATUS_CREATE_WAYBILL = 75426822
STATUS_WAYBILL_READY = 75426874

# amoCRM custom field IDs (сделка)
FIELD_CDEK_ORDER_NUMBER = 571657
FIELD_PVZ_CODE = 576719
FIELD_PVZ_CODE_FALLBACK = 572209
FIELD_DELIVERY_ADDRESS = 577311
FIELD_PAYMENT_METHOD = 577373
FIELD_SENDER_COMPANY = 577551
FIELD_PACKAGE_NUMBER = 577415
# То же поле 577415 — по факту это «Номер заказа на сайте» (= WC order id).
# Имя FIELD_PACKAGE_NUMBER историческое; для woo-синка используем понятный алиас.
FIELD_SITE_ORDER_NUMBER = 577415
FIELD_ORDER_TOTAL = 576703
FIELD_COMPOSITION = 577313
FIELD_URGENCY = 578127          # поле «Срочность» (select)

# amoCRM custom field IDs (контакт)
FIELD_PHONE = 413385
FIELD_EMAIL = 413387

# Теги
TAG_ERROR = "ошибка накладной"
TAG_PACKED = "посылка упакована"

# Автотег «Срочно»: когда менеджер ставит Срочность = «Срочно» → вешаем тег «Срочно».
URGENCY_SROCHNO_VALUE = "Срочно"   # enum-метка «Срочно» поля 578127 (enum id 1041803)
TAG_SROCHNO_ID = 504609           # существующий тег «Срочно»
TAG_SROCHNO_NAME = "Срочно"

# Автотег «Запись в шоурум»: когда тип доставки (577315) = самовывоз из офиса Sunscrypt.
# Матч по подстроке «самовывоз из офиса» — дискриминатор vs «CDEK: Самовывоз» (пункт СДЭК).
DELIVERY_SHOWROOM_MARKER = "самовывоз из офиса"
TAG_SHOWROOM_ID = 533267           # существующий тег «Запись в шоурум»
TAG_SHOWROOM_NAME = "Запись в шоурум"

# ---------------------------------------------------------------------------
# Синхронизация статусов СДЭК → этапы воронки «офис».
# id этапов резолвятся по названиям при старте (cdek_status_sync.init).
# ---------------------------------------------------------------------------

STAGE_WAYBILL_READY = "Готова накладная"
STAGE_SHIPPED = "Посылка отгружена"
STAGE_IN_TRANSIT = "В пути"
STAGE_AT_PVZ = "Ожидает в ПВЗ"
STAGE_DELIVERED = "Успешно реализовано"
STAGE_NOT_DELIVERED = "Закрыто и не реализовано"

# Этапы, в которых сделки опрашиваются фоновой страховкой
SYNC_POLL_STAGES = (STAGE_WAYBILL_READY, STAGE_SHIPPED, STAGE_IN_TRANSIT, STAGE_AT_PVZ)

# Код статуса СДЭК → название этапа воронки «офис».
# Возвратные статусы (RETURNED_*, POSTOMAT_SEIZED, SENT_TO_SENDER_CITY,
# ACCEPTED_IN_SENDER_CITY) намеренно отсутствуют: по ним сделку не двигаем,
# ждём финальный NOT_DELIVERED.
CDEK_STATUS_TO_STAGE = {
    "ACCEPTED": STAGE_WAYBILL_READY,
    "CREATED": STAGE_WAYBILL_READY,
    "RECEIVED_AT_SHIPMENT_WAREHOUSE": STAGE_SHIPPED,
    "READY_FOR_SHIPMENT_IN_SENDER_CITY": STAGE_SHIPPED,
    "READY_TO_SHIP_AT_SENDING_OFFICE": STAGE_SHIPPED,
    "TAKEN_BY_TRANSPORTER_FROM_SENDER_CITY": STAGE_IN_TRANSIT,
    "SENT_TO_TRANSIT_CITY": STAGE_IN_TRANSIT,
    "ACCEPTED_IN_TRANSIT_CITY": STAGE_IN_TRANSIT,
    "ACCEPTED_AT_TRANSIT_WAREHOUSE": STAGE_IN_TRANSIT,
    "READY_TO_SHIP_IN_TRANSIT_OFFICE": STAGE_IN_TRANSIT,
    "READY_FOR_SHIPMENT_IN_TRANSIT_CITY": STAGE_IN_TRANSIT,
    "TAKEN_BY_TRANSPORTER_FROM_TRANSIT_CITY": STAGE_IN_TRANSIT,
    "SENT_TO_RECIPIENT_CITY": STAGE_IN_TRANSIT,
    "ACCEPTED_IN_RECIPIENT_CITY": STAGE_IN_TRANSIT,
    "ACCEPTED_AT_RECIPIENT_CITY_WAREHOUSE": STAGE_IN_TRANSIT,
    "TAKEN_BY_COURIER": STAGE_IN_TRANSIT,
    "IN_CUSTOMS_INTERNATIONAL": STAGE_IN_TRANSIT,
    "SHIPPED_TO_DESTINATION": STAGE_IN_TRANSIT,
    "PASSED_TO_TRANSIT_CARRIER": STAGE_IN_TRANSIT,
    "IN_CUSTOMS_LOCAL": STAGE_IN_TRANSIT,
    "CUSTOMS_COMPLETE": STAGE_IN_TRANSIT,
    "ACCEPTED_AT_PICK_UP_POINT": STAGE_AT_PVZ,
    "POSTOMAT_POSTED": STAGE_AT_PVZ,
    "DELIVERED": STAGE_DELIVERED,
    "POSTOMAT_RECEIVED": STAGE_DELIVERED,
    "NOT_DELIVERED": STAGE_NOT_DELIVERED,
    "INVALID": STAGE_NOT_DELIVERED,
}

# Тарифы СДЭК — определяются по подстроке в FIELD_ORDER_TOTAL
TARIFF_MAP = {
    "CDEK: Самовывоз": 136,
    "CDEK: Посылка склад-постамат": 368,
    "Посылка склад-дверь": 137,
}
TARIFFS_PVZ = (136, 368)
TARIFF_DOOR = 137

# Статичные данные отправителя
SENDER = {
    "company": "ИП Перфилов",
    "name": "Перфилов Андрей Владимирович",
    "phones": [{"number": "+79322575768"}],
    "address": "Москва, улица Бутлерова, дом 17, офис 5126",
    "city": "Москва",
    "country_code": "RU",
}

# СДЭК
CDEK_API_URL = os.getenv("CDEK_API_URL", "https://api.cdek.ru/v2").rstrip("/")
CDEK_CLIENT_ID = os.getenv("CDEK_CLIENT_ID", "")
CDEK_CLIENT_SECRET = os.getenv("CDEK_CLIENT_SECRET", "")
# Публичный HTTPS-адрес сервиса — для ссылок в примечаниях и подписки на вебхуки.
PUBLIC_BASE_URL = os.getenv(
    "PUBLIC_BASE_URL", "https://koshkaterina-amo-fix-fields-a7a1.twc1.net"
).rstrip("/")
# HTTPS-URL эндпоинта /cdek_status — для подписки на вебхуки СДЭК.
# Пусто → подписка не оформляется, работает только фоновый опрос.
CDEK_WEBHOOK_URL = os.getenv("CDEK_WEBHOOK_URL", f"{PUBLIC_BASE_URL}/cdek_status").strip()
# Интервал фонового опроса-страховки, сек (0 → опрос выключен)
CDEK_SYNC_POLL_INTERVAL_S = int(os.getenv("CDEK_SYNC_POLL_INTERVAL_S", "3600"))

# Заглушка объявленной стоимости для СДЭК, когда сумма ПРЕДОПЛАЧЕННОГО заказа
# распарсилась в 0 (замена/гарантия или в поле «Заказ» нет строки «Итого»). СДЭК
# требует положительный cost; товар уже оплачен, поэтому ставим минимальную
# ценность вместо ручной правки менеджером (раньше он ставил 1). Настраивается env.
WAYBILL_ZERO_COST_PLACEHOLDER = int(os.getenv("WAYBILL_ZERO_COST_PLACEHOLDER", "1"))

# ---------------------------------------------------------------------------
# Яндекс.Метрика CDP — сквозная аналитика (amoCRM → Метрика)
# ---------------------------------------------------------------------------
METRIKA_API_URL = os.getenv("METRIKA_API_URL", "https://api-metrika.yandex.net").rstrip("/")
METRIKA_TOKEN = os.getenv("METRIKA_TOKEN", "").strip()
# Номер счётчика. Пусто → если в аккаунте один счётчик, подхватим по токену.
_raw_counter = os.getenv("METRIKA_COUNTER_ID", "").strip()
METRIKA_COUNTER_ID: int | None = int(_raw_counter) if _raw_counter.isdigit() else None

# Гард по дате старта интеграции: заказы, созданные раньше METRIKA_SINCE, не
# синкаем (отсекает исторические сделки и массовые правки старья). Формат
# YYYY-MM-DD по МСК. Пусто → гард выключен.
_raw_since = os.getenv("METRIKA_SINCE", "").strip()


def _parse_since_ts(s: str) -> int | None:
    if not s:
        return None
    try:
        d = datetime.datetime.strptime(s, "%Y-%m-%d").replace(
            tzinfo=datetime.timezone(datetime.timedelta(hours=3))
        )
        return int(d.timestamp())
    except ValueError:
        return None


METRIKA_SINCE_TS: int | None = _parse_since_ts(_raw_since)

# Воронки amoCRM
PIPELINE_CLEVER = 10593102       # [CLEVER] Основная — отдел продаж, ОРИГИНАЛЫ сделок
PIPELINE_OFFICE = 9421022        # Офис
PIPELINE_FULFILLMENT = 10997702  # Фулфилмент

# Целевые статусы. 142/143 — системные, общие для всех воронок.
STATUS_SUCCESS = 142             # Успешно реализовано
STATUS_CLOSED_LOST = 143         # Закрыто и не реализовано
FULFILLMENT_DELIVERED = 86476486          # Фулфилмент «09. Доставлено»
FULFILLMENT_PAYMENT_FORWARDED = 86451330  # Фулфилмент «09.2 Платёж отправлен владельцу»

# Поля сделки для Метрики
FIELD_YM_CLIENT_ID = 578015          # «id (для метрики)» — ClientID Яндекс.Метрики (_ym_uid)
FIELD_MOYSKLAD_ORDER_UUID = 576689   # «ID Заказа» (UUID МойСклад) — ключ связки дубликат→оригинал
# FIELD_PAYMENT_METHOD = 577373 (способ оплаты) уже определён выше
# FIELD_PHONE = 413385, FIELD_EMAIL = 413387 (контакт) уже определены выше

# Наложка (оплата по факту получения) определяется по полю «Способ оплаты».
def is_cod_payment(payment_method) -> bool:
    s = str(payment_method or "").lower()
    # «при получении» / эвотор / наложенный — однозначно наложка
    if "при получении" in s or "эвотор" in s or "наложен" in s:
        return True
    # наличные — наложка, но НЕ путать с «безналичный» (это предоплата)
    if "налич" in s and "безнал" not in s:
        return True
    return False


# Явно распознанная ПРЕДОПЛАТА (онлайн/картой/перевод/крипта/безнал). Крипта —
# предоплата, как и в kontrol_gate.categorize_payment. Пустой/непонятный способ
# оплаты сюда НЕ попадает (вернёт False) — это нужно, чтобы при нулевой сумме не
# считать заказ предоплаченным по умолчанию.
_PREPAID_TOKENS = (
    "онлайн", "картой", "на карт", "перевод", "банк", "безнал",
    "крипт", "crypto", "usdt", "usdc", "tether", "wallet",
)


def is_prepaid_payment(payment_method) -> bool:
    if is_cod_payment(payment_method):
        return False
    s = str(payment_method or "").lower()
    return any(t in s for t in _PREPAID_TOKENS)

# ---------------------------------------------------------------------------
# WooCommerce — простановка статуса заказа 'completed' для рефералки (amo → WC).
# Передаём ТОЛЬКО статус и ТОЛЬКО когда заказ оплачен (PAID по логике Метрики,
# metrika_sync._classify). Сумму/товары/промежуточные статусы не трогаем.
# Ключ связки — поле сделки 577415 «Номер заказа на сайте» = WC order id
# (FIELD_SITE_ORDER_NUMBER). МойСклад не задействован. Идёт ВМЕСТЕ с metrika_sync:
# тем же элементом очереди (queue_manager) и тем же ночным проходом сверки.
# ---------------------------------------------------------------------------
WC_URL = os.getenv("WC_URL", "").rstrip("/")
WC_CONSUMER_KEY = os.getenv("WC_CONSUMER_KEY", "").strip()
WC_CONSUMER_SECRET = os.getenv("WC_CONSUMER_SECRET", "").strip()
# Слаг «выполнен» в WooCommerce (к нему привязана комиссия рефералки).
WOO_COMPLETED_STATUS = os.getenv("WOO_COMPLETED_STATUS", "completed").strip()
# Боевой флаг записи в WC. Пусто/false → синк ВЫКЛЮЧЕН даже при заданных WC_*
# (для dry-run и безопасного выката). Включить: WOO_STATUS_SYNC_ENABLED=true.
WOO_STATUS_SYNC_ENABLED = os.getenv("WOO_STATUS_SYNC_ENABLED", "").strip().lower() in (
    "1", "true", "yes", "on",
)
# Гард по дате СОЗДАНИЯ заказа (как METRIKA_SINCE): заказы старше не трогаем.
# Формат YYYY-MM-DD по МСК. Пусто → гард выключен.
WOO_STATUS_SINCE_TS: int | None = _parse_since_ts(
    os.getenv("WOO_STATUS_SINCE", "2026-03-30").strip()
)

# ---------------------------------------------------------------------------
# МойСклад API — для ms_status_sync (ведём ФФ-копию по статусу заказа склада).
# Только чтение. MS_TOKEN — Bearer-токен главного админа МС (тот же, что в
# проекте woocommerce-sklad). Пусто → ms_status_sync ВЫКЛЮЧЕН (сервис работает).
# ---------------------------------------------------------------------------
MS_API_URL = os.getenv("MS_API_URL", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
MS_TOKEN = os.getenv("MS_TOKEN", "").strip()
MS_SYNC_POLL_INTERVAL_S = int(os.getenv("MS_SYNC_POLL_INTERVAL_S", "30"))
MS_SYNC_LOOKBACK_MIN = int(os.getenv("MS_SYNC_LOOKBACK_MIN", "120"))
# Час ночной ПОЛНОЙ сверки ФФ (amo-driven страховка от промахов узкого окна
# живого опроса: рестарт/деплой/подвисание сервиса дольше lookback теряет
# изменение статуса МС навсегда). ≠1 (Метрика в 01:00), 0..23 МСК.
MS_RECONCILE_HOUR_MSK = int(os.getenv("MS_RECONCILE_HOUR_MSK", "2"))

# Трек-номер: атрибут заказа МойСклад → поле сделки amoCRM. Цель — поле 571657
# «Трек-номер» (то же, что FIELD_CDEK_ORDER_NUMBER; у ФФ-копий оно пустое,
# конфликта с CDEK-синком нет — тот пишет в офисные сделки).
MS_ATTR_TREK = "e25b4e11-2aa4-11f1-0a80-0704003169db"
FIELD_FF_TREK = 571657

# Telegram
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
_raw_chat_id = os.getenv("TG_ALLOWED_CHAT_ID", "")
TG_ALLOWED_CHAT_ID: int | None = int(_raw_chat_id) if _raw_chat_id else None
# Прокси для Telegram API (api.telegram.org заблокирован в РФ).
# Поддерживается HTTP/HTTPS из коробки: http://user:pass@host:port
# Для SOCKS5 нужен пакет aiohttp_socks + код в telegram_bot.py не активирует
# его автоматически (см. README).
TG_PROXY_URL = os.getenv("TG_PROXY_URL", "").strip()


_TOTAL_RE = re.compile(
    r"Итого:\s*([\d\s]+[\d])[.,]\d+\s*(?:руб(?:ль|ля|лей|\.?)|₽)",
    re.IGNORECASE,
)


def parse_total(order_text: str | None) -> int:
    if not order_text:
        return 0
    text = order_text.replace(" ", " ")
    m = _TOTAL_RE.search(text)
    if not m:
        return 0
    return int(m.group(1).replace(" ", ""))


def parse_tariff(order_text: str | None) -> int | None:
    if not order_text:
        return None
    for pattern, tariff in TARIFF_MAP.items():
        if pattern in order_text:
            return tariff
    return None


_PVZ_RE = re.compile(r"[A-Z]{2,}\d+")


def extract_pvz_code(raw: str | None) -> str | None:
    if not raw:
        return None
    m = _PVZ_RE.search(raw)
    return m.group(0) if m else None


_UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def looks_like_uuid(value: str | None) -> bool:
    if not value:
        return False
    return bool(_UUID_RE.match(value.strip()))
