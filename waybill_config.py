import datetime
import os
import re

from dotenv import load_dotenv

load_dotenv()

# amoCRM статусы (этапы воронки)
STATUS_CREATE_WAYBILL = 75426822
STATUS_WAYBILL_READY = 75426874

# Фулфилмент: гейт «КОНТРОЛЬ» → «00. Обрабатывается» (автоматическая проверка заказа).

# amoCRM custom field IDs (сделка)
FIELD_CDEK_ORDER_NUMBER = 571657
FIELD_PVZ_CODE = 576719
FIELD_PVZ_CODE_FALLBACK = 572209
# 576719 «Адрес получателя» — единое поле, которое заполняет сайт/МС: для курьера
# (тариф «дверь») здесь адрес, для ПВЗ/постамата — код пункта. То же поле, что
# FIELD_PVZ_CODE; семантика выбирается по тарифу. Старое 577311 «Адрес получателя
# (арх)» больше не заполняется — накладную из него читать нельзя.
FIELD_DELIVERY_ADDRESS = 576719
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
# Гейт КОНТРОЛЬ: заказ не прошёл автопроверку → остаётся в КОНТРОЛЕ с этим тегом
# (причина — примечанием в сделке).
TAG_KONTROL_ERROR = "ошибка передачи"

# Автотег «Срочно»: когда менеджер ставит Срочность = «Срочно» → вешаем тег «Срочно».
URGENCY_SROCHNO_VALUE = "Срочно"   # enum-метка «Срочно» поля 578127 (enum id 1041803)
TAG_SROCHNO_ID = 504609           # существующий тег «Срочно»
TAG_SROCHNO_NAME = "Срочно"

# Автотег «Запись в шоурум»: когда тип доставки (577315) = самовывоз из офиса Sunscrypt.
# Матч по подстроке «самовывоз из офиса» — дискриминатор vs «CDEK: Самовывоз» (пункт СДЭК).
DELIVERY_SHOWROOM_MARKER = "самовывоз из офиса"

# Авто-перенос в ЗИН по «мусорной» причине отказа: менеджер ставит «Причина отказа»
# (577623) в одно из мусорных значений → сделка автоматически уходит в «Закрыто и не
# реализовано» (143) в своей воронке. Работает во всех воронках.
# Включено всегда (хардкод, без env — по решению Кати 14.07).
DUP_AUTOCLOSE_ENABLED = True
DUP_REASON_FIELD_ID = 577623      # поле «Причина отказа» (select) — ⚠️ имя устарело,
                                   # живое название поля в amoCRM «Причина ЗИН» (сверено
                                   # live 30.07.2026); тот же field_id переиспользует
                                   # office_transfer.py, см. REASON_WAITLIST/REASON_ACADEMY ниже
DUP_REASON_ENUM_IDS = {           # значения-триггеры (enum id → смысл):
    1041141,                      #   Дубль сделки
    1041163,                      #   Тест
    1041691,                      #   Обменник
    1041159,                      #   Тех поддержка
    1041161,                      #   Не ЦА
}
DUP_CLOSE_STATUS_ID = 143         # «Закрыто и не реализовано» (ЗИН), есть во всех воронках
TAG_SHOWROOM_ID = 533267           # существующий тег «Запись в шоурум»
TAG_SHOWROOM_NAME = "Запись в шоурум"

# Автоснятие «пропущенный» при дозвоне: UIS вешает «пропущенный» на потерянный
# входящий и «Успешный звонок» на успешный (вх./исх.). Когда до клиента ДОЗВОНИЛИСЬ
# (появился «Успешный звонок»), снимаем «пропущенный» со сделки и её контактов.
TAG_MISSED_NAME = "пропущенный"            # тег UIS: leads 531917 / contacts 513357
TAG_SUCCESS_CALL_NAME = "Успешный звонок"  # тег UIS при успешном звонке — триггер снятия

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
    "Самовывоз СДЭК": 136,  # оптовый/B2B формат строки доставки — тот же ПВЗ-самовывоз (136)
    "CDEK: Посылка склад-постамат": 368,
    "Посылка склад-дверь": 137,
}
TARIFFS_PVZ = (136, 368)
TARIFF_DOOR = 137

# Страна получателя по коду телефона (E.164). Раньше to_location для тарифа
# «дверь» ВСЕГДА уходил в СДЭК с country_code="RU", даже когда получатель
# реально за границей — СДЭК искал город получателя ТОЛЬКО в России и мог
# перепутать зарубежный город с российским тёзкой (разбор 26.08.2026, сделка
# 36519063: «Минск» есть и в Беларуси (BY, код 9220), и как село в
# Красноярском крае (RU, код 1912192) — с захардкоженным RU СДЭК пытался
# найти ул. Иосифа Жиновича в сибирском селе и отклонял заказ «Recipient
# location is not recognized»). +7 НАМЕРЕННО не сюда: Казахстан тоже +7,
# однозначно отличить от РФ по префиксу нельзя без справочника кодов городов
# — ложный BY/KZ хуже сегодняшнего поведения, поэтому +7 остаётся RU, как и
# было. Остальные — однозначные международные коды стран СНГ/ближнего
# зарубежья, куда возит СДЭК.
PHONE_COUNTRY_PREFIXES = {
    "+375": "BY",  # Беларусь
    "+380": "UA",  # Украина
    "+374": "AM",  # Армения
    "+994": "AZ",  # Азербайджан
    "+995": "GE",  # Грузия
    "+996": "KG",  # Киргизия
    "+992": "TJ",  # Таджикистан
    "+993": "TM",  # Туркменистан
    "+998": "UZ",  # Узбекистан
}


def country_code_from_phone(phone) -> str:
    """Код страны получателя по номеру телефона контакта (E.164). Без явного
    совпадения по префиксу — "RU" (прежнее поведение, домашние заказы не трогаем)."""
    s = str(phone or "").strip()
    if s and not s.startswith("+"):
        s = "+" + s.lstrip("0")
    for prefix, cc in PHONE_COUNTRY_PREFIXES.items():
        if s.startswith(prefix):
            return cc
    return "RU"


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
PIPELINE_TANGEMSHOP = 9822330    # TangemShop

# Целевые статусы. 142/143 — системные, общие для всех воронок.
STATUS_SUCCESS = 142             # Успешно реализовано
STATUS_CLOSED_LOST = 143         # Закрыто и не реализовано

# Поля сделки для Метрики
FIELD_YM_CLIENT_ID = 578015          # «id (для метрики)» — ClientID Яндекс.Метрики (_ym_uid)
FIELD_MOYSKLAD_ORDER_UUID = 576689   # «ID Заказа» (UUID МойСклад) — ключ связки дубликат→оригинал
# FIELD_PAYMENT_METHOD = 577373 (способ оплаты) уже определён выше
# FIELD_PHONE = 413385, FIELD_EMAIL = 413387 (контакт) уже определены выше

# Резерв товара в МойСклад (перенос с amGroup, 04.08.2026) — см. reserve_service.py.
# Работаем только со сделками, где заполнено FIELD_MOYSKLAD_ORDER_UUID выше
# (заказ МС уже создан amGroup/виджетом сайта — свой заказ мы не создаём).
# Триггер пересмотра резерва при изменении корзины — поле 576703 «Состав
# заказа», уже читается в webhooks.py рядом с parse_the_cart_field.

# [CLEVER] Основная — статусы, где резерв ставим/держим (кроме уже определённых
# выше STATUS_PAYMENT_REQUESTED/STATUS_LINK_SENT/STATUS_PAYMENT_RECEIVED и
# системных STATUS_SUCCESS/STATUS_CLOSED_LOST — см. ниже).
STATUS_CLEVER_NEW_LEAD = 83537714        # «Новый лид»
STATUS_CLEVER_IN_PROGRESS = 83537718     # «Взят в работу»
STATUS_CLEVER_OFFICE_RECORD = 86706902   # «Запись в офис»
STATUS_CLEVER_QUALIFIED = 83537722       # «Квалификация проведена»
STATUS_CLEVER_WALLET_PICKED = 83537858   # «Кошелек подобран»
STATUS_CLEVER_UPSELL_DONE = 83893786     # «Допродажа сделана»
STATUS_CLEVER_TERMS_AGREED = 83537862    # «Условия согласованы»
STATUS_CLEVER_PRECLOSED = 83660350       # «Предварительно закрыт» — резерв СНИМАЕМ

# TangemShop
STATUS_TANGEM_NEW_ORDER = 78157066                    # «Новый заказ»
STATUS_TANGEM_IN_PROGRESS = 78157070                   # «взят в работу»
STATUS_TANGEM_UPSELL_DONE = 78157074                   # «апсейл / допродажа сделаны»
STATUS_TANGEM_ADDITIONAL_PAYMENT_RECEIVED = 86477050   # «доплата получена»

# Офис — отгрузка реально расходует резерв (STATUS_WAYBILL_READY уже определена
# выше = «Готова накладная»; STATUS_SUCCESS здесь = «Успешно реализовано» в
# Офисе, где сделка реально закрыта — в отличие от УР в Основной/TangemShop,
# где резерв держим, т.к. сделка едет в Офис дальше).
STATUS_OFFICE_COURIER_MSK = 75426866      # «Достависта МСК»
STATUS_OFFICE_COURIER_OWN = 75426870      # «Доставка наш курьер»
STATUS_OFFICE_SHIPPED = 75426878          # «Посылка отгружена»
STATUS_OFFICE_IN_TRANSIT = 75426882       # «В пути»
STATUS_OFFICE_AWAITING_PICKUP = 75426886  # «Ожидает в ПВЗ»

# Офис — этапы, где резерв держим БЕССРОЧНО (решение встречи 04.08.2026): товар
# отложен под конкретного клиента осознанно, тайм-аут трёх дней тут не применяем.
STATUS_OFFICE_PREORDER_PAID = 83953914     # «Предзаказ оплачен»
STATUS_OFFICE_DEFERRED_RESERVE = 75426858  # «Отложенный/резерв товар»

# Мастер-флаг сервиса резерва. ПО УМОЛЧАНИЮ ВЫКЛЮЧЕН — как у ozon_invoice и
# office_transfer: код едет на прод мёртвым грузом, а включается отдельным
# движением. Так выключение резерва в виджете amGroup и включение нашего
# сервиса делаются встык, без окна, где резерв ставят оба или ни один.
# Выключенный сервис не пишет в МойСклад и не гоняет фоновый цикл тайм-аута.
RESERVE_SERVICE_ENABLED = os.getenv("RESERVE_SERVICE_ENABLED", "").strip() == "1"

# Тайм-аут автосброса резерва (то, чего не умеет amGroup): если с момента
# первой постановки резерва прошло столько дней, а сделка не дошла до оплаты —
# снимаем резерв сами. Статус сделки в amo НЕ трогаем, только резерв в МС.
RESERVE_TIMEOUT_DAYS = int(os.getenv("RESERVE_TIMEOUT_DAYS", "3"))
RESERVE_TIMEOUT_POLL_INTERVAL_S = int(os.getenv("RESERVE_TIMEOUT_POLL_INTERVAL_S", "900"))  # 15 мин

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
# предоплата. Пустой/непонятный способ
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
# МойСклад API — счёт Ozon (ozon_invoice) читает суммы заказа.
# Только чтение. MS_TOKEN — Bearer-токен главного админа МС (тот же, что в
# проекте woocommerce-sklad).
# ---------------------------------------------------------------------------
MS_API_URL = os.getenv("MS_API_URL", "https://api.moysklad.ru/api/remap/1.2").rstrip("/")
MS_TOKEN = os.getenv("MS_TOKEN", "").strip()
MS_SYNC_POLL_INTERVAL_S = int(os.getenv("MS_SYNC_POLL_INTERVAL_S", "30"))
MS_SYNC_LOOKBACK_MIN = int(os.getenv("MS_SYNC_LOOKBACK_MIN", "120"))
# ═══ Выключатели контура Фулфилмента (05.08.2026, решение Кати) ═══
# Воронку Фулфилмент разобрали и удаляют: сделки переехали в Офис, основную и «Работу
# с базой». Механизмы вокруг неё гасим настройкой, а не удалением кода — если ФФ вернут,
# достаточно снова поставить 1. По умолчанию ВКЛЮЧЕНО: молча отключить чужой контур,
# просто выкатив новый код, нельзя.

# Час ночной ПОЛНОЙ сверки ФФ (amo-driven страховка от промахов узкого окна
# живого опроса: рестарт/деплой/подвисание сервиса дольше lookback теряет
# изменение статуса МС навсегда). ≠1 (Метрика в 01:00), 0..23 МСК.
MS_RECONCILE_HOUR_MSK = int(os.getenv("MS_RECONCILE_HOUR_MSK", "2"))

# Трек-номер: атрибут заказа МойСклад → поле сделки amoCRM. Цель — поле 571657
# «Трек-номер» (то же, что FIELD_CDEK_ORDER_NUMBER; у ФФ-копий оно пустое,
# конфликта с CDEK-синком нет — тот пишет в офисные сделки).
MS_ATTR_TREK = "e25b4e11-2aa4-11f1-0a80-0704003169db"
# Доп. поле заказа МС «Номер заказа на сайте» (= id заказа WooCommerce).
# По нему woocommerce-sklad связывает заказ сайта с заказом покупателя, и по нему
# же сторож order_watchdog проверяет, что заказ вообще доехал.
MS_ATTR_ORDER_NUMBER_ID = os.getenv(
    "MS_ATTR_ORDER_NUMBER_ID", "70c4735f-c542-11f0-0a80-1755000e25a7")

# Сторож заказов (order_watchdog): раз в час сверяет заказы сайта за сутки с
# заказами в МойСкладе и пишет в технический чат, если чего-то не хватает.
# Заведён 07.08.2026 после потери заказа №18287: вебхук не дошёл, а сверка в
# woocommerce-sklad девять дней молча возвращала ноль.
ORDER_WATCHDOG_ENABLED = os.getenv("ORDER_WATCHDOG_ENABLED", "1") == "1"
ORDER_WATCHDOG_INTERVAL_S = int(os.getenv("ORDER_WATCHDOG_INTERVAL_S", "3600"))
ORDER_WATCHDOG_LOOKBACK_H = int(os.getenv("ORDER_WATCHDOG_LOOKBACK_H", "24"))
# Заказ моложе этого возраста ещё может ехать штатно — не тревожим.
ORDER_WATCHDOG_MIN_AGE_MIN = int(os.getenv("ORDER_WATCHDOG_MIN_AGE_MIN", "15"))
FIELD_FF_TREK = 571657

# Протез amgroup (amgroup_fallback): сторонняя интеграция МойСклад -> amoCRM
# встала 02.09.2026 (истёк сертификат *.amgbp.ru, потом лёг сам сервер), с
# 02.09 около 19:30 новые сделки не создаются вовсе. Пока мост чужой не
# починен, модуль сам находит заказы покупателя без сделки и заводит её.
# По умолчанию ВЫКЛЮЧЕН - включаем осознанно, когда решаем, что дублирование
# сделок безопаснее их отсутствия. Сухой режим по умолчанию ВКЛЮЧЁН - сначала
# смотрим лог, что модуль бы сделал, и только потом разрешаем запись.
AMGROUP_FALLBACK_ENABLED = os.getenv("AMGROUP_FALLBACK_ENABLED", "0") == "1"
AMGROUP_FALLBACK_DRY_RUN = os.getenv("AMGROUP_FALLBACK_DRY_RUN", "1") == "1"
AMGROUP_FALLBACK_INTERVAL_SEC = int(os.getenv("AMGROUP_FALLBACK_INTERVAL_SEC", "180"))
# Насколько назад смотрим заказы МойСклада на каждом проходе.
AMGROUP_FALLBACK_LOOKBACK_HOURS = int(os.getenv("AMGROUP_FALLBACK_LOOKBACK_HOURS", "48"))
# Тег на сделку-протез - чтобы отличить от сделок, которые создал бы amgroup сам.
AMGROUP_FALLBACK_TAG = os.getenv("AMGROUP_FALLBACK_TAG", "сбой МС")

# ---------------------------------------------------------------------------
# Склад шоурума (задача Кати 06.08.2026). Кирилл отгружает из отдельного склада
# «Sunscrypt Шоурум», заведённого в МойСкладе 03.08.
#
# Источник правды — УСЛУГА ДОСТАВКИ в заказе: стоит «Самовывоз из Шоурума» →
# склад заказа обязан быть шоурумным; убрали услугу → склад возвращается на
# «Sunscrypt Основной». Менеджеров этим не грузим, склад ведёт showroom_store.py.
#
# ⚠️ Возврат делаем ТОЛЬКО со шоурумного склада: заказы ЭРМС и «Вскрытые» живут
# по своим правилам, трогать их нельзя.
# ---------------------------------------------------------------------------
# По умолчанию ВКЛЮЧЕНО (решение Кати 06.08): отдельный шаг «добавь строку в env»
# ей не нужен, а проверить работу сторожа можно по логам. Переменная остаётся
# аварийным выключателем: SHOWROOM_STORE_ENABLED=0 усыпляет модуль без выкатки кода.
SHOWROOM_STORE_ENABLED = os.getenv("SHOWROOM_STORE_ENABLED", "1") == "1"
SHOWROOM_STORE_POLL_INTERVAL_S = int(os.getenv("SHOWROOM_STORE_POLL_INTERVAL_S", "120"))
SHOWROOM_STORE_LOOKBACK_MIN = int(os.getenv("SHOWROOM_STORE_LOOKBACK_MIN", "30"))

MS_STORE_SHOWROOM_ID = os.getenv("MS_STORE_SHOWROOM_ID", "1c480a71-8f76-11f1-0a80-16b200011979")
MS_STORE_MAIN_ID = os.getenv("MS_STORE_MAIN_ID", "0e5a2b05-c413-11ee-0a80-13fd002f63f9")
# Услуга «Самовывоз из Шоурума» в МойСкладе (id надёжнее названия: название правят руками)
MS_SERVICE_SHOWROOM_PICKUP_ID = os.getenv(
    "MS_SERVICE_SHOWROOM_PICKUP_ID", "15ff040c-529c-11f1-0a80-0d0c00781fe6")
# Запасной признак, если услугу пересоздадут с новым id
SHOWROOM_SERVICE_NAME_MARKER = "самовывоз из шоурума"

# Telegram
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
_raw_chat_id = os.getenv("TG_ALLOWED_CHAT_ID", "")
TG_ALLOWED_CHAT_ID: int | None = int(_raw_chat_id) if _raw_chat_id else None
# Прокси для Telegram API (api.telegram.org заблокирован в РФ).
# Поддерживается HTTP/HTTPS из коробки: http://user:pass@host:port
# Для SOCKS5 нужен пакет aiohttp_socks + код в telegram_bot.py не активирует
# его автоматически (см. README).
TG_PROXY_URL = os.getenv("TG_PROXY_URL", "").strip()

# Секрет в пути вебхука UIS «Потерянный звонок» (/uis/<secret>) — простая защита
# от посторонних запросов. Пусто → эндпоинт /uis отвечает 403 (выключен).
UIS_WEBHOOK_SECRET = os.getenv("UIS_WEBHOOK_SECRET", "").strip()

# ---------------------------------------------------------------------------
# Wazzup — приём сообщений WA/TG для SLA-уведомлений «клиент без ответа N мин».
# Источник направления сообщения (входящее/исходящее) — вебхук Wazzup, где
# сообщения (messages[]) и статусы доставки (statuses[]) — разные массивы, поэтому
# служебная запись Wazzup (ошибка WABA-шаблона «SYSTEM WZ») не считается входящим.
# ---------------------------------------------------------------------------
WAZZUP_API_URL = os.getenv("WAZZUP_API_URL", "https://api.wazzup24.com/v3").rstrip("/")
WAZZUP_API_KEY = os.getenv("WAZZUP_API_KEY", "").strip()
# Секрет в пути вебхука Wazzup (/wazzup/<secret>) — простая защита. Пусто → /wazzup 403.
WAZZUP_WEBHOOK_SECRET = os.getenv("WAZZUP_WEBHOOK_SECRET", "").strip()
# Полный URL, который прописываем в подписке Wazzup (webhooksUri). По умолчанию —
# наш публичный адрес + /wazzup/<secret>. Пусто → авто-подписку не оформляем.
WAZZUP_WEBHOOK_URL = os.getenv(
    "WAZZUP_WEBHOOK_URL",
    f"{PUBLIC_BASE_URL}/wazzup/{WAZZUP_WEBHOOK_SECRET}" if WAZZUP_WEBHOOK_SECRET else "",
).strip()
# ЗАХАРДКОЖЕНО ВКЛ (решение Кати 09.07): env НЕ читаем, чтобы пустая строка в .env
# случайно не выключила фичу. Оформление подписки Wazzup на вебхуки при старте
# (PATCH /v3/webhooks) — реально подписывает только при заданных WAZZUP_API_KEY и
# WAZZUP_WEBHOOK_URL (Wazzup при установке шлёт тест, ждёт 200).
WAZZUP_ENSURE_WEBHOOK = True

# ЗАХАРДКОЖЕНО ВКЛ (решение Кати 09.07): приём вебхука + цикл проверки активны.
# Реальные алерты идут только при наличии ключа/секрета Wazzup и в окне 12–19 МСК.
WAZZUP_SLA_ENABLED = True
# Персональный тег ответственного: amo user_id → Telegram @handle. Тегаем
# ответственного по сделке + WAZZUP_ALWAYS_TAG. Если ответственного не удалось
# определить за WAZZUP_RESPONSIBLE_TIMEOUT_S (нет сделки / нет хендла в карте) —
# тегаем всю смену (MANAGERS_ON_SHIFT в tg_recipients). Та же карта — и для
# пропущенных звонков (uis_missed_call). ⚠️ карту сверять с Катей.
WAZZUP_TG_HANDLES = {
    9291546:  "@thebarsa1",    # Игорь Оанча
    13929334: "@egorkonsss",   # Егор Константинов
    13946318: "@offf1cer",     # Кирилл Полесский
    # 11513202 Александр Гладков (Саша, РОП) — снят из карты 13.08.2026 (отпуск,
    # его же просьба). Алерты по его сделкам теперь уходят фолбэком всей смене.
    13822630: "@sunscryptb2b", # Артём Коннов (B2B/ОПТ, pipeline 10131762)
    # Тимофей Мигачёв (13821022) — уволен, в карте не нужен.
}
# Кого тегаем ВСЕГДА вместе с ответственным. Пусто → надзорного тега нет.
# Был "@gladkov_369" (Саша, РОП) — снят 13.08.2026 по решению Кати: Саша ушёл в
# отпуск и попросил убрать его из уведомлений, а надзорный слой вернём не тегом
# РОПа, а отдельной задачей про точки контроля.
WAZZUP_ALWAYS_TAG = ""
WAZZUP_RESPONSIBLE_TIMEOUT_S = float(os.getenv("WAZZUP_RESPONSIBLE_TIMEOUT_S", "10"))

WAZZUP_SLA_MINUTES = int(os.getenv("WAZZUP_SLA_MINUTES", "30"))       # порог «без ответа», мин
WAZZUP_SLA_WINDOW_START_H = int(os.getenv("WAZZUP_SLA_WINDOW_START_H", "12"))  # окно, МСК, включительно
WAZZUP_SLA_WINDOW_END_H = int(os.getenv("WAZZUP_SLA_WINDOW_END_H", "19"))      # окно, МСК, до (не вкл.)
WAZZUP_SLA_POLL_INTERVAL_S = int(os.getenv("WAZZUP_SLA_POLL_INTERVAL_S", "60"))  # период проверки, сек

# Ускоренный порог для клиента, который едет за заказом САМ (Катя 13.08.2026).
# Тип доставки сделки — наш самовывоз (офис или шоурум, DELIVERY_PICKUP_MARKERS) →
# ждём не WAZZUP_SLA_MINUTES, а эти минуты: человек может уже стоять у дверей, и
# 15 минут молчания для него много. Гейт — только тип доставки: этап и воронка
# сделки НЕ смотрим (её слова: «вне зависимости от этапа»). «CDEK: Самовывоз» это
# ПВЗ СДЭК, он под правило не идёт. Тег в таком алерте один — SLA_PICKUP_TAG.
WAZZUP_SLA_PICKUP_MINUTES = int(os.getenv("WAZZUP_SLA_PICKUP_MINUTES", "3"))

# Второй порог: менеджерам уже написали, а клиенту всё равно не ответили (Катя 28.08.2026).
# Это КОНТУР 2 из ТЗ точек контроля — уведомление уходит не отделу, а руководству, в
# отдельную группу ROP_ALERT_CHAT_ID. Пустой чат → эскалации нет совсем.
WAZZUP_SLA_ESCALATE_MINUTES = int(os.getenv("WAZZUP_SLA_ESCALATE_MINUTES", "30"))

# Группа «ОП срочные уведомления» — чат РУКОВОДСТВА (просьба Андрея через Катю 27.08.2026).
# Сюда идёт только провал, а не поток событий: дословно «им каждый пропущенный и ссылки не
# нужен, только пиздец». Пусто → ничего не шлём, это выключатель.
#
# ⚠️ id без префикса «-100» — обычная группа, а НЕ супергруппа. Включат в ней топики,
# историю для новых участников или сделают публичной — Telegram сконвертирует её, и id
# поменяется. Отправка при этом умирает молча, поэтому в telegram_bot.send_alert ловится
# migrate_to_chat_id: новый адрес попадёт в лог, и его надо будет прописать сюда.
ROP_ALERT_CHAT_ID = int(os.getenv("ROP_ALERT_CHAT_ID", "0")) or None

# Имена менеджеров для чата руководства. В общем чате ОП мы тегаем человека (@handle) —
# там он и прочитает. В группе руководства тег бесполезен: менеджеров в ней нет, а
# «@egorkonsss» руководителю читать неудобно. Правило Кати 03.08.2026 — в том, что читает
# человек, ID и техническим кличкам не место, поэтому здесь имя словами.
# Второй контур по пропущенным звонкам: час прошёл, клиенту не перезвонили (Катя 28.08.2026).
# Порог по её словам - «не перезвонили в течение часа». Окно шире, чем у SLA переписки:
# звонки идут с утра, и пропущенный в 10:30 надо досчитать к 11:30, а не к обеду.
MISSED_CALLBACK_ESCALATE_MINUTES = int(os.getenv("MISSED_CALLBACK_ESCALATE_MINUTES", "60"))
MISSED_CALLBACK_WINDOW_START_H = int(os.getenv("MISSED_CALLBACK_WINDOW_START_H", "10"))
MISSED_CALLBACK_WINDOW_END_H = int(os.getenv("MISSED_CALLBACK_WINDOW_END_H", "20"))
MISSED_CALLBACK_POLL_INTERVAL_S = int(os.getenv("MISSED_CALLBACK_POLL_INTERVAL_S", "300"))

# Новый лид не взяли в работу (Катя 28.08.2026). Порог в РАБОЧИХ минутах: ночь и вечер
# не считаются, отсчёт идёт только внутри окна. Начало окна 12:00 - требование из ТЗ
# точек контроля: до полудня менеджеры разгребают ночную пачку заявок.
NEW_LEAD_ESCALATE_MINUTES = int(os.getenv("NEW_LEAD_ESCALATE_MINUTES", "120"))
NEW_LEAD_WINDOW_START_H = int(os.getenv("NEW_LEAD_WINDOW_START_H", "12"))
NEW_LEAD_WINDOW_END_H = int(os.getenv("NEW_LEAD_WINDOW_END_H", "19"))
NEW_LEAD_POLL_INTERVAL_S = int(os.getenv("NEW_LEAD_POLL_INTERVAL_S", "300"))

MANAGER_NAMES = {
    9291546:  "Игорь Оанча",
    13929334: "Егор Константинов",
    13946318: "Кирилл Полесский",
    11513202: "Александр Гладков",
    13822630: "Артём Коннов",
}

# Каналы Wazzup, которые SLA не сторожит (решение Кати 06.08.2026). Пока один —
# телеграм +7 901 960-80-28 (в Wazzup зовётся «79019608028»), личный канал Саши
# Гладкова под партнёрку: обменники, крипто-боты, блогеры, рекламные каналы.
# По корпусу 28.07–06.08 оттуда пришло 35 алертов из 153 — почти четверть, и это
# не клиенты: отдел продаж будили «Вас приветствует MAXMINER» и «Выберите язык :».
# Фильтром текста такое не лечится, там нет закрывашки — просто не наш разговор.
# Клиентские каналы (WhatsApp и телеграм +7 926 082-36-03) сторожим как раньше.
WAZZUP_SLA_SKIP_CHANNELS = {
    c.strip() for c in os.getenv(
        "WAZZUP_SLA_SKIP_CHANNELS",
        "33be01a6-7d00-4fae-b797-93fd66e9f0f4",
    ).split(",") if c.strip()
}

# ---------------------------------------------------------------------------
# Контроль ДОСТАВКИ Wazzup (01.08.2026, просьба Кати). Не путать с SLA выше:
# там таймер про молчание менеджера, здесь — про то, что сообщение не дошло до
# клиента, хотя интерфейс нарисовал «отправлено». Модуль wazzup_delivery.
# ---------------------------------------------------------------------------
# ЗАХАРДКОЖЕНО ВКЛ, как WAZZUP_SLA_ENABLED: пустая строка в .env не должна
# случайно погасить контроль.
WAZZUP_DELIVERY_ENABLED = True
# Сколько ждём delivered, прежде чем считать «отправлено» враньём.
WAZZUP_UNDELIVERED_MINUTES = int(os.getenv("WAZZUP_UNDELIVERED_MINUTES", "15"))
WAZZUP_DELIVERY_POLL_INTERVAL_S = int(os.getenv("WAZZUP_DELIVERY_POLL_INTERVAL_S", "60"))
# Каналы, где таймер «sent без delivered» имеет смысл. У Telegram Personal
# delivered не приходит вообще (срез 31.07: 15 исходящих, delivered — ноль),
# там sent висит до прочтения → таймер дал бы ложные алерты. Ошибки (error)
# ловятся на ВСЕХ каналах независимо от этого списка.
WAZZUP_UNDELIVERED_CHAT_TYPES = {
    s.strip().lower()
    for s in os.getenv("WAZZUP_UNDELIVERED_CHAT_TYPES", "whatsapp,wapi").split(",")
    if s.strip()
}
# Куда слать. Пусто → технический чат (TG_ALLOWED_CHAT_ID, тот же, куда /print и
# сторож). Решение Кати 01.08: менеджеров и чат ОП пока не трогаем.
_raw_delivery_chat = os.getenv("WAZZUP_DELIVERY_CHAT_ID", "").strip()
WAZZUP_DELIVERY_CHAT_ID: int | None = int(_raw_delivery_chat) if _raw_delivery_chat else None
_raw_delivery_thread = os.getenv("WAZZUP_DELIVERY_THREAD_ID", "").strip()
WAZZUP_DELIVERY_THREAD_ID: int | None = int(_raw_delivery_thread) if _raw_delivery_thread else None
# Антиспам на случай массового сбоя: больше BURST_MAX алертов за окно — дальше
# одна сводная строка вместо лавины.
WAZZUP_DELIVERY_BURST_MAX = int(os.getenv("WAZZUP_DELIVERY_BURST_MAX", "8"))
WAZZUP_DELIVERY_BURST_WINDOW_S = int(os.getenv("WAZZUP_DELIVERY_BURST_WINDOW_S", "600"))
# Вечерняя сводка «отправлено / дошло / упало» по каналам, час МСК. Цифры даёт
# панель (GET /api/ingest/wazzup/daily) — у неё вся история, а счётчики в памяти
# интеграции обнуляются на каждом деплое. Пусто → сводку не шлём.
_raw_summary_hour = os.getenv("WAZZUP_SUMMARY_HOUR_MSK", "20").strip()
WAZZUP_SUMMARY_HOUR_MSK_ENABLED = bool(_raw_summary_hour)
WAZZUP_SUMMARY_HOUR_MSK = int(_raw_summary_hour) if _raw_summary_hour else 20

# Дайджест «висит sent, доставки нет» — час МСК (решение Кати 02.08.2026).
# Раньше каждое такое сообщение било в чат через 15 минут; важен сам факт
# отправки, а не скорость доставки, поэтому копим за день и шлём ОДНИМ списком.
# Ошибки отправки (error) этого не касаются — они уходят сразу, как и раньше.
# Пусто → дайджест не шлём (и «висяки» тогда не всплывают нигде, кроме панели).
_raw_stuck_hour = os.getenv("WAZZUP_STUCK_DIGEST_HOUR_MSK", "18").strip()
WAZZUP_STUCK_DIGEST_ENABLED = bool(_raw_stuck_hour)
WAZZUP_STUCK_DIGEST_HOUR_MSK = int(_raw_stuck_hour) if _raw_stuck_hour else 18
# Сколько строк печатаем в дайджесте; остальные сворачиваем в «и ещё N».
WAZZUP_STUCK_DIGEST_MAX_LINES = int(os.getenv("WAZZUP_STUCK_DIGEST_MAX_LINES", "30"))
# Потолок очереди в памяти, чтобы сутки молчания не съели процесс.
WAZZUP_STUCK_QUEUE_MAX = int(os.getenv("WAZZUP_STUCK_QUEUE_MAX", "500"))

# Глушилка по вложению (просьба Кати 05.08.2026). Офисный шаблон уходит двумя
# сообщениями: сам шаблон и картинка «наш офис.jpg» отдельным сообщением. Шаблон
# доходит, а картинка регулярно падает с 24_HOURS_EXCEEDED — обычным сообщением
# в закрытое 24-часовое окно нельзя. Менять там нечего, а алерт и примечание
# пугают. Совпало по имени файла (подстрока в content_uri) → молчим: ни ТГ, ни
# примечания в сделке. На счётчики панели это не влияет — туда вебхуки уезжают
# отдельно (wazzup_forward), картина по ошибкам остаётся полной.
WAZZUP_DELIVERY_MUTE_CONTENT = [
    s.strip().lower()
    for s in os.getenv("WAZZUP_DELIVERY_MUTE_CONTENT", "наш офис.jpg").split(",")
    if s.strip()
]

# ---------------------------------------------------------------------------
# Ozon Pay: счёт СБП из amo — замена виджета int2_ozonpay (MAG-285).
# createPayment (payType=SBP), режим «самостоятельная интеграция» — тот же,
# что у плагина сайта sunscrypt-sbp, и ключи ТЕ ЖЕ (ЛК Ozon Pay → Магазины →
# Интеграция). Суммы Ozon и МойСклад обе в копейках.
# ---------------------------------------------------------------------------
# Мастер-флаг: без "1" вебхук не ставит счета в очередь (безопасный выкат).
OZON_INVOICE_ENABLED = os.getenv("OZON_INVOICE_ENABLED", "").strip() == "1"
OZON_PAY_API_URL = os.getenv("OZON_PAY_API_URL", "https://payapi.ozon.ru").rstrip("/")
OZON_PAY_ACCESS_KEY = os.getenv("OZON_PAY_ACCESS_KEY", "").strip()
OZON_PAY_SECRET_KEY = os.getenv("OZON_PAY_SECRET_KEY", "").strip()
# Ссылка живёт сутки (дефолт Ozon 600с=10мин — протухнет раньше, чем клиент откроет).
OZON_INVOICE_TTL_S = int(os.getenv("OZON_INVOICE_TTL_S", "86400"))
OZON_INVOICE_REDIRECT_URL = os.getenv("OZON_INVOICE_REDIRECT_URL", "https://sunscrypt.ru/").strip()
# Секрет подписи УВЕДОМЛЕНИЙ Ozon (отдельный от secretKey, ЛК Ozon Pay; тот же,
# что у плагина сайта). Задан → в createPayment передаём notificationUrl
# (PUBLIC_BASE_URL/ozon_notify) и по вебхуку «Completed» сами двигаем сделку в
# «Оплата получена». Пусто → вебхук-факт оплаты выключен (MVP-поведение).
OZON_PAY_NOTIFICATION_SECRET_KEY = os.getenv("OZON_PAY_NOTIFICATION_SECRET_KEY", "").strip()

# CLEVER Основная, схема «тех-этап» (17.07.2026): менеджер двигает сделку в
# «Оплата запрошена» (НОВЫЙ тех-этап 87280230, без DP-автоматики) → мы создаём
# счёт, пишем ссылку в 577617 и ОДНИМ атомарным PATCH переводим сделку в
# «Ссылка отправлена» (бывший «Оплата запрошена» 83537866) — там штатные боты
# (7173) шлют шаблон с уже заполненным полем. Ошибка счёта → сделка остаётся
# висеть на тех-этапе с тегом (видно в воронке).
PIPELINE_CLEVER_MAIN = 10593102

# Воронка ОПТ — вторая воронка-ИСТОЧНИК переноса (09.08.2026). Этапы 142/143 у
# неё те же, что у розницы, поэтому контракт «переносим на входе в 142, ни на
# этап раньше» повторяется один в один: своё «Оплата получена» 86132358 в
# триггер НЕ берём — сделка обязана физически войти в ОПТ/142, иначе won_at
# останется NULL и опт-продажи молча обнулятся у панели и Метрики.
PIPELINE_OPT = 10131762

# Вход воронки: хаб «Новый лид» + четыре буферных, куда падают заявки до
# распределения Genezis. Свежий заказ с сайта всегда в одном из пяти.
STATUS_NEW_LEAD = 83537714
STATUS_NEW_LEAD_BUFFERS = (83915186, 84215622, 86794306, 86794354)
STATUS_NEW_LEAD_ALL = {STATUS_NEW_LEAD, *STATUS_NEW_LEAD_BUFFERS}

# Шоурум-алерт (showroom_alert): заказ с самовывозом → сообщение в топик ШОУРУМ.
# ⚠️ Мастер-флаг заведён 07.08.2026 после спама в бою: без него единственным
# способом заглушить фичу была правка кода прямо на сервере.
SHOWROOM_ALERT_ENABLED = os.getenv("SHOWROOM_ALERT_ENABLED", "1") == "1"
# Пауза перед чтением сделки: поля (состав, сумма, контакт) плагин сайта
# дозаписывает не разом, первый вебхук приходит с полупустой сделкой.
SHOWROOM_ALERT_DELAY_S = int(os.getenv("SHOWROOM_ALERT_DELAY_S", "90"))
# Возраст сделки, старше — не наш случай. Защита от массовых прогонов по старью.
SHOWROOM_ALERT_MAX_AGE_MIN = int(os.getenv("SHOWROOM_ALERT_MAX_AGE_MIN", "60"))

STATUS_PAYMENT_REQUESTED = 87280230   # «Оплата запрошена» (тех-этап, вход)
STATUS_LINK_SENT = 83537866           # «Ссылка отправлена» (боты этапа живут здесь)
STATUS_PAYMENT_RECEIVED = 83537874    # «Оплата получена» (этап 2 — автодвижение по факту оплаты)
FIELD_PAYMENT_LINK = 577617           # «Ссылка для оплаты»
# «Другая сумма» (text, создано Катей 20.07.2026): если заполнено — счёт СБП
# создаётся на ЭТУ сумму (в рублях) вместо суммы заказа МС; пусто — сумма заказа.
FIELD_INVOICE_OTHER_AMOUNT = 578141
# «Оплата картой» (checkbox, создано Катей 21.07.2026): галочка стоит → счёт
# создаётся ВМЕСТЕ с заказом Ozon → ссылка ведёт на checkout.ozon.ru (страница
# выбора способа: карта/СБП/Ozon Карта). Пусто → чистый СБП (прямая qr.nspk.ru).
# У Ozon нет «прямой только-карты» ссылки — карта живёт лишь на checkout-странице.
FIELD_INVOICE_BY_CARD = 578145
TAG_INVOICE_ERROR = "ошибка счёта"

# Реконсиляция оплат (28.07.2026). Живой тест показал: при оплате КАРТОЙ
# (checkout.ozon.ru, order-флоу) Ozon НЕ шлёт уведомление на notificationUrl —
# счёт в ЛК «Оплачен», а сделка молча висит на «Ссылка отправлена» (карта 0/2,
# СБП 6/6 за те же сутки). Поэтому не ждём вебхук, а сами опрашиваем Ozon по
# висящим счетам: getPaymentDetails → «Completed» → двигаем сделку. Чинит и
# карту, и любой потерянный вебхук СБП (сеть моргнула, рестарт контейнера).
# 0 = выключить фоновый опрос.
OZON_RECONCILE_INTERVAL_S = int(os.getenv("OZON_RECONCILE_INTERVAL_S", "180"))
# Пересылка вебхуков Wazzup в панель team.sunscrypt.ru (wazzup_forward.py, го Кати
# 31.07.2026): тексты WA/TG сохраняет ПАНЕЛЬ (таблица wazzup_message), мы только
# пересылаем. Пустой токен → пересылка выключена (в лог — предупреждение).
TEAM_INGEST_URL = os.getenv(
    "TEAM_INGEST_URL", "https://team.sunscrypt.ru/api/ingest/wazzup"
).strip()
TEAM_INGEST_TOKEN = os.getenv("TEAM_INGEST_TOKEN", "").strip()

# Счёт выставлен, а оплаты нет дольше N минут → напоминание в ТГ ОП с
# @ответственного. Чтобы про застрявшую оплату узнавали мы, а не клиент.
# 0 = напоминания выключены. Час — договорённость встречи с Сашей 30.07.2026
# (было 40 минут, порог никем не обсуждался).
OZON_STALE_ALERT_MIN = int(os.getenv("OZON_STALE_ALERT_MIN", "60"))
# Рабочее окно напоминаний, МСК. Без него они уходили ночью: инцидент
# 31.07.2026, сообщения в 00:11 и 00:14. Совпадает с окном Wazzup SLA.
OZON_ALERT_WINDOW_START_H = int(os.getenv("OZON_ALERT_WINDOW_START_H", "12"))
OZON_ALERT_WINDOW_END_H = int(os.getenv("OZON_ALERT_WINDOW_END_H", "19"))
# Час вечернего напоминания: первое уходит через OZON_STALE_ALERT_MIN, второе
# вечером того же дня, дальше каждый вечер, пока счёт не оплатят (встреча 30.07).
OZON_STALE_EVENING_H = int(os.getenv("OZON_STALE_EVENING_H", "18"))
# Трое суток без оплаты → отдельно руководителю. Пустой чат = эскалация молчит:
# тегать Сашу в общем чате ОП нельзя, ему нужен свой (решение встречи 30.07.2026).
OZON_STALE_ESCALATE_DAYS = float(os.getenv("OZON_STALE_ESCALATE_DAYS", "3"))
OZON_STALE_ESCALATE_CHAT_ID = os.getenv("OZON_STALE_ESCALATE_CHAT_ID", "").strip()

# ---------------------------------------------------------------------------
# Office Transfer (30.07.2026): УР(142)/ЗНР(143) в [CLEVER] Основная переносятся
# (PATCH pipeline_id+status_id той же сделки) в целевую воронку/этап вместо
# нативного копирования F5-виджетом/«Создать сделку» — см. office_transfer.py.
# Решение Кати: без ретроактивности (не трогаем сделки, уже висевшие в УР/ЗНР
# до включения), рулим поэтапно по мере отключения нативной автоматики.
# ---------------------------------------------------------------------------
# Мастер-флаг, OFF по умолчанию.
OFFICE_TRANSFER_ENABLED = os.getenv("OFFICE_TRANSFER_ENABLED", "").strip() == "1"

# Свой флаг на каждое правило (независимо от мастер-флага — оба должны быть
# включены), чтобы включать по одному по мере отключения нативки в Digital Funnel.
OFFICE_TRANSFER_RULE_UR_DELIVERY = os.getenv("OFFICE_TRANSFER_RULE_UR_DELIVERY", "").strip() == "1"
OFFICE_TRANSFER_RULE_UR_PICKUP = os.getenv("OFFICE_TRANSFER_RULE_UR_PICKUP", "").strip() == "1"
OFFICE_TRANSFER_RULE_UR_WAYBILL = os.getenv("OFFICE_TRANSFER_RULE_UR_WAYBILL", "").strip() == "1"
OFFICE_TRANSFER_RULE_UR_PREORDER = os.getenv("OFFICE_TRANSFER_RULE_UR_PREORDER", "").strip() == "1"
OFFICE_TRANSFER_RULE_UR_RESERVE = os.getenv("OFFICE_TRANSFER_RULE_UR_RESERVE", "").strip() == "1"
OFFICE_TRANSFER_RULE_ZNR_WAITLIST = os.getenv("OFFICE_TRANSFER_RULE_ZNR_WAITLIST", "").strip() == "1"
OFFICE_TRANSFER_RULE_ZNR_ACADEMY = os.getenv("OFFICE_TRANSFER_RULE_ZNR_ACADEMY", "").strip() == "1"
OFFICE_TRANSFER_RULE_ZNR_OPT = os.getenv("OFFICE_TRANSFER_RULE_ZNR_OPT", "").strip() == "1"
OFFICE_TRANSFER_RULE_UR_POST = os.getenv("OFFICE_TRANSFER_RULE_UR_POST", "").strip() == "1"

# Воронка ОПТ как ИСТОЧНИК переноса (09.08.2026). Отдельный флаг, а не правило:
# сами правила у опта те же пять, что у розницы (решение Кати 09.08.2026), новый
# здесь только вход. Порядок включения тот же — сперва убрать ручное
# копирование в ОПТ, потом флаг, иначе получим и копию, и перенос.
OFFICE_TRANSFER_SOURCE_OPT = os.getenv("OFFICE_TRANSFER_SOURCE_OPT", "").strip() == "1"

# Cutover-граница (unix ts): события ДО неё игнорируются везде (вебхук и
# reconciliation) — без ретроактивности. 0 = не задана; в этом состоянии
# фича не должна включаться на проде (задать перед первым боевым включением).
OFFICE_TRANSFER_SINCE_TS = int(os.getenv("OFFICE_TRANSFER_SINCE_TS", "0"))

# ═══ Заморозка на время миграции воронок (решение Кати 03.08.2026) ═══
# Скрипт переноса вешает каждой перенесённой сделке тег MIGRATION_FREEZE_TAG.
# Пока идёт окно [FROM, TO], наши обработчики такие сделки игнорируют:
# office_transfer, Метрика, Woo, гейт КОНТРОЛЬ, «Причина→ЗИН». После окна тег
# остаётся (менеджеру видно, откуда сделка), блокировка снимается — работают
# как с обычными. Подробности и границы применимости — migration_freeze.py.
# Пустой тег или TO=0 → механизм выключен целиком.
MIGRATION_FREEZE_TAG = os.getenv("MIGRATION_FREEZE_TAG", "").strip()
MIGRATION_FREEZE_FROM_TS = int(os.getenv("MIGRATION_FREEZE_FROM_TS", "0"))
MIGRATION_FREEZE_TO_TS = int(os.getenv("MIGRATION_FREEZE_TO_TS", "0"))
# ТОЧЕЧНАЯ отсечка вебхуков на время МАССОВОГО прогона (легаси-воронка, ~103 тыс.
# сделок = двести тысяч вебхуков). Гасит только поток самой миграции: вебхуки из
# воронок-источников и заходы в 142/143 основной воронки. Накладные, счета и новые
# заказы идут штатно. Действует только ВНУТРИ окна — migration_freeze.bulk_skip().
MIGRATION_BULK_PAUSE = os.getenv("MIGRATION_BULK_PAUSE", "").strip() == "1"
# Воронки, ИЗ которых идёт перенос. Через запятую, по умолчанию легаси «Отдел продаж».
MIGRATION_SOURCE_PIPELINES = {
    int(x) for x in os.getenv("MIGRATION_SOURCE_PIPELINES", "901105").replace(" ", "").split(",") if x
}


# Периодическая reconciliation-проверка (страховка от зависания amo API):
# пересматривает сделки, недавно вошедшие в 142/143, но ещё не перенесённые.
# 0 = фоновый проход выключен (только вебхук).
OFFICE_TRANSFER_RECONCILE_INTERVAL_S = int(os.getenv("OFFICE_TRANSFER_RECONCILE_INTERVAL_S", "120"))

# Сделка не переносится дольше N минут → один алерт в ТГ (дедуп, снимается при
# успехе). 0 = алерт выключен.
OFFICE_TRANSFER_STALE_ALERT_MIN = int(os.getenv("OFFICE_TRANSFER_STALE_ALERT_MIN", "30"))

# Поля сделки
FIELD_DELIVERY_TYPE = 577315       # Тип доставки (text) — используется и в showroom_tag.py как литерал
FIELD_APPLICATION_TYPE = 577671    # Тип заявки (select)
FIELD_ORDER_WAREHOUSE = 576723     # Склад заказа (select)
FIELD_FORMER_RESPONSIBLE = 578151  # Ответственный МОП (text) — прежний ответственный,
                                    # пишем ТОЛЬКО при переносе в Офис (не в другие воронки)

# 577671 «Тип заявки» — enum_id
APPLICATION_TYPE_ORDER = 1041237     # Заказ
APPLICATION_TYPE_PREORDER = 1041239  # Предзаказ
APPLICATION_TYPE_RESERVE = 1041903   # Резерв (сверено live 25.08.2026)

# 576723 «Склад заказа» — enum_id, нужные для office-transfer (у поля есть и другие значения)
WAREHOUSE_SUNSCRYPT_MAIN = 1040201    # Sunscrypt Основной
WAREHOUSE_SUNSCRYPT_OPENED = 1040207  # Sunscrypt Вскрытые
WAREHOUSE_ERMS_MAIN = 1041653         # ЭРМС_Основной
WAREHOUSE_SUNSCRYPT_SHOWROOM = 1041885  # Sunscrypt Шоурум (заведён 03.08.2026 под отгрузки Кирилла)
# Шоурум добавлен 06.08.2026: без него сделка с новым складом переставала подходить
# под правила автопереноса и зависала в УР розницы с алертом «заказ заполнен некорректно».
OFFICE_TRANSFER_WAREHOUSES = {
    WAREHOUSE_SUNSCRYPT_MAIN, WAREHOUSE_SUNSCRYPT_OPENED, WAREHOUSE_SUNSCRYPT_SHOWROOM}

# 577623 (= DUP_REASON_FIELD_ID выше, живое имя «Причина ЗИН») — доп. enum_id для office-transfer
REASON_WAITLIST = 1041245  # Лист ожидания
REASON_ACADEMY = 1041243   # Академия
REASON_OPT = 1041905       # Опт (сверено live 25.08.2026)

# Подстроки «Тип доставки» (577315, text) — регистронезависимо (.casefold(), как DELIVERY_SHOWROOM_MARKER)
# Самовывоз бывает двух видов: из офиса (как было) и из шоурума (с 06.08.2026, свой склад).
# Для розницы оба ведут в один и тот же этап Офиса; для ОПТ самовывоз из шоурума —
# исключение (см. DELIVERY_SHOWROOM_PICKUP_MARKER + STATUS_OFFICE_RESERVE ниже).
DELIVERY_SHOWROOM_PICKUP_MARKER = "самовывоз из шоурума"
DELIVERY_PICKUP_MARKERS = (DELIVERY_SHOWROOM_MARKER, DELIVERY_SHOWROOM_PICKUP_MARKER)
DELIVERY_COURIER_MOSCOW_MARKER = "курьером по москве"
DELIVERY_CDEK_MARKERS = ("cdek", "сдэк")
DELIVERY_RUSSIAN_POST_MARKER = "почта россии"

# Целевые этапы воронки Офис (PIPELINE_OFFICE уже определена ниже)
STATUS_OFFICE_DELIVERY = 75426826       # «Оформить доставку» (Достависта)
STATUS_OFFICE_PICKUP = 75426862         # «Самовывоз»
STATUS_OFFICE_PREORDER_PAID = 83953914  # «Предзаказ оплачен»
# «Отложенный/резерв товар» — ОПТ + самовывоз из ШОУРУМА (решение Кати 10.08.2026):
# в отличие от розницы, опт-заказ на этот момент физически ещё не выдан клиенту,
# поэтому уходит не в УР(142), а сюда — товар числится в резерве до выдачи.
STATUS_OFFICE_RESERVE = 75426858
# «Сделать накладную» — уже есть как STATUS_CREATE_WAYBILL (75426822)

# Целевая воронка «Лист ожидания»
PIPELINE_WAITLIST = 10611934
STATUS_WAITLIST = 83669950  # «Лист ожидания»

# Целевая воронка «Академия»
PIPELINE_ACADEMY = 8642410
STATUS_ACADEMY_FIRST_CONTACT = 70070966  # «Первичный контакт»

# Целевые этапы воронки ОПТ (PIPELINE_OPT уже определена выше) — причина ЗИН=Опт
# (постановка Тианы 25.08.2026): контакт без других сделок → «Первичный контакт»;
# контакт уже встречался (есть другие сделки) → «Найден контакт».
STATUS_OPT_PRIMARY_CONTACT = 80276162  # «Первичный контакт» (новый контакт)
STATUS_OPT_CONTACT_FOUND = 86989418    # «Найден контакт» (контакт уже был)

# Ответственный при переносе в Офис — Екатерина Зубалий. ⚠️ id 13962422 — деактивированный
# дубль аккаунта (is_active=false, сверено live 30.07.2026) — НЕ использовать.
RESPONSIBLE_OFFICE_MANAGER_USER_ID = 13963494

# Ответственный при переносе в ОПТ (причина ЗИН=Опт) — Артём Коннов, B2B/ОПТ-менеджер
# (id из WAZZUP_TG_HANDLES ниже). Решение Тианы 25.08.2026: у lead_distribution.py
# нет точки входа в воронке ОПТ, без явной смены сделка осталась бы на прежнем
# розничном МОПе.
RESPONSIBLE_OPT_MANAGER_USER_ID = 13822630

# Тег зависшего/неудавшегося переноса. Информационный — НЕ блокирует повторные
# попытки reconciliation (по образцу TAG_KONTROL_ERROR).
TAG_OFFICE_TRANSFER_ERROR = "ошибка переноса в офис"

# Теги-сигналы «автоперенос невозможен, нужен человек» (решение Кати 31.07.2026,
# анализ 90 дней: на 1816 УР — 6 заказов с пустым «Типом доставки», 7 мусорных
# без заказа, 2 с битыми полями). Тег = дедуп ТГ-алерта (переживает рестарт).
# Менеджер дозаполнил поля → вебхук на обновление сделки повторит матчинг,
# сделка переносится штатно, тег снимается.
TAG_NO_DELIVERY = "доставка не заполнена"        # Заказ + склад на месте, «Тип доставки» пуст
TAG_BAD_FILL = "заказ заполнен некорректно"      # нет типа заявки/склада, чужой склад, нераспознанная доставка

# ---------------------------------------------------------------------------
# Lead Distribution (05.08.2026): конструктор профилей распределения лидов —
# замена нативного виджета «Генезис» (F5). В отличие от office_transfer, здесь
# нет захардкоженных правил в этом файле — профили редактируются в team-panel
# (владелец данных с 09.08.2026), amo_fix_fields читает их через
# lead_distribution_profiles_client.py и держит write-through кэш в
# var/lead_distribution_profiles.json. См. lead_distribution.py.
# ---------------------------------------------------------------------------
# Мастер-флаг, OFF по умолчанию. Отдельные профили ТАКЖЕ должны быть enabled=True
# в своей записи — оба уровня должны совпасть, как OFFICE_TRANSFER_ENABLED + правило.
LEAD_DISTRIBUTION_ENABLED = os.getenv("LEAD_DISTRIBUTION_ENABLED", "").strip() == "1"

# Cutover-граница (unix ts): события ДО неё игнорируются в reconciliation —
# без ретроактивности. 0 = не задана; фича не должна включаться в этом состоянии.
LEAD_DISTRIBUTION_SINCE_TS = int(os.getenv("LEAD_DISTRIBUTION_SINCE_TS", "0"))

# Периодическая reconciliation-проверка (страховка от сбоев API/сети).
# 0 = фоновый проход выключен (только вебхук).
LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S = int(os.getenv("LEAD_DISTRIBUTION_RECONCILE_INTERVAL_S", "120"))

# Сделка не распределяется дольше N минут → один алерт в ТГ (дедуп, снимается
# при успехе). 0 = алерт выключен.
LEAD_DISTRIBUTION_STALE_ALERT_MIN = int(os.getenv("LEAD_DISTRIBUTION_STALE_ALERT_MIN", "30"))

# Защита от гонки amgroup (создаёт сделку и асинхронно привязывает контакт):
# если у свежепрочитанной сделки ещё нет контакта — активное ожидание короткими
# проверками (не блокирующее воркер очереди) до этого бюджета, затем — алерт.
LEAD_DISTRIBUTION_CONTACT_WAIT_S = int(os.getenv("LEAD_DISTRIBUTION_CONTACT_WAIT_S", "10"))
LEAD_DISTRIBUTION_CONTACT_POLL_S = float(os.getenv("LEAD_DISTRIBUTION_CONTACT_POLL_S", "2"))

# Источник UIS (телефония): тег «Успешный звонок»/«пропущенный» ставит UIS уже
# ПОСЛЕ создания сделки (минута, иногда дольше) — на входе в точку профиля тега
# обычно ещё нет. Активное ожидание тем же приёмом, что и контакт-гонка выше,
# но с более широким бюджетом. Не дождались — назначаем как обычно (решение
# Тианы 24.08.2026: лучше отдать живому клиенту менеджера, чем держать без
# ответственного из-за одной лишь задержки вебхука UIS).
LEAD_DISTRIBUTION_UIS_TAG_WAIT_S = int(os.getenv("LEAD_DISTRIBUTION_UIS_TAG_WAIT_S", "180"))
LEAD_DISTRIBUTION_UIS_TAG_POLL_S = float(os.getenv("LEAD_DISTRIBUTION_UIS_TAG_POLL_S", "15"))

# Разница в сегодняшних счётчиках (по источнику / по общему кол-ву), после
# которой алгоритм «по нагрузке» перестаёт отдавать приоритет исходному
# кандидату — см. lead_distribution._pick_load_balanced.
LEAD_DISTRIBUTION_FAIRNESS_GAP = int(os.getenv("LEAD_DISTRIBUTION_FAIRNESS_GAP", "2"))

# Индивидуальный график сотрудника («на месте ли он сейчас») — источника этих
# данных в компании пока нет (team-panel не отслеживает присутствие/перерывы),
# поэтому единое захардкоженное окно на всех участников распределения.
LEAD_DISTRIBUTION_DEFAULT_WINDOW = (10, 19)  # (час начала МСК, час конца МСК)

# Тег успешного распределения — идемпотентность (повторный вебхук на уже
# помеченной сделке — no-op).
TAG_LEAD_DISTRIBUTION_ROUTED = "распределено автоматически"
# Тег зависшего/неудавшегося распределения — информационный, НЕ блокирует
# повторные попытки reconciliation (по образцу TAG_OFFICE_TRANSFER_ERROR).
TAG_LEAD_DISTRIBUTION_ERROR = "ошибка распределения"

# Секрет в пути для /admin/lead-distribution/* (пайплайны/источники/сотрудники —
# CRUD профилей 09.08.2026 переехал в team-panel, см. lead_distribution_profiles_client.py).
# Пусто → эндпоинты недоступны (403 на любой секрет).
LEAD_DISTRIBUTION_ADMIN_SECRET = os.getenv("LEAD_DISTRIBUTION_ADMIN_SECRET", "").strip()

# Опрос team-panel за профилями конструктора (владелец данных с 09.08.2026, см.
# lead_distribution_profiles_client.py) — write-through кэш в var/, не мастер-файл.
LEAD_DISTRIBUTION_PROFILES_POLL_INTERVAL_S = int(os.getenv("LEAD_DISTRIBUTION_PROFILES_POLL_INTERVAL_S", "30"))

# ---------------------------------------------------------------------------
# team-panel (05.08.2026) — источник правды графика сотрудников. См.
# team_panel_client.py. Мастер-флаг выключен по умолчанию: до включения
# _is_on_shift работает на плейсхолдере LEAD_DISTRIBUTION_DEFAULT_WINDOW,
# как и раньше — включать только когда график в team-panel реально заполнен
# для всех участников профилей lead_distribution.
# ---------------------------------------------------------------------------
TEAM_PANEL_BASE_URL = os.getenv("TEAM_PANEL_BASE_URL", "").strip()
# Тот же X-Ingest-Token, каким amo_fix_fields уже пользуется для Wazzup-обмена
# с team-panel — общий секрет на все /api/ingest/* team-panel.
TEAM_PANEL_INGEST_TOKEN = os.getenv("TEAM_PANEL_INGEST_TOKEN", "").strip()
TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S = int(os.getenv("TEAM_PANEL_SCHEDULE_POLL_INTERVAL_S", "300"))
TEAM_PANEL_SCHEDULE_ENABLED = os.getenv("TEAM_PANEL_SCHEDULE_ENABLED", "").strip() == "1"


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


# --- Наименование товара в накладной СДЭК -----------------------------------
# СДЭК требует, чтобы в накладной было расписано, что именно едет: что за товар и
# как называется (требование СДЭК, Катя 31.07.2026, MAG-303). Раньше в items[].name
# уходило имя сделки amo — «Заказ №18180», «Онлайн-чат Jivo — Амира», «Сделка по
# звонку с +7925…»: для СДЭК это не описание товара. Теперь имя собирается из поля
# «Состав заказа» (577313): «Аппаратный кошелёк <наименования>».
CDEK_ITEM_NAME_PREFIX = "Аппаратный кошелёк"
# У СДЭК на items[].name лимит 255 символов. Живой максимум по 699 сделкам воронки
# Офис (срез 31.07.2026) — 186 символов, но запас нужен: заказы бывают на 6 позиций.
CDEK_ITEM_NAME_MAX_LEN = 250

# Строка состава: «Tangem 2.0 (3 карты), 1 шт, 7 590.00 рублей».
# «шт» есть не всегда — у части заказов «…, 1 , 7 590.00 рублей» (24 из 699 живых
# сделок на 31.07.2026), поэтому в шаблоне оно опционально. Регэксп листа сборки
# (picking_pdf.parse_items) строже и эти 24 заказа не видит.
_COMPOSITION_ITEM_RE = re.compile(
    r"(.+?),\s*(\d+)\s*(?:шт)?\s*,\s*[\d\s]+[.,]\d+\s*руб(?:ль|ля|лей|\.?)",
    re.IGNORECASE | re.DOTALL,
)


def parse_composition_items(composition: str | None) -> list[tuple[str, int]]:
    """[(наименование, количество)] из поля «Состав заказа» (577313)."""
    if not composition:
        return []
    normalized = re.sub(r"\s+", " ", composition)
    items: list[tuple[str, int]] = []
    for m in _COMPOSITION_ITEM_RE.finditer(normalized):
        name = m.group(1).strip(" ,;\n")
        if name:
            items.append((name, int(m.group(2))))
    return items


def build_cdek_item_name(composition: str | None) -> str:
    """Наименование товара для накладной СДЭК.

    «Аппаратный кошелёк Keystone 3 Pro» — одна позиция;
    «Аппаратный кошелёк YubiKey 5 NFC, 4 шт» — количество больше одного;
    «Аппаратный кошелёк A; B; C» — несколько позиций (23% заказов);
    «Аппаратный кошелёк» — состав пуст или не распознан (имя сделки не подставляем
    никогда: клиентское ФИО и номер заказа в накладной СДЭК не нужны).
    """
    parts = [
        f"{name}, {qty} шт" if qty > 1 else name
        for name, qty in parse_composition_items(composition)
    ]
    if not parts:
        return CDEK_ITEM_NAME_PREFIX

    full = f"{CDEK_ITEM_NAME_PREFIX} {'; '.join(parts)}"
    if len(full) <= CDEK_ITEM_NAME_MAX_LEN:
        return full

    trimmed = full[: CDEK_ITEM_NAME_MAX_LEN - 1]
    cut = trimmed.rfind(";")
    if cut > len(CDEK_ITEM_NAME_PREFIX):
        trimmed = trimmed[:cut]
    return trimmed.rstrip(" ,;") + "…"


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
