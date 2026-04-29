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
FIELD_ORDER_TOTAL = 576703
FIELD_COMPOSITION = 577313

# amoCRM custom field IDs (контакт)
FIELD_PHONE = 413385
FIELD_EMAIL = 413387

# Теги
TAG_ERROR = "ошибка накладной"
TAG_PACKED = "посылка упакована"

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

# Telegram
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
_raw_chat_id = os.getenv("TG_ALLOWED_CHAT_ID", "")
TG_ALLOWED_CHAT_ID: int | None = int(_raw_chat_id) if _raw_chat_id else None


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
