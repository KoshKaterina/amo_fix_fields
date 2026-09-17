"""Приём контактных форм сайта → сделки в amoCRM.

Один роут POST /site_form (заголовок X-Api-Key), два контракта.

Схема 1 (13.09.2026). WPCode-сниппет на sunscrypt.ru, тестовые копии старых форм CF7:
    {"form": "<slug>", "page_url": "https://...", "fields": {"your-name": "...", ...}}
    Обработка фоном сразу, ответ 200 всегда. Живёт, пока сниппет не снят.

Схема 2 (14.09.2026). Плагин sun-contact-forms, формы по ТЗ «Новые контактные формы»
(обратный звонок или вопрос, запись на консультацию). Отдельный тип «Нет в наличии»
принимается только при дополнительном включателе и явной карте формы:
    {"schema": 2, "form": "test-callback", "form_type": "callback" | "consultation" | "unavailable",
     "submission_id": "<uuid одной попытки человека>", "client_ip": "...",
     "contact": {"name": "...", "phone": "+7...", "telegram": "@..."}, "comment": "...",
     "context": {"title", "entry", "page_url", "page_title", "referrer", "utm": {...},
                 "product": {id, name, sku, url} | null,
                 "service": {id, name, url, verified} | null,
                 "format": {"code": "online" | "showroom", "label"} | null}}
    Проверка → запись в очередь site_form_store → ответ 200 {"ok": true, "status":
    "accepted" | "duplicate"}. Сделку создаёт фоновый обработчик с повторами: amo недоступна -
    заявка ждёт в очереди, а не теряется. Ответ не 200 (422 проверка, 429 частота, 503 очередь) -
    сайт показывает человеку «Не получилось отправить заявку» и сохраняет введённое.
    Повтор той же попытки (тот же submission_id) вторую сделку не создаёт.
    Перед create pending-строка атомарно захватывается worker. Это исключает
    одновременный create двумя worker. При неопределённом исходе внешнего create
    строка становится uncertain (или остаётся processing при сбое БД) и не
    повторяется автоматически: нужна ручная сверка по submission_id. После
    внезапной смерти процесса processing также требует сверки; crash-gap не закрыт.

Сделка идёт через «Неразобранное» воронки и сразу принимается в этап карты: только этот путь
amo даёт заполнить метаданные формы. Нативная графа «Источник» токен-интеграции закрыта
(«Integration needs widget», боем 13.09.2026) - имя формы едет тегом и в названии сделки.

Env:
    SITE_FORM_ENABLED=1     - включатель, по умолчанию ВЫКЛЮЧЕНО (деплой безопасен)
    SITE_FORM_SECRET=...    - сверяется с заголовком X-Api-Key
    SITE_FORM_MAP='{"test-callback": {"source": "Форма: обратный звонок",
                    "pipeline_id": 8642414, "status_id": 70070982, "tags": ["тест"]}}'
        slug формы → куда класть. status_id не задан → заявка остаётся в «Неразобранном»
        воронки pipeline_id. Формы не из карты: схема 1 - пропуск, схема 2 - ответ 422.
        Для «Нет в наличии» запись обязана иметь "form_type": "unavailable" и свой
        slug/source. Без неё новый тип не принимается; схему 1 для этого slug не используем.
    SITE_FORM_UNAVAILABLE_ENABLED=1 - отдельный включатель «Нет в наличии», по умолчанию ВЫКЛЮЧЕН
    SITE_FORM_RATE_PER_MINUTE=30        - схема 1: заявок с одного адреса в минуту
    SITE_FORM_CLIENT_RATE_PER_MINUTE=5  - схема 2: заявок от одного посетителя в минуту
    SITE_FORM_DB_PATH=/app/var/site_form.sqlite3 - очередь схемы 2
    SITE_FORM_KEEP_DAYS=7               - сколько хранить законченные и проваленные строки
    SITE_FORM_WORKER_INTERVAL_S=15      - как часто фоновый обработчик смотрит очередь
    SITE_FORM_TELEGRAM_FIELD_ID=0       - поле контакта «Telegram» для новых контактов
                                          (0 - ник только в примечании сделки)
"""

import asyncio
import hmac
import json
import logging
import os
import re
import time

import api
import site_form_store as store

logger = logging.getLogger("uvicorn.error")

SITE_FORM_ENABLED = os.getenv("SITE_FORM_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")
SITE_FORM_UNAVAILABLE_ENABLED = os.getenv("SITE_FORM_UNAVAILABLE_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")
SITE_FORM_SECRET = os.getenv("SITE_FORM_SECRET", "").strip()


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        logger.error("%s: не целое число (%r) — беру дефолт %s", name, raw, default)
        return default


SITE_FORM_RATE_PER_MINUTE = _env_int("SITE_FORM_RATE_PER_MINUTE", 30)
SITE_FORM_CLIENT_RATE_PER_MINUTE = _env_int("SITE_FORM_CLIENT_RATE_PER_MINUTE", 5)
SITE_FORM_KEEP_DAYS = _env_int("SITE_FORM_KEEP_DAYS", 7)
SITE_FORM_WORKER_INTERVAL_S = _env_int("SITE_FORM_WORKER_INTERVAL_S", 15)
SITE_FORM_TELEGRAM_FIELD_ID = _env_int("SITE_FORM_TELEGRAM_FIELD_ID", 0)

# Паузы между попытками создать сделку; после последней заявка «failed» и алерт в технический чат.
# В сумме около двух часов - дольше amo у нас не лежала.
RETRY_DELAYS_S = (30, 60, 120, 300, 600, 1800, 3600)

# Обрезка недоверенного ввода перед отправкой в amo (иначе 400).
MAX_NAME_LEN = 200
MAX_NOTE_LEN = 5000
# Схема 1: антидубль повторной отправки той же заявки (даблклик, ретрай WP).
SEEN_TTL_SECONDS = _env_int("SITE_FORM_SEEN_TTL_SECONDS", 120)

# Схема 1: ключи полей CF7, из которых достаём контакт (первый непустой).
NAME_KEYS = ("your-name", "name", "fio", "imya")
PHONE_KEYS = ("your-tel", "your-phone", "tel", "phone", "telefon")
EMAIL_KEYS = ("your-email", "email")

# Схема 2: справочники формы. Тексты - как их видит человек на сайте.
FORM_TYPES = {
    "callback": "Обратный звонок или вопрос",
    "consultation": "Запись на консультацию",
    "unavailable": "Нет в наличии",
}
FORMATS = {
    "online": "Онлайн",
    "showroom": "В шоуруме в Москве",
}
UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
_SUBMISSION_RE = re.compile(r"^[a-f0-9-]{16,64}$")


def _load_map() -> dict:
    """SITE_FORM_MAP: slug → {source, pipeline_id, status_id?, tags?}. Битая
    запись выбрасывается с логом, битый JSON не роняет импорт (весь сервер)."""
    raw = os.getenv("SITE_FORM_MAP", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except ValueError:
        logger.error("SITE_FORM_MAP: битый JSON — карта пуста, формы игнорируются")
        return {}
    if not isinstance(data, dict):
        logger.error("SITE_FORM_MAP: ожидался объект slug->настройки — карта пуста")
        return {}
    out = {}
    for slug, cfg in data.items():
        if not isinstance(cfg, dict):
            logger.error("SITE_FORM_MAP[%s]: не объект — пропуск", slug)
            continue
        source = str(cfg.get("source") or "").strip()
        pipeline_id = cfg.get("pipeline_id")
        if not source or not isinstance(pipeline_id, int):
            logger.error("SITE_FORM_MAP[%s]: нужны source и pipeline_id (int) — пропуск", slug)
            continue
        status_id = cfg.get("status_id")
        form_type = cfg.get("form_type")
        if form_type is not None and (not isinstance(form_type, str) or form_type not in FORM_TYPES):
            logger.error("SITE_FORM_MAP[%s]: неизвестный form_type — пропуск", slug)
            continue
        entry = {
            "source": source,
            "pipeline_id": pipeline_id,
            "status_id": status_id if isinstance(status_id, int) else None,
            "tags": [str(t) for t in (cfg.get("tags") or []) if str(t).strip()],
            "form_type": form_type,
        }
        out[str(slug)] = entry
    return out


FORM_MAP = _load_map()

_rate: dict[str, list] = {}
_client_rate: dict[str, list] = {}
_seen: dict[str, float] = {}

_wake: asyncio.Event | None = None
_worker_task: asyncio.Task | None = None


def is_enabled() -> bool:
    return SITE_FORM_ENABLED and bool(SITE_FORM_SECRET) and bool(FORM_MAP)


def secret_ok(key: str) -> bool:
    if not SITE_FORM_SECRET or not key:
        return False
    return hmac.compare_digest(SITE_FORM_SECRET, key)


def _allow(buckets: dict, key: str, limit: int) -> bool:
    """Скользящее окно на минуту. Пустой ключ - общая корзина."""
    now = time.monotonic()
    bucket = buckets.setdefault(key or "-", [])
    bucket[:] = [t for t in bucket if now - t < 60]
    if len(bucket) >= limit:
        return False
    bucket.append(now)
    return True


def allow_ip(ip: str) -> bool:
    """Схема 1: предел по адресу запроса (это адрес сервера WP, а не посетителя)."""
    return _allow(_rate, ip, SITE_FORM_RATE_PER_MINUTE)


def allow_client(client_ip: str) -> bool:
    """Схема 2: предел по адресу посетителя, который сайт передаёт в заявке."""
    return _allow(_client_rate, client_ip, SITE_FORM_CLIENT_RATE_PER_MINUTE)


def _seen_recently(key: str) -> bool:
    now = time.monotonic()
    for k, t in list(_seen.items()):
        if now - t > SEEN_TTL_SECONDS:
            _seen.pop(k, None)
    if key in _seen:
        return True
    _seen[key] = now
    return False


def _pick(fields: dict, keys: tuple) -> str:
    for k in keys:
        v = str(fields.get(k) or "").strip()
        if v:
            return v
    return ""


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("8"):
        digits = "7" + digits[1:]
    return f"+{digits}" if digits else ""


def normalize_phone_v2(raw: str) -> str:
    """Те же правила, что у плагина на сайте. '' - номер неоднозначный, не угадываем.
    Без «+» принимаем только российские записи: 8/7 и 10 цифр или 10 цифр с 9 в начале."""
    raw = (raw or "").strip()
    if not raw or re.search(r"[^0-9+()\s.\-]", raw):
        return ""
    plus = raw.startswith("+")
    digits = re.sub(r"\D", "", raw)
    n = len(digits)
    if not plus:
        if n == 11 and digits[0] in "78":
            return "+7" + digits[1:]
        if n == 10 and digits[0] == "9":
            return "+7" + digits
        return ""
    if n < 10 or n > 15 or digits[0] == "0":
        return ""
    if digits[0] == "7" and n != 11:
        return ""
    if digits.startswith("89"):  # «+8 9…» - перепутали код страны
        return ""
    return "+" + digits


def _fallback_phone(fields: dict) -> str:
    """Поле телефона названо нестандартно (CF7 позволяет что угодно) — ищем по
    значению: первое поле, где после чистки остаётся 10-15 цифр."""
    for v in fields.values():
        p = _normalize_phone(str(v or ""))
        if 11 <= len(p) <= 16:  # "+" и 10-15 цифр
            return p
    return ""


def _fallback_email(fields: dict) -> str:
    for v in fields.values():
        v = str(v or "").strip()
        if re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", v):
            return v
    return ""


def external_id(slug: str) -> str:
    return f"site_form_{slug}"


def _note_text(slug: str, fields: dict, page_url: str) -> str:
    lines = [f"Заявка с формы сайта «{slug}»"]
    if page_url:
        lines.append(f"Страница: {page_url}")
    for k, v in fields.items():
        v = str(v or "").strip()
        if v:
            lines.append(f"{k}: {v}")
    return "\n".join(lines)[:MAX_NOTE_LEN]


# ---------------------------------------------------------------------------
# Схема 1
# ---------------------------------------------------------------------------

async def process(payload: dict, ip: str = "") -> int | None:
    """Схема 1: одна заявка → сделка. Возвращает id сделки или None (не создана/пропуск)."""
    slug = str(payload.get("form") or "").strip()
    cfg = FORM_MAP.get(slug)
    if not cfg:
        logger.warning("site_form: форма %r не в карте — пропуск", slug)
        return None
    if cfg.get("form_type") == "unavailable" or payload.get("form_type") == "unavailable":
        # Схема 1 не имеет обязательных товара и submission_id: не даём ей
        # превратить новый тип заявки в обычную форму без очереди и антидубля.
        logger.warning("site_form[%s]: форма «Нет в наличии» требует схему 2", slug)
        return None
    fields = payload.get("fields")
    if not isinstance(fields, dict):
        logger.warning("site_form[%s]: нет блока fields — пропуск", slug)
        return None

    name = _pick(fields, NAME_KEYS)[:MAX_NAME_LEN]
    phone = _normalize_phone(_pick(fields, PHONE_KEYS)) or _fallback_phone(fields)
    email = _pick(fields, EMAIL_KEYS) or _fallback_email(fields)
    if not phone and not email:
        logger.warning("site_form[%s]: ни телефона, ни почты — пропуск (спам-отсев)", slug)
        return None
    if _seen_recently(f"{slug}|{phone or email}"):
        logger.info("site_form[%s]: повтор %s за %sс — пропуск", slug, phone or email, SEEN_TTL_SECONDS)
        return None

    page_url = str(payload.get("page_url") or "").strip()
    source = cfg["source"]

    contact_id = None
    if phone:
        contact_id = await api.find_contact_id(phone)
    if not contact_id and email:
        contact_id = await api.find_contact_id(email)
    if contact_id:
        contact = {"id": int(contact_id)}
    else:
        cf = []
        if phone:
            cf.append({"field_code": "PHONE", "values": [{"value": phone, "enum_code": "WORK"}]})
        if email:
            cf.append({"field_code": "EMAIL", "values": [{"value": email, "enum_code": "WORK"}]})
        contact = {"name": name or phone or email}
        if cf:
            contact["custom_fields_values"] = cf

    # Имя источника — тегом всегда: /api/v4/sources для нашей интеграции закрыт
    # («Integration needs widget», проверено боем 13.09.2026), нативная графа
    # «Источник» недоступна. Менеджер видит форму тегом, в названии сделки и в
    # метаданных заявки (form_name).
    res = await api.create_unsorted_lead_ex(
        lead_name=f"{source}: {name or phone or email}",
        pipeline_id=cfg["pipeline_id"],
        contact=contact,
        source_uid=external_id(slug),
        page_url=page_url,
        created_ts=int(time.time()),
        source_name=source,
        form_id=slug,
        lead_tags=[source] + cfg["tags"],
        ip=ip,
    )
    lead_id = res.get("lead_id")
    if not lead_id:
        logger.error("site_form[%s]: заявка не создана", slug)
        return None

    if cfg["status_id"] and res.get("uid"):
        accepted = await api.accept_unsorted(res["uid"], cfg["status_id"])
        if accepted:
            lead_id = accepted
        else:
            logger.error("site_form[%s]: accept в этап %s не прошёл, заявка %s осталась в Неразобранном",
                         slug, cfg["status_id"], lead_id)

    # Дожим тегов обычным PATCH: unsorted/forms существующие теги по имени не
    # линкует (создаёт только новые), поэтому «Тест» через него не встаёт.
    await api.set_lead_tags(lead_id, [source] + cfg["tags"])
    await api.add_note_to_lead(lead_id, _note_text(slug, fields, page_url))
    logger.info("site_form[%s]: сделка %s, источник %s", slug, lead_id, source)
    return lead_id


async def _safe_process(payload: dict, ip: str) -> None:
    try:
        await process(payload, ip)
    except Exception:
        logger.exception("site_form: ошибка обработки заявки")


def handle_bg(payload: dict, ip: str = "") -> None:
    """Схема 1: обработка фоном, вебхук отвечает 200 сразу, WP не ждёт amo."""
    asyncio.get_running_loop().create_task(_safe_process(payload, ip))


# ---------------------------------------------------------------------------
# Схема 2: проверка и очередь
# ---------------------------------------------------------------------------

class PayloadError(ValueError):
    """Заявка схемы 2 не прошла проверку. Текст - короткий код без персональных данных."""


def _s(value, limit: int) -> str:
    if value is None or isinstance(value, (dict, list, tuple)):
        return ""
    return str(value).strip()[:limit]


def _url(value, limit: int = 1000) -> str:
    url = _s(value, limit + 1)
    if len(url) > limit or not re.match(r"^https?://[^\s]+$", url):
        return ""
    return url


def _int_or_none(value) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _clean_ref(value, with_sku: bool = False, with_verified: bool = False) -> dict | None:
    """Товар или консультация из контекста страницы. Пустышка → None."""
    if not isinstance(value, dict):
        return None
    ref = {
        "id": _int_or_none(value.get("id")),
        "name": _s(value.get("name"), 200),
        "url": _url(value.get("url")),
    }
    if with_sku:
        ref["sku"] = _s(value.get("sku"), 64)
    if with_verified:
        ref["verified"] = bool(value.get("verified"))
    if not ref["name"] and ref["id"] is None:
        return None
    return ref


def clean_v2(payload: dict) -> dict:
    """Проверка и нормализация заявки схемы 2. Сайт уже проверил поля, но на слово ему не верим."""
    slug = _s(payload.get("form"), 64)
    if slug not in FORM_MAP:
        raise PayloadError("unknown-form")
    submission_id = _s(payload.get("submission_id"), 64).lower()
    if not _SUBMISSION_RE.match(submission_id):
        raise PayloadError("bad-submission-id")
    form_type = _s(payload.get("form_type"), 32)
    if form_type not in FORM_TYPES:
        raise PayloadError("bad-form-type")
    declared_type = FORM_MAP[slug].get("form_type")
    if form_type == "unavailable" and not SITE_FORM_UNAVAILABLE_ENABLED:
        raise PayloadError("unavailable-disabled")
    if (form_type == "unavailable" and declared_type != "unavailable") or (declared_type and declared_type != form_type):
        raise PayloadError("form-type-mismatch")

    contact_in = payload.get("contact") if isinstance(payload.get("contact"), dict) else {}
    name = _s(contact_in.get("name"), 80)
    if not name:
        raise PayloadError("no-name")
    phone = normalize_phone_v2(_s(contact_in.get("phone"), 32))
    if not phone:
        raise PayloadError("bad-phone")

    ctx_in = payload.get("context") if isinstance(payload.get("context"), dict) else {}
    fmt_in = ctx_in.get("format") if isinstance(ctx_in.get("format"), dict) else {}
    fmt_code = _s(fmt_in.get("code"), 16)
    if form_type == "consultation" and fmt_code not in FORMATS:
        raise PayloadError("no-format")

    utm_in = ctx_in.get("utm") if isinstance(ctx_in.get("utm"), dict) else {}
    utm = {}
    for key in UTM_KEYS:
        value = _s(utm_in.get(key), 200)
        if value:
            utm[key] = value

    page_url = _url(ctx_in.get("page_url")) or _url(payload.get("page_url"))
    product = _clean_ref(ctx_in.get("product"), with_sku=True)
    if form_type == "unavailable":
        # Не подменяем конкретный товар общим вопросом: идентификатор, название
        # и карточка товара нужны менеджеру и для проверки источника обращения.
        if not product or not product["id"] or product["id"] <= 0 or not product["name"] or not product["url"]:
            raise PayloadError("no-product")
        if not _s(ctx_in.get("entry"), 64):
            raise PayloadError("no-entry")
    return {
        "schema": 2,
        "form": slug,
        "form_type": form_type,
        "submission_id": submission_id,
        "client_ip": _s(payload.get("client_ip"), 64),
        "contact": {
            "name": name,
            "phone": phone,
            "telegram": _s(contact_in.get("telegram"), 64),
        },
        "comment": _s(payload.get("comment"), 2000),
        "context": {
            "title": _s(ctx_in.get("title"), 200),
            "entry": _s(ctx_in.get("entry"), 64),
            "page_url": page_url,
            "page_title": _s(ctx_in.get("page_title"), 300),
            "referrer": _url(ctx_in.get("referrer")),
            "utm": utm,
            "product": product,
            "service": _clean_ref(ctx_in.get("service"), with_verified=True) if form_type == "consultation" else None,
            "format": {"code": fmt_code, "label": FORMATS[fmt_code]} if form_type != "unavailable" and fmt_code in FORMATS else None,
        },
    }


async def accept_v2(payload: dict) -> tuple[int, dict]:
    """Схема 2: проверка и постановка в очередь. Возвращает (HTTP-код, тело ответа)."""
    try:
        clean = clean_v2(payload)
    except PayloadError as exc:
        logger.warning("site_form[v2]: заявка отклонена: %s (форма %r)", exc, _s(payload.get("form"), 64))
        return 422, {"ok": False, "error": str(exc)}
    if not allow_client(clean["client_ip"]):
        logger.warning("site_form[%s]: превышена частота заявок от одного посетителя", clean["form"])
        return 429, {"ok": False, "error": "rate-limit"}
    short_id = clean["submission_id"][:8]
    try:
        inserted, row = await asyncio.to_thread(
            store.insert_pending, clean["submission_id"], clean["form"], json.dumps(clean, ensure_ascii=False),
        )
    except Exception:
        logger.exception("site_form[%s]: очередь недоступна, заявка %s не принята", clean["form"], short_id)
        return 503, {"ok": False, "error": "store"}
    if not inserted:
        logger.info("site_form[%s]: повтор попытки %s (статус %s) — вторую сделку не создаём",
                    clean["form"], short_id, row.get("status"))
        return 200, {"ok": True, "status": "duplicate"}
    logger.info("site_form[%s]: заявка %s принята в очередь", clean["form"], short_id)
    _kick()
    return 200, {"ok": True, "status": "accepted"}


# ---------------------------------------------------------------------------
# Схема 2: сделка
# ---------------------------------------------------------------------------

def _page_title(title: str) -> str:
    """Название страницы без хвоста сайта: «Keystone 3 Pro - Sunscrypt» → «Keystone 3 Pro»."""
    return re.sub(r"\s+[-–—|]\s+Sunscrypt\s*$", "", title or "").strip()


def note_text_v2(p: dict, source: str) -> str:
    """Примечание к сделке - три блока через пустую строку.

    1. Что за заявка: тип, страница, окно, консультация с форматом или товар, вопрос. Менеджер с
       первого взгляда видит, с какой страницы, в какую форму и с чем пришёл человек.
    2. Контакт: имя, телефон, Telegram.
    3. Техническое: источник в amo, адреса страницы и товара, кнопка, UTM, откуда пришёл, номер заявки.

    Жирного в примечаниях amo нет: текст идёт без разметки, HTML amo экранирует («&» в API отдаётся
    как «&amp;»). Поэтому заголовки блоков - капсом и со значком. Значки только из базовой плоскости
    Unicode (✉️ ☎️ ⚙️): четырёхбайтовые эмодзи вроде 📩 и 👤 amo молча вырезает (сделка 36555685,
    14.09.2026). Пустые необязательные поля не выводятся.
    """
    ctx = p["context"]
    contact = p["contact"]
    consultation = p["form_type"] == "consultation"
    service = ctx["service"]
    product = ctx["product"]
    same_item = bool(service and product and service.get("id") and service.get("id") == product.get("id"))

    head = [f"✉️ ЗАЯВКА С САЙТА: {FORM_TYPES[p['form_type']].upper()}"]
    if p["form_type"] == "unavailable":
        # Даже если длинное примечание обрежется до MAX_NOTE_LEN, полный ключ
        # остаётся в начале для сопоставления с локальной очередью.
        head.append(f"ID заявки: {p['submission_id']}")
    page_title = _page_title(ctx["page_title"])
    if page_title:
        head.append(f"Страница: {page_title}")
    if ctx["title"]:
        head.append(f"Форма: {ctx['title']}")
    if consultation and service:
        head.append(f"Консультация: {service['name'] or 'без названия'}")
    if consultation and ctx["format"]:
        head.append(f"Формат: {ctx['format']['label']}")
    if product and not same_item:
        line = f"Товар: {product['name'] or 'без названия'}"
        if product.get("sku"):
            line += f", артикул {product['sku']}"
        head.append(line)
    if p["comment"]:
        head.append(f"{'Запрос' if consultation or p['form_type'] == 'unavailable' else 'Вопрос'}: {p['comment']}")

    person = ["☎️ КОНТАКТ", f"Имя: {contact['name']}", f"Телефон: {contact['phone']}"]
    if contact["telegram"]:
        person.append(f"Telegram: {contact['telegram']}")

    tech = ["⚙️ ТЕХНИЧЕСКОЕ", f"Источник: {source}"]
    if ctx["page_url"]:
        tech.append(f"Адрес страницы: {ctx['page_url']}")
    if consultation and service and service["url"]:
        tech.append(f"Ссылка на консультацию: {service['url']}")
    if consultation and service and not service.get("verified"):
        # Кнопка передала только название (не номер страницы) - сайт его не сверил.
        tech.append("Консультация не сверена с сайтом: кнопка передала только название")
    if product and not same_item and product["url"]:
        tech.append(f"Ссылка на товар: {product['url']}")
    if ctx["entry"]:
        tech.append(f"Кнопка на сайте: {ctx['entry']}")
    if ctx["utm"]:
        tech.append("UTM: " + ", ".join(f"{k}={v}" for k, v in ctx["utm"].items()))
    if ctx["referrer"]:
        tech.append(f"Пришёл с: {ctx['referrer']}")
    tech.append(f"Номер заявки: {p['submission_id'][:8]}")

    return "\n\n".join("\n".join(block) for block in (head, person, tech))[:MAX_NOTE_LEN]


async def _create_lead_v2(p: dict, cfg: dict, attempt: dict) -> tuple[int | None, str | None]:
    """Заявка в «Неразобранное». Контакт ищем по телефону по действующему правилу (как Jivo
    и схема 1); новый контакт - имя и телефон, Telegram - если задано поле."""
    contact_in = p["contact"]
    source = cfg["source"]
    lead_name = f"{source}: {contact_in['name']}"
    if p["form_type"] == "unavailable":
        # Уникальный ключ должен попасть именно в первичный POST: после timeout
        # примечание может не существовать, а source_uid/form_id остаются slug.
        suffix = f" [ID заявки: {p['submission_id']}]"
        lead_name = f"{lead_name[:MAX_NAME_LEN - len(suffix)]}{suffix}"
    contact_id = await api.find_contact_id(contact_in["phone"])
    if contact_id:
        contact = {"id": int(contact_id)}
    else:
        fields = [{"field_code": "PHONE", "values": [{"value": contact_in["phone"], "enum_code": "WORK"}]}]
        if contact_in["telegram"] and SITE_FORM_TELEGRAM_FIELD_ID:
            fields.append({"field_id": SITE_FORM_TELEGRAM_FIELD_ID, "values": [{"value": contact_in["telegram"]}]})
        contact = {"name": contact_in["name"], "custom_fields_values": fields}
    # С этого момента timeout/отмена не доказывают, что amo не создала сделку.
    attempt["remote_create_started"] = True
    create_options = {"max_attempts": 1} if p["form_type"] == "unavailable" else {}
    res = await api.create_unsorted_lead_ex(
        lead_name=lead_name,
        pipeline_id=cfg["pipeline_id"],
        contact=contact,
        source_uid=external_id(p["form"]),
        page_url=p["context"]["page_url"],
        created_ts=int(time.time()),
        source_name=source,
        form_id=p["form"],
        lead_tags=[source] + cfg["tags"],
        ip=p["client_ip"] or "0.0.0.0",
        **create_options,
    )
    return res.get("lead_id"), res.get("uid")


async def _finish_lead_v2(p: dict, cfg: dict, row: dict, attempt: dict) -> int:
    """Доводка сделки; строгий one-shot note только для unavailable."""
    lead_id = int(row["lead_id"])
    source = cfg["source"]
    if cfg["status_id"] and row.get("unsorted_uid"):
        accepted = await api.accept_unsorted(row["unsorted_uid"], cfg["status_id"])
        if accepted:
            lead_id = int(accepted)
        else:
            logger.error("site_form[%s]: accept в этап %s не прошёл, сделка %s осталась в Неразобранном",
                         p["form"], cfg["status_id"], lead_id)
    await api.set_lead_tags(lead_id, [source] + cfg["tags"])
    if p["context"]["utm"]:
        if not await api.set_lead_utm(lead_id, p["context"]["utm"]):
            logger.warning("site_form[%s]: UTM в поля сделки %s не записались, остались в примечании",
                           p["form"], lead_id)
    note = note_text_v2(p, source)
    if p["form_type"] == "unavailable":
        # Если ответ на добавление примечания потерян, повтор может его удвоить.
        attempt["note_started"] = True
        if not await api.add_note_to_lead(lead_id, note, max_attempts=1):
            raise RuntimeError("amo-note-outcome-unknown")
    else:
        # Прежняя политика callback/consultation: API сам повторяет POST,
        # отрицательный ответ note не препятствует завершению заявки.
        await api.add_note_to_lead(lead_id, note)
    return lead_id


async def _retry_or_fail(row: dict, error: str) -> None:
    attempts = int(row.get("attempts") or 0) + 1
    short_id = row["submission_id"][:8]
    if attempts > len(RETRY_DELAYS_S):
        await asyncio.to_thread(store.mark_failed, row["submission_id"], attempts, error)
        logger.error("site_form[%s]: заявка %s не доставлена за %s попыток (%s) — нужен ручной разбор",
                     row["form"], short_id, attempts, error)
        await _alert_failed(row, error, attempts)
        return
    delay = RETRY_DELAYS_S[attempts - 1]
    await asyncio.to_thread(store.mark_retry, row["submission_id"], attempts, time.time() + delay, error)
    logger.warning("site_form[%s]: заявка %s не доставлена (%s), попытка %s, следующая через %s с",
                   row["form"], short_id, error, attempts, delay)


async def _hold_unknown_external_write(row: dict, reason: str) -> None:
    """Не повторяем внешнюю запись при неизвестном ответе; claim остаётся вне due."""
    try:
        if await asyncio.to_thread(store.mark_uncertain, row["submission_id"], reason):
            logger.error("site_form[%s]: исход внешней записи заявки %s неизвестен — нужна сверка вручную (%s)",
                         row["form"], row["submission_id"][:8], reason)
    except Exception:
        logger.exception("site_form[%s]: не удалось записать uncertain для заявки %s",
                         row["form"], row["submission_id"][:8])


async def _alert_failed(row: dict, error: str, attempts: int) -> None:
    """Технический чат: без имени и телефона - они лежат в очереди на сервере."""
    import alerts
    import telegram_bot

    source = (FORM_MAP.get(row["form"]) or {}).get("source") or row["form"]
    text = (
        "Заявка с сайта не создалась в amoCRM\n"
        f"Форма: {source}\n"
        f"Попыток: {attempts}, последняя ошибка: {error}\n"
        f"Номер заявки: {row['submission_id'][:8]}\n"
        f"Данные заявки хранятся на сервере в очереди форм {SITE_FORM_KEEP_DAYS} дн."
    )
    try:
        decision = alerts.decide("site_form_failed", legacy_text=text, values={})
        if decision is not None:
            await telegram_bot.send_alert(decision.text, **decision.send_kwargs())
    except Exception:
        logger.exception("site_form: алерт о недоставленной заявке не отправлен")


async def deliver(row: dict, attempt: dict) -> bool | None:
    """Одна строка очереди → сделка; fail-closed create только для unavailable."""
    submission_id = row["submission_id"]
    try:
        payload = json.loads(row.get("payload") or "")
    except ValueError:
        await asyncio.to_thread(store.mark_failed, submission_id, int(row.get("attempts") or 0), "broken-payload")
        logger.error("site_form[%s]: заявка %s испорчена в очереди", row["form"], submission_id[:8])
        return
    is_unavailable = payload.get("form_type") == "unavailable"
    attempt["is_unavailable"] = is_unavailable
    if is_unavailable and not SITE_FORM_UNAVAILABLE_ENABLED:
        # Убираем из due, сохраняя строку/попытки до восстановления gate.
        await asyncio.to_thread(store.mark_held, submission_id, "unavailable-disabled")
        logger.info("site_form[%s]: заявка %s удержана (тип выключен)", row["form"], submission_id[:8])
        return False
    cfg = FORM_MAP.get(row["form"])
    if not cfg:
        if payload.get("form_type") == "unavailable":
            await asyncio.to_thread(store.mark_held, submission_id, "form-not-in-map")
            logger.error("site_form[%s]: заявка %s удержана (форма отсутствует в карте)",
                         row["form"], submission_id[:8])
            return False
        # Существующие формы сохраняют прежний порядок повторов.
        await _retry_or_fail(row, "form-not-in-map")
        return
    declared_type = cfg.get("form_type")
    if (payload.get("form_type") == "unavailable" and declared_type != "unavailable") or (
        declared_type and declared_type != payload.get("form_type")
    ):
        if payload.get("form_type") == "unavailable":
            await asyncio.to_thread(store.mark_held, submission_id, "form-type-mismatch")
            logger.error("site_form[%s]: заявка %s удержана (тип формы изменился в карте)",
                         row["form"], submission_id[:8])
            return False
        await _retry_or_fail(row, "form-type-mismatch")
        return
    try:
        if row["status"] == "pending":
            lead_id, uid = await _create_lead_v2(payload, cfg, attempt)
            if not lead_id:
                if is_unavailable:
                    await _hold_unknown_external_write(row, "amo-create-outcome-unknown")
                else:
                    await _retry_or_fail(row, "amo-create-failed")
                return
            # Сохраняем lead_id и владение доводкой одним UPDATE: другой worker
            # не увидит промежуточную created до примечания.
            if not await asyncio.to_thread(store.mark_created, submission_id, lead_id, uid, claim_finish=True):
                raise RuntimeError("mark-created-claim-lost")
            row = {**row, "status": "created", "lead_id": lead_id, "unsorted_uid": uid}
        lead_id = await _finish_lead_v2(payload, cfg, row, attempt)
        if not await asyncio.to_thread(store.mark_done, submission_id, lead_id, require_finishing=True):
            raise RuntimeError("mark-done-claim-lost")
        logger.info("site_form[%s]: заявка %s → сделка %s", row["form"], submission_id[:8], lead_id)
    except Exception as exc:
        logger.exception("site_form[%s]: заявка %s — сбой доставки", row["form"], submission_id[:8])
        if is_unavailable and row["status"] == "pending" and attempt["remote_create_started"]:
            await _hold_unknown_external_write(row, f"amo-create-outcome-unknown:{type(exc).__name__}")
        elif is_unavailable and row["status"] == "created" and attempt["note_started"]:
            await _hold_unknown_external_write(row, f"amo-note-outcome-unknown:{type(exc).__name__}")
        else:
            await _retry_or_fail(row, type(exc).__name__)


async def run_due(now: float | None = None) -> int:
    if SITE_FORM_UNAVAILABLE_ENABLED:
        available_forms = [slug for slug, cfg in FORM_MAP.items()
                           if cfg.get("form_type") == "unavailable"]
        released = await asyncio.to_thread(store.release_held, available_forms, now)
        if released:
            logger.info("site_form: удержанных заявок возвращено в очередь: %s", released)
    processed = 0
    while True:
        rows = await asyncio.to_thread(store.due, now)
        if not rows:
            return processed
        held = 0
        handled = 0
        for row in rows:
            if row["status"] == "pending":
                # due() только читает: другой worker мог выбрать ту же строку.
                # Условный UPDATE в SQLite оставит право на create лишь одному.
                # Синхронный короткий claim не может продолжить работу в thread
                # после отмены ожидающей корутины.
                claimed = store.claim_pending(row["submission_id"], now)
                if not claimed:
                    continue
            elif row["status"] == "created":
                if not store.claim_created(row["submission_id"], now):
                    continue
            handled += 1
            attempt = {"remote_create_started": False, "note_started": False, "is_unavailable": False}
            try:
                result = await deliver(row, attempt)
            except asyncio.CancelledError:
                is_unavailable = attempt["is_unavailable"]
                if row["status"] == "pending":
                    try:
                        if is_unavailable and attempt["remote_create_started"]:
                            store.mark_uncertain(row["submission_id"], "cancelled-after-create-start")
                            logger.error("site_form[%s]: create заявки %s прерван — нужна сверка вручную",
                                         row["form"], row["submission_id"][:8])
                        else:
                            store.release_claim(row["submission_id"])
                            # mark_created мог уже сохранить lead_id и перейти в
                            # finishing, пока ожидающий to_thread был отменён.
                            if not is_unavailable:
                                store.release_created_claim(row["submission_id"])
                    except Exception:
                        # processing не находится в due; безопаснее ручная
                        # сверка, чем второй create после ошибки БД.
                        logger.exception("site_form[%s]: не удалось завершить claim при отмене заявки %s",
                                         row["form"], row["submission_id"][:8])
                elif row["status"] == "created":
                    try:
                        if is_unavailable and attempt["note_started"]:
                            store.mark_uncertain(row["submission_id"], "cancelled-after-note-start")
                            logger.error("site_form[%s]: примечание заявки %s прервано — нужна сверка вручную",
                                         row["form"], row["submission_id"][:8])
                        else:
                            store.release_created_claim(row["submission_id"])
                    except Exception:
                        logger.exception("site_form[%s]: не удалось завершить доводку при отмене заявки %s",
                                         row["form"], row["submission_id"][:8])
                raise
            if result is False:
                held += 1
        processed += handled
        if handled == 0:
            return processed
        # Если всю страницу заняли удержанные строки, сразу берём следующую:
        # callback/consultation не ждут очередного тика worker.
        if held != handled:
            return processed


def _kick() -> None:
    if _wake is not None:
        _wake.set()


async def _worker() -> None:
    last_purge = 0.0
    while True:
        _wake.clear()
        try:
            await run_due()
            if time.time() - last_purge > 3600:
                removed = await asyncio.to_thread(store.purge, SITE_FORM_KEEP_DAYS)
                if removed:
                    logger.info("site_form: из очереди удалено старых строк: %s", removed)
                last_purge = time.time()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("site_form: сбой фонового обработчика")
        try:
            await asyncio.wait_for(_wake.wait(), timeout=SITE_FORM_WORKER_INTERVAL_S)
        except asyncio.TimeoutError:
            pass


async def ensure_sources() -> None:
    """Регистрирует недостающие источники карты на нашей интеграции. Ошибка не
    роняет старт: без источника amo подставит источник интеграции по умолчанию."""
    wanted = {external_id(slug): cfg["source"] for slug, cfg in FORM_MAP.items()
              if cfg.get("form_type") != "unavailable" or SITE_FORM_UNAVAILABLE_ENABLED}
    if not wanted:
        return
    try:
        existing = {str(s.get("external_id")): s for s in await api.list_sources()}
        missing = [{"name": name, "external_id": ext}
                   for ext, name in wanted.items() if ext not in existing]
        if missing:
            created = await api.create_sources(missing)
            if created:
                logger.info("site_form: зарегистрировано источников: %s из %s",
                            len(created), len(missing))
            else:
                # amo: «Integration needs widget» — sources API только для
                # виджетов. Не ошибка контура: форма видна тегом и именем сделки.
                logger.info("site_form: amo не даёт регистрировать источники "
                            "токен-интеграции (нужен виджет) — форма видна тегом")
    except Exception:
        logger.exception("site_form: регистрация источников не удалась (не критично)")


async def init() -> None:
    global _wake, _worker_task
    if not SITE_FORM_ENABLED:
        logger.info("site_form: выключено (SITE_FORM_ENABLED не задан)")
        return
    if not SITE_FORM_SECRET:
        logger.error("site_form: SITE_FORM_ENABLED=1, но SITE_FORM_SECRET пуст — приём не работает")
        return
    if not FORM_MAP:
        logger.error("site_form: SITE_FORM_ENABLED=1, но карта SITE_FORM_MAP пуста — приём не работает")
        return
    try:
        await asyncio.to_thread(store.init_db)
    except Exception:
        # Схема 1 работает и без очереди; схема 2 будет отвечать 503, сайт покажет ошибку.
        logger.exception("site_form: очередь %s не открылась — схема 2 не принимает заявки", store.DB_PATH)
    await ensure_sources()
    _wake = asyncio.Event()
    _worker_task = asyncio.get_running_loop().create_task(_worker())
    logger.info("site_form: включено, форм в карте: %s", len(FORM_MAP))


async def shutdown() -> None:
    global _worker_task
    if _worker_task is None:
        return
    _worker_task.cancel()
    try:
        await _worker_task
    except asyncio.CancelledError:
        pass
    _worker_task = None
