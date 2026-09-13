"""Приём контактных форм сайта → сделки в amoCRM с источником на каждую форму.

Контур: Contact Form 7 на sunscrypt.ru → WP-сниппет (хук wpcf7_mail_sent, сервер
WP сам постит сюда JSON) → POST /site_form → заявка в «Неразобранное» воронки →
(опционально) сразу принимается в этап. Через «Неразобранное» ходим намеренно:
только этот путь amo даёт проставить сделке «источник создания» — обычному
POST /leads источник не передаётся.

Источники вида «ContactForm_Академия» регистрируются на нашей интеграции через
/api/v4/sources при старте (ensure_sources); связь заявки с источником — по
source_uid = external_id ("site_form_<slug>").

Контракт запроса от WP-сниппета:
    POST /site_form
    X-Api-Key: <SITE_FORM_SECRET>
    {"form": "<slug>", "page_url": "https://...", "fields": {"your-name": "...", ...}}

Env:
    SITE_FORM_ENABLED=1     — включатель, по умолчанию ВЫКЛЮЧЕНО (деплой безопасен)
    SITE_FORM_SECRET=...    — сверяется с заголовком X-Api-Key
    SITE_FORM_MAP='{"svyazatsya": {"source": "ContactForm_Связаться",
                    "pipeline_id": 123, "status_id": 456, "tags": ["Форма сайта"]},
                    "test-svyazatsya": {"source": "ContactForm_Тест",
                    "pipeline_id": <воронка Тест>, "status_id": <её этап>,
                    "tags": ["Тест"]}}'
        slug формы → куда класть. status_id не задан → заявка остаётся
        в «Неразобранном» воронки pipeline_id. Формы не из карты игнорируются.
    SITE_FORM_RATE_PER_MINUTE=30 — предел заявок с одного IP в минуту
"""

import asyncio
import hmac
import json
import logging
import os
import re
import time

import api

logger = logging.getLogger("uvicorn.error")

SITE_FORM_ENABLED = os.getenv("SITE_FORM_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")
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

# Обрезка недоверенного ввода перед отправкой в amo (иначе 400).
MAX_NAME_LEN = 200
MAX_NOTE_LEN = 5000
# Антидубль повторной отправки той же заявки (даблклик, ретрай WP).
SEEN_TTL_SECONDS = _env_int("SITE_FORM_SEEN_TTL_SECONDS", 120)

# Ключи полей CF7, из которых достаём контакт (первый непустой).
NAME_KEYS = ("your-name", "name", "fio", "imya")
PHONE_KEYS = ("your-tel", "your-phone", "tel", "phone", "telefon")
EMAIL_KEYS = ("your-email", "email")


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
        entry = {
            "source": source,
            "pipeline_id": pipeline_id,
            "status_id": status_id if isinstance(status_id, int) else None,
            "tags": [str(t) for t in (cfg.get("tags") or []) if str(t).strip()],
        }
        out[str(slug)] = entry
    return out


FORM_MAP = _load_map()

_rate: dict[str, list] = {}
_seen: dict[str, float] = {}


def is_enabled() -> bool:
    return SITE_FORM_ENABLED and bool(SITE_FORM_SECRET) and bool(FORM_MAP)


def secret_ok(key: str) -> bool:
    if not SITE_FORM_SECRET or not key:
        return False
    return hmac.compare_digest(SITE_FORM_SECRET, key)


def allow_ip(ip: str) -> bool:
    """Скользящее окно на минуту. IP пустой (нет X-Forwarded-For) — общая корзина."""
    now = time.monotonic()
    bucket = _rate.setdefault(ip or "-", [])
    bucket[:] = [t for t in bucket if now - t < 60]
    if len(bucket) >= SITE_FORM_RATE_PER_MINUTE:
        return False
    bucket.append(now)
    return True


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


async def process(payload: dict, ip: str = "") -> int | None:
    """Одна заявка → сделка. Возвращает id сделки или None (не создана/пропуск)."""
    slug = str(payload.get("form") or "").strip()
    cfg = FORM_MAP.get(slug)
    if not cfg:
        logger.warning("site_form: форма %r не в карте — пропуск", slug)
        return None
    fields = payload.get("fields")
    if not isinstance(fields, dict):
        logger.warning("site_form[%s]: нет блока fields — пропуск", slug)
        return None

    name = _pick(fields, NAME_KEYS)[:MAX_NAME_LEN]
    phone = _normalize_phone(_pick(fields, PHONE_KEYS))
    email = _pick(fields, EMAIL_KEYS)
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

    res = await api.create_unsorted_lead_ex(
        lead_name=f"{source}: {name or phone or email}",
        pipeline_id=cfg["pipeline_id"],
        contact=contact,
        source_uid=external_id(slug),
        page_url=page_url,
        created_ts=int(time.time()),
        source_name=source,
        form_id=slug,
        lead_tags=cfg["tags"] or None,
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

    await api.add_note_to_lead(lead_id, _note_text(slug, fields, page_url))
    logger.info("site_form[%s]: сделка %s, источник %s", slug, lead_id, source)
    return lead_id


async def _safe_process(payload: dict, ip: str) -> None:
    try:
        await process(payload, ip)
    except Exception:
        logger.exception("site_form: ошибка обработки заявки")


def handle_bg(payload: dict, ip: str = "") -> None:
    """Обработка фоном: вебхук отвечает 200 сразу, WP не ждёт amo."""
    asyncio.get_running_loop().create_task(_safe_process(payload, ip))


async def ensure_sources() -> None:
    """Регистрирует недостающие источники карты на нашей интеграции. Ошибка не
    роняет старт: без источника amo подставит источник интеграции по умолчанию."""
    wanted = {external_id(slug): cfg["source"] for slug, cfg in FORM_MAP.items()}
    if not wanted:
        return
    try:
        existing = {str(s.get("external_id")): s for s in await api.list_sources()}
        missing = [{"name": name, "external_id": ext}
                   for ext, name in wanted.items() if ext not in existing]
        if missing:
            created = await api.create_sources(missing)
            logger.info("site_form: зарегистрировано источников: %s из %s",
                        len(created), len(missing))
    except Exception:
        logger.exception("site_form: регистрация источников не удалась (не критично)")


async def init() -> None:
    if not SITE_FORM_ENABLED:
        logger.info("site_form: выключено (SITE_FORM_ENABLED не задан)")
        return
    if not SITE_FORM_SECRET:
        logger.error("site_form: SITE_FORM_ENABLED=1, но SITE_FORM_SECRET пуст — приём не работает")
        return
    if not FORM_MAP:
        logger.error("site_form: SITE_FORM_ENABLED=1, но карта SITE_FORM_MAP пуста — приём не работает")
        return
    await ensure_sources()
    logger.info("site_form: включено, форм в карте: %s", len(FORM_MAP))
