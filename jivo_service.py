"""Мост Jivo → amoCRM (замена связки через Albato) + триаж чатов.

Jivo (CRM Settings → CRM Webhooks) шлёт POST на /jivo/<secret>. На завершение
чата (chat_finished) / оффлайн-сообщение, ЕСЛИ у посетителя есть телефон/email:

БЕЗ триажа (JIVO_TRIAGE_ENABLED off) — прежнее поведение: контакт (дедуп) →
заявка в «Неразобранное» CLEVER → примечание с перепиской.

С триажем (JIVO_TRIAGE_ENABLED on) — инверсия по тегу:
  • тег закрытия (JIVO_CLOSE_TAGS) стоит → создать сделку и СРАЗУ закрыть
    (статус 143, ответственный = сервисный юзер, причина отказа «Пропал»);
  • тега нет → «в работу»: сделать ответственным amo-юзера, сопоставленного
    оператору Jivo (JIVO_AGENT_MAP), + задача «Связаться». Оператор не
    сопоставлен → фолбэк в «Неразобранное» (распределение разведёт само).

Все целевые id/статусы/теги — через env (см. .env), чтобы крутить без правки кода.
JIVO_LOG_PAYLOAD=true пишет сырой payload Jivo в лог (для сверки полей tags/agents).
"""

import json
import logging
import os
import re
import time

import api
from waybill_config import PIPELINE_CLEVER

logger = logging.getLogger("uvicorn")


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in ("1", "true", "yes", "on")


def _flag_default_true(name: str) -> bool:
    """Как _flag, но по умолчанию (env не задан) → True."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return True
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    """Безопасный int из env: кривое значение НЕ роняет импорт модуля (а с ним
    весь сервер — СДЭК/МС/Woo/amo). Логируем и берём дефолт."""
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        logger.error("%s: не целое число (%r) — беру дефолт %s", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return float(str(raw).strip())
    except ValueError:
        logger.error("%s: не число (%r) — беру дефолт %s", name, raw, default)
        return default


JIVO_ENABLED = _flag("JIVO_WEBHOOK_ENABLED")
JIVO_WEBHOOK_SECRET = os.getenv("JIVO_WEBHOOK_SECRET", "").strip()
JIVO_PIPELINE_ID = _env_int("JIVO_PIPELINE_ID", PIPELINE_CLEVER)

# --- Триаж ---------------------------------------------------------------
JIVO_TRIAGE_ENABLED = _flag("JIVO_TRIAGE_ENABLED")
JIVO_LOG_PAYLOAD = _flag("JIVO_LOG_PAYLOAD")
# Теги-«закрыть» (по тегу сделка закрывается, без тега — в работу). Регистр игнор.
JIVO_CLOSE_TAGS = {t.strip().lower() for t in os.getenv("JIVO_CLOSE_TAGS", "закрыть,без ответа,не в работу").split(",") if t.strip()}
JIVO_CLOSE_STATUS_ID = _env_int("JIVO_CLOSE_STATUS_ID", 143)          # Закрыто и не реализовано
JIVO_WORK_STATUS_ID = _env_int("JIVO_WORK_STATUS_ID", 83537714)       # Новый лид (ХАБ; распределение исключено тегом)
JIVO_SERVICE_USER_ID = _env_int("JIVO_SERVICE_USER_ID", 11513202)     # Гладков — для закрытых
# Метка на триажных сделках (оператор уже назначен) → правила распределения
# «Новый лид» гейтятся «тег ≠ этой метки», чтобы не переназначали ответственного.
JIVO_LEAD_TAG = os.getenv("JIVO_LEAD_TAG", "Jivo").strip()
JIVO_CLOSE_REASON_FIELD = _env_int("JIVO_CLOSE_REASON_FIELD", 577623)   # Причина отказа
JIVO_CLOSE_REASON_ENUM = _env_int("JIVO_CLOSE_REASON_ENUM", 1041791)    # «jivo»
JIVO_TASK_HOURS = _env_float("JIVO_TASK_HOURS", 4.0)

# Дополнять найденный по дедупу контакт данными из чата (иначе почта/имя из
# онлайн-чата не попадают в карточку вернувшегося клиента: контакт находится по
# телефону и только ЛИНКуется, а email/имя отбрасываются). Аддитивно и осторожно:
# email/телефон только ДОБАВЛЯЕМ (не затираем существующие), имя меняем только
# если у контакта оно пустое/заглушка (== телефон/почта/«Клиент»…).
JIVO_ENRICH_CONTACT = _flag_default_true("JIVO_ENRICH_CONTACT")

# Если у клиента УЖЕ есть открытая сделка — не плодить новую, а дописать историю
# чата в неё (при нескольких открытых — в самую свежую по работе). Убирает дубли
# «Онлайн-чат Jivo» на одном контакте. Закрытыми считаем 142/143 (успех/отказ) —
# они есть в каждой воронке; переопределяется JIVO_CLOSED_STATUS_IDS.
JIVO_APPEND_TO_OPEN_LEAD = _flag_default_true("JIVO_APPEND_TO_OPEN_LEAD")


def _load_closed_status_ids() -> set:
    raw = os.getenv("JIVO_CLOSED_STATUS_IDS", "").strip()
    if not raw:
        return {142, 143}
    out = set()
    for part in raw.split(","):
        part = part.strip()
        if part:
            try:
                out.add(int(part))
            except ValueError:
                logger.error("JIVO_CLOSED_STATUS_IDS: не число (%r) — пропуск", part)
    return out or {142, 143}


JIVO_CLOSED_STATUS_IDS = _load_closed_status_ids()

# Идемпотентность (антидубль при повторной доставке вебхука ПОСЛЕ обработки):
# in-memory set недавно обработанных chat_id с TTL.
JIVO_SEEN_TTL_SECONDS = _env_int("JIVO_SEEN_TTL_SECONDS", 900)
# Обрезка недоверенного ввода из чата перед отправкой в amo (иначе 400).
JIVO_MAX_NAME_LEN = 200
JIVO_MAX_NOTE_LEN = 5000
_jivo_seen: dict[str, float] = {}


def _seen_recently(chat_id: str) -> bool:
    """True, если этот chat_id уже обработан за последние JIVO_SEEN_TTL_SECONDS."""
    if not chat_id:
        return False
    now = time.monotonic()
    if _jivo_seen:
        stale = [k for k, t in _jivo_seen.items() if now - t > JIVO_SEEN_TTL_SECONDS]
        for k in stale:
            _jivo_seen.pop(k, None)
    return chat_id in _jivo_seen


def _mark_seen(chat_id: str) -> None:
    if chat_id:
        _jivo_seen[chat_id] = time.monotonic()


def _load_agent_map() -> dict:
    """JIVO_AGENT_MAP — JSON {"<email|id оператора Jivo>": <amo user_id>}.
    Ключи нормализуем к нижнему регистру-строке."""
    raw = os.getenv("JIVO_AGENT_MAP", "").strip()
    if not raw:
        return {}
    try:
        m = json.loads(raw)
        return {str(k).strip().lower(): int(v) for k, v in m.items()}
    except Exception:
        logger.error("JIVO_AGENT_MAP: невалидный JSON — игнорирую")
        return {}


JIVO_AGENT_MAP = _load_agent_map()

# --- Мультисайт (Jivo на нескольких сайтах: Sunscrypt, Tangemshop) -----------
# Все сайты ведут в ТУ ЖЕ воронку (CLEVER Основная) и на ТЕХ ЖЕ операторов;
# различается ТОЛЬКО пометка источника на сделке (тег + префикс в названии).
# Сайт приходит сегментом в URL вебхука: /jivo/<secret>/<site> — у каждого
# канала Jivo свой URL. Без сегмента → дефолтный сайт (обратная совместимость
# с Sunscrypt: его сделки остаются без доп.метки, как раньше).
JIVO_DEFAULT_SITE = (os.getenv("JIVO_DEFAULT_SITE", "sunscrypt").strip().lower() or "sunscrypt")


def _load_site_labels() -> dict:
    """site-ключ (из URL) → человекочитаемая метка источника. Метку получает
    только НЕдефолтный сайт (Sunscrypt = базовый, помечать не нужно).
    Переопределяется env JIVO_SITE_LABELS (JSON {"<site>": "<label>"})."""
    defaults = {"tangem": "Tangemshop", "tangemshop": "Tangemshop"}
    raw = os.getenv("JIVO_SITE_LABELS", "").strip()
    if not raw:
        return defaults
    try:
        m = json.loads(raw)
        return {str(k).strip().lower(): str(v).strip() for k, v in m.items() if str(v).strip()}
    except Exception:
        logger.error("JIVO_SITE_LABELS: невалидный JSON — беру дефолт")
        return defaults


JIVO_SITE_LABELS = _load_site_labels()


def _load_domain_sites() -> dict:
    """Подстрока домена → site-ключ. Нужно, когда виджет Jivo ОДИН на оба сайта
    (один website-канал) — тогда сегмента сайта в URL вебхука нет, и сайт
    определяем по домену страницы визитёра (page.url). Env JIVO_DOMAIN_SITES."""
    defaults = {"tangemshop": "tangem"}
    raw = os.getenv("JIVO_DOMAIN_SITES", "").strip()
    if not raw:
        return defaults
    try:
        m = json.loads(raw)
        return {str(k).strip().lower(): str(v).strip().lower() for k, v in m.items() if str(v).strip()}
    except Exception:
        logger.error("JIVO_DOMAIN_SITES: невалидный JSON — беру дефолт")
        return defaults


JIVO_DOMAIN_SITES = _load_domain_sites()


def resolve_site(site) -> str:
    """Нормализует site из URL к ключу карты; пусто → дефолтный сайт."""
    return (str(site).strip().lower() if site else "") or JIVO_DEFAULT_SITE


def _site_from_page_url(page_url) -> str:
    """Определяет site по домену страницы визитёра ('' если не распознан)."""
    u = (page_url or "").lower()
    for needle, site in JIVO_DOMAIN_SITES.items():
        if needle in u:
            return site
    return ""


def resolve_site_full(site, page_url) -> str:
    """Сайт из сегмента URL вебхука (приоритет), иначе по домену page.url,
    иначе дефолт. Работает и с раздельными каналами, и с одним виджетом на 2 сайта."""
    resolved = resolve_site(site)
    if resolved == JIVO_DEFAULT_SITE:
        return _site_from_page_url(page_url) or resolved
    return resolved


def source_label(site) -> str:
    """Метка источника для НЕдефолтного сайта; для дефолта (Sunscrypt) — ''."""
    s = resolve_site(site)
    if s == JIVO_DEFAULT_SITE:
        return ""
    return JIVO_SITE_LABELS.get(s, "")


HANDLED_EVENTS = {"chat_finished", "offline_message"}

_TYPE_PREFIX = {
    "visitor": "Клиент", "client": "Клиент",
    "agent": "Оператор", "operator": "Оператор",
    "bot": "Бот", "system": "Система",
}


def is_enabled() -> bool:
    return JIVO_ENABLED and bool(JIVO_WEBHOOK_SECRET)


def secret_ok(token: str) -> bool:
    return bool(JIVO_WEBHOOK_SECRET) and token == JIVO_WEBHOOK_SECRET


def log_payload(event) -> None:
    """Пишет сырой payload Jivo в лог при JIVO_LOG_PAYLOAD — чтобы вживую
    свериться, где приходят tags/agents (доки про chat_finished неоднозначны)."""
    if not JIVO_LOG_PAYLOAD:
        return
    try:
        logger.info("Jivo RAW payload: %s", json.dumps(event, ensure_ascii=False)[:4000])
    except Exception:
        logger.info("Jivo RAW payload (repr): %s", repr(event)[:4000])


def _clean(value) -> str:
    return "" if value is None else str(value).strip()


def _format_transcript(messages) -> str:
    if not isinstance(messages, list):
        return ""
    lines = []
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        who = _TYPE_PREFIX.get(_clean(msg.get("type")).lower(), _clean(msg.get("type")) or "—")
        text = _clean(msg.get("message") or msg.get("body") or msg.get("text"))
        if text:
            lines.append(f"{who}: {text}")
    return "\n".join(lines)


def _extract_tags(event: dict, visitor: dict) -> list:
    """Собирает теги из всех правдоподобных мест payload (доки расходятся:
    корневой tags[], visitor.tags, chat.tags). Возвращает список в нижнем регистре."""
    chat = event.get("chat") if isinstance(event.get("chat"), dict) else {}
    out = []
    for src in (event.get("tags"), visitor.get("tags"), chat.get("tags")):
        if not isinstance(src, list):
            continue
        for t in src:
            if isinstance(t, dict):
                val = _clean(t.get("title") or t.get("name") or t.get("value"))
            else:
                val = _clean(t)
            if val:
                out.append(val.lower())
    return out


def _extract_agents(event: dict) -> list:
    """Список операторов: chat_finished.agents[] (массив) или chat_accepted.agent."""
    agents = event.get("agents")
    if not isinstance(agents, list):
        single = event.get("agent")
        agents = [single] if isinstance(single, dict) else []
    out = []
    for a in agents:
        if isinstance(a, dict):
            out.append({
                "id": _clean(a.get("id")),
                "email": _clean(a.get("email")).lower(),
                "name": _clean(a.get("name")),
            })
    return out


def _pick_contact_value(visitor: dict, event: dict, singular: str, plural: str) -> str:
    """Достаёт телефон/email из правдоподобных мест payload. По доке Jivo
    Webhooks API — это visitor.<singular> (chat_finished). Защитно проверяем
    также корень event и plural-массивы (emails/phones — форма CRM-webhook
    client), чтобы не потерять контакт при отклонении формата от доки."""
    for src in (visitor, event):
        if isinstance(src, dict):
            val = _clean(src.get(singular))
            if val:
                return val
    for src in (visitor, event):
        if not isinstance(src, dict):
            continue
        arr = src.get(plural)
        if isinstance(arr, list):
            for item in arr:
                if isinstance(item, dict):
                    val = _clean(item.get("value") or item.get(singular))
                else:
                    val = _clean(item)
                if val:
                    return val
    return ""


def parse_event(event: dict, site: str | None = None) -> dict | None:
    """Достаёт нужные поля. None — если событие не наше или у посетителя нет
    ни телефона, ни email («только когда есть контакт»). site — сайт-источник
    из URL вебхука (для пометки Tangemshop и т.п.)."""
    if not isinstance(event, dict):
        return None
    event_name = _clean(event.get("event_name"))
    if event_name and event_name not in HANDLED_EVENTS:
        return None

    visitor = event.get("visitor") or event.get("client") or {}
    if not isinstance(visitor, dict):
        visitor = {}

    phone = _normalize_phone(_pick_contact_value(visitor, event, "phone", "phones"))
    email = _pick_contact_value(visitor, event, "email", "emails")
    if not phone and not email:
        return None

    name = (_clean(visitor.get("name")) or _clean(event.get("name")) or phone or email)[:JIVO_MAX_NAME_LEN]

    messages = event.get("messages")
    if not isinstance(messages, list):
        chat = event.get("chat")
        messages = chat.get("messages") if isinstance(chat, dict) else None

    page = event.get("page")
    page_url = _clean(page.get("url")) if isinstance(page, dict) else ""

    return {
        "event_name": event_name or "chat",
        "name": name,
        "phone": phone,
        "email": email,
        # Реальный Webhooks API Jivo отдаёт готовый текст переписки в
        # plain_messages (в chat.messages[].message текст скрыт) — берём его,
        # иначе строим из messages (фолбэк/синтетика).
        "transcript": _clean(event.get("plain_messages")) or _format_transcript(messages),
        "page_url": page_url,
        "chat_id": _clean(event.get("chat_id")),
        "tags": _extract_tags(event, visitor),
        "agents": _extract_agents(event),
        "site": resolve_site_full(site, page_url),
    }


# --- helpers --------------------------------------------------------------

def _lead_name(name: str, site=None) -> str:
    label = source_label(site)
    if label:
        return f"Онлайн-чат Jivo · {label} — {name}"
    return f"Онлайн-чат Jivo — {name}"


def _transcript_note(payload: dict) -> str:
    label = source_label(payload.get("site"))
    parts = ["💬 Онлайн-чат Jivo" + (f" · {label}" if label else "")]
    if payload.get("page_url"):
        parts.append(f"Страница: {payload['page_url']}")
    parts.append("")
    parts.append(payload.get("transcript") or "(переписка не передана)")
    text = "\n".join(parts)
    if len(text) > JIVO_MAX_NOTE_LEN:
        text = text[:JIVO_MAX_NOTE_LEN] + "\n…(обрезано)"
    return text


def _build_contact(name: str, phone: str, email: str, contact_id: int | None) -> dict:
    if contact_id:
        return {"id": int(contact_id)}
    cf = []
    if phone:
        cf.append({"field_code": "PHONE", "values": [{"value": phone, "enum_code": "WORK"}]})
    if email:
        cf.append({"field_code": "EMAIL", "values": [{"value": email, "enum_code": "WORK"}]})
    contact = {"name": name or phone or email or "Клиент Jivo"}
    if cf:
        contact["custom_fields_values"] = cf
    return contact


async def _dedup_contact(phone: str, email: str) -> int | None:
    contact_id = None
    if phone:
        contact_id = await api.find_contact_id(phone)
    if not contact_id and email:
        contact_id = await api.find_contact_id(email)
    return contact_id


async def _ensure_contact(contact_id: int | None, name: str, phone: str, email: str) -> int | None:
    if contact_id:
        return contact_id
    return await api.create_contact(name, phone, email)


def _field_by_code(cfv: list, code: str) -> dict | None:
    if not isinstance(cfv, list):
        return None
    for f in cfv:
        if isinstance(f, dict) and str(f.get("field_code") or "").upper() == code:
            return f
    return None


def _field_value_strings(field: dict | None) -> list:
    if not field or not isinstance(field.get("values"), list):
        return []
    return [_clean(v.get("value")) for v in field["values"] if isinstance(v, dict) and _clean(v.get("value"))]


def _digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")


def _normalize_phone(raw: str) -> str:
    """Приводит телефон из Jivo к формату с ведущим «+» (Wazzup/телефония ждут
    E.164 — Jivo же шлёт номер как есть, часто без «+»). Российские
    8XXXXXXXXXX / 10-значные без кода → +7…; все остальные (в т.ч. зарубежные,
    длина ≠ 10/11) — просто «+» перед цифрами. Пусто / без цифр — возвращаем
    как есть (не телефон)."""
    d = _digits(raw)
    if not d:
        return raw
    if len(d) == 11 and d[0] == "8":
        d = "7" + d[1:]
    elif len(d) == 10:
        d = "7" + d
    return "+" + d


_PLACEHOLDER_NAMES = {"клиент", "гость", "посетитель", "клиент jivo", "no name", "noname", "unknown", "—"}


def _is_placeholder_name(existing: str, phone: str, email: str) -> bool:
    """Имя контакта считается заглушкой (можно заменить настоящим из чата), если
    пусто / равно телефону или почте / это чистые цифры-телефон / дежурное слово."""
    existing = _clean(existing)
    if not existing:
        return True
    low = existing.lower()
    if low in _PLACEHOLDER_NAMES:
        return True
    if email and low == email.lower():
        return True
    d = _digits(existing)
    if d and (existing == phone or d == _digits(phone)):
        return True
    return False


async def _find_open_lead_id(contact: dict | None) -> int | None:
    """Возвращает id открытой сделки контакта, в которую последний раз велась
    работа (max updated_at), либо None. Открытая = status_id не из
    JIVO_CLOSED_STATUS_IDS. contact должен быть получен с ?with=leads."""
    refs = ((contact or {}).get("_embedded") or {}).get("leads") or []
    ids = [r.get("id") for r in refs if isinstance(r, dict) and r.get("id")]
    if not ids:
        return None
    leads = await api.get_leads_by_ids(ids)
    open_leads = [
        ld for ld in leads
        if int(ld.get("status_id") or 0) not in JIVO_CLOSED_STATUS_IDS
    ]
    if not open_leads:
        return None
    # «последний раз велась работа» ≈ самый свежий updated_at (тай-брейк по id).
    best = max(open_leads, key=lambda ld: (ld.get("updated_at") or 0, ld.get("id") or 0))
    return best.get("id")


async def _enrich_contact(contact_id: int, name: str, phone: str, email: str,
                          contact: dict | None = None) -> None:
    """Аддитивно дополняет найденный по дедупу контакт данными из чата: добавляет
    email/телефон, если их у контакта ещё нет, и подставляет настоящее имя вместо
    заглушки. Существующие значения НЕ затираются. Без флага JIVO_ENRICH_CONTACT
    — no-op (прежнее поведение: контакт только линкуется). contact — уже
    загруженная карточка (иначе GET сам)."""
    if not JIVO_ENRICH_CONTACT:
        return
    if not (email or phone or name):
        return
    if contact is None:
        contact = await api.get_contact(contact_id)
    if not contact:
        logger.info("Jivo[enrich]: контакт %s не прочитан — пропуск дополнения", contact_id)
        return

    cfv = contact.get("custom_fields_values") or []
    patch_cfv = []

    email_field = _field_by_code(cfv, "EMAIL")
    if email:
        existing = {e.lower() for e in _field_value_strings(email_field)}
        if email.lower() not in existing:
            values = list(email_field["values"]) if email_field and isinstance(email_field.get("values"), list) else []
            values.append({"value": email, "enum_code": "WORK"})
            patch_cfv.append({"field_code": "EMAIL", "values": values})

    phone_field = _field_by_code(cfv, "PHONE")
    if phone:
        existing_phones = {_digits(p) for p in _field_value_strings(phone_field)}
        if _digits(phone) not in existing_phones:
            values = list(phone_field["values"]) if phone_field and isinstance(phone_field.get("values"), list) else []
            values.append({"value": phone, "enum_code": "WORK"})
            patch_cfv.append({"field_code": "PHONE", "values": values})

    # Имя: подставляем настоящее из чата только поверх заглушки (не затираем
    # руками выставленное имя). «Настоящее» = само не является заглушкой
    # (не пусто, не телефон, не почта, не дежурное слово).
    new_name = None
    real = _clean(name)
    if real and not _is_placeholder_name(real, phone, email):
        if _is_placeholder_name(contact.get("name"), phone, email):
            new_name = real[:JIVO_MAX_NAME_LEN]

    if not patch_cfv and not new_name:
        logger.info("Jivo[enrich]: контакту %s дополнять нечего", contact_id)
        return

    ok = await api.update_contact(contact_id, name=new_name, custom_fields_values=patch_cfv or None)
    if ok:
        added = []
        if any(f["field_code"] == "EMAIL" for f in patch_cfv):
            added.append("email")
        if any(f["field_code"] == "PHONE" for f in patch_cfv):
            added.append("phone")
        if new_name:
            added.append("name")
        logger.info("Jivo[enrich]: контакт %s дополнен (%s)", contact_id, ", ".join(added))
    else:
        logger.warning("Jivo[enrich]: не удалось дополнить контакт %s", contact_id)


def _map_agent(agents: list):
    """Возвращает (amo_user_id|None, agent_dict|None). Ищем оператора в
    JIVO_AGENT_MAP по email, затем по id. Идём с КОНЦА: при передаче чата Jivo
    дописывает текущего/закрывшего оператора в конец agents[], и сделка должна
    уйти на него, а не на того, кто чат только начал."""
    for a in reversed(agents):
        email = a.get("email")
        if email and email in JIVO_AGENT_MAP:
            return JIVO_AGENT_MAP[email], a
        aid = a.get("id")
        if aid and aid.lower() in JIVO_AGENT_MAP:
            return JIVO_AGENT_MAP[aid.lower()], a
    return None, (agents[-1] if agents else None)


async def _create_unsorted(payload: dict, contact_id: int | None, name: str, phone: str, email: str) -> int | None:
    contact = _build_contact(name, phone, email, contact_id)
    source_uid = f"jivo-{payload.get('chat_id') or phone or email}"
    lead_id, _ = await api.create_unsorted_lead(
        lead_name=_lead_name(name, payload.get("site")),
        pipeline_id=JIVO_PIPELINE_ID,
        contact=contact,
        source_uid=source_uid,
        page_url=payload.get("page_url") or "",
        created_ts=int(time.time()),
    )
    return lead_id


# --- main ----------------------------------------------------------------

async def process_jivo_chat(payload: dict) -> None:
    """Идемпотентная обёртка: один chat_id не обрабатываем дважды (антидубль при
    повторной доставке вебхука ПОСЛЕ обработки). In-flight-повтор ловит очередь
    (_pending_jivo снимается в finally воркера). Работа — в _do_process."""
    chat_id = str(payload.get("chat_id") or "")
    if chat_id and _seen_recently(chat_id):
        logger.info("Jivo: chat %s уже обработан недавно — пропуск (идемпотентность)", chat_id)
        return
    lead_id = await _do_process(payload)
    if lead_id and chat_id:
        _mark_seen(chat_id)


async def _do_process(payload: dict) -> int | None:
    """Дедуп контакта → (триаж) → сделка → примечание. Возвращает id созданной
    сделки или None (ничего не создано)."""
    phone = payload.get("phone") or ""
    email = payload.get("email") or ""
    name = payload.get("name") or phone or email or "Клиент Jivo"
    note_text = _transcript_note(payload)
    # Пометка источника: тег «Jivo» (гейт распределения) + метка сайта (Tangemshop).
    site = payload.get("site")
    lead_tags = [t for t in (JIVO_LEAD_TAG, source_label(site)) if t] or None

    contact_id = await _dedup_contact(phone, email)
    if contact_id:
        logger.info("Jivo: найден контакт %s (по %s)", contact_id, phone or email)
        # Один GET карточки (с её сделками) — и на дополнение, и на поиск открытой.
        contact = await api.get_contact(contact_id, with_leads=True)
        # Дедуп находит контакт по телефону и дальше сделка лишь ЛИНКуется к нему;
        # без этого email/имя из чата не попадут в карточку вернувшегося клиента.
        await _enrich_contact(contact_id, name, phone, email, contact=contact)
        # Уже есть открытая сделка → не плодим дубль, дописываем историю в неё.
        if JIVO_APPEND_TO_OPEN_LEAD:
            open_lead_id = await _find_open_lead_id(contact)
            if open_lead_id:
                await api.add_note_to_lead(open_lead_id, note_text)
                logger.info(
                    "Jivo: история дописана в открытую сделку %s (контакт %s) — новую не создаём",
                    open_lead_id, contact_id,
                )
                return open_lead_id

    # Триаж выключен → прежнее поведение: всё в «Неразобранное».
    if not JIVO_TRIAGE_ENABLED:
        lead_id = await _create_unsorted(payload, contact_id, name, phone, email)
        if not lead_id:
            logger.error("Jivo: заявка не создана (контакт %s)", contact_id)
            return None
        logger.info("Jivo: заявка %s в Неразобранное", lead_id)
        await api.add_note_to_lead(lead_id, note_text)
        return lead_id

    tags = payload.get("tags") or []
    # Подстроковое совпадение: «закрыть чат», «закрыть — без ответа» и т.п. тоже
    # ловятся тегом «закрыть» (операторы тегируют неточно).
    is_close = any(ct in t for t in tags for ct in JIVO_CLOSE_TAGS)

    # Бакет 1: тег закрытия → создать и сразу закрыть, без менеджера.
    if is_close:
        contact_id = await _ensure_contact(contact_id, name, phone, email)
        if not contact_id:
            logger.error("Jivo[close]: контакт не создан")
            return None
        cf = None
        if JIVO_CLOSE_REASON_FIELD and JIVO_CLOSE_REASON_ENUM:
            cf = [{"field_id": JIVO_CLOSE_REASON_FIELD, "values": [{"enum_id": JIVO_CLOSE_REASON_ENUM}]}]
        lead_id = await api.create_lead_direct(
            name=_lead_name(name, site), pipeline_id=JIVO_PIPELINE_ID,
            status_id=JIVO_CLOSE_STATUS_ID, responsible_user_id=JIVO_SERVICE_USER_ID,
            custom_fields_values=cf, contact_id=contact_id,
            tags=lead_tags,
        )
        if not lead_id:
            logger.error("Jivo[close]: сделка не создана")
            return None
        logger.info("Jivo[close]: сделка %s закрыта по тегу %s", lead_id, tags)
        await api.add_note_to_lead(lead_id, note_text)
        return lead_id

    # Бакет 2: без тега → в работу. Сопоставляем оператора Jivo → ответственный.
    amo_user, agent = _map_agent(payload.get("agents") or [])
    if not amo_user:
        # Оператор не сопоставлен → фолбэк в «Неразобранное» (распределение разведёт).
        lead_id = await _create_unsorted(payload, contact_id, name, phone, email)
        if lead_id:
            await api.add_note_to_lead(lead_id, note_text)
        logger.warning("Jivo[work]: оператор не сопоставлен (%s) → Неразобранное, сделка %s", agent, lead_id)
        return lead_id

    contact_id = await _ensure_contact(contact_id, name, phone, email)
    if not contact_id:
        logger.error("Jivo[work]: контакт не создан")
        return None
    lead_id = await api.create_lead_direct(
        name=_lead_name(name, site), pipeline_id=JIVO_PIPELINE_ID,
        status_id=JIVO_WORK_STATUS_ID, responsible_user_id=amo_user, contact_id=contact_id,
        tags=lead_tags,
    )
    if not lead_id:
        logger.error("Jivo[work]: сделка не создана")
        return None
    logger.info("Jivo[work]: сделка %s, ответственный %s (оператор %s)", lead_id, amo_user, agent)
    await api.add_note_to_lead(lead_id, note_text)
    complete_till = int(time.time() + JIVO_TASK_HOURS * 3600)
    await api.create_task(lead_id, "Связаться с клиентом (онлайн-чат Jivo)", amo_user, complete_till)
    return lead_id
