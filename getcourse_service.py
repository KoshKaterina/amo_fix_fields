"""GetCourse → amo: заказы школы Академии в сделки воронки «Академия».

Постановка Кати 21.10.2026 (через Тиану): каждый заказ GetCourse должен попасть в amo. Есть у
человека открытая сделка Академии - дополнить её примечанием и тегом (форма, откуда пришёл,
UTM). Нет - создать новую на «Входящем лиде», где её раздаст отдельное правило распределителя
поровну Артёму и Кириллу. Прежние сделки ответственного не меняют.

Путь события:
    процесс GetCourse по заказам → «Вызвать url» POST /getcourse/{secret}
    → accept(): проверка и запись в очередь (getcourse_store) → сразу 200
    → фоновый обработчик: найти человека → выбрать сделку → создать или дополнить
    → номер сделки обратно в заказ GetCourse (если включено)

Что приходит. Тело вызова собираем мы сами в настройке процесса, поэтому ключи плоские и
латинские (имена параметров процесса GetCourse - только латиница, цифры, «-» и «_»):
    event         order_created (по умолчанию) | order_paid
    order_id      {object.id}             - внутренний ID заказа, из адреса карточки
    order_number  {object.number}         - номер, который видит человек
    status, positions, cost, payed, created_at, payed_at
    origin        client - заказ создал ученик (процесс отобран по «Заказ создан клиентом»)
    page_url      {object.deal_created_url} - страница формы; GetCourse пишет её не всегда
    user_id, email, phone, first_name, last_name, telegram
    deal_utm_*    {object.deal_utm_source}... - UTM заказа (первые по приоритету)
    gc_utm_*      {object.gc_system_deal_utm_source}... - UTM, которые GetCourse пишет сам
    user_utm_*    UTM пользователя - последний запасной вариант
    a_<код>       ответ анкеты; вопрос по коду берётся из getcourse_map.json
Неподставленная переменная процесса («{object.что-то}») считается пустой.

Правила выбора сделки (ТЗ «GetCourse-amoCRM-Tiana-2026-10-21»):
- ключи человека - почта, телефон с кодом страны, ник Телеграма и связка «ученик GetCourse →
  контакт amo» из прошлых заказов. Поиск amo даёт кандидатов, совпадение проверяется точно;
- одна открытая сделка Академии → дополняем её;
- открытых нет → новая сделка на найденном контакте, контакта нет → вместе с новым контактом;
- несколько открытых → ждём сторож дублей и смотрим снова, не разрешилось → на разбор;
- ключи ведут к разным людям (почта у одного контакта, телефон у другого, и у каждого
  свои другие значения) → на разбор, наугад не создаём и не склеиваем;
- сделки других воронок и закрытые не трогаем;
- оплата и заказ, созданный сотрудником без формы, новую сделку не создают - только дополняют.

Что пишется в существующую сделку: примечание с маркером [GC:ID заказа:событие] (повтор его
не дублирует), теги дописываются к имеющимся, UTM - только в пустые поля (первая атрибуция
не затирается), номер заказа - в поле «ID заказа GetCourse». Этап и ответственный не меняются.
В контакт дописываются недостающие почта, телефон, ник; заполненное не перетирается.
"""

import asyncio
import datetime as dt
import hmac
import json
import logging
import os
import re
import time
from collections import defaultdict
from urllib.parse import urlsplit

import getcourse_amo_client as amo
import getcourse_api_client as gc
import getcourse_store as store
import telegram_contact
from shared.phones import digits_only, ru_8_to_7
from shared.timez import now_msk
from waybill_config import (
    GETCOURSE_ACCOUNT_URL,
    GETCOURSE_AMO_BASE_URL,
    GETCOURSE_DRY_RUN,
    GETCOURSE_ENABLED,
    GETCOURSE_GC_LEAD_FIELD,
    GETCOURSE_KEEP_DAYS,
    GETCOURSE_LEAD_ORDER_FIELD_ID,
    GETCOURSE_ONLY_USER_IDS,
    GETCOURSE_PIPELINE_ID,
    GETCOURSE_RECONCILE_FROM,
    GETCOURSE_RECONCILE_INTERVAL_S,
    GETCOURSE_SECRET,
    GETCOURSE_STATUS_ID,
    GETCOURSE_WORKER_INTERVAL_S,
)

logger = logging.getLogger("uvicorn.error")

EVENTS = {"order_created": "заказ создан", "order_paid": "заказ оплачен"}
CLOSED_STATUSES = (142, 143)

FIELD_TELEGRAM = 577785                       # TelegramUsername_WZ - сюда пишем ник
TELEGRAM_FIELDS = (577785, 571057, 575853)    # где ник уже может лежать (сверка)
UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term")
UTM_FIELDS = {                                # поля сделки, те же, что у импортов вебинаров
    "utm_source": 559303,
    "utm_medium": 559305,
    "utm_campaign": 559307,
    "utm_term": 559309,
    "utm_content": 559311,
}

# Повторы при недоступности amo или GetCourse; после последнего - failed и алерт.
RETRY_DELAYS_S = (60, 300, 900, 1800, 3600, 3600)
# Ответ на создание сделки потерян: ждём и ищем человека заново - созданная сделка найдётся.
UNCERTAIN_DELAY_S = 900
# Несколько открытых сделок: сторож дублей склеивает через минуту после события. Ждём его
# столько раз, сколько здесь пауз, потом отдаём на разбор.
DEDUP_WAITS_S = (300, 900)
# После рестарта брошенные «processing» возвращаются в очередь с этой паузой.
STALE_DELAY_S = 900

MAX_BODY_BYTES = 32 * 1024
MAX_VALUE_LEN = 1000
MAX_NOTE_LEN = 6000
MAX_NAME_LEN = 250

_PLACEHOLDER = re.compile(r"^\{[^{}]*\}$")
_EMAIL = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
_DIGITS_ID = re.compile(r"^\d{1,20}$")
_PHONE_LIKE = re.compile(r"[\d\s()+.-]{10,}")

MAP_PATH = os.getenv("GETCOURSE_MAP_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                        "getcourse_map.json"))

_wake: asyncio.Event | None = None
_worker_task: asyncio.Task | None = None
_reconcile_task: asyncio.Task | None = None


def _load_map() -> dict:
    try:
        with open(MAP_PATH, encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError):
        logger.exception("getcourse: карта %s не прочиталась — только базовый тег", MAP_PATH)
        data = {}
    pages = [p for p in data.get("pages") or [] if isinstance(p, dict) and (p.get("match") or p.get("offer"))]
    return {
        "base_tags": list(data.get("base_tags") or ["GetCourse"]),
        "paid_tags": list(data.get("paid_tags") or []),
        "manual_tags": list(data.get("manual_tags") or []),
        "pages": pages,
        "answers": dict(data.get("answers") or {}),
    }


MAP = _load_map()


# ─────────────────────────── вход ───────────────────────────

def is_enabled() -> bool:
    return GETCOURSE_ENABLED and bool(GETCOURSE_SECRET)


def authorized(secret: str) -> bool:
    return bool(GETCOURSE_SECRET) and hmac.compare_digest(str(secret or ""), GETCOURSE_SECRET)


def _clean(value) -> str:
    text = str(value if value is not None else "").strip()
    if not text or _PLACEHOLDER.match(text) or text.lower() in ("-", "null", "none"):
        return ""
    return text[:MAX_VALUE_LEN]


def parse(payload: dict) -> dict:
    """Тело вызова → событие в одном виде. ValueError(код) - принимать нельзя.

    Результат сам является допустимым телом: в очередь кладётся он, и обработчик разбирает
    его той же функцией.
    """
    if not isinstance(payload, dict):
        raise ValueError("not-object")
    user = payload.get("user") if isinstance(payload.get("user"), dict) else {}

    def pick(key: str, nested: str | None = None) -> str:
        return _clean(payload.get(key)) or (_clean(user.get(nested or key)) if user else "")

    event = _clean(payload.get("event")) or "order_created"
    if event not in EVENTS:
        raise ValueError("unknown-event")
    order_id = _clean(payload.get("order_id"))
    if not _DIGITS_ID.match(order_id):
        raise ValueError("no-order-id")
    ev = {
        "event": event,
        "order_id": order_id,
        "order_number": _clean(payload.get("order_number")),
        "status": _clean(payload.get("status")),
        "positions": _clean(payload.get("positions")),
        "cost": _clean(payload.get("cost")),
        "payed": _clean(payload.get("payed")),
        "created_at": _clean(payload.get("created_at")),
        "payed_at": _clean(payload.get("payed_at")),
        "page_url": _clean(payload.get("page_url")),
        "origin": _clean(payload.get("origin")).lower()[:20],
        "user_id": pick("user_id", "id"),
        "email": pick("email"),
        "phone": pick("phone"),
        "first_name": pick("first_name"),
        "last_name": pick("last_name"),
        "telegram": pick("telegram"),
    }
    for prefix in ("deal_", "gc_", "user_"):
        for key in UTM_KEYS:
            value = _clean(payload.get(prefix + key))
            if value:
                ev[prefix + key] = value
    answers = payload.get("answers") if isinstance(payload.get("answers"), dict) else {}
    for key, value in list(answers.items()) + [(k[2:], v) for k, v in payload.items() if str(k).startswith("a_")]:
        code = re.sub(r"[^A-Za-z0-9_-]", "", str(key))[:40]
        value = _clean(value)
        if code and value:
            ev["a_" + code] = value
    return ev


def answer_states(payload: dict) -> str:
    """ВРЕМЕННО, 21.10.2026: что пришло в параметрах a_* - диагностика ответа «заработок» у
    анкеты strategy. Значения ответов в журнал не идут: только длина, а у неподставленной
    переменной - её текст (это название поля ГК, не данные человека). Убрать после проверки."""
    states = []
    for key, value in payload.items():
        if not str(key).startswith("a_"):
            continue
        text = str(value if value is not None else "").strip()
        if not text:
            states.append(f"{key}=пусто")
        elif _PLACEHOLDER.match(text):
            states.append(f"{key}=не подставлено {text[:120]!r}")
        else:
            states.append(f"{key}={len(text)} симв.")
    missing = [f"a_{code}" for code in MAP["answers"] if f"a_{code}" not in payload]
    if missing:
        states.append("нет параметров: " + ", ".join(missing))
    return "; ".join(states) or "параметров a_* нет"


async def accept(payload: dict) -> tuple[int, dict]:
    """Приём вызова процесса: 200 только когда событие легло в очередь."""
    try:
        ev = parse(payload)
    except ValueError as exc:
        logger.warning("getcourse: вызов отклонён (%s)", exc)
        return 422, {"ok": False, "error": str(exc)}
    event_id = f"{ev['order_id']}:{ev['event']}"
    try:
        inserted, _ = await asyncio.to_thread(
            store.insert_pending, event_id, ev["event"], ev["order_id"], ev["order_number"],
            ev["user_id"], json.dumps(ev, ensure_ascii=False),
        )
    except Exception:
        logger.exception("getcourse: очередь недоступна, событие %s не принято", event_id)
        return 503, {"ok": False, "error": "store"}
    if inserted:
        logger.info("getcourse: принято %s (заказ #%s)", event_id, ev["order_number"] or "?")
        logger.info("getcourse: ответы %s: %s", event_id, answer_states(payload))
        _kick()
    return 200, {"ok": True, "status": "accepted" if inserted else "duplicate"}


# ─────────────────────────── нормализация ───────────────────────────

def norm_email(raw) -> str:
    value = str(raw or "").strip().lower()
    return value if _EMAIL.fullmatch(value) else ""


def norm_phone(raw) -> str:
    """Цифры номера с кодом страны или ''. Российские 8… и 9… (10 цифр) приводим к 7…;
    международные оставляем целиком - хвостом из 10 цифр разные страны не склеиваем."""
    digits = ru_8_to_7(digits_only(str(raw or "")))
    if len(digits) == 10 and digits.startswith("9"):
        digits = "7" + digits
    if not 10 <= len(digits) <= 15 or digits.startswith("0"):
        return ""
    return digits


def norm_telegram(raw) -> str:
    """«@Name», «t.me/name», «name» → «@name» в нижнем регистре; не ник (телефон, имя) → ''."""
    return (telegram_contact.normalize(raw) or "").lower()


def _keys(ev: dict) -> dict[str, str]:
    keys = {
        "email": norm_email(ev.get("email")),
        "phone": norm_phone(ev.get("phone")),
        "telegram": norm_telegram(ev.get("telegram")),
    }
    # Люди часто пишут в поле Телеграма телефон: тогда это телефон.
    tg_raw = ev.get("telegram") or ""
    if not keys["telegram"] and not keys["phone"] and _PHONE_LIKE.fullmatch(tg_raw):
        keys["phone"] = norm_phone(tg_raw)
    return {k: v for k, v in keys.items() if v}


def _contact_values(contact: dict) -> dict[str, set[str]]:
    values = {"email": set(), "phone": set(), "telegram": set()}
    for field in contact.get("custom_fields_values") or []:
        raw = [str(v.get("value") or "") for v in field.get("values") or []]
        if field.get("field_code") == "EMAIL":
            values["email"].update(norm_email(v) for v in raw)
        elif field.get("field_code") == "PHONE":
            values["phone"].update(norm_phone(v) for v in raw)
        elif field.get("field_id") in TELEGRAM_FIELDS:
            values["telegram"].update(norm_telegram(v) for v in raw)
    for kind in values:
        values[kind].discard("")
    return values


# ─────────────────────────── поиск человека и сделки ───────────────────────────

def _score(hits: set[str]) -> tuple:
    return (("gc" in hits) * 2 + len(hits - {"gc"}), "email" in hits, "phone" in hits)


def _contradiction(keys: dict, contacts: dict[int, dict], hits: dict[int, set]) -> bool:
    """Ключи ведут к РАЗНЫМ людям: у контакта A наша почта, но СВОЙ другой телефон, а наш
    телефон - у контакта B. Если у A телефона нет вовсе, это скорее дубль того же человека."""
    for a_id, a in contacts.items():
        a_values = _contact_values(a)
        for kind in ("email", "phone"):
            if kind not in keys or kind in hits[a_id] or not a_values[kind]:
                continue
            if any(kind in hits[b_id] for b_id in contacts if b_id != a_id):
                return True
    return False


async def resolve(ev: dict) -> dict:
    """Решение по человеку: existing | create | review. Сбой чтения amo - исключение
    AmoUnavailable: «не смогли поискать» не значит «человека нет»."""
    keys = _keys(ev)
    contacts: dict[int, dict] = {}
    hits: dict[int, set] = defaultdict(set)

    linked = await asyncio.to_thread(store.contact_for, ev.get("user_id") or "")
    if not keys and not linked:
        return {"kind": "review", "reason": "no-keys"}
    if linked:
        contact = await amo.get_contact(linked)
        if contact and not contact.get("is_deleted"):
            contacts[int(contact["id"])] = contact
            hits[int(contact["id"])].add("gc")

    queries = []
    if "email" in keys:
        queries.append(keys["email"])
    if "phone" in keys:
        queries.append(keys["phone"][-10:])
    if "telegram" in keys:
        queries.append(keys["telegram"].lstrip("@"))
    for query in queries:
        for contact in await amo.search_contacts(query):
            if contact.get("id") and not contact.get("is_deleted"):
                contacts.setdefault(int(contact["id"]), contact)
    # Точная сверка: поиск amo ищет по подстроке и отдаёт лишних.
    for contact_id, contact in list(contacts.items()):
        values = _contact_values(contact)
        for kind, value in keys.items():
            if value in values[kind]:
                hits[contact_id].add(kind)
        if not hits[contact_id]:
            del contacts[contact_id]

    if not contacts:
        if not keys:
            return {"kind": "review", "reason": "no-keys"}
        return {"kind": "create", "contact": None, "reason": "new-person"}

    strong = [cid for cid in contacts if hits[cid] - {"telegram"}]
    if strong:
        candidates = {cid: contacts[cid] for cid in strong}
    else:
        if len(contacts) > 1:
            return {"kind": "review", "reason": "shared-telegram", "contacts": sorted(contacts)}
        candidates = dict(contacts)
    if _contradiction(keys, candidates, hits):
        return {"kind": "review", "reason": "conflict-keys", "contacts": sorted(candidates)}

    lead_owner: dict[int, list[int]] = defaultdict(list)
    for contact_id, contact in candidates.items():
        for lead in ((contact.get("_embedded") or {}).get("leads") or []):
            if lead.get("id"):
                lead_owner[int(lead["id"])].append(contact_id)
    leads = await amo.get_leads(list(lead_owner))
    open_leads = [
        lead for lead in leads
        if int(lead.get("pipeline_id") or 0) == GETCOURSE_PIPELINE_ID
        and int(lead.get("status_id") or 0) not in CLOSED_STATUSES
        and not lead.get("is_deleted")
    ]
    if len(open_leads) > 1:
        return {"kind": "review", "reason": "several-open-deals", "leads": sorted(int(l["id"]) for l in open_leads)}
    if len(open_leads) == 1:
        lead = open_leads[0]
        owners = lead_owner.get(int(lead["id"])) or list(candidates)
        contact_id = max(owners, key=lambda cid: _score(hits[cid]))
        return {"kind": "existing", "lead": lead, "contact_id": contact_id}

    ranked = sorted(candidates, key=lambda cid: _score(hits[cid]), reverse=True)
    if len(ranked) > 1 and _score(hits[ranked[0]]) == _score(hits[ranked[1]]):
        return {"kind": "review", "reason": "ambiguous-contacts", "contacts": ranked}
    return {"kind": "create", "contact": candidates[ranked[0]], "reason": "known-contact"}


# ─────────────────────────── оформление ───────────────────────────

def page_entry(page_url: str) -> dict | None:
    for entry in MAP["pages"]:
        if entry.get("match") and entry["match"] in page_url:
            return entry
    return None


def from_client(ev: dict) -> bool:
    """Заказ создал сам ученик (анкета, виджет), а не сотрудник.

    Адрес страницы GetCourse пишет не всегда: 21.10.2026 два заказа с одной и той же формы
    пришли без `deal_created_url`, а вчерашний - с ним. Поэтому главный признак - `origin=client`:
    его ставит процесс, отобранный по условию «Заказ создан клиентом». Адрес - запасной.
    """
    return bool(ev.get("page_url")) or ev.get("origin") == "client"


def form_entry(ev: dict) -> dict | None:
    """Строка карты для заказа: по адресу страницы, а без него - по названию предложения."""
    entry = page_entry(ev["page_url"]) if ev.get("page_url") else None
    if entry is None and ev.get("positions"):
        for candidate in MAP["pages"]:
            if candidate.get("offer") and candidate["offer"] in ev["positions"]:
                return candidate
    return entry


def _page_path(page_url: str) -> str:
    try:
        parts = urlsplit(page_url)
    except ValueError:
        return page_url[:200]
    path = parts.path or page_url
    if "widget" in path and parts.query:
        path += "?" + parts.query
    return path[:200]


def tags_for(ev: dict) -> list[str]:
    tags = list(MAP["base_tags"])
    if ev["event"] == "order_paid":
        tags += MAP["paid_tags"]
    elif not from_client(ev):
        tags += MAP["manual_tags"]
    else:
        tags += (form_entry(ev) or {}).get("tags") or []
    seen, result = set(), []
    for tag in tags:
        if tag and tag.lower() not in seen:
            seen.add(tag.lower())
            result.append(tag)
    return result


def utm_of(ev: dict) -> dict[str, str]:
    """UTM заказа, по каждой метке первый непустой из трёх наборов."""
    utm = {}
    for key in UTM_KEYS:
        for prefix in ("deal_", "gc_", "user_"):
            value = ev.get(prefix + key)
            if value:
                utm[key] = value
                break
    return utm


def marker(ev: dict) -> str:
    return f"[GC:{ev['order_id']}:{ev['event']}]"


def _person_name(ev: dict) -> str:
    name = " ".join(x for x in (ev.get("first_name"), ev.get("last_name")) if x).strip()
    return name or ev.get("telegram") or ev.get("email") or "Ученик GetCourse"


def note_text(ev: dict) -> str:
    entry = form_entry(ev)
    label = (entry or {}).get("label")
    number = ev.get("order_number") or ev["order_id"]
    lines = [marker(ev), f"GetCourse: {EVENTS[ev['event']]} (заказ #{number})"]
    if ev.get("positions"):
        lines.append(f"Предложение: {ev['positions']}")
    if ev.get("page_url"):
        lines.append(f"Форма: {label} - {ev['page_url']}" if label else f"Форма: {ev['page_url']}")
    elif from_client(ev):
        lines.append(f"Форма: {label} (адрес страницы GetCourse не передал)" if label
                     else "Форма: заказ создал ученик, адрес страницы GetCourse не передал")
    else:
        lines.append("Форма: нет, заказ создан в GetCourse вручную или процессом")
    lines.append(f"Заказ: {GETCOURSE_ACCOUNT_URL}/sales/control/deal/update/id/{ev['order_id']}")
    if ev.get("user_id"):
        lines.append(f"Ученик: {GETCOURSE_ACCOUNT_URL}/user/control/user/update/id/{ev['user_id']}")
    if ev.get("created_at"):
        lines.append(f"Создан: {ev['created_at']}")
    money = []
    if ev.get("status"):
        money.append(f"статус {ev['status']}")
    if ev.get("cost"):
        money.append(f"стоимость {ev['cost']}")
    if ev.get("payed"):
        money.append(f"оплачено {ev['payed']}")
    if ev["event"] == "order_paid" and ev.get("payed_at"):
        money.append(f"оплата {ev['payed_at']}")
    if money:
        lines.append("Заказ: " + ", ".join(money))
    utm = utm_of(ev)
    if utm:
        lines.append("UTM: " + ", ".join(f"{k}={v}" for k, v in utm.items()))
    contact = [f"{label} {ev[key]}" for key, label in
               (("email", "почта"), ("phone", "телефон"), ("telegram", "Telegram")) if ev.get(key)]
    lines.append(f"Из заявки: {_person_name(ev)}" + (", " + ", ".join(contact) if contact else ""))
    answers = [(k[2:], v) for k, v in ev.items() if k.startswith("a_")]
    if answers:
        lines.append("Ответы анкеты:")
        for code, value in answers:
            lines.append(f"- {MAP['answers'].get(code, code)}: {value}")
    return "\n".join(lines)[:MAX_NOTE_LEN]


def _field(field_id: int, value) -> dict:
    return {"field_id": int(field_id), "values": [{"value": str(value)}]}


def _lead_fields(ev: dict, current: dict | None) -> list[dict]:
    """UTM - только в пустые поля; номер заказа - всегда последний (история в примечаниях)."""
    filled = set()
    for field in (current or {}).get("custom_fields_values") or []:
        if any(str(v.get("value") or "").strip() for v in field.get("values") or []):
            filled.add(int(field.get("field_id") or 0))
    fields = [_field(UTM_FIELDS[k], v) for k, v in utm_of(ev).items() if UTM_FIELDS[k] not in filled]
    if GETCOURSE_LEAD_ORDER_FIELD_ID:
        number = ev.get("order_number") or ev["order_id"]
        current_value = ""
        for field in (current or {}).get("custom_fields_values") or []:
            if int(field.get("field_id") or 0) == GETCOURSE_LEAD_ORDER_FIELD_ID:
                current_value = str(((field.get("values") or [{}])[0]).get("value") or "")
        if current_value != number:
            fields.append(_field(GETCOURSE_LEAD_ORDER_FIELD_ID, number))
    return fields


def _new_contact(ev: dict) -> dict:
    keys = _keys(ev)
    fields = []
    if "email" in keys:
        fields.append({"field_code": "EMAIL", "values": [{"value": keys["email"], "enum_code": "WORK"}]})
    if "phone" in keys:
        fields.append({"field_code": "PHONE", "values": [{"value": "+" + keys["phone"], "enum_code": "MOB"}]})
    if "telegram" in keys:
        fields.append(_field(FIELD_TELEGRAM, keys["telegram"]))
    contact = {"name": _person_name(ev)[:MAX_NAME_LEN], "custom_fields_values": fields}
    if ev.get("first_name"):
        contact["first_name"] = ev["first_name"][:MAX_NAME_LEN]
    if ev.get("last_name"):
        contact["last_name"] = ev["last_name"][:MAX_NAME_LEN]
    return contact


def new_lead_body(ev: dict, contact: dict | None) -> dict:
    what = (form_entry(ev) or {}).get("label") or ev.get("positions") or "заказ"
    number = ev.get("order_number") or ev["order_id"]
    body = {
        "name": f"ГК #{number}: {what} - {_person_name(ev)}"[:MAX_NAME_LEN],
        "pipeline_id": GETCOURSE_PIPELINE_ID,
        "status_id": GETCOURSE_STATUS_ID,
        "_embedded": {
            "tags": [{"name": tag} for tag in tags_for(ev)],
            "contacts": [{"id": int(contact["id"])}] if contact else [_new_contact(ev)],
        },
    }
    fields = _lead_fields(ev, None)
    if fields:
        body["custom_fields_values"] = fields
    return body


def _contact_patch(ev: dict, contact: dict) -> dict | None:
    """Недостающие почта, телефон, ник. Списки почт и телефонов amo заменяет целиком,
    поэтому прежние значения переписываются в запрос как есть."""
    keys = _keys(ev)
    have = _contact_values(contact)
    by_code = {f.get("field_code"): f for f in contact.get("custom_fields_values") or [] if f.get("field_code")}
    fields = []
    for kind, code, enum, value in (("email", "EMAIL", "WORK", keys.get("email")),
                                    ("phone", "PHONE", "MOB", "+" + keys["phone"] if keys.get("phone") else "")):
        if not value or keys[kind] in have[kind]:
            continue
        old = [{"value": v.get("value"), **({"enum_id": v["enum_id"]} if v.get("enum_id") else {})}
               for v in (by_code.get(code) or {}).get("values") or [] if v.get("value")]
        fields.append({"field_code": code, "values": old + [{"value": value, "enum_code": enum}]})
    if keys.get("telegram") and not have["telegram"]:
        fields.append(_field(FIELD_TELEGRAM, keys["telegram"]))
    return {"custom_fields_values": fields} if fields else None


async def decorate(ev: dict, lead_id: int, contact_id: int | None, *, is_new: bool, contact_is_new: bool) -> None:
    """Примечание, теги, поля, контакт. Каждый шаг можно повторить без вреда."""
    texts = await amo.lead_note_texts(lead_id)
    if not any(marker(ev) in text for text in texts):
        await amo.add_note(lead_id, note_text(ev))
    if not is_new:
        lead = await amo.get_lead(lead_id) or {}
        body: dict = {}
        current_tags = ((lead.get("_embedded") or {}).get("tags")) or []
        names = {str(t.get("name") or "").lower() for t in current_tags}
        add = [tag for tag in tags_for(ev) if tag.lower() not in names]
        if add:
            body["_embedded"] = {"tags": [{"id": int(t["id"])} for t in current_tags if t.get("id")]
                                 + [{"name": tag} for tag in add]}
        fields = _lead_fields(ev, lead)
        if fields:
            body["custom_fields_values"] = fields
        if body:
            await amo.patch_lead(lead_id, body)
    if contact_id and not contact_is_new:
        contact = await amo.get_contact(contact_id)
        patch = _contact_patch(ev, contact) if contact else None
        if patch:
            await amo.patch_contact(contact_id, patch)


async def mirror_to_getcourse(ev: dict, lead_id: int) -> str:
    """Номер сделки amo в доп. поле заказа GetCourse. Сбой не мешает сделке - только отмечается."""
    if not GETCOURSE_GC_LEAD_FIELD or not gc.configured():
        return ""
    email = norm_email(ev.get("email"))
    if not ev.get("order_number") or not email:
        return "gc-mirror: нет номера заказа или почты"
    try:
        ok = await gc.set_deal_field(ev["order_number"], email, GETCOURSE_GC_LEAD_FIELD, str(lead_id))
    except gc.GcUnavailable as exc:
        return f"gc-mirror: {exc}"
    return "" if ok else "gc-mirror: GetCourse не подтвердил"


def _usable(lead: dict | None) -> bool:
    return bool(lead) and not lead.get("is_deleted") \
        and int(lead.get("pipeline_id") or 0) == GETCOURSE_PIPELINE_ID \
        and int(lead.get("status_id") or 0) not in CLOSED_STATUSES


# ─────────────────────────── обработка ───────────────────────────

async def _finish(row: dict, status: str, action: str, **kw) -> None:
    await asyncio.to_thread(store.finish, row["event_id"], status, action, **kw)
    logger.info("getcourse: %s → %s %s%s", row["event_id"], status, action,
                f" (сделка {kw['lead_id']})" if kw.get("lead_id") else "")


async def _retry(row: dict, error: str, delay: float | None = None) -> None:
    attempts = int(row.get("attempts") or 0) + 1
    if delay is None:
        if attempts > len(RETRY_DELAYS_S):
            await _finish(row, "failed", "retries-exhausted", error=error)
            await _alert("getcourse_failed",
                         f"GetCourse → amo: заказ #{row.get('order_number') or row['order_id']} не обработан "
                         f"за {attempts} попыток.\nПоследняя ошибка: {error}\nСобытие: {row['event_id']}")
            return
        delay = RETRY_DELAYS_S[attempts - 1]
    await asyncio.to_thread(store.reschedule, row["event_id"], attempts, time.time() + delay, error)
    logger.warning("getcourse: %s - %s, повтор через %s с", row["event_id"], error, int(delay))


async def process(row: dict) -> None:
    """Одно событие из очереди. Исключения клиентов amo и GetCourse ловит run_due."""
    try:
        ev = parse(json.loads(row.get("payload") or ""))
    except (ValueError, TypeError):
        await _finish(row, "failed", "broken-payload")
        return
    if GETCOURSE_ONLY_USER_IDS and ev.get("user_id") not in GETCOURSE_ONLY_USER_IDS:
        await _finish(row, "skipped", "test-filter")
        return
    await _note_new_page(ev)

    lead_id = None
    contact_id = row.get("contact_id")
    is_new = contact_is_new = False
    if row.get("lead_id"):
        lead = await amo.get_lead(int(row["lead_id"]))
        if _usable(lead):
            lead_id = int(row["lead_id"])
            is_new = bool(row.get("create_started"))
    if lead_id is None:
        decision = await resolve(ev)
        if decision["kind"] == "review":
            await _review(row, ev, decision)
            return
        if decision["kind"] == "existing":
            lead_id = int(decision["lead"]["id"])
            contact_id = decision["contact_id"]
        else:
            can_create = ev["event"] == "order_created" and from_client(ev)
            if not can_create:
                reason = "paid-without-open-deal" if ev["event"] == "order_paid" else "manual-order-without-open-deal"
                await _finish(row, "skipped", reason)
                return
            contact = decision.get("contact")
            if GETCOURSE_DRY_RUN:
                await _finish(row, "done", "dry:create" + (f" on contact {contact['id']}" if contact else " with new contact"))
                return
            await asyncio.to_thread(store.mark_create_started, row["event_id"])
            try:
                lead_id, created_contact = await amo.create_lead_complex(new_lead_body(ev, contact))
            except amo.AmoUncertain as exc:
                await _retry(row, f"create-outcome-unknown: {exc}", delay=UNCERTAIN_DELAY_S)
                return
            contact_id = int(contact["id"]) if contact else created_contact
            contact_is_new = contact is None
            is_new = True
            await asyncio.to_thread(store.set_lead, row["event_id"], lead_id, contact_id)
    if GETCOURSE_DRY_RUN:
        await _finish(row, "done", f"dry:augment {lead_id}", lead_id=lead_id, contact_id=contact_id)
        return

    await decorate(ev, lead_id, contact_id, is_new=is_new, contact_is_new=contact_is_new)
    mirror_error = await mirror_to_getcourse(ev, lead_id)
    if contact_id and ev.get("user_id"):
        await asyncio.to_thread(store.link_contact, ev["user_id"], contact_id)
    await _finish(row, "done", "created" if is_new else "augmented",
                  lead_id=lead_id, contact_id=contact_id, error=mirror_error or None)
    if mirror_error:
        logger.warning("getcourse: %s - %s", row["event_id"], mirror_error)


async def _review(row: dict, ev: dict, decision: dict) -> None:
    attempts = int(row.get("attempts") or 0)
    if decision["reason"] == "several-open-deals" and attempts < len(DEDUP_WAITS_S):
        # Чаще всего это бот и форма создали сделку одновременно: сторож дублей склеит.
        await _retry(row, "several-open-deals", delay=DEDUP_WAITS_S[attempts])
        return
    await _finish(row, "review", decision["reason"])
    refs = [f"{GETCOURSE_AMO_BASE_URL}/leads/detail/{lead}" for lead in decision.get("leads") or []]
    refs += [f"{GETCOURSE_AMO_BASE_URL}/contacts/detail/{c}" for c in decision.get("contacts") or []]
    reasons = {
        "several-open-deals": "у человека несколько открытых сделок Академии",
        "conflict-keys": "почта и телефон ведут к разным контактам",
        "shared-telegram": "ник Телеграма есть у нескольких контактов",
        "ambiguous-contacts": "несколько равнозначных контактов",
        "no-keys": "в заказе нет почты, телефона и ника",
    }
    await _alert("getcourse_review",
                 f"GetCourse → amo: заказ #{ev.get('order_number') or ev['order_id']} нужно разобрать руками - "
                 f"{reasons.get(decision['reason'], decision['reason'])}.\n"
                 f"Заказ: {GETCOURSE_ACCOUNT_URL}/sales/control/deal/update/id/{ev['order_id']}"
                 + ("\n" + "\n".join(refs) if refs else ""))


async def _note_new_page(ev: dict) -> None:
    if ev["event"] != "order_created" or not from_client(ev) or form_entry(ev):
        return
    if ev.get("page_url"):
        what, key = f"новая форма {_page_path(ev['page_url'])}", "page:" + _page_path(ev["page_url"])
    else:
        offer = (ev.get("positions") or "без предложения")[:100]
        what, key = f"новое предложение «{offer}» (адрес страницы не передан)", "offer:" + offer
    if await asyncio.to_thread(store.first_seen, key):
        await _alert("getcourse_new_form",
                     f"GetCourse → amo: {what}. Сделки идут с тегом "
                     f"{', '.join(MAP['base_tags'])}; свой тег и название - строкой в getcourse_map.json.")


async def _alert(event_key: str, text: str) -> None:
    """Технический чат. Без имён, почт и телефонов - только номера заказов и ссылки."""
    try:
        import alerts
        import telegram_bot

        decision = alerts.decide(event_key, legacy_text=text, values={})
        if decision is not None:
            await telegram_bot.send_alert(decision.text, **decision.send_kwargs())
    except Exception:
        logger.exception("getcourse: алерт %s не отправлен", event_key)


async def run_due(now: float | None = None) -> int:
    processed = 0
    while True:
        rows = await asyncio.to_thread(store.due, now)
        if not rows:
            return processed
        handled = 0
        for row in rows:
            if not store.claim(row["event_id"], now):
                continue
            handled += 1
            try:
                await process(row)
            except asyncio.CancelledError:
                raise
            except (amo.AmoUnavailable, gc.GcUnavailable) as exc:
                await _retry(row, f"unavailable: {exc}")
            except amo.AmoUncertain as exc:
                # Примечание или правка могли пройти; повтор безопасен (маркер, теги, пустые поля).
                await _retry(row, f"write-outcome-unknown: {exc}")
            except amo.AmoError as exc:
                await _finish(row, "failed", f"amo-{exc.status}", error=str(exc))
                await _alert("getcourse_failed",
                             f"GetCourse → amo: заказ #{row.get('order_number') or row['order_id']} - amo отказала "
                             f"({exc}).\nСобытие: {row['event_id']}")
            except Exception as exc:
                logger.exception("getcourse: %s - сбой обработки", row["event_id"])
                await _retry(row, type(exc).__name__, delay=await _delay_after_crash(row))
        processed += handled
        if handled == 0:
            return processed


async def _delay_after_crash(row: dict) -> float | None:
    """Упали после запроса на создание, а ID сделки не сохранили - сделка могла появиться.
    Тогда ждём, пока поиск amo её увидит, иначе следующий заход создаст вторую."""
    try:
        fresh = await asyncio.to_thread(store.get, row["event_id"])
    except Exception:
        return UNCERTAIN_DELAY_S
    if fresh and fresh.get("create_started") and not fresh.get("lead_id"):
        attempts = int(row.get("attempts") or 0) + 1
        return max(UNCERTAIN_DELAY_S, RETRY_DELAYS_S[min(attempts, len(RETRY_DELAYS_S)) - 1])
    return None


# ─────────────────────────── сверка с выгрузкой ───────────────────────────

def _column(fields: list[str], *names: str) -> int | None:
    lowered = [str(f).strip().lower() for f in fields]
    for name in names:
        if name.lower() in lowered:
            return lowered.index(name.lower())
    return None


async def reconcile_once(poll_interval_s: float = 60, polls: int = 15) -> dict:
    """Заказы из выгрузки GetCourse, которых нет в очереди, - в технический чат, по разу.

    Процесс GetCourse вызов не повторяет, поэтому потерянный вызов виден только так.
    Сами пропуски сюда не догружаем: в выгрузке нет ни страницы формы, ни ответов анкеты.
    """
    if not GETCOURSE_RECONCILE_FROM or not gc.configured() or GETCOURSE_ONLY_USER_IDS:
        return {"skipped": True}
    since = max(GETCOURSE_RECONCILE_FROM, (now_msk() - dt.timedelta(days=2)).date().isoformat())
    export_id = await gc.start_deals_export(since)
    if not export_id:
        return {"started": False}
    result = None
    for _ in range(polls):
        await asyncio.sleep(poll_interval_s)
        result = await gc.fetch_export(export_id)
        if result is not None:
            break
    if result is None:
        logger.info("getcourse: выгрузка %s не готова за %s проверок, сверка в следующий раз", export_id, polls)
        return {"ready": False}
    fields, items = result
    id_col = _column(fields, "ID заказа", "Номер", "id")
    number_col = _column(fields, "Номер", "Номер заказа")
    created_col = _column(fields, "Дата создания", "Создан")
    if id_col is None:
        logger.error("getcourse: в выгрузке заказов нет колонки ID: %s", fields[:20])
        return {"error": "no-id-column"}
    # Свежие заказы не трогаем: процесс GetCourse мог ещё не дойти до вызова.
    fresh_after = (now_msk() - dt.timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M")
    candidates = []
    for item in items:
        value = str(item[id_col]).strip()
        if not value:
            continue
        if created_col is not None and str(item[created_col])[:16] >= fresh_after:
            continue
        number = str(item[number_col]).strip() if number_col is not None else value
        candidates.append((value, number))
    known = await asyncio.to_thread(store.known_orders, [v for pair in candidates for v in pair], "order_created")
    missing = [(v, n) for v, n in candidates if v not in known and n not in known]
    new_missing = [pair for pair in missing if await asyncio.to_thread(store.first_seen, f"missing:{pair[0]}")]
    if new_missing:
        numbers = ", ".join(f"#{n}" for _, n in new_missing[:20])
        more = f" и ещё {len(new_missing) - 20}" if len(new_missing) > 20 else ""
        await _alert("getcourse_missing",
                     f"GetCourse → amo: заказы не дошли до интеграции: {numbers}{more}. "
                     f"Процесс GetCourse их не передал - проверить ветку «Ошибка» процесса.")
    logger.info("getcourse: сверка - заказов %s, не дошли %s, новых из них %s", len(candidates), len(missing), len(new_missing))
    return {"checked": len(candidates), "missing": len(missing), "alerted": len(new_missing)}


# ─────────────────────────── фон ───────────────────────────

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
                removed = await asyncio.to_thread(store.purge, GETCOURSE_KEEP_DAYS)
                if removed:
                    logger.info("getcourse: из очереди удалено старых строк: %s", removed)
                last_purge = time.time()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("getcourse: сбой фонового обработчика")
        try:
            await asyncio.wait_for(_wake.wait(), timeout=GETCOURSE_WORKER_INTERVAL_S)
        except asyncio.TimeoutError:
            pass


async def _reconcile_loop() -> None:
    while True:
        await asyncio.sleep(GETCOURSE_RECONCILE_INTERVAL_S)
        try:
            await reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("getcourse: сверка с выгрузкой не удалась")


async def init() -> None:
    global _wake, _worker_task, _reconcile_task
    if not GETCOURSE_ENABLED:
        logger.info("getcourse: выключено (GETCOURSE_ENABLED не задан)")
        return
    if not GETCOURSE_SECRET or not amo.configured():
        logger.error("getcourse: GETCOURSE_ENABLED=1, но нет GETCOURSE_SECRET или GETCOURSE_AMO_TOKEN — приём не работает")
        return
    try:
        await asyncio.to_thread(store.init_db)
        recovered = await asyncio.to_thread(store.recover_stale, 0, STALE_DELAY_S)
        if recovered:
            logger.warning("getcourse: после рестарта возвращено в очередь событий: %s", recovered)
    except Exception:
        logger.exception("getcourse: очередь %s не открылась — вызовы будут получать 503", store.DB_PATH)
    _wake = asyncio.Event()
    loop = asyncio.get_running_loop()
    _worker_task = loop.create_task(_worker())
    if GETCOURSE_RECONCILE_FROM:
        _reconcile_task = loop.create_task(_reconcile_loop())
    logger.info("getcourse: включено%s, форм в карте: %s", " (холостой ход)" if GETCOURSE_DRY_RUN else "",
                len(MAP["pages"]))


async def shutdown() -> None:
    global _worker_task, _reconcile_task
    for task in (_worker_task, _reconcile_task):
        if task is None:
            continue
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    _worker_task = _reconcile_task = None
