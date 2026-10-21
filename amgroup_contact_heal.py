"""Самоисцеление сделок amgroup без контакта (МойСклад -> amoCRM), 21.10.2026.

Повод - 20-21.10.2026: шесть сделок по заказам сайта остались без контакта
(36583137, 36583153, 36583813, 36583847, 36583849, 36583851). amgroup создаёт
контакт, потом сделку, потом привязку; все шесть сбоев пришлись на утренние
окна, когда API amo отдавал 500, таймауты и 429. Наши сервисы запросы
повторяют, amgroup - нет: шаг «привязать контакт» упал и больше не случился.
Без контакта офис не может создать накладную СДЭК. Разбор - журнал
projects/amo-cleanup/JOURNAL.md рабочей папки, записи 20 и 21.10.

Что делает модуль, по одной сделке (heal_lead):
1. Сделка создана из МойСклада («Заказ создан» = «Из МойСклад»), у неё есть
   «ID Контрагента», нет контакта, и ей больше AMGROUP_CONTACT_HEAL_MIN_AGE_MIN
   минут - в норме amgroup привязывает за 2-5 с, раньше порога не вмешиваемся.
2. Контрагент МС по ID -> телефон и почта -> поиск контакта в amo. Совпадение
   проверяем по значению поля (телефон - по последним 10 цифрам, почта - без
   учёта регистра), а не по факту попадания в полнотекстовую выдачу.
3. Ровно один кандидат -> перечитать сделку (вдруг amgroup успел) -> привязать
   основным -> проверить чтением -> примечание -> распределение штатно.
4. Ноль или несколько кандидатов -> контакт НЕ создаём (amgroup может ожить и
   завести свой - получится дубль человека), только примечание и алерт.

⚠️ Молчание - не ответ. МойСклад или amo не ответили (None от ms_client.get и
amo_service: сеть, таймаут, 5xx, 429, открытый брейкер) - исход «не знаю»:
ничего не пишем, никого не зовём, повторим на следующем проходе. Прочитать
сбой поиска как «контакта нет» - значит позвать человека зря; прочитать сбой
чтения сделки как «контакта нет» - привязать поверх amgroup.

Вход два: фоновый проход по сделкам, созданным за AMGROUP_CONTACT_HEAL_LOOKBACK_H
(ловит и сделки вне профилей распределения, и пропущенное на перезапусках), и
отложенный вызов из lead_distribution._contact_wait_loop, который такую
сделку видит первым (schedule_after_contact_wait).

Сухой режим (по умолчанию): все чтения и решение как в боевом, в лог пишем,
что сделали бы; ни привязки, ни примечаний, ни боевого состояния на диске. С
AMGROUP_CONTACT_HEAL_DRY_RUN_ALERTS решение ещё и уходит в тех.чат с пометкой
«сухой режим», один раз на сделку (отметки - в отдельном файле).

Состояние на диске - сделки с завершённым исходом (привязали или позвали
человека), чтобы не повторять алерт на каждом проходе и после пересборки.
Исходы «не знаю» туда не попадают никогда.

Персональные данные (телефон, почта, имя) в лог и в тех.чат не пишем - только
id сделок и контактов.
"""

import asyncio
import logging
import os
import re
import time

import alerts
import amo_service
import lead_distribution
import ms_client
import telegram_bot
from shared.files import read_json, write_json_atomic
from shared.phones import digits_only, ru_8_to_7
from waybill_config import (
    AMGROUP_CONTACT_HEAL_DRY_RUN,
    AMGROUP_CONTACT_HEAL_DRY_RUN_ALERTS,
    AMGROUP_CONTACT_HEAL_ENABLED,
    AMGROUP_CONTACT_HEAL_INTERVAL_SEC,
    AMGROUP_CONTACT_HEAL_LOOKBACK_H,
    AMGROUP_CONTACT_HEAL_MIN_AGE_MIN,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
)

logger = logging.getLogger("uvicorn")

# Поля сделки, которые заполняет amgroup (id и enum - из amgroup_lead_builder.FIELD/ENUM,
# сверены там на живых сделках 03.09.2026). Свести в общий конфиг на сшивке.
FIELD_MS_AGENT_UUID = 576695      # «ID Контрагента»
FIELD_CREATED_BY = 576683         # «Заказ создан»
ENUM_CREATED_BY_MS = 1040195      # «Из МойСклад»
FIELD_ORDER_NUMBER = 576697       # «№ Заказа» - только для текста алерта

_CONTACT_SEARCH_LIMIT = 100
# Потолок сделок на проход и пауза между ними: в норме кандидатов единицы, но после
# долгого сбоя amo их может набраться десятки, и залп запросов отодвинул бы накладные.
_PER_PASS_CAP = 10
_PAUSE_SEC = 1.0
# Запас после порога у отложенного вызова: сделка гарантированно уже «старше порога».
_SCHEDULE_MARGIN_S = 5.0
_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")
_EMAIL_FIELD_CODE = "EMAIL"
_PHONE_FIELD_CODE = "PHONE"

AMO_LEAD_URL = "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{}"
AMO_CONTACT_URL = "https://new5a2e8ea7b16b4.amocrm.ru/contacts/detail/{}"

# Исходы heal_lead. «Не знаю» (UNKNOWN_*) - повтор на следующем проходе.
LINKED = "linked"
DRY_RUN_WOULD_LINK = "dry-run-would-link"
ALREADY_LINKED = "already-linked"
NO_CANDIDATE = "no-candidate"
AMBIGUOUS = "ambiguous"
NO_IDENTIFIERS = "no-identifiers"
LINK_FAILED = "link-failed"
LINK_UNVERIFIED = "link-unverified"
UNKNOWN_LEAD = "unknown-lead-read"
UNKNOWN_MS = "unknown-ms"
UNKNOWN_SEARCH = "unknown-amo-search"
UNKNOWN_LINK = "unknown-link"
NOT_MS_LEAD = "not-ms-lead"
CLOSED = "closed"
TOO_YOUNG = "too-young"
DISABLED = "disabled"
ALREADY_HANDLED = "already-handled"
IN_FLIGHT = "in-flight"

# Завершённые исходы: человека позвали или контакт привязан - больше не трогаем.
_FINAL = {LINKED, NO_CANDIDATE, AMBIGUOUS, NO_IDENTIFIERS, LINK_FAILED, LINK_UNVERIFIED}

_STATE_PATH = os.getenv("AMGROUP_CONTACT_HEAL_STATE_PATH", "/app/var/amgroup_contact_heal.json")
_STATE_CAP = 2000
_state: dict[str, dict] = {}
_state_loaded = False
# Сухой режим: что уже залогировали, чтобы один и тот же вывод не повторялся каждые 3 минуты.
_dry_logged: set[tuple[int, str]] = set()
# Сухой режим с алертами: по каким сделкам решение уже ушло в тех.чат (на диске).
_DRY_STATE_PATH = os.getenv("AMGROUP_CONTACT_HEAL_DRY_STATE_PATH", "/app/var/amgroup_contact_heal_dry.json")
_dry_alerted: set[str] = set()
_dry_alerted_loaded = False

_in_flight: set[int] = set()
_bg_tasks: set[asyncio.Task] = set()
_task: asyncio.Task | None = None


# ════════════════ состояние ════════════════

def _load_state() -> None:
    global _state_loaded
    if _state_loaded:
        return
    _state_loaded = True
    data = read_json(_STATE_PATH, default={})
    if isinstance(data, dict):
        _state.update({str(k): v for k, v in data.items() if isinstance(v, dict)})


def _remember(lead_id: int, outcome: str) -> None:
    _state[str(lead_id)] = {"outcome": outcome, "ts": int(time.time())}
    if len(_state) > _STATE_CAP:
        for key, _ in sorted(_state.items(), key=lambda kv: kv[1].get("ts", 0))[: len(_state) - _STATE_CAP]:
            _state.pop(key, None)
    try:
        write_json_atomic(_STATE_PATH, _state)
    except Exception:
        logger.exception("amgroup_contact_heal: не записался %s", _STATE_PATH)


def _handled(lead_id: int) -> bool:
    _load_state()
    return str(lead_id) in _state


# ════════════════ признаки сделки и контакта ════════════════

def _contacts(lead: dict) -> list[dict]:
    return (lead.get("_embedded") or {}).get("contacts") or []


def ms_agent_uuid(lead: dict) -> str | None:
    """ID контрагента МС, если сделку создал amgroup из МойСклада; иначе None."""
    # Главный признак - enum_id (метку могут переименовать); метка - только если enum нет.
    enum_id = amo_service.get_custom_field_enum_id(lead, FIELD_CREATED_BY)
    if enum_id is not None:
        if enum_id != ENUM_CREATED_BY_MS:
            return None
    elif str(amo_service.get_custom_field_value(lead, FIELD_CREATED_BY) or "").strip().casefold() != "из мойсклад":
        return None
    agent = str(amo_service.get_custom_field_value(lead, FIELD_MS_AGENT_UUID) or "").strip()
    return agent if _UUID_RE.match(agent) else None


def _age_min(lead: dict) -> float | None:
    try:
        return (time.time() - int(lead.get("created_at"))) / 60
    except (TypeError, ValueError):
        return None


def _is_closed(lead: dict) -> bool:
    return int(lead.get("status_id") or 0) in (STATUS_SUCCESS, STATUS_CLOSED_LOST)


def needs_heal(lead: dict) -> bool:
    """Сделка из МС без контакта, открытая и старше порога. Чистая функция."""
    if _contacts(lead) or _is_closed(lead) or ms_agent_uuid(lead) is None:
        return False
    age = _age_min(lead)
    return age is not None and age >= AMGROUP_CONTACT_HEAL_MIN_AGE_MIN


def _phones(raw: str | None) -> list[str]:
    """Номера контрагента цифрами (8 -> 7 у российских). В поле МС бывает несколько
    номеров через запятую или точку с запятой; меньше 10 цифр - не номер."""
    out = []
    for part in re.split(r"[,;]", raw or ""):
        digits = ru_8_to_7(digits_only(part))
        if len(digits) >= 10 and digits not in out:
            out.append(digits)
    return out


def _email_key(raw: str | None) -> str:
    value = (raw or "").strip().casefold()
    return value if "@" in value else ""


def _field_values(contact: dict, code: str) -> list[str]:
    out = []
    for f in contact.get("custom_fields_values") or []:
        if f.get("field_code") == code:
            out.extend(str(v.get("value") or "") for v in f.get("values") or [])
    return out


def _contact_matches(contact: dict, phones: list[str], email: str) -> bool:
    """Совпадение по значению поля: телефон - по последним 10 цифрам, почта - целиком."""
    tails = {p[-10:] for p in phones}
    for value in _field_values(contact, _PHONE_FIELD_CODE):
        digits = digits_only(value)
        if len(digits) >= 10 and digits[-10:] in tails:
            return True
    if email:
        return any(v.strip().casefold() == email for v in _field_values(contact, _EMAIL_FIELD_CODE))
    return False


async def _find_candidates(phones: list[str], email: str) -> set[int] | None:
    """id контактов, у которых совпал телефон или почта. None - amo не ответил хотя
    бы на один поиск: неполная выдача может спрятать второго кандидата, а это уже
    не «ровно один». Телефон ищем цифрами, как amgroup_lead_builder."""
    queries = list(phones) + ([email] if email else [])
    found: set[int] = set()
    for query in queries:
        contacts = await amo_service.find_contacts_by_query(query, limit=_CONTACT_SEARCH_LIMIT)
        if contacts is None:
            return None
        for contact in contacts:
            cid = contact.get("id")
            if cid and _contact_matches(contact, phones, email):
                found.add(int(cid))
    return found


# ════════════════ одна сделка ════════════════

async def heal_lead(lead_id: int | str, *, source: str = "scan") -> str:
    """Привязать контакт к сделке amgroup, если он находится однозначно. Возвращает исход
    (константы выше). Сделку всегда перечитывает сама - чужому снимку не верит."""
    if not AMGROUP_CONTACT_HEAL_ENABLED:
        return DISABLED
    lid = int(lead_id)
    if _handled(lid):
        return ALREADY_HANDLED
    if lid in _in_flight:
        return IN_FLIGHT
    _in_flight.add(lid)
    try:
        outcome = await _heal(lid)
        if outcome == LINKED or (
            outcome == ALREADY_LINKED and source == "contact_wait" and not AMGROUP_CONTACT_HEAL_DRY_RUN
        ):
            # Распределение ждало контакт и сдалось (_contact_wait_loop) - зовём заново,
            # в том числе когда amgroup привязал сам, но позже его короткого ожидания.
            # Повторный заход идемпотентен: уже распределённую сделку он не тронет.
            await _redistribute(lid)
    finally:
        _in_flight.discard(lid)
    log = logger.warning if outcome in _FINAL or outcome.startswith("unknown") else logger.info
    log("amgroup_contact_heal %s (%s): %s%s", lid, source, outcome,
        " [сухой режим]" if AMGROUP_CONTACT_HEAL_DRY_RUN else "")
    return outcome


async def _heal(lid: int) -> str:
    lead = await amo_service.get_lead_full(lid, with_=("contacts",))
    if not lead or not lead.get("id"):
        return UNKNOWN_LEAD
    if _contacts(lead):
        return ALREADY_LINKED
    if _is_closed(lead):
        return CLOSED
    agent_uuid = ms_agent_uuid(lead)
    if agent_uuid is None:
        return NOT_MS_LEAD
    age = _age_min(lead)
    if age is None or age < AMGROUP_CONTACT_HEAL_MIN_AGE_MIN:
        return TOO_YOUNG

    agent = await ms_client.get(f"entity/counterparty/{agent_uuid}")
    if not agent:
        return UNKNOWN_MS
    phones = _phones(agent.get("phone"))
    email = _email_key(agent.get("email"))
    if not phones and not email:
        return await _call_human(lead, NO_IDENTIFIERS, [])

    candidates = await _find_candidates(phones, email)
    if candidates is None:
        return UNKNOWN_SEARCH
    if len(candidates) != 1:
        return await _call_human(lead, NO_CANDIDATE if not candidates else AMBIGUOUS, sorted(candidates))
    contact_id = next(iter(candidates))

    if AMGROUP_CONTACT_HEAL_DRY_RUN:
        _dry_log(lid, DRY_RUN_WOULD_LINK, f"привязали бы контакт {contact_id}")
        await _dry_alert(lead, f"Привязали бы контакт {AMO_CONTACT_URL.format(contact_id)}")
        return DRY_RUN_WOULD_LINK

    # Перечитываем прямо перед записью: amgroup мог успеть, пока мы ходили в склад.
    fresh = await amo_service.get_lead_full(lid, with_=("contacts",))
    if not fresh or not fresh.get("id"):
        return UNKNOWN_LEAD
    if _contacts(fresh):
        return ALREADY_LINKED

    result = await amo_service.link_contact_to_lead(lid, contact_id, is_main=True)
    # Проверяем чтением при любом ответе: после таймаута привязка могла и пройти.
    check = await amo_service.get_lead_full(lid, with_=("contacts",))
    linked_ids = {int(c.get("id")) for c in _contacts(check or {}) if c.get("id")}
    if contact_id in linked_ids:
        await amo_service.add_note(
            lid,
            "Контакт привязан автоматически: amgroup создал сделку из заказа МойСклада, "
            f"но не привязал контакт. Найден по телефону или почте контрагента: {AMO_CONTACT_URL.format(contact_id)}",
        )
        _remember(lid, LINKED)
        return LINKED
    if linked_ids:
        # Пока мы привязывали, контакт привязал кто-то другой (amgroup или человек).
        return ALREADY_LINKED
    if check is None or not check.get("id"):
        if result.get("ok"):
            return await _call_human(lead, LINK_UNVERIFIED, [contact_id])
        return UNKNOWN_LINK
    if result.get("ok") or not result.get("retryable"):
        return await _call_human(lead, LINK_FAILED, [contact_id])
    return UNKNOWN_LINK


async def _redistribute(lid: int) -> None:
    try:
        await lead_distribution.process_lead_distribution(lid, source="contact_heal")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("amgroup_contact_heal %s: распределение после привязки упало", lid)


def _dry_log(lid: int, outcome: str, text: str) -> None:
    if (lid, outcome) in _dry_logged:
        return
    _dry_logged.add((lid, outcome))
    logger.warning("amgroup_contact_heal %s [сухой режим]: %s", lid, text)


_HUMAN_TEXT = {
    NO_IDENTIFIERS: "у контрагента МойСклада нет ни телефона, ни почты",
    NO_CANDIDATE: "контакт с телефоном или почтой контрагента МойСклада в amoCRM не найден",
    AMBIGUOUS: "подходят несколько контактов",
    LINK_FAILED: "amoCRM не принял привязку найденного контакта",
    LINK_UNVERIFIED: "привязку отправили, но проверить её чтением сделки не удалось",
}


async def _call_human(lead: dict, outcome: str, contact_ids: list[int]) -> str:
    """Примечание в сделку и один алерт в тех.чат. Контакт не создаём."""
    lid = int(lead["id"])
    reason = _HUMAN_TEXT[outcome]
    contacts_line = ", ".join(AMO_CONTACT_URL.format(c) for c in contact_ids)
    if AMGROUP_CONTACT_HEAL_DRY_RUN:
        _dry_log(lid, outcome, f"позвали бы человека: {reason} {contacts_line}".strip())
        await _dry_alert(
            lead, f"Позвали бы человека: {reason}." + (f"\nКандидаты: {contacts_line}" if contacts_line else ""),
        )
        return outcome

    await amo_service.add_note(
        lid,
        f"⚠️ amgroup не привязал контакт к сделке, автоматически привязать не вышло: {reason}."
        + (f" Кандидаты: {contacts_line}." if contacts_line else "")
        + " Привяжите контакт вручную - без него не создаётся накладная СДЭК.",
    )
    text = (
        f"⚠️ Сделка без контакта: amgroup не привязал контакт{_order_suffix(lead)}.\n"
        f"Автоматически не привязали: {reason}.\n"
        + (f"Кандидаты: {contacts_line}\n" if contacts_line else "")
        + f"{AMO_LEAD_URL.format(lid)}\nКонтакт не создавали, привяжите вручную."
    )
    await _send_alert(lid, text)
    _remember(lid, outcome)
    return outcome


def _order_suffix(lead: dict) -> str:
    order = str(amo_service.get_custom_field_value(lead, FIELD_ORDER_NUMBER) or "").strip()
    return f" (заказ МС {order})" if order else ""


async def _send_alert(lid: int, text: str) -> bool:
    try:
        d = alerts.decide("amgroup_contact_heal", legacy_text=text, values={}, keep_text=True)
        if d is None:
            return True
        return bool(await telegram_bot.send_alert(d.text, **d.send_kwargs()))
    except Exception:
        logger.exception("amgroup_contact_heal %s: алерт не отправился", lid)
        return False


def _load_dry_state() -> None:
    global _dry_alerted_loaded
    if _dry_alerted_loaded:
        return
    _dry_alerted_loaded = True
    data = read_json(_DRY_STATE_PATH, default=[])
    if isinstance(data, list):
        _dry_alerted.update(str(x) for x in data)


async def _dry_alert(lead: dict, verdict: str) -> None:
    """Сухой режим с AMGROUP_CONTACT_HEAL_DRY_RUN_ALERTS: решение в тех.чат, один раз на
    сделку (отметки на диске отдельно от боевого состояния - боевой режим их не видит)."""
    if not AMGROUP_CONTACT_HEAL_DRY_RUN_ALERTS:
        return
    lid = int(lead["id"])
    _load_dry_state()
    if str(lid) in _dry_alerted:
        return
    text = (
        f"🧪 Сухой режим, в amo ничего не записано.\n"
        f"Сделка без контакта: amgroup не привязал контакт{_order_suffix(lead)}.\n"
        f"{verdict}\n{AMO_LEAD_URL.format(lid)}"
    )
    if not await _send_alert(lid, text):
        return
    _dry_alerted.add(str(lid))
    try:
        write_json_atomic(_DRY_STATE_PATH, sorted(_dry_alerted)[-_STATE_CAP:])
    except Exception:
        logger.exception("amgroup_contact_heal: не записался %s", _DRY_STATE_PATH)


# ════════════════ входы: фоновый проход и хвост ожидания распределения ════════════════

async def check_once() -> dict:
    """Один фоновый проход. amo_answered=False - выборка сорвалась, проход пропущен
    (это не «сделок без контакта нет»)."""
    now = int(time.time())
    ts_from = now - AMGROUP_CONTACT_HEAL_LOOKBACK_H * 3600
    ts_to = now - AMGROUP_CONTACT_HEAL_MIN_AGE_MIN * 60
    leads = await amo_service.get_leads_created_between(ts_from, ts_to, with_=("contacts",))
    if leads is None:
        logger.warning("amgroup_contact_heal: amoCRM не отдал сделки за окно - проход пропущен")
        return {"amo_answered": False, "candidates": 0, "outcomes": {}}

    candidates = [int(l["id"]) for l in leads if l.get("id") and needs_heal(l) and not _handled(int(l["id"]))]
    outcomes: dict[str, int] = {}
    for idx, lid in enumerate(candidates[:_PER_PASS_CAP]):
        if idx:
            await asyncio.sleep(_PAUSE_SEC)
        outcome = await heal_lead(lid, source="scan")
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
    if candidates:
        logger.info(
            "amgroup_contact_heal: сделок в окне %s, без контакта %s, исходы %s",
            len(leads), len(candidates), outcomes,
        )
    return {"amo_answered": True, "candidates": len(candidates), "outcomes": outcomes}


def schedule_after_contact_wait(lead: dict) -> None:
    """Зовёт lead_distribution._contact_wait_loop, когда контакт не появился за его
    короткое ожидание. Ждём порог AMGROUP_CONTACT_HEAL_MIN_AGE_MIN от создания
    сделки и пробуем привязать. Фоновый проход - страховка поверх этого."""
    if not AMGROUP_CONTACT_HEAL_ENABLED or ms_agent_uuid(lead) is None:
        return
    try:
        created = int(lead.get("created_at"))
    except (TypeError, ValueError):
        created = int(time.time())
    delay = max(0.0, created + AMGROUP_CONTACT_HEAL_MIN_AGE_MIN * 60 + _SCHEDULE_MARGIN_S - time.time())
    task = asyncio.create_task(_delayed_heal(int(lead["id"]), delay))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _delayed_heal(lead_id: int, delay: float) -> None:
    try:
        await asyncio.sleep(delay)
        await heal_lead(lead_id, source="contact_wait")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("amgroup_contact_heal %s: отложенная привязка упала", lead_id)


async def _loop() -> None:
    import api
    api.set_api_priority(api.API_PRIORITY_BACKGROUND)
    await asyncio.sleep(60)
    while True:
        try:
            if not api.skip_if_congested("amgroup_contact_heal"):
                await check_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("amgroup_contact_heal: проход не удался")
        await asyncio.sleep(AMGROUP_CONTACT_HEAL_INTERVAL_SEC)


async def init() -> None:
    global _task
    if not AMGROUP_CONTACT_HEAL_ENABLED:
        logger.info("amgroup_contact_heal выключен (AMGROUP_CONTACT_HEAL_ENABLED=0)")
        return
    _task = asyncio.create_task(_loop())
    logger.info(
        "amgroup_contact_heal запущен: раз в %s с, окно %s ч, порог %s мин, режим %s",
        AMGROUP_CONTACT_HEAL_INTERVAL_SEC, AMGROUP_CONTACT_HEAL_LOOKBACK_H,
        AMGROUP_CONTACT_HEAL_MIN_AGE_MIN, "сухой" if AMGROUP_CONTACT_HEAL_DRY_RUN else "боевой",
    )


async def shutdown() -> None:
    """Отложенные привязки просто снимаем: сделку подберёт фоновый проход после старта."""
    global _task
    tasks = [t for t in (_task, *_bg_tasks) if t is not None and not t.done()]
    for t in tasks:
        t.cancel()
    for t in tasks:
        try:
            await t
        except (asyncio.CancelledError, Exception):
            pass
    _task = None
