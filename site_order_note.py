"""Заметка в сделку: данные клиента так, как он ввёл их на сайте (29.10.2026).

Повод: старый клиент оформил заказ на tangemshop.ru, а в amoCRM его контакт назывался
«Новый контакт по звонку с +7…» (завела телефония). amgroup привязывает сделку к
существующему контакту по телефону и имя из МойСклада не переписывает, поэтому менеджер
видел в сделке чужое имя. Наш мост сайт -> МойСклад (woocommerce-sklad) реальное имя в
контрагенте тоже не перезаписывает, если оно уже «настоящее» по его правилам.

Что делает модуль, по одной сделке (note_lead):
1. Сделка создана из МойСклада («Заказ создан» = «Из МойСклад»), есть «ID Заказа».
2. Заказ МС -> доп.поле «Клиент с сайта» (его пишет woocommerce-sklad при создании
   заказа: имя, телефон, почта, телеграм, адрес, комментарий). Нет поля - заказ не с
   сайта или создан до этой доработки: молча пропускаем.
3. Примечание в сделку: только данные с сайта (решение Кати 29.10: сравнивать с карточкой
   контакта менеджер будет сам, пометок «совпадает» и «отличается» нет).

⚠️ Молчание - не ответ. МойСклад или amo не ответили (None) - исход «не знаю»: ничего не
пишем, повторим на следующем проходе. Заметка пишется ровно один раз на сделку: отметка
на диске, плюс перед записью перечитываем примечания сделки.

Сухой режим (по умолчанию): все чтения и решение как в боевом, в amo ничего не пишем,
в лог идут только id сделок. Выключен по умолчанию (SITE_ORDER_NOTE_ENABLED).

Персональные данные (имя, телефон, почта, адрес) в лог не пишем - только id.
"""

import asyncio
import logging
import re
import time

import amo_service
import ms_client
from shared.files import read_json, write_json_atomic
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    SITE_ORDER_NOTE_DRY_RUN,
    SITE_ORDER_NOTE_ENABLED,
    SITE_ORDER_NOTE_INTERVAL_SEC,
    SITE_ORDER_NOTE_LOOKBACK_H,
    SITE_ORDER_NOTE_MIN_AGE_SEC,
    SITE_ORDER_NOTE_MS_ATTR,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
)

logger = logging.getLogger("uvicorn")

# Поля сделки, которые заполняет amgroup (id и enum сверены в amgroup_contact_heal).
FIELD_CREATED_BY = 576683         # «Заказ создан»
ENUM_CREATED_BY_MS = 1040195      # «Из МойСклад»
FIELD_ORDER_NUMBER = 576697       # «№ Заказа» - для заметки

NOTE_MARKER = "Данные клиента с сайта"
_UUID_RE = re.compile(r"^[0-9a-fA-F-]{36}$")
_PER_PASS_CAP = 20
_PAUSE_SEC = 1.0
_NOTES_LIMIT = 100

# Исходы note_lead. «Не знаю» (UNKNOWN_*) - повтор на следующем проходе.
WRITTEN = "written"
DRY_RUN_WOULD_WRITE = "dry-run-would-write"
ALREADY_NOTED = "already-noted"
NO_DATA = "no-data"
NOT_MS_LEAD = "not-ms-lead"
CLOSED = "closed"
TOO_YOUNG = "too-young"
DISABLED = "disabled"
IN_FLIGHT = "in-flight"
ALREADY_HANDLED = "already-handled"
UNKNOWN_LEAD = "unknown-lead-read"
UNKNOWN_MS = "unknown-ms"
UNKNOWN_NOTES = "unknown-notes"
UNKNOWN_WRITE = "unknown-write"

_STATE_PATH = "/app/var/site_order_note.json"
_STATE_CAP = 3000
_state: dict[str, dict] = {}
_state_loaded = False
# Сухой режим: решённые сделки живут только в памяти (боевое состояние на диск не пишем),
# чтобы одни и те же сделки не съедали потолок прохода и не повторяли строку в логе.
_dry_seen: set[int] = set()

_in_flight: set[int] = set()
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
        logger.exception("site_order_note: не записался %s", _STATE_PATH)


def _handled(lead_id: int) -> bool:
    _load_state()
    return str(lead_id) in _state or lead_id in _dry_seen


# ════════════════ признаки сделки ════════════════

def _is_ms_lead(lead: dict) -> bool:
    enum_id = amo_service.get_custom_field_enum_id(lead, FIELD_CREATED_BY)
    if enum_id is not None:
        return enum_id == ENUM_CREATED_BY_MS
    label = str(amo_service.get_custom_field_value(lead, FIELD_CREATED_BY) or "")
    return label.strip().casefold() == "из мойсклад"


def order_uuid(lead: dict) -> str | None:
    value = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
    return value if _UUID_RE.match(value) else None


def _is_closed(lead: dict) -> bool:
    return int(lead.get("status_id") or 0) in (STATUS_SUCCESS, STATUS_CLOSED_LOST)


def _age_sec(lead: dict) -> float | None:
    try:
        return time.time() - int(lead.get("created_at"))
    except (TypeError, ValueError):
        return None


def site_client_text(order: dict) -> str | None:
    """Значение доп.поля «Клиент с сайта» заказа МС; None - поля нет или оно пустое."""
    for attr in order.get("attributes") or []:
        if isinstance(attr, dict) and attr.get("id") == SITE_ORDER_NOTE_MS_ATTR:
            value = attr.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def build_note(site_text: str, order_number: str) -> str:
    """Только то, что клиент ввёл на сайте: сверять с карточкой менеджер будет сам."""
    head = NOTE_MARKER + (f" (заказ МС {order_number})" if order_number else "")
    return f"{head}:\n{site_text}"


# ════════════════ одна сделка ════════════════

async def note_lead(lead_id: int | str, *, source: str = "scan") -> str:
    """Записать заметку с данными клиента с сайта. Возвращает исход (константы выше).
    Сделку всегда перечитывает сама - чужому снимку не верит."""
    if not SITE_ORDER_NOTE_ENABLED or not SITE_ORDER_NOTE_MS_ATTR:
        return DISABLED
    lid = int(lead_id)
    if _handled(lid):
        return ALREADY_HANDLED
    if lid in _in_flight:
        return IN_FLIGHT
    _in_flight.add(lid)
    try:
        outcome = await _note(lid)
    finally:
        _in_flight.discard(lid)
    log = logger.warning if outcome.startswith("unknown") else logger.info
    log("site_order_note %s (%s): %s%s", lid, source, outcome, " [сухой режим]" if SITE_ORDER_NOTE_DRY_RUN else "")
    return outcome


async def _note(lid: int) -> str:
    lead = await amo_service.get_lead_full(lid, with_=())
    if not lead or not lead.get("id"):
        return UNKNOWN_LEAD
    if _is_closed(lead):
        return CLOSED
    uuid = order_uuid(lead)
    if not _is_ms_lead(lead) or uuid is None:
        return NOT_MS_LEAD
    age = _age_sec(lead)
    if age is None or age < SITE_ORDER_NOTE_MIN_AGE_SEC:
        return TOO_YOUNG

    order = await ms_client.get(f"entity/customerorder/{uuid}")
    if not order:
        return UNKNOWN_MS
    site_text = site_client_text(order)
    if site_text is None:
        _settle(lid, NO_DATA)
        return NO_DATA

    number = str(amo_service.get_custom_field_value(lead, FIELD_ORDER_NUMBER) or "").strip()
    note = build_note(site_text, number)

    if SITE_ORDER_NOTE_DRY_RUN:
        logger.info("site_order_note %s: записали бы заметку (%s симв.)", lid, len(note))
        _settle(lid, DRY_RUN_WOULD_WRITE)
        return DRY_RUN_WOULD_WRITE

    # Уже писали (отметка на диске потерялась при пересборке контейнера)? Лучше не писать
    # вовсе, чем дважды: сбой чтения примечаний - исход «не знаю».
    notes = await amo_service.get_lead_notes(lid, _NOTES_LIMIT)
    if any(NOTE_MARKER in str((n.get("params") or {}).get("text") or "") for n in notes):
        _settle(lid, ALREADY_NOTED)
        return ALREADY_NOTED

    result = await amo_service.add_note(lid, note)
    if not (isinstance(result, dict) and result.get("ok")):
        return UNKNOWN_WRITE
    _settle(lid, WRITTEN)
    return WRITTEN


def _settle(lid: int, outcome: str) -> None:
    """Исход решён: в боевом режиме - на диск, в сухом - только в память."""
    if SITE_ORDER_NOTE_DRY_RUN:
        _dry_seen.add(lid)
    else:
        _remember(lid, outcome)


# ════════════════ фоновый проход ════════════════

async def check_once() -> dict:
    """Один проход по недавним сделкам. amo_answered=False - выборка сорвалась, проход
    пропущен (это не «подходящих сделок нет»)."""
    now = int(time.time())
    ts_from = now - SITE_ORDER_NOTE_LOOKBACK_H * 3600
    ts_to = now - SITE_ORDER_NOTE_MIN_AGE_SEC
    leads = await amo_service.get_leads_created_between(ts_from, ts_to)
    if leads is None:
        logger.warning("site_order_note: amoCRM не отдал сделки за окно - проход пропущен")
        return {"amo_answered": False, "candidates": 0, "outcomes": {}}

    candidates = [
        int(lead["id"]) for lead in leads
        if lead.get("id") and not _is_closed(lead) and _is_ms_lead(lead)
        and order_uuid(lead) is not None and not _handled(int(lead["id"]))
    ]
    outcomes: dict[str, int] = {}
    for idx, lid in enumerate(candidates[:_PER_PASS_CAP]):
        if idx:
            await asyncio.sleep(_PAUSE_SEC)
        outcome = await note_lead(lid, source="scan")
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
    logger.info("site_order_note: сделок в окне %s, из МС %s, исходы %s", len(leads), len(candidates), outcomes)
    return {"amo_answered": True, "candidates": len(candidates), "outcomes": outcomes}


async def _loop() -> None:
    import api
    api.set_api_priority(api.API_PRIORITY_BACKGROUND)
    await asyncio.sleep(75)
    while True:
        try:
            if not api.skip_if_congested("site_order_note"):
                await check_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("site_order_note: проход не удался")
        await asyncio.sleep(SITE_ORDER_NOTE_INTERVAL_SEC)


async def init() -> None:
    global _task
    if not SITE_ORDER_NOTE_ENABLED:
        logger.info("site_order_note выключен (SITE_ORDER_NOTE_ENABLED=0)")
        return
    if not SITE_ORDER_NOTE_MS_ATTR:
        logger.warning("site_order_note: не задано поле МС (SITE_ORDER_NOTE_MS_ATTR), цикл не запущен")
        return
    _task = asyncio.create_task(_loop())
    logger.info(
        "site_order_note запущен: раз в %s с, окно %s ч, порог %s с, режим %s",
        SITE_ORDER_NOTE_INTERVAL_SEC, SITE_ORDER_NOTE_LOOKBACK_H,
        SITE_ORDER_NOTE_MIN_AGE_SEC, "сухой" if SITE_ORDER_NOTE_DRY_RUN else "боевой",
    )


async def shutdown() -> None:
    global _task
    if _task is not None and not _task.done():
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
    _task = None
