"""Коллтрекинг сайта: клик по номеру на sunscrypt.ru → звонок Sipuni → UTM в сделку amo.

Решение Кати 17.11.2026 (вариант А исследования features/kolltreking-sayt-amo/RESEARCH.md
рабочей папки): один номер, без пула. Сайт сообщает о клике по ссылке tel:, интеграция
сопоставляет клик со входящим звонком по времени и пишет метки в сделку.

Вход. POST /site_call_click, заголовок X-Api-Key (тот же секрет, что у /site_form). Браузер сюда
не ходит: событие принимает WordPress на своём домене и пересылает со своей стороны.
    {"schema": 1, "kind": "tel_click" | "utm_visit", "event_id": "<32 hex>",
     "at": <unix, время приёма сервером WP>, "number": "+79056063678",
     "page_url": "...", "referrer": "...", "client_id": "<ClientID Метрики>",
     "device": "mobile" | "desktop", "utm": {...последнее касание},
     "utm_first": {...первое касание} | {}, "utm_first_at": <unix> | 0}
utm_visit сайт шлёт при входе по ссылке с метками: он нужен повторному клиенту, который
позвонит позже и сам.

Склейка (на каждом закончившемся входящем звонке Sipuni, отвеченном и пропущенном):
  * клики по номерам из CALL_CLICK_NUMBERS в окне [начало звонка - WINDOW, начало + SKEW];
  * ровно один клик и в окне этого клика звонил ровно один человек → «точно»: примечание и,
    в режиме live, UTM в поля сделки, если поля пустые (метки сделки не перетираем);
  * кликов или звонящих больше → «спорно»: только примечание со списком вариантов;
  * кликов нет → повторный клиент: телефон → ClientID из прошлых заявок сайта → последний
    визит с метками за CALL_CLICK_VISIT_DAYS. Только примечание, с пометкой «по прошлому визиту».
Одна запись на звонок и одно примечание о визите на сделку (call_click_store.attributions).

Env:
    CALL_CLICK_MODE=off      off - маршрут 404, ничего не пишем; log - принимаем и пишем журнал;
                             note - плюс примечание в сделку; live - плюс поля UTM
    CALL_CLICK_SECRET=...    по умолчанию SITE_FORM_SECRET
    CALL_CLICK_NUMBERS=+79056063678   номера на Sipuni, клики по остальным не склеиваем
    CALL_CLICK_WINDOW_S=120  сколько секунд клик ждёт звонка
    CALL_CLICK_SKEW_S=15     допуск часов WP и Sipuni в обратную сторону
    CALL_CLICK_VISIT_DAYS=30 давность визита для повторного клиента
    CALL_CLICK_DB_PATH=/app/var/call_click.sqlite3
"""

import asyncio
import datetime as dt
import hmac
import logging
import os
import re
import time
from typing import Awaitable, Callable
from urllib.parse import urlsplit

import amo_service
import api
import call_click_store as store
from shared.phones import digits_only, ru_8_to_7
from shared.timez import MSK

logger = logging.getLogger("uvicorn.error")


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, "")).strip() or default)
    except ValueError:
        logger.error("%s: не целое число - беру %s", name, default)
        return default


MODES = ("off", "log", "note", "live")
CALL_CLICK_MODE = os.getenv("CALL_CLICK_MODE", "off").strip().lower()
if CALL_CLICK_MODE not in MODES:
    CALL_CLICK_MODE = "off"
CALL_CLICK_SECRET = (os.getenv("CALL_CLICK_SECRET") or os.getenv("SITE_FORM_SECRET") or "").strip()
CALL_CLICK_NUMBERS = {
    n.strip() for n in os.getenv("CALL_CLICK_NUMBERS", "+79056063678").split(",") if n.strip()
}
WINDOW_S = _env_int("CALL_CLICK_WINDOW_S", 120)
SKEW_S = _env_int("CALL_CLICK_SKEW_S", 15)
VISIT_DAYS = _env_int("CALL_CLICK_VISIT_DAYS", 30)
LEAD_RETRY_S = _env_int("CALL_CLICK_LEAD_RETRY_S", 60)

UTM_KEYS = ("utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content")
KINDS = ("tel_click", "utm_visit")
MAX_BODY_BYTES = 8192
_EVENT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_CLIENT_ID_RE = re.compile(r"^\d{6,30}$")
_PLACE_RE = re.compile(r"^[a-z0-9#_\-]{1,60}$")
_RATE_PER_MINUTE = _env_int("CALL_CLICK_RATE_PER_MINUTE", 120)
_rate: list[float] = []
_bg: set = set()
_last_cleanup = 0.0


class PayloadError(ValueError):
    pass


def is_enabled() -> bool:
    return CALL_CLICK_MODE != "off" and bool(CALL_CLICK_SECRET)


def secret_ok(key: str) -> bool:
    return bool(CALL_CLICK_SECRET and key) and hmac.compare_digest(CALL_CLICK_SECRET, key)


def init() -> None:
    if not is_enabled():
        logger.info("call_click: выключено (CALL_CLICK_MODE=%s)", CALL_CLICK_MODE)
        return
    store.init_db()
    logger.info("call_click: режим %s, номера %s, окно %s с", CALL_CLICK_MODE,
                ",".join(sorted(CALL_CLICK_NUMBERS)), WINDOW_S)


# ---------------------------------------------------------------------------
# Приём событий сайта
# ---------------------------------------------------------------------------

def _s(value, limit: int) -> str:
    return str(value).strip()[:limit] if isinstance(value, (str, int, float)) else ""


def _url(value) -> str:
    text = _s(value, 1000)
    try:
        parts = urlsplit(text)
    except ValueError:
        return ""
    return text if parts.scheme in ("http", "https") and parts.netloc else ""


def _utm(value) -> dict:
    if not isinstance(value, dict):
        return {}
    return {k: _s(value.get(k), 200) for k in UTM_KEYS if _s(value.get(k), 200)}


def _phone(value) -> str:
    digits = ru_8_to_7(digits_only(_s(value, 32)))
    return "+" + digits if 10 <= len(digits) <= 15 else ""


def clean_event(payload: dict, now: float | None = None) -> dict:
    """Проверка события сайта. На слово сайту не верим: всё режем и проверяем."""
    now = time.time() if now is None else now
    if payload.get("schema") != 1:
        raise PayloadError("bad-schema")
    kind = _s(payload.get("kind"), 16)
    if kind not in KINDS:
        raise PayloadError("bad-kind")
    event_id = _s(payload.get("event_id"), 64).lower()
    if not _EVENT_ID_RE.match(event_id):
        raise PayloadError("bad-event-id")
    try:
        at = float(payload.get("at"))
    except (TypeError, ValueError):
        raise PayloadError("bad-at")
    if not now - 600 <= at <= now + 60:
        raise PayloadError("stale")
    number = _phone(payload.get("number")) if kind == "tel_click" else ""
    if kind == "tel_click" and not number:
        raise PayloadError("bad-number")
    client_id = _s(payload.get("client_id"), 30)
    if not _CLIENT_ID_RE.match(client_id):
        client_id = ""
    utm = _utm(payload.get("utm"))
    if kind == "utm_visit" and not utm:
        raise PayloadError("no-utm")
    try:
        first_at = float(payload.get("utm_first_at") or 0)
    except (TypeError, ValueError):
        first_at = 0.0
    device = _s(payload.get("device"), 16)
    place = _s(payload.get("place"), 60).lower()
    return {
        "kind": kind, "event_id": event_id, "at": at, "number": number, "client_id": client_id,
        "page_url": _url(payload.get("page_url")), "referrer": _url(payload.get("referrer")),
        "device": device if device in ("mobile", "desktop") else "",
        "place": place if _PLACE_RE.match(place) else "",
        "link_text": re.sub(r"\s+", " ", _s(payload.get("link_text"), 60)),
        "utm": utm, "utm_first": _utm(payload.get("utm_first")), "utm_first_at": first_at,
    }


def _allow() -> bool:
    now = time.monotonic()
    while _rate and now - _rate[0] > 60:
        _rate.pop(0)
    if len(_rate) >= _RATE_PER_MINUTE:
        return False
    _rate.append(now)
    return True


async def accept(payload: dict) -> tuple[int, dict]:
    try:
        event = clean_event(payload)
    except PayloadError as exc:
        logger.info("call_click: событие отклонено: %s", exc)
        return 422, {"ok": False, "error": str(exc)}
    if not _allow():
        logger.warning("call_click: превышена частота событий сайта")
        return 429, {"ok": False, "error": "rate-limit"}
    try:
        inserted = await asyncio.to_thread(store.add_event, event)
    except Exception:
        logger.exception("call_click: хранилище недоступно")
        return 503, {"ok": False, "error": "store"}
    logger.info("call_click: %s %s %s в %s, страница %s, метки %s, ClientID %s",
                "принят" if inserted else "повтор", event["kind"], event["number"] or "-",
                _hms(event["at"]), _path(event["page_url"]), _utm_line(event["utm"]) or "-",
                "есть" if event["client_id"] else "нет")
    return 200, {"ok": True}


async def link_form_phone(phone: str, client_id: str) -> None:
    """Заявка сайта дала пару «телефон ↔ ClientID»: так узнаём повторного клиента."""
    if not is_enabled() or not phone or not _CLIENT_ID_RE.match(client_id or ""):
        return
    try:
        await asyncio.to_thread(store.link_phone, _phone(phone), client_id)
    except Exception:
        logger.exception("call_click: связка телефона и ClientID не записалась")


# ---------------------------------------------------------------------------
# Склейка со звонком
# ---------------------------------------------------------------------------

def call_started(call_id: str, phone: str, started_at: int | float | None) -> None:
    """Первое событие входящего звонка Sipuni. Синхронно и быстро, ошибки глотаем."""
    if not is_enabled() or not call_id or not started_at:
        return
    try:
        store.record_call_start(call_id, phone or "", float(started_at))
    except Exception:
        logger.exception("call_click: начало звонка %s не записалось", call_id)


def decide(clicks: list[dict], callers: dict) -> tuple[str, list]:
    """Чистое решение. clicks - клики по нашим номерам в окне звонка, ещё не занятые.
    callers[event_id] - разные звонящие в окне этого клика (сам звонок туда входит).
    → ("sure", [click]) | ("ambiguous", clicks) | ("none", [])."""
    if not clicks:
        return "none", []
    if len(clicks) == 1 and len(callers.get(clicks[0]["event_id"]) or ()) <= 1:
        return "sure", clicks
    return "ambiguous", clicks


def _candidates(call_start: float) -> list[dict]:
    rows = store.clicks_between(call_start - WINDOW_S, call_start + SKEW_S)
    return [r for r in rows if r["number"] in CALL_CLICK_NUMBERS and not r["matched_call"]]


def _phones_in_window(click_at: float) -> set:
    return store.call_phones_between(click_at - SKEW_S, click_at + WINDOW_S)


LeadFinder = Callable[[str], Awaitable[dict | None]]


def schedule_attribution(call_id: str, phone: str, first_ts, find_lead: LeadFinder) -> None:
    """Звонок закончился (тихое окно Sipuni прошло). Работа фоном, алерт и теги не ждут."""
    if not is_enabled() or not call_id or not first_ts:
        return
    task = asyncio.create_task(attribute_call(call_id, phone or "", float(first_ts), find_lead))
    _bg.add(task)
    task.add_done_callback(_bg.discard)


async def attribute_call(call_id: str, phone: str, call_start: float, find_lead: LeadFinder) -> str:
    """Возвращает итог для журнала и тестов: sure / ambiguous / repeat / none / no-lead / skip."""
    global _last_cleanup
    try:
        if time.time() - _last_cleanup > 3600:
            _last_cleanup = time.time()
            await asyncio.to_thread(cleanup)
        clicks = await asyncio.to_thread(_candidates, call_start)
        callers = {}
        for c in clicks:
            callers[c["event_id"]] = await asyncio.to_thread(_phones_in_window, c["at"])
        verdict, chosen = decide(clicks, callers)
        visit = None
        if verdict == "none":
            if not phone:
                return "none"
            client_ids = await asyncio.to_thread(store.client_ids_for_phone, phone)
            visit = await asyncio.to_thread(store.latest_utm_event, client_ids,
                                            call_start - VISIT_DAYS * 86400)
            if not visit:
                logger.info("call_click: звонок %s - кликов в окне нет, прошлых визитов с метками нет", call_id)
                return "none"
            verdict = "repeat"
        if verdict == "sure":
            await asyncio.to_thread(store.mark_matched, chosen[0]["event_id"], call_id)
        logger.info("call_click: звонок %s в %s - %s (кликов в окне %s)", call_id, _hms(call_start),
                    verdict, len(clicks))
        if not phone:
            logger.info("call_click: звонок %s со скрытого номера, сделку не найти", call_id)
            return "no-lead"
        lead = await find_lead(phone.lstrip("+"))
        if not lead and LEAD_RETRY_S > 0:
            await asyncio.sleep(LEAD_RETRY_S)
            lead = await find_lead(phone.lstrip("+"))
        if not lead:
            logger.info("call_click: звонок %s - открытой сделки по номеру нет, метки не записаны", call_id)
            return "no-lead"
        lead_id = int(lead["id"])
        ref = f"visit:{visit['event_id']}" if verdict == "repeat" else f"call:{call_id}"
        if not await asyncio.to_thread(store.claim_attribution, lead_id, ref):
            logger.info("call_click: в сделку %s уже писали (%s)", lead_id, ref)
            return "skip"
        if verdict == "sure":
            note = note_sure(chosen[0]["payload"], call_start)
        elif verdict == "ambiguous":
            note = note_ambiguous([c["payload"] for c in chosen], call_start)
        else:
            note = note_repeat(visit["payload"])
        fields = {}
        if verdict == "sure":
            fields = dict(chosen[0]["payload"]["utm"])
            if chosen[0]["payload"].get("referrer"):
                fields["referrer"] = chosen[0]["payload"]["referrer"]
        await _write(lead_id, ref, verdict, note, fields)
        return verdict
    except Exception:
        logger.exception("call_click: склейка звонка %s не удалась", call_id)
        return "error"


async def _write(lead_id: int, ref: str, verdict: str, note: str, utm: dict) -> None:
    """utm - метки и referrer для стандартных полей «Статистики» сделки (utm_* и REFERRER).
    Пишем только когда метки поставлены у ОДНОЗНАЧНОЙ склейки и в сделке их ещё нет."""
    if CALL_CLICK_MODE == "log":
        logger.info("call_click: сделка %s, %s - режим log, в amo не пишу. Примечание было бы:\n%s",
                    lead_id, verdict, note)
        return
    if not await api.add_note_to_lead(lead_id, note):
        logger.warning("call_click: примечание в сделку %s не легло (%s)", lead_id, ref)
        await asyncio.to_thread(store.release_attribution, lead_id, ref)
        return
    logger.info("call_click: примечание в сделке %s (%s)", lead_id, verdict)
    if CALL_CLICK_MODE != "live" or not any(utm.get(k) for k in UTM_KEYS):
        return
    lead = await amo_service.get_lead_full(lead_id, with_=())
    if lead is None:
        logger.warning("call_click: сделка %s не прочиталась, поля UTM не пишу", lead_id)
        return
    if has_utm(lead):
        logger.info("call_click: в сделке %s метки уже есть, не перетираю (новые в примечании)", lead_id)
        return
    ok = await api.set_lead_utm(lead_id, utm)
    logger.info("call_click: поля UTM сделки %s %s", lead_id, "записаны" if ok else "НЕ записаны")


def has_utm(lead: dict) -> bool:
    for field in lead.get("custom_fields_values") or []:
        if str(field.get("field_code") or "").upper() in {k.upper() for k in UTM_KEYS}:
            if any(str(v.get("value") or "").strip() for v in field.get("values") or []):
                return True
    return False


# ---------------------------------------------------------------------------
# Тексты примечаний. Словаря меток пока нет: пишем переменные с именами.
# ---------------------------------------------------------------------------

def _hms(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, MSK).strftime("%H:%M:%S")


def _dm_hm(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, MSK).strftime("%d.%m %H:%M")


def _path(url: str) -> str:
    if not url:
        return "-"
    parts = urlsplit(url)
    return parts.path or "/"


def _page(url: str) -> str:
    """Адрес страницы без хвоста с метками: метки идут отдельной строкой."""
    if not url:
        return "неизвестна"
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}{parts.path}"


def _utm_line(utm: dict) -> str:
    return ", ".join(f"{k}={utm[k]}" for k in UTM_KEYS if utm.get(k))


def _number(n: str) -> str:
    d = digits_only(n)
    if len(d) == 11 and d[0] == "7":
        return f"+7 {d[1:4]} {d[4:7]}-{d[7:9]}-{d[9:11]}"
    return n or "-"


_DEVICES = {"mobile": "телефон", "desktop": "компьютер"}

# Место клика по номеру (плагин сайта sun-call-tracking, функция place()).
_PLACES = {
    "header-phone": "шапка сайта, номер в шапке",
    "mobile-menu": "мобильное меню",
    "header": "шапка сайта",
    "footer": "подвал сайта",
    "popup": "всплывающее окно",
    "main": "основная часть страницы",
    "page": "страница",
}


def place_label(place: str) -> str:
    if not place:
        return ""
    if place.startswith("#"):
        return f"блок страницы {place}"
    return _PLACES.get(place, place)


def _visit_lines(ev: dict) -> list[str]:
    lines = [f"Страница: {_page(ev.get('page_url', ''))}"]
    if ev.get("place"):
        where = place_label(ev["place"])
        text = ev.get("link_text") or ""
        if re.search(r"[A-Za-zА-Яа-яЁё]", text):
            where += f", кнопка «{text}»"
        lines.append(f"Где нажал номер: {where}")
    utm = ev.get("utm") or {}
    lines.append("Метки визита: " + (_utm_line(utm) or "нет (прямой заход или метки потерялись)"))
    first = ev.get("utm_first") or {}
    if first and first != utm:
        when = f" ({_dm_hm(ev['utm_first_at'])})" if ev.get("utm_first_at") else ""
        lines.append(f"Первый визит с метками{when}: {_utm_line(first)}")
    if ev.get("referrer"):
        lines.append(f"Откуда пришёл: {ev['referrer']}")
    if ev.get("device"):
        lines.append(f"Устройство: {_DEVICES.get(ev['device'], ev['device'])}")
    return lines


def note_sure(ev: dict, call_start: float) -> str:
    delay = max(0, int(round(call_start - ev["at"])))
    return "\n".join([
        "Звонок с сайта (коллтрекинг)",
        f"Нажал номер {_number(ev.get('number', ''))} на сайте в {_hms(ev['at'])}, позвонил через {delay} с.",
        *_visit_lines(ev),
    ])


def note_ambiguous(events: list[dict], call_start: float) -> str:
    lines = [
        "Возможно, звонок с сайта (коллтрекинг, точно не связать)",
        f"За {WINDOW_S // 60} мин до звонка номер на сайте нажимали {len(events)} раз(а) "
        "или звонили несколько человек. Метки в поля не записаны. Варианты:",
    ]
    for i, ev in enumerate(events[:3], 1):
        where = f" ({place_label(ev['place'])})" if ev.get("place") else ""
        lines.append(f"{i}) {_hms(ev['at'])}, {_path(ev.get('page_url', ''))}{where}, "
                     f"{_utm_line(ev.get('utm') or {}) or 'без меток'}")
    return "\n".join(lines)


def note_repeat(ev: dict) -> str:
    return "\n".join([
        "Звонок, возможно, после визита на сайт (коллтрекинг, по прошлому визиту)",
        f"Клика по номеру не было. Этот клиент оставлял заявку на сайте, и с того же браузера "
        f"{_dm_hm(ev['at'])} был визит с метками. Метки в поля не записаны.",
        *_visit_lines(ev),
    ])


def cleanup() -> None:
    if is_enabled():
        store.cleanup(time.time(), click_keep_s=2 * 86400, visit_keep_s=(VISIT_DAYS + 1) * 86400,
                      link_keep_s=365 * 86400)
