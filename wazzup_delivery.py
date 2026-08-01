"""Контроль доставки Wazzup: алерт в технический чат, когда сообщение НЕ дошло.

Зачем (просьба Кати 01.08.2026): менеджер пишет из карточки amo или инбокса,
интерфейс рисует «отправлено», а клиент сообщение не получает. Техподдержка
Wazzup кивает на amo. Разведка 01.08 показала, что гадать не нужно: Wazzup сам
присылает нам статус доставки, мы его просто выбрасывали (`wazzup_sla` явно
игнорирует statuses[] — там таймер про ДРУГОЕ, про молчание менеджера).

Ловим два разных случая:

  1. `error` — Wazzup честно говорит «не доставлено» и называет причину
     (UNKNOWN_ERROR, BAD_CONTACT, 24_HOURS_EXCEEDED). Приходит сразу, тем же
     dateTime, что и само сообщение → алерт мгновенный, без таймера.
  2. `sent`, который так и не стал `delivered` за WAZZUP_UNDELIVERED_MINUTES —
     «ушло в никуда». Здесь нужен таймер и состояние.

⚠️ Случай 2 считаем ТОЛЬКО по каналам из WAZZUP_UNDELIVERED_CHAT_TYPES
(по умолчанию whatsapp/wapi). Причина фактическая: в срезе за 31.07 у Telegram
`delivered` не приходит ВООБЩЕ (15 исходящих: 13 read, 2 sent, ни одного
delivered) — там `sent` висит, пока клиент не прочитает, и таймер по нему
означал бы «клиент не открыл чат», а не «мы не доставили». Для Telegram
остаётся случай 1 (ошибки ловятся полностью).

Состояние — in-memory, как в wazzup_sla: рестарт теряет незакрытые ожидания,
и это осознанно. Алерт про «прямо сейчас»; историческую картину даёт панель,
куда те же вебхуки уезжают через wazzup_forward.

Антиспам: при массовом сбое Wazzup ошибки сыплются лавиной. Больше
WAZZUP_DELIVERY_BURST_MAX алертов за окно — шлём одну строку «дальше молчу» и
до конца окна замолкаем. Чат должен остаться читаемым, а полная картина всё
равно лежит в панели.

Куда шлём: WAZZUP_DELIVERY_CHAT_ID, по умолчанию — технический чат
(TG_ALLOWED_CHAT_ID, тот же, куда /print и сторож). Менеджеров и чат ОП пока
НЕ трогаем (решение Кати: «в телеграм пока технический только»).
"""

import asyncio
import datetime
import logging

import httpx

import amo_service
import telegram_bot
from api import BASE_URL
from waybill_config import (
    TEAM_INGEST_TOKEN,
    TEAM_INGEST_URL,
    WAZZUP_DELIVERY_BURST_MAX,
    WAZZUP_DELIVERY_BURST_WINDOW_S,
    WAZZUP_DELIVERY_CHAT_ID,
    WAZZUP_DELIVERY_ENABLED,
    WAZZUP_DELIVERY_POLL_INTERVAL_S,
    WAZZUP_DELIVERY_THREAD_ID,
    WAZZUP_RESPONSIBLE_TIMEOUT_S,
    WAZZUP_SUMMARY_HOUR_MSK,
    WAZZUP_SUMMARY_HOUR_MSK_ENABLED,
    WAZZUP_UNDELIVERED_CHAT_TYPES,
    WAZZUP_UNDELIVERED_MINUTES,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))

# messageId → сведения об исходящем, ждущем доставки.
#   sent_mono    : monotonic, когда мы узнали о сообщении (для таймера)
#   chat_type / chat_id / contact_name / author_name / text / msg_type
#   alerted      : алерт по этому сообщению уже ушёл (не дублируем)
_tracked: dict[str, dict] = {}

# Сколько держим запись без развязки (доставки/ошибки), прежде чем забыть.
_TTL_SECONDS = 6 * 3600
_SNIPPET_MAX = 200

# Статусы, снимающие ожидание: дошло.
_OK_STATUSES = {"delivered", "read"}

# Человеческие названия кодов ошибок Wazzup — чтобы в чате было видно, что чинить,
# а что вообще не наша беда. Незнакомый код печатаем как есть.
_ERROR_HINTS = {
    "24_HOURS_EXCEEDED": "окно 24 часа закрыто: обычным сообщением уже нельзя, только шаблон",
    "BAD_CONTACT": "номера нет в WhatsApp или очень старая версия",
    "UNKNOWN_ERROR": "сбой на стороне Wazzup или Meta, повод писать в их поддержку",
    "NOT_ENOUGH_MONEY": "закончились деньги на канале",
    "CHANNEL_BLOCKED": "канал заблокирован",
    "CHANNEL_NOT_FOUND": "канал не найден",
    "TEMPLATE_NOT_FOUND": "шаблон не найден или не одобрен",
}

# --- антиспам ---------------------------------------------------------------
_burst_window_start: float = 0.0
_burst_count: int = 0
_burst_suppressed: int = 0

_loop_task: asyncio.Task | None = None
_enabled = False


def is_enabled() -> bool:
    return _enabled


async def init() -> None:
    global _loop_task, _enabled
    if not WAZZUP_DELIVERY_ENABLED:
        logger.info("Wazzup доставка: ВЫКЛЮЧЕНА (WAZZUP_DELIVERY_ENABLED)")
        return
    _enabled = True
    _loop_task = asyncio.create_task(_poll_loop())
    logger.info(
        "Wazzup доставка: включена — ошибки сразу, «sent без delivered» через %s мин "
        "(каналы %s), опрос %s сек, чат %s",
        WAZZUP_UNDELIVERED_MINUTES,
        ",".join(sorted(WAZZUP_UNDELIVERED_CHAT_TYPES)) or "—",
        WAZZUP_DELIVERY_POLL_INTERVAL_S,
        WAZZUP_DELIVERY_CHAT_ID if WAZZUP_DELIVERY_CHAT_ID is not None else "технический (по умолчанию)",
    )


async def shutdown() -> None:
    global _loop_task, _enabled
    _enabled = False
    if _loop_task is not None:
        _loop_task.cancel()
        try:
            await _loop_task
        except asyncio.CancelledError:
            pass
        _loop_task = None
    logger.info("Wazzup доставка: остановлена")


# ---------------------------------------------------------------------------
# Приём вебхука
# ---------------------------------------------------------------------------

def handle_webhook(payload: dict) -> None:
    """Разбирает вебхук Wazzup. Не блокирует и не бросает — вызывается из
    обработчика, который обязан быстро ответить Wazzup 200.

    messages[] — исходящее (isEcho=true) регистрируем и/или сразу алертим,
    если status уже `error` (Wazzup кладёт ошибку прямо в тело сообщения).
    statuses[] — развязка по messageId: delivered/read снимают ожидание,
    error поднимает алерт по уже известным нам данным сообщения.
    """
    if not (_enabled and isinstance(payload, dict)):
        return
    try:
        _handle_messages(payload.get("messages"))
        _handle_statuses(payload.get("statuses"))
    except Exception:
        logger.exception("Wazzup доставка: ошибка разбора вебхука")


def _handle_messages(messages) -> None:
    if not isinstance(messages, list):
        return
    for m in messages:
        if not isinstance(m, dict):
            continue
        if not _is_outbound(m):
            continue
        message_id = str(m.get("messageId") or "").strip()
        if not message_id:
            continue

        contact = m.get("contact") if isinstance(m.get("contact"), dict) else {}
        info = _tracked.get(message_id) or {}
        info.update({
            "chat_type": str(m.get("chatType") or "").lower(),
            "chat_id": str(m.get("chatId") or ""),
            "contact_name": str((contact or {}).get("name") or ""),
            "contact_phone": str((contact or {}).get("phone") or ""),
            "author_name": str(m.get("authorName") or ""),
            "msg_type": str(m.get("type") or ""),
            "text": _snippet(m.get("text")),
            "alerted": info.get("alerted", False),
            "sent_mono": info.get("sent_mono") or _monotonic(),
        })
        status = str(m.get("status") or "").lower()

        if status == "error":
            err = m.get("error") if isinstance(m.get("error"), dict) else {}
            _tracked[message_id] = info
            _alert_bg(message_id, info, reason="error", error=err)
            continue

        if status in _OK_STATUSES:
            _tracked.pop(message_id, None)
            continue

        # sent (или статуса ещё нет) — ждём развязки, но только там, где
        # delivered у нас реально бывает (см. докстринг про Telegram).
        if info["chat_type"] in WAZZUP_UNDELIVERED_CHAT_TYPES:
            _tracked[message_id] = info


def _handle_statuses(statuses) -> None:
    if not isinstance(statuses, list):
        return
    for s in statuses:
        if not isinstance(s, dict):
            continue
        message_id = str(s.get("messageId") or "").strip()
        status = str(s.get("status") or "").lower()
        if not (message_id and status):
            continue

        if status in _OK_STATUSES:
            _tracked.pop(message_id, None)
            continue

        if status == "error":
            info = _tracked.get(message_id)
            if info is None:
                # Сообщения мы не видели (рестарт / статус пришёл раньше messages[]).
                # Алертим по тому, что есть в самом статусе: лучше скупой алерт,
                # чем молчание про недоставленное.
                info = {
                    "chat_type": str(s.get("chatType") or "").lower(),
                    "chat_id": str(s.get("chatId") or ""),
                    "contact_name": "",
                    "contact_phone": "",
                    "author_name": "",
                    "msg_type": "",
                    "text": "",
                    "alerted": False,
                    "sent_mono": _monotonic(),
                }
                _tracked[message_id] = info
            err = s.get("error") if isinstance(s.get("error"), dict) else {}
            _alert_bg(message_id, info, reason="error", error=err)


def _is_outbound(m: dict) -> bool:
    """True — исходящее (наш ответ: оператор, бот или CRM). Та же логика, что в
    wazzup_sla: isEcho у Wazzup стоит у всех исходящих, статус — фолбэк."""
    if m.get("isEcho") is True:
        return True
    status = str(m.get("status") or "").lower()
    return bool(status) and status != "inbound"


def _snippet(text) -> str:
    if not isinstance(text, str):
        return ""
    t = " ".join(text.split())
    return t if len(t) <= _SNIPPET_MAX else t[: _SNIPPET_MAX - 1] + "…"


# ---------------------------------------------------------------------------
# Таймер «отправлено, но не доставлено»
# ---------------------------------------------------------------------------

async def _poll_loop() -> None:
    threshold = WAZZUP_UNDELIVERED_MINUTES * 60
    while True:
        try:
            await asyncio.sleep(WAZZUP_DELIVERY_POLL_INTERVAL_S)
            await _sweep(threshold)
            await _maybe_daily_summary()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Wazzup доставка: ошибка в цикле проверки")


# --- вечерняя сводка --------------------------------------------------------
# Дата (МСК), за которую сводку уже отправили: один раз в сутки, без персиста.
# Рестарт в час сводки → возможен повтор; дубль сводки безобиднее пропуска.
_summary_sent_for: str = ""


async def _maybe_daily_summary() -> None:
    if not WAZZUP_SUMMARY_HOUR_MSK_ENABLED:
        return
    global _summary_sent_for
    now = _now_msk()
    if now.hour != WAZZUP_SUMMARY_HOUR_MSK:
        return
    day = now.date().isoformat()
    if _summary_sent_for == day:
        return
    _summary_sent_for = day
    try:
        data = await _fetch_daily()
        if data is None:
            logger.warning("Wazzup доставка: сводка не собрана — панель не ответила")
            return
        await _send(_build_summary(data))
        logger.info("Wazzup доставка: вечерняя сводка отправлена за %s", day)
    except Exception:
        logger.exception("Wazzup доставка: вечерняя сводка не отправилась")


async def _fetch_daily() -> dict | None:
    """Сводку считает панель (у неё вся история), мы только рисуем текст.
    Свои счётчики в памяти не держим: они обнуляются на каждом деплое."""
    if not (TEAM_INGEST_URL and TEAM_INGEST_TOKEN):
        return None
    url = TEAM_INGEST_URL.rstrip("/") + "/daily"
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(
                url,
                headers={"X-Ingest-Token": TEAM_INGEST_TOKEN},
                params={"hours": 24},
            )
        if r.status_code != 200:
            logger.warning("Wazzup доставка: сводка — панель ответила %s", r.status_code)
            return None
        return r.json()
    except Exception:
        logger.exception("Wazzup доставка: запрос сводки в панель упал")
        return None


def _build_summary(d: dict) -> str:
    lines = ["📊 <b>Доставка Wazzup за сутки</b>"]
    lines.append(f"Исходящих {d.get('outbound', 0)}, входящих {d.get('inbound', 0)}")

    for chan, statuses in sorted((d.get("by_channel") or {}).items()):
        parts = []
        for key, label in (("delivered", "дошло"), ("read", "прочитано"),
                           ("sent", "висит sent"), ("error", "ошибок")):
            if statuses.get(key):
                parts.append(f"{label} {statuses[key]}")
        if parts:
            lines.append(f"💬 {_esc(chan)}: " + ", ".join(parts))

    errors = d.get("errors") or {}
    if errors:
        lines.append("⚠️ Причины ошибок:")
        for code, n in sorted(errors.items(), key=lambda kv: -kv[1]):
            hint = _ERROR_HINTS.get(code, "")
            lines.append(f"  • {_esc(code)} — {n}" + (f" ({_esc(hint)})" if hint else ""))
    else:
        lines.append("✅ Ошибок доставки не было")

    delay = d.get("delivery_delay_median_s")
    if delay is not None:
        lines.append(f"⏱ Медиана «отправлено → доставлено»: {_human_delay(delay)}")
    else:
        lines.append("⏱ Задержку пока не посчитать: нет пар «отправлено → доставлено» за сутки")
    return "\n".join(lines)


def _human_delay(seconds: float) -> str:
    s = max(0.0, float(seconds))
    if s < 60:
        return f"{s:.0f} сек"
    if s < 3600:
        return f"{s / 60:.1f} мин"
    return f"{s / 3600:.1f} ч"


async def _sweep(threshold_s: int) -> None:
    now_mono = _monotonic()
    due: list[tuple[str, dict]] = []
    for message_id, info in list(_tracked.items()):
        age = now_mono - info["sent_mono"]
        if age >= _TTL_SECONDS:
            _tracked.pop(message_id, None)
            continue
        if not info.get("alerted") and age >= threshold_s:
            due.append((message_id, info))

    for message_id, info in due:
        await _alert(message_id, info, reason="stuck", error=None)


# ---------------------------------------------------------------------------
# Алерт
# ---------------------------------------------------------------------------

def _alert_bg(message_id: str, info: dict, reason: str, error: dict | None) -> None:
    """Алерт из обработчика вебхука — фоном, чтобы не задерживать ответ Wazzup."""
    try:
        asyncio.get_running_loop().create_task(_alert(message_id, info, reason, error))
    except RuntimeError:
        # Нет живого цикла (тесты, синхронный вызов) — молча пропускаем.
        logger.debug("Wazzup доставка: нет event loop, алерт %s не отправлен", message_id)


async def _alert(message_id: str, info: dict, reason: str, error: dict | None) -> None:
    if info.get("alerted"):
        return
    # Помечаем СРАЗУ и запись НЕ удаляем: Wazzup спокойно шлёт тот же вебхук
    # повторно, и удалённая запись завела бы второй алерт про то же сообщение.
    # Чистит запись TTL в _sweep.
    info["alerted"] = True
    try:
        allowed, first_suppressed = _burst_allow()
        lead_id, _ = await _resolve_lead_safe(info.get("chat_id") or info.get("contact_phone") or "")

        # Примечание в сделку пишем ДО антиспама и независимо от него (го Кати
        # 01.08): чат можно приглушить, а менеджер должен увидеть в своей сделке,
        # что клиент сообщения не получил. Заглушка антиспама режет только ТГ.
        if reason == "error" and lead_id:
            await _note_in_lead_safe(lead_id, info, error)

        if not allowed:
            if first_suppressed:
                await _send(
                    f"🚫 Wazzup: недоставленных больше {WAZZUP_DELIVERY_BURST_MAX} за "
                    f"{WAZZUP_DELIVERY_BURST_WINDOW_S // 60} мин — похоже на массовый сбой. "
                    f"Дальше в этом окне молчу, чтобы не залить чат: подробности в панели и логе."
                )
            return

        await _send(_build_message(info, reason, error, lead_id))
        logger.info(
            "Wazzup доставка: алерт (%s) по сообщению %s, беседа %s",
            reason, message_id, info.get("chat_id") or "—",
        )
    except Exception:
        logger.exception("Wazzup доставка: не смогла отправить алерт %s", message_id)


async def _send(text: str) -> bool:
    return await telegram_bot.send_alert(
        text,
        parse_mode="HTML",
        chat_id=WAZZUP_DELIVERY_CHAT_ID,          # None → технический чат по умолчанию
        message_thread_id=WAZZUP_DELIVERY_THREAD_ID,
    )


async def _note_in_lead_safe(lead_id, info: dict, error: dict | None) -> None:
    """Примечание в сделку: «клиент этого сообщения не получил».

    Зачем отдельно от ТГ-алерта: алерт видит только технический чат, а работать
    с клиентом дальше менеджеру — он должен найти причину в своей же карточке.

    Best-effort: amo лёг или сделка не открылась → пишем в лог и живём дальше,
    ТГ-алерт от этого не страдает (он уходит следующей строкой)."""
    try:
        await asyncio.wait_for(
            amo_service.add_note(lead_id, _build_note(info, error)),
            timeout=WAZZUP_RESPONSIBLE_TIMEOUT_S,
        )
        logger.info("Wazzup доставка: примечание о недоставке в сделке %s", lead_id)
    except asyncio.TimeoutError:
        logger.warning("Wazzup доставка: примечание в сделку %s не успело за %sс",
                       lead_id, WAZZUP_RESPONSIBLE_TIMEOUT_S)
    except Exception:
        logger.exception("Wazzup доставка: примечание в сделку %s не записалось", lead_id)


def _build_note(info: dict, error: dict | None) -> str:
    """Текст примечания. Без HTML: лента amo его не рендерит, а показывает как есть.
    Первая строка — суть, дальше причина и что делать."""
    chan = info.get("chat_type") or "мессенджер"
    lines = [f"⚠️ Сообщение НЕ доставлено клиенту ({chan})"]

    code = str((error or {}).get("error") or "").strip()
    hint = _ERROR_HINTS.get(code, "")
    if code:
        lines.append(f"Причина: {code}" + (f" — {hint}" if hint else ""))
    if code in _NOTE_ADVICE:
        lines.append(_NOTE_ADVICE[code])

    author = info.get("author_name") or ""
    if author:
        lines.append("Отправляла автоматика amo" if author.lower() == "admin" else f"Отправлял: {author}")
    if info.get("text"):
        lines.append(f"Текст: «{info['text']}»")
    lines.append("Запись сделал контроль доставки Wazzup (amo_fix_fields).")
    return "\n".join(lines)


# Что делать менеджеру — только там, где совет однозначный. Молчим, если действие
# неочевидно: пустой совет лучше вредного.
_NOTE_ADVICE = {
    "24_HOURS_EXCEEDED": "Что делать: написать шаблоном или связаться другим каналом.",
    "BAD_CONTACT": "Что делать: проверить номер и написать в Telegram либо позвонить.",
    "UNKNOWN_ERROR": "Что делать: отправить сообщение повторно; если снова упадёт — сказать Кате.",
    "NOT_ENOUGH_MONEY": "Что делать: сообщить Кате, на канале кончились деньги.",
}


def _burst_allow() -> tuple[bool, bool]:
    """(можно ли слать алерт, надо ли предупредить о молчании).

    Внутри окна пропускаем WAZZUP_DELIVERY_BURST_MAX алертов. На первом
    подавленном отдаём True вторым элементом — это сигнал отправить ОДНУ строку
    «дальше молчу», чтобы лавина при сбое Wazzup не залила чат. Остальные
    подавленные уходят молча, их видно в панели и в логе."""
    global _burst_window_start, _burst_count, _burst_suppressed
    now = _monotonic()
    if now - _burst_window_start >= WAZZUP_DELIVERY_BURST_WINDOW_S:
        _burst_window_start = now
        _burst_count = 0
        _burst_suppressed = 0
    if _burst_count < WAZZUP_DELIVERY_BURST_MAX:
        _burst_count += 1
        return True, False
    _burst_suppressed += 1
    return False, _burst_suppressed == 1


_CLOSED_STATUS_IDS = {142, 143}


async def _resolve_lead_safe(query: str):
    """(lead_id, responsible_user_id) открытой сделки по телефону/chat_id.
    Best-effort: не нашли, не успели, amo лёг → (None, None), алерт уходит без
    ссылки. Своя копия, а не импорт из wazzup_sla — чтобы правки здесь не
    задевали боевой SLA-модуль."""
    if not query:
        return None, None
    try:
        return await asyncio.wait_for(_resolve_lead(query), timeout=WAZZUP_RESPONSIBLE_TIMEOUT_S)
    except asyncio.TimeoutError:
        logger.warning("Wazzup доставка: сделка не определена за %sс (%s)", WAZZUP_RESPONSIBLE_TIMEOUT_S, query)
        return None, None
    except Exception:
        logger.exception("Wazzup доставка: поиск сделки не удался (%s)", query)
        return None, None


async def _resolve_lead(query: str):
    leads = await amo_service.find_leads_by_query(query)
    open_leads = [ld for ld in leads if ld.get("status_id") not in _CLOSED_STATUS_IDS]
    if not open_leads:
        return None, None
    best = max(open_leads, key=lambda ld: (ld.get("updated_at") or 0, ld.get("id") or 0))
    return best.get("id"), best.get("responsible_user_id")


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(info: dict, reason: str, error: dict | None, lead_id) -> str:
    if reason == "error":
        lines = ["🚫 <b>Сообщение НЕ доставлено</b>"]
    else:
        lines = [
            f"⏱ <b>Сообщение висит «отправлено» {WAZZUP_UNDELIVERED_MINUTES}+ мин</b> — доставки нет"
        ]

    chan = info.get("chat_type") or ""
    who = info.get("contact_name") or ""
    head = " · ".join(x for x in [f"💬 {_esc(chan)}" if chan else "", _esc(who)] if x)
    if head:
        lines.append(head)

    ident = info.get("chat_id") or info.get("contact_phone") or ""
    if ident:
        lines.append(f"📞 {_esc(ident)}")

    author = info.get("author_name") or ""
    if author:
        # «Admin» у Wazzup — это отправка из автоматики (Salesbot/CRM), не человек.
        who_sent = "автоматика amo" if author.lower() == "admin" else _esc(author)
        lines.append(f"👤 отправил: {who_sent}")

    if info.get("msg_type") == "wapi_template":
        lines.append("📄 WABA-шаблон")

    if error:
        code = str(error.get("error") or "").strip()
        desc = str(error.get("description") or "").strip()
        hint = _ERROR_HINTS.get(code, "")
        line = f"⚠️ <b>{_esc(code or 'ошибка')}</b>"
        if hint:
            line += f" — {_esc(hint)}"
        elif desc:
            line += f" — {_esc(desc)}"
        lines.append(line)

    if info.get("text"):
        lines.append(f"«{_esc(info['text'])}»")

    if lead_id:
        lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# время (обёртки — чтобы тесты могли подменить)
# ---------------------------------------------------------------------------

def _monotonic() -> float:
    import time
    return time.monotonic()


def _now_msk() -> datetime.datetime:
    return datetime.datetime.now(tz=_MSK)
