"""SLA-уведомления в Telegram: клиент написал и не получил ответа N минут.

По тому же принципу, что uis_missed_call (алерт в чат ОП), но триггер другой —
не разовое событие, а ТАЙМЕР с состоянием:

  клиент написал  →  ждём WAZZUP_SLA_MINUTES  →  если ответа так и нет
  и сейчас окно 12:00–19:00 МСК  →  один алерт в ТГ отделу продаж.

Источник — вебхук Wazzup (POST /wazzup/<secret>). Почему Wazzup, а НЕ лента amo:
в ленте сделки служебные записи Wazzup (ошибка доставки WABA-шаблона
«=== SYSTEM WZ ===») ошибочно считаются входящими → прошлая попытка через задачи
amo плодила мусор. В вебхуке Wazzup входящее клиентское сообщение (messages[],
isEcho=false) и ошибка/статус доставки (statuses[]) — РАЗНЫЕ массивы, поэтому
служебные записи структурно не попадают в «клиент написал».

Что считаем ответом: ЛЮБОЕ исходящее по этой беседе (isEcho=true — оператор/бот/
CRM) сбрасывает таймер. Компромисс: если менеджер перезвонил ЗВОНКОМ (без
сообщения), Wazzup исходящего не увидит → придёт лишний алерт. Учёт «Успешного
звонка» — возможная доработка, не MVP.

Состояние — in-memory (персиста в проекте нет): при рестарте ожидания теряются,
это приемлемо (алерт — про «прямо сейчас», а не исторический долг). Записи чистятся
по TTL, чтобы словарь не рос.
"""

import asyncio
import datetime
import logging

import httpx

import amo_service
import telegram_bot
from api import BASE_URL
from waybill_config import (
    WAZZUP_API_KEY,
    WAZZUP_API_URL,
    WAZZUP_ENSURE_WEBHOOK,
    WAZZUP_RESPONSIBLE_TIMEOUT_S,
    WAZZUP_SLA_ENABLED,
    WAZZUP_SLA_MINUTES,
    WAZZUP_SLA_POLL_INTERVAL_S,
    WAZZUP_SLA_WINDOW_END_H,
    WAZZUP_SLA_WINDOW_START_H,
    WAZZUP_WEBHOOK_URL,
)
# Адресат и логика тега ответственного — общие с пропущенными звонками.
from tg_recipients import (
    NOTIFY_CHAT_ID,
    NOTIFY_THREAD_ID,
    mentions_for,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))

# --- состояние ожиданий -------------------------------------------------------
# ключ: (channelId, chatId) → беседа. Одна беседа = один клиент в одном канале.
#   waiting_since : monotonic-время последнего клиентского сообщения БЕЗ ответа
#   wall_since    : то же в стенных часах (для текста и TTL)
#   text          : текст последнего клиентского сообщения (обрезанный)
#   chat_type     : whatsapp/telegram/… — для текста
#   contact_name  : имя из вебхука (если есть)
#   alerted       : уже отправили алерт по этому ожиданию (не спамим)
_pending: dict[tuple[str, str], dict] = {}

# TTL записи без активности (сброс, чтобы словарь не рос): 12 часов.
_TTL_SECONDS = 12 * 3600
# Ограничение длины сниппета клиентского текста в алерте.
_SNIPPET_MAX = 160

_loop_task: asyncio.Task | None = None
_enabled = False


def is_enabled() -> bool:
    return _enabled


# ---------------------------------------------------------------------------
# Приём вебхука Wazzup
# ---------------------------------------------------------------------------

def handle_webhook(payload: dict) -> None:
    """Разбирает тело вебхука Wazzup и обновляет состояние ожиданий.

    Только messages[] (statuses[] — доставка/ошибки, НЕ сообщения — игнор):
      входящее (isEcho != true / status=="inbound")  → старт/обновление ожидания;
      исходящее (isEcho == true)                       → сброс ожидания (ответили).
    """
    if not isinstance(payload, dict):
        return
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return

    for m in messages:
        if not isinstance(m, dict):
            continue
        channel_id = str(m.get("channelId") or "")
        chat_id = str(m.get("chatId") or "")
        if not chat_id:
            continue
        key = (channel_id, chat_id)

        if _is_outbound(m):
            # Ответили (оператор/бот/CRM) → снимаем ожидание.
            if _pending.pop(key, None) is not None:
                logger.info("Wazzup SLA: ответ по беседе %s — ожидание снято", chat_id)
            continue

        # Входящее от клиента. Таймер считаем от ПЕРВОГО неотвеченного сообщения:
        # если беседа уже висит без ответа — waiting_since и alerted НЕ трогаем
        # (иначе частые сообщения клиента бесконечно сдвигали бы порог и алерт не
        # сработал бы никогда), обновляем только сниппет/имя. Сброс — только на
        # исходящем (см. ветку выше, где ожидание снимается целиком).
        contact = m.get("contact") if isinstance(m.get("contact"), dict) else {}
        snapshot = {
            "text": _snippet(m.get("text")),
            "chat_type": str(m.get("chatType") or ""),
            "contact_name": str((contact or {}).get("name") or ""),
            "chat_id": chat_id,
        }
        existing = _pending.get(key)
        if existing is not None:
            existing.update(snapshot)  # ожидание продолжается — время не сбрасываем
        else:
            _pending[key] = {
                "waiting_since": _monotonic(),
                "wall_since": _now_msk(),
                "alerted": False,
                **snapshot,
            }
            logger.info("Wazzup SLA: клиент написал (беседа %s, канал %s) — таймер пошёл", chat_id, m.get("chatType"))


def _is_outbound(m: dict) -> bool:
    """True — исходящее (наш ответ). Wazzup: isEcho=true у всех исходящих
    (оператор/бот/CRM). Фолбэк по status, если isEcho не пришёл."""
    if m.get("isEcho") is True:
        return True
    status = str(m.get("status") or "").lower()
    if status and status != "inbound":
        # sent/delivered/read/error — это про исходящее сообщение.
        return True
    return False


def _snippet(text) -> str:
    s = " ".join(str(text or "").split())
    return s[:_SNIPPET_MAX] + ("…" if len(s) > _SNIPPET_MAX else "")


# ---------------------------------------------------------------------------
# Фоновый цикл: проверка «висит ≥ N минут» в окне 12–19
# ---------------------------------------------------------------------------

async def init() -> None:
    global _loop_task, _enabled
    if not WAZZUP_SLA_ENABLED:
        logger.info("Wazzup SLA: ВЫКЛЮЧЕН (WAZZUP_SLA_ENABLED пуст/false)")
        return
    if NOTIFY_CHAT_ID is None:
        logger.warning("Wazzup SLA: NOTIFY_CHAT_ID не задан — алерты некуда слать, ВЫКЛЮЧЕН")
        return
    _enabled = True
    await _maybe_ensure_webhook_subscription()
    _loop_task = asyncio.create_task(_poll_loop())
    logger.info(
        "Wazzup SLA: включён — порог %s мин, окно %02d:00–%02d:00 МСК, опрос %s сек",
        WAZZUP_SLA_MINUTES, WAZZUP_SLA_WINDOW_START_H, WAZZUP_SLA_WINDOW_END_H,
        WAZZUP_SLA_POLL_INTERVAL_S,
    )


async def _maybe_ensure_webhook_subscription() -> None:
    """Прописывает наш webhooksUri в аккаунте Wazzup (PATCH /v3/webhooks),
    подписка messagesAndStatuses. Гейт WAZZUP_ENSURE_WEBHOOK (по умолчанию выкл) —
    боевой, перезаписывает webhooksUri аккаунта. Идемпотентно: если уже наш —
    ничего не меняем. Ошибку логируем, но фичу не валим."""
    if not WAZZUP_ENSURE_WEBHOOK:
        logger.info("Wazzup SLA: авто-подписка вебхука выключена (WAZZUP_ENSURE_WEBHOOK) — прописать вручную")
        return
    if not (WAZZUP_API_KEY and WAZZUP_WEBHOOK_URL):
        logger.warning("Wazzup SLA: нет WAZZUP_API_KEY/WAZZUP_WEBHOOK_URL — подписку не оформляю")
        return
    headers = {"Authorization": f"Bearer {WAZZUP_API_KEY}", "Content-Type": "application/json"}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            cur = await client.get(f"{WAZZUP_API_URL}/webhooks", headers=headers)
            if cur.status_code == 200 and (cur.json() or {}).get("webhooksUri") == WAZZUP_WEBHOOK_URL:
                logger.info("Wazzup SLA: webhooksUri уже наш — подписка не менялась")
                return
            body = {
                "webhooksUri": WAZZUP_WEBHOOK_URL,
                "subscriptions": {
                    "messagesAndStatuses": True,
                    "contactsAndDealsCreation": False,
                    "channelsUpdates": False,
                    "templateStatus": False,
                },
            }
            r = await client.patch(f"{WAZZUP_API_URL}/webhooks", headers=headers, json=body)
            if r.status_code in (200, 201, 204):
                logger.info("Wazzup SLA: подписка вебхука оформлена (messagesAndStatuses)")
            else:
                logger.warning("Wazzup SLA: подписка вебхука не удалась: HTTP %s %s", r.status_code, r.text[:300])
    except Exception:
        logger.exception("Wazzup SLA: ошибка оформления подписки вебхука")


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
    logger.info("Wazzup SLA stopped")


def _in_window(now: datetime.datetime | None = None) -> bool:
    now = now or _now_msk()
    return WAZZUP_SLA_WINDOW_START_H <= now.hour < WAZZUP_SLA_WINDOW_END_H


async def _poll_loop() -> None:
    threshold = WAZZUP_SLA_MINUTES * 60
    while True:
        try:
            await asyncio.sleep(WAZZUP_SLA_POLL_INTERVAL_S)
            await _sweep(threshold)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Wazzup SLA: ошибка в цикле проверки")


async def _sweep(threshold_s: int) -> None:
    now_mono = _monotonic()
    in_window = _in_window()

    # чистка протухших + сбор просроченных
    due: list[tuple[tuple[str, str], dict]] = []
    for key, st in list(_pending.items()):
        age = now_mono - st["waiting_since"]
        if age >= _TTL_SECONDS:
            _pending.pop(key, None)
            continue
        if not st["alerted"] and age >= threshold_s:
            due.append((key, st))

    if not due:
        return
    if not in_window:
        # Вне окна не досылаем (решение Кати). Ждём следующего прохода в окне.
        return

    for key, st in due:
        try:
            lead_id, responsible_id = await _resolve_lead_safe(st["chat_id"])
            mentions = mentions_for(responsible_id)
            text = _build_message(st, lead_id, mentions)
            ok = await telegram_bot.send_alert(
                text, parse_mode="HTML",
                chat_id=NOTIFY_CHAT_ID, message_thread_id=NOTIFY_THREAD_ID,
            )
            st["alerted"] = True
            logger.info(
                "Wazzup SLA: алерт %s (беседа %s lead=%s)",
                "отправлен" if ok else "НЕ отправлен", st["chat_id"], lead_id or "—",
            )
        except Exception:
            logger.exception("Wazzup SLA: ошибка отправки алерта (беседа %s)", st.get("chat_id"))


# ---------------------------------------------------------------------------
# Поиск сделки и текст
# ---------------------------------------------------------------------------

_CLOSED_STATUS_IDS = {142, 143}


async def _resolve_lead_safe(chat_id: str):
    """(lead_id, responsible_user_id) открытой сделки по chat_id (для WhatsApp это
    телефон). Best-effort с таймаутом WAZZUP_RESPONSIBLE_TIMEOUT_S (10с): не нашли/
    не успели → (None, None) → тегаем всю смену. Открытая = не 142/143, самая
    свежая по работе (как в uis_missed_call)."""
    try:
        return await asyncio.wait_for(_resolve_lead(chat_id), timeout=WAZZUP_RESPONSIBLE_TIMEOUT_S)
    except asyncio.TimeoutError:
        logger.warning(
            "Wazzup SLA: сделка/ответственный не определены за %sс — тегаем смену (беседа %s)",
            WAZZUP_RESPONSIBLE_TIMEOUT_S, chat_id,
        )
        return None, None
    except Exception:
        logger.exception("Wazzup SLA: поиск сделки не удался (беседа %s)", chat_id)
        return None, None


async def _resolve_lead(chat_id: str):
    if not chat_id:
        return None, None
    leads = await amo_service.find_leads_by_query(chat_id)
    open_leads = [ld for ld in leads if ld.get("status_id") not in _CLOSED_STATUS_IDS]
    if not open_leads:
        return None, None
    best = max(open_leads, key=lambda ld: (ld.get("updated_at") or 0, ld.get("id") or 0))
    return best.get("id"), best.get("responsible_user_id")


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(st: dict, lead_id, mentions: str) -> str:
    wait_min = WAZZUP_SLA_MINUTES
    lines = [
        f"⏳ Клиент ждёт ответа {wait_min}+ мин — ответьте",
        mentions,
    ]
    chan = st.get("chat_type") or ""
    who = st.get("contact_name") or ""
    ident = st.get("chat_id") or ""
    head = " ".join(x for x in [f"💬 {_esc(chan)}" if chan else "", f"{_esc(who)}" if who else ""] if x).strip()
    if head:
        lines.append(head)
    if ident:
        lines.append(f"📞 {_esc(ident)}")
    if st.get("text"):
        lines.append(f"«{_esc(st['text'])}»")
    if lead_id:
        lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# время (обёртки — чтобы не звать Date.now-эквивалент напрямую в тестах)
# ---------------------------------------------------------------------------

def _monotonic() -> float:
    import time
    return time.monotonic()


def _now_msk() -> datetime.datetime:
    return datetime.datetime.now(tz=_MSK)
