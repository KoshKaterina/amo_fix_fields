"""Уведомление в Telegram о пропущенном ВХОДЯЩЕМ звонке — по вебхуку UIS.

UIS (кабинет → Уведомления → HTTP-уведомления, событие «Потерянный звонок») шлёт
GET на наш /uis/<secret> с нативными макросами:
  contact_phone_number — телефон звонящего
  contact_full_name    — имя (если UIS знает контакт), иначе пусто
  call_session_id      — id звонка (для дедупа)
  virtual_phone_number, notification_time — доп.

Шлём в чат ОП:
  ПРОПУЩЕННЫЙ звонок, срочно связаться с клиентом
  <теги менеджеров на смене>  <телефон>  <имя>  <ссылка на сделку>

Почему по вебхуку UIS, а не по тегу «пропущенный»: тег ставится позже (когда UIS
создал сделку) И вешается в т.ч. на ИСХОДЯЩИЕ непозвоны → ложные «перезвони».
Вебхук «Потерянный звонок» прилетает в момент разрыва и это именно входящий.

Ссылку на сделку добираем поиском по телефону в amo (best-effort): нашли —
кликабельная; новый номер / сделки ещё нет — шлём без ссылки и без имени (не ждём).
Дедуп по call_session_id (защита от ретраев UIS). Работа — в фоне, эндпоинт
отвечает 200 сразу (UIS ждёт быстрый ответ, иначе ретраит 4 раза).

Шлём в супергруппу ОП (NOTIFY_CHAT_ID), в топик РОЗНИЦА (NOTIFY_THREAD_ID).

⚠️ ВРЕМЕННОЕ (уточнить перед закреплением):
  • MANAGERS_ON_SHIFT — фикс.список хендлов. TODO: динамика «кто на смене».
  • NOTIFY_THREAD_ID сменить топик — взять новый thread_id из логов catch-all
    (thread_id=… по сообщению в нужном топике).
"""

import asyncio
import logging
from collections import deque

import amo_service
import telegram_bot
from api import BASE_URL

logger = logging.getLogger("uvicorn")

# --- ⚠️ ВРЕМЕННОЕ (уточнить перед закреплением) -------------------------------
MANAGERS_ON_SHIFT = "@offf1cer @egorkonsss @kathrina_bistraya"
# Супергруппа ОП «Store [Отдел продаж]», топик РОЗНИЦА
# (thread_id=2, из ссылки t.me/c/3680811996/2/…). None → General.
NOTIFY_CHAT_ID = -1003680811996
NOTIFY_THREAD_ID: int | None = 2
# ------------------------------------------------------------------------------

_bg_tasks: set = set()
_seen_ids: set = set()
_seen_order: deque = deque()
_SEEN_CAP = 5000


def _is_new(call_id: str) -> bool:
    """True — звонок новый (слать). False — уже видели (ретрай UIS). Пустой id не
    дедупим: лучше задублить «срочно», чем потерять."""
    if not call_id:
        return True
    if call_id in _seen_ids:
        return False
    _seen_ids.add(call_id)
    _seen_order.append(call_id)
    if len(_seen_order) > _SEEN_CAP:
        _seen_ids.discard(_seen_order.popleft())
    return True


def notify_bg(params: dict) -> None:
    """Планирует фон и сразу возвращает — эндпоинт отвечает UIS 200 мгновенно."""
    task = asyncio.create_task(_apply(params))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(params: dict) -> None:
    try:
        phone = (params.get("contact_phone_number") or "").strip()
        name = (params.get("contact_full_name") or "").strip()
        call_id = (params.get("call_session_id") or "").strip()

        if not _is_new(call_id):
            logger.info("UIS пропущенный: дубль call_session_id=%s — пропускаю", call_id)
            return

        # Ссылку на сделку добираем с таймаутом 5с: не ответил amo вовремя
        # (медленный API / завал очереди) → шлём БЕЗ ссылки, не задерживая «срочно».
        try:
            lead_id = await asyncio.wait_for(_find_lead_id(phone), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("UIS пропущенный: поиск сделки >5с — шлём без ссылки (call=%s)", call_id)
            lead_id = None
        text = _build_message(phone, name, lead_id)
        ok = await telegram_bot.send_alert(
            text, parse_mode="HTML",
            chat_id=NOTIFY_CHAT_ID, message_thread_id=NOTIFY_THREAD_ID,
        )
        logger.info(
            "UIS пропущенный: алерт %s (тел=%s lead=%s call=%s)",
            "отправлен" if ok else "НЕ отправлен", phone or "—", lead_id or "—", call_id or "—",
        )
    except Exception:
        logger.exception("UIS пропущенный: ошибка обработки (call=%s)", params.get("call_session_id"))


async def _find_lead_id(phone: str):
    """Best-effort: сделка по телефону для кликабельной ссылки. Нет — None."""
    if not phone:
        return None
    try:
        leads = await amo_service.find_leads_by_query(phone)
        return leads[0]["id"] if leads else None
    except Exception:
        logger.exception("UIS пропущенный: поиск сделки по телефону не удался")
        return None


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(phone: str, name: str, lead_id) -> str:
    lines = [
        "🔴 ПРОПУЩЕННЫЙ звонок — срочно связаться с клиентом",
        MANAGERS_ON_SHIFT,
    ]
    if phone:
        lines.append(f"📞 {_esc(phone)}")
    if name and name != phone:  # новый номер — без имени
        lines.append(f"👤 {_esc(name)}")
    if lead_id:
        lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)
