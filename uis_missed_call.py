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
from tg_recipients import (
    NOTIFY_CHAT_ID,
    NOTIFY_THREAD_ID,
    mentions_for,
)

logger = logging.getLogger("uvicorn")

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

        # Сделку (+ответственного) добираем с таймаутом 5с: не ответил amo вовремя
        # (медленный API / завал очереди) → шлём БЕЗ ссылки и тегаем всю смену,
        # не задерживая «срочно».
        try:
            lead_id, responsible_id = await asyncio.wait_for(_find_lead(phone), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("UIS пропущенный: поиск сделки >5с — без ссылки, тегаем смену (call=%s)", call_id)
            lead_id, responsible_id = None, None
        text = _build_message(phone, name, lead_id, mentions_for(responsible_id))
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


# Закрытые = системные статусы «успех»/«отказ», общие для ВСЕХ воронок
# (см. amo_service: 142/143 имеют одинаковый id во всех воронках). Та же логика,
# что у моста Jivo (_find_open_lead_id).
_CLOSED_STATUS_IDS = {142, 143}


async def _find_lead(phone: str):
    """(lead_id, responsible_user_id) ОТКРЫТОЙ сделки по телефону (не закрытую и не
    случайную — жалоба МОП: ссылка вела на рандомную/старую сделку). Открытая =
    status_id не из 142/143. Несколько открытых → самую свежую ПО РАБОТЕ (max
    updated_at, тай-брейк по id). Открытых нет → (None, None) (лучше без ссылки и
    тег смены, чем ссылка на закрытую)."""
    if not phone:
        return None, None
    try:
        leads = await amo_service.find_leads_by_query(phone)
        open_leads = [ld for ld in leads if ld.get("status_id") not in _CLOSED_STATUS_IDS]
        if not open_leads:
            return None, None
        best = max(open_leads, key=lambda ld: (ld.get("updated_at") or 0, ld.get("id") or 0))
        return best.get("id"), best.get("responsible_user_id")
    except Exception:
        logger.exception("UIS пропущенный: поиск сделки по телефону не удался")
        return None, None


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(phone: str, name: str, lead_id, mentions: str) -> str:
    lines = [
        "🔴 ПРОПУЩЕННЫЙ звонок — срочно связаться с клиентом",
        mentions,
    ]
    if phone:
        lines.append(f"📞 {_esc(phone)}")
    if name and name != phone:  # новый номер — без имени
        lines.append(f"👤 {_esc(name)}")
    if lead_id:
        lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)
