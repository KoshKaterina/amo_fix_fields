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
import datetime
import logging
import time
from collections import deque

import amo_service
import telegram_bot
from api import BASE_URL
from tg_recipients import (
    NOTIFY_CHAT_ID,
    NOTIFY_THREAD_ID,
    ROP_CHAT_ID,
    manager_name,
    missed_call_mentions,
)
from waybill_config import (
    MISSED_CALLBACK_ESCALATE_MINUTES,
    MISSED_CALLBACK_POLL_INTERVAL_S,
    MISSED_CALLBACK_WINDOW_END_H,
    MISSED_CALLBACK_WINDOW_START_H,
    TAG_MISSED_NAME,
    TAG_SUCCESS_CALL_NAME,
)

logger = logging.getLogger("uvicorn")

_bg_tasks: set = set()
_seen_ids: set = set()
_seen_order: deque = deque()
_SEEN_CAP = 5000

_MSK = datetime.timezone(datetime.timedelta(hours=3))

# Ожидания перезвона: сделка → когда по ней пропустили звонок. Живут в памяти, как у
# wazzup_sla: рестарт контейнера их теряет, и это осознанный размен. Алерт «час никто
# не перезвонил» ценен в тот же день, а не как исторический долг.
_callback_pending: dict[int, dict] = {}
_watch_task: asyncio.Task | None = None
# Сутки — потолок жизни ожидания. Дольше держать незачем: вне окна счётчик стоит, а
# позавчерашний непрозвон это уже работа с базой, а не срочное уведомление.
_TTL_SECONDS = 24 * 3600


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
        text = _build_message(phone, name, lead_id, missed_call_mentions(responsible_id))
        ok = await telegram_bot.send_alert(
            text, parse_mode="HTML",
            chat_id=NOTIFY_CHAT_ID, message_thread_id=NOTIFY_THREAD_ID,
        )
        logger.info(
            "UIS пропущенный: алерт %s (тел=%s lead=%s call=%s)",
            "отправлен" if ok else "НЕ отправлен", phone or "—", lead_id or "—", call_id or "—",
        )
        _watch_callback(lead_id, phone, name, responsible_id)
    except Exception:
        logger.exception("UIS пропущенный: ошибка обработки (call=%s)", params.get("call_session_id"))


# ---------------------------------------------------------------------------
# Второй контур: час прошёл, а клиенту так и не перезвонили (Катя 28.08.2026)
# ---------------------------------------------------------------------------
#
# Уведомление уходит не менеджерам, а руководству, в группу «ОП срочные уведомления».
# Разница с первым алертом принципиальная: там событие («перезвоните»), здесь провал
# («не перезвонили»), и адресат другой.
#
# **Перезвон определяем по тегам amo, а не по своей арифметике над звонками.** UIS сам
# вешает «Успешный звонок» на дозвон и «пропущенный» на потерянный (кабинет UIS →
# Интеграция → amoCRM → Телефония → «Тегирование сделок и контактов»), а наш unmiss_tag
# снимает «пропущенный», когда дозвон случился. Значит признак «перезвонили» уже посчитан
# двумя независимыми механизмами, и городить третий счёт звонков не надо.
#
# ⚠️ Отдельно от расчёта «перезвонили за 15 минут» в панели: тот считает по сессиям UIS и
# сейчас врёт (Катя 28.08.2026 - «часто висит как не перезвон, хотя по факту в сделке есть
# перезвон»). Здесь источник другой, поэтому баг витрины на эти алерты не переносится.


def _watch_callback(lead_id, phone: str, name: str, responsible_id) -> None:
    """Ставит сделку на счётчик «перезвонят или нет».

    Без сделки не сторожим ВООБЩЕ: проверить перезвон не по чему, а гадать нельзя -
    ложная эскалация в чат руководства дороже пропущенной.
    """
    if not lead_id or ROP_CHAT_ID is None:
        return
    if lead_id in _callback_pending:
        return  # по этой сделке уже ждём: второй пропущенный счётчик не сдвигает
    _callback_pending[int(lead_id)] = {
        "since": time.monotonic(),
        "phone": phone,
        "name": name,
        "responsible_id": responsible_id,
    }


def _in_window(now: datetime.datetime | None = None) -> bool:
    now = now or datetime.datetime.now(_MSK)
    return MISSED_CALLBACK_WINDOW_START_H <= now.hour < MISSED_CALLBACK_WINDOW_END_H


async def _called_back(lead_id: int) -> bool | None:
    """Перезвонили ли по сделке. None - не смогли прочитать сделку (amo молчит).

    Два признака, любой достаточен: с сделки снят «пропущенный» (это делает unmiss_tag
    по факту дозвона) или на ней появился «Успешный звонок» от UIS.
    """
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
    except Exception:
        logger.exception("UIS перезвон: не прочиталась сделка %s", lead_id)
        return None
    if not lead:
        return None
    if amo_service.has_tag(lead, TAG_SUCCESS_CALL_NAME):
        return True
    return not amo_service.has_tag(lead, TAG_MISSED_NAME)


async def _sweep_callbacks() -> None:
    now = time.monotonic()
    threshold = MISSED_CALLBACK_ESCALATE_MINUTES * 60

    due: list[tuple[int, dict, float]] = []
    for lead_id, st in list(_callback_pending.items()):
        age = now - st["since"]
        if age >= _TTL_SECONDS:
            _callback_pending.pop(lead_id, None)
            continue
        if age >= threshold:
            due.append((lead_id, st, age))

    if not due or not _in_window():
        # Вне окна счётчик не идёт дальше: звонок в 19:50 не повод будить руководство
        # в 20:50. Ожидание остаётся и дождётся утра.
        return

    for lead_id, st, age in due:
        try:
            back = await _called_back(lead_id)
            if back is None:
                continue  # amo не ответил - попробуем на следующем проходе
            _callback_pending.pop(lead_id, None)
            if back:
                continue
            ok = await telegram_bot.send_alert(
                _build_escalation(lead_id, st, int(age // 60)),
                parse_mode="HTML", chat_id=ROP_CHAT_ID,
            )
            logger.info(
                "UIS перезвон: эскалация %s (сделка %s, %s мин без перезвона)",
                "отправлена" if ok else "НЕ отправлена", lead_id, int(age // 60),
            )
        except Exception:
            logger.exception("UIS перезвон: ошибка проверки сделки %s", lead_id)


def _build_escalation(lead_id, st: dict, waited_min: int) -> str:
    """Текст руководству: факт и виновник, без тегов и без ID (правило Кати 03.08.2026)."""
    hours = waited_min / 60
    waited = f"{waited_min} минут" if waited_min < 90 else f"{hours:.0f} часа"
    lines = [
        f"🚨 Клиенту не перезвонили {waited}",
        f"Менеджер: {_esc(manager_name(st.get('responsible_id')))}",
    ]
    who = (st.get("name") or "").strip()
    if who and who != st.get("phone"):
        lines.append(f"Клиент: {_esc(who)}")
    if st.get("phone"):
        lines.append(f"Телефон: {_esc(st['phone'])}")
    lines.append("")
    lines.append(f'<a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)


async def _watch_loop() -> None:
    while True:
        try:
            await asyncio.sleep(MISSED_CALLBACK_POLL_INTERVAL_S)
            await _sweep_callbacks()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("UIS перезвон: ошибка в цикле проверки")


async def init() -> None:
    """Поднимает счётчик перезвонов. Чат руководства не задан → цикл не нужен."""
    global _watch_task
    if ROP_CHAT_ID is None:
        logger.info("UIS перезвон: ROP_ALERT_CHAT_ID не задан - счётчик перезвонов выключен")
        return
    if _watch_task is None:
        _watch_task = asyncio.create_task(_watch_loop())
        logger.info(
            "UIS перезвон: счётчик запущен (порог %s мин, окно %s:00-%s:00 МСК)",
            MISSED_CALLBACK_ESCALATE_MINUTES,
            MISSED_CALLBACK_WINDOW_START_H, MISSED_CALLBACK_WINDOW_END_H,
        )


async def shutdown() -> None:
    global _watch_task
    if _watch_task is not None:
        _watch_task.cancel()
        try:
            await _watch_task
        except asyncio.CancelledError:
            pass
        _watch_task = None


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
