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
import alerts
import lead_distribution_routed_store as routed_store
import sipuni_events
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
    SIPUNI_EVENTS_MODE,
    SIPUNI_ROUTING_POLL_S,
    SIPUNI_ROUTING_WAIT_S,
    SIPUNI_SET_TAGS,
    STATUS_NEW_LEAD_ALL,
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

        await _notify_missed(phone, name, call_id)
    except Exception:
        logger.exception("UIS пропущенный: ошибка обработки (call=%s)", params.get("call_session_id"))


# Как называем источник звонка в журнале. Ключ - provider из _notify_missed.
_PROVIDER_LABELS = {"uis": "UIS", "sipuni": "Sipuni"}

# Пометки в алерте для звонка, брошенного до сотрудников (решения Кати 03.11 и 04.11.2026).
_DROPPED_IN_MENU_NOTE = "ℹ️ Клиент бросил трубку на приветствии, до сотрудников звонок не дошёл"
_ROUTING_TIMEOUT_NOTE = "ℹ️ Сделка не распределилась вовремя, поэтому пингуем всех"
# Запись о распределении, сделанная чуть раньше начала звонка, всё равно относится к нему.
_ROUTING_SLACK_S = 60.0
# Ожидания назначения ответственного, которые надо отпустить при остановке приложения.
_routing_waiters: set = set()


async def _notify_missed(
    phone: str, name: str, call_id: str, *, provider: str = "uis",
    dropped_in_menu: bool = False, call_started: float | None = None,
) -> None:
    """Общая часть алерта о пропущенном звонке: сделка по телефону, текст, алерт, счётчик
    перезвона. Её зовут и вход UIS (_apply), и вход Sipuni (_sipuni_on_missed).

    provider нужен для журнала и для способа проверки «перезвонили»: у uis это теги amo,
    у sipuni поток событий звонков (sipuni_events). Дедуп по id звонка делает вызывающий:
    у UIS это _is_new, у Sipuni - сам учёт звонков по call_id.

    dropped_in_menu (только Sipuni): звонок не дошёл до сотрудников. Кого пинговать, решает сделка
    (решение Кати 04.11.2026):
      * сделки по телефону нет: пингуем весь пул пинга по звонку, ответственного ещё нет;
      * сделка уже в работе (этап не «Новый лид»): пингуем её ответственного, как обычно;
      * новая сделка, распределения ещё не было: ждём до SIPUNI_ROUTING_WAIT_S, пока распределение
        назначит ответственного, и пингуем его; не дождались - весь пул.
    В тексте кода к такому алерту добавлена пояснительная строка (шаблон панели её не показывает).
    call_started: Unix-время начала звонка, чтобы отличить распределение этого звонка от старого.
    """
    label = _PROVIDER_LABELS.get(provider, provider)
    # Сделку (+ответственного) добираем с таймаутом 5с: не ответил amo вовремя
    # (медленный API / завал очереди) → шлём БЕЗ ссылки и тегаем всю смену,
    # не задерживая «срочно».
    try:
        row = await asyncio.wait_for(_find_lead_row(phone), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("%s пропущенный: поиск сделки >5с — без ссылки, тегаем смену (call=%s)", label, call_id)
        row = None
    lead_id = row.get("id") if row else None
    responsible_id = row.get("responsible_user_id") if row else None
    ping_all = False
    note = ""
    if dropped_in_menu:
        note = _DROPPED_IN_MENU_NOTE
        if lead_id is None:
            ping_all = True
        elif row.get("status_id") in STATUS_NEW_LEAD_ALL and SIPUNI_ROUTING_WAIT_S > 0:
            assigned = await _wait_for_routing(lead_id, call_id, call_started)
            if assigned is None:
                ping_all = True
                note = note + "\n" + _ROUTING_TIMEOUT_NOTE
            elif assigned:
                responsible_id = assigned
    mentions = missed_call_mentions(None if ping_all else responsible_id)
    text = _build_message(phone, name, lead_id, mentions, note=note)
    d = alerts.decide(
        "missed_call", legacy_text=text, parse_mode="HTML",
        chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID,
        responsible_id=None if ping_all else responsible_id,
        values={
            "теги": mentions,
            "телефон": phone,
            "клиент": name if name and name != phone else "",
            "ссылка_на_сделку": alerts.lead_link(lead_id),
        },
    )
    if d is None:
        logger.info("%s пропущенный: событие выключено в панели (call=%s)", label, call_id or "—")
    else:
        ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
        logger.info(
            "%s пропущенный: алерт %s (тел=%s lead=%s call=%s)",
            label, "отправлен" if ok else "НЕ отправлен", phone or "—", lead_id or "—", call_id or "—",
        )
    _watch_callback(lead_id, phone, name, responsible_id, provider=provider)


async def _wait_for_routing(lead_id, call_id: str, call_started: float | None) -> int | None:
    """Ждёт, пока распределение назначит ответственного новой сделке.

    Возвращает user_id назначенного (запись «распределена» не старше начала звонка), 0 если ждать
    нечего (сделка распределена раньше, ответственный у неё уже есть и берётся из amo), None если
    не дождались за SIPUNI_ROUTING_WAIT_S или приложение останавливается.
    """
    since = (call_started or time.time()) - _ROUTING_SLACK_S
    loop = asyncio.get_running_loop()
    deadline = loop.time() + SIPUNI_ROUTING_WAIT_S
    stop = asyncio.Event()
    _routing_waiters.add(stop)
    logger.info("Sipuni пропущенный: сделка %s новая и ещё не распределена, жду назначения до %.0f с (call=%s)",
                lead_id, SIPUNI_ROUTING_WAIT_S, call_id)
    try:
        while True:
            try:
                rec = await asyncio.to_thread(routed_store.get, lead_id)
            except Exception:
                logger.exception("Sipuni пропущенный: хранилище распределённых не ответило (сделка %s)", lead_id)
                rec = None
            if rec:
                if (rec.get("routed_at") or 0) >= since:
                    assigned = rec.get("assigned_user_id")
                    logger.info("Sipuni пропущенный: сделка %s распределена на %s (call=%s)",
                                lead_id, assigned or "—", call_id)
                    return int(assigned) if assigned else 0
                logger.info("Sipuni пропущенный: сделка %s распределена раньше этого звонка, ждать нечего (call=%s)",
                            lead_id, call_id)
                return 0
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.warning("Sipuni пропущенный: сделка %s не распределилась за %.0f с, пингуем всех (call=%s)",
                               lead_id, SIPUNI_ROUTING_WAIT_S, call_id)
                return None
            try:
                await asyncio.wait_for(stop.wait(), timeout=min(SIPUNI_ROUTING_POLL_S, remaining))
            except asyncio.TimeoutError:
                continue
            logger.info("Sipuni пропущенный: остановка приложения, не жду распределения сделки %s (call=%s)",
                        lead_id, call_id)
            return None
    finally:
        _routing_waiters.discard(stop)


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


def _watch_callback(lead_id, phone: str, name: str, responsible_id, provider: str = "uis") -> None:
    """Ставит сделку на счётчик «перезвонят или нет».

    Без сделки не сторожим ВООБЩЕ: проверить перезвон не по чему, а гадать нельзя -
    ложная эскалация в чат руководства дороже пропущенной.

    provider выбирает способ проверки «перезвонили» (см. _check_called_back): у uis это теги
    сделки, у sipuni - поток событий звонков.
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
        "provider": provider,
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


def _called_back_sipuni(st: dict) -> bool | None:
    """Перезвонили ли по пропущенному Sipuni: по ПОТОКУ ЗВОНКОВ, а не по тегам.

    True - после пропущенного от этого номера был отвеченный звонок с его участием
    (исходящий на номер или входящий от него). False - пропущенный есть, перезвона нет.
    None - записи о пропущенном нет (рестарт, истёк срок): проверить нечем, а ложная
    эскалация дороже пропущенной, поэтому, как и при молчании amo, вердикта не выносим.
    """
    record = sipuni_events.tracker.missed_record(st.get("phone") or "")
    if record is None:
        return None
    return record["called_back_at"] is not None


async def _check_called_back(lead_id: int, st: dict) -> bool | None:
    """Выбор способа проверки перезвона по источнику звонка. У UIS всё как раньше."""
    if st.get("provider") == "sipuni":
        return _called_back_sipuni(st)
    return await _called_back(lead_id)


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
        label = _PROVIDER_LABELS.get(st.get("provider", "uis"), "UIS")
        try:
            back = await _check_called_back(lead_id, st)
            if back is None:
                continue  # amo не ответил - попробуем на следующем проходе
            _callback_pending.pop(lead_id, None)
            if back:
                continue
            waited_min = int(age // 60)
            who = (st.get("name") or "").strip()
            d = alerts.decide(
                "missed_call_no_callback",
                legacy_text=_build_escalation(lead_id, st, waited_min),
                parse_mode="HTML", chat_id=ROP_CHAT_ID,
                values={
                    "сколько_ждали": _waited_words(waited_min),
                    "ответственный": manager_name(st.get("responsible_id")),
                    "клиент": who if who and who != st.get("phone") else "",
                    "телефон": st.get("phone") or "",
                    "ссылка_на_сделку": alerts.lead_link(lead_id),
                },
            )
            if d is None:
                logger.info("%s перезвон: эскалация выключена в панели (сделка %s)", label, lead_id)
                continue
            ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
            logger.info(
                "%s перезвон: эскалация %s (сделка %s, %s мин без перезвона)",
                label, "отправлена" if ok else "НЕ отправлена", lead_id, int(age // 60),
            )
        except Exception:
            logger.exception("%s перезвон: ошибка проверки сделки %s", label, lead_id)


def _waited_words(waited_min: int) -> str:
    """«47 минут» до полутора часов, дальше «2 часа» - одно правило и для текста из кода,
    и для переменной {{сколько_ждали}} шаблона из панели."""
    return f"{waited_min} минут" if waited_min < 90 else f"{waited_min / 60:.0f} часа"


def _build_escalation(lead_id, st: dict, waited_min: int) -> str:
    """Текст руководству: факт и виновник, без тегов и без ID (правило Кати 03.08.2026)."""
    waited = _waited_words(waited_min)
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
    _sipuni_start()
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
    # Алерты, которые ждут распределения сделки, уходят сейчас (пингуем всех): после остановки
    # контейнера ожидание пропало бы вместе с алертом.
    for stop in list(_routing_waiters):
        stop.set()
    await sipuni_events.tracker.drain(8.0)
    sipuni_events.tracker.reset()
    if _watch_task is not None:
        _watch_task.cancel()
        try:
            await _watch_task
        except asyncio.CancelledError:
            pass
        _watch_task = None


# ---------------------------------------------------------------------------
# Вход Sipuni: «События на АТС» (28.10.2026). Рядом с UIS, UIS-вход выше не тронут
# ---------------------------------------------------------------------------
#
# У UIS есть готовое уведомление «Потерянный звонок». У Sipuni его НЕТ: приходят события
# начала, ответа и завершения каждой ноги звонка, а «пропущенный» решает sipuni_events по
# итогу всего call_id (тихое окно, см. его шапку). Дальше работает та же общая часть, что и
# у UIS: _notify_missed (сделка по телефону, текст, alerts.decide, счётчик перезвона).
#
# Режим SIPUNI_EVENTS_MODE: log (по умолчанию) - только строка журнала по каждому событию и
# по каждому решению, ни Telegram, ни amo; live - алерт и счётчик перезвона. Учёт звонков в
# памяти идёт в обоих режимах: так по журналу режима log видно, что БЫЛО БЫ отправлено.


def _sipuni_start() -> None:
    """Подключает решение «пропущенный» к алерту. Зовётся из init() и вместе с событием."""
    sipuni_events.tracker.on_missed = _sipuni_on_missed
    sipuni_events.tracker.on_answered = _sipuni_on_answered


def sipuni_notify_bg(payload: list) -> None:
    """Планирует разбор событий Sipuni в фоне и сразу возвращает: Sipuni нужен быстрый ответ
    {"success": true}, иначе он приостановит отправку событий."""
    task = asyncio.create_task(_sipuni_apply(payload))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _sipuni_apply(payload: list) -> None:
    _sipuni_start()
    for raw in payload or []:
        try:
            ev = sipuni_events.parse_event(raw)
            st, duplicate = sipuni_events.tracker.ingest(ev)
            logger.info("%s%s", sipuni_events.log_line(ev, st), " dup=1" if duplicate else "")
        except Exception:
            logger.exception("Sipuni событие: ошибка обработки")


async def _sipuni_on_missed(missed) -> None:
    """Решение «пропущенный» принято (тихое окно истекло, ни одна нога не отвечена)."""
    dropped = bool(getattr(missed, "dropped_in_menu", False))
    if SIPUNI_EVENTS_MODE != "live":
        logger.info(
            "Sipuni пропущенный: call=%s тел=%s ног=%s к_сотрудникам=%s на_приветствии=%s "
            "статусы=%s - режим log, алерт НЕ шлём",
            missed.call_id, missed.phone or "—", missed.legs,
            getattr(missed, "staff_legs", "—"), "да" if dropped else "нет",
            ",".join(missed.statuses) or "—",
        )
        return
    if not _is_new("sipuni:" + missed.call_id):
        logger.info("Sipuni пропущенный: дубль call_id=%s — пропускаю", missed.call_id)
        return
    try:
        # Цифры без плюса: так телефон приходит от UIS, и поиск сделки в amo проверен именно на
        # этом виде. Имени клиента у Sipuni в событиях нет, ищем только по телефону.
        await _notify_missed(
            missed.phone.lstrip("+"), "", missed.call_id, provider="sipuni",
            dropped_in_menu=dropped, call_started=missed.first_ts,
        )
    except Exception:
        logger.exception("Sipuni пропущенный: ошибка обработки (call=%s)", missed.call_id)
    # Теги после алерта: «срочно» не ждёт amo, а сбой алерта не отменяет теги.
    await _sipuni_tag_missed(missed)


async def _sipuni_on_answered(call) -> None:
    """Звонок ответили (входящий или исходящий), тихое окно прошло: «Успешный звонок»."""
    if SIPUNI_EVENTS_MODE != "live" or not SIPUNI_SET_TAGS:
        logger.info(
            "Sipuni отвеченный: call=%s напр=%s тел=%s - %s, тег НЕ ставлю",
            call.call_id, call.direction, call.phone or "—",
            "режим log" if SIPUNI_EVENTS_MODE != "live" else "SIPUNI_SET_TAGS выключен",
        )
        return
    if not _is_new("sipuni-ok:" + call.call_id):
        return
    try:
        lead_id, _resp = await _find_lead(call.phone.lstrip("+"), statuses=STATUS_NEW_LEAD_ALL)
        if not lead_id:
            logger.info("Sipuni отвеченный: нет новой сделки (этап «Новый лид») по номеру, «%s» не ставлю "
                        "(call=%s)", TAG_SUCCESS_CALL_NAME, call.call_id)
            return
        # add_tag идемпотентен (тег уже есть - запись не делает). «пропущенный» снимет
        # unmiss_tag.py по эху правки сделки.
        result = await amo_service.add_tag(lead_id, TAG_SUCCESS_CALL_NAME)
        logger.info(
            "Sipuni отвеченный: тег «%s» на сделке %s %s (call=%s напр=%s)",
            TAG_SUCCESS_CALL_NAME, lead_id, "поставлен" if result.get("ok") else
            f"НЕ поставлен (status={result.get('status_code')})", call.call_id, call.direction,
        )
    except Exception:
        logger.exception("Sipuni отвеченный: ошибка постановки тега (call=%s)", call.call_id)


async def _sipuni_tag_missed(missed) -> None:
    """«пропущенный» на НОВУЮ сделку по телефону (открытая, этап «Новый лид» или его буфер) и на
    её контакты. Сделка уже в работе или на другом этапе - не тегаем (решение Кати 03.11.2026).

    Тегаем только когда такая сделка есть: контакт без сделки получил бы тег, который некому
    снять (unmiss_tag.py работает от правок сделки). Свежий пропуск сбрасывает «Успешный звонок»
    сделки: иначе тег от прошлого разговора заставил бы unmiss_tag снять «пропущенный»
    сразу после постановки. Возвращает «Успешный звонок» следующий отвеченный звонок.
    """
    if SIPUNI_EVENTS_MODE != "live" or not SIPUNI_SET_TAGS or not missed.phone:
        return
    try:
        lead_id, _resp = await _find_lead(missed.phone.lstrip("+"), statuses=STATUS_NEW_LEAD_ALL)
        if not lead_id:
            logger.info("Sipuni пропущенный: нет новой сделки (этап «Новый лид») по номеру, «%s» не ставлю "
                        "(call=%s)", TAG_MISSED_NAME, missed.call_id)
            return
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        if not lead:
            logger.warning("Sipuni пропущенный: сделка %s не прочиталась, теги не ставлю (call=%s)",
                           lead_id, missed.call_id)
            return
        had_missed = amo_service.has_tag(lead, TAG_MISSED_NAME)
        had_success = amo_service.has_tag(lead, TAG_SUCCESS_CALL_NAME)
        if had_missed and not had_success:
            logger.info("Sipuni пропущенный: «%s» на сделке %s уже есть (call=%s)",
                        TAG_MISSED_NAME, lead_id, missed.call_id)
        else:
            tags = amo_service.filter_tags_excluding(amo_service.get_tags(lead), TAG_SUCCESS_CALL_NAME)
            if not had_missed:
                tags.append({"name": TAG_MISSED_NAME})
            result = await amo_service.patch_lead(lead_id, tags=tags)
            logger.info(
                "Sipuni пропущенный: тег «%s» на сделке %s %s%s (call=%s)",
                TAG_MISSED_NAME, lead_id,
                "поставлен" if result.get("ok") else f"НЕ поставлен (status={result.get('status_code')})",
                f", «{TAG_SUCCESS_CALL_NAME}» снят" if had_success else "", missed.call_id,
            )
        for contact in (lead.get("_embedded") or {}).get("contacts") or []:
            cid = contact.get("id")
            if cid is None:
                continue
            result = await amo_service.add_contact_tag(cid, TAG_MISSED_NAME)
            if not result.get("ok"):
                logger.warning("Sipuni пропущенный: тег на контакт %s НЕ поставлен (status=%s, call=%s)",
                               cid, result.get("status_code"), missed.call_id)
    except Exception:
        logger.exception("Sipuni пропущенный: ошибка постановки тега (call=%s)", missed.call_id)


# Закрытые = системные статусы «успех»/«отказ», общие для ВСЕХ воронок
# (см. amo_service: 142/143 имеют одинаковый id во всех воронках). Та же логика,
# что у моста Jivo (_find_open_lead_id).
_CLOSED_STATUS_IDS = {142, 143}


async def _find_lead_row(phone: str, statuses=None) -> dict | None:
    """ОТКРЫТАЯ сделка по телефону целиком (id, responsible_user_id, status_id…) или None (не
    закрытую и не случайную — жалоба МОП: ссылка вела на рандомную/старую сделку). Открытая =
    status_id не из 142/143. Несколько открытых → самую свежую ПО РАБОТЕ (max
    updated_at, тай-брейк по id). Открытых нет → None (лучше без ссылки и тег смены, чем
    ссылка на закрытую).

    statuses: если задан, берём только сделки на этих этапах (теги Sipuni ставим лишь новой
    сделке, см. STATUS_NEW_LEAD_ALL)."""
    if not phone:
        return None
    try:
        leads = await amo_service.find_leads_by_query(phone)
        if leads is None:
            # Молчание amoCRM - не «сделок нет». Привязывать пропущенный звонок
            # наугад нельзя, выходим ни с чем.
            logger.warning("uis_missed_call: amoCRM не ответила на поиск по телефону")
            return None
        open_leads = [ld for ld in leads if ld.get("status_id") not in _CLOSED_STATUS_IDS]
        if statuses is not None:
            open_leads = [ld for ld in open_leads if ld.get("status_id") in statuses]
        if not open_leads:
            return None
        return max(open_leads, key=lambda ld: (ld.get("updated_at") or 0, ld.get("id") or 0))
    except Exception:
        logger.exception("UIS пропущенный: поиск сделки по телефону не удался")
        return None


async def _find_lead(phone: str, statuses=None):
    """(lead_id, responsible_user_id) ОТКРЫТОЙ сделки по телефону или (None, None), см. _find_lead_row."""
    row = await _find_lead_row(phone, statuses)
    if not row:
        return None, None
    return row.get("id"), row.get("responsible_user_id")


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(phone: str, name: str, lead_id, mentions: str, note: str = "") -> str:
    lines = [
        "🔴 ПРОПУЩЕННЫЙ звонок — срочно связаться с клиентом",
        mentions,
    ]
    if note:
        lines.append(note)
    if phone:
        lines.append(f"📞 {_esc(phone)}")
    if name and name != phone:  # новый номер — без имени
        lines.append(f"👤 {_esc(name)}")
    if lead_id:
        lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)
