"""Сторож писем: входящее письмо в закрытую сделку становится новым обращением.

────────────────────────────── что лечим ──────────────────────────────
Штатное правило почты amoCRM: письмо с тем же адресом и той же темой не создаёт новое
«Неразобранное», а продолжает прежнюю цепочку и ложится в ту сделку, где эта цепочка
началась — даже если сделка закрыта год назад. Замер 26.09.2026 за 30 дней: из 117
входящих писем 105 упали в закрытые сделки. Клиент написал — в работу это не попало.

Разбор проблемы и справка amo — projects/amo-cleanup/knowledge/
amo-povtornoe-pismo-ne-sozdaet-sdelku.md, техническая изнанка —
pisma-amo-kuda-padayut-i-vebhuki.md в той же папке.

────────────────────────────── почему опрос, а не вебхук ──────────────────────────────
Отдельного события «письмо» среди подписок вебхуков amo нет вовсе, а `update_lead` на
письмо не приходит (сверено с логом nginx на двух письмах 25.09.2026). У Цифровой воронки
триггер `mail_in` есть, но правило пришлось бы ставить руками на УР и ЗНР в каждой из
десяти воронок, оно живёт в UI, а не в git, и теряется при простое сервиса.

Опрос журнала событий (`GET /api/v4/events`, `filter[type][]=incoming_mail`) от этого
свободен: читает окно времени и догоняет пропущенное после любого простоя. Цена
мгновенности здесь нулевая — медиана времени до нашего ответа на письмо 249 минут
(39 переписок за 60 дней), ни одного ответа быстрее 5 минут. Трёхминутный опрос в этом
масштабе неотличим от мгновенного хука.

⚠️ Почему события, а не список примечаний: у `/api/v4/leads/notes` фильтр
`filter[created_at][from]` молча игнорируется (проверено 26.09.2026 — при окне в неделю
приходят письма 2021 года), а `filter[updated_at][from]` тянет старьё. У событий окно по
`created_at` работает честно, поэтому письмо дочитывается отдельным запросом по id
примечания из `value_after[0].note.id`. Это +1 запрос на письмо, при 4 письмах в сутки —
4 запроса.

────────────────────────────── три предохранителя ──────────────────────────────
1. **Дедуп по письму.** Одно письмо amo кладёт примечанием в НЕСКОЛЬКО сделок контакта
   (13 писем из 289 за 30 дней легли в две-три сделки, одна из них бывает закрыта задолго
   до письма). Без дедупа по `message_id` одно письмо создало бы три сделки.
2. **Антидубль по переписке.** Клиент пишет три дня подряд в одну тему — это один диалог.
   Пока наша сделка по этому `thread_id` открыта, новых не создаём.
3. **Граница включения (`MAIL_WATCH_SINCE_TS`).** Без неё первый же проход поехал бы по
   архиву. Не задана — модуль не работает вовсе, как в office_transfer.

────────────────────────────── режимы ──────────────────────────────
`MAIL_WATCH_ENABLED` поднимает цикл, `MAIL_WATCH_CREATE_ENABLED` разрешает СОЗДАВАТЬ
сделки. По умолчанию выключены оба: сначала сутки в режиме отчёта (что бы сторож сделал),
потом запись. Порядок из схемы разбора, шаг 5.

⚠️ Копия письма в новой сделке будет НЕПОЛНОЙ и это ограничение площадки: API v4 отдаёт
только тему, адреса и первые слова (`content_summary`); тела письма в нём нет, а создать
примечание типа `amomail_message` API не даёт (10 разрешённых типов, письма среди них
нет). Поэтому в новой сделке лежит карточка письма со ссылкой на сделку с оригиналом, и
отвечать менеджер будет оттуда.
"""

import asyncio
import logging
import time

import alerts
import amo_service
import api
import mail_watch_store as store
import telegram_bot
from waybill_config import (
    MAIL_WATCH_ALERT_CHAT_ID,
    MAIL_WATCH_ALERT_ENABLED,
    MAIL_WATCH_CREATE_ENABLED,
    MAIL_WATCH_ENABLED,
    MAIL_WATCH_IGNORE_PIPELINES,
    MAIL_WATCH_IGNORE_SENDERS,
    MAIL_WATCH_INTERVAL_S,
    MAIL_WATCH_LEAD_NAME_PREFIX,
    MAIL_WATCH_MAX_AGE_MIN,
    MAIL_WATCH_MAX_LOOKBACK_S,
    MAIL_WATCH_OVERLAP_S,
    MAIL_WATCH_RESPONSIBLE_USER_ID,
    MAIL_WATCH_SINCE_TS,
    MAIL_WATCH_TAG,
    MAIL_WATCH_TARGET_PIPELINE_ID,
    MAIL_WATCH_TARGET_STATUS_ID,
)

logger = logging.getLogger("uvicorn")

_CLOSED_STATUSES = {142, 143}
_ENTITY_PATH = {"lead": "leads", "contact": "contacts", "company": "companies", "customer": "customers"}

_task: asyncio.Task | None = None
_last_run: dict = {}


def _lead_link(lead_id) -> str:
    return f"{api.BASE_URL}/leads/detail/{lead_id}"


def is_service_sender(email: str | None) -> bool:
    """Робот, рассылка, уведомление площадки. Список настраиваемый: в замере половина
    писем, подходящих под правило, оказалась рассылками Авито, Яндекса и Ellipal плюс
    холодные предложения услуг. Держим маски в конфиге, чтобы новый источник шума
    гасился переменной окружения, а не выкаткой кода."""
    if not email:
        # Письмо без адреса отправителя разбирать нечем: считаем служебным, чтобы не
        # завести сделку, в которой нечего ответить.
        return True
    low = email.strip().lower()
    return any(mask and mask in low for mask in MAIL_WATCH_IGNORE_SENDERS)


async def _fetch_note(entity_type: str, entity_id: int, note_id: int) -> dict | None:
    path = _ENTITY_PATH.get(entity_type)
    if not path:
        return None
    return await amo_service._do_get(f"/api/v4/{path}/{entity_id}/notes/{note_id}")


def _mail_params(note: dict) -> dict:
    return (note or {}).get("params") or {}


async def _lead_ids_of_contact(contact_id: int) -> list[int]:
    contact = await amo_service.get_contact_by_id(contact_id, with_=("leads",))
    if contact is None:
        raise _AmoSilent(f"контакт {contact_id} не прочитан")
    leads = ((contact.get("_embedded") or {}).get("leads")) or []
    return [int(x["id"]) for x in leads if x.get("id")]


async def _has_open_lead(lead_ids: list[int], *, exclude: set[int]) -> int | None:
    """Есть ли среди сделок контакта ОТКРЫТАЯ (кроме тех, куда легло само письмо).
    Возвращает id первой найденной. amo не ответил — поднимаем _AmoSilent: молчание
    площадки нельзя читать как «открытых сделок нет», иначе заведём лишнюю.

    Воронки из MAIL_WATCH_IGNORE_PIPELINES не считаются работой: открытая сделка в
    «Тесте» или в картотеке «Работа с базой» не значит, что письмо кто-то увидит."""
    ids = [i for i in lead_ids if i not in exclude]
    if not ids:
        return None
    leads = await amo_service.get_leads_by_ids(ids)
    if leads is None:
        raise _AmoSilent("список сделок контакта не прочитан")
    skipped: list[int] = []
    for lead in leads:
        if int(lead.get("status_id") or 0) in _CLOSED_STATUSES:
            continue
        if int(lead.get("pipeline_id") or 0) in MAIL_WATCH_IGNORE_PIPELINES:
            skipped.append(int(lead["id"]))
            continue
        return int(lead["id"])
    if skipped:
        logger.info(
            "mail_watch: открытые сделки %s лежат в воронках-исключениях — работой не считаю",
            skipped,
        )
    return None


class _AmoSilent(Exception):
    """amo не ответил. Письмо не помечаем разобранным — вернёмся к нему следующим
    проходом (окно читается с перекрытием). Лучше повтор, чем пропуск или дубль."""


def _mail_card(params: dict, mail_at: int, source_lead_id: int | None) -> str:
    """Карточка письма для примечания. Полного текста в API нет — только тема, адреса и
    первые слова, поэтому главное здесь — ссылка на сделку с оригиналом."""
    sender = (params.get("from") or {}).get("email") or "адрес не указан"
    name = (params.get("from") or {}).get("name") or ""
    who = f"{name} <{sender}>" if name else sender
    when = time.strftime("%d.%m.%Y %H:%M", time.localtime(mail_at)) if mail_at else "время не указано"
    lines = [
        "Новое письмо от клиента по закрытой сделке.",
        f"От: {who}",
        f"Тема: {params.get('subject') or 'без темы'}",
        f"Получено: {when}",
    ]
    summary = (params.get("content_summary") or "").strip()
    if summary:
        lines.append(f"Начало письма: {summary}")
    if params.get("attach_cnt"):
        lines.append(f"Вложений: {params.get('attach_cnt')}")
    if source_lead_id:
        lines.append(f"Само письмо лежит в прежней сделке: {_lead_link(source_lead_id)}")
        lines.append("Отвечать клиенту нужно оттуда — amoCRM держит переписку в той сделке,"
                     " где началась цепочка.")
    return "\n".join(lines)


async def _create_lead_for_mail(
    *,
    params: dict,
    mail_at: int,
    contact_id: int | None,
    source_lead_id: int | None,
) -> int | None:
    if not MAIL_WATCH_TARGET_PIPELINE_ID or not MAIL_WATCH_TARGET_STATUS_ID:
        logger.error(
            "mail_watch: не заданы воронка и этап для новых сделок "
            "(MAIL_WATCH_TARGET_PIPELINE_ID / MAIL_WATCH_TARGET_STATUS_ID) — сделку не создаю"
        )
        return None
    if not MAIL_WATCH_RESPONSIBLE_USER_ID:
        logger.error(
            "mail_watch: не задан MAIL_WATCH_RESPONSIBLE_USER_ID — сделку не создаю. "
            "Без ответственного её никто не увидит, а распределитель на такие сделки не настроен"
        )
        return None

    subject = (params.get("subject") or "").strip() or "без темы"
    name = f"{MAIL_WATCH_LEAD_NAME_PREFIX}: {subject}"[:200]
    lead_id = await api.create_lead_direct(
        name=name,
        pipeline_id=MAIL_WATCH_TARGET_PIPELINE_ID,
        status_id=MAIL_WATCH_TARGET_STATUS_ID,
        responsible_user_id=MAIL_WATCH_RESPONSIBLE_USER_ID,
        contact_id=contact_id,
        tags=[MAIL_WATCH_TAG] if MAIL_WATCH_TAG else None,
    )
    if lead_id is None:
        logger.error("mail_watch: сделка по письму НЕ создалась (тема «%s»)", subject)
        return None

    await amo_service.add_note(lead_id, _mail_card(params, mail_at, source_lead_id))
    if source_lead_id:
        # Обратная ссылка в старой сделке: менеджер отвечает из неё и должен видеть, что
        # обращение уже учтено — иначе заведёт вторую сделку руками.
        await amo_service.add_note(
            source_lead_id,
            "Клиент написал по этой закрытой сделке. Обращение учтено новой сделкой: "
            f"{_lead_link(lead_id)}\nОтвечать клиенту — из этой сделки, письмо лежит здесь.",
        )
    logger.info(
        "mail_watch: создана сделка %s по письму (тема «%s», контакт %s, прежняя сделка %s)",
        lead_id, subject, contact_id, source_lead_id,
    )
    return lead_id


async def _handle_event(ev: dict) -> str:
    """Разбор одного события письма. Возвращает решение (см. константы стора)."""
    entity_type = str(ev.get("entity_type") or "")
    entity_id = ev.get("entity_id")
    value_after = ev.get("value_after") or []
    note_id = ((value_after[0].get("note") if value_after else None) or {}).get("id")
    if not note_id or not entity_id:
        logger.warning("mail_watch: событие без примечания или сущности, пропускаю: %s", ev.get("id"))
        return store.DECISION_FAILED

    note_id = int(note_id)
    entity_id = int(entity_id)
    if store.note_seen(note_id):
        return store.DECISION_DUPLICATE

    note = await _fetch_note(entity_type, entity_id, note_id)
    if note is None:
        raise _AmoSilent(f"примечание {note_id} не прочитано")
    params = _mail_params(note)
    mail_at = int(note.get("created_at") or ev.get("created_at") or 0)
    message_id = str(params.get("message_id") or "")
    thread_id = str(params.get("thread_id") or "")
    sender = (params.get("from") or {}).get("email")

    def _mark(decision: str, lead_id: int | None = None) -> str:
        store.mark(
            note_id,
            decision=decision,
            message_id=message_id,
            thread_id=thread_id,
            entity_type=entity_type,
            entity_id=entity_id,
            lead_id=lead_id,
            mail_at=mail_at,
            sender=sender,
            subject=(params.get("subject") or None),
        )
        return decision

    if not params.get("income"):
        # В событиях incoming_mail такого быть не должно, но проверка дешёвая, а цена
        # ошибки — сделка по нашему же письму.
        return _mark(store.DECISION_DUPLICATE)

    if mail_at and MAIL_WATCH_SINCE_TS and mail_at < MAIL_WATCH_SINCE_TS:
        return _mark(store.DECISION_DUPLICATE)

    if MAIL_WATCH_MAX_AGE_MIN and mail_at and (time.time() - mail_at) > MAIL_WATCH_MAX_AGE_MIN * 60:
        logger.info("mail_watch: письмо старше %s мин — не разбираю", MAIL_WATCH_MAX_AGE_MIN)
        return _mark(store.DECISION_DUPLICATE)

    if is_service_sender(sender):
        return _mark(store.DECISION_SERVICE_SENDER)

    prior = store.message_decision(message_id)
    if prior is not None:
        # То же физическое письмо уже разобрано в другой сделке этого контакта.
        return _mark(store.DECISION_DUPLICATE)

    # Где лежит письмо и кто клиент.
    source_lead_id: int | None = None
    contact_ids: list[int] = []
    if entity_type == "lead":
        lead = await amo_service.get_lead_full(entity_id, with_=("contacts",))
        if lead is None:
            raise _AmoSilent(f"сделка {entity_id} не прочитана")
        if int(lead.get("status_id") or 0) not in _CLOSED_STATUSES:
            # Письмо попало в открытую сделку — менеджер его видит, это не наш случай.
            return _mark(store.DECISION_OPEN_LEAD)
        source_lead_id = entity_id
        contact_ids = [int(c["id"]) for c in ((lead.get("_embedded") or {}).get("contacts") or []) if c.get("id")]
    elif entity_type == "contact":
        contact_ids = [entity_id]
    else:
        logger.info("mail_watch: письмо на сущности %s — не разбираю", entity_type)
        return _mark(store.DECISION_DUPLICATE)

    # Наша же сделка по этой переписке, если ещё открыта. Проверяем ПЕРЕД общей проверкой
    # открытых сделок контакта: формально сработала бы и она (наша сделка у контакта тоже
    # открытая), но в журнале осталось бы невнятное «у клиента есть открытая сделка»
    # вместо «это продолжение того же диалога».
    our_lead = store.thread_lead(thread_id)
    if our_lead is not None:
        leads = await amo_service.get_leads_by_ids([our_lead])
        if leads is None:
            raise _AmoSilent("сделка по переписке не прочитана")
        if leads and int(leads[0].get("status_id") or 0) not in _CLOSED_STATUSES:
            return _mark(store.DECISION_THREAD_ACTIVE, lead_id=our_lead)

    # Есть ли у клиента открытая сделка помимо той, куда легло письмо.
    all_lead_ids: list[int] = []
    for cid in contact_ids:
        all_lead_ids.extend(await _lead_ids_of_contact(cid))
    open_lead = await _has_open_lead(all_lead_ids, exclude={source_lead_id} if source_lead_id else set())
    if open_lead is not None:
        return _mark(store.DECISION_CONTACT_HAS_OPEN)

    if not MAIL_WATCH_CREATE_ENABLED:
        logger.info(
            "mail_watch: РЕЖИМ ОТЧЁТА — завёл бы сделку по письму от %s (тема «%s», "
            "прежняя сделка %s)",
            sender, params.get("subject"), _lead_link(source_lead_id) if source_lead_id else "нет",
        )
        return _mark(store.DECISION_REPORT_ONLY)

    lead_id = await _create_lead_for_mail(
        params=params,
        mail_at=mail_at,
        contact_id=contact_ids[0] if contact_ids else None,
        source_lead_id=source_lead_id,
    )
    if lead_id is None:
        return _mark(store.DECISION_FAILED)
    return _mark(store.DECISION_CREATED, lead_id=lead_id)


async def _fetch_events(ts_from: int, ts_to: int) -> list[dict]:
    events: list[dict] = []
    page = 1
    while page <= 20:
        params = [
            ("filter[type][]", "incoming_mail"),
            ("filter[created_at][from]", str(ts_from)),
            ("filter[created_at][to]", str(ts_to)),
            ("limit", "100"),
            ("page", str(page)),
        ]
        data = await amo_service._do_get("/api/v4/events", params)
        if data is None:
            raise _AmoSilent("журнал событий не прочитан")
        batch = ((data.get("_embedded") or {}).get("events")) or []
        events.extend(batch)
        if len(batch) < 100:
            break
        page += 1
    return events


async def reconcile_once() -> str:
    now = int(time.time())
    if not MAIL_WATCH_SINCE_TS:
        logger.warning("mail_watch: MAIL_WATCH_SINCE_TS не задан — проход пропущен")
        return "skipped-no-cutover"

    last = store.get_last_ts()
    window_from = max(last - MAIL_WATCH_OVERLAP_S, MAIL_WATCH_SINCE_TS)
    window_from = max(window_from, now - MAIL_WATCH_MAX_LOOKBACK_S)

    try:
        events = await _fetch_events(window_from, now)
    except _AmoSilent as exc:
        logger.warning("mail_watch: %s — проход пропущен, окно не двигаю", exc)
        return "skipped-amo-silent"

    decisions: dict[str, int] = {}
    silent = 0
    for ev in events:
        try:
            decision = await _handle_event(ev)
        except _AmoSilent as exc:
            silent += 1
            logger.warning("mail_watch: %s — письмо разберём следующим проходом", exc)
            continue
        except Exception:
            logger.exception("mail_watch: ошибка разбора события %s", ev.get("id"))
            decisions["failed"] = decisions.get("failed", 0) + 1
            continue
        decisions[decision] = decisions.get(decision, 0) + 1

    # Окно двигаем только если все письма разобраны: иначе следующий проход должен
    # увидеть их снова.
    if silent == 0:
        store.set_last_ts(now)

    _last_run.clear()
    _last_run.update({"at": now, "events": len(events), "decisions": decisions, "silent": silent})
    if events:
        logger.info(
            "mail_watch: окно [%s, %s), писем %s, решения %s%s",
            window_from, now, len(events), decisions,
            f", отложено {silent}" if silent else "",
        )

    interesting = decisions.get(store.DECISION_CREATED, 0) + decisions.get(store.DECISION_REPORT_ONLY, 0)
    if interesting and MAIL_WATCH_ALERT_ENABLED:
        await _notify(decisions)
    return f"events={len(events)}"


async def _notify(decisions: dict[str, int]) -> None:
    created = decisions.get(store.DECISION_CREATED, 0)
    would = decisions.get(store.DECISION_REPORT_ONLY, 0)
    if created:
        text = f"📬 Письмо клиента по закрытой сделке: создано новых сделок — {created}"
    else:
        text = (
            f"📬 Письмо клиента по закрытой сделке: сторож завёл бы сделок — {would} "
            "(режим отчёта, ничего не создано)"
        )
    try:
        d = alerts.decide(
            "mail_watch",
            legacy_text=text,
            values={},
            chat_id=MAIL_WATCH_ALERT_CHAT_ID or None,
            keep_text=True,
        )
        if d is None:
            # Панель уведомлений велела не слать это событие — это нормальный ответ.
            return
        await telegram_bot.send_alert(d.text, **d.send_kwargs())
    except Exception:
        logger.exception("mail_watch: не смогла отправить уведомление")


async def _loop() -> None:
    while True:
        try:
            await reconcile_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("mail_watch: проход упал")
        await asyncio.sleep(MAIL_WATCH_INTERVAL_S)


async def init() -> None:
    global _task
    if not MAIL_WATCH_ENABLED:
        return
    await asyncio.to_thread(store.init_db)
    if not MAIL_WATCH_SINCE_TS:
        logger.warning(
            "mail_watch: MAIL_WATCH_ENABLED=1, но MAIL_WATCH_SINCE_TS не задан — "
            "цикл поднят, проходы будут пропускаться (защита от разбора архива)"
        )
    _task = asyncio.create_task(_loop())
    logger.info(
        "mail_watch: сторож писем поднят (интервал %ss, создание сделок %s)",
        MAIL_WATCH_INTERVAL_S, "ВКЛ" if MAIL_WATCH_CREATE_ENABLED else "выкл (режим отчёта)",
    )


async def shutdown() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        try:
            await _task
        except (asyncio.CancelledError, Exception):
            pass
        _task = None


def status() -> dict:
    """Для статусной ручки сервиса: видно, что сторож жив и что он решал."""
    if not MAIL_WATCH_ENABLED:
        return {"enabled": False}
    out = {
        "enabled": True,
        "create": MAIL_WATCH_CREATE_ENABLED,
        "interval_s": MAIL_WATCH_INTERVAL_S,
        "last_run": dict(_last_run),
    }
    try:
        out["totals"] = store.counts_by_decision()
        out["last_ts"] = store.get_last_ts()
    except Exception:
        out["totals"] = {}
    return out
