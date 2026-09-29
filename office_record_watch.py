"""Сторож просроченной записи в офис (постановка Кати 27.09.2026).

Клиент записывается на приём через виджет NOVA «Онлайн-запись», сделка встаёт на этап
«Запись в офис» - и дальше этап не протухает никак: ни автозадачи, ни автоперевода, ни
алерта. Клиент не приехал, менеджер сделку не двинул и не перезаписал - сделка стоит
молча. Замер этапа 27.09.2026: из 17 сделок у 13 запись уже прошла, от 3 до 40 дней,
медиана 17 дней. Живой пример - сделка 36555973: запись была на 15.09, сделка на месте.

Что делаем: время записи прошло (поле «Дата окончания записи» плюс запас), сделку не
двинули и не перезаписали - ответственному менеджеру ставим задачу связаться с клиентом
и записать заново.

────────────────────────── почему опрос, а не таймер в памяти ──────────────────────────
Соседний `new_lead_watch` держит ожидания в словаре, который наполняет вебхук. Здесь так
нельзя: момент срабатывания наступает через ЧАСЫ или ДНИ после вебхука, а контейнер
`amo-fix-fields` пересобирают несколько раз в день соседние выкатки - каждая пересборка
молча теряла бы все ожидающие записи. Вдобавок вебхук о смене этапа приходит РАНЬШЕ, чем
NOVA пишет дату, и её в payload нет. И бэклог из 13 просроченных сделок вебхуками не
увидеть вовсе: по ним вебхуков больше не будет.

Поэтому опрос этапа. Он дешёвый: 17 сделок влезают в одну страницу, проход стоит ОДИН
GET, список `/api/v4/leads` отдаёт `custom_fields_values` (проверено живьём). При
интервале 10 минут это 144 запроса в сутки - 0,02% лимита интеграции.

⚠️ В `webhooks.lead_change()` этот модуль НЕ подключён, и это осознанно. Вебхук приходит
на любое изменение любой сделки, и делать тут ему нечего.

──────────────────────────────── на чём стоит правильность ────────────────────────────
  • перед постановкой задачи сделка ПЕРЕЧИТЫВАЕТСЯ из amo: увели с этапа или перезаписали
    пока шёл проход - молчим («ложная эскалация дороже пропущенной»);
  • ответственный берётся из свежей сделки, не из снимка списка: на входе в этап работает
    автоматика change_responsible, она могла сработать между двумя чтениями;
  • дедуп на ДИСКЕ (`autopilot_store.claim_notice`), а не в памяти: иначе каждая
    пересборка контейнера ставила бы задачи по всему этапу заново;
  • ключ дедупа - сделка ПЛЮС время записи. Перезапись меняет поле, значит меняется ключ,
    значит сторож честно взводится заново. Ключ по одной сделке запретил бы это навсегда;
  • в режиме отчёта (`OFFICE_RECORD_WATCH_CREATE_ENABLED=0`) ключи НЕ жжём - иначе сутки
    обкатки выжгли бы их все и фича уехала бы в бой навсегда молчащей.
  • у сделки есть ЛЮБАЯ незакрытая задача - молчим: сделку уже держит человек,
    вторая задача об одном только мусорит список (решение Кати 29.09.2026);

Решения Кати 27.09.2026: одна задача на одну запись (без напоминаний по кругу), тип
задачи «Связаться».
⚠️ Решение Кати 29.09.2026 ОТМЕНИЛО её же прежнее «ставить всегда»: теперь перед
созданием смотрим открытые задачи сделки и при любой незакрытой молчим
(`OFFICE_RECORD_SKIP_IF_OPEN_TASK`). Повод: 13 задач бэклога легли пачкой на двух человек.
"""

import asyncio
import datetime
import logging
import time

import alerts
import amo_service
import api
import autopilot_store as notices
import telegram_bot
from waybill_config import (
    FIELD_OFFICE_RECORD_END,
    OFFICE_RECORD_ALERT_CHAT_ID,
    OFFICE_RECORD_ALERT_ENABLED,
    OFFICE_RECORD_GRACE_MIN,
    OFFICE_RECORD_MAX_AGE_DAYS,
    OFFICE_RECORD_MAX_PER_PASS,
    OFFICE_RECORD_NOTE_ENABLED,
    OFFICE_RECORD_SKIP_IF_OPEN_TASK,
    OFFICE_RECORD_TASK_DEADLINE_H,
    OFFICE_RECORD_TASK_RESPONSIBLE_USER_ID,
    OFFICE_RECORD_TASK_TEXT,
    OFFICE_RECORD_TASK_TYPE_ID,
    OFFICE_RECORD_WATCH_CREATE_ENABLED,
    OFFICE_RECORD_WATCH_ENABLED,
    OFFICE_RECORD_WATCH_INTERVAL_S,
    OFFICE_RECORD_WINDOW_END_H,
    OFFICE_RECORD_WINDOW_START_H,
    PIPELINE_CLEVER_MAIN,
    STATUS_CLEVER_OFFICE_RECORD,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))

# Префикс ключа в общей таблице отметок autopilot_notified.
NOTICE_PREFIX = "office_record_missed"

# Отметки живут 90 дней. Порог намеренно БОЛЬШЕ, чем 60 у autopilot: таблица общая,
# и владельцы не должны удалять друг у друга живые отметки.
_PURGE_KEEP_DAYS = 90
_PURGE_EVERY_S = 86400

_task: asyncio.Task | None = None
# Единственное состояние в памяти, и оно только для статусной ручки: что сторожить,
# модуль каждый проход выясняет у amo заново.
_last_run: dict = {}
_last_purge_ts = 0


# ─────────────────────────────── чистые функции ───────────────────────────────


def end_ts_of(lead: dict) -> int | None:
    """Время окончания записи из сделки. Пусто или мусор - None (у 1 сделки из 17
    поля нет вовсе, и это само по себе похоже на сбой виджета)."""
    raw = amo_service.get_custom_field_value(lead, FIELD_OFFICE_RECORD_END)
    try:
        ts = int(raw)
    except (TypeError, ValueError):
        return None
    return ts if ts > 0 else None


def notice_kind(end_ts: int) -> str:
    """Ключ идемпотентности: сделка плюс ВРЕМЯ ЗАПИСИ, а не одна сделка.

    Перезаписали клиента на новую дату - поле поменялось, ключ поменялся, сторож
    взводится заново. Одна задача на одну запись (решение Кати 27.09.2026).
    """
    return f"{NOTICE_PREFIX}:{int(end_ts)}"


def fmt_when(end_ts: int) -> str:
    """«15.09 в 14:15» - как это прочитает менеджер. МСК, без года и без ID."""
    return datetime.datetime.fromtimestamp(int(end_ts), _MSK).strftime("%d.%m в %H:%M")


def task_text(end_ts: int) -> str:
    return OFFICE_RECORD_TASK_TEXT.replace("{когда}", fmt_when(end_ts))


def task_deadline(now_ts: int) -> int:
    """Срок задачи: сейчас плюс N часов, но внутри рабочего окна.

    Задача со сроком в три ночи рождается просроченной, и менеджер читает это как сбой
    сервиса, а не как просьбу. Поэтому срок, выпавший из окна, переносим на начало
    ближайшего рабочего дня - результат всегда в будущем.
    """
    dt = datetime.datetime.fromtimestamp(int(now_ts), _MSK) + datetime.timedelta(
        hours=OFFICE_RECORD_TASK_DEADLINE_H
    )
    if dt.hour < OFFICE_RECORD_WINDOW_START_H:
        dt = dt.replace(hour=OFFICE_RECORD_WINDOW_START_H, minute=0, second=0, microsecond=0)
    elif dt.hour >= OFFICE_RECORD_WINDOW_END_H:
        dt = (dt + datetime.timedelta(days=1)).replace(
            hour=OFFICE_RECORD_WINDOW_START_H, minute=0, second=0, microsecond=0
        )
    return int(dt.timestamp())


def decide(lead: dict, now_ts: int) -> tuple[str, int | None]:
    """Решение по ОДНОЙ сделке из списка этапа. Без сети и без диска.

    Отдельной проверки «сделка не закрыта» здесь нет намеренно: закрытие в amo меняет
    `status_id` на 142 или 143, и это уже ловит проверка этапа. Не добавляйте вторую.
    """
    try:
        status_id = int(lead.get("status_id") or 0)
        pipeline_id = int(lead.get("pipeline_id") or 0)
    except (TypeError, ValueError):
        return "other-stage", None
    if pipeline_id != PIPELINE_CLEVER_MAIN:
        return "other-pipeline", None
    # Фильтр запроса этап уже отобрал, но `decide` зовут и тесты, и разбор отчёта -
    # проверяем явно, а не «по построению».
    if status_id != STATUS_CLEVER_OFFICE_RECORD:
        return "other-stage", None

    end_ts = end_ts_of(lead)
    if not end_ts:
        return "no-date", None
    if now_ts < end_ts + OFFICE_RECORD_GRACE_MIN * 60:
        return "not-due", end_ts
    if end_ts < now_ts - OFFICE_RECORD_MAX_AGE_DAYS * 86400:
        return "too-old", end_ts
    return "fire", end_ts


# ─────────────────────────────── работа с amo ───────────────────────────────


async def _stage_leads() -> list[dict]:
    """Сделки этапа «Запись в офис».

    ⚠️ `get_leads_by_status` возвращает пустой список и когда этап пуст, и когда amo
    молчит - различить нельзя. Для нас это безопасно в нужную сторону: пустой список
    даёт ноль задач, ложных задач молчание amo не создаёт.
    """
    return await amo_service.get_leads_by_status(STATUS_CLEVER_OFFICE_RECORD, with_=())


async def _still_waiting(lead_id: int, end_ts: int) -> tuple[str, dict | None]:
    """Перечитывание сделки прямо перед действием.

    "ok" - всё ещё ждёт задачи · "moved" - увели с этапа или из воронки ·
    "rescheduled" - дату поменяли, пока шёл проход · "silent" - amo не ответил.
    Во всех случаях кроме "ok" ключ дедупа НЕ жжём.
    """
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
    except Exception:
        logger.exception("Сторож записи в офис: не прочиталась сделка %s", lead_id)
        return "silent", None
    if not lead:
        return "silent", None
    try:
        status_id = int(lead.get("status_id") or 0)
        pipeline_id = int(lead.get("pipeline_id") or 0)
    except (TypeError, ValueError):
        return "silent", None
    if status_id != STATUS_CLEVER_OFFICE_RECORD or pipeline_id != PIPELINE_CLEVER_MAIN:
        return "moved", lead
    if end_ts_of(lead) != int(end_ts):
        return "rescheduled", lead
    return "ok", lead


async def _has_open_task(lead_id: int) -> bool | None:
    """Есть ли у сделки хоть одна незакрытая задача. None - amo не ответил.

    Зовётся ТОЛЬКО по сделке, уже решённой на «сработать», а не на каждую сделку
    этапа: в стоячем режиме это ноль-два запроса в сутки, а не семнадцать на каждый проход.
    """
    try:
        tasks = await api.get_open_tasks(lead_id)
    except Exception:
        logger.exception("Сторож записи в офис: не прочитались задачи сделки %s", lead_id)
        return None
    if tasks is None:
        return None
    return bool(tasks)


async def _create(lead: dict, end_ts: int) -> str:
    """Задача менеджеру. "created" · "already" (по этой записи уже просили) · "failed".

    Ключ берём ДО запроса в amo - по правилу из `autopilot_store`: «лучше не отправить,
    чем отправить дважды». Упавший POST ключ не освобождает, но кричит в лог и в чат,
    чтобы потеря была видна, а не молчалива.
    """
    lead_id = int(lead["id"])
    claimed = await asyncio.to_thread(notices.claim_notice, lead_id, notice_kind(end_ts))
    if not claimed:
        return "already"

    responsible = OFFICE_RECORD_TASK_RESPONSIBLE_USER_ID or lead.get("responsible_user_id")
    now_ts = int(time.time())
    ok = await api.create_task(
        lead_id,
        task_text(end_ts),
        responsible,
        task_deadline(now_ts),
        task_type_id=OFFICE_RECORD_TASK_TYPE_ID or None,
    )
    if not ok:
        logger.error(
            "Сторож записи в офис: задачу по сделке %s создать НЕ удалось (запись была %s)",
            lead_id, fmt_when(end_ts),
        )
        await _notify_failure(lead_id, end_ts)
        return "failed"

    if OFFICE_RECORD_NOTE_ENABLED:
        # След, который переживёт автозакрытие задачи ботом amo.
        try:
            await amo_service.add_note(
                lead_id,
                f"Задача на перезапись поставлена автоматически: запись на "
                f"{fmt_when(end_ts)} прошла, сделка осталась на этапе.",
            )
        except Exception:
            logger.exception("Сторож записи в офис: примечание к сделке %s не легло", lead_id)
    logger.info(
        "Сторож записи в офис: задача поставлена по сделке %s (запись была %s)",
        lead_id, fmt_when(end_ts),
    )
    return "created"


# ─────────────────────────────── проход ───────────────────────────────


async def sweep_once() -> dict:
    """Один проход по этапу. Возвращает счётчики решений."""
    now_ts = int(time.time())
    leads = await _stage_leads()

    decisions: dict[str, int] = {}
    due: list[tuple[dict, int]] = []
    for lead in leads:
        decision, end_ts = decide(lead, now_ts)
        if decision == "fire":
            due.append((lead, int(end_ts)))
            continue
        decisions[decision] = decisions.get(decision, 0) + 1

    created: list[dict] = []
    for lead, end_ts in due:
        lead_id = int(lead.get("id") or 0)
        if not lead_id:
            continue
        if OFFICE_RECORD_WATCH_CREATE_ENABLED and len(created) >= OFFICE_RECORD_MAX_PER_PASS:
            # Предохранитель сработал: остальных возьмём следующим проходом.
            decisions["capped"] = decisions.get("capped", 0) + 1
            continue
        try:
            state, fresh = await _still_waiting(lead_id, end_ts)
            if state != "ok":
                decisions[state] = decisions.get(state, 0) + 1
                continue
            if OFFICE_RECORD_SKIP_IF_OPEN_TASK:
                busy = await _has_open_task(lead_id)
                if busy is None:
                    # amo не ответил про задачи - молчание не значит «путь свободен».
                    # Ключ не жжём, вернёмся следующим проходом.
                    decisions["tasks-silent"] = decisions.get("tasks-silent", 0) + 1
                    continue
                if busy:
                    # Сделку уже держит человек (решение Кати 29.09.2026).
                    # Ключ ТОЖЕ не жжём: закроет задачу и не двинет сделку - напомним.
                    decisions["has-open-task"] = decisions.get("has-open-task", 0) + 1
                    continue
            if not OFFICE_RECORD_WATCH_CREATE_ENABLED:
                decisions["would-fire"] = decisions.get("would-fire", 0) + 1
                created.append({"lead_id": lead_id, "end_ts": end_ts, "created": False,
                                "responsible": (fresh or {}).get("responsible_user_id")})
                continue
            result = await _create(fresh or lead, end_ts)
            decisions[result] = decisions.get(result, 0) + 1
            if result == "created":
                created.append({"lead_id": lead_id, "end_ts": end_ts, "created": True,
                                "responsible": (fresh or {}).get("responsible_user_id")})
        except Exception:
            logger.exception("Сторож записи в офис: ошибка по сделке %s", lead_id)
            decisions["failed"] = decisions.get("failed", 0) + 1

    _last_run.clear()
    _last_run.update({"at": now_ts, "leads": len(leads), "decisions": dict(decisions)})
    logger.info(
        "Сторож записи в офис: сделок на этапе %s, решения %s%s",
        len(leads), decisions or "{}",
        "" if OFFICE_RECORD_WATCH_CREATE_ENABLED else " (режим отчёта, ничего не создано)",
    )

    if created and OFFICE_RECORD_ALERT_ENABLED:
        await _notify(created)
    await _purge_if_due(now_ts)
    return decisions


async def report_once() -> dict:
    """Проход без создания задач, чем бы ни был выставлен флаг - для разбора бэклога
    и обкатки. Ключи дедупа не жжёт."""
    global OFFICE_RECORD_WATCH_CREATE_ENABLED
    saved = OFFICE_RECORD_WATCH_CREATE_ENABLED
    OFFICE_RECORD_WATCH_CREATE_ENABLED = False
    try:
        return await sweep_once()
    finally:
        OFFICE_RECORD_WATCH_CREATE_ENABLED = saved


async def _purge_if_due(now_ts: int) -> None:
    global _last_purge_ts
    if now_ts - _last_purge_ts < _PURGE_EVERY_S:
        return
    _last_purge_ts = now_ts
    try:
        gone = await asyncio.to_thread(notices.purge_notices_older_than, _PURGE_KEEP_DAYS)
        if gone:
            logger.info("Сторож записи в офис: убрано старых отметок %s", gone)
    except Exception:
        logger.exception("Сторож записи в офис: уборка отметок не прошла")


# ─────────────────────────────── уведомления ───────────────────────────────


def _lead_link(lead_id: int) -> str:
    return alerts.lead_link(lead_id)


async def _notify(created: list[dict]) -> None:
    """Итог прохода в чат. В режиме отчёта - что сторож сделал БЫ."""
    real = [c for c in created if c.get("created")]
    if real:
        head = f"🗓 Запись в офис прошла: поставлено задач {len(real)}"
        rows = real
    else:
        head = (f"🗓 Запись в офис прошла: сторож поставил бы задач {len(created)} "
                "(режим отчёта, ничего не создано)")
        rows = created
    lines = [head]
    for c in rows:
        lines.append(f"— запись была {fmt_when(c['end_ts'])}, {_lead_link(c['lead_id'])}")
    try:
        await telegram_bot.send_alert(
            "\n".join(lines),
            parse_mode="HTML",
            chat_id=int(OFFICE_RECORD_ALERT_CHAT_ID) if OFFICE_RECORD_ALERT_CHAT_ID else None,
        )
    except Exception:
        logger.exception("Сторож записи в офис: не смогла отправить итог прохода")


async def _notify_failure(lead_id: int, end_ts: int) -> None:
    if not OFFICE_RECORD_ALERT_ENABLED:
        return
    try:
        await telegram_bot.send_alert(
            f"⚠️ Запись в офис прошла, а задачу поставить не удалось\n"
            f"Запись была {fmt_when(end_ts)}\n{_lead_link(lead_id)}",
            parse_mode="HTML",
            chat_id=int(OFFICE_RECORD_ALERT_CHAT_ID) if OFFICE_RECORD_ALERT_CHAT_ID else None,
        )
    except Exception:
        logger.exception("Сторож записи в офис: не смогла пожаловаться на сбой")


# ─────────────────────────────── жизненный цикл ───────────────────────────────


async def _loop() -> None:
    while True:
        try:
            await sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Сторож записи в офис: проход упал")
        try:
            await asyncio.sleep(OFFICE_RECORD_WATCH_INTERVAL_S)
        except asyncio.CancelledError:
            raise


async def init() -> None:
    global _task
    if not OFFICE_RECORD_WATCH_ENABLED:
        logger.info("Сторож записи в офис: выключен (OFFICE_RECORD_WATCH_ENABLED=0)")
        return
    # Таблицу отметок поднимаем сами: `autopilot_store.init` зовётся только при
    # AUTOPILOT_ENABLED=1, а фича не должна зависеть от флага соседа.
    # CREATE TABLE IF NOT EXISTS делает вызов безобидным, даже если сосед уже поднял.
    await asyncio.to_thread(notices.init)
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info(
            "Сторож записи в офис: поднят (опрос %ss, запас %s мин, давность до %s дн, "
            "не больше %s задач за проход, тип задачи %s, создание %s, чужая открытая задача - %s)",
            OFFICE_RECORD_WATCH_INTERVAL_S, OFFICE_RECORD_GRACE_MIN,
            OFFICE_RECORD_MAX_AGE_DAYS, OFFICE_RECORD_MAX_PER_PASS,
            OFFICE_RECORD_TASK_TYPE_ID,
            "ВКЛ" if OFFICE_RECORD_WATCH_CREATE_ENABLED else "выкл (режим отчёта)",
            "молчим" if OFFICE_RECORD_SKIP_IF_OPEN_TASK else "ставим всё равно",
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
    if not OFFICE_RECORD_WATCH_ENABLED:
        return {"enabled": False}
    return {
        "enabled": True,
        "create": OFFICE_RECORD_WATCH_CREATE_ENABLED,
        "interval_s": OFFICE_RECORD_WATCH_INTERVAL_S,
        "grace_min": OFFICE_RECORD_GRACE_MIN,
        "max_age_days": OFFICE_RECORD_MAX_AGE_DAYS,
        "skip_if_open_task": OFFICE_RECORD_SKIP_IF_OPEN_TASK,
        "last_run": dict(_last_run),
    }
