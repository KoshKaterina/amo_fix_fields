"""Новый лид не взяли в работу: уведомление руководству (выбор Кати 28.08.2026).

Третий триггер группы «ОП срочные уведомления» и такой же второй контур, как счётчик
перезвонов: в чат ОП уходит событие, сюда - провал. Лид упал во вход воронки розницы и
через два РАБОЧИХ часа так и висит нетронутым.

────────────────────────────── почему часы рабочие ──────────────────────────────
Требование из ТЗ точек контроля (встреча с Сашей 30.07.2026): отсчёт начинается не
раньше 12:00. С десяти до двенадцати, а иногда и до часа, менеджеры разгребают ночную
пачку заявок, и дёргать их раньше бессмысленно. Ночь и вечер в счёт не идут вовсе: лид,
упавший в 23:40, к утру не «просрочен на десять часов», у него всё ещё ноль.

Поэтому время считается не вычитанием, а пересечением с рабочими окнами по дням -
`worktime_minutes()`. Она же единственное место, где живёт календарь этого триггера.

────────────────────────────── что считается «взяли» ──────────────────────────────
Уход с ВХОДНЫХ этапов воронки (хаб «Новый лид» плюс четыре буферных, `STATUS_NEW_LEAD_ALL`).
Ловушку, что менеджер двигает лид в «Взят в работу» и ничего не делает, Саша назвал сам
и решил на той же встрече не усложнять: меряем факт перехода. Начнут гонять вхолостую -
поменяем на «был ли звонок или сообщение».

Вебхук о смене этапа мог и не дойти, поэтому перед отправкой сделка ВСЕГДА перечитывается
из amo. Ложная эскалация в чат руководства дороже пропущенной: пара сообщений «а его уже
взяли» - и чат перестанут читать.

Состояние в памяти, как у соседей (`wazzup_sla`, `uis_missed_call`): рестарт контейнера
теряет ожидания. Размен осознанный - алерт ценен в тот же день.
"""

import asyncio
import datetime
import logging

import amo_service
import telegram_bot
import alerts
from api import BASE_URL
from tg_recipients import ROP_CHAT_ID, manager_name
from waybill_config import (
    NEW_LEAD_ESCALATE_MINUTES,
    NEW_LEAD_POLL_INTERVAL_S,
    NEW_LEAD_WINDOW_END_H,
    NEW_LEAD_WINDOW_START_H,
    PIPELINE_CLEVER_MAIN,
    STATUS_NEW_LEAD_ALL,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))

_pending: dict[int, dict] = {}
_watch_task: asyncio.Task | None = None
# Двое суток: за это время лид либо взяли, либо про него уже сказали руководству.
_TTL_HOURS = 48


def _now_msk() -> datetime.datetime:
    return datetime.datetime.now(_MSK)


def worktime_minutes(start: datetime.datetime, end: datetime.datetime) -> float:
    """Рабочие минуты между двумя моментами: сумма пересечений с окном по каждым суткам.

    Именно сумма по дням, а не «разница минус ночь»: лид может пролежать через вечер,
    ночь и утро, и только правильный подсчёт по окнам даёт честные «два часа работы».
    """
    if end <= start:
        return 0.0
    total = 0.0
    day = start.date()
    while day <= end.date():
        win_start = datetime.datetime.combine(
            day, datetime.time(NEW_LEAD_WINDOW_START_H, 0), tzinfo=_MSK)
        win_end = datetime.datetime.combine(
            day, datetime.time(NEW_LEAD_WINDOW_END_H, 0), tzinfo=_MSK)
        lo = max(start, win_start)
        hi = min(end, win_end)
        if hi > lo:
            total += (hi - lo).total_seconds() / 60
        day += datetime.timedelta(days=1)
    return total


def note_lead(lead_id, pipeline_id, status_id) -> None:
    """Разбор вебхука `/lead_change`: лид встал на вход воронки или ушёл с него.

    Зовём на КАЖДОМ изменении сделки розницы, поэтому здесь нет ни сети, ни чтения amo -
    только словарь. Тяжёлое живёт в `_sweep`.
    """
    if ROP_CHAT_ID is None or lead_id is None:
        return
    try:
        lead_id = int(lead_id)
        status_id = int(status_id) if status_id is not None else None
        pipeline_id = int(pipeline_id) if pipeline_id is not None else None
    except (TypeError, ValueError):
        return
    if pipeline_id != PIPELINE_CLEVER_MAIN:
        return

    if status_id in STATUS_NEW_LEAD_ALL:
        if lead_id not in _pending:
            _pending[lead_id] = {"since": _now_msk()}
            logger.info("Новый лид %s встал на вход воронки - счётчик пошёл", lead_id)
        return
    # Ушёл с входа: взяли в работу (или увели куда-то ещё) - сторожить нечего.
    if _pending.pop(lead_id, None) is not None:
        logger.info("Новый лид %s ушёл с входа воронки - счётчик снят", lead_id)


async def _still_untouched(lead_id: int) -> tuple[bool | None, dict | None]:
    """Висит ли лид всё ещё на входе. None - сделку не прочитать (amo молчит)."""
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
    except Exception:
        logger.exception("Новый лид: не прочиталась сделка %s", lead_id)
        return None, None
    if not lead:
        return None, None
    try:
        status_id = int(lead.get("status_id") or 0)
    except (TypeError, ValueError):
        return None, None
    return status_id in STATUS_NEW_LEAD_ALL, lead


def _build_message(lead_id: int, lead: dict, waited_min: float) -> str:
    """Руководству: факт и виновник. Без тегов и ID (правило Кати 03.08.2026), без
    точек посередине (правило Кати 26.08.2026)."""
    waited = _waited_words(waited_min)
    lines = [
        f"🚨 Новый лид не взяли в работу {waited}",
        f"Ответственный: {_esc(manager_name(lead.get('responsible_user_id')))}",
    ]
    name = (lead.get("name") or "").strip()
    if name:
        lines.append(f"Сделка: {_esc(name)}")
    lines.append("")
    lines.append(f'<a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)


def _waited_words(waited_min: float) -> str:
    """«47 минут» до полутора часов, дальше «2 часа» - одно правило и для текста из кода,
    и для переменной {{сколько_ждали}} шаблона из панели."""
    return f"{int(waited_min)} минут" if waited_min < 90 else f"{waited_min / 60:.0f} часа"


def _esc(s) -> str:
    return str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def _sweep() -> None:
    now = _now_msk()
    due: list[tuple[int, dict, float]] = []
    for lead_id, st in list(_pending.items()):
        if (now - st["since"]).total_seconds() >= _TTL_HOURS * 3600:
            _pending.pop(lead_id, None)
            continue
        waited = worktime_minutes(st["since"], now)
        if waited >= NEW_LEAD_ESCALATE_MINUTES:
            due.append((lead_id, st, waited))

    for lead_id, st, waited in due:
        try:
            untouched, lead = await _still_untouched(lead_id)
            if untouched is None:
                continue  # amo не ответил - вернёмся на следующем проходе
            _pending.pop(lead_id, None)
            if not untouched:
                continue  # взяли, вебхук о смене этапа просто не дошёл
            d = alerts.decide(
                "new_lead_untaken", legacy_text=_build_message(lead_id, lead or {}, waited),
                parse_mode="HTML", chat_id=ROP_CHAT_ID, lead=lead,
                values={
                    "сколько_ждали": _waited_words(waited),
                    "ответственный": manager_name((lead or {}).get("responsible_user_id")),
                    "сделка": ((lead or {}).get("name") or "").strip(),
                    "ссылка_на_сделку": alerts.lead_link(lead_id),
                },
            )
            if d is None:
                logger.info("Новый лид: эскалация выключена в панели (сделка %s)", lead_id)
                continue
            ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
            logger.info(
                "Новый лид: эскалация %s (сделка %s, %s рабочих минут на входе)",
                "отправлена" if ok else "НЕ отправлена", lead_id, int(waited),
            )
        except Exception:
            logger.exception("Новый лид: ошибка проверки сделки %s", lead_id)


async def _loop() -> None:
    while True:
        try:
            await asyncio.sleep(NEW_LEAD_POLL_INTERVAL_S)
            await _sweep()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Новый лид: ошибка в цикле проверки")


async def init() -> None:
    global _watch_task
    if ROP_CHAT_ID is None:
        logger.info("Новый лид: ROP_ALERT_CHAT_ID не задан - счётчик выключен")
        return
    if _watch_task is None:
        _watch_task = asyncio.create_task(_loop())
        logger.info(
            "Новый лид: счётчик запущен (порог %s рабочих мин, окно %s:00-%s:00 МСК)",
            NEW_LEAD_ESCALATE_MINUTES, NEW_LEAD_WINDOW_START_H, NEW_LEAD_WINDOW_END_H,
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
