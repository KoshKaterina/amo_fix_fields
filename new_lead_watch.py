"""Новый лид не взяли в работу: розница и Академия (Катя 28.08.2026, 29.09.2026).

Третий триггер группы «ОП срочные уведомления» и такой же второй контур, как счётчик
перезвонов: в чат ОП уходит событие, сюда - провал. Лид упал во вход воронки и через два
РАБОЧИХ часа так и висит нетронутым.

──────────────────────────── две воронки, один сторож ────────────────────────────
29.09.2026 к рознице добавилась Академия. Сторож ПАРАМЕТРИЗОВАН по воронке, а не
скопирован: копия означала бы вторую `worktime_minutes`, второе перечитывание сделки и
второй файл состояния - то самое размножение правила, от которого мы лечимся в
`knowledge/edinyy-kontur-pravila-i-storozh.md`. Таблица воронок - `_WATCHED`.

Различаются воронки тремя вещами, и все три записаны в таблице:

  • ЭТАПЫ входа. У розницы их пять (хаб плюс четыре буферных), у Академии один -
    «Входящий лид». Массовый прогон по другим этапам Академии сторожа не будит.
  • АДРЕСАТ и язык. Розничный провал идёт руководству: имя менеджера словами, без тегов.
    Академия идёт в чат менеджеров - там нужен @тег и «возьмите в работу», иначе
    сообщение читать некому. Это правило Кати 28.08.2026 про два чата, а не вкусовщина.
  • МАРШРУТ по ответственному. Сделку Академии ведёт менеджер её команды - уведомление
    уходит в группу этой команды (см. `alerts.channel_for_responsible`).

Порог и рабочее окно у обеих ОДНИ И ТЕ ЖЕ (прямой ответ Кати 29.09.2026: «те же, 30 мин /
2 часа, окно 12:00-19:00»). Своего порога у Академии нет намеренно - разойдутся.

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
import dataclasses
import datetime
import json
import logging
import os
import pathlib

import amo_service
import telegram_bot
import alerts
from api import BASE_URL
from tg_recipients import (
    NOTIFY_CHAT_ID,
    NOTIFY_THREAD_ID,
    ROP_CHAT_ID,
    academy_mentions_for,
    manager_name,
)
from waybill_config import (
    ACADEMY_LEAD_UNTAKEN_ENABLED,
    ACADEMY_PANEL_FALLBACK_AMO_ID,
    NEW_LEAD_ESCALATE_MINUTES,
    NEW_LEAD_POLL_INTERVAL_S,
    NEW_LEAD_WINDOW_END_H,
    NEW_LEAD_WINDOW_START_H,
    PIPELINE_ACADEMY,
    PIPELINE_CLEVER_MAIN,
    STATUS_ACADEMY_INBOUND_LEAD,
    STATUS_NEW_LEAD_ALL,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))


@dataclasses.dataclass(frozen=True)
class _Watch:
    """Что сторожим в одной воронке. Порога и окна здесь нет - они общие на все воронки."""

    event: str                  # ключ события в каталоге панели
    statuses: frozenset[int]    # входные этапы: стоит на таком - счётчик идёт
    chat: str                   # куда по умолчанию: "rop" (руководство) | "op_notify" (отдел)
    subject: str                # как зовём лид в тексте: «Новый лид», «Лид Академии»
    tags: bool                  # чат менеджеров - нужен @тег; руководству - имя словами
    route: bool                 # адрес уточняется по ответственному
    panel: bool                 # копия в ленту панели, лично ответственному


_WATCHED: dict[int, _Watch] = {
    PIPELINE_CLEVER_MAIN: _Watch(
        event="new_lead_untaken", statuses=frozenset(STATUS_NEW_LEAD_ALL),
        chat="rop", subject="Новый лид", tags=False, route=False, panel=False,
    ),
    PIPELINE_ACADEMY: _Watch(
        event="academy_lead_untaken", statuses=frozenset({STATUS_ACADEMY_INBOUND_LEAD}),
        chat="op_notify", subject="Лид Академии", tags=True, route=True, panel=True,
    ),
}

_pending: dict[int, dict] = {}
_watch_task: asyncio.Task | None = None
# Двое суток: за это время лид либо взяли, либо про него уже сказали руководству.
_TTL_HOURS = 48

# ⚠️ Счётчики лежат на ДИСКЕ, а не только в памяти (27.09.2026). Раньше состояние жило в
# памяти процесса, и это тихо съедало эскалации: 27.09 контейнер пересобирался дважды, и пять
# заказов, простоявших на входном этапе с вечера (11-21 час), не дали алерта руководству
# вообще - ожидания по ним были потеряны первой же пересборкой. Файл на томе `var` рядом с
# базой авто-режима и дедупами соседей.
STATE_PATH = pathlib.Path(os.getenv("NEW_LEAD_WATCH_PATH", "/app/var/new_lead_watch.json"))


def _now_msk() -> datetime.datetime:
    return datetime.datetime.now(_MSK)


def _destination(w: _Watch) -> tuple[int | None, int | None]:
    """Адрес, который сендер послал бы сам (панель может его переопределить).

    ⚠️ Читается в МОМЕНТ отправки, а не складывается в `_WATCHED` при импорте: номера чатов
    подменяют тесты, да и боевой `ROP_CHAT_ID` - выключатель, а таблица, снятая на импорте,
    его смену уже не увидит.
    """
    if w.chat == "rop":
        return ROP_CHAT_ID, None
    return NOTIFY_CHAT_ID, NOTIFY_THREAD_ID


def _is_on(w: _Watch) -> bool:
    """Сторожим ли эту воронку прямо сейчас: есть куда слать и не погашен мастер-флаг.

    Выключатели РАЗДЕЛЬНЫЕ по воронкам намеренно: до 29.09.2026 пустой `ROP_ALERT_CHAT_ID`
    глушил сторож целиком, и Академия, которой этот чат не нужен, замолчала бы вместе с
    розницей.
    """
    if w.event == "academy_lead_untaken" and not ACADEMY_LEAD_UNTAKEN_ENABLED:
        return False
    return _destination(w)[0] is not None


def _watch_for(pipeline_id) -> _Watch | None:
    """Воронка из таблицы наблюдаемых, если она сейчас включена. Иначе None - молчим."""
    w = _WATCHED.get(pipeline_id)
    return w if (w is not None and _is_on(w)) else None


def _save() -> None:
    """Слепок счётчиков на диск. Best-effort: не записалось - сторож работает как раньше.

    Пишем атомарно (файл рядом плюс `os.replace`), тем же приёмом, что
    `autopilot_settings_client`: половинчатый файл читался бы как пустое состояние, то есть
    ровно как та потеря, из-за которой всё это и появилось.
    """
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "saved_at": _now_msk().isoformat(),
            "pending": {
                str(k): {"since": v["since"].isoformat(), "pipeline": v.get("pipeline")}
                for k, v in _pending.items()
            },
        }
        tmp = STATE_PATH.with_name(STATE_PATH.name + f".tmp{os.getpid()}")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, STATE_PATH)
    except OSError:
        logger.exception("Новый лид: не смог сохранить счётчики в %s", STATE_PATH)


def _load() -> None:
    """Поднять счётчики после рестарта. Битый или отсутствующий файл - начинаем с чистого.

    ⚠️ Отметки времени берём КАК БЫЛИ, а не «с этой минуты»: лид, пролежавший на входе три
    рабочих часа до пересборки, обязан остаться просроченным и после неё.

    ⚠️ Файл, записанный до 29.09.2026, воронки не знает. Такие счётчики поднимаем как
    РОЗНИЧНЫЕ: до Академии сторож сторожил только её, и другого источника у этих строк быть
    не может. Молча выбрасывать их нельзя - это те же потерянные эскалации, из-за которых
    файл и появился.
    """
    if not STATE_PATH.exists():
        return
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8")) or {}
    except (ValueError, OSError):
        logger.exception("Новый лид: не смог прочитать счётчики %s", STATE_PATH)
        return
    restored = 0
    for raw_id, st in (data.get("pending") or {}).items():
        try:
            lead_id = int(raw_id)
            since = datetime.datetime.fromisoformat(str((st or {}).get("since")))
        except (TypeError, ValueError):
            continue
        if since.tzinfo is None:
            since = since.replace(tzinfo=_MSK)
        try:
            pipeline = int((st or {}).get("pipeline") or PIPELINE_CLEVER_MAIN)
        except (TypeError, ValueError):
            pipeline = PIPELINE_CLEVER_MAIN
        _pending[lead_id] = {"since": since, "pipeline": pipeline}
        restored += 1
    if restored:
        logger.info("Новый лид: с диска поднято счётчиков: %s", restored)


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

    Зовём на КАЖДОМ изменении любой сделки, поэтому здесь нет ни сети, ни чтения amo -
    только словарь. Тяжёлое живёт в `_sweep`.
    """
    if lead_id is None:
        return
    try:
        lead_id = int(lead_id)
        status_id = int(status_id) if status_id is not None else None
        pipeline_id = int(pipeline_id) if pipeline_id is not None else None
    except (TypeError, ValueError):
        return
    w = _watch_for(pipeline_id)
    if w is None:
        # Воронку не сторожим (или её сторож выключен). Счётчик при этом НЕ снимаем: сделку
        # могли увести в другую воронку, и обратный переезд должен продолжить прежний отсчёт.
        return

    if status_id in w.statuses:
        if lead_id not in _pending:
            _pending[lead_id] = {"since": _now_msk(), "pipeline": pipeline_id}
            logger.info("%s %s встал на вход воронки - счётчик пошёл", w.subject, lead_id)
            _save()
        return
    # Ушёл с входа: взяли в работу (или увели куда-то ещё) - сторожить нечего.
    if _pending.pop(lead_id, None) is not None:
        logger.info("%s %s ушёл с входа воронки - счётчик снят", w.subject, lead_id)
        _save()


async def _still_untouched(lead_id: int, w: _Watch) -> tuple[bool | None, dict | None]:
    """Висит ли лид всё ещё на входе СВОЕЙ воронки. None - сделку не прочитать (amo молчит).

    Воронку сверяем тоже: сделку могли увести в другую, и там её входные этапы другие -
    розничный «Новый лид» и академический «Входящий лид» это разные числа, но спутать их
    сравнением по одному набору легко.
    """
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=())
    except Exception:
        logger.exception("%s: не прочиталась сделка %s", w.subject, lead_id)
        return None, None
    if not lead:
        return None, None
    try:
        status_id = int(lead.get("status_id") or 0)
    except (TypeError, ValueError):
        return None, None
    pipeline_id = lead.get("pipeline_id")
    if pipeline_id is not None:
        try:
            if int(pipeline_id) not in _WATCHED or _WATCHED[int(pipeline_id)] is not w:
                return False, lead   # уехал в другую воронку - этот сторож своё отработал
        except (TypeError, ValueError):
            pass
    return status_id in w.statuses, lead


def _build_message(lead_id: int, lead: dict, waited_min: float, w: _Watch) -> str:
    """Текст под адресата, и разница тут не косметическая.

    Руководству - факт и виновник: имя менеджера словами, без тегов, звать некого. Чату
    менеджеров - призыв и @тег, иначе сообщение прочитать некому. Правило Кати 28.08.2026
    про два чата. Без ID (03.08.2026) и без точек посередине (26.08.2026) в обоих.
    """
    waited = _waited_words(waited_min)
    responsible = lead.get("responsible_user_id")
    if w.tags:
        lines = [
            f"🚨 {w.subject} не взяли в работу {waited} — возьмите",
            academy_mentions_for(responsible),
        ]
    else:
        lines = [
            f"🚨 {w.subject} не взяли в работу {waited}",
            f"Ответственный: {_esc(manager_name(responsible))}",
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
    changed = False
    for lead_id, st in list(_pending.items()):
        if (now - st["since"]).total_seconds() >= _TTL_HOURS * 3600:
            _pending.pop(lead_id, None)
            changed = True
            continue
        waited = worktime_minutes(st["since"], now)
        if waited >= NEW_LEAD_ESCALATE_MINUTES:
            due.append((lead_id, st, waited))

    for lead_id, st, waited in due:
        try:
            w = _watch_for(st.get("pipeline"))
            if w is None:
                # Сторож этой воронки погасили, пока лид ждал. Счётчик снимаем: держать его
                # вечно незачем, а включат обратно - заведётся со следующего вебхука.
                _pending.pop(lead_id, None)
                changed = True
                continue
            untouched, lead = await _still_untouched(lead_id, w)
            if untouched is None:
                continue  # amo не ответил - вернёмся на следующем проходе
            _pending.pop(lead_id, None)
            changed = True
            if not untouched:
                continue  # взяли, вебхук о смене этапа просто не дошёл
            chat_id, thread_id = _destination(w)
            responsible = (lead or {}).get("responsible_user_id")
            d = alerts.decide(
                w.event, legacy_text=_build_message(lead_id, lead or {}, waited, w),
                parse_mode="HTML", chat_id=chat_id, thread_id=thread_id, lead=lead,
                responsible_id=responsible if w.route else None,
                route_by_responsible=w.route,
                values={
                    "сколько_ждали": _waited_words(waited),
                    "ответственный": manager_name(responsible),
                    "теги": academy_mentions_for(responsible) if w.tags else "",
                    "сделка": ((lead or {}).get("name") or "").strip(),
                    "ссылка_на_сделку": alerts.lead_link(lead_id),
                },
            )
            if d is None:
                logger.info("%s: эскалация выключена в панели (сделка %s)", w.subject, lead_id)
                continue
            ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
            logger.info(
                "%s: эскалация %s (сделка %s, %s рабочих минут на входе)",
                w.subject, "отправлена" if ok else "НЕ отправлена", lead_id, int(waited),
            )
            if ok and w.panel:
                # Лента панели вторым каналом - только у Академии (ТЗ Кати 29.09.2026).
                # Розничная эскалация идёт руководству и в ленту не дублируется: там
                # адресат не человек со сделкой, а чат руководителей.
                plain, link = alerts.strip_link(d.text)
                alerts.panel_notify_bg(
                    kind=w.event, level="warn",
                    title=f"{w.subject} не взяли в работу {_waited_words(waited)}",
                    body=plain, url=link,
                    dedupe_key=f"{w.event}:{lead_id}",
                    amo_user_id=responsible,
                    fallback_amo_user_id=ACADEMY_PANEL_FALLBACK_AMO_ID,
                )
        except Exception:
            logger.exception("Новый лид: ошибка проверки сделки %s", lead_id)

    # Снятые счётчики фиксируем ОДНОЙ записью за проход, а не на каждую сделку: проход
    # разбирает пачку, и десять записей файла подряд ничего не добавляют к одной.
    if changed:
        _save()


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
    # Запускаемся, если сторожим ХОТЬ ОДНУ воронку. До 29.09.2026 здесь стояло
    # `if ROP_CHAT_ID is None: return`, и пустой чат руководства погасил бы заодно Академию,
    # которой этот чат не нужен вовсе.
    live = [w for w in _WATCHED.values() if _is_on(w)]
    if not live:
        logger.info("Новый лид: ни одной воронки не сторожим (нет чатов) - счётчик выключен")
        return
    _load()
    if _watch_task is None:
        _watch_task = asyncio.create_task(_loop())
        logger.info(
            "Новый лид: счётчик запущен (порог %s рабочих мин, окно %s:00-%s:00 МСК, воронки: %s)",
            NEW_LEAD_ESCALATE_MINUTES, NEW_LEAD_WINDOW_START_H, NEW_LEAD_WINDOW_END_H,
            ", ".join(w.subject for w in live),
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
