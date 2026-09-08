"""Авто-режим ОП розница: движок ведения сделок.

Устройство целиком - `features/avtorezhim-op-roznica/DESIGN.md` в рабочей папке, разбор
рисков - `REVIEW-zamysla.md` там же. Здесь только код, решения заново не переобъясняются.

Что делает: ведёт сделку по этапам маршрута, настроенного в team-panel. На этапе запускает
ботов, ждёт доставки сообщения и ответа клиента, а когда что-то идёт не так - останавливается
и зовёт человека. В конце маршрута решает по способу оплаты, куда сделку двинуть, и передаёт
её уже работающим модулям (`ozon_invoice`, `office_transfer`).

Три вещи, без которых движок опасен, и все три здесь есть:

1. **Гейт от повторного вебхука.** `/lead_change` приходит на ЛЮБОЕ изменение сделки, а не на
   смену этапа - по сделке, спокойно стоящей на этапе, прилетят десятки вебхуков. Гейт -
   `autopilot_store.claim()` на паре «сделка и этап»: первый вебхук берёт работу, остальные
   получают отказ. Атомарность даёт первичный ключ, а не проверка «посмотрим и вставим».
2. **Две отметки запуска бота.** `launch_attempted_at` до вызова amoCRM, `launch_ok_at` после.
   Рестарт между ними оставляет запись «попытка была, результат неизвестен» - такую НЕ
   перезапускаем, а зовём человека: лучше не отправить, чем отправить дважды.
3. **Белый список в режиме «Тест».** В воронке «Тест» ничто не мешает завести сделку с
   реальным человеком. Контакта нет в списке - бот не запускается вовсе.

⚠️ **Движок не пишет в поле «Ссылка для оплаты» ни при каких обстоятельствах.** Заполненное
поле - это гейт, по которому `ozon_invoice` решает НЕ создавать счёт.

⚠️ **Перевод в УР - ЗАВЕРШЕНИЕ маршрута, а не «сделку увели».** `office_transfer` уносит её в
Офис за пару секунд, и без этой оговорки каждый успешный прогон кончался бы ложным алертом.
"""
import asyncio
import datetime
import json
import logging
from typing import Any

import httpx

import amo_service
import autopilot_settings_client as settings_client
import autopilot_store as store
import telegram_bot
from tg_recipients import NOTIFY_CHAT_ID, NOTIFY_THREAD_ID, mentions_for
from waybill_config import (
    AUTOPILOT_ENABLED,
    AUTOPILOT_HOURLY_CAP,
    AUTOPILOT_STATE_TTL_DAYS,
    AUTOPILOT_TICK_INTERVAL_S,
    TEAM_PANEL_BASE_URL,
    TEAM_PANEL_INGEST_TOKEN,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))
_UTC = datetime.timezone.utc

_loop_task: asyncio.Task | None = None
_bg_tasks: set[asyncio.Task] = set()

# Счётчик действий за час - потолок против всплеска вебхуков при массовой правке в amoCRM.
_hour_bucket: tuple[int, int] = (0, 0)  # (час эпохи, сколько действий)
_cap_warned_hour: int = -1

# Статусы Wazzup, которые считаем доставкой. `read` тоже: прочитанное доставлено по
# определению. ⚠️ У Telegram `delivered` не приходит ВООБЩЕ - там успех это `sent`.
DELIVERED_STATUSES = {"delivered", "read"}
TELEGRAM_CHAT_TYPES = {"telegram", "tgapi"}


def is_enabled() -> bool:
    """Два независимых рубильника: флаг на сервере и режим в панели. Любой из них выключает."""
    return AUTOPILOT_ENABLED and settings_client.get_mode() != "off"


# ── рабочие часы ────────────────────────────────────────────────────────────────

def _work_hours() -> list[dict[str, str]]:
    return list((settings_client.get_settings().get("settings") or {}).get("work_hours") or [])


def in_work_hours(now: datetime.datetime | None = None) -> bool:
    """Пустой список часов означает «не работает никогда», а не «работает всегда».

    Это не педантизм: пустые часы - способ приостановить робота, не теряя настроек, и читать
    их как «круглосуточно» значило бы включить его ровно тогда, когда человек выключал.
    """
    hours = _work_hours()
    if not hours:
        return False
    hhmm = (now or datetime.datetime.now(_MSK)).astimezone(_MSK).strftime("%H:%M")
    return any(iv["start"] <= hhmm < iv["end"] for iv in hours)


def next_work_moment(now: datetime.datetime | None = None) -> datetime.datetime | None:
    """Ближайшее начало рабочего промежутка СТРОГО в будущем.

    Сперва среди сегодняшних (покрывает и «до первого», и «в перерыве»), потом самое раннее
    завтра. Приём взят у `lead_distribution._next_work_moment`: там он убрал искусственное
    ожидание до утра, когда следующий промежуток начинается сегодня же.
    """
    hours = _work_hours()
    if not hours:
        return None
    moment = (now or datetime.datetime.now(_MSK)).astimezone(_MSK)
    today = moment.date()
    starts = []
    for iv in hours:
        hh, mm = (int(x) for x in iv["start"].split(":"))
        starts.append(datetime.datetime.combine(today, datetime.time(hh, mm), tzinfo=_MSK))
    upcoming = [t for t in starts if t > moment]
    if upcoming:
        return min(upcoming)
    earliest = min(iv["start"] for iv in hours)
    hh, mm = (int(x) for x in earliest.split(":"))
    return datetime.datetime.combine(
        today + datetime.timedelta(days=1), datetime.time(hh, mm), tzinfo=_MSK,
    )


# ── потолок действий ────────────────────────────────────────────────────────────

def allow_action() -> bool:
    """Потолок в час. Упёрлись - один раз пишем в технический чат и молчим до нового часа."""
    global _hour_bucket, _cap_warned_hour
    hour = int(datetime.datetime.now(_UTC).timestamp() // 3600)
    bucket_hour, count = _hour_bucket
    if bucket_hour != hour:
        _hour_bucket = (hour, 0)
        count = 0
    if count >= AUTOPILOT_HOURLY_CAP:
        if _cap_warned_hour != hour:
            _cap_warned_hour = hour
            alert_tech(
                f"Упёрся в потолок {AUTOPILOT_HOURLY_CAP} действий в час и остановился до конца "
                "часа. Похоже на массовую правку сделок в amoCRM."
            )
        return False
    _hour_bucket = (hour, count + 1)
    return True


def reset_hour_bucket() -> None:
    """Только для тестов."""
    global _hour_bucket, _cap_warned_hour
    _hour_bucket = (0, 0)
    _cap_warned_hour = -1


# ── алерты и журнал ─────────────────────────────────────────────────────────────

def _send_bg(text: str, *, chat_id=None, thread_id=None) -> None:
    """Отправка в фоне: алерт не должен задерживать разбор вебхука, а сбой Телеграма не
    должен ронять ведение сделки. Ссылку на задачу держим, иначе сборщик мусора может
    забрать её на полпути - тот же приём, что у `ozon_invoice._init_tasks`.
    """
    task = asyncio.create_task(
        telegram_bot.send_alert(text, chat_id=chat_id, message_thread_id=thread_id)
    )
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


def alert_tech(text: str) -> None:
    """Наши поломки - в технический чат (адресат по умолчанию у `send_alert`)."""
    _send_bg("🤖 Авто-режим" + chr(10) + text)


def alert_op(text: str, responsible_id=None) -> None:
    """Событие, требующее менеджера, - в чат отдела продаж, топик УВЕДОМЛЕНИЯ, с тегом
    ответственного.

    Правило Кати 28.08.2026: в чат ОП идёт СОБЫТИЕ (клиент ждёт), в чат руководства -
    ПРОВАЛ. Авто-режим шлёт только события: он останавливается ДО того, как что-то стало
    провалом, поэтому в чат руководства не пишет вовсе.
    """
    body = "🤖 Авто-режим" + chr(10) + text
    if responsible_id:
        try:
            mention = mentions_for(responsible_id)
        except Exception:
            mention = ""
        if mention:
            body = body + chr(10) + mention
    _send_bg(body, chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID)


def journal_bg(payload: dict[str, Any]) -> None:
    """Строка журнала уезжает в панель. Журнал важнее чата: Телеграм у нас уже глушился на
    сутки одним сетевым сбоем (28-29.08.2026), а журнал - источник правды о работе робота."""
    task = asyncio.create_task(_journal(payload))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _journal(payload: dict[str, Any]) -> None:
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        logger.info("autopilot journal (панель не настроена): %s",
                    json.dumps(payload, ensure_ascii=False))
        return
    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/autopilot/run"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                url, json=payload, headers={"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN},
            )
        if resp.status_code >= 400:
            logger.warning("autopilot: журнал не принят панелью, HTTP %s", resp.status_code)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot: не удалось записать строку журнала")


def run_row(lead: dict, stage: dict | None = None, **extra) -> dict[str, Any]:
    """Общая часть строки журнала. Имена кладём снимками: журнал читают через месяцы, когда
    бот переименован, а этап переехал."""
    row = {
        "mode": settings_client.get_mode(),
        "lead_id": int(lead.get("id") or 0),
        "lead_name": str(lead.get("name") or ""),
        "pipeline_id": int(lead.get("pipeline_id") or 0),
        "status_id": int(lead.get("status_id") or 0),
        "status_name": str((stage or {}).get("status_name") or ""),
    }
    row.update(extra)
    return row


# ── условия ─────────────────────────────────────────────────────────────────────

def lead_field_value(lead: dict, field: str) -> Any:
    """Значение поля сделки для условия. Неизвестное поле даёт None - условие по нему честно
    не выполнится, вместо того чтобы уронить весь разбор."""
    if field.startswith("cf:"):
        try:
            return amo_service.get_custom_field_value(lead, int(field[3:]))
        except (TypeError, ValueError):
            return None
    if field == "responsible":
        return lead.get("responsible_user_id")
    if field == "source":
        return lead.get("source_id")
    if field == "tag":
        return [t.get("name") for t in amo_service.get_tags(lead)]
    return None


def _match_one(value: Any, op: str, expected: Any) -> bool:
    if op == "filled":
        return value not in (None, "", [], {})
    if op == "not_filled":
        return value in (None, "", [], {})

    if isinstance(value, list):
        haystack = [str(v).strip().lower() for v in value]
        needle = str(expected if expected is not None else "").strip().lower()
        if op == "eq":
            return needle in haystack
        if op == "ne":
            return needle not in haystack
        if op == "contains":
            return any(needle in h for h in haystack)
        if op == "not_contains":
            return all(needle not in h for h in haystack)
        return False

    left = str(value if value is not None else "").strip().lower()
    right = str(expected if expected is not None else "").strip().lower()
    if op == "eq":
        return left == right
    if op == "ne":
        return left != right
    if op == "contains":
        return right in left
    if op == "not_contains":
        return right not in left
    return False


def conditions_match(lead: dict, conditions: list[dict]) -> bool:
    """Плоский список со связкой «и / или», слева направо, без скобок - как у сейлсботов amoCRM.

    Пустой список значит «всегда». Порядок именно левый, а не «сначала все И»: редактор
    обещает человеку «считается слева направо», и считать иначе значит соврать экрану.
    """
    if not conditions:
        return True
    result: bool | None = None
    for cond in conditions:
        ok = _match_one(
            lead_field_value(lead, str(cond.get("field") or "")),
            str(cond.get("op") or ""),
            cond.get("value"),
        )
        if result is None:
            result = ok
        elif str(cond.get("join") or "and") == "or":
            result = result or ok
        else:
            result = result and ok
    return bool(result)


# ── сравнение ответа клиента ────────────────────────────────────────────────────

def normalize_answer(text: Any) -> str:
    """Ровно три шага, те же, что в панели (`app/autopilot/validation.py::normalize_answer`):
    обрезать пробелы, привести к нижнему регистру, заменить «ё» на «е».

    ⚠️ Это ЕДИНСТВЕННАЯ часть правила сравнения, живущая на нашей стороне: список вариантов
    панель присылает уже нормализованным (`stop_answers_norm`), а входящий текст нормализуем
    мы. Меняются шаги - меняются в обоих местах, это записано контрактом в DESIGN.md. Ловушка
    «Да, все верно» против «Да, всё верно» стоила живого кейса 23.08.2026.
    """
    return str(text or "").strip().lower().replace("ё", "е")


def answer_decision(bot: dict, answer: str) -> str:
    """Что делать с ответом клиента: `advance` - дальше по маршруту, `stop` - позвать человека."""
    mode = str(bot.get("stop_mode") or "any")
    listed = [
        normalize_answer(a)
        for a in (bot.get("stop_answers_norm") or bot.get("stop_answers") or [])
    ]
    got = normalize_answer(answer)
    if mode == "never":
        return "advance"
    if mode == "any":
        return "stop"
    if mode == "listed":
        # «Только на эти ответы»: перечисленное ведёт дальше, всё прочее останавливает.
        return "advance" if got in listed else "stop"
    if mode == "except":
        # «На любой ответ, кроме этих»: перечисленное останавливает.
        return "stop" if got in listed else "advance"
    return "stop"


# Вердикт доставки. Ровно четыре исхода, и «провал» среди них - самый дорогой.
VERDICT_OK = "ok"          # дошло хотя бы одним каналом
VERDICT_WAIT = "wait"      # ещё ждём: попытки могут продолжаться
VERDICT_FAILED = "failed"  # окно вышло, все известные попытки отбиты
VERDICT_SILENT = "silent"  # окно вышло, а статусов не пришло ни одного


def delivery_ok(statuses: list[dict]) -> bool:
    """Дошло ли хотя бы одним каналом.

    ⚠️ Бот перебирает каналы: не ушло телеграмом - пробует WhatsApp. Успех ЛЮБОГО канала
    означает, что клиент сообщение получил, даже если соседний отбил и оставил в ленте amoCRM
    примечание «SYSTEM WZ». Прочитать такую жалобу как «клиент ничего не получил» мы уже
    один раз успели, 08.09.2026.

    ⚠️ У Telegram статуса «доставлено» не бывает ВООБЩЕ: там путь `sent` → `read`. Ждать
    `delivered` значит ждать вечно, поэтому на телеграм-канале успехом считаем `sent`. Это
    слабее, чем у WhatsApp - `sent` означает «Wazzup принял», а не «человек увидел», - и
    поэтому в журнале такая доставка подписывается отдельно, см. `delivery_note`.
    """
    for st in statuses:
        status = str(st.get("status") or "").lower()
        chat_type = str(st.get("chatType") or st.get("chat_type") or "").lower()
        if status in DELIVERED_STATUSES:
            return True
        if status == "sent" and chat_type in TELEGRAM_CHAT_TYPES:
            return True
    return False


def delivery_verdict(statuses: list[dict], waited_s: float, wait_limit_s: float) -> str:
    """Что делать прямо сейчас: ждать, идти дальше или звать человека.

    ⚠️ Отказ ОДНОГО канала - это НЕ провал, пока не вышло окно ожидания. Сколько каналов
    попробует бот, мы заранее не знаем: он решает это сам своими шагами. Поэтому единственный
    честный признак провала - «окно вышло, а успеха так и нет», и до конца окна мы ждём даже
    при видимых отказах. Раньше здесь стоял отказ по первой же ошибке, и это была ровно та
    ошибка, на которой мы обожглись 08.09.2026, только записанная в код.

    Отдельный исход `silent` - когда за всё окно не пришло ни одного статуса. Это другой
    случай: сообщение не просто не дошло, а, похоже, не отправлялось вовсе (нет телефона и
    юзернейма в карточке, канал до неё не работает). Человеку об этом надо сказать другими
    словами, поэтому и исход отдельный.
    """
    if delivery_ok(statuses):
        return VERDICT_OK
    if waited_s < wait_limit_s:
        return VERDICT_WAIT
    return VERDICT_FAILED if statuses else VERDICT_SILENT


def delivery_note(statuses: list[dict]) -> str:
    """Человеческая подпись для журнала: что случилось по каждому каналу.

    Пишем ПОКАНАЛЬНО и с оговоркой про телеграм. Без неё строка «отправлено» у телеграма
    читается как более слабая, чем «доставлено» у ватсапа, и человек идёт искать поломку там,
    где её нет.
    """
    if not statuses:
        return "статусов от Wazzup не пришло ни одного"
    parts = []
    titles = {"sent": "отправлено", "delivered": "доставлено", "read": "прочитано",
              "error": "отказ канала"}
    for st in statuses:
        status = str(st.get("status") or "").lower()
        chat_type = str(st.get("chatType") or st.get("chat_type") or "").lower()
        name = "Telegram" if chat_type in TELEGRAM_CHAT_TYPES else "WhatsApp"
        text = titles.get(status, status or "без статуса")
        if status == "sent" and chat_type in TELEGRAM_CHAT_TYPES:
            text = "отправлено, подтверждения доставки телеграм не присылает"
        parts.append(name + ": " + text)
    return ", ".join(parts)



# ── жизненный цикл ──────────────────────────────────────────────────────────────

async def init() -> None:
    if not AUTOPILOT_ENABLED:
        logger.info("autopilot: выключен флагом AUTOPILOT_ENABLED")
        return
    await asyncio.to_thread(store.init)
    settings_client.start()
    await report_unfinished_launches()
    global _loop_task
    _loop_task = asyncio.create_task(_tick_loop())
    logger.info("autopilot: включён, тик каждые %s сек", AUTOPILOT_TICK_INTERVAL_S)


async def shutdown() -> None:
    global _loop_task
    if _loop_task is not None:
        _loop_task.cancel()
        try:
            await _loop_task
        except asyncio.CancelledError:
            pass
        _loop_task = None
    await settings_client.stop()


async def report_unfinished_launches() -> None:
    """Разбор после рестарта: были попытки запуска без подтверждения.

    Такие НЕ перезапускаем. Возможно, бот отработал и клиент уже получил сообщение, а мы не
    успели записать - повторный запуск отправил бы второе. Зовём человека и снимаем ведение.
    """
    rows = await asyncio.to_thread(store.list_unfinished_launches)
    for row in rows:
        await asyncio.to_thread(
            store.finish, row["lead_id"], row["status_id"], store.PHASE_STOPPED,
            "рестарт в момент запуска бота",
        )
        alert_op(
            f"Сделка {row['lead_id']}: робот перезапустился в момент запуска бота и не знает, "
            "ушло сообщение или нет. Повторно не отправляю, посмотрите переписку."
        )
    if rows:
        logger.warning("autopilot: %s незавершённых запусков после рестарта", len(rows))


async def _tick_loop() -> None:
    while True:
        try:
            await tick_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("autopilot: ошибка фонового цикла")
        await asyncio.sleep(AUTOPILOT_TICK_INTERVAL_S)


async def tick_once() -> None:
    """Фон делает ровно две вещи: будит уснувших до утра и убирает протухшие записи.

    Таймеров дожима здесь нет и не будет: своих напоминаний молчащему клиенту мы не шлём
    (решение Кати 08.09.2026). Если в воронке есть чужая автоматика напоминания - она и
    работает, движок ей не мешает.
    """
    if not is_enabled():
        return
    dropped = await asyncio.to_thread(store.purge_older_than, AUTOPILOT_STATE_TTL_DAYS)
    if dropped:
        logger.info("autopilot: снято с ведения по сроку давности: %s", dropped)
    if not in_work_hours():
        return
    for row in await asyncio.to_thread(store.list_due):
        logger.info("autopilot: сделка %s проснулась на этапе %s", row["lead_id"], row["status_id"])
        await asyncio.to_thread(
            store.update, row["lead_id"], row["status_id"],
            phase=store.PHASE_LAUNCHING, wake_at=None,
        )
