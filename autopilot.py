"""Авто-режим ОП розница: движок ведения сделок.

Устройство целиком - `features/avtorezhim-op-roznica/DESIGN.md` в рабочей папке, разбор
рисков - `REVIEW-zamysla.md` там же. Здесь только код, решения заново не переобъясняются.

Что делает: ведёт сделку по этапам маршрута, настроенного в team-panel, - и ТОЛЬКО сделку
с типом заявки «Заказ», остальные не трогает. На этапе запускает ботов, ждёт доставки
сообщения и ответа клиента, а когда что-то идёт не так - останавливается и зовёт человека.
В конце маршрута решает по способу оплаты: наложка и оплаченный онлайн едут в успех (дальше
сделку уводит `office_transfer`), неоплаченный онлайн остаётся на месте с красным алертом.

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

## Кого и когда зовём (правка Кати 27.09.2026)

Поводов четыре, и у каждого своё событие в панели - свой текст, свой выключатель, своя
настройка получателей. Раньше всё шло одним кодом `autopilot_event`, и заглушить шум по
одному поводу означало заглушить робота целиком.

| Повод | Событие | Куда |
|---|---|---|
| заявка на входе воронки, которую робот НЕ ведёт (предзаказ, консультация, сделка по сообщению) | `autopilot_lead_unhandled` | чат ОП с тегом + лента |
| сообщение до клиента не дошло | `autopilot_not_delivered` | чат ОП с тегом + лента |
| способ оплаты «Другой способ» - говорим на ВХОДЕ маршрута, не в конце | `autopilot_payment_other` | чат ОП с тегом + лента |
| чат клиента неизвестен: в карточке нет телефона, отследить нечем | `autopilot_no_chat` | чат ОП с тегом + лента |
| клиент молчит дольше срока ожидания (по умолчанию сутки) | `autopilot_no_reply` | чат ОП с тегом + лента |
| любой сбой по сделке: amo не принял, МойСклад молчит, потолок действий, исключение | `autopilot_error` | чат ОП с тегом + лента |
| нужен человек по ходу маршрута (клиент ответил не кнопкой, нет остатка) | `autopilot_event` | чат ОП с тегом + лента |
| наша поломка БЕЗ сделки (панель не посчитала остаток) | `autopilot_failure` | технический чат |

Три гейта, без которых алерт в живой чат заводить нельзя (`knowledge/alerty-kuda-shlem.md`),
здесь такие:

1. **Выключатель** - на каждое событие свой, в панели. Плюс режим робота: в «Тесте» и в
   пилоте всё уходит в технический чат, менеджеров не дёргаем вовсе.
2. **Дедуп** - про заявку говорим один раз (отметка на ДИСКЕ, переживает рестарт), про один
   и тот же сбой по одной сделке - раз в `AUTOPILOT_ERROR_DEDUPE_S`.
3. **Антиспам** - больше `AUTOPILOT_OP_BURST_MAX` сообщений в рабочий чат за окно, и чат
   замолкает до конца окна с объявлением технарям. Лента панели антиспамом НЕ режется.
"""
import asyncio
import datetime
import json
import logging
import re
from typing import Any

import httpx

import amo_service
import autopilot_settings_client as settings_client
import autopilot_store as store
import ms_client
import telegram_bot
import alerts
from tg_recipients import NOTIFY_CHAT_ID, NOTIFY_THREAD_ID, mentions_for
from waybill_config import (
    AUTOPILOT_ENABLED,
    AUTOPILOT_ERROR_DEDUPE_S,
    AUTOPILOT_HOURLY_CAP,
    AUTOPILOT_ANSWER_WINDOW_MIN,
    AUTOPILOT_CATCHUP_INTERVAL_S,
    AUTOPILOT_CATCHUP_LOOKBACK_S,
    AUTOPILOT_CONTACT_RETRY_S,
    AUTOPILOT_OP_BURST_MAX,
    AUTOPILOT_OP_BURST_WINDOW_S,
    AUTOPILOT_REPLY_WAIT_H,
    AUTOPILOT_STATE_TTL_DAYS,
    AUTOPILOT_TICK_INTERVAL_S,
    FIELD_APPLICATION_TYPE,
    FIELD_MOYSKLAD_ORDER_UUID,
    FIELD_PAYMENT_METHOD,
    FIELD_PHONE,
    STATUS_PAYMENT_RECEIVED,
    STATUS_PAYMENT_REQUESTED,
    STATUS_SUCCESS,
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

# ⚠️ Автор исходящего, которого мы считаем НАШИМ сообщением. Wazzup ставит `Admin` всему, что
# отправила автоматика amoCRM (боты, CRM), а сообщения людей приходят с их именами. Без этого
# фильтра доставку шаблона закрывало ЛЮБОЕ эхо в чат - в том числе то, что менеджер написал
# руками (правка Кати 27.09.2026). Замер за неделю: автоматика 259 в WhatsApp и 118 в Telegram
# против 417 и 505 человеческих - шума больше, чем сигнала.
ROBOT_AUTHORS = {"admin", "bot", ""}


def is_robot_echo(author: Any) -> bool:
    """Это исходящее отправила автоматика, а не человек.

    ⚠️ Пустой автор считаем нашим намеренно: у части каналов Wazzup имени не присылает вовсе,
    и трактовать пустоту как «написал менеджер» значило бы терять подтверждения доставки.
    """
    return str(author or "").strip().lower() in ROBOT_AUTHORS


def is_enabled() -> bool:
    """Два независимых рубильника: флаг на сервере и режим в панели. Любой из них выключает."""
    return AUTOPILOT_ENABLED and settings_client.get_mode() != "off"


def is_shadow() -> bool:
    """Режим «Призрак»: думает как в бою, наружу не делает ничего (Катя 27.09.2026).

    Воронка боевая, настройки боевые, поток сделок боевой - разница ровно в том, что робот
    не отправляет сообщений, не переводит сделки и не пишет людям. Каждое решение уходит в
    журнал словами «сделал бы то-то».

    ⚠️ Проверка стоит у САМОЙ границы с внешним миром - в `launch_bot`, `move_to` и
    `dispatch_op`, - а не в начале разбора. Если бы призрак отваливался раньше, он бы и
    думал иначе, чем бой, и смысла в такой репетиции не было бы никакого.
    """
    return settings_client.get_mode() == "shadow"


def shadow_note(text: str) -> str:
    """Причина в журнале от лица призрака: не «сделал», а «сделал бы»."""
    return f"призрак: {text}" if is_shadow() else text


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
    # Призраку потолок не нужен: он ничего не делает, а расходуя бюджет, он бы упирался в
    # него и записывал в журнал «остановился из-за потолка» - то есть врал бы о бое.
    if is_shadow():
        return True
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

def _send_bg(text: str, *, chat_id=None, thread_id=None, parse_mode=None) -> None:
    """Отправка в фоне: алерт не должен задерживать разбор вебхука, а сбой Телеграма не
    должен ронять ведение сделки. Ссылку на задачу держим, иначе сборщик мусора может
    забрать её на полпути - тот же приём, что у `ozon_invoice._init_tasks`.
    """
    task = asyncio.create_task(
        telegram_bot.send_alert(text, chat_id=chat_id, message_thread_id=thread_id, parse_mode=parse_mode)
    )
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


# Коды событий уведомлений. Раздельные, а не один «событие робота» на всё (правка Кати
# 27.09.2026): у каждого свой текст на экране панели и свой выключатель. Пока код был один,
# выключить шум по одному поводу означало заглушить робота целиком.
EVENT_EVENT = "autopilot_event"                    # нужен человек: клиент ответил не как ждали
EVENT_LEAD_UNHANDLED = "autopilot_lead_unhandled"  # заявка, которую робот НЕ ведёт
EVENT_NOT_DELIVERED = "autopilot_not_delivered"    # сообщение до клиента не дошло
EVENT_PAYMENT_OTHER = "autopilot_payment_other"    # способ оплаты, который робот не понимает
EVENT_NO_CHAT = "autopilot_no_chat"                # чат клиента неизвестен: отследить нечем
EVENT_NO_REPLY = "autopilot_no_reply"              # клиент молчит дольше срока ожидания
EVENT_ERROR = "autopilot_error"                    # сбой на конкретной сделке
EVENT_FAILURE = "autopilot_failure"                # наша поломка, сделки за ней нет

# Антиспам рабочего чата: отметки времени отправок в топик УВЕДОМЛЕНИЯ и момент, до которого
# молчим. Приём взят у `wazzup_delivery`: при массовом сбое сотня сообщений менеджеру не
# помогает, а полная картина всё равно лежит в ленте панели - лента антиспамом не режется.
_op_sent_at: list[float] = []
_op_muted_until: float = 0.0

# Сбои, о которых уже сказали: ключ → когда. Без этого ошибка фонового цикла звонила бы раз
# в минуту (тик), а ошибка разбора вебхука - на каждый вебхук по сделке.
_error_said_at: dict[str, float] = {}

# Когда последний раз подбирали пропущенное из переписки панели (unix, UTC).
_catchup_at: float = 0.0

# ⚠️ Пары «сделка и этап», куда сделку перевёл САМ робот. Нужны журналу: без них запись
# «маршрут пройден» появлялась и там, где этап сменил человек, и читалась как заслуга робота
# (замечание Кати 27.09.2026 по сделке 36564965). Метка живёт секунды - от `move_to` до входа
# в `handle_lead_change`, который её и снимает.
_moved_by_us: set[tuple[int, int]] = set()


def alert_tech(text: str) -> None:
    """Наши поломки - в технический чат (адресат по умолчанию у `send_alert`)."""
    if is_shadow():
        logger.info("autopilot: призрак молчит, сказал бы в техчат: %s", text)
        return
    d = alerts.decide(
        EVENT_FAILURE, legacy_text="🤖 Авто-режим" + chr(10) + text,
        values={"текст_поломки": text},
    )
    if d is None:
        logger.info("Авто-режим: уведомление о поломке выключено в панели")
        return
    _send_bg(d.text, chat_id=d.chat_id, thread_id=d.thread_id, parse_mode=d.parse_mode)


def op_burst_allows(now: float | None = None) -> bool:
    """Пускать ли ещё одно сообщение в рабочий чат. Первое «нет» за окно - с объявлением.

    Окно скользящее, а не «сбрасываем счётчик каждые десять минут»: ровные окна дают всплеск
    на стыке - девять сообщений в конце одного окна и девять в начале следующего.
    """
    global _op_muted_until
    moment = now if now is not None else datetime.datetime.now(_UTC).timestamp()
    if moment < _op_muted_until:
        return False
    edge = moment - AUTOPILOT_OP_BURST_WINDOW_S
    _op_sent_at[:] = [t for t in _op_sent_at if t >= edge]
    if len(_op_sent_at) < AUTOPILOT_OP_BURST_MAX:
        _op_sent_at.append(moment)
        return True
    _op_muted_until = moment + AUTOPILOT_OP_BURST_WINDOW_S
    alert_tech(
        f"Алертов в рабочий чат больше {AUTOPILOT_OP_BURST_MAX} за "
        f"{AUTOPILOT_OP_BURST_WINDOW_S // 60} мин - похоже на массовый сбой. Дальше в этом "
        "окне в чат ОП молчу, чтобы не залить топик; всё видно в ленте панели и в журнале."
    )
    return False


def reset_op_burst() -> None:
    """Только для тестов: окно антиспама и заглушка - чистые."""
    global _op_muted_until
    _op_sent_at.clear()
    _op_muted_until = 0.0
    _error_said_at.clear()


def error_is_fresh(key: str, now: float | None = None) -> bool:
    """Про этот сбой ещё не говорили в пределах окна дедупа."""
    moment = now if now is not None else datetime.datetime.now(_UTC).timestamp()
    said = _error_said_at.get(key)
    if said is not None and moment - said < AUTOPILOT_ERROR_DEDUPE_S:
        return False
    if len(_error_said_at) > 2000:
        _error_said_at.clear()
    _error_said_at[key] = moment
    return True


def alert_op(text: str, responsible_id=None, lead: dict | None = None) -> None:
    """Событие, требующее менеджера, - в чат отдела продаж, топик УВЕДОМЛЕНИЯ, с тегом
    ответственного. Совместимая обёртка: события со своим кодом зовут `dispatch_op`.

    Правило Кати 28.08.2026: в чат ОП идёт СОБЫТИЕ (клиент ждёт), в чат руководства -
    ПРОВАЛ. Авто-режим шлёт только события: он останавливается ДО того, как что-то стало
    провалом, поэтому в чат руководства не пишет вовсе.

    ⚠️ `lead` нужен ради ссылки и названия. Без него шаблон панели остаётся без ссылки на
    сделку: 27.09.2026 так и ушло «платёжная система говорит оплачено, а в МойСкладе оплаты
    нет» - текст был, тег был, а открыть сделку из чата было нечем.
    """
    lead_id = int((lead or {}).get("id") or 0)
    name = str((lead or {}).get("name") or "").strip()
    dispatch_op(
        EVENT_EVENT, text, responsible_id=responsible_id,
        values={"текст_события": text, "сделка": name or "без названия",
                "ссылка_на_сделку": alerts.lead_link(lead_id) if lead_id else ""},
        panel_title="Авто-режим: нужен человек",
        panel_url=AMO_LEAD_URL.format(lead_id) if lead_id else None,
    )


def dispatch_op(event_key: str, text: str, *, responsible_id=None,
                values: dict[str, Any] | None = None, panel_title: str,
                panel_level: str = "critical", panel_url: str | None = None,
                dedupe_key: str | None = None, bypass_burst: bool = False) -> None:
    """Одно место, через которое авто-режим говорит с людьми: чат и лента панели.

    Порядок именно такой, и он важен: **сперва лента, потом чат**. Лента - основной канал
    (Телеграм у нас глушился на сутки одним сетевым сбоем 28-29.08.2026) и она не режется
    антиспамом: там полная картина даже тогда, когда чат замолчал.

    ⚠️ В ограниченных режимах (тест, пилот боя) менеджеров не дёргаем: события читает тот,
    кто тестирует, и читает он их в техническом чате. Иначе в рабочий топик УВЕДОМЛЕНИЯ
    полетело бы «клиент ответил...» по сделке с тестовым контактом.
    """
    # ⚠️ Призрак не говорит ни с кем: ни чат ОП, ни технический, ни лента панели. Сообщение
    # о событии, которого не было, пугает менеджера ровно так же, как настоящее, - а робот в
    # этом режиме ничего не сделал (решение Кати 27.09.2026). Что алерт УШЁЛ БЫ, видно в
    # строке журнала: там стоит адресат.
    if is_shadow():
        logger.info("autopilot: призрак молчит, сказал бы людям (%s): %s", event_key, text)
        return
    plain = _LINK_RE.sub(lambda m: m.group(2) or m.group(1), text)
    found = _LINK_RE.search(text)
    if settings_client.get_mode() == "live":
        # В «Тесте» ленту не трогаем - тестовый шум приучил бы людей её игнорировать.
        panel_notify_bg(
            kind=event_key, level=panel_level, title=panel_title, body=plain,
            url=panel_url or (found.group(1) if found else None),
            dedupe_key=dedupe_key,
        )
    limited = limited_mode()
    if limited:
        label = "ТЕСТОВЫЙ прогон" if limited == "тест" else "ПИЛОТ прода"
        _send_bg("🤖 Авто-режим, " + label + chr(10) + text)
        return
    if not bypass_burst and not op_burst_allows():
        logger.warning("autopilot: алерт в чат ОП придержан антиспамом (%s)", event_key)
        return
    mention = ""
    if responsible_id:
        try:
            mention = mentions_for(responsible_id)
        except Exception:
            mention = ""
    body = "🤖 Авто-режим" + chr(10) + text + ((chr(10) + mention) if mention else "")
    d = alerts.decide(
        event_key, legacy_text=body, chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID,
        responsible_id=responsible_id, values={**(values or {}), "теги": mention},
    )
    if d is None:
        logger.info("Авто-режим: событие %s выключено в панели", event_key)
        return
    _send_bg(d.text, chat_id=d.chat_id, thread_id=d.thread_id, parse_mode=d.parse_mode)


def alert_error(what: str, *, lead: dict | None = None, lead_id: int | None = None,
                dedupe: str | None = None) -> None:
    """Сбой. Есть сделка - зовём менеджера в рабочий чат: робот по ней дальше не пойдёт, и
    доделывать руками ему. Сделки нет - это только наша поломка, менеджеру делать нечего.

    Требование Кати 27.09.2026: «любой сбой = алерт в чат», и в чате должны быть тег
    ответственного, суть ошибки и ссылка на сделку.
    """
    ident = int((lead or {}).get("id") or lead_id or 0)
    if not error_is_fresh(f"{ident}:{dedupe or what}"):
        logger.info("autopilot: о сбое уже говорили, молчу: %s", what)
        return
    if not ident:
        alert_tech(what)
        return
    name = str((lead or {}).get("name") or "").strip()
    dispatch_op(
        EVENT_ERROR, f"{lead_link(ident, name)}: {what}",
        responsible_id=(lead or {}).get("responsible_user_id"),
        values={"сделка": name or "без названия", "что_случилось": what,
                "ссылка_на_сделку": alerts.lead_link(ident)},
        panel_title="Авто-режим: сбой", panel_url=AMO_LEAD_URL.format(ident),
    )


# Ссылка в тексте алерта - html для Телеграма. Лента панели рендерит плоский текст,
# поэтому для неё тег вынимается: текст остаётся словами, адрес уезжает в url.
_LINK_RE = re.compile(r"<a href=\"([^\"]+)\">([^<]*)</a>")


def panel_notify_bg(*, kind: str, title: str, body: str, url: str | None = None,
                    level: str = "warn", dedupe_key: str | None = None) -> None:
    """Уведомление в ленту панели - вторым каналом рядом с Телеграмом.

    Лента, а не чат - основной канал: Телеграм у нас уже глушился на сутки одним
    сетевым сбоем (28-29.08.2026). Шлём фоном и не ждём: сбой доставки уведомления
    не должен трогать ведение сделки.

    ⚠️ Призрак в ленту не пишет: лента - такой же разговор с человеком, как чат, и
    уведомление о событии, которого не было, читается наравне с настоящим. Гейт стоит
    ЗДЕСЬ, у самой отправки, чтобы его не приходилось помнить в каждом вызове.
    """
    if is_shadow():
        logger.info("autopilot: призрак молчит, положил бы в ленту (%s): %s", kind, title)
        return
    task = asyncio.create_task(_panel_notify(kind=kind, title=title, body=body,
                                             url=url, level=level,
                                             dedupe_key=dedupe_key))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _panel_notify(**payload) -> None:
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return
    body = {k: v for k, v in payload.items() if v is not None}
    body["audience_cap"] = "manage_autopilot"
    url = f"{TEAM_PANEL_BASE_URL.rstrip(chr(47))}/api/ingest/notification"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                url, json=body, headers={"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN},
            )
        if resp.status_code >= 400:
            logger.warning("autopilot: панель не приняла уведомление, HTTP %s",
                           resp.status_code)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot: не удалось отправить уведомление в панель")


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


# Приветствия, которыми клиент открывает разговор. Список закрытый и короткий намеренно:
# это предохранитель, а не словарь вежливости, и каждое лишнее слово в нём - риск проглотить
# осмысленный ответ.
_GREETINGS = (
    "здравствуйте", "здравствуй", "здравствуйте всем", "здрасте", "здрасьте",
    "привет", "приветствую", "добрый день", "добрый вечер", "доброе утро", "добрый",
    "доброго времени суток", "доброго дня", "доброго вечера", "доброе утро всем",
    "hi", "hello", "good day",
)

# Сколько ждём продолжения после одинокого приветствия. Замер по переписке 01.10.2026: зазор
# до следующего сообщения клиента - медиана 13 секунд, 80% укладываются в 40 секунд, 88% в 75.
# Десять минут берём с большим запасом: цена ожидания - отложенный алерт менеджеру, цена
# спешки - потерянное подтверждение заказа.
GREETING_GRACE_S = 10 * 60


def bare_greeting(text: Any) -> bool:
    """Сообщение состоит ТОЛЬКО из приветствия и ничего больше не несёт.

    Правило просила Катя 01.10.2026. Повод - заказ 19400: клиент написал «Здравствуйте», через
    СЕКУНДУ «Да», а робот успел разобрать первое, не нашёл в нём слова согласия, встал и позвал
    менеджера. Второе сообщение в журнал уже не попало: сделка снята с ожидания.

    ⚠️ Проверяем «ТОЛЬКО приветствие», а не «содержит приветствие», и разница тут не
    стилистическая. В нашем же журнале лежит ответ 28.09.2026 по заказу 19309: «Здравствуйте.
    С учётом того, что в составе вы ничего не написали, пока подтвердить не могу». Это отказ,
    и проглотить его нельзя. «Содержит приветствие» проглотило бы.

    ⚠️ Чистим всё, что не буква: точки, восклицательные знаки, смайлики. «Здравствуйте 👋» -
    то же самое приветствие, а «Здравствуйте, 2 шт» - уже нет.
    """
    norm = normalize_answer(text)
    if not norm:
        return False
    letters = re.sub(r"[^a-zа-я]+", " ", norm).strip()
    return letters in _GREETINGS


def answer_decision(bot: dict, answer: str) -> str:
    """Что делать с ответом клиента: `advance` - следующий шаг, `stop` - позвать человека.

    Режимы названы с экрана, заголовок там - «Когда идём дальше», и следующий шаг это не
    обязательно следующий ЭТАП: сперва ищется следующий бот этого же этапа.

      never  - «Ответ не нужен»: информационное сообщение, ответа не ждём вовсе;
      any    - «Как только клиент ответит»: дальше ведёт ЛЮБОЙ ответ;
      listed - «Только на эти ответы»: перечисленное ведёт дальше, прочее останавливает;
      except - «На любой ответ, кроме этих»: перечисленное останавливает;
      word   - «Если в ответе есть это слово»: слово целиком, см. `answer_has_word`.

    ⚠️ Остановка живёт в `listed` и `except`, а не в `any`. Правка Кати 09.09.2026: экран
    обещает «идём дальше», и движок обязан обещанию соответствовать, а не читать его наоборот.
    """
    mode = str(bot.get("stop_mode") or "any")
    listed = [
        normalize_answer(a)
        for a in (bot.get("stop_answers_norm") or bot.get("stop_answers") or [])
    ]
    got = normalize_answer(answer)
    if mode in ("never", "any"):
        return "advance"
    if mode == "listed":
        return "advance" if got in listed else "stop"
    if mode == "except":
        return "stop" if got in listed else "advance"
    if mode == "word":
        return "advance" if answer_has_word(got, listed) else "stop"
    return "stop"


# Клиент спрашивает - значит ждёт человека, а не хода робота.
_QUESTION_MARKS = ("?", "？")

# Отказ в любом виде. Ищем отдельными словами: «нет» перебивает найденное согласие, а «нетто»
# или «интернет» - не отказ, поэтому границы слова обязательны.
_REFUSAL_WORDS = ("нет", "не")


def answer_has_word(answer_norm: str, words: list[str]) -> bool:
    """Есть ли в ответе одно из слов ЦЕЛИКОМ - с двумя предохранителями.

    Правило просила Катя 30.09.2026: «любой ответ с да в тексте поведет сделку вперед». Повод -
    замер живых ответов за 30 дней: 183 совпали со списком дословно, а ещё 12 содержали «да» и
    НЕ совпали («Да верно», «Да,верно», «Да все верно», «Здравствуйте! Да, все верно») - робот
    звал человека к подтверждённому заказу.

    ⚠️ Предохранитель первый - ВОПРОС. Среди тех же двенадцати было «Здравствуйте да / Могу через
    беп20 оплатить?»: человек согласился и тут же спросил. Увести такую сделку в успех значит
    оставить клиента без ответа, поэтому вопрос всегда зовёт менеджера.

    ⚠️ Предохранитель второй - ОТКАЗ. «Да, но нет», «да, не надо» согласием не являются: слово
    «не» или «нет» перебивает найденное слово согласия.

    ⚠️ Границы слова обязательны: без них «да» нашлось бы в «давайте подумаю» и «дайте скидку».
    """
    if not answer_norm or not words:
        return False
    if any(mark in answer_norm for mark in _QUESTION_MARKS):
        return False
    if any(re.search(rf"(?<!\w){re.escape(bad)}(?!\w)", answer_norm) for bad in _REFUSAL_WORDS):
        return False
    return any(
        re.search(rf"(?<!\w){re.escape(w)}(?!\w)", answer_norm) for w in words if w
    )


def has_question_or_refusal(answer: str) -> bool:
    """Клиент о чём-то спрашивает или отказывается - значит ждёт человека, а не хода робота."""
    norm = normalize_answer(answer)
    if any(mark in norm for mark in _QUESTION_MARKS):
        return True
    return any(re.search(rf"(?<!\w){re.escape(bad)}(?!\w)", norm) for bad in _REFUSAL_WORDS)


def decide_on_answers(bot: dict, answers: list[str]) -> str:
    """Решение по ВСЕМ ответам окна, а не по одному сообщению (правка Кати 01.10.2026).

    Разбор заказа 19388: клиент написал «Да, всё верно», через секунды «Заказ оплачен», потом
    «Заказ подтверждаю». Подбор брал последнее сообщение и звал человека - при том что
    подтверждение лежало в первом. За сутки клиенты писали на один шаблон по 3-26 сообщений:
    «один шаблон - один ответ» в жизни почти не встречается.

    Правило: подтверждение ищем в ЛЮБОМ сообщении окна, но вопрос или отказ в любом из них
    перебивает - человек дороже скорости. Режим «на любой ответ, кроме этих» считается иначе:
    там достаточно ОДНОГО попадания в список, чтобы остановиться.
    """
    texts = [str(a) for a in answers if str(a or "").strip()]
    if not texts:
        return "stop"
    mode = str(bot.get("stop_mode") or "any")
    if mode in ("never", "any"):
        return "advance"
    if mode == "except":
        return "stop" if any(answer_decision(bot, t) == "stop" for t in texts) else "advance"
    if any(has_question_or_refusal(t) for t in texts):
        return "stop"
    return "advance" if any(answer_decision(bot, t) == "advance" for t in texts) else "stop"


# Правила движения вперёд - словами экрана. Заголовок там «Когда идём дальше», и в журнале
# правило должно называться ровно так же: человек сверяет строку журнала с настройкой бота,
# и два разных названия одного правила эту сверку ломают.
STOP_MODE_TITLES = {
    "any": "как только клиент ответит",
    "listed": "только на эти ответы",
    "except": "на любой ответ, кроме этих",
    "word": "если в ответе есть это слово",
    "never": "ответ не нужен",
}


def answer_verdict_note(bot: dict, answer: str, decision: str) -> str:
    """Разбор ответа клиента словами - для строки журнала (просьба Кати 27.09.2026).

    Дословно: «почему нет в логе анализа ответа клиента на бота? типа ответ клиента такой-то,
    он есть в белом списке (или нет в чёрном списке, в зависимости от правила движения вперед
    для бота) - ведем сделку далее». До этого в журнале стояло «ответ клиента подходит под
    условие успеха» - вердикт без разбора: ни правила, ни списка, ни того, чем совпало.

    Список в журнал кладём ЦЕЛИКОМ, а не «совпало с одним из вариантов»: настройки бота могут
    поменять завтра, а журнал читают через месяц, и тогда восстановить, с чем сравнивали, будет
    уже нечем. Длинный список подрезаем - в журнал, а не в роман.
    """
    mode = str(bot.get("stop_mode") or "any")
    said = str(answer or "").strip()
    rule = STOP_MODE_TITLES.get(mode, mode)
    listed = [str(a).strip() for a in (bot.get("stop_answers") or []) if str(a).strip()]
    shown = "; ".join(listed[:8]) + (" и ещё" if len(listed) > 8 else "")
    head = f"клиент ответил «{said[:200]}»" if said else "клиент ответил пустым сообщением"
    tail = "веду дальше" if decision == "advance" else "дальше не веду"

    if mode == "listed":
        hit = "ответ в списке" if decision == "advance" else "ответа в списке НЕТ"
        return f"{head}. Правило бота - «{rule}»: {shown or 'список пуст'}. {hit}, {tail}"
    if mode == "except":
        hit = "ответ в списке-исключении" if decision == "stop" else "в списке-исключении его нет"
        return f"{head}. Правило бота - «{rule}»: {shown or 'список пуст'}. {hit}, {tail}"
    if mode == "word":
        # Причина остановки у этого правила бывает троякой, и человеку важно знать, какая:
        # слова нет вовсе, слово есть но клиент спросил, слово есть но клиент отказался.
        if decision == "advance":
            hit = "слово найдено целиком"
        elif any(mark in normalize_answer(said) for mark in _QUESTION_MARKS):
            hit = "клиент о чём-то спрашивает - это к менеджеру"
        elif any(re.search(rf"(?<!\w){re.escape(bad)}(?!\w)", normalize_answer(said))
                 for bad in _REFUSAL_WORDS):
            hit = "в ответе есть отказ"
        else:
            hit = "ни одного слова из списка в ответе нет"
        return f"{head}. Правило бота - «{rule}»: {shown or 'список пуст'}. {hit}, {tail}"
    return f"{head}. Правило бота - «{rule}», сравнивать не с чем, {tail}"


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



# ── чтение сделки и контакта ────────────────────────────────────────────────────

AMO_LEAD_URL = "https://new5a2e8ea7b16b4.amocrm.ru/leads/detail/{}"


def lead_link(lead_id: int, title: str = "") -> str:
    """Ссылка на сделку словами. Голый номер в чате менеджеру ничего не говорит - правило
    Кати 03.08.2026 про «я человек, я не понимаю цифры»."""
    name = (title or "").strip() or "сделка"
    return f'<a href="{AMO_LEAD_URL.format(lead_id)}">{name}</a>'


async def load_lead(lead_id: int) -> dict | None:
    """Свежая сделка из amoCRM. Перед КАЖДЫМ действием, а не по телу вебхука.

    Между вебхуком и нашим ходом проходят секунды, а после ночного сна и десять часов. За это
    время менеджер успевает увести сделку с этапа, закрыть её или переписать поля. Действовать
    по телу вебхука значит действовать по прошлому.
    """
    try:
        return await amo_service.get_lead_full(lead_id, with_=("contacts",))
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("autopilot: сделка %s не прочиталась", lead_id)
        # Сделку не прочитали - значит ведение по ней сейчас не продолжится. Это сбой, и о
        # нём говорим в чат: ссылка в алерте есть, а имя сделки взять негде - его как раз и
        # не прочитали.
        alert_error(f"не смог прочитать сделку в amoCRM: {type(exc).__name__}: {exc}",
                    lead_id=lead_id, dedupe="load_lead")
        return None


async def main_contact(lead: dict) -> dict | None:
    """Главный контакт, дочитанный целиком: в теле сделки у контакта только номер и признак
    главного, ни имени, ни телефона там нет."""
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    if not contacts:
        return None
    main = next((c for c in contacts if c.get("is_main")), contacts[0])
    try:
        return await amo_service.get_contact_by_id(main.get("id"))
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot: контакт %s не прочитался", main.get("id"))
        return None


def chat_id_of(contact: dict | None) -> str:
    """Идентификатор чата Wazzup - телефон одними цифрами, ровно как в `wazzup_message`
    панели. Склейка идёт по нему, поэтому формат обязан совпадать посимвольно."""
    if not contact:
        return ""
    phone = amo_service.get_custom_field_value(contact, FIELD_PHONE)
    return "".join(ch for ch in str(phone or "") if ch.isdigit())


# ── настройки, к которым обращаемся часто ───────────────────────────────────────

def _flag(key: str, default: bool = False) -> bool:
    value = (settings_client.get_settings().get("settings") or {}).get(key)
    return default if value is None else bool(value)


def _num(key: str, default: float) -> float:
    value = (settings_client.get_settings().get("settings") or {}).get(key)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def delivery_wait_s() -> float:
    return _num("delivery_wait_minutes", 15) * 60


# ── ограниченные режимы: тест и пилот боя ───────────────────────────────────────

def limited_mode() -> str | None:
    """None - робот работает для всех. Иначе имя ограниченного режима.

    «тест» - воронка «Тест». «пилот» - боевой режим с тумблером «В бою вести только
    тестовые контакты» (Катя 12.09.2026): бой обкатывается на живой воронке, но робот
    трогает только сделки белого списка. Тумблер по умолчанию ВКЛЮЧЁН - первый запуск
    боя начинается пилотом, полный запуск это осознанное выключение.
    """
    mode = settings_client.get_mode()
    if mode == "test":
        return "тест"
    if mode == "live" and _flag("live_whitelist_enabled", True):
        return "пилот"
    return None


def whitelist_ok(lead: dict) -> bool:
    """В ограниченном режиме пропускаем только контакты из белого списка.

    ⚠️ Имя воронки само по себе не защищает никого: и в «Тест», и в боевую можно
    завести сделку с кем угодно. Пустой список значит «никому».
    """
    if limited_mode() is None:
        return True
    allowed = settings_client.get_test_contact_ids()
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    return any(int(c.get("id") or 0) in allowed for c in contacts)


# ── гейт остатка ────────────────────────────────────────────────────────────────

async def stock_gate(lead: dict) -> tuple[bool, str]:
    """Есть ли свободный остаток по всем позициям заказа. Считает панель, мы только спрашиваем.

    Почему не считаем сами: остаток уже умеет считать раздел «Остатки» панели - там и список
    складов, и тумблер вычитания резерва. Вторая копия правила разошлась бы с первой, это
    ровно тот случай, ради которого заведён `knowledge/edinyy-kontur-pravila-i-storozh.md`.

    ⚠️ Исходов ТРИ, а не два. «Панель не ответила» - это НЕ «товара нет»: молчащий склад уже
    останавливал сторож заказов 03.09.2026. Не ответила - идём дальше и пишем себе в
    технический чат: не отправить шаблон живому заказу дороже, чем отправить его при
    неизвестном остатке.
    """
    order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
    if not order_uuid:
        return True, "заказа МойСклада в сделке нет, остаток не проверяю"
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return True, "панель не настроена, остаток не проверял"
    base = TEAM_PANEL_BASE_URL.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                base + "/api/ingest/autopilot/stock-check",
                params={"order_uuid": str(order_uuid)},
                headers={"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN},
            )
        if resp.status_code >= 400:
            alert_tech(
                f"Панель не посчитала остаток, ответ {resp.status_code}. Иду дальше без проверки."
            )
            return True, f"панель ответила {resp.status_code}, остаток неизвестен"
        data = resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot: гейт остатка не отработал")
        alert_tech("Не смог спросить у панели остаток. Иду дальше без проверки.")
        return True, "остаток спросить не удалось"
    if data.get("enough"):
        return True, "остаток есть"
    missing = ", ".join(str(x) for x in (data.get("missing") or []))
    return False, "не хватает: " + (missing or "позиции не названы")


# ── запуск сейлсбота ────────────────────────────────────────────────────────────

# `entity_type`: 1 контакт, 2 сделка, 3 компания. Нам нужна СДЕЛКА: маршрут идёт по сделке, а
# запуск по контакту на закрытой сделке рождает побочную (замер Кати 08.09.2026).
SALESBOT_ENTITY_LEAD = 2


async def launch_bot(lead_id: int, bot_id: int) -> bool:
    """Запуск сейлсбота. Метод недокументирован, форма выяснена замером 08.09.2026.

    ⚠️ Сообщение уходит НЕ сразу: у бота приветствия первым шагом свой таймер на 15 секунд, в
    замере от запуска до отправки прошло 35. Поэтому гейт доставки не считает первые минуты
    молчания провалом - окно ожидания задаётся на экране и по умолчанию равно четверти часа.
    """
    if is_shadow():
        logger.info("autopilot: призрак не запускает бота %s по сделке %s", bot_id, lead_id)
        return False
    result = await amo_service._do_post(
        "/api/v2/salesbot/run",
        [{"bot_id": int(bot_id), "entity_id": int(lead_id), "entity_type": SALESBOT_ENTITY_LEAD}],
    )
    return bool(result.get("ok"))


# ── маршрут: выбор бота и следующего этапа ──────────────────────────────────────

def bot_index(stage: dict | None, bot_id) -> int:
    for i, bot in enumerate((stage or {}).get("bots") or []):
        if int(bot.get("bot_id") or 0) == int(bot_id or 0):
            return i
    return -1


def pick_bot(lead: dict, stage: dict, after_bot_id=None) -> dict | None:
    """Следующий включённый бот этапа, чьи условия сошлись.

    Боты этапа идут ЦЕПОЧКОЙ, по очереди: первый спросил, клиент ответил «да» - слово берёт
    второй, и только когда боты кончились, сделка едет на следующий этап. Порядок задаёт
    человек на экране.

    ⚠️ Одновременно бот всегда ОДИН. Запусти движок всех сошедшихся разом, клиент получил бы
    несколько сообщений подряд; очередь же честно ждёт ответа на предыдущее. Условия отбирают,
    кто из них вообще участвует: несошедшийся бот не запускается, ход переходит к следующему.
    """
    start = 0 if after_bot_id is None else bot_index(stage, after_bot_id) + 1
    for bot in (stage.get("bots") or [])[start:]:
        if not bot.get("enabled", True):
            continue
        if conditions_match(lead, bot.get("conditions") or []):
            return bot
    return None


def stage_position(status_id: int) -> int:
    for i, stage in enumerate(settings_client.get_route()):
        if int(stage.get("status_id") or 0) == int(status_id):
            return i
    return -1


def next_stage(status_id: int) -> dict | None:
    """Следующий этап маршрута. Этап без ботов маршрут проходит не останавливаясь - решение
    Кати 08.09.2026, - поэтому «следующий» здесь просто следующий по порядку."""
    route = settings_client.get_route()
    i = stage_position(status_id)
    if i < 0 or i + 1 >= len(route):
        return None
    return route[i + 1]


def is_last_stage(stage: dict) -> bool:
    if stage.get("is_final"):
        return True
    return next_stage(int(stage.get("status_id") or 0)) is None

# ── журнал одной строкой ────────────────────────────────────────────────────────

def log_run(lead: dict, stage: dict | None, *, bot: dict | None = None, **extra) -> None:
    """Строка журнала о том, что робот сделал. Пишем на КАЖДОМ шаге, включая «ничего не
    сделал и почему»: журнал - единственный источник правды о роботе, чат может молчать
    сутками (Телеграм у нас уже глушился одним сетевым сбоем 28-29.08.2026)."""
    if bot:
        extra.setdefault("bot_id", bot.get("bot_id"))
        extra.setdefault("bot_name", bot.get("bot_name"))
        extra.setdefault("launched_by", bot.get("launched_by"))
    journal_bg(run_row(lead, stage, **extra))


def bot_by_id(stage: dict | None, bot_id) -> dict:
    """Бот этапа по номеру. Пустой словарь вместо None: настройки могли поменять, пока сделка
    ждала ответа, и разбор ответа не должен падать из-за исчезнувшего бота - у пустого бота
    режим по умолчанию «останавливаться на любой ответ», то есть самый осторожный."""
    for bot in (stage or {}).get("bots") or []:
        if int(bot.get("bot_id") or 0) == int(bot_id or 0):
            return bot
    return {}


# ── остановка ───────────────────────────────────────────────────────────────────

async def stop_here(
    lead: dict, stage: dict | None, outcome: str, reason: str,
    *, bot: dict | None = None, op_text: str = "", event_key: str = EVENT_EVENT,
    values: dict[str, Any] | None = None, panel_title: str = "Авто-режим: нужен человек",
) -> None:
    """Снять сделку с ведения и позвать человека.

    Останавливаемся ОХОТНО. Робот в этой воронке пишет живым людям и двигает деньги: цена
    лишней остановки - минута менеджера, цена лишнего хода - клиент, получивший не то.

    `event_key` выбирает, каким событием это уйдёт в панель и в чат: у недоставки, у
    непонятного способа оплаты и у сбоя свои тексты и свои выключатели.
    """
    lead_id = int(lead.get("id") or 0)
    status_id = int(lead.get("status_id") or 0)
    await asyncio.to_thread(store.finish, lead_id, status_id, store.PHASE_STOPPED, reason)
    refresh_watched_chats()
    log_run(lead, stage, bot=bot, action="route", outcome=outcome, reason=reason,
            alert_target="op" if op_text else "")
    if op_text:
        name = str(lead.get("name") or "").strip()
        dispatch_op(
            event_key, f"{lead_link(lead_id, name)}: {op_text}",
            responsible_id=lead.get("responsible_user_id"),
            values={"сделка": name or "без названия", "текст_события": op_text,
                    "ссылка_на_сделку": alerts.lead_link(lead_id), **(values or {})},
            panel_title=panel_title, panel_url=AMO_LEAD_URL.format(lead_id) if lead_id else None,
        )


# ── ход по маршруту ─────────────────────────────────────────────────────────────

async def run_stage(lead: dict, stage: dict, *, moved_by_us: bool = True) -> None:
    """Этап маршрута: оплата, остаток, первый бот цепочки. Вызывается уже ПОСЛЕ `store.claim`.

    `moved_by_us` - перевёл ли сделку в этот этап сам робот. Нужно ТОЛЬКО журналу: строка
    «маршрут пройден» по сделке, которую в успех перетащил человек, читается как заслуга
    робота, а это неправда (замечание Кати 27.09.2026).
    """
    lead_id = int(lead["id"])
    status_id = int(lead["status_id"])

    if not in_work_hours():
        wake = next_work_moment()
        await asyncio.to_thread(
            store.update, lead_id, status_id,
            phase=store.PHASE_SLEEPING,
            wake_at=wake.astimezone(_UTC).isoformat() if wake else None,
        )
        when = wake.strftime("%d.%m в %H:%M") if wake else "когда включат часы работы"
        log_run(lead, stage, action="route", outcome="sleeping",
                reason=f"вне рабочих часов, продолжу {when}")
        return

    # Вход маршрута спрашиваем у панели, а не считаем по порядку карточек: человек волен
    # собрать маршрут, начав его с середины воронки, и тогда «первая карточка» входом не
    # является. Панель не сказала - падаем на порядок, это лучше, чем не проверить вовсе.
    entry = settings_client.get_entry_status_id()
    at_entry = status_id == entry if entry else stage_position(status_id) == 0

    # Оплаченность на входе НЕ проверяем (правка Кати 12.09.2026, отменяет правку 09.09):
    # независимо от статуса оплаты сделка идёт по ВСЕМ шагам маршрута, и в успех её пускает
    # только развилка после них. Оплаченный заказ, телепортом уезжавший в УР без единого
    # слова клиенту («Заказ №19005»), человека только путал.

    # ⚠️ Непонятный способ оплаты говорим СРАЗУ, на входе, а не в конце маршрута (Катя
    # 27.09.2026). Развилка оплаты стоит последней, и по заказу 19296 алерт «Другой способ» не
    # пришёл вовсе - робот честно ждал ответа клиента на бота, а до развилки дело не дошло.
    # Менеджеру же надо знать в момент заказа: оплату такой сделки роботу не понять никогда,
    # сколько бы шагов маршрута он ни прошёл.
    if at_entry:
        method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
        if is_ambiguous_payment(method):
            await stop_here(
                lead, stage, "stop_payment_unclear",
                f"способ оплаты «{method}» роботу непонятен, на входе маршрута",
                op_text=f"способ оплаты «{method}» - не понимаю, ждать ли оплату. "
                        "Сделку не веду, посмотрите сами.",
                event_key=EVENT_PAYMENT_OTHER,
                values={"способ_оплаты": str(method)},
                panel_title="Авто-режим: непонятный способ оплаты",
            )
            return

    bot = pick_bot(lead, stage)
    if bot is None:
        # Этап без ботов - ПРОХОДНОЙ (правка Кати 09.09.2026): сделку в него перевели, чужая
        # автоматика этапа получила своё событие, нам здесь делать нечего - идём дальше.
        where = str(stage.get("status_name") or "этап без названия")
        reason = (f"этап «{where}» проходной, ботов на нём нет" if not (stage.get("bots") or [])
                  else f"на этапе «{where}» ни один бот не подошёл по условиям")
        log_run(lead, stage, action="route", outcome="skipped_no_bots", reason=reason)
        await advance(lead, stage, reason, moved_by_us=moved_by_us)
        return

    # Гейт остатка - на входе в маршрут, до первого слова клиенту. Дальше по маршруту заказ
    # уже подтверждён, и перепроверять остаток на каждом этапе значит гонять склад впустую.
    if at_entry and _flag("stock_check_enabled", True):
        enough, note = await stock_gate(lead)
        log_run(lead, stage, bot=bot, action="stock_gate",
                outcome="advanced" if enough else "stop_no_stock", reason=note)
        if not enough:
            await stop_here(
                lead, stage, "stop_no_stock", note, bot=bot,
                op_text=f"шаблон клиенту НЕ отправлял, {note}",
            )
            return

    await run_bot(lead, stage, bot)


async def run_bot(lead: dict, stage: dict, bot: dict) -> None:
    """Один бот цепочки: запуск и ожидание. Отдельной функцией, потому что ботов на этапе
    бывает несколько, и второго зовёт уже не вход в этап, а ответ клиента на первого."""
    lead_id = int(lead["id"])
    status_id = int(lead["status_id"])
    contact = await main_contact(lead)
    chat_id = chat_id_of(contact)
    bot_id = int(bot.get("bot_id") or 0)

    # ⚠️ Чат клиента неизвестен - зовём человека (Катя 27.09.2026). Склейка с Wazzup идёт по
    # телефону контакта: нет телефона в карточке - робот не увидит ни статусов доставки, ни
    # ответа клиента, и сделка просто провисит до конца окна. Проверяем ДО запуска бота: у
    # ботов, которых запускает сам робот, иначе сообщение уже ушло бы, а отследить его нечем.
    #
    # ⚠️ Но сперва ПЕРЕСПРАШИВАЕМ. Заказ создаёт интеграция: сначала сделка, через секунды -
    # привязанный контакт с телефоном. Вебхук успевает прийти в зазор, и 27.09.2026 это дало
    # ложное «в карточке контакта нет телефона» по заказу 07975, где телефон был. Тот же зазор
    # уже ловил `telegram_contact` - он ждёт не только заказ МойСклада, но и контакт сделки.
    if not chat_id and str(bot.get("stop_mode") or "any") != "never":
        await asyncio.sleep(AUTOPILOT_CONTACT_RETRY_S)
        fresh = await load_lead(lead_id)
        if fresh is not None:
            lead = fresh
            contact = await main_contact(lead)
            chat_id = chat_id_of(contact)
        if chat_id:
            logger.info("autopilot: телефон по сделке %s приехал со второй попытки", lead_id)

    if not chat_id and str(bot.get("stop_mode") or "any") != "never":
        await stop_here(
            lead, stage, "stop_no_chat", "в карточке контакта нет телефона, чат не определить",
            bot=bot,
            op_text="в карточке клиента нет телефона - не смогу отследить ни доставку, ни ответ. "
                    "Сделку не веду, посмотрите переписку сами.",
            event_key=EVENT_NO_CHAT,
            values={"что_случилось": "в карточке контакта нет телефона, чат Wazzup не определить"},
            panel_title="Авто-режим: не вижу чат клиента",
        )
        return

    # ⚠️ Призрак и шаблон: развилка ровно здесь. Бота с грида Цифровой воронки отправляет сама
    # amoCRM - значит сообщение клиенту уйдёт и без нас, и призрак спокойно доводит репетицию
    # до конца: ждёт доставку, читает ответ, разбирает его. А бота, которого запускает наша
    # интеграция, в призраке не запускает никто, ждать нечего - пишем, что отправили бы, и
    # ведение на этом заканчиваем. Иначе окно доставки истекло бы ложным «не дошло».
    if is_shadow() and str(bot.get("launched_by") or "engine") == "engine":
        await asyncio.to_thread(
            store.finish, lead_id, status_id, store.PHASE_STOPPED, "призрак: шаблон не отправлял",
        )
        refresh_watched_chats()
        log_run(lead, stage, bot=bot, action="launch_bot", outcome="shadow_would_send",
                reason=f"отправил бы клиенту шаблон ботом «{bot.get('bot_name') or bot_id}», "
                       "но в режиме призрака сообщений не отправляю")
        return

    if str(bot.get("launched_by") or "engine") == "engine":
        if not allow_action():
            await stop_here(
                lead, stage, "failed", "упёрся в потолок действий в час", bot=bot,
                op_text="упёрся в потолок действий в час и остановился, "
                        "сообщение клиенту не отправлял - напишите сами",
                event_key=EVENT_ERROR, panel_title="Авто-режим: сбой",
                values={"что_случилось": "упёрся в потолок действий в час"},
            )
            return
        await asyncio.to_thread(store.mark_launch_attempted, lead_id, status_id, bot_id)
        if not await launch_bot(lead_id, bot_id):
            await stop_here(
                lead, stage, "failed", "amoCRM не принял запуск бота", bot=bot,
                op_text="не смог запустить бота, напишите клиенту сами",
                event_key=EVENT_ERROR, panel_title="Авто-режим: сбой",
                values={"что_случилось": "amoCRM не принял запуск бота"},
            )
            return
    else:
        # Бот приезжает с грида Цифровой воронки - мы его не вызываем, иначе клиент получит
        # два одинаковых сообщения. Отметка времени всё равно нужна: от неё считается окно
        # ожидания доставки.
        await asyncio.to_thread(store.update, lead_id, status_id, bot_id=bot_id)
    # Имя контакта кладём в состояние рядом с чатом: по нему подбор находит телеграмную
    # переписку, которую по телефону не найти (правка Кати 01.10.2026).
    contact_name = str((contact or {}).get("name") or "").strip()
    await asyncio.to_thread(
        store.mark_launch_ok, lead_id, status_id, chat_id, contact_name,
    )
    refresh_watched_chats()

    # ⚠️ Факт отправки спрашиваем СРАЗУ, а не ждём вебхуков (правка Кати 27.09.2026). Бот грида
    # мог отстрелять задолго до того, как робот взял сделку: по заказу 19288 шаблон ушёл вечером,
    # статусы прошли до включения робота, и пятнадцать минут ожидания кончились ложным «не
    # дошло». Один запрос к панели снимает весь этот класс: у неё вся переписка уже лежит.
    settled = False
    if str(bot.get("launched_by") or "engine") != "engine":
        settled = await confirm_grid_send(lead, stage, bot, chat_id, contact_name)

    if str(bot.get("stop_mode") or "any") == "never":
        # «Ответ не нужен» - информационное сообщение, а не разговор. Ни доставки, ни ответа
        # не ждём: у бота в успешной реализации отправка успешна по определению.
        log_run(lead, stage, bot=bot, action="launch_bot", outcome="advanced",
                reason="сообщение информационное, ответа не жду")
        await advance(lead, stage, "информационное сообщение отправлено", from_bot=bot)
        return

    if settled:
        # Вопрос доставки уже решён строкой выше - «жду подтверждения» здесь было бы неправдой.
        return

    log_run(lead, stage, bot=bot, action="launch_bot", outcome="waiting_delivery",
            reason="жду подтверждения доставки от Wazzup")


async def advance(lead: dict, stage: dict, reason: str, *, from_bot: dict | None = None,
                  moved_by_us: bool = True) -> None:
    """Следующий шаг. Сперва следующий БОТ этого этапа, и только когда боты кончились - этап.

    Боты этапа идут цепочкой: первый спросил «всё верно?», клиент ответил «да» - слово берёт
    второй. Пока цепочка не кончилась, сделка с места не двигается.

    ⚠️ В успешную реализацию карточка маршрута не ведёт: туда пускает только развилка оплаты
    (и отдельный вход - этап «Оплата получена»). Иначе решение о деньгах зависело бы от того,
    в каком порядке человек перетащил карточки.
    """
    if from_bot is not None:
        nxt_bot = pick_bot(lead, stage, after_bot_id=from_bot.get("bot_id"))
        if nxt_bot is not None:
            await asyncio.to_thread(
                store.start_next_bot, int(lead["id"]), int(lead.get("status_id") or 0),
                int(nxt_bot.get("bot_id") or 0),
            )
            log_run(lead, stage, bot=nxt_bot, action="route", outcome="advanced",
                    reason="передаю ход следующему боту этапа")
            await run_bot(lead, stage, nxt_bot)
            return

    status_id = int(lead.get("status_id") or 0)
    if status_id == STATUS_SUCCESS:
        await asyncio.to_thread(
            store.finish, int(lead["id"]), status_id, store.PHASE_DONE, reason,
        )
        log_run(lead, stage, action="route", outcome="done",
                reason=("маршрут пройден до «Успешно реализовано», дальше сделку уводит перевод "
                        "в офис" if moved_by_us else
                        "в успех сделку перевёл не робот, маршрут считаю пройденным"),
                moved_to_status_name="Успешно реализовано" if moved_by_us else "")
        return
    nxt = next_stage(status_id)
    # На этап запроса оплаты, как и в успех, по порядку карточек не переходим. Сам этап
    # робот больше не использует (правка Кати 12.09.2026) - он остался в воронке для чужой
    # автоматики; дошли до него по порядку карточек - значит маршрут пройден, слово за
    # развилкой оплаты.
    pay_status = settings_client.get_payment_status_id() or STATUS_PAYMENT_REQUESTED
    if nxt is None or int(nxt.get("status_id") or 0) in (STATUS_SUCCESS, pay_status):
        await payment_fork(lead, stage, reason)
        return
    await move_to(lead, stage, int(nxt["status_id"]), str(nxt.get("status_name") or ""), reason)


async def move_to(lead: dict, stage: dict | None, status_id: int, status_name: str,
                  reason: str) -> None:
    """Перевод сделки на этап и немедленный вход в него.

    Вход делаем САМИ, не дожидаясь эха вебхука о собственной правке: эхо приходит не всегда и
    не сразу, а ждать его значит поставить маршрут в зависимость от чужой очереди доставки.
    Повторного хода это не создаёт - `store.claim` пропустит первого и откажет второму.
    """
    lead_id = int(lead["id"])
    was = int(lead.get("status_id") or 0)
    # ⚠️ Призрак сделку не двигает, и на этом его репетиция кончается: следующий шаг маршрута
    # начинается с того, что сделка УЖЕ на новом этапе, а она там не окажется. Обходного пути
    # нет, поэтому говорим об этом прямо в журнале, а не делаем вид, что прошли дальше.
    if is_shadow():
        await asyncio.to_thread(
            store.finish, lead_id, was, store.PHASE_STOPPED,
            f"призрак: перевёл бы на «{status_name}»",
        )
        refresh_watched_chats()
        log_run(lead, stage, action="route", outcome="shadow_would_move",
                reason=f"перевёл бы сделку на этап «{status_name}»: {reason}. В режиме призрака "
                       "сделку не двигаю, дальше по маршруту репетиция не идёт",
                moved_to_status_name=status_name)
        return
    if not allow_action():
        await stop_here(
            lead, stage, "failed", "упёрся в потолок действий в час",
            op_text=f"упёрся в потолок действий в час, сделку на этап «{status_name}» "
                    "не перевёл - сделайте это руками",
            event_key=EVENT_ERROR, panel_title="Авто-режим: сбой",
            values={"что_случилось": "упёрся в потолок действий в час"},
        )
        return
    result = await amo_service.patch_lead(
        lead_id, status_id=status_id, pipeline_id=int(lead.get("pipeline_id") or 0) or None,
    )
    if not result.get("ok"):
        await stop_here(
            lead, stage, "failed", f"amoCRM не принял перевод на «{status_name}»",
            op_text=f"не смог перевести сделку на этап «{status_name}», сделайте это руками",
            event_key=EVENT_ERROR, panel_title="Авто-режим: сбой",
            values={"что_случилось": f"amoCRM не принял перевод на «{status_name}»"},
        )
        return
    await asyncio.to_thread(store.finish, lead_id, was, store.PHASE_DONE, reason)
    log_run(lead, stage, action="route", outcome="advanced", reason=reason,
            moved_to_status_name=status_name)
    if len(_moved_by_us) > 2000:
        _moved_by_us.clear()
    _moved_by_us.add((lead_id, int(status_id)))
    await handle_lead_change(lead_id)


# ── развилка оплаты ─────────────────────────────────────────────────────────────

def is_cod_strict(payment_method) -> bool:
    """Наложка в УЗКОМ смысле: строго «При получении».

    ⚠️ Соседний `waybill_config.is_cod_payment` шире - в нём есть «Эвотор» и «наличные», а это
    шоурум, где наложки нет вовсе. Возьми мы широкое определение, шоурумные заказы уехали бы в
    успешную реализацию мимо оплаты. Расхождение намеренное, оно описано в DESIGN.md.
    """
    return "при получении" in str(payment_method or "").lower()


# Способы оплаты, по которым робот не понимает, чего ждать (Катя 27.09.2026). «Другой способ»
# приходит с сайта, когда человек в корзине выбрал оплату не из списка: за этим стоит счёт
# юрлицу, перевод, рассрочка - что именно, знает менеджер, а не мы. Дальше такую сделку не
# ведём и говорим об этом вслух.
AMBIGUOUS_PAYMENT_MARKERS = ("другой способ",)


def is_ambiguous_payment(payment_method) -> bool:
    """Способ оплаты назван, но роботу непонятен.

    ⚠️ Список УЗКИЙ и держится именно таким. «Наличными» и «Эвотор» сюда не входят: это
    шоурум, деньги берут на месте, и в МойСкладе такой заказ помечается оплаченным - развилка
    разберётся с ним по факту оплаты. Расширять список - решением Кати, не догадкой.
    """
    value = str(payment_method or "").casefold()
    return any(marker in value for marker in AMBIGUOUS_PAYMENT_MARKERS)


async def order_payment(order_uuid) -> tuple[bool | None, float]:
    """Оплата заказа в МойСкладе: оплачен ли и на какую сумму.

    Сумма нужна ЖУРНАЛУ (просьба Кати 27.09.2026 - «в этапе проверки оплаты хорошо писать,
    какой способ оплаты и статус в мс»): «оплачен» без цифры не отличить от «оплачен на рубль»,
    а разбирают такие сделки как раз по цифре.

    ⚠️ Признак оплаты - `payedSum > 0`, а не сравнение с суммой заказа. Сумма первые минуты
    пляшет: `woocommerce-sklad` раз в три минуты обнуляет цену доставки по правилу «предоплата
    - доставка за наш счёт», и заказ мигает 387 → 0 → 387.

    ⚠️ Молчание склада читать как «не оплачен» нельзя: оплаченный заказ получил бы ложный
    алерт «не оплачен», и человек пошёл бы разбирать исправный заказ. Поэтому три исхода -
    у неизвестности своя честная причина остановки.
    """
    if not order_uuid:
        return None, 0.0
    try:
        data = await ms_client.get(f"entity/customerorder/{order_uuid}")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot: заказ %s не прочитался из МойСклада", order_uuid)
        return None, 0.0
    if not data:
        return None, 0.0
    try:
        # В МойСкладе деньги лежат в копейках - так же их читает раздел цен панели
        # (`app/prices/digest.py`), второй трактовки этой цифры у нас нет.
        payed = float(data.get("payedSum") or 0) / 100
    except (TypeError, ValueError):
        return None, 0.0
    return payed > 0, payed


async def order_is_paid(order_uuid) -> bool | None:
    """Оплачен ли заказ в МойСкладе. None - склад не ответил, и это НЕ «не оплачен»."""
    paid, _ = await order_payment(order_uuid)
    return paid


# Сколько ждём между попытками переспросить склад об оплате и сколько попыток делаем.
# ⚠️ Константы живут ЗДЕСЬ, а не в общем конфиге, намеренно: общий файл правят несколько
# сессий разом, и ради двух чисел рисковать чужой работой не стоит.
PAY_RECHECK_PAUSE_S = 45
PAY_RECHECK_TRIES = 3


async def order_paid_confirmed(order_uuid, *, tries: int | None = None,
                               pause_s: int | None = None) -> bool | None:
    """То же, но «не оплачен» перепроверяем с паузой. Молчание склада не переспрашиваем.

    ⚠️ Дефект, найденный дежурством 01.10.2026: платёж в МойСкладе создаёт наша же автоматика
    по тому же вебхуку платёжной системы, что переводит сделку на «Оплата получена». Робот
    успевал спросить склад РАНЬШЕ, чем платёж там появлялся, видел честный ноль и звал
    человека. По «Заказу №19399» алерт ушёл в 13:37, а к 13:45 заказ был оплачен, собран и
    отгружен; по «Заказу №19286» так ушло четыре алерта подряд.

    ⚠️ Замолчать совсем НЕЛЬЗЯ, и это проверено фактом: по «Заказу №19302» то же сообщение
    было ВЕРНЫМ - Ozon подтвердил 8 533 ₽ 27.09, товар уехал клиенту, а в МойСкладе оплата
    нулевая до сих пор. Поэтому не «молчим», а «переспрашиваем»: гонка рассасывается за
    десятки секунд, настоящее расхождение остаётся и алерт уходит.
    """
    # ⚠️ Значения берём ЗДЕСЬ, а не в подписи: дефолт в подписи вычисляется один раз при
    # импорте, и тест, подменивший константу, спал бы боевые 45 секунд.
    tries = PAY_RECHECK_TRIES if tries is None else tries
    pause_s = PAY_RECHECK_PAUSE_S if pause_s is None else pause_s
    paid = await order_is_paid(order_uuid)
    attempt = 1
    while paid is False and attempt < tries:
        logger.info("autopilot: МойСклад говорит «не оплачен», попытка %s из %s - переспрошу "
                    "через %s сек", attempt, tries, pause_s)
        await asyncio.sleep(pause_s)
        attempt += 1
        paid = await order_is_paid(order_uuid)
    if paid is True and attempt > 1:
        logger.info("autopilot: оплата в МойСкладе появилась с попытки %s - гонка, не алерт",
                    attempt)
    return paid


def payment_note(method, paid: bool | None, payed: float = 0.0) -> str:
    """Строка для журнала: способ оплаты и что сказал МойСклад.

    Без этой строки в журнале стояло «заказ оплачен, сверено с МойСкладом» - верно, но по ней
    нельзя ни проверить робота, ни понять сделку: способ оплаты решает всё, а его в строке не
    было вовсе.
    """
    name = str(method or "").strip() or "не заполнен"
    if paid is None:
        return f"способ оплаты «{name}», МойСклад про оплату не ответил"
    if paid:
        amount = f"{payed:,.0f}".replace(",", " ")
        return f"способ оплаты «{name}», в МойСкладе оплата есть: {amount} ₽"
    return f"способ оплаты «{name}», в МойСкладе оплаты нет"


async def payment_fork(lead: dict, stage: dict | None, reason: str) -> None:
    """Конец маршрута: решаем по способу оплаты (правка Кати 12.09.2026).

    Наложка (строго «При получении») едет в успех сразу - деньги возьмут при вручении,
    дальше сделку уводит офисная автоматика. Онлайн-заказ обязан быть УЖЕ оплачен:
    сверяемся с МойСкладом, оплачен - успех, не оплачен или неизвестно - сделка ОСТАЁТСЯ
    на месте, человек получает красный алерт. Выставление счёта больше не наш ход: в
    «Оплату запрошену» робот не ведёт никого, этап живёт для чужой автоматики.

    Развилка одна на оба режима: у «Теста» здесь нет поблажек, кроме прощённого пустого
    типа заявки выше по маршруту, - неоплаченный онлайн и там стоит на месте.
    """
    method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)

    if not str(method or "").strip():
        await stop_here(
            lead, stage, "stop_no_payment_method", "способ оплаты в сделке не заполнен",
            op_text="способ оплаты не заполнен, не понимаю, ждать ли оплату",
        )
        return

    if is_cod_strict(method):
        log_run(lead, stage, action="payment_fork", outcome="advanced",
                reason=f"способ оплаты «{method}» - деньги возьмут при вручении, "
                       "МойСклад об оплате не спрашиваю")
        await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано",
                      "оплата при получении")
        return

    # ⚠️ Смена поведения 27.09.2026 по просьбе Кати: «Другой способ» больше НЕ считается
    # онлайном. Раньше такая сделка уезжала в успех, если в МойСкладе стояла оплата, - то
    # есть решение о деньгах принималось по способу, которого робот не понимает.
    if is_ambiguous_payment(method):
        await stop_here(
            lead, stage, "stop_payment_unclear",
            f"способ оплаты «{method}» роботу непонятен",
            op_text=f"способ оплаты «{method}» - не понимаю, ждать ли оплату. "
                    "В успех не веду, посмотрите сделку.",
            event_key=EVENT_PAYMENT_OTHER,
            values={"способ_оплаты": str(method)},
            panel_title="Авто-режим: непонятный способ оплаты",
        )
        return

    order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
    if not order_uuid:
        # Онлайн-оплата, а сверяться не с чем. В бою такой сделки быть не должно (заказ
        # создаёт интеграция), в тесте это ручная сделка - исход один: без подтверждённой
        # оплаты в успех не ведём, сделка стоит где стояла, человек смотрит.
        await stop_here(
            lead, stage, "stop_unpaid",
            f"способ оплаты «{method}», а заказа МойСклада в сделке нет - оплату не проверить",
            op_text="онлайн-оплата, а заказа МойСклада в сделке нет - оплату не проверить, "
                    "дальше не веду",
        )
        return

    paid, payed = await order_payment(order_uuid)
    note = payment_note(method, paid, payed)
    if paid:
        log_run(lead, stage, action="payment_fork", outcome="advanced", reason=note)
        await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано", "заказ оплачен")
        return
    if paid is None:
        await stop_here(
            lead, stage, "failed", note,
            op_text="не смог узнать в МойСкладе, оплачен ли заказ, дальше не веду",
            event_key=EVENT_ERROR, panel_title="Авто-режим: сбой",
            values={"что_случилось": "МойСклад не ответил, оплачен ли заказ"},
        )
        return
    await stop_here(
        lead, stage, "stop_unpaid", note,
        op_text="онлайн-заказ не оплачен - в успех не веду, посмотрите оплату",
    )


async def force_ur(lead: dict, stage: dict | None, note: str) -> None:
    """Тумблер «вести в успех, даже если шаблоны не ушли».

    Включён - недоставленный шаблон перестаёт быть стопом ТАМ, где деньги уже не под вопросом:
    заказ оплачен либо это наложка. Неоплаченный онлайн-заказ так не проводим никогда - иначе
    робот закроет успехом сделку, за которую никто не заплатил.
    """
    method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
    order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
    paid, payed = await order_payment(order_uuid)
    if paid or is_cod_strict(method):
        why = "заказ оплачен" if paid else "оплата при получении"
        log_run(lead, stage, action="payment_fork", outcome="advanced",
                reason=f"шаблоны не дошли ({note}), но {payment_note(method, paid, payed)}",
                alert_target="op")
        lead_id = int(lead["id"])
        name = str(lead.get("name") or "").strip()
        dispatch_op(
            EVENT_NOT_DELIVERED,
            f"{lead_link(lead_id, name)}: заказ ушёл БЕЗ подтверждения клиентом, {note}. "
            f"Веду в успешную реализацию, потому что {why}.",
            responsible_id=lead.get("responsible_user_id"),
            values={"сделка": name or "без названия", "что_с_доставкой": note,
                    "текст_события": f"заказ ушёл без подтверждения клиентом, {note}",
                    "этап": str((stage or {}).get("status_name") or ""),
                    "бот": "", "ссылка_на_сделку": alerts.lead_link(lead_id)},
            panel_title="Авто-режим: сообщение не дошло",
            panel_url=AMO_LEAD_URL.format(lead_id),
        )
        await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано",
                      "шаблоны не дошли, но оплата не под вопросом")
        return
    await stop_here(
        lead, stage, "stop_not_delivered", f"шаблоны не дошли ({note}), заказ не оплачен",
        op_text=f"сообщение до клиента не дошло ({note}), заказ не оплачен, дальше не веду",
    )

# ── точка входа: изменение сделки ───────────────────────────────────────────────

def is_order(lead: dict) -> bool:
    """Робот ведёт ТОЛЬКО сделки с типом заявки «Заказ» (правка Кати 12.09.2026).

    Консультации, предзаказы, резервы - работа человека: их робот не трогает совсем, о
    них говорит только уведомление о заявке в ленте. Сравнение - строгим равенством:
    «Предзаказ» содержит слово «заказ», и поиск подстроки брал бы его в работу.

    ⚠️ Пустой тип прощается только в «Тесте»: тестовые сделки заводятся руками, и
    требовать от них заполненное поле значило бы не протестировать ничего. В бою заказ
    создаёт интеграция, и тип у него заполнен всегда - пустое поле там означает НЕ заказ.
    """
    value = str(amo_service.get_custom_field_value(lead, FIELD_APPLICATION_TYPE) or "").strip()
    if not value:
        return settings_client.get_mode() == "test"
    return value.casefold() == "заказ"


def entry_window_start(now: datetime.datetime | None = None) -> datetime.datetime | None:
    """С какого момента сделка считается «этого захода»: конец ПРЕДЫДУЩИХ рабочих часов.

    Правило Кати 27.09.2026 дословно: «если бота включили сегодня в 10, то он будет работать
    со всем, что появилось сегодня плюс сделки с 19 вчерашней даты до 10 сегодняшней». То есть
    ночные заказы - наши: их никто не видел, потому что смены не было. А заказ, пролежавший
    рабочий день, робот не трогает - его уже видели люди.

    Порогом в часах это не выражается: в понедельник «вчера» это пятница, а не воскресенье.
    Поэтому считаем по самим рабочим окнам - как `worktime_minutes` в стороже нового лида.

    None - часы работы не заданы: тогда гейта нет, решают другие проверки.
    """
    hours = _work_hours()
    if not hours:
        return None
    moment = (now or datetime.datetime.now(_MSK)).astimezone(_MSK)
    ends: list[datetime.datetime] = []
    for back in range(0, 9):
        day = (moment - datetime.timedelta(days=back)).date()
        for iv in hours:
            hh, mm = (int(x) for x in iv["end"].split(":"))
            end = datetime.datetime.combine(day, datetime.time(hh, mm), tzinfo=_MSK)
            if end <= moment:
                ends.append(end)
    return max(ends) if ends else None


def lead_is_fresh(lead: dict, now: datetime.datetime | None = None) -> bool:
    """Сделка появилась в этом заходе, а не лежит с прошлых дней.

    ⚠️ Гейт нужен в двух местах, и оба стоили нам шума. Уведомление о «новой заявке» уходило
    по сделке любого возраста: `/lead_change` приходит на ЛЮБОЕ изменение, и сделка, неделю
    стоящая на входном этапе, от правки поля выглядит новее некуда. А ведение по такой сделке
    27.09.2026 дало девять ложных «сообщение до клиента не дошло».

    ⚠️ Раньше здесь стоял потолок в часах (шесть на ведение, сутки на уведомление), и он
    отрезал ровно то, что отрезать нельзя: ночной заказ, пришедший в 21:12, к утру «старел».
    Теперь правило одно на оба случая - рабочие окна.
    """
    try:
        created = int(lead.get("created_at") or 0)
    except (TypeError, ValueError):
        return False
    if created <= 0:
        return False
    moment = (now or datetime.datetime.now(_MSK)).astimezone(_MSK)
    if created > moment.timestamp() + 60:
        return False           # сделка «из будущего» - данные врут, не угадываем
    start = entry_window_start(moment)
    if start is None:
        return True
    return created >= start.timestamp()


async def notify_entry_lead(lead: dict) -> None:
    """Заявка встала на вход воронки: лента панели, а для «не наших» - ещё и рабочий чат.

    Два разных случая, и адресат у них разный (правка Кати 27.09.2026):

    * заявка с типом «Заказ» - её ведёт робот, человеку делать нечего: только лента;
    * всё остальное (предзаказ, консультация, сделка по сообщению, звонок) - **робот её не
      ведёт**, и если об этом не сказать, заявка ляжет в воронку молча. Такие идут в топик
      УВЕДОМЛЕНИЯ с тегом ответственного и ссылкой.

    ⚠️ Дедуп чата - на диске (`store.claim_notice`), а не множеством в памяти: пересборка
    контейнера обнуляла память, и по всем сделкам на входном этапе уведомление уходило
    заново. Лента переживает повтор сама, по `dedupe_key`.
    """
    # Призрак сюда доходит намеренно: сказать он ничего не скажет (гейты в `dispatch_op` и
    # `panel_notify_bg`), зато в журнале останется строка «сказал бы в чат» - по ней видно,
    # сколько шума даёт этот поток, не заливая топик.
    if settings_client.get_mode() not in ("live", "shadow"):
        return
    # В ПИЛОТЕ (тумблер «на проде вести только тестовые контакты») - только белый список:
    # пока робот обкатывается, ни чат, ни лента не должны шуметь живым потоком.
    if limited_mode() == "пилот" and not whitelist_ok(lead):
        return
    lead_id = int(lead["id"])
    if not lead_is_fresh(lead):
        logger.info("autopilot: сделка %s старше окна уведомления, о заявке молчу", lead_id)
        return
    name = str(lead.get("name") or "").strip() or "сделка без названия"
    app_type = str(amo_service.get_custom_field_value(
        lead, FIELD_APPLICATION_TYPE) or "").strip()

    if is_order(lead):
        if lead_id in _lead_notified:
            return
        _lead_notified.add(lead_id)
        if len(_lead_notified) > 5000:
            _lead_notified.clear()
        panel_notify_bg(
            kind="autopilot_lead", level="warn",
            title="Новая заявка в рознице",
            body=name + ", тип: " + (app_type or "не указан"),
            url=AMO_LEAD_URL.format(lead_id),
            dedupe_key=f"ap-lead-{lead_id}",
        )
        return

    if not await asyncio.to_thread(store.claim_notice, lead_id, "lead_unhandled"):
        return
    kind = app_type or "не указан"
    if is_shadow():
        log_run(lead, None, action="lead_change", outcome="shadow_would_alert",
                reason=f"сказал бы в чат: заявка «{kind}», робот такие не ведёт",
                alert_target="op")
        return
    dispatch_op(
        EVENT_LEAD_UNHANDLED,
        f"{lead_link(lead_id, name)}: заявка «{kind}», робот такие не ведёт - возьмите в работу.",
        responsible_id=lead.get("responsible_user_id"),
        values={"сделка": name, "тип_заявки": kind,
                "ссылка_на_сделку": alerts.lead_link(lead_id)},
        panel_title="Заявка без робота", panel_level="warn",
        panel_url=AMO_LEAD_URL.format(lead_id),
        dedupe_key=f"ap-unhandled-{lead_id}",
    )


async def guarded(coro, *, what: str, lead_id=None) -> None:
    """Обёртка вокруг фоновой работы: исключение НЕ остаётся в логе молча.

    Требование Кати 27.09.2026 - «любой сбой = алерт в чат». До этого необработанное
    исключение в разборе вебхука или в фоновом цикле писалось в `logger.exception` и всё:
    сделка стояла недоведённой, а знал об этом только тот, кто открыл логи контейнера.
    """
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("autopilot: %s", what)
        lead = None
        if lead_id:
            try:
                lead = await load_lead(int(lead_id))
            except Exception:  # noqa: BLE001 - на алерт это влиять не должно
                lead = None
        alert_error(f"{what}: {type(exc).__name__}: {exc}",
                    lead=lead, lead_id=lead_id, dedupe=what)


def on_lead_change(lead_id) -> None:
    """Врезка в вебхук `/lead_change`. Синхронная и мгновенная: amoCRM ждёт быстрый ответ,
    а при задержке повторяет вебхук - и повтор стоил бы клиенту второго сообщения."""
    if not is_enabled():
        return
    try:
        lead_id = int(lead_id)
    except (TypeError, ValueError):
        return
    task = asyncio.create_task(guarded(
        handle_lead_change(lead_id), what="разбор изменения сделки", lead_id=lead_id,
    ))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def handle_lead_change(lead_id: int) -> None:
    """Разбор изменения сделки. Сюда же входим сами после собственного перевода этапа."""
    if not is_enabled():
        return
    lead = await load_lead(lead_id)
    if not lead:
        return
    pipeline_id = int(lead.get("pipeline_id") or 0)
    status_id = int(lead.get("status_id") or 0)

    want = settings_client.get_pipeline_id()
    if want and pipeline_id != want:
        # Сделку увели в другую воронку. Снимаем с ведения МОЛЧА: это обычный ход менеджера,
        # а не поломка, и алерт на каждый такой случай быстро научит чат нас не читать.
        await asyncio.to_thread(store.drop_lead, lead_id)
        return

    if status_id == STATUS_PAYMENT_RECEIVED:
        await on_payment_received(lead)
        return

    if status_id == (settings_client.get_entry_status_id() or 0):
        await notify_entry_lead(lead)

    if not is_order(lead):
        # Не заказ (консультация, предзаказ, резерв) - работа человека: робот такую
        # сделку не трогает СОВСЕМ (Катя 12.09.2026). Уведомление о заявке выше уже
        # ушло - этим «алертим» и ограничиваемся.
        return

    stage = settings_client.get_stage(status_id)
    if stage is None:
        return

    # ⚠️ Финальный этап чужой сделки не наш (поймано наблюдением 27.09.2026). В маршруте есть
    # карточка «Успешно реализовано», и по ней робот брал в ведение ЛЮБУЮ успешную сделку
    # компании: августовский «Заказ №17860» попал в УР и получил две строки журнала «этап
    # проходной» и «маршрут пройден». Записи и блокировки на сделки, которых робот не касался,
    # не нужны никому. Вели сами - строка в состоянии есть, и завершение маршрута отработает.
    if status_id == STATUS_SUCCESS or stage.get("is_final"):
        if not await asyncio.to_thread(store.list_for_lead, lead_id):
            logger.info("autopilot: сделка %s пришла в финал без ведения, не трогаю", lead_id)
            return

    # ⚠️ Гейт залежавшейся сделки (поймано боем 27.09.2026). `/lead_change` приходит на ЛЮБОЕ
    # изменение, поэтому сделка, простоявшая на входном этапе сутки, от синка или правки поля
    # попадает в ведение как новая. В 11:13 так забрались девять заказов с вечера, а в 11:28
    # по ним ушли девять алертов «сообщение до клиента не дошло» - ложных: бота на этом этапе
    # запускает грид, робот его не вызывал, и статусы по вчерашним сообщениям уже не придут.
    #
    # Ночная заявка гейтом не страдает: её вебхук приходит в момент создания, робот берёт её
    # сразу и просто спит до начала рабочих часов.
    if status_id == (settings_client.get_entry_status_id() or 0) and not lead_is_fresh(lead):
        logger.info("autopilot: сделка %s лежит с прошлых рабочих часов, в ведение не беру",
                    lead_id)
        # ⚠️ В журнал - ОДИН раз на сделку и этап. Гейт стоит до `claim`, значит отбивает каждый
        # вебхук, а их по стоящей сделке десятки: 27.09.2026 по одному заказу натекло 28 строк за
        # семь минут. Отметка на диске тут ровно к месту - её и так проверяет уведомление о заявке.
        if await asyncio.to_thread(store.claim_notice, lead_id, f"stale-{status_id}"):
            log_run(lead, stage, action="route", outcome="skipped_stale_entry",
                    reason="сделка лежит с прошлых рабочих часов, в ведение не беру")
        return

    if not whitelist_ok(lead):
        logger.info("autopilot: сделка %s не в белом списке (%s), не трогаю",
                    lead_id, limited_mode())
        return

    # Гейт от повторного вебхука. `/lead_change` приходит на ЛЮБОЕ изменение сделки: правку
    # поля, тег, смену ответственного. Работу берёт первый, остальные получают отказ.
    if not await asyncio.to_thread(store.claim, lead_id, status_id, pipeline_id):
        return

    ours = (lead_id, status_id) in _moved_by_us
    _moved_by_us.discard((lead_id, status_id))
    await run_stage(lead, stage, moved_by_us=ours)


async def on_payment_received(lead: dict) -> None:
    """Сделка дошла до «Оплата получена» - последний шаг маршрута.

    Трогаем ТОЛЬКО те сделки, которые вели сами: до этого этапа сделку могли довести руками
    или чужой автоматикой, и хватать чужое роботу нечего.
    """
    lead_id = int(lead["id"])
    rows = await asyncio.to_thread(store.list_for_lead, lead_id)
    if not rows:
        return

    # Сверка с МойСкладом (правка Кати 09.09.2026). На этот этап сделку переводит скрипт по
    # вебхуку платёжной системы - источник хороший, но одинокий. Склад видит те же деньги с
    # другой стороны, и расхождение двух источников стоит минуты менеджера.
    #
    # ⚠️ Молчание склада успех не блокирует: событие об оплате уже пришло, и держать сделку
    # из-за неотвечающего отчёта значит наказывать клиента за наш склад. А вот явное «не
    # оплачен» - останавливает: два источника разошлись, и решать это человеку.
    order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
    # ⚠️ С перепроверкой: платёж в складе появляется ПОЗЖЕ вебхука платёжной системы, и
    # первый же ноль - обычно гонка, а не расхождение. Разбор - в `order_paid_confirmed`.
    paid = await order_paid_confirmed(order_uuid)
    if paid is False:
        # ⚠️ Один раз на сделку. Эта ветка живёт ДО `store.claim`, поэтому её не защищает гейт
        # от повторного вебхука: 27.09.2026 по одному заказу такой алерт ушёл дважды за минуту с
        # половиной, а вебхуков по сделке, стоящей на этапе, приходят десятки.
        if not await asyncio.to_thread(store.claim_notice, lead_id, "pay-mismatch"):
            logger.info("autopilot: о расхождении оплаты по сделке %s уже говорили", lead_id)
            return
        await stop_here(
            lead, None, "failed",
            "платёжная система сообщила об оплате, а в МойСкладе заказ не оплачен",
            op_text="платёжная система говорит «оплачено», а в МойСкладе оплаты нет. "
                    "В успех не веду, посмотрите заказ.",
        )
        return

    checked = "оплата получена, сверено с МойСкладом" if paid else \
        "оплата получена, МойСклад промолчал - веду по событию платёжной системы"
    alert_op(
        f"{lead_link(lead_id, lead.get('name'))}: оплата получена, перевожу в успешную реализацию.",
        lead.get("responsible_user_id"), lead=lead,
    )
    log_run(lead, None, action="payment_fork", outcome="advanced",
            reason=checked, alert_target="op")
    await move_to(lead, None, STATUS_SUCCESS, "Успешно реализовано", checked)


# ── точка входа: события Wazzup ─────────────────────────────────────────────────

# messageId → пара «сделка и этап». Статусы доставки приходят отдельным событием и знают
# только номер сообщения, а чат в них не приходит. Карта живёт в памяти процесса, и это
# осознанно: САМИ статусы копятся на диске, поэтому рестарт теряет лишь связку свежих
# сообщений, а не результат ожидания.
_msg_owner: dict[str, tuple[int, int, str]] = {}
# ⚠️ Чаты ведомых сделок. Вебхук Wazzup приходит на КАЖДОЕ сообщение аккаунта - все чаты, все
# менеджеры, все воронки, - а роботу нужны только свои. Держим их множеством в памяти и выходим
# до всякой работы (правка Кати 27.09.2026: «хотелось бы, чтобы он хорошо выполнял эту одну
# узкую задачу и остальное его не касалось»). Множество обновляется при взятии сделки в ведение,
# при снятии и каждым фоновым тиком - то есть отстать от состояния оно может лишь на секунды.
_watched_chats: set[str] = set()


def refresh_watched_chats() -> None:
    """Перечитать чаты, которые робот сейчас ведёт. Дёшево: две выборки по индексу фазы."""
    try:
        rows = list(store.list_by_phase(store.PHASE_DELIVERY))
        rows += list(store.list_by_phase(store.PHASE_REPLY))
    except Exception:  # noqa: BLE001 - фильтр не должен ронять разбор сообщений
        logger.exception("autopilot: не смог обновить список ведомых чатов")
        return
    _watched_chats.clear()
    for row in rows:
        chat = str(row.get("chat_id") or "")
        if chat:
            _watched_chats.add(chat)


def watches_chat(candidates: list[str]) -> bool:
    """Есть ли среди ключей сообщения чат, который робот ведёт."""
    return any(c in _watched_chats for c in candidates)


# Заявки, о которых уже уведомили, - чтобы не дёргать панель на каждый вебхук сделки.
_lead_notified: set[int] = set()
_MSG_OWNER_MAX = 5000


def _digits(value) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def on_wazzup(payload) -> None:
    """Врезка в вебхук `/wazzup/{secret}` - четвёртый потребитель рядом с SLA, контролем
    доставки и пересылкой в панель. Отвечать Wazzup надо быстро, поэтому работа уходит в фон.
    """
    if not is_enabled() or not isinstance(payload, dict):
        return
    task = asyncio.create_task(guarded(handle_wazzup(payload), what="разбор события Wazzup"))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def handle_wazzup(payload: dict) -> None:
    """Разбор по элементам: сбой на одном сообщении не должен мешать соседним.

    Поэтому исключение ловим ЗДЕСЬ, а не только внешней обёрткой, - но теперь оно ещё и
    зовёт человека, а не просто ложится в лог (Катя 27.09.2026).
    """
    for message in payload.get("messages") or []:
        if isinstance(message, dict):
            try:
                await _handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("autopilot: не разобрал сообщение Wazzup")
                alert_error(f"не разобрал сообщение Wazzup: {type(exc).__name__}: {exc}",
                            dedupe="wazzup_message")
    for status in payload.get("statuses") or []:
        if isinstance(status, dict):
            try:
                await _handle_status(status)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("autopilot: не разобрал статус Wazzup")
                alert_error(f"не разобрал статус доставки Wazzup: {type(exc).__name__}: {exc}",
                            dedupe="wazzup_status")


def _pick_row(rows: list[dict], phase: str) -> dict | None:
    """Строка сделки в нужной фазе. Строк по чату может быть несколько: одна сделка проходит
    маршрут, и на каждом этапе остаётся своя. Берём самую свежую в нужной фазе."""
    fit = [r for r in rows if r.get("phase") == phase]
    return max(fit, key=lambda r: str(r.get("updated_at") or "")) if fit else None


def _chat_candidates(message: dict) -> list[str]:
    """По каким ключам искать сделку этого сообщения.

    ⚠️ Телефон - ключ только у WhatsApp. У Telegram `chatId` - телеграмный номер чата
    (например 1920391385), с телефоном контакта не совпадающий никогда, и склейка по одному
    `chatId` глушит телеграмные диалоги целиком: на первом живом прогоне 09.09.2026 бот
    написал в Telegram, человек ответил - робот не увидел ни статусов, ни ответа. Спасает
    то, что Wazzup кладёт в каждое сообщение ещё и `contact.phone` - ищем по обоим.
    Контакт без телефона в карточке Wazzup так и останется невидимым - это предел способа.
    """
    contact = message.get("contact") if isinstance(message.get("contact"), dict) else {}
    out: list[str] = []
    for value in (message.get("chatId"), (contact or {}).get("phone")):
        digits = _digits(value)
        if digits and digits not in out:
            out.append(digits)
    return out


async def _handle_message(message: dict) -> None:
    candidates = _chat_candidates(message)
    if not watches_chat(candidates):
        # Чужой чат: ни одной ведомой сделки по нему нет. Дальше не идём - ни в базу, ни в
        # разбор. Это и есть «остальное его не касается».
        return
    rows: list[dict] = []
    for chat in candidates:
        for row in await asyncio.to_thread(store.find_by_chat, chat):
            if row not in rows:
                rows.append(row)
    if not rows:
        return
    chat_type = str(message.get("chatType") or "").lower()

    if message.get("isEcho"):
        row = _pick_row(rows, store.PHASE_DELIVERY)
        if row is None:
            return
        if not is_robot_echo(message.get("authorName")):
            # Менеджер написал клиенту сам - это не доставка нашего шаблона, и судить по ней
            # нельзя. Молча выходим: работа человека роботу не мешает и его не касается.
            logger.info("autopilot: исходящее от человека в чате сделки %s - не считаю доставкой",
                        row["lead_id"])
            return
        message_id = str(message.get("messageId") or "").strip()
        if message_id:
            if len(_msg_owner) >= _MSG_OWNER_MAX:
                _msg_owner.clear()
            _msg_owner[message_id] = (row["lead_id"], row["status_id"], chat_type)
        status = str(message.get("status") or "").lower()
        if status:
            await record_delivery(row, status, chat_type)
        return

    # ⚠️ Уведомление ленты о входящем шлёт ДВИЖОК, и только по своим сделкам (правка Кати
    # 27.09.2026). Раньше их поднимала панель по КАЖДОМУ входящему аккаунта - она не знает,
    # ведёт ли робот эту сделку, и лента шумела чужими диалогами. Сюда мы попадаем, только
    # если чат нашёлся в ведомых, значит уведомление адресное по определению.
    if not message.get("isEcho") and settings_client.get_mode() == "live":
        inbox_notify(message)

    # Входящее. Ответ клиента - сам по себе доказательство доставки: человек не отвечает на
    # сообщение, которого не видел. Поэтому ждущую доставки сделку он закрывает вместе с
    # ожиданием, не дожидаясь отдельного статуса от Wazzup.
    row = _pick_row(rows, store.PHASE_REPLY) or _pick_row(rows, store.PHASE_DELIVERY)
    if row is None:
        return
    await on_client_answer(row, str(message.get("text") or ""), chat_type)


def inbox_notify(message: dict) -> None:
    """Уведомление ленты о входящем сообщении ведомой сделки.

    Формат повторяет панельный `inbox.message_notification`: тот же вид, тот же
    `dedupe_key` по номеру сообщения - кто бы ни доносил, уведомление одно.
    """
    message_id = str(message.get("messageId") or "").strip()
    if not message_id:
        return
    contact = message.get("contact") if isinstance(message.get("contact"), dict) else {}
    name = str((contact or {}).get("name") or "").strip() or str(
        message.get("chatId") or "клиент")
    text = str(message.get("text") or "").strip()
    msg_type = str(message.get("type") or "").strip()
    if text:
        body = text[:200]
    elif msg_type and msg_type != "text":
        body = f"вложение ({msg_type})"
    else:
        body = "сообщение без текста"
    chat = _digits(message.get("chatId")) or str(message.get("chatId") or "")
    panel_notify_bg(
        kind="autopilot_inbox", level="warn",
        title=f"Сообщение от {name}", body=body,
        url=f"/contacts?q={chat}" if chat else None,
        dedupe_key=f"ap-inbox-{message_id}",
    )


async def _handle_status(status: dict) -> None:
    message_id = str(status.get("messageId") or "").strip()
    owner = _msg_owner.get(message_id)
    if not owner:
        return
    lead_id, status_id, chat_type = owner
    row = await asyncio.to_thread(store.get, lead_id, status_id)
    if row is None:
        return
    await record_delivery(row, str(status.get("status") or "").lower(), chat_type)


def waited_s(row: dict, now: datetime.datetime | None = None) -> float:
    """Сколько секунд ждём доставку. Отсчёт от отметки запуска, а не от создания строки:
    сделка могла проспать ночь, и та ночь к ожиданию доставки отношения не имеет."""
    return _seconds_since(row.get("launch_ok_at") or row.get("created_at"), now)


def waiting_for_reply_s(row: dict, now: datetime.datetime | None = None) -> float:
    """Сколько секунд ждём ОТВЕТ клиента - от входа в фазу ожидания, а не от запуска бота.

    Отдельно от `waited_s`, потому что это другие часы: сообщение могло уйти вечером, доставка
    подтвердиться ночью, а ожидание ответа начаться только с этого момента.

    ⚠️ Считаем от `reply_since` - отметки, которая ставится один раз при входе в фазу (правка
    Кати 01.10.2026). Раньше счёт шёл от `updated_at`, а его двигает ЛЮБАЯ правка строки:
    подбор из переписки, статус доставки, запоминание чата. Робот сам обнулял свой счётчик, и
    срок ожидания не истекал никогда - сделки висели 39 и 94 часа без единого алерта.

    Старые строки отметки не имеют, для них остаётся прежний отсчёт: это хуже, но лучше, чем
    считать их ждущими с начала времён и высыпать алерты пачкой на первом же тике.
    """
    return _seconds_since(
        row.get("reply_since") or row.get("updated_at") or row.get("launch_ok_at"), now,
    )


def shift_iso(stamp: str, seconds: int) -> str:
    """Сдвинуть отметку времени на `seconds` (может быть отрицательным). Битую отдаём как есть."""
    try:
        moment = datetime.datetime.fromisoformat(str(stamp))
    except (TypeError, ValueError):
        return str(stamp)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_UTC)
    return (moment + datetime.timedelta(seconds=seconds)).isoformat()


def _at_or_after(stamp, edge: str) -> bool:
    """Отметка `stamp` не раньше `edge`. Нечитаемую отметку пропускаем: лучше разобрать лишнее
    сообщение, чем потерять ответ клиента из-за формата даты."""
    try:
        a = datetime.datetime.fromisoformat(str(stamp))
        b = datetime.datetime.fromisoformat(str(edge))
    except (TypeError, ValueError):
        return True
    if a.tzinfo is None:
        a = a.replace(tzinfo=_UTC)
    if b.tzinfo is None:
        b = b.replace(tzinfo=_UTC)
    return a >= b


def _seconds_since(stamp, now: datetime.datetime | None = None) -> float:
    try:
        moment = datetime.datetime.fromisoformat(str(stamp))
    except (TypeError, ValueError):
        return 0.0
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_UTC)
    return max(0.0, ((now or datetime.datetime.now(_UTC)) - moment).total_seconds())


async def record_delivery(row: dict, status: str, chat_type: str) -> None:
    """Записать статус Wazzup и, если доставка подтвердилась, перейти к ожиданию ответа."""
    if not status:
        return
    entry = {
        "status": status,
        "chatType": chat_type,
        "at": datetime.datetime.now(_UTC).isoformat(),
    }
    items = await asyncio.to_thread(
        store.add_delivery_status, row["lead_id"], row["status_id"], entry,
    )
    if row.get("phase") != store.PHASE_DELIVERY:
        return
    if delivery_verdict(items, waited_s(row), delivery_wait_s()) == VERDICT_OK:
        await on_delivered(row, items)


async def on_delivered(row: dict, items: list[dict]) -> None:
    lead = await load_lead(row["lead_id"])
    stage = settings_client.get_stage(row["status_id"])
    if lead is None or stage is None:
        return
    bot = bot_by_id(stage, row.get("bot_id"))
    if int(lead.get("status_id") or 0) != int(row["status_id"]):
        # Сделку увёл человек, пока мы ждали. Это его право и не повод писать в чат.
        await asyncio.to_thread(
            store.finish, row["lead_id"], row["status_id"], store.PHASE_STOPPED,
            "сделку увели с этапа, пока ждали доставки",
        )
        log_run(lead, stage, bot=bot, action="delivery", outcome="stop_left_stage",
                reason="сделку увели с этапа, пока ждали доставки")
        return
    await asyncio.to_thread(
        store.update, row["lead_id"], row["status_id"], phase=store.PHASE_REPLY,
    )
    log_run(lead, stage, bot=bot, action="delivery", outcome="waiting_reply",
            reason=delivery_note(items), delivery={"statuses": items})


async def on_client_answer(row: dict, text: str, chat_type: str = "",
                           answers: list[str] | None = None) -> None:
    """Ответ клиента. Решение принимает режим остановки бота, настроенный на экране.

    `answers` - все сообщения окна ответа, когда их несколько (подбор из переписки). Решение
    считается по ним целиком: подтверждение ищется в любом, а вопрос или отказ перебивает.
    Вебхук приносит по одному сообщению, и там список из одного элемента - это тот же случай.
    """
    lead = await load_lead(row["lead_id"])
    stage = settings_client.get_stage(row["status_id"])
    if lead is None or stage is None:
        return
    bot = bot_by_id(stage, row.get("bot_id"))
    if int(lead.get("status_id") or 0) != int(row["status_id"]):
        await asyncio.to_thread(
            store.finish, row["lead_id"], row["status_id"], store.PHASE_STOPPED,
            "сделку увели с этапа, пока ждали ответа",
        )
        log_run(lead, stage, bot=bot, action="reply", outcome="stop_left_stage",
                reason="сделку увели с этапа, пока ждали ответа", client_answer=text)
        return

    said = list(answers) if answers else [text]
    # В журнал кладём то, по чему решение и принято: при нескольких сообщениях - все, через « / ».
    shown = " / ".join(s.strip() for s in said if s.strip()) or text
    # ⚠️ Одно приветствие - это ещё НЕ ответ (правка Кати 01.10.2026, заказ 19400). Клиент
    # сперва здоровается, и только потом говорит по делу: зазор между двумя сообщениями по
    # замеру переписки - медиана 13 секунд. Решать по приветствию значит решать по сообщению,
    # в котором решения нет.
    #
    # Остаёмся в фазе ожидания, ничего не пишем людям и ждём продолжения. Следующее сообщение
    # придёт вебхуком сразу, а если вебхук потеряется - подбор из переписки вернётся сюда через
    # `AUTOPILOT_CATCHUP_INTERVAL_S` и разберёт окно целиком.
    #
    # ⚠️ Ждём не бесконечно, а `GREETING_GRACE_S` от начала ожидания ответа. Клиент, который
    # поздоровался и пропал, после этого срока разбирается ПО-СТАРОМУ - робот встаёт и звонит
    # менеджеру. Без этого срока такая сделка висела бы до суточного «клиент молчит», то есть
    # менеджер узнавал бы о ней на день позже, чем сейчас.
    #
    # ⚠️ Чего это НЕ лечит: приветствие, пришедшее позже срока ожидания (клиент поздоровался
    # через час после шаблона). Там всё остаётся как было - робот встанет на приветствии.
    # Лечится склейкой окна по времени САМОГО сообщения, это отдельная работа.
    if said and all(bare_greeting(s) for s in said):
        if waiting_for_reply_s(row) < GREETING_GRACE_S:
            logger.info("autopilot: по сделке %s клиент только поздоровался, жду продолжения",
                        row["lead_id"])
            log_run(lead, stage, bot=bot, action="reply", outcome="waiting_reply",
                    reason=f"клиент только поздоровался («{shown.strip()[:60]}») - это ещё не "
                           "ответ на шаблон, жду продолжения разговора",
                    client_answer=shown)
            return
        logger.info("autopilot: по сделке %s кроме приветствия ничего не пришло, зову человека",
                    row["lead_id"])

    if decide_on_answers(bot, said) == "advance":
        log_run(lead, stage, bot=bot, action="reply", outcome="advanced",
                reason=answer_verdict_note(bot, shown, "advance"), client_answer=shown)
        await advance(lead, stage, "клиент ответил так, как ждали", from_bot=bot)
        return

    listed = [normalize_answer(a) for a in (bot.get("stop_answers_norm")
                                            or bot.get("stop_answers") or [])]
    known = any(normalize_answer(s) in listed for s in said)
    outcome = "stop_fix_requested" if known else "stop_free_text"
    # Состоянию сделки нужна короткая причина, журналу - разбор целиком: в состоянии эта
    # строка живёт как пометка «почему стоим», её читают в отладке, а не человек на экране.
    note = ("клиент выбрал ответ, на котором робот останавливается" if known
            else "клиент ответил не кнопкой, а своими словами")
    reason = f"{answer_verdict_note(bot, shown, 'stop')}. {note}"
    await asyncio.to_thread(
        store.update, row["lead_id"], row["status_id"], phase=store.PHASE_STOPPED, note=note,
    )
    log_run(lead, stage, bot=bot, action="reply", outcome=outcome, reason=reason,
            client_answer=shown, alert_target="op")
    answer = shown.strip()
    alert_op(
        f"{lead_link(int(lead['id']), lead.get('name'))}: клиент ответил «{answer[:200]}». "
        "Дальше не веду, посмотрите переписку.",
        lead.get("responsible_user_id"), lead=lead,
    )


def learn_chat_id(lead_id: int, status_id: int, known: str, data: dict) -> None:
    """Запомнить чат, в котором на самом деле идёт переписка.

    Нашли сообщения по ИМЕНИ - значит у сделки телеграмный чат, а в состоянии лежит телефон.
    Записываем настоящий `chat_id`: со следующего сообщения робот узнает чат прямо по вебхуку,
    без подбора, и ответ клиента разберётся сразу, а не через пять минут.

    ⚠️ Берём чат только если он ОДИН: несколько разных чатов в ответе означают тёзок, и
    привязывать сделку к одному из них наугад нельзя.
    """
    chats = {
        str(item.get("chat_id") or "")
        for group in ("inbound", "echo")
        for item in (data.get(group) or [])
        if item.get("chat_id")
    }
    if len(chats) != 1:
        return
    found = chats.pop()
    if not found or found == str(known or ""):
        return
    try:
        store.update(lead_id, status_id, chat_id=found)
        refresh_watched_chats()
        logger.info("autopilot: запомнил чат %s по сделке %s (был %s)", found, lead_id, known)
    except Exception:  # noqa: BLE001 - не смогли запомнить, значит просто подберём снова
        logger.exception("autopilot: не смог запомнить чат сделки %s", lead_id)


async def fetch_chat_activity(chat_id: str, since: str,
                              name: str = "") -> dict[str, Any] | None:
    """Что было в чате после `since` - спрашиваем панель. None - спросить не удалось.

    `name` - имя контакта, второй ключ поиска (правка Кати 01.10.2026). Без него телеграмная
    переписка не находится: у Telegram `chat_id` анонимный, телефона в теле вебхука нет у 85%
    сообщений, а робот держит в состоянии телефон. Из-за этого 12 ответов из 15 телеграмных
    сделок призрака остались неразобранными - среди них «Да» и «Здравствуйте! Да, все верно».
    """
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        return None
    if not chat_id and not str(name or "").strip():
        return None
    base = TEAM_PANEL_BASE_URL.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(
                base + "/api/ingest/autopilot/chat-activity",
                params={"chat": chat_id, "since": since, "name": name},
                headers={"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN},
            )
        if resp.status_code >= 400:
            logger.warning("autopilot: панель не отдала переписку, ответ %s", resp.status_code)
            return None
        return resp.json()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot: не смог спросить у панели переписку чата")
        return None


async def report_not_delivered(lead: dict, stage: dict | None, bot: dict | None,
                               status: str, chat_type: str, source: str) -> None:
    """Объявить недоставку: снять с ведения и позвать менеджера. Одно место на три пути -
    сразу после запуска бота, на истечении окна и в подборе из переписки."""
    note = f"{chat_type or 'канал'}: отказ канала ({source})"
    log_run(lead, stage, bot=bot, action="delivery", outcome="stop_not_delivered",
            reason=note, delivery={"statuses": [{"status": status, "chatType": chat_type}]})
    await stop_here(
        lead, stage, "stop_not_delivered", note, bot=bot,
        op_text=f"сообщение до клиента не дошло: {note}. Дальше не веду.",
        event_key=EVENT_NOT_DELIVERED,
        values={"что_с_доставкой": note,
                "этап": str((stage or {}).get("status_name") or ""),
                "бот": str((bot or {}).get("bot_name") or "")},
        panel_title="Авто-режим: сообщение не дошло",
    )


async def panel_delivery_verdict(chat_id: str, since: str,
                                 name: str = "") -> tuple[str, str, str] | None:
    """Что панель знает о судьбе НАШЕГО сообщения в этом чате после `since`.

    Возвращает `(исход, статус, канал)`: `ok` - дошло, `error` - Wazzup отбил явной ошибкой.
    None - панель не ответила или сказать нечего.

    ⚠️ Ошибку ищем наравне с успехом, и это главное. 27.09.2026 по заказу 19303 Wazzup отбил
    шаблон с `BAD_CONTACT` за 21 секунду ДО того, как робот начал слушать чат: вебхук статуса
    он связать не мог, а подбор смотрел только на успешные статусы - и робот сказал «отправку
    подтвердить нечем, жду ответ клиента», тогда как сторож доставки в том же контейнере уже
    знал, что клиент сообщения не получил, и написал об этом примечание в сделку.
    """
    data = await fetch_chat_activity(chat_id, since, name)
    if not data:
        return None
    return echo_verdict(data)


def echo_verdict(data: dict) -> tuple[str, str, str] | None:
    """Тот же вердикт, что у `panel_delivery_verdict`, но по УЖЕ полученной переписке - чтобы
    не спрашивать панель дважды. Имя другое: `delivery_verdict` выше судит по статусам нашего
    ведения и окну ожидания, а это - по тому, что видно в переписке панели."""
    for item in (data.get("echo") or []):
        if not is_robot_echo(item.get("author_name")):
            continue
        status = str(item.get("status") or "").lower()
        chat_type = str(item.get("chat_type") or "")
        if status in DELIVERED_STATUSES or status == "sent":
            return "ok", status, chat_type
        if status == "error":
            return "error", status, chat_type
    return None


def template_sent_at(data: dict) -> str:
    """Когда автоматика последний раз писала клиенту в этом чате. Пусто - не писала.

    Это и есть честная граница «что считать ответом»: ответ - то, что пришло ПОСЛЕ шаблона.
    Граница «после того, как робот начал слушать» неверна, и 28.09.2026 это стоило сделки:
    ночной шаблон ушёл клиенту в 08:52:57, клиент ответил «Да, всё верно» в 08:56:34, а робот
    в это время спал до начала рабочих часов. Проснувшись в 10:00, он считал ответом только
    то, что придёт после 10:00, - и ждал ответа, который уже лежал в переписке.
    """
    times = [
        str(item.get("at") or "")
        for item in (data.get("echo") or [])
        if is_robot_echo(item.get("author_name")) and item.get("at")
    ]
    return max(times) if times else ""


def answer_window(inbound: list[dict]) -> list[dict]:
    """Сообщения, которые считаем ОТВЕТОМ на шаблон: первое входящее и всё, что пришло в
    пределах `AUTOPILOT_ANSWER_WINDOW_MIN` минут после него. Список приходит свежим вперёд,
    таким же и отдаём.

    Окно нужно, чтобы не принять за ответ на шаблон разговор с менеджером: за сутки один
    клиент писал 26 сообщений за несколько часов, и решать по ним о подтверждении заказа
    нельзя. А вот три сообщения подряд в пределах минуты - это один ответ, разбитый на части.
    """
    items = [i for i in inbound if i.get("at")]
    if not items:
        return list(inbound[:1])
    first = min(items, key=lambda i: str(i.get("at")))
    edge = shift_iso(str(first.get("at")), AUTOPILOT_ANSWER_WINDOW_MIN * 60)
    inside = [i for i in items if str(i.get("at")) <= edge]
    return inside or [first]


def first_answer_after(data: dict, border: str) -> dict | None:
    """Самое свежее входящее после границы. Панель отдаёт список свежим вперёд."""
    for item in (data.get("inbound") or []):
        if _at_or_after(item.get("at"), border):
            return item
    return None


def lead_since_iso(lead: dict, fallback_hours: int = 24) -> str:
    """С какого момента спрашивать переписку: от создания сделки, но не глубже суток.

    От создания, потому что бот грида срабатывает на входе воронки - почти одновременно со
    сделкой, а робот может прийти и через десять часов, после ночи.
    """
    try:
        created = int(lead.get("created_at") or 0)
    except (TypeError, ValueError):
        created = 0
    now = datetime.datetime.now(_UTC)
    floor = now - datetime.timedelta(hours=fallback_hours)
    moment = datetime.datetime.fromtimestamp(created, _UTC) if created else floor
    return max(moment, floor).isoformat()


async def confirm_grid_send(lead: dict, stage: dict, bot: dict, chat_id: str,
                            name: str = "") -> bool:
    """Сразу узнать у панели, что стало с шаблоном грида: дошёл, отбит или пока ничего.

    Дошёл - переходим к ожиданию ответа, не тратя окно. Отбит - это доказанная недоставка,
    зовём человека немедленно: ждать ответа от клиента, которого нет в WhatsApp, бессмысленно.

    Возвращает True, если вопрос доставки уже решён (дошло или доказанно не дошло). Вызывающий
    по этому признаку молчит про ожидание: 28.09.2026 по заказу 19307 в журнале одна за другой
    стояли строки «шаблон подтверждён (read), жду ответ клиента» и «жду подтверждения доставки
    от Wazzup» - робот работал правильно, а читалось это как «ждём того, что уже случилось».
    """
    lead_id = int(lead["id"])
    status_id = int(lead.get("status_id") or 0)
    if not chat_id and not name:
        return False
    data = await fetch_chat_activity(chat_id, lead_since_iso(lead), name)
    if not data:
        return False
    learn_chat_id(lead_id, status_id, chat_id, data)
    verdict = echo_verdict(data)
    if verdict is None:
        return False
    outcome, status, chat_type = verdict
    border = template_sent_at(data)
    if outcome == "ok":
        log_run(lead, stage, bot=bot, action="delivery", outcome="waiting_reply",
                reason=f"шаблон уже уходил и подтверждён ({status}), жду ответ клиента",
                delivery={"statuses": [{"status": status, "chatType": chat_type}]})
        # ⚠️ Ждать начинаем с момента ШАБЛОНА, а не с момента, когда робот это заметил
        # (дежурство 01.10.2026). Шаблон по заказу 19383 ушёл в 19:37, а робот подхватил
        # сделку утром в 10:23 - и срок «клиент молчит сутки» поехал бы с утра: менеджер
        # узнал бы о молчании через 39 часов вместо 24. Так же бывает после пересборки и
        # после потерянного вебхука - подхват всегда позже отправки.
        await asyncio.to_thread(
            store.update, lead_id, status_id,
            phase=store.PHASE_REPLY, note="шаблон подтверждён по переписке панели",
            **({"reply_since": border} if border else {}),
        )
        logger.info("autopilot: по сделке %s шаблон уже подтверждён (%s), жду ответ",
                    lead_id, status)
        # ⚠️ Клиент мог ответить, пока робот спал. Смотрим ЗДЕСЬ же, одним и тем же ответом
        # панели: иначе ответ нашёлся бы только подбором через пять минут, а в кейсе 28.09.2026
        # (заказ 19307) не нашёлся бы никогда - подбор смотрел переписку от начала ожидания.
        # ⚠️ Берём ВСЕ сообщения окна, а не одно (правка Кати 01.10.2026). Ретро-прогон ночных
        # сделок поймал это сразу: по заказу 19388 здесь брался один ответ - и им оказывалось
        # последнее сообщение «Заказ подтверждаю», хотя подтверждение «Да, всё верно» пришло
        # двумя сообщениями раньше. Та же беда, что в подборе, но другим путём.
        after = [i for i in (data.get("inbound") or []) if _at_or_after(i.get("at"), border)]
        window = answer_window(after) if after else []
        if window:
            first = window[-1]
            logger.info(
                "autopilot: по сделке %s ответ клиента пришёл до нас (%s, сообщений %s)",
                lead_id, str(first.get("at") or ""), len(window),
            )
            await on_client_answer(
                {"lead_id": lead_id, "status_id": status_id,
                 "bot_id": int(bot.get("bot_id") or 0)},
                str(first.get("text") or ""), str(first.get("chat_type") or ""),
                answers=[str(i.get("text") or "") for i in window],
            )
        return True
    await report_not_delivered(lead, stage, bot, status, chat_type, "по переписке панели")
    return True


async def catch_up_on_chats() -> None:
    """Подбор пропущенного по ведомым сделкам: раз в `AUTOPILOT_CATCHUP_INTERVAL_S`.

    ⚠️ Зачем вообще, если есть вебхуки. Потому что вебхук может не дойти, и тогда сделка
    зависает молча. По заказу 19288 (27.09.2026) так и вышло: бот грида отправил шаблон вечером,
    клиент ответил «Да, всё верно» в 14:00, а робот ответа не увидел. Просьба Кати в тот же
    день - «отслеживаемые сделки надо проверять хотя бы каждые 5 мин».

    Спрашиваем ПАНЕЛЬ, а не Wazzup: всю переписку она и так собирает, второй копии не нужно.
    Нашлось входящее - ведём себя точно так же, как по вебхуку: тот же `on_client_answer`, то
    же решение по режиму бота. Нашлось только эхо со статусом - дозаписываем статус доставки.
    """
    global _catchup_at
    now = datetime.datetime.now(_UTC).timestamp()
    if now - _catchup_at < AUTOPILOT_CATCHUP_INTERVAL_S:
        return
    _catchup_at = now

    rows = list(await asyncio.to_thread(store.list_by_phase, store.PHASE_REPLY))
    rows += list(await asyncio.to_thread(store.list_by_phase, store.PHASE_DELIVERY))
    for row in rows:
        chat_id = str(row.get("chat_id") or "")
        # ⚠️ Окно считаем от того, когда робот ВЗЯЛ сделку, а не когда начал ждать ответ. Между
        # этими моментами помещается целая ночь: заказ пришёл в 08:52, робот его взял и уснул до
        # десяти, шаблон и ответ клиента прошли в те же минуты. Окно от начала ожидания
        # заканчивалось в 09:00 и не покрывало ни шаблон, ни ответ (заказ 19307, 28.09.2026).
        started = str(row.get("created_at") or row.get("launch_ok_at") or "")
        since = str(row.get("launch_ok_at") or row.get("created_at") or "")
        if not since:
            continue
        # ⚠️ Спрашиваем с ЗАПАСОМ назад. По заказу 19303 отказ канала пришёл за 21 секунду ДО
        # того, как робот начал слушать (он ждал телефон), и подбор с точным `since` его не
        # находил - ровно тот же зазор, из-за которого кейс и случился.
        window = shift_iso(started or since, -AUTOPILOT_CATCHUP_LOOKBACK_S)
        # Имя контакта - второй ключ поиска, без него телеграмная переписка не находится. Берём
        # его из СОСТОЯНИЯ, а не из amoCRM: подбор идёт каждые пять минут по всем ждущим
        # сделкам, и два лишних запроса на каждую - это дорого и незачем.
        name = str(row.get("contact_name") or "")
        data = await fetch_chat_activity(chat_id, window, name)
        if not data:
            continue
        learn_chat_id(row["lead_id"], row["status_id"], chat_id, data)
        # ⚠️ Ответом считаем то, что пришло ПОСЛЕ шаблона, а не после начала ожидания.
        # Сообщение, написанное до шаблона, ответом на него не является - принять его за ответ
        # значило бы двинуть сделку по чужим словам. А шаблона в переписке не видно - остаётся
        # прежняя, более осторожная граница.
        border = template_sent_at(data) or since
        inbound = [i for i in (data.get("inbound") or []) if _at_or_after(i.get("at"), border)]
        echo = (data.get("echo") or [])
        if not inbound:
            # Доставка могла подтвердиться - или отбиться - статусом, вебхук которого не дошёл.
            for item in echo:
                if not is_robot_echo(item.get("author_name")):
                    continue
                status = str(item.get("status") or "").lower()
                chat_type = str(item.get("chat_type") or "")
                if status == "error":
                    # ⚠️ Отказ канала зовёт человека НЕМЕДЛЕННО, даже если сделка уже ждёт
                    # ответа. Иначе выходит кейс 19303: робот сутки ждёт ответа от клиента,
                    # которого нет в WhatsApp, при том что ошибка уже лежит в переписке.
                    lead = await load_lead(row["lead_id"])
                    if lead is None:
                        break
                    stage = settings_client.get_stage(row["status_id"])
                    await report_not_delivered(lead, stage, bot_by_id(stage, row.get("bot_id")),
                                               status, chat_type, "по переписке панели")
                    break
                if status in DELIVERED_STATUSES or status == "sent":
                    await record_delivery(row, status, chat_type)
                    break
            continue
        # ⚠️ Берём ВСЕ сообщения окна ответа, а не одно (правка Кати 01.10.2026). Панель отдаёт
        # свежее первым, и `inbound[0]` означало «последнее слово клиента»: по заказу 19388 это
        # оказалось «Заказ подтверждаю», а подтверждение «Да, всё верно» лежало двумя сообщениями
        # раньше - робот позвал человека к подтверждённому заказу.
        window = answer_window(inbound)
        texts = [str(item.get("text") or "") for item in window]
        first = window[-1]
        logger.info(
            "autopilot: подобрал ответ клиента по сделке %s из переписки панели (%s, сообщений %s)",
            row["lead_id"], str(first.get("at") or ""), len(texts),
        )
        await on_client_answer(row, str(first.get("text") or ""),
                               str(first.get("chat_type") or ""), answers=texts)


async def check_reply_windows() -> None:
    """Клиент молчит сутки - зовём человека и снимаем сделку с ведения (Катя 27.09.2026).

    ⚠️ До этого фаза «ждём ответ» не имела срока вообще: сделка висела, пока её молча не уберёт
    уборка по давности (14 дней). То есть заказ без подтверждения лежал, и НИКТО об этом не
    узнавал - ни менеджер, ни мы.

    ⚠️ Это не «напоминание клиенту». Своих напоминаний молчащему клиенту робот не шлёт (решение
    Кати 08.09.2026) - сообщение идёт МЕНЕДЖЕРУ, а клиента дальше ведёт человек.
    """
    limit = AUTOPILOT_REPLY_WAIT_H * 3600
    for row in await asyncio.to_thread(store.list_by_phase, store.PHASE_REPLY):
        waited = waiting_for_reply_s(row)
        if waited < limit:
            continue
        lead = await load_lead(row["lead_id"])
        if lead is None:
            continue
        stage = settings_client.get_stage(row["status_id"])
        bot = bot_by_id(stage, row.get("bot_id"))
        hours = int(waited // 3600)
        if int(lead.get("status_id") or 0) != int(row["status_id"]):
            # Сделку увели с этапа - это право человека, и шуметь не о чем.
            await asyncio.to_thread(
                store.finish, row["lead_id"], row["status_id"], store.PHASE_STOPPED,
                "сделку увели с этапа, пока ждали ответа",
            )
            continue
        # ⚠️ Прежде чем сказать «клиент молчит», смотрим НЕ ТОЛЬКО мессенджер (просьба Кати
        # 30.09.2026). По заказу 19377 клиент подтвердил заказ письмом через две минуты и семь
        # минут говорил с менеджером по телефону - для робота этого не существовало, и он
        # честно собирался написать «клиент не подтвердил заказ».
        touch = await human_touch_after(lead, row)
        if touch:
            await asyncio.to_thread(
                store.finish, row["lead_id"], row["status_id"], store.PHASE_STOPPED, touch,
            )
            refresh_watched_chats()
            log_run(lead, stage, bot=bot, action="reply", outcome="stop_off_channel",
                    reason=f"в мессенджере тишина {hours} ч, но {touch} - дальше человек")
            continue
        await stop_here(
            lead, stage, "stop_no_reply", f"клиент не ответил за {hours} ч", bot=bot,
            op_text=f"клиент не подтвердил заказ за {hours} ч - напишите или позвоните сами. "
                    "Дальше не веду.",
            event_key=EVENT_NO_REPLY,
            values={"сколько_ждали": f"{hours} ч",
                    "этап": str((stage or {}).get("status_name") or "")},
            panel_title="Авто-режим: клиент не отвечает",
        )


async def human_touch_after(lead: dict, row: dict) -> str:
    """Общались ли с клиентом ВНЕ мессенджера после того, как робот начал ждать ответ.

    Просьба Кати 30.09.2026: «лучше смотреть не только wazzup, если это возможно». Возможно:
    письма и звонки лежат в примечаниях КОНТАКТА (не сделки, это проверено живьём), и оттуда
    видно факт и время - входящее письмо (`amomail_message` с `income`) и состоявшийся звонок
    (`call_in`/`call_out` с длительностью).

    ⚠️ Возвращаем ОПИСАНИЕ касания, а не «да/нет», и вперёд по нему сделку НЕ ведём. Тела письма
    amoCRM в примечании не отдаёт - «Подтверждаю» и «отмените заказ» выглядят для нас одинаково,
    и решать за клиента мы не вправе. Наше дело скромнее: не говорить «клиент молчит», когда он
    не молчал, и отдать сделку человеку без ложного алерта.
    """
    started = str(row.get("launch_ok_at") or row.get("created_at") or "")
    if not started:
        return ""
    for contact in (((lead.get("_embedded") or {}).get("contacts")) or [])[:2]:
        contact_id = contact.get("id")
        if not contact_id:
            continue
        try:
            data = await amo_service._do_get(
                f"/api/v4/contacts/{contact_id}/notes",
                [("order[created_at]", "desc"), ("limit", "20")],
            )
        except Exception:  # noqa: BLE001 - молчание amo не повод соврать про клиента
            logger.exception("autopilot: не смог прочитать примечания контакта %s", contact_id)
            return ""
        for note in ((data or {}).get("_embedded") or {}).get("notes") or []:
            stamp = note.get("created_at")
            if not stamp:
                continue
            at = datetime.datetime.fromtimestamp(int(stamp), _UTC)
            if not _at_or_after(at.isoformat(), started):
                continue
            kind = str(note.get("note_type") or "")
            params = note.get("params") or {}
            when = at.astimezone(_MSK).strftime("%d.%m в %H:%M")
            if kind == "amomail_message" and str(params.get("income")).lower() == "true":
                return f"клиент ответил письмом {when}"
            if kind in ("call_in", "call_out"):
                try:
                    seconds = int(params.get("duration") or 0)
                except (TypeError, ValueError):
                    seconds = 0
                if seconds > 0:
                    side = "клиент звонил" if kind == "call_in" else "с клиентом говорили"
                    return f"{side} по телефону {when}, {max(seconds // 60, 1)} мин"
    return ""


async def check_delivery_windows() -> None:
    """Окно ожидания доставки истекает молча - никакого события об этом не приходит.

    ⚠️ Провал объявляем ТОЛЬКО здесь, по истечении окна. Отказ отдельного канала провалом не
    считается: бот перебирает каналы, и жалоба телеграма ничего не говорит про ватсап. Ровно
    на этом мы обожглись 08.09.2026, читая примечание «SYSTEM WZ» в ленте amoCRM.
    """
    limit = delivery_wait_s()
    for row in await asyncio.to_thread(store.list_by_phase, store.PHASE_DELIVERY):
        waited = waited_s(row)
        if waited < limit:
            continue
        items = row.get("delivery") or []
        verdict = delivery_verdict(items, waited, limit)
        if verdict == VERDICT_OK:
            await on_delivered(row, items)
            continue
        lead = await load_lead(row["lead_id"])
        stage = settings_client.get_stage(row["status_id"])
        if lead is None:
            continue
        bot = bot_by_id(stage, row.get("bot_id"))
        note = (delivery_note(items) if verdict == VERDICT_FAILED
                else "ни одного статуса от Wazzup, похоже, сообщение и не отправлялось")

        # ⚠️ «Ни одного статуса» по боту, которого запускал ГРИД, - это не «не доставлено» и
        # НЕ повод бросать сделку. Мы его не вызывали и не знаем, стрелял ли он вообще: по
        # заказу 19288 бот грида отправил шаблон ВЧЕРА в 21:12 (ночная версия бота, «в не вр»),
        # статусы по нему прошли до включения робота, а клиент ответил «Да, всё верно» в 14:00 -
        # и этот ответ ушёл в пустоту, потому что в 11:28 сделку сняли с ведения.
        #
        # Поэтому переходим к ожиданию ОТВЕТА и продолжаем слушать чат: ответ клиента сам по
        # себе доказательство доставки. Менеджера не дёргаем, технарям говорим один раз.
        if verdict == VERDICT_SILENT and str((bot or {}).get("launched_by") or "engine") != "engine":
            # ⚠️ Прежде чем сказать «подтвердить нечем», спрашиваем панель. По заказу 19303
            # Wazzup отбил шаблон за 21 секунду ДО того, как робот начал слушать чат: связать
            # вебхук статуса он не мог, а в переписке панели ошибка лежала - и сторож доставки
            # уже написал о ней примечание в сделку. Робот обязан знать не меньше сторожа.
            from_panel = await panel_delivery_verdict(
                str(row.get("chat_id") or ""), lead_since_iso(lead))
            if from_panel is not None:
                outcome, status, chat_type = from_panel
                if outcome == "error":
                    await report_not_delivered(lead, stage, bot, status, chat_type,
                                               "по переписке панели")
                    continue
                await record_delivery(row, status, chat_type)
                continue
            # Ждать начинаем от отметки запуска бота: подтверждения доставки нет вовсе,
            # и ближайшее честное «когда клиенту написали» - это она. См. разбор выше.
            await asyncio.to_thread(
                store.update, row["lead_id"], row["status_id"], phase=store.PHASE_REPLY,
                note="подтверждения доставки не было, слушаю ответ клиента",
                **({"reply_since": row["launch_ok_at"]} if row.get("launch_ok_at") else {}),
            )
            log_run(lead, stage, bot=bot, action="delivery", outcome="waiting_reply",
                    reason="статусов от Wazzup нет, бота запускает грид - жду ответ клиента, "
                           "с ведения не снимаю")
            if await asyncio.to_thread(store.claim_notice, row["lead_id"],
                                       f"no-proof-{row['status_id']}"):
                alert_tech(
                    f"{lead_link(row['lead_id'], lead.get('name'))}: статусов доставки от Wazzup "
                    "не пришло, а бота на этапе запускает грид - отправку подтвердить нечем. "
                    "Сделку не бросаю, жду ответ клиента; менеджеру не писал."
                )
            continue

        log_run(lead, stage, bot=bot, action="delivery", outcome="stop_not_delivered",
                reason=note, delivery={"statuses": items})
        if _flag("force_ur_when_templates_failed"):
            await force_ur(lead, stage, note)
            continue
        await stop_here(
            lead, stage, "stop_not_delivered", note, bot=bot,
            op_text=f"сообщение до клиента не дошло: {note}. Дальше не веду.",
            event_key=EVENT_NOT_DELIVERED,
            values={"что_с_доставкой": note,
                    "этап": str((stage or {}).get("status_name") or ""),
                    "бот": str((bot or {}).get("bot_name") or "")},
            panel_title="Авто-режим: сообщение не дошло",
        )


# ── жизненный цикл ──────────────────────────────────────────────────────────────

async def init() -> None:
    if not AUTOPILOT_ENABLED:
        logger.info("autopilot: выключен флагом AUTOPILOT_ENABLED")
        return
    # Хранилище само про режимы не знает: говорим ему, чем спросить про призрака, - у него
    # для призрака отдельная база, иначе репетиция заняла бы боевые пары «сделка и этап».
    store.set_shadow_probe(is_shadow)
    await asyncio.to_thread(store.init)
    await asyncio.to_thread(refresh_watched_chats)
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
        # Сделку читаем ради тега ответственного и названия: алерт без тега легко теряется
        # в топике, а «сделка» без имени заставляет менеджера открывать ссылку, чтобы
        # понять, о ком речь.
        lead = await load_lead(row["lead_id"])
        alert_error(
            "робот перезапустился в момент запуска бота и не знает, ушло сообщение или нет. "
            "Повторно не отправляю, посмотрите переписку.",
            lead=lead, lead_id=row["lead_id"], dedupe=f"unfinished-{row['status_id']}",
        )
    if rows:
        logger.warning("autopilot: %s незавершённых запусков после рестарта", len(rows))


async def _tick_loop() -> None:
    while True:
        try:
            await tick_once()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("autopilot: ошибка фонового цикла")
            # Дедуп обязателен: тик идёт раз в минуту, и одна незалеченная ошибка без него
            # дала бы шестьдесят одинаковых сообщений в час.
            alert_error(f"ошибка фонового цикла: {type(exc).__name__}: {exc}", dedupe="tick")
        await asyncio.sleep(AUTOPILOT_TICK_INTERVAL_S)


async def tick_once() -> None:
    """Фон делает ровно две вещи: будит уснувших до утра и убирает протухшие записи.

    Таймеров дожима здесь нет и не будет: своих напоминаний молчащему клиенту мы не шлём
    (решение Кати 08.09.2026). Если в воронке есть чужая автоматика напоминания - она и
    работает, движок ей не мешает.
    """
    if not is_enabled():
        return
    await asyncio.to_thread(refresh_watched_chats)
    dropped = await asyncio.to_thread(store.purge_older_than, AUTOPILOT_STATE_TTL_DAYS)
    if dropped:
        logger.info("autopilot: снято с ведения по сроку давности: %s", dropped)
    # Отметки «об этой заявке уже сказали» живут дольше состояния: сделка, о которой
    # сказали месяц назад, на входной этап не вернётся, а вернётся - сказать заново верно.
    await asyncio.to_thread(store.purge_notices_older_than, 60)
    await check_delivery_windows()
    await check_reply_windows()
    await catch_up_on_chats()
    if not in_work_hours():
        return
    for row in await asyncio.to_thread(store.list_due):
        logger.info("autopilot: сделка %s проснулась на этапе %s", row["lead_id"], row["status_id"])
        await asyncio.to_thread(
            store.update, row["lead_id"], row["status_id"],
            phase=store.PHASE_LAUNCHING, wake_at=None,
        )
        lead = await load_lead(row["lead_id"])
        stage = settings_client.get_stage(row["status_id"])
        if lead is None or stage is None:
            continue
        if int(lead.get("status_id") or 0) != int(row["status_id"]):
            # За ночь сделку увели. Продолжать с того места, где её уже нет, нельзя.
            await asyncio.to_thread(
                store.finish, row["lead_id"], row["status_id"], store.PHASE_STOPPED,
                "за ночь сделку увели с этапа",
            )
            continue
        await run_stage(lead, stage)
