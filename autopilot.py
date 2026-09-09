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
import ms_client
import telegram_bot
from tg_recipients import NOTIFY_CHAT_ID, NOTIFY_THREAD_ID, mentions_for
from waybill_config import (
    AUTOPILOT_ENABLED,
    AUTOPILOT_HOURLY_CAP,
    AUTOPILOT_STATE_TTL_DAYS,
    AUTOPILOT_TICK_INTERVAL_S,
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
    # В режиме «Тест» менеджеров не дёргаем: события тестового прогона читает тот, кто
    # тестирует, и читает он их в техническом чате. Иначе в рабочий топик УВЕДОМЛЕНИЯ
    # полетело бы «клиент ответил...» по сделке, которой не существует.
    if settings_client.get_mode() == "test":
        _send_bg("🤖 Авто-режим, ТЕСТОВЫЙ прогон" + chr(10) + text)
        return
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
    """Что делать с ответом клиента: `advance` - следующий шаг, `stop` - позвать человека.

    Режимы названы с экрана, заголовок там - «Когда идём дальше», и следующий шаг это не
    обязательно следующий ЭТАП: сперва ищется следующий бот этого же этапа.

      never  - «Ответ не нужен»: информационное сообщение, ответа не ждём вовсе;
      any    - «Как только клиент ответит»: дальше ведёт ЛЮБОЙ ответ;
      listed - «Только на эти ответы»: перечисленное ведёт дальше, прочее останавливает;
      except - «На любой ответ, кроме этих»: перечисленное останавливает.

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
    except Exception:
        logger.exception("autopilot: сделка %s не прочиталась", lead_id)
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


# ── режим «Тест»: белый список контактов ────────────────────────────────────────

def test_gate_ok(lead: dict) -> bool:
    """В бою пропускаем всех, в «Тесте» - только контакты из белого списка.

    ⚠️ Воронка с именем «Тест» сама по себе не защищает никого: завести там сделку с живым
    человеком ничто не мешает, и одного раза хватит. Пустой список значит «никому».
    """
    if settings_client.get_mode() != "test":
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
    *, bot: dict | None = None, op_text: str = "",
) -> None:
    """Снять сделку с ведения и позвать человека.

    Останавливаемся ОХОТНО. Робот в этой воронке пишет живым людям и двигает деньги: цена
    лишней остановки - минута менеджера, цена лишнего хода - клиент, получивший не то.
    """
    lead_id = int(lead.get("id") or 0)
    status_id = int(lead.get("status_id") or 0)
    await asyncio.to_thread(store.finish, lead_id, status_id, store.PHASE_STOPPED, reason)
    log_run(lead, stage, bot=bot, action="route", outcome=outcome, reason=reason,
            alert_target="op" if op_text else "")
    if op_text:
        alert_op(
            f"{lead_link(lead_id, lead.get('name'))}: {op_text}",
            lead.get("responsible_user_id"),
        )


# ── ход по маршруту ─────────────────────────────────────────────────────────────

async def run_stage(lead: dict, stage: dict) -> None:
    """Этап маршрута: оплата, остаток, первый бот цепочки. Вызывается уже ПОСЛЕ `store.claim`."""
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

    # На ВХОДЕ в маршрут сперва смотрим, не оплачен ли заказ уже (правка Кати 09.09.2026).
    # Оплаченному заказу подтверждать нечего: спрашивать «всё верно?» у человека, который
    # уже заплатил, - это лишний шаг и лишний повод передумать. Ведём сразу в успех.
    #
    # ⚠️ Молчание МойСклада здесь НЕ останавливает, в отличие от развилки в конце маршрута:
    # там неизвестность грозит второй ссылкой на оплату, а тут - всего лишь лишним вопросом
    # клиенту. Не знаем - идём обычным путём.
    if at_entry:
        order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
        if await order_is_paid(order_uuid):
            log_run(lead, stage, action="payment_fork", outcome="advanced",
                    reason="заказ оплачен ещё до первого сообщения, веду в успех")
            await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано",
                          "заказ уже оплачен")
            return

    bot = pick_bot(lead, stage)
    if bot is None:
        # Этап без ботов - ПРОХОДНОЙ (правка Кати 09.09.2026): сделку в него перевели, чужая
        # автоматика этапа получила своё событие, нам здесь делать нечего - идём дальше.
        reason = ("этап проходной, ботов на нём нет" if not (stage.get("bots") or [])
                  else "ни один бот этапа не подошёл по условиям")
        log_run(lead, stage, action="route", outcome="skipped_no_bots", reason=reason)
        await advance(lead, stage, reason)
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

    if str(bot.get("launched_by") or "engine") == "engine":
        if not allow_action():
            await stop_here(lead, stage, "failed", "упёрся в потолок действий в час", bot=bot)
            return
        await asyncio.to_thread(store.mark_launch_attempted, lead_id, status_id, bot_id)
        if not await launch_bot(lead_id, bot_id):
            await stop_here(
                lead, stage, "failed", "amoCRM не принял запуск бота", bot=bot,
                op_text="не смог запустить бота, напишите клиенту сами",
            )
            return
    else:
        # Бот приезжает с грида Цифровой воронки - мы его не вызываем, иначе клиент получит
        # два одинаковых сообщения. Отметка времени всё равно нужна: от неё считается окно
        # ожидания доставки.
        await asyncio.to_thread(store.update, lead_id, status_id, bot_id=bot_id)
    await asyncio.to_thread(store.mark_launch_ok, lead_id, status_id, chat_id)

    if str(bot.get("stop_mode") or "any") == "never":
        # «Ответ не нужен» - информационное сообщение, а не разговор. Ни доставки, ни ответа
        # не ждём: у бота в успешной реализации отправка успешна по определению.
        log_run(lead, stage, bot=bot, action="launch_bot", outcome="advanced",
                reason="сообщение информационное, ответа не жду")
        await advance(lead, stage, "информационное сообщение отправлено", from_bot=bot)
        return

    log_run(lead, stage, bot=bot, action="launch_bot", outcome="waiting_delivery",
            reason="жду подтверждения доставки от Wazzup")


async def advance(lead: dict, stage: dict, reason: str, *, from_bot: dict | None = None) -> None:
    """Следующий шаг. Сперва следующий БОТ этого этапа, и только когда боты кончились - этап.

    Боты этапа идут цепочкой: первый спросил «всё верно?», клиент ответил «да» - слово берёт
    второй. Пока цепочка не кончилась, сделка с места не двигается.

    ⚠️ В успешную реализацию карточка маршрута не ведёт: туда пускает только развилка оплаты
    (и отдельные входы - оплаченный заказ на старте и этап «Оплата получена»). Иначе решение
    о деньгах зависело бы от того, в каком порядке человек перетащил карточки.
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
                reason="маршрут пройден, дальше сделку уводит перевод в офис")
        return
    nxt = next_stage(status_id)
    # На этап запроса оплаты, как и в успех, по порядку карточек не переходим - только
    # через развилку: в бою вход в «Оплату запрошену» выставляет клиенту счёт, и решение
    # об этом не должно зависеть от того, как расставлены карточки.
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
    if not allow_action():
        await stop_here(lead, stage, "failed", "упёрся в потолок действий в час")
        return
    result = await amo_service.patch_lead(
        lead_id, status_id=status_id, pipeline_id=int(lead.get("pipeline_id") or 0) or None,
    )
    if not result.get("ok"):
        await stop_here(
            lead, stage, "failed", f"amoCRM не принял перевод на «{status_name}»",
            op_text=f"не смог перевести сделку на этап «{status_name}», сделайте это руками",
        )
        return
    await asyncio.to_thread(store.finish, lead_id, was, store.PHASE_DONE, reason)
    log_run(lead, stage, action="route", outcome="advanced", reason=reason,
            moved_to_status_name=status_name)
    await handle_lead_change(lead_id)


# ── развилка оплаты ─────────────────────────────────────────────────────────────

def is_cod_strict(payment_method) -> bool:
    """Наложка в УЗКОМ смысле: строго «При получении».

    ⚠️ Соседний `waybill_config.is_cod_payment` шире - в нём есть «Эвотор» и «наличные», а это
    шоурум, где наложки нет вовсе. Возьми мы широкое определение, шоурумные заказы уехали бы в
    успешную реализацию мимо оплаты. Расхождение намеренное, оно описано в DESIGN.md.
    """
    return "при получении" in str(payment_method or "").lower()


async def order_is_paid(order_uuid) -> bool | None:
    """Оплачен ли заказ в МойСкладе. None - склад не ответил, и это НЕ «не оплачен».

    ⚠️ Признак оплаты - `payedSum > 0`, а не сравнение с суммой заказа. Сумма первые минуты
    пляшет: `woocommerce-sklad` раз в три минуты обнуляет цену доставки по правилу «предоплата
    - доставка за наш счёт», и заказ мигает 387 → 0 → 387.

    ⚠️ Молчание склада читать как «не оплачен» нельзя: оплаченному заказу тогда уйдёт ссылка
    на оплату второй раз. Поэтому три исхода, и неизвестность останавливает робота.
    """
    if not order_uuid:
        return None
    try:
        data = await ms_client.get(f"entity/customerorder/{order_uuid}")
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("autopilot: заказ %s не прочитался из МойСклада", order_uuid)
        return None
    if not data:
        return None
    try:
        return float(data.get("payedSum") or 0) > 0
    except (TypeError, ValueError):
        return None


def _payment_target() -> tuple[int, str]:
    """Куда развилка отправляет неоплаченный онлайн-заказ.

    Этап отдаёт панель - у неё карта соответствий воронок: в бою «Оплата запрошена»
    (дальше счёт выставляет автоматика), в тестовой воронке «Оплата» - перевод форсится
    и там (правка Кати 09.09.2026), чтобы прогон был виден движением сделки, а не
    только строкой в журнале. Панель поле ещё не отдала (старый кэш) - боевая константа.
    """
    target = settings_client.get_payment_status_id() or STATUS_PAYMENT_REQUESTED
    stage = settings_client.get_stage(target)
    name = str((stage or {}).get("status_name") or "").strip()
    return target, name or ("Оплата запрошена" if target == STATUS_PAYMENT_REQUESTED else "Оплата")


async def payment_fork(lead: dict, stage: dict | None, reason: str) -> None:
    """Конец маршрута: куда сделку вести по способу оплаты и факту оплаты."""
    lead_id = int(lead["id"])
    method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
    order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)

    # Сделка БЕЗ заказа МойСклада - это не «склад молчит», это «спрашивать не о чем».
    # В бою такой сделки быть не должно (робот заведён под заказы) - останавливаемся и зовём.
    # В тесте это норма: сделки заводятся руками, без заказа (правка Кати 09.09.2026) -
    # наложку ведём в успех, ей факт оплаты не нужен, остальное честно завершаем.
    if not order_uuid:
        if settings_client.get_mode() == "test":
            if is_cod_strict(method):
                log_run(lead, stage, action="payment_fork", outcome="advanced",
                        reason="оплата при получении, счёт не нужен")
                await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано",
                              "оплата при получении")
                return
            pay_status, pay_name = _payment_target()
            if int(lead.get("status_id") or 0) == pay_status:
                await asyncio.to_thread(
                    store.finish, lead_id, pay_status, store.PHASE_DONE,
                    "сделка на этапе запроса оплаты",
                )
                log_run(lead, stage, action="payment_fork", outcome="done",
                        reason="сделка на этапе запроса оплаты, дальше не веду: счёт в тесте не выставляем")
                return
            log_run(lead, stage, action="payment_fork", outcome="advanced",
                    reason="онлайн-оплата не поступила, перевожу на этап оплаты")
            await move_to(lead, stage, pay_status, pay_name,
                          "онлайн-оплата не поступила")
            return
        await stop_here(
            lead, stage, "failed", "в сделке нет заказа МойСклада",
            op_text="в сделке не заполнен заказ МойСклада, не могу проверить оплату, дальше не веду",
        )
        return

    paid = await order_is_paid(order_uuid)

    if paid is None and not is_cod_strict(method):
        await stop_here(
            lead, stage, "failed", "МойСклад не сказал, оплачен ли заказ",
            op_text="не смог узнать в МойСкладе, оплачен ли заказ, дальше не веду",
        )
        return

    if paid:
        log_run(lead, stage, action="payment_fork", outcome="advanced", reason="заказ оплачен")
        await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано", "заказ оплачен")
        return

    if not str(method or "").strip():
        await stop_here(
            lead, stage, "stop_no_payment_method", "способ оплаты в сделке не заполнен",
            op_text="способ оплаты не заполнен, не понимаю, чего ждать от клиента",
        )
        return

    if is_cod_strict(method):
        log_run(lead, stage, action="payment_fork", outcome="advanced",
                reason="оплата при получении, счёт не нужен")
        await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано",
                      "оплата при получении")
        return

    # Онлайн и не оплачен - переводим на этап запроса оплаты. В бою дальше счёт
    # выставляет уже работающая автоматика; в тесте счёта не будет, но перевод форсится
    # (правка Кати 09.09.2026) - прогон должен быть виден движением сделки.
    pay_status, pay_name = _payment_target()
    if int(lead.get("status_id") or 0) == pay_status:
        await asyncio.to_thread(
            store.finish, lead_id, pay_status, store.PHASE_DONE, reason,
        )
        log_run(lead, stage, action="payment_fork", outcome="done",
                reason="сделка уже на этапе запроса оплаты, дальше не веду")
        return
    log_run(lead, stage, action="payment_fork", outcome="advanced",
            reason="онлайн-оплата не поступила, перевожу на этап оплаты")
    await move_to(lead, stage, pay_status, pay_name,
                  "онлайн-оплата не поступила")


async def force_ur(lead: dict, stage: dict | None, note: str) -> None:
    """Тумблер «вести в успех, даже если шаблоны не ушли».

    Включён - недоставленный шаблон перестаёт быть стопом ТАМ, где деньги уже не под вопросом:
    заказ оплачен либо это наложка. Неоплаченный онлайн-заказ так не проводим никогда - иначе
    робот закроет успехом сделку, за которую никто не заплатил.
    """
    method = amo_service.get_custom_field_value(lead, FIELD_PAYMENT_METHOD)
    order_uuid = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
    paid = await order_is_paid(order_uuid)
    if paid or is_cod_strict(method):
        why = "заказ оплачен" if paid else "оплата при получении"
        log_run(lead, stage, action="payment_fork", outcome="advanced",
                reason=f"шаблоны не дошли ({note}), но {why}", alert_target="op")
        alert_op(
            f"{lead_link(int(lead['id']), lead.get('name'))}: заказ ушёл БЕЗ подтверждения "
            f"клиентом, {note}. Веду в успешную реализацию, потому что {why}.",
            lead.get("responsible_user_id"),
        )
        await move_to(lead, stage, STATUS_SUCCESS, "Успешно реализовано",
                      "шаблоны не дошли, но оплата не под вопросом")
        return
    await stop_here(
        lead, stage, "stop_not_delivered", f"шаблоны не дошли ({note}), заказ не оплачен",
        op_text=f"сообщение до клиента не дошло ({note}), заказ не оплачен, дальше не веду",
    )

# ── точка входа: изменение сделки ───────────────────────────────────────────────

def on_lead_change(lead_id) -> None:
    """Врезка в вебхук `/lead_change`. Синхронная и мгновенная: amoCRM ждёт быстрый ответ,
    а при задержке повторяет вебхук - и повтор стоил бы клиенту второго сообщения."""
    if not is_enabled():
        return
    try:
        lead_id = int(lead_id)
    except (TypeError, ValueError):
        return
    task = asyncio.create_task(handle_lead_change(lead_id))
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

    stage = settings_client.get_stage(status_id)
    if stage is None:
        return

    if not test_gate_ok(lead):
        logger.info("autopilot: сделка %s не в белом списке режима «Тест», не трогаю", lead_id)
        return

    # Гейт от повторного вебхука. `/lead_change` приходит на ЛЮБОЕ изменение сделки: правку
    # поля, тег, смену ответственного. Работу берёт первый, остальные получают отказ.
    if not await asyncio.to_thread(store.claim, lead_id, status_id, pipeline_id):
        return

    await run_stage(lead, stage)


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
    paid = await order_is_paid(order_uuid)
    if paid is False:
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
        lead.get("responsible_user_id"),
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
_MSG_OWNER_MAX = 5000


def _digits(value) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def on_wazzup(payload) -> None:
    """Врезка в вебхук `/wazzup/{secret}` - четвёртый потребитель рядом с SLA, контролем
    доставки и пересылкой в панель. Отвечать Wazzup надо быстро, поэтому работа уходит в фон.
    """
    if not is_enabled() or not isinstance(payload, dict):
        return
    task = asyncio.create_task(handle_wazzup(payload))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def handle_wazzup(payload: dict) -> None:
    for message in payload.get("messages") or []:
        if isinstance(message, dict):
            try:
                await _handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("autopilot: не разобрал сообщение Wazzup")
    for status in payload.get("statuses") or []:
        if isinstance(status, dict):
            try:
                await _handle_status(status)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("autopilot: не разобрал статус Wazzup")


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
    rows: list[dict] = []
    for chat in _chat_candidates(message):
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
        message_id = str(message.get("messageId") or "").strip()
        if message_id:
            if len(_msg_owner) >= _MSG_OWNER_MAX:
                _msg_owner.clear()
            _msg_owner[message_id] = (row["lead_id"], row["status_id"], chat_type)
        status = str(message.get("status") or "").lower()
        if status:
            await record_delivery(row, status, chat_type)
        return

    # Входящее. Ответ клиента - сам по себе доказательство доставки: человек не отвечает на
    # сообщение, которого не видел. Поэтому ждущую доставки сделку он закрывает вместе с
    # ожиданием, не дожидаясь отдельного статуса от Wazzup.
    row = _pick_row(rows, store.PHASE_REPLY) or _pick_row(rows, store.PHASE_DELIVERY)
    if row is None:
        return
    await on_client_answer(row, str(message.get("text") or ""), chat_type)


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
    started = row.get("launch_ok_at") or row.get("created_at")
    try:
        moment = datetime.datetime.fromisoformat(str(started))
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


async def on_client_answer(row: dict, text: str, chat_type: str = "") -> None:
    """Ответ клиента. Решение принимает режим остановки бота, настроенный на экране."""
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

    if answer_decision(bot, text) == "advance":
        log_run(lead, stage, bot=bot, action="reply", outcome="advanced",
                reason="ответ клиента подходит под условие успеха", client_answer=text)
        await advance(lead, stage, "клиент ответил так, как ждали", from_bot=bot)
        return

    listed = [normalize_answer(a) for a in (bot.get("stop_answers_norm")
                                            or bot.get("stop_answers") or [])]
    known = normalize_answer(text) in listed
    outcome = "stop_fix_requested" if known else "stop_free_text"
    reason = ("клиент выбрал ответ, на котором робот останавливается" if known
              else "клиент ответил не кнопкой, а своими словами")
    await asyncio.to_thread(
        store.update, row["lead_id"], row["status_id"], phase=store.PHASE_STOPPED, note=reason,
    )
    log_run(lead, stage, bot=bot, action="reply", outcome=outcome, reason=reason,
            client_answer=text, alert_target="op")
    answer = text.strip()
    alert_op(
        f"{lead_link(int(lead['id']), lead.get('name'))}: клиент ответил «{answer[:200]}». "
        "Дальше не веду, посмотрите переписку.",
        lead.get("responsible_user_id"),
    )


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
        log_run(lead, stage, bot=bot, action="delivery", outcome="stop_not_delivered",
                reason=note, delivery={"statuses": items})
        if _flag("force_ur_when_templates_failed"):
            await force_ur(lead, stage, note)
            continue
        await stop_here(
            lead, stage, "stop_not_delivered", note, bot=bot,
            op_text=f"сообщение до клиента не дошло: {note}. Дальше не веду.",
        )


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
            f"{lead_link(row['lead_id'])}: робот перезапустился в момент запуска бота и не "
            "знает, ушло сообщение или нет. Повторно не отправляю, посмотрите переписку."
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
    await check_delivery_windows()
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
