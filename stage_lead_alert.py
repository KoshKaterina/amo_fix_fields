"""Новый лид в ОПТ и в листе ожидания → сообщение в свою ветку с тегом Артёма (Катя 22.10.2026).

Три события одним механизмом, потому что устроены они одинаково: сделка встала на этап
воронки, человек должен увидеть её в тот же момент, а не найти вечером в списке.

    opt_lead                 воронка ОПТ, этапы «Первичный контакт» и «Найден контакт»
                             → ветка «Уведомления опт»
    waitlist_club_lead       воронка «Лист ожидания», этап «Клуб»
    waitlist_vladimir_lead   воронка «Лист ожидания», этап «Консультация Владимир»
                             → ветка «Уведомления лист ожидания»

Этап «Товар» (бывший общий «Лист ожидания», сюда идёт и перенос ЗНР) уведомления НЕ даёт:
по ТЗ Кати нужны только «Клуб» и «Консультация Владимир».

────────────────────────────── что считается новым лидом ──────────────────────────────
ЛЮБОЕ попадание сделки на этап правила: и созданная сразу там (форма Mini App, сайт), и
переведённая сюда с другого этапа или из другой воронки (перенос ЗНР → ОПТ по причине «Опт»).
Для Артёма это одно и то же - лид, которого раньше у него не было, - и различать их нечем.
Тот же принцип у academy_lead_alert, и по той же причине гейта по возрасту сделки нет.

Два входных этапа у ОПТ, потому что перенос ЗНР кладёт сделку на «Первичный контакт» (контакт
новый) или на «Найден контакт» (контакт уже встречался). Формы Mini App и сайта идут на
«Первичный контакт». Сузить до одного этапа - правкой RULES, без перекройки модуля.

⚠️ Дедуп ключуется по паре «событие + сделка», с окном. Переезд «Найден контакт» → «Первичный
контакт» одной сделки второго сообщения не даёт, а перенос «Клуб» → «Консультация Владимир»
даёт, потому что это разные события с разными заголовками.

Защиты те же, что у academy_lead_alert: сделка перечитывается из amo после паузы (за неё лид
могли увести - тогда молчим и снимаем отметку дедупа); часовой лимит на ГРУППУ (Опт, лист
ожидания) - перебор значит массовый перенос, одно предупреждение и тишина до конца часа.

⚠️ Номера веток приходят из окружения (OPT_NOTIFY_THREAD_ID, WAITLIST_NOTIFY_THREAD_ID). Пока
номера нет - модуль молчит, а не падает в общий топик УВЕДОМЛЕНИЯ: розницу чужими лидами не
засоряем. Это видно одной строкой в логе при первом же вебхуке по правилу.

Дедуп переживает рестарт: список лежит в /app/var (постоянный том контейнера).
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass

import alerts
import amo_service
import telegram_bot
from api import BASE_URL
from shared.files import read_json, write_json_atomic
from tg_recipients import (
    B2B_ALERT_TAG,
    NOTIFY_CHAT_ID,
    OPT_NOTIFY_THREAD,
    WAITLIST_NOTIFY_THREAD,
)
from waybill_config import (
    FIELD_PHONE,
    PIPELINE_OPT,
    PIPELINE_WAITLIST,
    STAGE_LEAD_ALERT_DEDUP_H,
    STAGE_LEAD_ALERT_DELAY_S,
    STAGE_LEAD_ALERT_ENABLED,
    STAGE_LEAD_ALERT_HOUR_LIMIT,
    STATUS_OPT_CONTACT_FOUND,
    STATUS_OPT_PRIMARY_CONTACT,
    STATUS_WAITLIST_CLUB,
    STATUS_WAITLIST_VLADIMIR,
)

logger = logging.getLogger("uvicorn")

GROUP_OPT = "opt"
GROUP_WAITLIST = "waitlist"


@dataclass(frozen=True)
class Rule:
    event: str                    # ключ события в каталоге панели
    group: str                    # группа общего часового лимита и «потока выше лимита»
    pipeline_id: int
    stages: dict                  # этап → название для сообщения (правило Кати 03.08.2026: без ID)
    head: str                     # заголовок сообщения
    show_stage: bool = False      # строка с этапом нужна, когда этапов у правила несколько


RULES: tuple[Rule, ...] = (
    Rule(
        event="opt_lead", group=GROUP_OPT, pipeline_id=PIPELINE_OPT,
        stages={
            STATUS_OPT_PRIMARY_CONTACT: "Первичный контакт",
            STATUS_OPT_CONTACT_FOUND: "Найден контакт",
        },
        head="🤝 Новый лид в ОПТ", show_stage=True,
    ),
    Rule(
        event="waitlist_club_lead", group=GROUP_WAITLIST, pipeline_id=PIPELINE_WAITLIST,
        stages={STATUS_WAITLIST_CLUB: "Клуб"},
        head="⏳ Новый лид в листе ожидания: Клуб",
    ),
    Rule(
        event="waitlist_vladimir_lead", group=GROUP_WAITLIST, pipeline_id=PIPELINE_WAITLIST,
        stages={STATUS_WAITLIST_VLADIMIR: "Консультация Владимир"},
        head="⏳ Новый лид в листе ожидания: Консультация Владимир",
    ),
)

# Группа → событие «поток выше лимита» и заголовок для запасного текста.
_BURST = {
    GROUP_OPT: ("opt_lead_burst", "🤝 Лидов в ОПТ"),
    GROUP_WAITLIST: ("waitlist_lead_burst", "⏳ Лидов в лист ожидания"),
}

# Вебхук amo отдаёт значения формой, то есть строками - ищем по строковым ключам.
_RULE_BY_STAGE: dict[tuple[str, str], Rule] = {
    (str(rule.pipeline_id), str(status)): rule
    for rule in RULES for status in rule.stages
}

_bg_tasks: set = set()
# «событие:сделка» → время последней отправки. Со временем, а не set: дедуп с окном.
_seen: dict[str, float] = {}
_SEEN_CAP = 5000
_SEEN_PATH = os.getenv("STAGE_LEAD_ALERT_SEEN_PATH", "/app/var/stage_lead_alert_seen.json")
_seen_loaded = False

# Часовое окно по группам: времена отправок за последний час и отметка «про перебор сказали».
_sent_times: dict[str, list[float]] = {GROUP_OPT: [], GROUP_WAITLIST: []}
_burst_notified: dict[str, bool] = {GROUP_OPT: False, GROUP_WAITLIST: False}
_warned_no_thread: set[str] = set()


def _thread(rule: Rule) -> int | None:
    """Ветка правила, нет номера - None (молчим). Читается функцией, а не константой модуля:
    так тест подменяет её без перезагрузки конфига."""
    return OPT_NOTIFY_THREAD if rule.group == GROUP_OPT else WAITLIST_NOTIFY_THREAD


def _key(rule: Rule, lead_id) -> str:
    return f"{rule.event}:{lead_id}"


def _load_seen() -> None:
    """Поднять дедуп с диска. Файла нет / битый - начинаем с пустого."""
    global _seen_loaded
    if _seen_loaded:
        return
    _seen_loaded = True
    data = read_json(_SEEN_PATH, {})
    if isinstance(data, dict):
        for key, ts in data.items():
            try:
                _seen[str(key)] = float(ts)
            except (TypeError, ValueError):
                continue


def _save_seen() -> None:
    """Сбросить дедуп на диск. Не удался - не беда, в памяти он остаётся."""
    try:
        write_json_atomic(_SEEN_PATH, _seen)
    except Exception:
        logger.exception("Стадия-алерт: не записался %s", _SEEN_PATH)


def _forget_stale(now: float) -> None:
    window = STAGE_LEAD_ALERT_DEDUP_H * 3600
    for key in [k for k, ts in _seen.items() if now - ts > window]:
        _seen.pop(key, None)
    if len(_seen) > _SEEN_CAP:
        for key in sorted(_seen, key=lambda k: _seen[k])[: len(_seen) - _SEEN_CAP]:
            _seen.pop(key, None)


def _is_new(rule: Rule, lead_id) -> bool:
    """True - про эту пару «событие + сделка» в окне ещё не писали (писать).
    False - эхо вебхука, повтор после рестарта или сделку подёргали по этапам."""
    _load_seen()
    now = time.time()
    _forget_stale(now)
    key = _key(rule, lead_id)
    if key in _seen:
        return False
    _seen[key] = now
    _save_seen()
    return True


def _unsee(rule: Rule, lead_id) -> None:
    """Снять отметку: отправки не было (сделку увели, amo молчит, лимит, чат не принял).
    Без этого следующий честный заход на этап промолчал бы всё окно."""
    if _seen.pop(_key(rule, lead_id), None) is not None:
        _save_seen()


def _budget_ok(group: str) -> bool:
    now = time.time()
    times = _sent_times[group]
    times[:] = [ts for ts in times if now - ts < 3600]
    if len(times) < STAGE_LEAD_ALERT_HOUR_LIMIT:
        if not times:
            _burst_notified[group] = False
        return True
    return False


def rule_for(pipeline_id, status_id) -> Rule | None:
    """Правило, которому соответствует пара «воронка, этап». Значения - хоть числа, хоть строки."""
    if pipeline_id is None or status_id is None:
        return None
    return _RULE_BY_STAGE.get((str(pipeline_id), str(status_id)))


def is_alert_stage(rule: Rule, lead: dict) -> bool:
    """Сделка ПРЯМО СЕЙЧАС стоит на этапе этого правила? Проверяется по сделке, а не по
    вебхуку: между вебхуком и отправкой проходит пауза, и за неё лид могли увести."""
    if not lead:
        return False
    if str(lead.get("pipeline_id")) != str(rule.pipeline_id):
        return False
    return str(lead.get("status_id")) in {str(s) for s in rule.stages}


def _stage_name(rule: Rule, lead: dict) -> str:
    names = {str(k): v for k, v in rule.stages.items()}
    return names.get(str((lead or {}).get("status_id")), "")


def notify_bg(lead_id, pipeline_id, status_id) -> None:
    """Разбор вебхука `/lead_change`: сделка встала на этап одного из правил.

    Зовётся на КАЖДОМ изменении любой сделки, поэтому здесь только сравнения и словарь.
    Сеть и чтение amo - в фоне, в `_apply`."""
    if not STAGE_LEAD_ALERT_ENABLED or lead_id is None:
        return
    rule = rule_for(pipeline_id, status_id)
    if rule is None:
        return
    if _thread(rule) is None:
        if rule.group not in _warned_no_thread:
            _warned_no_thread.add(rule.group)
            logger.warning(
                "Стадия-алерт: для группы «%s» не задан номер ветки - уведомления не шлём "
                "(OPT_NOTIFY_THREAD_ID / WAITLIST_NOTIFY_THREAD_ID)", rule.group,
            )
        return
    if not _is_new(rule, lead_id):
        return
    task = asyncio.create_task(_apply(rule, lead_id))
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _apply(rule: Rule, lead_id) -> None:
    try:
        # Ждём, пока источник лида дозапишет поля и привяжет контакт.
        if STAGE_LEAD_ALERT_DELAY_S:
            await asyncio.sleep(STAGE_LEAD_ALERT_DELAY_S)

        try:
            lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        except Exception:
            logger.exception("Стадия-алерт: не прочиталась сделка %s", lead_id)
            _unsee(rule, lead_id)
            return

        if not is_alert_stage(rule, lead):
            logger.info(
                "Стадия-алерт %s: сделка %s уже ушла с этапа правила - молчим", rule.event, lead_id,
            )
            _unsee(rule, lead_id)
            return

        if not _budget_ok(rule.group):
            _unsee(rule, lead_id)
            await _report_burst(rule.group)
            return

        client, phone = await _client_card(lead)
        text = _build_message(rule, lead_id, lead, client, phone)
        name = ((lead or {}).get("name") or "").strip()
        d = alerts.decide(
            rule.event, legacy_text=text, parse_mode="HTML",
            chat_id=NOTIFY_CHAT_ID, thread_id=_thread(rule), lead=lead,
            values={
                "теги": B2B_ALERT_TAG,
                "телефон": phone or "",
                "клиент": client or "",
                "сделка": name if name and name != client else "",
                "этап": _stage_name(rule, lead),
                "ссылка_на_сделку": alerts.lead_link(lead_id),
            },
        )
        if d is None:
            logger.info("Стадия-алерт %s: событие выключено в панели (сделка %s)", rule.event, lead_id)
            return
        ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
        if ok:
            _sent_times[rule.group].append(time.time())
        else:
            # Не дошло - пусть следующий вебхук по этой сделке попробует снова.
            _unsee(rule, lead_id)
        logger.info(
            "Стадия-алерт %s: %s (сделка %s, клиент %s)",
            rule.event, "отправлен" if ok else "НЕ отправлен", lead_id, client or "-",
        )
    except Exception:
        logger.exception("Стадия-алерт %s: ошибка на сделке %s", rule.event, lead_id)
        _unsee(rule, lead_id)


async def _report_burst(group: str) -> None:
    """Перебор часового лимита: одно предупреждение за окно, дальше тишина.
    Молчать совсем нельзя - иначе пропажа уведомлений выглядит как поломка бота."""
    if _burst_notified[group]:
        return
    _burst_notified[group] = True
    event, head = _BURST[group]
    logger.warning(
        "Стадия-алерт: часовой лимит %s по группе «%s» исчерпан - уведомления приглушены",
        STAGE_LEAD_ALERT_HOUR_LIMIT, group,
    )
    rule = next(r for r in RULES if r.group == group)
    d = alerts.decide(
        event,
        legacy_text=(
            f"{head} за час пришло больше {STAGE_LEAD_ALERT_HOUR_LIMIT}, дальше уведомления "
            "приглушены до конца часа. Похоже на массовый перенос сделок, стоит заглянуть "
            "в воронку."
        ),
        chat_id=NOTIFY_CHAT_ID, thread_id=_thread(rule),
        values={"лимит": STAGE_LEAD_ALERT_HOUR_LIMIT},
    )
    if d is None:
        return
    await telegram_bot.send_alert(d.text, **d.send_kwargs())


async def _client_card(lead: dict) -> tuple[str | None, str | None]:
    """Имя и телефон клиента. ⚠️ Во вложенных контактах сделки amo отдаёт только id и
    is_main - ни имени, ни телефона там нет, контакт надо дочитывать отдельно."""
    contacts = ((lead.get("_embedded") or {}).get("contacts")) or []
    if not contacts:
        return None, None
    main = next((c for c in contacts if c.get("is_main")), contacts[0])
    try:
        contact = await amo_service.get_contact_by_id(main.get("id"))
    except Exception:
        logger.exception("Стадия-алерт: контакт %s не прочитался", main.get("id"))
        return None, None
    if not contact:
        return None, None
    name = (contact.get("name") or "").strip() or None
    phone = amo_service.get_custom_field_value(contact, FIELD_PHONE)
    phone = str(phone).strip() if phone else None
    return name, phone


def _esc(s) -> str:
    return str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_message(rule: Rule, lead_id, lead: dict, client, phone) -> str:
    """Без ID в тексте (правило Кати 03.08.2026) и без точек посередине (26.08.2026)."""
    lines = [rule.head, B2B_ALERT_TAG]
    if phone:
        lines.append(f"📞 {_esc(phone)}")
    if client:
        lines.append(f"👤 {_esc(client)}")
    name = ((lead or {}).get("name") or "").strip()
    if name and name != client:
        lines.append(f"📝 {_esc(name)}")
    if rule.show_stage:
        stage = _stage_name(rule, lead)
        if stage:
            lines.append(f"🪜 {_esc(stage)}")
    lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)
