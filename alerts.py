"""Уведомления по настройкам панели: одна точка решения для всех сендеров.

Сендер по-прежнему собирает свой сегодняшний текст и знает свой сегодняшний чат. Перед
отправкой он спрашивает здесь:

    d = alerts.decide("academy_lead", legacy_text=text, values={...}, chat_id=..., thread_id=...)
    if d is None:            # выключено в панели
        return
    await telegram_bot.send_alert(d.text, **d.send_kwargs())

Что возвращается, зависит от `ALERT_SETTINGS_FROM_PANEL` (см. `waybill_config.py`):

- `off`    - ровно то, что сендер принёс: текст, чат, режим разметки. Панель не опрашиваем.
             Так работает прод до включения флага: ни одно уведомление не меняется.
- `shadow` - то же, что `off`, но в лог пишется, что велела бы панель: текст, чат,
             «выключено». Режим для проверки на бою без единого изменённого сообщения.
- `on`     - действуют настройки панели: выключатель, чат, текст по шаблону, получатели.

Три несущих решения:

- Текст собирается ЗДЕСЬ, а не в панели: иначе каждое уведомление зависело бы от живой
  панели - уехала на деплой, и пропущенные звонки не долетели.
- Любая нестыковка деградирует к сегодняшнему тексту, а не к молчанию: панель про событие
  не знает, документа ещё нет, в шаблоне неизвестная переменная - шлём как раньше.
  Единственное «не шлём» - выключатель события в панели и чат, который у интеграции не
  настроен (руководство без ROP_ALERT_CHAT_ID, эскалация счетов без своего чата) - ровно
  как сегодня.
- Номер чата панель не знает: она отдаёт ключ канала, соответствие «ключ → чат и топик»
  живёт только здесь, в `_destination`.

Маршрут по ответственному (Катя 29.09.2026). До этого адресата выбирало ТОЛЬКО событие: один
ключ канала на событие, второго измерения не было. Теперь сендер может попросить
`route_by_responsible=True`, и тогда адрес переопределяется по человеку: сделку ведёт менеджер
Академии - уведомление уходит в группу команды Академии, а не в топик розницы. Переопределение
ложится и на панельное решение, и на легаси, иначе маршрут молча отвалился бы при смене режима
флага. Список менеджеров Академии - в окружении, пустой список выключает маршрут целиком.

Получатели. Панель отдаёт режим: `responsible` - ник ответственного из карточки сотрудника
в панели (сендер передаёт `responsible_id=`), нет его там - сегодняшние теги сендера из
`values["теги"]` со всем их фолбэком «не нашёлся → вся смена»; `listed` - ники из панели
(ни у кого нет ника → теги сендера); `shift` - вся смена; `nobody` - без тегов (строка с
тегами выпадает из текста). Пустым тег не остаётся никогда, кроме прямого «никого».

Поля сделки `{{amo.<id>}}` подставляются, только если сендер передал `lead=` - словарь
сделки из amo с `custom_fields_values`. Не передал - поле пустое, строка с ним выпадает.
"""
from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
import pathlib
import re
from dataclasses import dataclass, replace
from typing import Any

import httpx

import alert_settings_client as settings_client
import alert_templates
import tg_recipients
from waybill_config import (
    ACADEMY_PANEL_NOTIFY_ENABLED,
    OZON_STALE_ESCALATE_CHAT_ID,
    TEAM_PANEL_BASE_URL,
    TEAM_PANEL_INGEST_TOKEN,
)

logger = logging.getLogger("uvicorn")

# Хост amoCRM. Та же строка лежит в api.BASE_URL и в четырёх AMO_LEAD_URL сендеров -
# свести в одно место отдельной правкой; api сюда не импортируем, он тянет всё приложение.
AMO_BASE_URL = "https://new5a2e8ea7b16b4.amocrm.ru"


@dataclass(frozen=True)
class Decision:
    text: str
    chat_id: int | str | None       # None - адресат по умолчанию у send_alert (технический чат)
    thread_id: int | None
    parse_mode: str | None
    source: str                     # legacy | panel

    def send_kwargs(self) -> dict[str, Any]:
        return {"chat_id": self.chat_id, "message_thread_id": self.thread_id, "parse_mode": self.parse_mode}


def lead_link(lead_id) -> str:
    """Значение для `{{ссылка_на_сделку}}` - готовая ссылка «Открыть сделку». Пусто, если
    сделки нет: тогда строка со ссылкой выпадет из текста."""
    if not lead_id:
        return ""
    return f'<a href="{AMO_BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>'


def with_line_after_head(d: "Decision", line: str) -> "Decision":
    """Вставить строку кода сразу после заголовка текста из панели (строка про самовывоз в
    «Клиент ждёт ответа»: её добавляет код, потому что она объясняет порог, а не событие)."""
    head, sep, rest = d.text.partition("\n")
    return replace(d, text=head + "\n" + line + (sep + rest if sep else ""))


def _destination(channel_key: str | None) -> tuple[int | str | None, int | None] | None:
    """Ключ канала из панели → (чат, топик). None - канал у интеграции не настроен, молчим,
    как молчит сегодняшний код без этого чата."""
    if channel_key == "op_notify":
        return tg_recipients.NOTIFY_CHAT_ID, tg_recipients.NOTIFY_THREAD_ID
    if channel_key == "op_showroom":
        if tg_recipients.SHOWROOM_ALERT_THREAD_ID is None:
            return None
        return tg_recipients.NOTIFY_CHAT_ID, tg_recipients.SHOWROOM_ALERT_THREAD_ID
    if channel_key == "op_academy":
        # Ветка «Уведомления академии» в той же супергруппе ОП. Номер не задан - НЕ молчим,
        # а остаёмся в общем топике УВЕДОМЛЕНИЯ: до 29.09.2026 лид Академии жил именно там,
        # и потерять уведомление из-за незаполненной переменной хуже, чем показать его
        # соседям. У op_showroom наоборот, и это не рассогласование: там своя ветка - смысл
        # события (визит в шоурум), а здесь - только место в чате.
        return tg_recipients.NOTIFY_CHAT_ID, (
            tg_recipients.ACADEMY_NOTIFY_THREAD or tg_recipients.NOTIFY_THREAD_ID
        )
    if channel_key == "academy_team":
        return (tg_recipients.ACADEMY_TEAM_CHAT, None) if tg_recipients.ACADEMY_TEAM_CHAT else None
    if channel_key == "rop":
        return (tg_recipients.ROP_CHAT_ID, None) if tg_recipients.ROP_CHAT_ID else None
    if channel_key == "ozon_escalation":
        return (OZON_STALE_ESCALATE_CHAT_ID, None) if OZON_STALE_ESCALATE_CHAT_ID else None
    if channel_key == "tech":
        return None, None
    return None


def channel_for_responsible(responsible_id) -> str | None:
    """Канал по ОТВЕТСТВЕННОМУ: менеджер Академии читает свою группу (Катя 29.09.2026).

    None - маршрут не меняем, адресат остаётся тем, что решило событие. Так было всегда до
    29.09: один ключ канала на событие, и другого измерения не существовало. Здесь оно
    появляется, потому что одно и то же событие («клиент ждёт ответа») должно приходить в
    разные чаты в зависимости от того, чей это клиент.
    """
    return "academy_team" if tg_recipients.is_academy_manager(responsible_id) else None


def _routed(d: Decision, event_key: str, responsible_id) -> Decision:
    """Переопределить адрес решения маршрутом по ответственному.

    Применяется и к панельному решению, и к легаси, и это несущая деталь. Только к
    легаси - маршрут не сработал бы вовсе: прод живёт в режиме `on` с 13.09.2026. Только
    к панельному - маршрут молча отвалился бы, если флаг когда-нибудь вернут в `off`.
    """
    key = channel_for_responsible(responsible_id)
    if key is None:
        return d
    dest = _destination(key)
    if dest is None:
        # Группа Академии ещё не настроена - оставляем сегодняшний адрес, а не молчим:
        # это ровно состояние «код выкачен, chat_id не прописан».
        logger.info("alerts: %s - чат команды Академии не настроен, адрес не меняю", event_key)
        return d
    if (d.chat_id, d.thread_id) == (dest[0], dest[1]):
        return d
    logger.info(
        "alerts: %s - ответственный из команды Академии, адрес chat=%s thread=%s вместо chat=%s thread=%s",
        event_key, dest[0], dest[1], d.chat_id, d.thread_id,
    )
    return replace(d, chat_id=dest[0], thread_id=dest[1])


def _tags(event_key: str, cfg: dict[str, Any], values: dict[str, Any], responsible_id) -> str:
    """Строка тегов по режиму получателей из панели.

    Тег не бывает пустым иначе как по прямому выбору «никого»: везде, где панель не может
    назвать человека, подставляются сегодняшние теги сендера (`values["теги"]`) - а в них
    уже зашит фолбэк кода «ответственный не нашёлся → вся смена».
    """
    legacy = str(values.get("теги") or "")
    mode = cfg.get("recipients_mode") or "responsible"
    if mode == "listed":
        handles = [str(r.get("handle")) for r in (cfg.get("recipients") or []) if r.get("handle")]
        if not handles:
            logger.warning("alerts: %s - в панели «названным», но ни у кого нет ника; тегаем как код", event_key)
            return legacy
        return " ".join(handles)
    if mode == "shift":
        return tg_recipients.MANAGERS_ON_SHIFT
    if mode == "nobody":
        return ""
    # «Ответственному за сделку»: ник берём из карточки сотрудника в панели (Катя правит его
    # там), а карта в коде остаётся запасным путём - для тех, кого в панели нет.
    person = settings_client.person_by_amo_id(responsible_id) if responsible_id is not None else None
    if person:
        return str(person["handle"])
    return legacy


def _from_panel(
    event_key: str, cfg: dict[str, Any], values: dict[str, Any], lead: dict | None,
    keep_text: bool, legacy: Decision, responsible_id=None,
) -> Decision | None:
    if cfg.get("enabled") is False:
        return None
    dest = _destination(cfg.get("channel"))
    if dest is None:
        logger.info("alerts: %s - чат «%s» у интеграции не настроен, не шлём", event_key, cfg.get("channel"))
        return None
    if keep_text:
        # Текст этого события собирает код (списки заказов, пары дублей) - панель управляет
        # только выключателем и чатом.
        return Decision(legacy.text, dest[0], dest[1], legacy.parse_mode, "panel")
    vals = dict(values)
    vals["теги"] = _tags(event_key, cfg, values, responsible_id)
    try:
        text = alert_templates.render(str(cfg.get("template") or ""), vals, lead=lead)
    except alert_templates.UnknownVariable as e:
        logger.warning(
            "alerts: %s - в шаблоне панели переменная %s, которой сендер не даёт; шлём старый текст",
            event_key, e,
        )
        return Decision(legacy.text, dest[0], dest[1], legacy.parse_mode, "panel")
    if not text.strip():
        logger.warning("alerts: %s - шаблон панели дал пустой текст; шлём старый", event_key)
        return Decision(legacy.text, dest[0], dest[1], legacy.parse_mode, "panel")
    return Decision(text, dest[0], dest[1], "HTML", "panel")


def decide(
    event_key: str, *, legacy_text: str, values: dict[str, Any],
    chat_id: int | str | None = None, thread_id: int | None = None, parse_mode: str | None = None,
    lead: dict | None = None, keep_text: bool = False, responsible_id=None,
    route_by_responsible: bool = False,
) -> Decision | None:
    """Что и куда слать. None - не слать (выключено в панели или её чат не настроен).

    `legacy_text`, `chat_id`, `thread_id`, `parse_mode` - то, что сендер послал бы сегодня.
    `values` - значения переменных шаблона; `values["теги"]` - сегодняшние теги сендера.
    `responsible_id` - ответственный по сделке в amoCRM: в режиме «ответственному» его ник
    берётся из карточки сотрудника в панели, нет там - остаются теги сендера.
    `keep_text=True` - текст остаётся кодовым, панель решает только выключатель и чат.
    `route_by_responsible=True` - адрес выбирается по ответственному, а не только по событию:
    его сделку ведёт менеджер Академии - уведомление уходит в группу команды Академии.
    Список пуст или чат не настроен - адрес не меняется (см. `_routed`).
    """
    legacy = Decision(legacy_text, chat_id, thread_id, parse_mode, "legacy")
    if route_by_responsible:
        legacy = _routed(legacy, event_key, responsible_id)
    mode = settings_client.mode()
    if mode == "off":
        return legacy
    cfg = settings_client.get_event(event_key)
    if cfg is None:
        return legacy
    panel = _from_panel(event_key, cfg, values, lead, keep_text, legacy, responsible_id)
    if panel is not None and route_by_responsible:
        panel = _routed(panel, event_key, responsible_id)
    if mode == "shadow":
        if panel is None:
            logger.info("alerts[shadow] %s: панель велела бы НЕ слать; шлём как раньше", event_key)
        else:
            logger.info(
                "alerts[shadow] %s: панель велела бы chat=%s thread=%s parse=%s, текст:\n%s\n"
                "--- шлём как раньше: chat=%s thread=%s",
                event_key, panel.chat_id, panel.thread_id, panel.parse_mode, panel.text,
                legacy.chat_id, legacy.thread_id,
            )
        _decision_record("shadow", event_key, panel, legacy)
        return legacy
    _decision_record("on", event_key, panel, legacy)
    return panel


# Решения (и тени, и боевые) дублируются в файл на постоянном томе: `docker logs` живёт ровно
# столько, сколько контейнер, а его пересоздают по несколько раз в день соседние выкатки -
# 13.09.2026 так пропали пропущенные звонки и счета, которые тень видела. Файл - JSON-строки:
# `mode` (shadow | on), `event`, `panel` (что велела панель; null = не слать), `legacy` (что
# послал бы старый код). В `on` ушло `panel`, в `shadow` - `legacy`.
SHADOW_LOG_PATH = pathlib.Path(os.getenv("ALERT_SHADOW_LOG", "var/alert_shadow.jsonl"))
# Ответы Telegram на каждую отправку (пишет telegram_bot через record_sent): message_id, чат,
# топик, время, начало текста. Сверка «решение → факт приёма Телеграмом», не строка в логе.
SENT_LOG_PATH = pathlib.Path(os.getenv("ALERT_SENT_LOG", "var/alert_sent.jsonl"))
_LOG_CAP = 5 * 1024 * 1024


def _append_jsonl(path: pathlib.Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > _LOG_CAP:
        os.replace(path, path.with_suffix(".1.jsonl"))
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _decision_record(mode: str, event_key: str, panel: Decision | None, legacy: Decision) -> None:
    try:
        _append_jsonl(SHADOW_LOG_PATH, {
            "at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "mode": mode,
            "event": event_key,
            "panel": None if panel is None else {
                "chat_id": panel.chat_id, "thread_id": panel.thread_id,
                "parse_mode": panel.parse_mode, "text": panel.text,
            },
            "legacy": {"chat_id": legacy.chat_id, "thread_id": legacy.thread_id, "text": legacy.text},
        })
    except Exception:
        logger.exception("alerts: не записался файл решений %s", SHADOW_LOG_PATH)


# ── лента панели: второй канал рядом с Телеграмом (Катя 29.09.2026) ────────────────
#
# Почему лента, а не только чат: Телеграм у нас глушился на сутки одним сетевым сбоем
# (28-29.08.2026), а панель - рабочий инструмент, в который человек и так смотрит.
# Отправка фоновая и никогда не роняет сендер: не доехало уведомление - сообщение в чат
# всё равно ушло.
#
# ⚠️ Ссылку из текста вынимаем в поле `url`: лента рендерит ПЛОСКИЙ текст, и тег `<a>`
# приехал бы разметкой. Тот же приём уже стоит в `autopilot.py`.
_LINK_RE = re.compile(r'<a href="([^"]+)">([^<]*)</a>')

_panel_tasks: set = set()


def strip_link(text: str) -> tuple[str, str | None]:
    """Текст без html-ссылки плюс сам адрес. Ссылки нет - адрес None."""
    url = None
    m = _LINK_RE.search(text or "")
    if m:
        url = m.group(1)
    plain = _LINK_RE.sub(lambda mm: mm.group(2) or "", text or "")
    # Пустая строка на месте вынутой ссылки читается как обрыв - убираем.
    plain = "\n".join(line for line in plain.splitlines() if line.strip())
    return plain, url


def panel_notify_bg(
    *, kind: str, title: str, body: str = "", url: str | None = None,
    level: str = "info", dedupe_key: str | None = None,
    amo_user_id=None, fallback_amo_user_id=None,
) -> None:
    """Положить уведомление в ленту панели. Фоном, ошибки только в лог.

    Адресуем ЛИЧНО: `user_amo_id` - ответственный по сделке. Это и есть «пинг
    ответственного» из ТЗ Кати, а не рассылка по праву доступа.

    ⚠️ Приём панели отвечает 404, если такой amo id ни к кому не привязан - а новых
    менеджеров Академии в панели ещё нет. Поэтому на 404 пробуем `fallback_amo_user_id`
    (Гладков). Не вышло и с ним - пишем строку в лог и молчим: копия в ленте дешевле
    сообщения в чате, которое уже ушло.
    """
    if not ACADEMY_PANEL_NOTIFY_ENABLED:
        return
    if not TEAM_PANEL_BASE_URL or not TEAM_PANEL_INGEST_TOKEN:
        logger.info("alerts: панель не настроена, уведомление «%s» в ленту не кладу", title[:60])
        return
    task = asyncio.create_task(_panel_notify(
        kind=kind, title=title, body=body, url=url, level=level,
        dedupe_key=dedupe_key, amo_user_id=amo_user_id,
        fallback_amo_user_id=fallback_amo_user_id,
    ))
    _panel_tasks.add(task)
    task.add_done_callback(_panel_tasks.discard)


async def _panel_post(payload: dict) -> int:
    """Один POST. Возвращает http-код, 0 - до панели не дошли вовсе."""
    url = f"{TEAM_PANEL_BASE_URL.rstrip('/')}/api/ingest/notification"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                url, json=payload, headers={"X-Ingest-Token": TEAM_PANEL_INGEST_TOKEN},
            )
        return resp.status_code
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("alerts: панель не ответила на уведомление «%s»", str(payload.get("title"))[:60])
        return 0


async def _panel_notify(*, kind, title, body, url, level, dedupe_key,
                        amo_user_id, fallback_amo_user_id) -> None:
    base = {"kind": kind, "title": title[:300], "body": (body or "")[:2000], "level": level}
    if url:
        base["url"] = url
    if dedupe_key:
        base["dedupe_key"] = dedupe_key[:160]

    tried = []
    for candidate in (amo_user_id, fallback_amo_user_id):
        try:
            candidate = int(candidate) if candidate is not None else None
        except (TypeError, ValueError):
            candidate = None
        if candidate is None or candidate in tried:
            continue
        tried.append(candidate)
        code = await _panel_post({**base, "user_amo_id": candidate})
        if code < 400:
            logger.info("alerts: уведомление «%s» в ленте панели, адресат amo %s", title[:60], candidate)
            return
        if code != 404:
            logger.warning("alerts: панель не приняла уведомление «%s», HTTP %s", title[:60], code)
            return
        logger.info(
            "alerts: amo %s не привязан к сотруднику панели, пробую следующего адресата", candidate,
        )
    logger.warning(
        "alerts: уведомление «%s» в ленту НЕ легло - ни один адресат не привязан (пробовали %s)",
        title[:60], tried or "никого",
    )


def record_sent(*, chat_id, thread_id, message_id, sent_at, text: str) -> None:
    """Зовёт telegram_bot после успешного send_message. Никогда не бросает."""
    try:
        _append_jsonl(SENT_LOG_PATH, {
            "at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
            "chat_id": chat_id, "thread_id": thread_id, "message_id": message_id,
            "tg_date": sent_at.isoformat(timespec="seconds") if hasattr(sent_at, "isoformat") else sent_at,
            "text_head": (text or "")[:160],
        })
    except Exception:
        logger.exception("alerts: не записался файл отправок %s", SENT_LOG_PATH)
