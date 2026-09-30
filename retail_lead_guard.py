"""Клиент написал в канал розницы, а сделки в рознице нет → алерт в чат ОП.

Повод (Катя 30.09.2026). Настройка amoCRM «Беседы» стоит в положении «создавать беседу в
новой сделке, если у клиента нет активных покупателя или сделки». Значит ЛЮБАЯ открытая
сделка человека — в любой воронке — запрещает amo создать новую сделку под входящее
сообщение, и обращение падает в ту сделку, где переписка уже жила. Живой случай: у клиента
открыта сделка Академии, он пишет в телеграм про покупку кошелька — сделки в рознице не
появляется, отдел продаж обращения не видит вовсе, менеджер через четыре часа заводит
сделку руками.

Переселить беседу в другую сделку нечем: метода нет ни в REST, ни в виджетах. Поэтому
лечение одно — заметить факт и позвать человека.

────────────────────────────── что считается поводом ──────────────────────────────
ВХОДЯЩЕЕ (isEcho=false) в канал из RETAIL_GUARD_CHANNELS, после которого у клиента
нет ни одной открытой сделки в «своих» воронках (RETAIL_GUARD_OWN_PIPELINES — розница и
Офис), но есть открытая в чужой. Решение Кати 30.09.2026: Офис считаем своим (человек
пишет по заказу, который уехал на отгрузку, новая сделка ему не нужна), Лист ожидания —
чужим (сделка живая, а покупки в ней нет).

Открытых сделок нет вовсе — молчим: это как раз случай, когда amo сам создаст новую.

────────────────────────────── почему пауза обязательна ──────────────────────────────
Проверяем не сразу, а через RETAIL_GUARD_DELAY_S. Сделка и беседа появляются в amo не
мгновенно: 08.09.2026 беседа родилась через семь минут после первого сообщения. Спросим
сразу — получим алерт на случай, который amo через минуту закроет сам.

Дедуп двухслойный, как у SLA и академического алерта:
  • проверка — не чаще раза в RETAIL_GUARD_CHECK_EVERY_MIN на чат (человек пишет серией,
    ходить в amo на каждое сообщение незачем);
  • алерт — один на чат в RETAIL_GUARD_ALERT_DEDUP_H (иначе двадцать сообщений подряд
    дадут двадцать сообщений в чат). Отметка алерта лежит на постоянном томе и переживает
    пересборку контейнера, отметка проверки живёт в памяти — терять её не страшно.

Часовой лимит RETAIL_GUARD_HOUR_LIMIT — защита от массового прогона по старым сделкам
(урок showroom_alert 07.08.2026: пачка сообщений, и топик перестают читать).
"""

import asyncio
import datetime
import json
import logging
import os
import time

import alerts
import amo_service
import telegram_bot
import wazzup_sla
from api import BASE_URL
from tg_recipients import NOTIFY_CHAT_ID, NOTIFY_THREAD_ID, mentions_for
from waybill_config import (
    RETAIL_GUARD_ALERT_DEDUP_H,
    RETAIL_GUARD_CHANNEL_NAMES,
    RETAIL_GUARD_CHANNELS,
    RETAIL_GUARD_CHECK_EVERY_MIN,
    RETAIL_GUARD_DELAY_S,
    RETAIL_GUARD_DRY_RUN,
    RETAIL_GUARD_ENABLED,
    RETAIL_GUARD_HOUR_LIMIT,
    RETAIL_GUARD_OWN_PIPELINES,
    RETAIL_GUARD_PIPELINE_NAMES,
    RETAIL_GUARD_WINDOW_END_H,
    RETAIL_GUARD_WINDOW_START_H,
)

logger = logging.getLogger("uvicorn")

_MSK = datetime.timezone(datetime.timedelta(hours=3))

# Этапы «Успешно реализовано» и «Закрыто и не реализовано» — общие номера во ВСЕХ воронках
# аккаунта, поэтому закрытость считается по ним, а не по воронке (так же в wazzup_sla).
_CLOSED_STATUS_IDS = {142, 143}

_bg_tasks: set = set()

# chat_id → monotonic последней ПРОВЕРКИ. В памяти: рестарт всего лишь разрешит проверить
# заново, лишнего сообщения в чат это не даёт (отправку сторожит отметка на диске).
_checked: dict[str, float] = {}
_CHECKED_CAP = 5000

# chat_id → время последнего АЛЕРТА (стенные часы). Лежит на постоянном томе контейнера.
_alerted: dict[str, float] = {}
_ALERTED_CAP = 5000
_ALERTED_PATH = os.getenv("RETAIL_GUARD_SEEN_PATH", "/app/var/retail_lead_guard_seen.json")
_alerted_loaded = False

# Часовое окно отправок и отметка, что про перебор уже сказали.
_sent_times: list[float] = []
_burst_notified = False


# ---------------------------------------------------------------------------
# дедуп алертов на диске
# ---------------------------------------------------------------------------

def _load_alerted() -> None:
    global _alerted_loaded
    if _alerted_loaded:
        return
    _alerted_loaded = True
    try:
        with open(_ALERTED_PATH, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            for key, ts in data.items():
                try:
                    _alerted[str(key)] = float(ts)
                except (TypeError, ValueError):
                    continue
    except FileNotFoundError:
        pass
    except Exception:
        logger.exception("Сторож розничных лидов: не прочитался %s — дедуп с нуля", _ALERTED_PATH)


def _save_alerted() -> None:
    try:
        os.makedirs(os.path.dirname(_ALERTED_PATH), exist_ok=True)
        tmp = f"{_ALERTED_PATH}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_alerted, f)
        os.replace(tmp, _ALERTED_PATH)
    except Exception:
        logger.exception("Сторож розничных лидов: не записался %s", _ALERTED_PATH)


def _alert_is_new(chat_id: str) -> bool:
    """True — про этот чат в текущем окне ещё не писали (писать можно)."""
    _load_alerted()
    now = time.time()
    window = RETAIL_GUARD_ALERT_DEDUP_H * 3600
    for key in [k for k, ts in _alerted.items() if now - ts > window]:
        _alerted.pop(key, None)
    if len(_alerted) > _ALERTED_CAP:
        for key in sorted(_alerted, key=lambda k: _alerted[k])[: len(_alerted) - _ALERTED_CAP]:
            _alerted.pop(key, None)
    if chat_id in _alerted:
        return False
    _alerted[chat_id] = now
    _save_alerted()
    return True


def _unalert(chat_id: str) -> None:
    """Снять отметку: отправки не было (не дошло, лимит, сухой прогон). Иначе следующий
    честный случай промолчал бы всё окно."""
    if _alerted.pop(chat_id, None) is not None:
        _save_alerted()


def _check_is_due(chat_id: str, now_mono: float) -> bool:
    """Пора ли снова спрашивать amo про этот чат."""
    last = _checked.get(chat_id)
    if last is not None and (now_mono - last) < RETAIL_GUARD_CHECK_EVERY_MIN * 60:
        return False
    _checked[chat_id] = now_mono
    if len(_checked) > _CHECKED_CAP:
        for key in sorted(_checked, key=lambda k: _checked[k])[: len(_checked) - _CHECKED_CAP]:
            _checked.pop(key, None)
    return True


def _budget_ok() -> bool:
    global _burst_notified
    now = time.time()
    _sent_times[:] = [ts for ts in _sent_times if now - ts < 3600]
    if len(_sent_times) < RETAIL_GUARD_HOUR_LIMIT:
        if not _sent_times:
            _burst_notified = False
        return True
    return False


def _in_window(now: datetime.datetime | None = None) -> bool:
    """Рабочее окно МСК. Ночной алерт никто не прочитает, а чат разбудит; утреннее
    молчание не теряется — то же обращение поднимет SLA-таймер «клиент ждёт ответа»."""
    hour = (now or _now_msk()).hour
    return RETAIL_GUARD_WINDOW_START_H <= hour < RETAIL_GUARD_WINDOW_END_H


# ---------------------------------------------------------------------------
# приём вебхука Wazzup
# ---------------------------------------------------------------------------

def on_wazzup(payload: dict) -> None:
    """Пятый потребитель вебхука Wazzup (`POST /wazzup/<secret>`). Здесь только сравнения
    и словарь: сеть — в фоне, в `_apply`. Никогда не бросает наружу."""
    if not RETAIL_GUARD_ENABLED:
        return
    if not isinstance(payload, dict):
        return
    messages = payload.get("messages")
    if not isinstance(messages, list):
        return

    now_mono = time.monotonic()
    for m in messages:
        if not isinstance(m, dict):
            continue
        # Направление считает wazzup_sla — правило одно на весь контур, копий не делаем.
        if wazzup_sla._is_outbound(m):
            continue
        channel_id = str(m.get("channelId") or "")
        chat_id = str(m.get("chatId") or "")
        if not chat_id:
            continue
        if channel_id not in RETAIL_GUARD_CHANNELS:
            continue
        if not _check_is_due(chat_id, now_mono):
            continue
        contact = m.get("contact") if isinstance(m.get("contact"), dict) else {}
        st = {
            "chat_id": chat_id,
            "channel_id": channel_id,
            "chat_type": str(m.get("chatType") or ""),
            "contact_name": str((contact or {}).get("name") or ""),
            "username": str((contact or {}).get("username") or ""),
            "text": _snippet(m.get("text")),
        }
        task = asyncio.create_task(_apply(st))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)


def _snippet(text, limit: int = 160) -> str:
    s = " ".join(str(text or "").split())
    return s[:limit] + ("…" if len(s) > limit else "")


# ---------------------------------------------------------------------------
# проверка и алерт
# ---------------------------------------------------------------------------

def _dry_log(st: dict, verdict: str, leads: int = 0, open_leads: int = 0) -> None:
    """В сухом прогоне пишем КАЖДОЕ решение, а не только найденные случаи: иначе по
    журналу не отличить «сторож молчит, потому что всё в порядке» от «сторож не
    работает». В боевом режиме молчание остаётся молчанием - журнал не засоряем."""
    if not RETAIL_GUARD_DRY_RUN:
        return
    logger.info(
        "Сторож розничных лидов[сухой прогон]: беседа %s, канал %s — %s (сделок %s, открытых %s)",
        st.get("chat_id"), _channel_label(st), verdict, leads, open_leads,
    )


async def _apply(st: dict) -> None:
    chat_id = st["chat_id"]
    try:
        if RETAIL_GUARD_DELAY_S:
            await asyncio.sleep(RETAIL_GUARD_DELAY_S)

        leads = await amo_service.find_leads_by_query(chat_id)
        if leads is None:
            # Молчание amoCRM — не «сделок нет». Тревогу не поднимаем.
            logger.warning("Сторож розничных лидов: amoCRM не ответила на поиск сделок по чату")
            _dry_log(st, "amoCRM не ответила, молчим")
            return

        open_leads = [ld for ld in leads if ld.get("status_id") not in _CLOSED_STATUS_IDS]
        if not open_leads:
            # Открытых сделок нет — amo создаст новую сам, это штатный путь.
            _dry_log(st, "открытых сделок нет, amo создаст сам", len(leads), 0)
            return

        own = [ld for ld in open_leads if str(ld.get("pipeline_id")) in RETAIL_GUARD_OWN_PIPELINES]
        if own:
            _dry_log(
                st, f"своя открытая сделка есть — воронка «{_pipeline_name(own[0].get('pipeline_id'))}»",
                len(leads), len(open_leads),
            )
            return

        foreign = max(open_leads, key=lambda ld: (ld.get("updated_at") or 0, ld.get("id") or 0))
        if not _in_window():
            logger.info(
                "Сторож розничных лидов: вне окна %s-%s МСК — молчим (беседа %s)",
                RETAIL_GUARD_WINDOW_START_H, RETAIL_GUARD_WINDOW_END_H, chat_id,
            )
            _dry_log(st, "случай есть, но вне окна отправки", len(leads), len(open_leads))
            return
        if not _alert_is_new(chat_id):
            _dry_log(st, "случай есть, но про этот чат уже писали в окне дедупа",
                     len(leads), len(open_leads))
            return
        if not _budget_ok():
            _unalert(chat_id)
            await _report_burst()
            return

        lead_id = foreign.get("id")
        pipeline = _pipeline_name(foreign.get("pipeline_id"))
        text = _build_message(st, lead_id, pipeline, mentions_for(None))
        d = alerts.decide(
            "retail_lead_missing", legacy_text=text, parse_mode="HTML",
            chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID, lead=foreign,
            values={
                "теги": mentions_for(None),
                "клиент": _client_label(st),
                "канал": _channel_label(st),
                "сообщение": f"«{st['text']}»" if st.get("text") else "",
                "воронка": pipeline,
                "ссылка_на_сделку": alerts.lead_link(lead_id),
            },
        )
        if d is None:
            logger.info("Сторож розничных лидов: событие выключено в панели (беседа %s)", chat_id)
            _unalert(chat_id)
            return

        if RETAIL_GUARD_DRY_RUN:
            logger.info(
                "Сторож розничных лидов[сухой прогон]: отправил бы в чат %s топик %s:\n%s",
                d.chat_id, d.thread_id, d.text,
            )
            _unalert(chat_id)
            return

        ok = await telegram_bot.send_alert(d.text, **d.send_kwargs())
        if ok:
            _sent_times.append(time.time())
        else:
            _unalert(chat_id)
        logger.info(
            "Сторож розничных лидов: алерт %s (беседа %s, открытая сделка в воронке «%s»)",
            "отправлен" if ok else "НЕ отправлен", chat_id, pipeline,
        )
    except Exception:
        logger.exception("Сторож розничных лидов: ошибка на беседе %s", chat_id)
        _unalert(chat_id)


async def _report_burst() -> None:
    """Перебор часового лимита: одно предупреждение за окно, дальше тишина. Молчать совсем
    нельзя — пропажа уведомлений выглядит как поломка бота."""
    global _burst_notified
    if _burst_notified:
        return
    _burst_notified = True
    logger.warning(
        "Сторож розничных лидов: часовой лимит %s исчерпан — уведомления приглушены",
        RETAIL_GUARD_HOUR_LIMIT,
    )
    d = alerts.decide(
        "retail_lead_missing_burst",
        legacy_text=(
            f"🆕 Обращений без сделки в рознице за час больше {RETAIL_GUARD_HOUR_LIMIT}, "
            "дальше уведомления приглушены до конца часа. Похоже на массовый перенос сделок "
            "или прогон по базе, стоит заглянуть в воронку."
        ),
        chat_id=NOTIFY_CHAT_ID, thread_id=NOTIFY_THREAD_ID,
        values={"лимит": RETAIL_GUARD_HOUR_LIMIT},
    )
    if d is None or RETAIL_GUARD_DRY_RUN:
        return
    await telegram_bot.send_alert(d.text, **d.send_kwargs())


# ---------------------------------------------------------------------------
# текст
# ---------------------------------------------------------------------------

def _esc(s) -> str:
    return str(s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _pipeline_name(pipeline_id) -> str:
    """Название воронки словами. ID в тексте для человека не бывает (правило Кати
    03.08.2026), поэтому незнакомая воронка называется общим словом."""
    return RETAIL_GUARD_PIPELINE_NAMES.get(str(pipeline_id), "другая воронка")


def _channel_label(st: dict) -> str:
    """Канал словами. Имя из карты; нет его — тип чата из вебхука."""
    return RETAIL_GUARD_CHANNEL_NAMES.get(st.get("channel_id") or "", st.get("chat_type") or "")


def _client_label(st: dict) -> str:
    """Кто написал. У WhatsApp chat_id — это телефон, его показываем; у телеграма
    chat_id анонимный номер, вместо него ник (он же читаемый адрес человека)."""
    name = (st.get("contact_name") or "").strip()
    chat_type = (st.get("chat_type") or "").lower()
    extra = ""
    if chat_type in ("whatsapp", "wapi"):
        extra = st.get("chat_id") or ""
    else:
        username = (st.get("username") or "").strip()
        if username:
            extra = username if username.startswith("@") else f"@{username}"
    if name and extra:
        return f"{name}, {extra}"
    return name or extra or "клиент без имени в карточке"


def _build_message(st: dict, lead_id, pipeline: str, mentions: str) -> str:
    """Без ID и без точек посередине (правила Кати 03.08.2026 и 26.08.2026)."""
    lines = [
        "🆕 Клиент написал, а сделки в рознице нет",
        mentions,
    ]
    channel = _channel_label(st)
    if channel:
        lines.append(f"💬 {_esc(channel)}")
    lines.append(f"👤 {_esc(_client_label(st))}")
    if st.get("text"):
        lines.append(f"«{_esc(st['text'])}»")
    lines.append(
        f"📋 Переписка легла в открытую сделку воронки «{_esc(pipeline)}» — "
        "в рознице её не видно. Новое обращение? Заведите сделку в рознице"
    )
    if lead_id:
        lines.append(f'🔗 <a href="{BASE_URL}/leads/detail/{lead_id}">Открыть сделку</a>')
    return "\n".join(lines)


def _now_msk() -> datetime.datetime:
    return datetime.datetime.now(tz=_MSK)
