"""Сторож перемещений сделок по журналу событий amo (постановка Кати 13.11.2026).

Что делает. Раз в `STAGE_GUARD_INTERVAL_S` читает `/api/v4/events` (тип
`lead_status_changed`) и судит каждый переход людьми по правилам `stage_guard_rules.judge`:
  • нарушение (G5 межворонковый ход, G6 онлайн-МОП в Офисе, G2 чужой в УР Офиса) - вернуть
    сделку туда, откуда её перевели, и написать в чат ОП с тегом того, кто перевёл;
  • G7 (сделку вернули из Офиса в ОП) - только уведомить.
Основная защита - виджет F5 в интерфейсе amo. Сторож - страховка: при живом виджете
нарушений почти нет; три и больше за час значат, что виджет не держит, - об этом
отдельное сообщение в технический чат.

Почему журнал событий, а не вебхук `lead_change`:
  • вебхук не говорит, КТО сдвинул сделку, и не несёт прошлого этапа (подписка без
    `status_lead`, см. `lead_status_store.py`);
  • вебхуки amo теряются насовсем (16.09.2026 доехало 23%), журнал - нет.
Цена - один GET в минуту: за 14.10-13.11.2026 было 9428 переходов, ~13 в час.

────────────────────────────── почему нет карусели ──────────────────────────────
  • Откат делает интеграция, а её токен выписан на Гладкова - он в исключениях правил,
    поэтому свой откат сторож в журнале не судит.
  • Не больше одного отката на сделку за `STAGE_GUARD_ROLLBACK_COOLDOWN_S` (30 мин):
    человек повторил - только уведомление.
  • Откатываем, только если сделка всё ещё там, куда её перевели. Ушла дальше (робот,
    человек) - не трогаем, уведомляем: иначе откат сломал бы чужое законное действие.
  • В «Неразобранное» (системный этап) обычным PATCH не вернуть - только уведомление.
⚠️ Откат возвращает сделку на прежний этап, и там снова срабатывает автоматика входа
(бот «Предзаказ оплачен», раздача «Нового лида»). А побочный эффект ошибочного хода
(накладная СДЭК, отгрузка МойСклада) откат не отменяет. Решение Кати 13.11: откатывать
всё равно - при живом виджете это редкость.

Режимы (`STAGE_GUARD_MODE`): `dry` - только лог и журнал на диске, ничего не шлём и не
двигаем (по умолчанию: так выкатываем и сверяем неделю); `notify` - уведомляем, не откатываем;
`rollback` - уведомляем и откатываем. Выключатель всего - `STAGE_GUARD_ENABLED=1`.

Курсор журнала и дедуп событий - в SQLite на постоянном томе: контейнер пересоздают по
несколько раз в день, без этого каждая пересборка заново судила бы последние минуты.
Первый запуск начинает с «сейчас минус `STAGE_GUARD_START_LOOKBACK_S`», историю не судит.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import pathlib
import sqlite3
import time
from collections import deque

import alerts
import amo_service
import telegram_bot
import tg_recipients
from stage_guard_rules import Config, Move, Verdict, judge, parse_event
from waybill_config import MANAGER_NAMES, WAZZUP_TG_HANDLES

logger = logging.getLogger("uvicorn")

STAGE_GUARD_ENABLED = os.getenv("STAGE_GUARD_ENABLED", "").strip() == "1"
STAGE_GUARD_MODE = os.getenv("STAGE_GUARD_MODE", "dry").strip().lower()
if STAGE_GUARD_MODE not in ("dry", "notify", "rollback"):
    STAGE_GUARD_MODE = "dry"
STAGE_GUARD_INTERVAL_S = int(os.getenv("STAGE_GUARD_INTERVAL_S", "45"))
STAGE_GUARD_ROLLBACK_COOLDOWN_S = int(os.getenv("STAGE_GUARD_ROLLBACK_COOLDOWN_S", "1800"))
STAGE_GUARD_START_LOOKBACK_S = int(os.getenv("STAGE_GUARD_START_LOOKBACK_S", "300"))
# Перекрытие окна запроса: событие с той же секундой, что курсор, или записанное amo с
# опозданием, не должно выпасть. Дубли отсекает таблица `seen`.
STAGE_GUARD_OVERLAP_S = int(os.getenv("STAGE_GUARD_OVERLAP_S", "120"))
STAGE_GUARD_MAX_PAGES = int(os.getenv("STAGE_GUARD_MAX_PAGES", "20"))
# Сколько нарушений за час значат «виджет F5 не держит».
STAGE_GUARD_F5_ALARM_PER_HOUR = int(os.getenv("STAGE_GUARD_F5_ALARM_PER_HOUR", "3"))
STAGE_GUARD_F5_ALARM_REPEAT_S = int(os.getenv("STAGE_GUARD_F5_ALARM_REPEAT_S", str(6 * 3600)))
# Окно сводки: первое нарушение менеджера - подробным сообщением сразу, следующие в течение
# окна копятся и уходят одной сводкой (за выходные 13-15.11: 8 одинаковых сообщений в день).
# 0 - каждое нарушение отдельным сообщением.
STAGE_GUARD_DIGEST_S = int(os.getenv("STAGE_GUARD_DIGEST_S", "3600"))

# Поля сделки для подробностей в уведомлении.
FIELD_MS_ORDER_NUMBER = 576697     # «№ Заказа» МойСклада
FIELD_SITE_ORDER_NUMBER = 577415   # номер заказа на сайте
FIELD_CDEK_NUMBER = 571657         # номер накладной СДЭК
PIPELINE_OFFICE = 9421022
STATUS_MAKE_WAYBILL = 75426822
SHIPMENT_STATUSES = {75426866, 75426870, 75426874, 75426878, 75426882, 75426886, 142}

_DB_PATH = pathlib.Path(os.getenv("STAGE_GUARD_DB", "/app/var/stage_guard.sqlite"))
_JOURNAL_PATH = pathlib.Path(os.getenv("STAGE_GUARD_JOURNAL", "/app/var/stage_guard.jsonl"))
_JOURNAL_CAP = 5 * 1024 * 1024

EVENT_VIOLATION = "stage_guard_violation"
EVENT_OFFICE_RETURN = "stage_guard_office_return"
EVENT_F5_DOWN = "stage_guard_f5_down"

_cfg: Config = Config.from_env()
_task: asyncio.Task | None = None
_names: dict[tuple[int, int], str] = {}    # (воронка, этап) → «Воронка / Этап»
_pipe_names: dict[int, str] = {}
_unsorted: set[tuple[int, int]] = set()    # системные этапы «Неразобранное»
_names_loaded_at = 0.0
_violations_ts: deque[float] = deque()
_recent: deque[str] = deque(maxlen=50)          # короткие строки нарушений - для сигнала «F5 не держит»
_last_alert: dict[int, int] = {}                # менеджер → время его последнего полного уведомления
_digest: dict[int, dict] = {}                   # менеджер → повторы, ждущие сводки
_last_f5_alarm = 0.0
_last_run: dict = {}


# ─────────────────────────────── хранилище ───────────────────────────────


@contextlib.contextmanager
def _conn():
    """Соединение на одну операцию: коммит при успехе и ВСЕГДА закрытие.
    (`with sqlite3.connect()` коммитит, но не закрывает - соединения копились бы.)"""
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(_DB_PATH)
    try:
        c.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS seen (event_id TEXT PRIMARY KEY, ts INTEGER)")
        c.execute("CREATE TABLE IF NOT EXISTS rollbacks (lead_id INTEGER PRIMARY KEY, ts INTEGER)")
        yield c
        c.commit()
    finally:
        c.close()


def _get_cursor() -> int | None:
    with _conn() as c:
        row = c.execute("SELECT v FROM kv WHERE k='cursor'").fetchone()
    return int(row[0]) if row else None


def _set_cursor(ts: int) -> None:
    with _conn() as c:
        c.execute("INSERT OR REPLACE INTO kv (k, v) VALUES ('cursor', ?)", (str(int(ts)),))


def _claim_event(event_id: str, ts: int) -> bool:
    """True - событие новое и теперь помечено. Проверка и запись одним действием."""
    with _conn() as c:
        cur = c.execute("INSERT OR IGNORE INTO seen (event_id, ts) VALUES (?, ?)", (event_id, ts))
        return cur.rowcount == 1


def _rollback_allowed(lead_id: int, now: int) -> bool:
    with _conn() as c:
        row = c.execute("SELECT ts FROM rollbacks WHERE lead_id=?", (lead_id,)).fetchone()
    return row is None or now - int(row[0]) >= STAGE_GUARD_ROLLBACK_COOLDOWN_S


def _note_rollback(lead_id: int, now: int) -> None:
    with _conn() as c:
        c.execute("INSERT OR REPLACE INTO rollbacks (lead_id, ts) VALUES (?, ?)", (lead_id, now))


def _prune(now: int) -> None:
    with _conn() as c:
        c.execute("DELETE FROM seen WHERE ts < ?", (now - 3 * 86400,))
        c.execute("DELETE FROM rollbacks WHERE ts < ?", (now - 7 * 86400,))


def _journal(row: dict) -> None:
    """Журнал решений на диске - для сверки dry-режима и будущего экрана в панели."""
    try:
        _JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
        if _JOURNAL_PATH.exists() and _JOURNAL_PATH.stat().st_size > _JOURNAL_CAP:
            _JOURNAL_PATH.replace(_JOURNAL_PATH.with_suffix(".jsonl.1"))
        with _JOURNAL_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        logger.exception("Сторож перемещений: журнал на диск не записался")


# ─────────────────────────────── названия ───────────────────────────────


async def _load_names(force: bool = False) -> None:
    global _names_loaded_at
    if not force and time.time() - _names_loaded_at < 3600 and _names:
        return
    pipes = await amo_service.get_pipelines_with_statuses()
    if not pipes:
        return
    _names.clear(); _pipe_names.clear(); _unsorted.clear()
    for p in pipes:
        pid = int(p["id"])
        _pipe_names[pid] = p.get("name") or ""
        for s in (p.get("_embedded") or {}).get("statuses") or []:
            key = (pid, int(s["id"]))
            _names[key] = f"{p.get('name')} / {s.get('name')}"
            if int(s.get("type") or 0) == 1:
                _unsorted.add(key)
    _names_loaded_at = time.time()


def _stage(pipeline_id: int, status_id: int) -> str:
    return _names.get((pipeline_id, status_id)) or "этап не найден"


async def _who(user_id: int) -> tuple[str, str]:
    """(тег или имя для текста, имя словами)."""
    name = MANAGER_NAMES.get(user_id) or await amo_service.get_user_name(user_id) or "сотрудник"
    handle = WAZZUP_TG_HANDLES.get(user_id)
    return (handle or name), name


# ─────────────────────────────── чтение журнала ───────────────────────────────


async def _fetch_events(since_ts: int) -> list[dict] | None:
    """Все события смены этапа с `since_ts`. None - amo не ответил (курсор не двигаем)."""
    out: list[dict] = []
    for page in range(1, STAGE_GUARD_MAX_PAGES + 1):
        data = await amo_service._do_get("/api/v4/events", [
            ("filter[type]", "lead_status_changed"),
            ("filter[created_at][from]", str(since_ts)),
            ("limit", "100"),
            ("page", str(page)),
        ])
        if data is None:
            return None
        batch = (data.get("_embedded") or {}).get("events") or []
        out.extend(batch)
        if len(batch) < 100:
            break
    return out


# ─────────────────────────────── действие ───────────────────────────────


async def _rollback(m: Move, now: int) -> str:
    """Попробовать вернуть сделку. Возвращает итог для текста и журнала:
    done | dry | notify_only | cooldown | moved_on | unsorted | lead_unknown | failed."""
    if STAGE_GUARD_MODE == "dry":
        return "dry"
    if STAGE_GUARD_MODE != "rollback":
        return "notify_only"
    if (m.from_pipeline, m.from_status) in _unsorted:
        return "unsorted"
    if not _rollback_allowed(m.lead_id, now):
        return "cooldown"
    lead = await amo_service.get_lead_full(m.lead_id, with_=())
    if not lead:
        return "lead_unknown"
    if (int(lead.get("pipeline_id") or 0), int(lead.get("status_id") or 0)) != (m.to_pipeline, m.to_status):
        return "moved_on"
    # Слот занимаем ДО записи: обрыв сети после PATCH не должен дать второй откат.
    _note_rollback(m.lead_id, now)
    res = await amo_service.patch_lead(m.lead_id, status_id=m.from_status, pipeline_id=m.from_pipeline)
    return "done" if res.get("ok") else "failed"


_OUTCOME_TEXT = {
    "done": "Сделка возвращена в «{back}».",
    "dry": "Режим проверки: сторож вернул бы её в «{back}».",
    "notify_only": "Сделка не возвращена (сторож только уведомляет).",
    "cooldown": "Сделку уже возвращали за последние полчаса - второй раз не двигаю, проверьте руками.",
    "moved_on": "Сделка уже ушла с этого этапа - не трогаю, проверьте руками.",
    "unsorted": "В «Неразобранное» вернуть нельзя - проверьте руками.",
    "lead_unknown": "amo не отдал сделку - не трогаю, проверьте руками.",
    "failed": "Вернуть не получилось - проверьте руками.",
}


async def _send(event_key: str, text: str, values: dict, *, tech: bool = False) -> None:
    if STAGE_GUARD_MODE == "dry":
        return
    chat_id = None if tech else tg_recipients.NOTIFY_CHAT_ID
    thread_id = None if tech else tg_recipients.NOTIFY_THREAD_ID
    try:
        d = alerts.decide(event_key, legacy_text=text, values=values,
                          chat_id=chat_id, thread_id=thread_id, parse_mode="HTML")
        if d is None:
            return
        await telegram_bot.send_alert(d.text, **d.send_kwargs())
    except Exception:
        logger.exception("Сторож перемещений: уведомление %s не ушло", event_key)


def _field(lead: dict | None, field_id: int) -> str:
    if not lead:
        return ""
    return str(amo_service.get_custom_field_value(lead, field_id) or "").strip()


def _deal_line(lead: dict | None) -> str:
    """«Название», заказ №…, сумма - чтобы сделку узнавали без перехода по ссылке."""
    if not lead:
        return "Сделка: amo не отдал карточку"
    parts = [f"«{lead.get('name') or 'без названия'}»"]
    order = _field(lead, FIELD_MS_ORDER_NUMBER) or _field(lead, FIELD_SITE_ORDER_NUMBER)
    if order:
        parts.append(f"заказ №{order}")
    if lead.get("price"):
        parts.append(f"{int(lead['price']):,} ₽".replace(",", " "))
    return "Сделка: " + ", ".join(parts)


def _hint(m: Move) -> str | None:
    """Что этот переход уже успел сделать сам по себе - чтобы было понятно, что проверить."""
    if m.to_pipeline == PIPELINE_OFFICE and m.to_status == STATUS_MAKE_WAYBILL:
        return ("«Сделать накладную» заново создаёт накладную СДЭК - проверьте в СДЭК, не осталось ли "
                "двух накладных на одну посылку, лишнюю отмените.")
    if m.to_pipeline == PIPELINE_OFFICE and m.to_status in SHIPMENT_STATUSES:
        return "На этом этапе МойСклад создаёт отгрузку - товар списывается со склада."
    if m.to_pipeline != m.from_pipeline:
        return "Переносы между воронками делает автоматика по «Успешно реализовано» и «Закрыто»."
    return None


def _now_line(lead: dict | None) -> str | None:
    if not lead:
        return None
    stage = _stage(int(lead.get("pipeline_id") or 0), int(lead.get("status_id") or 0))
    cdek = _field(lead, FIELD_CDEK_NUMBER)
    return f"Сейчас: «{stage}»" + (f", накладная СДЭК № {cdek}" if cdek else "")


def _hhmm(ts: int) -> str:
    return time.strftime("%H:%M", time.gmtime(ts + 3 * 3600))


async def _add_rollback_note(m: Move, name: str, src: str, dst: str, reason: str) -> None:
    """Примечание в сделку после отката (просьба Кати 17.11.2026): в карточке видно, почему этап
    вернулся. Сбой примечания откат не отменяет - только лог."""
    text = f"{name} - ЗАПРЕЩЕННОЕ ДЕЙСТВИЕ: перемещение из «{src}» в «{dst}». Сделка возвращена."
    try:
        await amo_service.add_note(m.lead_id, text)
    except Exception:
        logger.exception("Сторож перемещений: примечание в сделку %s не записалось", m.lead_id)


async def _handle(m: Move, v: Verdict, now: int) -> dict:
    tag, name = await _who(m.user_id)
    src, dst = _stage(m.from_pipeline, m.from_status), _stage(m.to_pipeline, m.to_status)
    link = alerts.lead_link(m.lead_id)
    row = {"ts": now, "event_id": m.event_id, "lead_id": m.lead_id, "user_id": m.user_id,
           "user": name, "rule": v.rule, "from": src, "to": dst, "mode": STAGE_GUARD_MODE}
    lead = await amo_service.get_lead_full(m.lead_id, with_=())
    deal = _deal_line(lead)

    if v.action == "notify":
        text = (f"↩️ {name} вернул(а) сделку из Офиса в {_hhmm(m.created_at)}: «{src}» → «{dst}»\n"
                f"{deal}\n{link}")
        await _send(EVENT_OFFICE_RETURN, text,
                    {"кто": name, "откуда": src, "куда": dst, "ссылка_на_сделку": link})
        row["outcome"] = "notified" if STAGE_GUARD_MODE != "dry" else "dry"
        return row

    outcome = await _rollback(m, now)
    row["outcome"] = outcome
    if outcome == "done":
        await _add_rollback_note(m, name, src, dst, v.reason)
        lead = await amo_service.get_lead_full(m.lead_id, with_=())
    short = f"{_hhmm(m.created_at)} «{(lead or {}).get('name') or 'сделка'}»: «{src}» → «{dst}» - {link}"
    _recent.append(f"{name}, {short}")
    _violations_ts.append(now)

    if _in_digest_window(m.user_id, now):
        _digest.setdefault(m.user_id, {"tag": tag, "items": []})["items"].append(short)
    else:
        lines = [f"⛔ {tag} некорректный перевод в {_hhmm(m.created_at)}: «{src}» → «{dst}»", deal,
                 f"Почему нельзя: {v.reason}."]
        hint = _hint(m)
        if hint:
            lines.append(hint)
        now_line = _now_line(lead)
        if now_line:
            lines.append(now_line)
        lines += [_OUTCOME_TEXT[outcome].format(back=src), link]
        await _send(EVENT_VIOLATION, "\n".join(lines),
                    {"теги": tag, "кто": name, "откуда": src, "куда": dst,
                     "причина": v.reason, "ссылка_на_сделку": link})
        _last_alert[m.user_id] = now
    await _maybe_f5_alarm(now)
    return row


def _in_digest_window(user_id: int, now: int) -> bool:
    last = _last_alert.get(user_id)
    return STAGE_GUARD_DIGEST_S > 0 and last is not None and now - last < STAGE_GUARD_DIGEST_S


async def _flush_digests(now: int, force: bool = False) -> None:
    """Повторы одного менеджера за час - одной сводкой, а не восемью сообщениями подряд."""
    for user_id in list(_digest):
        if not force and _in_digest_window(user_id, now):
            continue
        d = _digest.pop(user_id)
        if not d["items"]:
            continue
        n = len(d["items"])
        text = "\n".join([f"⛔ {d['tag']} ещё некорректных переводов за час: {n}"]
                         + [f"— {x}" for x in d["items"]])
        await _send(EVENT_VIOLATION, text, {"теги": d["tag"], "сколько": n})


async def _maybe_f5_alarm(now: int) -> None:
    global _last_f5_alarm
    while _violations_ts and now - _violations_ts[0] > 3600:
        _violations_ts.popleft()
    if len(_violations_ts) < STAGE_GUARD_F5_ALARM_PER_HOUR:
        return
    if now - _last_f5_alarm < STAGE_GUARD_F5_ALARM_REPEAT_S:
        return
    _last_f5_alarm = now
    recent = list(_recent)[-len(_violations_ts):]
    text = "\n".join(
        [f"⚠️ За последний час {len(_violations_ts)} запрещённых перемещений сделок. "
         "Похоже, виджет F5 «Управление полями» перестал держать «Контроль перемещений» - "
         "проверьте виджет в amo (оплата, доступ, настройки) или каким путём двигают сделки:"]
        + [f"— {x}" for x in recent[-10:]])
    logger.warning("Сторож перемещений: %s", text)
    await _send(EVENT_F5_DOWN, text, {"сколько": len(_violations_ts)}, tech=True)


# ─────────────────────────────── проход ───────────────────────────────


async def sweep_once() -> dict:
    now = int(time.time())
    await _load_names()
    cursor = await asyncio.to_thread(_get_cursor)
    if cursor is None:
        cursor = now - STAGE_GUARD_START_LOOKBACK_S
        await asyncio.to_thread(_set_cursor, cursor)
    events = await _fetch_events(cursor - STAGE_GUARD_OVERLAP_S)
    if events is None:
        _last_run.update(at=now, ok=False, note="amo не ответил")
        return dict(_last_run)
    moves = [m for m in (parse_event(e) for e in events) if m is not None]
    moves.sort(key=lambda m: (m.created_at, m.event_id))
    judged = acted = 0
    newest = cursor
    for m in moves:
        newest = max(newest, m.created_at)
        if m.created_at < cursor - STAGE_GUARD_OVERLAP_S:
            continue
        if not await asyncio.to_thread(_claim_event, m.event_id, m.created_at):
            continue
        judged += 1
        v = judge(m, _cfg)
        if v is None:
            continue
        acted += 1
        try:
            row = await _handle(m, v, now)
        except Exception:
            logger.exception("Сторож перемещений: сделка %s, событие %s", m.lead_id, m.event_id)
            row = {"ts": now, "event_id": m.event_id, "lead_id": m.lead_id, "rule": v.rule,
                   "outcome": "error"}
        logger.info("Сторож перемещений: %s", row)
        await asyncio.to_thread(_journal, row)
    await _flush_digests(now)
    await asyncio.to_thread(_set_cursor, newest)
    await asyncio.to_thread(_prune, now)
    _last_run.update(at=now, ok=True, events=len(moves), judged=judged, violations=acted)
    return dict(_last_run)


# ─────────────────────────────── жизненный цикл ───────────────────────────────


async def _loop() -> None:
    import api
    api.set_api_priority(api.API_PRIORITY_BACKGROUND)
    while True:
        try:
            if not api.skip_if_congested("Сторож перемещений"):
                await sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Сторож перемещений: проход упал")
        await asyncio.sleep(STAGE_GUARD_INTERVAL_S)


async def init() -> None:
    global _task
    if not STAGE_GUARD_ENABLED:
        logger.info("Сторож перемещений: выключен (STAGE_GUARD_ENABLED=0)")
        return
    if _task is None:
        _task = asyncio.create_task(_loop())
        logger.info(
            "Сторож перемещений: поднят, режим %s, опрос %ss, исключены %s, онлайн-МОПы %s",
            STAGE_GUARD_MODE, STAGE_GUARD_INTERVAL_S, sorted(_cfg.exempt), sorted(_cfg.online_mops),
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
    if not STAGE_GUARD_ENABLED:
        return {"enabled": False}
    return {"enabled": True, "mode": STAGE_GUARD_MODE, "interval_s": STAGE_GUARD_INTERVAL_S,
            "last_run": dict(_last_run)}
