"""Перехват сделок менеджера (ВРЕМЕННО, решение Кати 12.11.2026).

Правило: клиент написал в сделке менеджера Игоря Оанча (любая воронка, в том числе «Офис»), и на его сообщение нет ответа человека
15 минут рабочего времени ИИ. Тогда движок отдаёт сделку ИИ-менеджеру, и тот консультирует клиента.

Порядок перехвата (`tick`):
  1. Серверная логика (не модель) пишет в сделку заметку «ИИ взял сделку». Она пишется сразу и не зависит от режима проверки
     человеком: это действие движка, а не вызов инструмента моделью.
  2. Сделка переходит к ИИ (`owner = ai`, эпоха +1), ставится событие `deal_intercepted`.
  3. Если после заметки менеджер что-то написал клиенту, ответ ИИ не уходит: движок отказывает в `permit` (`manager_replied_after`),
     а сообщение менеджера в чате к тому же забирает диалог у ИИ (`ai_engine._on_outbound`). Сервис держит свою отсечку по времени заметки
     (`manager_cutoff` = момент перед заметкой), поэтому при одобрении ответа из очереди проверки действует та же проверка.

Решения по неоднозначным местам (записаны и в журнале `PROGON-SKVOZNOY-2026-10-29.md`):
  • Ответом считается сообщение человека (автор не `Admin`, не пусто, не `bot`). Автоматика и сам ИИ ответом не считаются.
  • 15 минут считаются по рабочим часам ИИ (окно отправки, настройка панели): сообщение вечером ждёт утра и ещё 15 минут.
  • Отсчёт идёт от ПЕРВОГО неотвеченного сообщения клиента, следующие сообщения срок не сдвигают.
  • У клиента несколько открытых сделок Игоря: не берём ни одну (неоднозначно), пишем в лог.
  • Одну сделку берём один раз (таблица `intercept_taken`); сделки с владельцем в базе движка (ИИ, человек, движок) и сделки, чат
    которых уже ведёт обычный вход ИИ, не трогаем.
  • Контакт должен проходить список режима (`settings.contact_allowed`), режим `off` выключает перехват, тумблер в панели
    `intercept.enabled`.

Удаление: этот файл, вызовы `intercept.*` в `ai_engine.py`, параметр `any_pipeline_ids` в `ai_engine_dedup.py`, функция
`intercept_enabled` в `ai_engine_settings.py` и `test_ai_engine_intercept.py`.

Таблицы создаются при первом обращении (`IF NOT EXISTS`) в той же sqlite, что у `ai_engine_store`.
"""
import asyncio
import datetime
import logging
import os
from typing import Any

import ai_engine_dedup as dedup
import ai_engine_give as give
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from shared.timez import MSK

logger = logging.getLogger("uvicorn")

# Менеджер, чьи сделки перехватываем: Игорь Оанча (id 9291546 сверен по справочнику пользователей amo и конфигу движка).
RESPONSIBLE_ID = int(os.getenv("AI_INTERCEPT_USER_ID", "9291546"))
# Сколько рабочего времени ИИ клиент ждёт ответа, прежде чем сделку перехватят.
WAIT_S = int(os.getenv("AI_INTERCEPT_WAIT_S", "900"))
# Сколько раз пробуем довести перехват (amo не ответила, заметка не записалась, ИИ-сервис лежит), прежде чем бросить.
MAX_TRIES = int(os.getenv("AI_INTERCEPT_MAX_TRIES", "20"))
RETRY_S = int(os.getenv("AI_INTERCEPT_RETRY_S", "60"))
PER_TICK = 3
CHAT_TYPES = frozenset({"whatsapp", "telegram"})        # группы и прочие мессенджеры не берём
NOTE_TEXT = "ИИ взял сделку: клиент написал, ответа менеджера нет {minutes} минут. Дальше клиента консультирует ИИ-менеджер."
NOTE_REVOKED = "ИИ сделку не ведёт: пока писалась прошлая заметка, менеджер ответил клиенту."

_SCHEMA = """
CREATE TABLE IF NOT EXISTS intercept_wait (
    chat_key TEXT PRIMARY KEY, channel_id TEXT NOT NULL, chat_id TEXT NOT NULL, chat_type TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '', contact_name TEXT NOT NULL DEFAULT '', first_at REAL NOT NULL, last_at REAL NOT NULL,
    tries INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS intercept_taken (
    deal_id INTEGER PRIMARY KEY, chat_key TEXT NOT NULL, taken_at REAL NOT NULL, cutoff_at REAL NOT NULL, manager_out_at REAL);
"""


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


# =============================================================================================
# настройка и рабочее время
# =============================================================================================

def enabled() -> bool:
    """Тумблер панели включён и режим ИИ не `off` (режим `off` отдаёт `effective_mode`)."""
    return settings.intercept_enabled() and settings.effective_mode() != "off"


def _hm(value: str) -> tuple[int, int]:
    hh, mm = str(value).split(":")
    return int(hh), int(mm)


def due_at(first_at: float, wait_s: int, hours: list[dict[str, str]]) -> float | None:
    """Момент, когда после `first_at` наберётся `wait_s` секунд рабочего времени (`hours`: промежутки МСК вида «10:00»-«19:00»).
    Нет рабочих часов - никогда (None)."""
    spans = sorted((_hm(h["start"]), _hm(h["end"])) for h in hours)
    if not spans:
        return None
    t = float(first_at)
    left = float(wait_s)
    day = datetime.datetime.fromtimestamp(t, MSK).replace(hour=0, minute=0, second=0, microsecond=0)
    for _ in range(40):
        for (sh, sm), (eh, em) in spans:
            start = (day + datetime.timedelta(hours=sh, minutes=sm)).timestamp()
            end = (day + datetime.timedelta(hours=eh, minutes=em)).timestamp()
            if t >= end:
                continue
            begin = max(t, start)
            if end - begin >= left:
                return begin + left
            left -= end - begin
            t = end
        day += datetime.timedelta(days=1)
    return None


# =============================================================================================
# наблюдение за чатами (вебхук Wazzup, любой канал)
# =============================================================================================

def observe_inbound(chat_key: str, channel_id: str, chat_id: str, chat_type: str, phone: str, name: str, now: float) -> None:
    """Клиент написал: чат ждёт ответа. Срок идёт от первого неотвеченного сообщения."""
    with _Db() as conn:
        conn.execute(
            "INSERT INTO intercept_wait (chat_key, channel_id, chat_id, chat_type, phone, contact_name, first_at, last_at) "
            "VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(chat_key) DO UPDATE SET last_at=excluded.last_at, "
            "phone=CASE WHEN excluded.phone != '' THEN excluded.phone ELSE phone END",
            (chat_key, channel_id, chat_id, chat_type, phone, name, now, now))


def observe_human_out(chat_key: str, sent_ts: float) -> None:
    """Человек написал клиенту: ожидание снято, а у перехваченной сделки запомнено время сообщения (для отказа в `permit`)."""
    with _Db() as conn:
        conn.execute("DELETE FROM intercept_wait WHERE chat_key=?", (chat_key,))
        conn.execute("UPDATE intercept_taken SET manager_out_at=MAX(COALESCE(manager_out_at, 0), ?) WHERE chat_key=?",
                     (float(sent_ts), chat_key))


def is_taken(deal_id: int) -> bool:
    with _Db() as conn:
        return conn.execute("SELECT 1 FROM intercept_taken WHERE deal_id=?", (int(deal_id),)).fetchone() is not None


def is_tracked(chat_key: str) -> bool:
    """Чат перехваченной сделки: сообщения из него разбираем, даже если канал не входит в `AI_ENGINE_CHANNELS`."""
    with _Db() as conn:
        return conn.execute("SELECT 1 FROM intercept_taken WHERE chat_key=?", (chat_key,)).fetchone() is not None


def manager_replied_after(deal_id: int) -> bool:
    """После заметки «ИИ взял сделку» менеджер написал клиенту: ответ ИИ не отправляем."""
    with _Db() as conn:
        row = conn.execute("SELECT cutoff_at, manager_out_at FROM intercept_taken WHERE deal_id=?", (int(deal_id),)).fetchone()
    return bool(row and row["manager_out_at"] is not None and row["manager_out_at"] > row["cutoff_at"])


def has_wait(chat_key: str) -> bool:
    with _Db() as conn:
        return conn.execute("SELECT 1 FROM intercept_wait WHERE chat_key=?", (chat_key,)).fetchone() is not None


def forget_wait(chat_key: str) -> None:
    with _Db() as conn:
        conn.execute("DELETE FROM intercept_wait WHERE chat_key=?", (chat_key,))


def _retry(chat_key: str, now: float, why: str) -> None:
    """Перехват не удался по временной причине: повтор через `RETRY_S`, после `MAX_TRIES` попыток ожидание снимается."""
    with _Db() as conn:
        row = conn.execute("SELECT tries FROM intercept_wait WHERE chat_key=?", (chat_key,)).fetchone()
        if row is None:
            return
        if row["tries"] + 1 >= MAX_TRIES:
            conn.execute("DELETE FROM intercept_wait WHERE chat_key=?", (chat_key,))
            logger.warning("ai_engine: перехват чата %s брошен после %s попыток (%s)", chat_key, MAX_TRIES, why)
        else:
            conn.execute("UPDATE intercept_wait SET tries=tries+1, retry_at=? WHERE chat_key=?", (now + RETRY_S, chat_key))


def _due_rows(now: float, hours: list[dict[str, str]]) -> list[dict[str, Any]]:
    with _Db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM intercept_wait WHERE retry_at<=? ORDER BY first_at", (now,))]
    out = []
    for r in rows:
        due = due_at(r["first_at"], WAIT_S, hours)
        if due is not None and due <= now:
            out.append(r)
    return out[:PER_TICK]


def _mark_taken(deal_id: int, chat_key: str, now: float, cutoff_at: float) -> None:
    with _Db() as conn:
        conn.execute("INSERT OR REPLACE INTO intercept_taken (deal_id, chat_key, taken_at, cutoff_at, manager_out_at) VALUES (?,?,?,?,NULL)",
                     (int(deal_id), chat_key, now, cutoff_at))
        conn.execute("DELETE FROM intercept_wait WHERE chat_key=?", (chat_key,))


# =============================================================================================
# перехват
# =============================================================================================

async def tick(eng) -> None:
    """Один проход: чаты, у которых вышел срок ожидания. `eng` - модуль `ai_engine` (его замок, база, заметки, события)."""
    if not enabled() or not settings.send_window()["open_now"]:
        return                                           # перехват только в окне отправки: ответ ИИ вне окна всё равно не уйдёт
    now = eng._now()
    rows = await eng._db(_due_rows, now, settings.work_hours())
    for row in rows:
        try:
            await _process(eng, row, now)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: ошибка перехвата чата %s", row.get("chat_key"))
            await eng._db(_retry, row["chat_key"], now, "exception")


async def _process(eng, row: dict[str, Any], now: float) -> None:
    chat_key, chat_id, phone = row["chat_key"], row["chat_id"], row["phone"]
    # Чат, который уже ведёт обычный вход ИИ (канал из AI_ENGINE_CHANNELS, сессия есть): ему перехват не нужен.
    s = await eng._db(store.get_session, chat_key)
    if s is not None and s["state"] in (store.S_COLLECTING, store.S_AI, store.S_IN_TURN):
        logger.info("ai_engine: перехват %s: чат уже ведёт ИИ, пропускаю", chat_key)
        await eng._db(forget_wait, chat_key)
        return
    leads = await dedup.find_leads(chat_id, phone)
    if leads is None:
        await eng._db(_retry, chat_key, now, "amo не ответила")
        return
    mine = [ld for ld in leads if dedup.is_open(ld) and _responsible(ld) == RESPONSIBLE_ID]
    if not mine:
        await eng._db(forget_wait, chat_key)             # сделка не Игоря (или закрыта): не наше дело
        return
    if len(mine) > 1:
        logger.warning("ai_engine: перехват %s: у клиента несколько открытых сделок Игоря %s, не беру ни одну",
                       chat_key, [int(ld["id"]) for ld in mine])
        await eng._db(forget_wait, chat_key)
        return
    deal = mine[0]
    deal_id = int(deal["id"])
    why = await _skip_reason(eng, deal, chat_id, phone)
    if why:
        logger.info("ai_engine: перехват сделки %s пропущен: %s", deal_id, why)
        await eng._db(forget_wait, chat_key)
        return
    if not await eng._ai_is_up():
        await eng._db(_retry, chat_key, now, "ИИ-сервис не отвечает")
        return

    # Отсечка берётся ДО заметки: сообщение менеджера, написанное после неё (а значит и в промежутке), останавливает ИИ.
    cutoff_at = eng._now()
    text = NOTE_TEXT.format(minutes=WAIT_S // 60)
    try:
        await asyncio.wait_for(amo_service.add_note(deal_id, text), timeout=15)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: заметка «ИИ взял сделку» в сделке %s не записалась, сделку не беру", deal_id)
        await eng._db(_retry, chat_key, now, "заметка не записалась")
        return

    async with eng._get_lock():
        if not await eng._db(has_wait, chat_key):
            # Пока писалась заметка, менеджер ответил клиенту (вебхук снял ожидание): сделку не берём, заметку поправляем второй.
            logger.info("ai_engine: перехват %s: менеджер ответил во время заметки, сделку не беру", chat_key)
            try:
                await asyncio.wait_for(amo_service.add_note(deal_id, NOTE_REVOKED), timeout=15)
            except Exception:  # noqa: BLE001
                logger.exception("ai_engine: поправочная заметка в сделке %s не записалась", deal_id)
            return
        owner = await eng._db(store.get_owner, deal_id)
        if owner is not None:                            # пока писали заметку, сделку уже кто-то занял: ничего не меняем
            logger.warning("ai_engine: сделка %s получила владельца (%s) во время перехвата, не беру", deal_id, owner["owner"])
            await eng._db(forget_wait, chat_key)
            return
        mode = settings.effective_mode()
        chat = {"chat_id": chat_id, "chat_type": row["chat_type"], "channel_id": row["channel_id"], "chat_key": chat_key}
        owner = await eng._db(
            store.set_owner, deal_id, store.OWNER_AI, reason="intercept", chat=chat,
            pipeline_id=int(deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(deal), mode=mode)
        await eng._db(give.mark, deal_id, cutoff_at, True)     # отсечка менеджера и «вопреки параллельным сделкам»
        await eng._db(_mark_taken, deal_id, chat_key, eng._now(), cutoff_at)
        await eng._db(store.put_session, chat_key, channel_id=row["channel_id"], chat_id=chat_id, chat_type=row["chat_type"], phone=phone,
                      contact_name=row["contact_name"], state=store.S_AI, deal_id=deal_id, first_batch=0, last_message_id="",
                      next_at=None, has_media=0)
        await eng._queue_event(owner, "deal_intercepted", extra={
            "reason": f"клиент написал, ответа менеджера нет {WAIT_S // 60} минут; заметка «ИИ взял сделку» в сделке уже записана"})
    logger.info("ai_engine: сделка %s перехвачена (чат %s, эпоха %s, режим %s)", deal_id, chat_key, owner["epoch"], mode)


def _responsible(lead: dict) -> int:
    try:
        return int(lead.get("responsible_user_id") or 0)
    except (TypeError, ValueError):
        return 0


async def _skip_reason(eng, deal: dict, chat_id: str, phone: str) -> str:
    deal_id = int(deal["id"])
    if await eng._db(is_taken, deal_id):
        return "сделку уже перехватывали"
    if await eng._db(store.get_owner, deal_id) is not None:
        return "у сделки уже есть владелец в базе движка"
    if not settings.contact_allowed(dedup.contact_ids_of(deal), [phone, chat_id]):
        return "контакта нет в списке режима"
    if eng._autopilot_busy(chat_id, phone, deal_id):
        return "сделку ведёт авто-режим"
    return ""
