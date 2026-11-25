"""Перехват сделок менеджера (ВРЕМЕННО, решение Кати 12.11.2026, доработка того же дня).

Правило: клиент написал в сделке выбранного менеджера, и на его сообщение нет ответа человека N минут рабочего времени ИИ (N из панели,
1-240, по умолчанию 15). Дальше ИИ вмешивается в одном из двух режимов (настройка панели `intercept.mode`):

  • `take` («Взять сделку»): серверная заметка «ИИ взял сделку», сделка переходит к ИИ (`owner = ai`), ИИ консультирует и отвечает сразу на
    следующие сообщения клиента, пока менеджер не напишет. Одна сделка берётся один раз (таблица `intercept_taken`, kind=take).
  • `reply` («Отвечать только при молчании»): серверная заметка «ИИ ответил вместо менеджера», ИИ отвечает ОДИН раз. Владелец на время хода
    ставится (иначе нет `permit`), после итога хода владелец и чат удаляются: сделка остаётся у менеджера, как и была. Следующий ответ ИИ даёт, только
    если на следующее сообщение клиента снова нет ответа человека за то же время.

Чьи сделки: пользователи amo из панели (`managers`, по умолчанию Игорь Оанча 9291546). Для воронки «Офис» (9421022) вместо ответственного смотрим
кастомное поле «Ответственный МОП» (578151, текст с именем пользователя amo ровно как в `/api/v4/users`, например «Оанча Игорь»), сравнение по набору слов без
учёта регистра и «ё». «Офис» трогаем только при включённом `office` (по умолчанию выключено).

Сделки со звонками (`with_calls`, по умолчанию выключено, то есть НЕ трогаем): звонок это примечание `call_in`/`call_out` в карточке КОНТАКТА сделки,
созданное не раньше создания сделки минус 10 минут (на сделке такие примечания не лежат: сверено по 40 сделкам Игоря, звонки только у контактов).

Отказ ИИ после ответа менеджера (оба режима): перед заметкой берётся отсечка; сообщение человека после неё гасит ответ ИИ (`permit` движка отказывает,
шлюз сервиса видит `manager_cutoff`). Заметка и отсечка не зависят от режима проверки человеком: это действие движка, не модели.

Прочие решения (подробнее в журнале): ответом считается только человек (автор не `Admin`/`bot`/пусто); срок идёт по рабочему времени ИИ и от ПЕРВОГО
неотвеченного сообщения; несколько подходящих сделок у клиента: не берём ни одну; не берём сделки с владельцем в базе движка, закрытые и те, чей чат уже
ведёт обычный вход ИИ.

Удаление: этот файл, вызовы `intercept.*` в `ai_engine.py`, параметр `any_pipeline_ids` в `ai_engine_dedup.py`, `intercept_*` в `ai_engine_settings.py` и
`test_ai_engine_intercept.py`. Таблицы создаются при первом обращении (`IF NOT EXISTS`) в той же sqlite, что у `ai_engine_store`.
"""
import asyncio
import datetime
import logging
import re
import sqlite3
from typing import Any

import ai_engine_dedup as dedup
import ai_engine_filters as filters
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from shared.timez import MSK
from waybill_config import FIELD_FORMER_RESPONSIBLE, PIPELINE_OFFICE

logger = logging.getLogger("uvicorn")

# Сколько раз пробуем довести перехват (amo не ответила, заметка не записалась, ИИ-сервис лежит), прежде чем бросить.
MAX_TRIES = 20
RETRY_S = 60
PER_TICK = 3
CHAT_TYPES = frozenset({"whatsapp", "telegram"})        # группы и прочие мессенджеры не берём
KIND_TAKE, KIND_REPLY = "take", "reply"
EVENT_TAKE, EVENT_REPLY = "deal_intercepted", "silence_reply"
CALL_NOTE_TYPES = frozenset({"call_in", "call_out"})
CALL_SLACK_S = 600                                       # звонок, из-за которого создали сделку, мог лечь на несколько секунд раньше сделки
REPLY_STALE_S = 1800                                     # ход «ответа вместо менеджера» без итога дольше этого срока закрывается сам
OFFICE_PIPELINE = int(PIPELINE_OFFICE)
FIELD_MOP = int(FIELD_FORMER_RESPONSIBLE)                # «Ответственный МОП» (text)

NOTE_TAKE = "ИИ взял сделку: клиент написал, ответа менеджера нет {minutes} минут. Дальше клиента консультирует ИИ-менеджер."
NOTE_REPLY = ("ИИ ответил клиенту вместо менеджера: клиент написал, ответа менеджера нет {minutes} минут. "
              "Сделка остаётся у менеджера; на следующее сообщение клиента ИИ ответит снова, только если менеджер опять промолчит.")
NOTE_REVOKED = "ИИ клиенту не отвечал: пока писалась прошлая заметка, менеджер ответил."

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
        try:
            conn.execute("ALTER TABLE intercept_taken ADD COLUMN kind TEXT NOT NULL DEFAULT 'take'")     # таблица из первого выката без колонки
        except sqlite3.OperationalError:
            pass
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


# =============================================================================================
# настройки, рабочее время, имена
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


def name_key(value: Any) -> frozenset[str]:
    """Имя для сравнения: слова без регистра и «ё», порядок не важен («Оанча Игорь» и «Игорь Оанча» одно и то же)."""
    words = re.split(r"[\s,.;:()\-]+", str(value or "").casefold().replace("ё", "е"))
    return frozenset(w for w in words if w)


def lead_belongs_to(lead: dict, cfg: dict[str, Any]) -> bool:
    """Сделка выбранного менеджера. «Офис»: только при включённом `office` и по полю «Ответственный МОП»; остальные воронки: по ответственному."""
    managers = cfg["managers"]
    try:
        pipeline = int(lead.get("pipeline_id") or 0)
    except (TypeError, ValueError):
        pipeline = 0
    if pipeline == OFFICE_PIPELINE:
        if not cfg["office"]:
            return False
        key = name_key(amo_service.get_custom_field_value(lead, FIELD_MOP))
        return bool(key) and any(key == name_key(m["name"]) for m in managers)
    try:
        return int(lead.get("responsible_user_id") or 0) in {int(m["id"]) for m in managers}
    except (TypeError, ValueError):
        return False


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
    """Человек написал клиенту: ожидание снято, а у сделки в перехвате запомнено время сообщения (для отказа в `permit`)."""
    with _Db() as conn:
        conn.execute("DELETE FROM intercept_wait WHERE chat_key=?", (chat_key,))
        conn.execute("UPDATE intercept_taken SET manager_out_at=MAX(COALESCE(manager_out_at, 0), ?) WHERE chat_key=?",
                     (float(sent_ts), chat_key))


def is_taken(deal_id: int) -> bool:
    """Сделка в перехвате: взята ИИ насовсем (take) или ИИ сейчас отвечает за менеджера (reply)."""
    with _Db() as conn:
        return conn.execute("SELECT 1 FROM intercept_taken WHERE deal_id=?", (int(deal_id),)).fetchone() is not None


def kind_of(deal_id: int) -> str | None:
    with _Db() as conn:
        row = conn.execute("SELECT kind FROM intercept_taken WHERE deal_id=?", (int(deal_id),)).fetchone()
        return row["kind"] if row else None


def is_tracked(chat_key: str) -> bool:
    """Чат сделки, взятой насовсем: сообщения из него разбираем, даже если канал не входит в `AI_ENGINE_CHANNELS`. Для режима reply чат не отслеживается."""
    with _Db() as conn:
        return conn.execute("SELECT 1 FROM intercept_taken WHERE chat_key=? AND kind='take'", (chat_key,)).fetchone() is not None


def cutoff_iso(deal_id: int) -> str | None:
    """Момент перед заметкой: сообщения менеджера до него ИИ не останавливают, после останавливают (`manager_cutoff` хода)."""
    with _Db() as conn:
        row = conn.execute("SELECT cutoff_at FROM intercept_taken WHERE deal_id=?", (int(deal_id),)).fetchone()
    if not row:
        return None
    return datetime.datetime.fromtimestamp(float(row["cutoff_at"]), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def manager_replied_after(deal_id: int) -> bool:
    """После заметки менеджер написал клиенту: ответ ИИ не отправляем."""
    with _Db() as conn:
        row = conn.execute("SELECT cutoff_at, manager_out_at FROM intercept_taken WHERE deal_id=?", (int(deal_id),)).fetchone()
    return bool(row and row["manager_out_at"] is not None and row["manager_out_at"] > row["cutoff_at"])


def rearm(deal_id: int, now: float) -> None:
    """Человек вернул сделку ИИ примечанием «старт»: прежние сообщения менеджера ИИ больше не останавливают. Без этого `permit` отказывал бы вечно, а ход уходил бы со старой отсечкой.
    Взятая насовсем сделка получает новую отсечку; строка «ответа вместо менеджера» убирается: дальше это обычная сделка ИИ, и уборка не должна удалить её владельца."""
    with _Db() as conn:
        conn.execute("DELETE FROM intercept_taken WHERE deal_id=? AND kind='reply'", (int(deal_id),))
        conn.execute("UPDATE intercept_taken SET cutoff_at=?, manager_out_at=NULL WHERE deal_id=?", (float(now), int(deal_id)))


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


def _due_rows(now: float, hours: list[dict[str, str]], wait_s: int) -> list[dict[str, Any]]:
    with _Db() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM intercept_wait WHERE retry_at<=? ORDER BY first_at", (now,))]
    out = []
    for r in rows:
        due = due_at(r["first_at"], wait_s, hours)
        if due is not None and due <= now:
            out.append(r)
    return out[:PER_TICK]


def _mark_taken(deal_id: int, chat_key: str, now: float, cutoff_at: float, kind: str) -> None:
    with _Db() as conn:
        conn.execute("INSERT OR REPLACE INTO intercept_taken (deal_id, chat_key, taken_at, cutoff_at, manager_out_at, kind) VALUES (?,?,?,?,NULL,?)",
                     (int(deal_id), chat_key, now, cutoff_at, kind))
        conn.execute("DELETE FROM intercept_wait WHERE chat_key=?", (chat_key,))


def _drop_reply(deal_id: int) -> None:
    with _Db() as conn:
        conn.execute("DELETE FROM intercept_taken WHERE deal_id=? AND kind='reply'", (int(deal_id),))


def _stale_replies(now: float) -> list[dict[str, Any]]:
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM intercept_taken WHERE kind='reply' AND taken_at<?", (now - REPLY_STALE_S,))]


def _delete_owner(deal_id: int) -> None:
    with store._connect() as conn:
        conn.execute("DELETE FROM deal_owner WHERE deal_id=?", (int(deal_id),))


# =============================================================================================
# звонки
# =============================================================================================

async def has_calls(lead: dict) -> bool | None:
    """Есть ли по сделке звонки: примечания `call_in`/`call_out` в карточках контактов сделки (до трёх), созданные не раньше создания сделки
    минус `CALL_SLACK_S`. None: amo не ответила (решить нельзя, повтор позже)."""
    try:
        since = int(lead.get("created_at") or 0) - CALL_SLACK_S
    except (TypeError, ValueError):
        since = 0
    for cid in dedup.contact_ids_of(lead)[:3]:
        data = await amo_service._do_get(f"/api/v4/contacts/{cid}/notes", [("limit", "100"), ("order[id]", "desc")])
        if data is None:
            return None
        for note in (data.get("_embedded") or {}).get("notes") or []:
            if note.get("note_type") in CALL_NOTE_TYPES and int(note.get("created_at") or 0) >= since:
                return True
    return False


# =============================================================================================
# перехват
# =============================================================================================

async def tick(eng) -> None:
    """Один проход: чаты, у которых вышел срок ожидания, и уборка зависших ходов режима reply. `eng` - модуль `ai_engine`."""
    now = eng._now()
    await _gc_replies(eng, now)
    if not enabled() or not settings.send_window()["open_now"]:
        return                                           # перехват только в окне отправки: ответ ИИ вне окна всё равно не уйдёт
    cfg = settings.intercept_config()
    rows = await eng._db(_due_rows, now, settings.work_hours(), cfg["wait_min"] * 60)
    for row in rows:
        try:
            await _process(eng, row, now, cfg)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: ошибка перехвата чата %s", row.get("chat_key"))
            await eng._db(_retry, row["chat_key"], now, "exception")


async def _process(eng, row: dict[str, Any], now: float, cfg: dict[str, Any]) -> None:
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
    mine = [ld for ld in leads if dedup.is_open(ld) and lead_belongs_to(ld, cfg)]
    if not mine:
        await eng._db(forget_wait, chat_key)             # сделка не выбранного менеджера (или закрыта, или «Офис» выключен): не наше дело
        return
    if len(mine) > 1:
        logger.warning("ai_engine: перехват %s: у клиента несколько открытых сделок выбранных менеджеров %s, не беру ни одну",
                       chat_key, [int(ld["id"]) for ld in mine])
        await eng._db(forget_wait, chat_key)
        return
    deal = mine[0]
    deal_id = int(deal["id"])
    kind = KIND_REPLY if cfg["mode"] == "reply" else KIND_TAKE
    why = await _skip_reason(eng, deal, chat_id, phone, cfg, kind)
    if why == "retry":
        await eng._db(_retry, chat_key, now, "amo не ответила на звонки")
        return
    if why:
        logger.info("ai_engine: перехват сделки %s пропущен: %s", deal_id, why)
        await eng._db(forget_wait, chat_key)
        return
    if not await eng._ai_is_up():
        await eng._db(_retry, chat_key, now, "ИИ-сервис не отвечает")
        return

    # Отсечка берётся ДО заметки: сообщение менеджера, написанное после неё (а значит и в промежутке), останавливает ИИ.
    cutoff_at = eng._now()
    minutes = cfg["wait_min"]
    text = (NOTE_REPLY if kind == KIND_REPLY else NOTE_TAKE).format(minutes=minutes)
    try:
        await asyncio.wait_for(amo_service.add_note(deal_id, text), timeout=15)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: заметка перехвата в сделке %s не записалась, сделку не беру", deal_id)
        await eng._db(_retry, chat_key, now, "заметка не записалась")
        return

    async with eng._get_lock():
        if not await eng._db(has_wait, chat_key):
            # Пока писалась заметка, менеджер ответил клиенту (вебхук снял ожидание): сделку не берём, заметку поправляем второй.
            logger.info("ai_engine: перехват %s: менеджер ответил во время заметки, ИИ не вмешивается", chat_key)
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
        mode = settings.active_mode_id()        # режим сделки (`ai_engine_modes`): в нём она и остаётся у ИИ
        chat = {"chat_id": chat_id, "chat_type": row["chat_type"], "channel_id": row["channel_id"], "chat_key": chat_key}
        owner = await eng._db(
            store.set_owner, deal_id, store.OWNER_AI, reason="intercept" if kind == KIND_TAKE else "intercept_reply", chat=chat,
            pipeline_id=int(deal.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(deal), mode=mode)
        await eng._db(_mark_taken, deal_id, chat_key, eng._now(), cutoff_at, kind)
        await eng._db(store.put_session, chat_key, channel_id=row["channel_id"], chat_id=chat_id, chat_type=row["chat_type"], phone=phone,
                      contact_name=row["contact_name"], state=store.S_AI, deal_id=deal_id, first_batch=0, last_message_id="",
                      next_at=None, has_media=0)
        await eng._queue_event(owner, EVENT_TAKE if kind == KIND_TAKE else EVENT_REPLY, extra={
            "reason": f"клиент написал, ответа менеджера нет {minutes} минут; заметка о вмешательстве ИИ в сделке уже записана"})
    logger.info("ai_engine: сделка %s: перехват (%s), чат %s, эпоха %s, режим %s", deal_id, kind, chat_key, owner["epoch"], mode)


async def _skip_reason(eng, deal: dict, chat_id: str, phone: str, cfg: dict[str, Any], kind: str) -> str:
    deal_id = int(deal["id"])
    if await eng._db(is_taken, deal_id):
        return "сделку уже перехватывали" if kind == KIND_TAKE else "ИИ уже отвечает по сделке"
    if await eng._db(store.get_owner, deal_id) is not None:
        return "у сделки уже есть владелец в базе движка"
    if not settings.contact_allowed(dedup.contact_ids_of(deal), [phone, chat_id]):
        return "контакта нет в списке режима"
    if eng._autopilot_busy(chat_id, phone, deal_id):
        return "сделку ведёт авто-режим"
    why = await filters.intercept_refusal(deal, [phone, chat_id])      # фильтры, бюджет и товары, как при обычном взятии (`ai_engine_filters`)
    if why:
        return why
    if not cfg["with_calls"]:
        calls = await has_calls(deal)
        if calls is None:
            return "retry"
        if calls:
            return "по сделке есть звонки"
    return ""


# =============================================================================================
# режим reply: после хода сделка возвращается менеджеру
# =============================================================================================

async def finish_reply(eng, deal_id: int) -> None:
    """Ход «ответа вместо менеджера» закончился: удалить временного владельца и чат, сделка остаётся у менеджера, как была. Зовётся под замком движка."""
    if await eng._db(kind_of, deal_id) != KIND_REPLY:
        return
    owner = await eng._db(store.get_owner, deal_id)
    if owner is not None and owner["owner"] == store.OWNER_AI:          # причина владельца после повторного разбора чата уже "client_message", признак режима - строка перехвата
        s = await eng._db(store.session_for_deal, deal_id)
        for t in await eng._db(store.cancel_active_turns, deal_id):
            logger.info("ai_engine: перехват reply: активный ход %s по сделке %s снят при уборке", t.get("turn_id"), deal_id)
        await eng._db(_delete_owner, deal_id)
        if s is not None:
            await eng._db(store.drop_session, s["chat_key"])
    await eng._db(_drop_reply, deal_id)


async def _gc_replies(eng, now: float) -> None:
    """Ход режима reply завис без итога (сервис лежал): через полчаса временный владелец убирается сам."""
    for row in await eng._db(_stale_replies, now):
        try:
            async with eng._get_lock():
                if await eng._db(store.active_turn_for_deal, int(row["deal_id"])) is None:
                    await finish_reply(eng, int(row["deal_id"]))
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            logger.exception("ai_engine: уборка перехвата reply по сделке %s не удалась", row.get("deal_id"))
