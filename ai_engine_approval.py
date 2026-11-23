"""Подтверждение человеком смены ответственного: «Перераспределить существующие» при включённом пилоте (этап 2, решение Кати 23.11.2026).

Модель. Подтверждает человек во вкладке «Пилот» панели Team, исполняет движок. Клиент пишет в существующую сделку, режим Полный, `existing.policy =
redistribute`, фильтры, бюджет и «менеджер не активен» прошли:
  • пилот выключен: ответственный назначается сразу (`ai_engine_filters.reassign_responsible`), как раньше;
  • пилот включён: вместо отказа создаётся ПРЕДЛОЖЕНИЕ (`propose`). Сделка «ждёт решения»: ИИ в ней не отвечает и не берёт её (отвечает менеджер, как
    было), в amo не пишется ничего; чат переходит в состояние `awaiting`, сообщения клиента копятся во входящих, чтобы ход по ним ушёл после «да».

Источник правды: таблица `reassign_proposal` в sqlite движка (`ref`, `deal_id`, `from_user_*`, `to_user_*`, `reason`, `proposed_at`, `state`
`pending|approved|rejected|stale|failed|cancelled`, `decided_by`, `decided_at`, `note`; плюс служебные: чат клиента для хода и учёт доставки в панель).
Открытое предложение это `pending` или `failed`; на сделку одно открытое (частичный уникальный индекс). После отказа новое предложение по сделке не
создаётся 24 часа (`REJECT_COOLDOWN_S`).

Панель получает предложение `POST <AI_PANEL_URL>/api/ingest/ai/reassign-proposal` (`X-Ingest-Token`, как у `engine-event`), тело (контракт с панелью, не менять):
`{"ref", "deal_id", "from_user": {"id", "name"} | null, "to_user": {"id", "name"}, "reason": "existing_deal_redistribute", "at": "<UTC ISO>"}`; ответ
200 `{"ok": true, "duplicate": bool}`, повторы по `ref` панель отсекает сама. Личных данных клиента в теле нет. Доставка надёжнее, чем у события снятия:
пока предложение `pending` и не доставлено, повторы через 1, 5, 25 секунд, потом раз в минуту; 404, 401, 403 и прочие 4xx повторяются редко (раз в 10
минут: панель может быть ещё не выкачена); в лог одна запись на вид отказа не чаще раза в 10 минут. Рестарт ничего не теряет (состояние в базе).

Решение человека: `POST /internal/ai/v1/reassign/decision` (`ai_engine_api`), узкий токен `X-Give-Token`, тело `{"ref", "deal_id", "decision":
"approve"|"reject", "decided_by"}`, ответ 200 `{"ok": true, "state": "approved"|"rejected"|"stale"|"failed", "detail": "<по-русски>"}`.
  • reject: `rejected`, сделку не берём, в amo ничего не пишем;
  • approve: проверка актуальности (предложение открыто, режим Полный и `redistribute` не сменили, аварийный рубильник не включён, менеджер не писал
    клиенту после предложения, сделка открыта и по ней нет владельца в базе движка, ответственный не сменён кем-то ещё, фильтры, бюджет и товары
    всё ещё проходят), затем назначение (PATCH, примечание «... подтвердил <имя>», строка `reassign_log`), `owner = ai`, охрана инициативы и ход по
    сообщениям клиента как по первому. Не прошла проверка: `stale` (предложение закрывается). Не удалась запись в amo, amo или ИИ-сервис не отвечают:
    `failed`, предложение остаётся открытым, панель может нажать ещё раз;
  • повторное решение по закрытому предложению: 200 с текущим state, без действий (идемпотентность); `cancelled` отдаётся как `stale`.

Устаревание само (`sweep`, раз в проход таймеров): режим сменили или выключили, `redistribute` выключили или поменяли, включён аварийный рубильник
`AI_REASSIGN_DISABLED=1`, менеджер написал клиенту после предложения (`stale`), по сделке появился владелец в базе движка; закрытая сделка снимает
предложение событием сделки из amo (`cancel_for_closed_deal`). Пока настройки панели протухли, ничего не снимается.

Источник предложения (`source`, только в базе, в панель не уходит): пусто, если ИИ просит взять сделку по сообщению клиента, `sweep`, если по подбору существующих
сделок (`ai_engine_sweep`, решение Кати 23.11.2026). У подобранной сделки во входящих обычно ничего нет (клиент давно молчит или ждёт ответа в прошлом), поэтому после
подтверждения ИИ получает не ход по накопленным сообщениям, а событие `deal_given`, как при кнопке «Дать сделку ИИ» (`ai_engine._hand_over`): он сам читает историю чата.
Если клиент написал, пока предложение ждало решения, всё как обычно: ход по накопленным сообщениям.
"""
import asyncio
import datetime
import html
import logging
import os
import time
from typing import Any

import httpx

import ai_engine_config as cfg
import ai_engine_dedup as dedup
import ai_engine_events as panel_events
import ai_engine_filters as filters
import ai_engine_marks as marks
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from api import BASE_URL

logger = logging.getLogger("uvicorn")

PATH = "/api/ingest/ai/reassign-proposal"
TIMEOUT_S = 10.0
REASON = "existing_deal_redistribute"
REJECT_COOLDOWN_S = 24 * 3600
RETRY_DELAYS_S = (1, 5, 25)              # первые повторы доставки; дальше раз в минуту
RETRY_EVERY_S = 60
REFUSED_EVERY_S = 600                    # 404, 401, 403 и прочие 4xx: панель, видимо, не выкачена, стучимся редко
AMO_TIMEOUT_S = 15

SOURCE_SWEEP = "sweep"                  # предложение создал подбор существующих сделок (`ai_engine_sweep`); у предложений по сообщению клиента источник пуст
PENDING, APPROVED, REJECTED, STALE, FAILED, CANCELLED = "pending", "approved", "rejected", "stale", "failed", "cancelled"
OPEN_STATES = (PENDING, FAILED)
AWAITING = "awaiting"                    # состояние чата: ждём решения человека (сообщения клиента копятся, ход не ставится)

# Подмена транспорта для тестов (None: настоящая сеть, под pytest её нет, см. `may_send`).
_transport: httpx.AsyncBaseTransport | None = None
SENT, RETRY, REFUSED = "sent", "retry", "refused"

_sending: set[str] = set()               # предложения, которые сейчас отправляются
_deciding: set[str] = set()              # предложения, по которым сейчас выполняется решение человека
_last_orphans = 0.0


# =============================================================================================
# хранилище
# =============================================================================================

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS reassign_proposal ("
    "ref TEXT PRIMARY KEY, deal_id INTEGER NOT NULL, from_user_id INTEGER, from_user_name TEXT NOT NULL DEFAULT '', "
    "to_user_id INTEGER NOT NULL, to_user_name TEXT NOT NULL, reason TEXT NOT NULL, proposed_at REAL NOT NULL, state TEXT NOT NULL, "
    "decided_by TEXT, decided_at REAL, note TEXT NOT NULL DEFAULT '', "
    "chat_key TEXT NOT NULL DEFAULT '', chat_id TEXT NOT NULL DEFAULT '', chat_type TEXT NOT NULL DEFAULT '', channel_id TEXT NOT NULL DEFAULT '', "
    "phone TEXT NOT NULL DEFAULT '', contact_name TEXT NOT NULL DEFAULT '', "
    "delivered_at REAL, delivery_tries INTEGER NOT NULL DEFAULT 0, delivery_next_at REAL NOT NULL DEFAULT 0);"
    "CREATE INDEX IF NOT EXISTS ix_reassign_deal ON reassign_proposal (deal_id, proposed_at);"
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_reassign_open ON reassign_proposal (deal_id) WHERE state IN ('pending','failed')"
)


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.executescript(_SCHEMA)
        if "source" not in {row[1] for row in conn.execute("PRAGMA table_info(reassign_proposal)")}:
            conn.execute("ALTER TABLE reassign_proposal ADD COLUMN source TEXT NOT NULL DEFAULT ''")      # таблица из прежнего выката без колонки: идемпотентно, откат кода безопасен
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get(ref: str) -> dict[str, Any] | None:
    with _Db() as conn:
        row = conn.execute("SELECT * FROM reassign_proposal WHERE ref=?", (str(ref),)).fetchone()
    return dict(row) if row else None


def open_for_deal(deal_id: int) -> dict[str, Any] | None:
    with _Db() as conn:
        row = conn.execute("SELECT * FROM reassign_proposal WHERE deal_id=? AND state IN ('pending','failed')", (int(deal_id),)).fetchone()
    return dict(row) if row else None


def list_open() -> list[dict[str, Any]]:
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM reassign_proposal WHERE state IN ('pending','failed') ORDER BY proposed_at").fetchall()]


def count_open() -> int:
    with _Db() as conn:
        return conn.execute("SELECT COUNT(*) FROM reassign_proposal WHERE state IN ('pending','failed')").fetchone()[0]


def journal(limit: int = 50) -> list[dict[str, Any]]:
    """Последние предложения, свежее первым: для проверки руками и тестов."""
    with _Db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM reassign_proposal ORDER BY proposed_at DESC, ref DESC LIMIT ?", (int(limit),)).fetchall()]


def block_reason(deal_id: int, now: float) -> str:
    """Почему новое предложение по сделке создавать нельзя: пустая строка, если можно. Открытое предложение: ждём его. После отказа 24 часа."""
    with _Db() as conn:
        row = conn.execute("SELECT ref FROM reassign_proposal WHERE deal_id=? AND state IN ('pending','failed')", (int(deal_id),)).fetchone()
        if row:
            return f"ждёт решения человека (предложение {row['ref']})"
        rej = conn.execute("SELECT decided_at FROM reassign_proposal WHERE deal_id=? AND state='rejected' ORDER BY decided_at DESC LIMIT 1",
                           (int(deal_id),)).fetchone()
    if rej and rej["decided_at"] is not None and now - float(rej["decided_at"]) < REJECT_COOLDOWN_S:
        return "предложение сменить ответственного отклонено, новое не раньше чем через 24 часа"
    return ""


def recent_deal_ids(since: float) -> list[int]:
    """Сделки, по которым предложения смены ответственного создавались не раньше `since` (любое состояние): подбор существующих (`ai_engine_sweep`) не предлагает
    их снова, чтобы отклонённое человеком не возвращалось и открытое не дублировалось."""
    with _Db() as conn:
        return [int(r[0]) for r in conn.execute("SELECT DISTINCT deal_id FROM reassign_proposal WHERE proposed_at>=?", (float(since),))]


def create(*, deal_id: int, from_id: int | None, from_name: str, to_id: int, to_name: str, chat: dict[str, Any], now: float, source: str = "") -> dict[str, Any] | None:
    """Новое предложение `pending`. None: по сделке уже есть открытое. `ref` вида `rp-<сделка>-<unix>` (при совпадении секунды с суффиксом)."""
    with _Db() as conn:
        if conn.execute("SELECT 1 FROM reassign_proposal WHERE deal_id=? AND state IN ('pending','failed')", (int(deal_id),)).fetchone():
            return None
        base = f"rp-{int(deal_id)}-{int(now)}"
        ref, n = base, 1
        while conn.execute("SELECT 1 FROM reassign_proposal WHERE ref=?", (ref,)).fetchone():
            n += 1
            ref = f"{base}-{n}"
        conn.execute(
            "INSERT INTO reassign_proposal (ref, deal_id, from_user_id, from_user_name, to_user_id, to_user_name, reason, proposed_at, state, "
            "chat_key, chat_id, chat_type, channel_id, phone, contact_name, delivery_next_at, source) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (ref, int(deal_id), from_id, from_name or "", int(to_id), to_name, REASON, float(now), PENDING,
             chat.get("chat_key") or "", chat.get("chat_id") or "", chat.get("chat_type") or "", chat.get("channel_id") or "",
             chat.get("phone") or "", chat.get("contact_name") or "", float(now), source or ""))
        return dict(conn.execute("SELECT * FROM reassign_proposal WHERE ref=?", (ref,)).fetchone())


def transition(ref: str, state: str, *, decided_by: str | None = None, note: str = "", now: float | None = None,
               from_states: tuple[str, ...] = OPEN_STATES) -> bool:
    """Перевести открытое предложение в новое состояние одним шагом. False: оно уже не в `from_states` (решение принято раньше)."""
    marks = ",".join("?" * len(from_states))
    with _Db() as conn:
        cur = conn.execute(
            f"UPDATE reassign_proposal SET state=?, decided_by=COALESCE(?, decided_by), decided_at=?, note=? WHERE ref=? AND state IN ({marks})",
            (state, decided_by, float(now if now is not None else time.time()), str(note)[:300], str(ref), *from_states))
        return cur.rowcount == 1


def wire(row: dict[str, Any]) -> dict[str, Any]:
    """Тело для панели (контракт зафиксирован, имена не менять). Личных данных клиента нет."""
    from_id = row.get("from_user_id")
    return {
        "ref": row["ref"], "deal_id": int(row["deal_id"]),
        "from_user": {"id": int(from_id), "name": row.get("from_user_name") or f"пользователь {from_id}"} if from_id else None,
        "to_user": {"id": int(row["to_user_id"]), "name": row["to_user_name"]},
        "reason": row["reason"], "at": _iso(row["proposed_at"]),
    }


# =============================================================================================
# доставка в панель
# =============================================================================================

def may_send() -> bool:
    """Можно ли сейчас слать в панель. Под pytest без подставного транспорта нельзя: настоящей сети в тестах нет."""
    if not panel_events.configured():
        return False
    if _transport is None and os.getenv("PYTEST_CURRENT_TEST"):
        return False
    return settings.active_mode_id() != "off"


async def _post(body: dict[str, Any]) -> tuple[str, int, str]:
    url = f"{cfg.PANEL_URL}{PATH}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_S, transport=_transport) as http:
            resp = await http.post(url, json=body, headers={"X-Ingest-Token": cfg.PANEL_TOKEN})
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001 - сеть, таймаут, что угодно: попытка не удалась
        return RETRY, 0, f"{type(exc).__name__}: {str(exc)[:120]}"
    code = resp.status_code
    if 200 <= code < 300:
        return SENT, code, ""
    if code in (408, 429) or code >= 500:
        return RETRY, code, f"HTTP {code}"
    return REFUSED, code, f"HTTP {code}"


def _due_for_delivery(now: float) -> list[dict[str, Any]]:
    with _Db() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM reassign_proposal WHERE state='pending' AND delivered_at IS NULL AND delivery_next_at<=? ORDER BY proposed_at", (float(now),)).fetchall()]


def _mark_delivered(ref: str, now: float) -> None:
    with _Db() as conn:
        conn.execute("UPDATE reassign_proposal SET delivered_at=?, delivery_tries=delivery_tries+1 WHERE ref=?", (float(now), ref))


def _mark_failed_attempt(ref: str, now: float, refused: bool) -> int:
    """Попытка не удалась: следующая через 1, 5, 25 секунд, потом раз в минуту; отказ панели (404, 401, 403, прочие 4xx) раз в 10 минут."""
    with _Db() as conn:
        row = conn.execute("SELECT delivery_tries FROM reassign_proposal WHERE ref=?", (ref,)).fetchone()
        tries = (row["delivery_tries"] if row else 0) + 1
        if refused:
            delay = REFUSED_EVERY_S
        else:
            delay = RETRY_DELAYS_S[tries - 1] if tries <= len(RETRY_DELAYS_S) else RETRY_EVERY_S
        conn.execute("UPDATE reassign_proposal SET delivery_tries=?, delivery_next_at=? WHERE ref=?", (tries, float(now) + delay, ref))
    return tries


async def _deliver(eng, ref: str) -> None:
    try:
        row = await eng._db(get, ref)
        if row is None or row["state"] != PENDING or row["delivered_at"] is not None or not may_send():
            return
        kind, status, detail = await _post(wire(row))
        now = eng._now()
        if kind == SENT:
            await eng._db(_mark_delivered, ref, now)
            return
        await eng._db(_mark_failed_attempt, ref, now, kind == REFUSED)
        if kind == REFUSED and status == 404:
            panel_events._log("reassign_404", logging.WARNING,
                              "ai_engine_approval: панель ответила 404 на %s (ручка ещё не выкачана?): предложения копятся и повторяются редко (сделка %s)", PATH, row["deal_id"])
        elif kind == REFUSED:
            panel_events._log(f"reassign_http_{status}", logging.WARNING,
                              "ai_engine_approval: панель отклонила предложение (%s): повтор через %s мин (сделка %s)", detail, REFUSED_EVERY_S // 60, row["deal_id"])
        else:
            panel_events._log("reassign_unavailable", logging.WARNING,
                              "ai_engine_approval: панель недоступна (%s), предложение по сделке %s отправим снова", detail, row["deal_id"])
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - сбой одного предложения не должен остановить остальные
        logger.exception("ai_engine_approval: сбой отправки предложения %s в панель", ref)
    finally:
        _sending.discard(ref)


async def _schedule_delivery(eng, now: float) -> None:
    if not may_send():
        return
    for row in await eng._db(_due_for_delivery, now):
        if row["ref"] in _sending:
            continue
        _sending.add(row["ref"])
        eng._spawn(_deliver(eng, row["ref"]))


# =============================================================================================
# создание предложения
# =============================================================================================

async def propose(eng, s: dict[str, Any], deal: dict[str, Any], gate, mode_id: str, source: str = "") -> bool:
    """Сделку брать нужно со сменой ответственного, а пилот включён: создать предложение и поставить сделку «ждёт решения». Зовётся под общим замком из
    `ai_engine._apply_resolution` (и из `ai_engine_sweep` с `source=SOURCE_SWEEP`), после проверок «ИИ жив» и «не медиа». Сообщения клиента остаются во входящих.
    True: предложение создано."""
    now = eng._now()
    plan = gate.reassign
    deal_id = int(deal["id"])
    from_id = plan.get("from_id")
    from_name = await filters._user_name(from_id) if from_id else ""
    row = await eng._db(create, deal_id=deal_id, from_id=from_id, from_name=from_name, to_id=int(plan["to_id"]), to_name=str(plan["to_name"]),
                        chat=s, now=now, source=source)
    if row is None:
        # Открытое предложение по сделке уже есть (второй путь к тому же решению): новое не создаём, чат ждёт, как и прежний
        await eng._db(store.put_session, s["chat_key"], state=AWAITING, deal_id=deal_id, next_at=None)
        return False
    await eng._db(store.put_session, s["chat_key"], state=AWAITING, deal_id=deal_id, next_at=None)
    logger.info("ai_engine: сделка %s: ИИ просит разрешения сменить ответственного, предложение %s ждёт решения человека", deal_id, row["ref"])
    link = f'<a href="{BASE_URL}/leads/detail/{deal_id}">{deal_id}</a>'
    old = html.escape(from_name or ("без ответственного" if not from_id else f"пользователь {from_id}"))
    await eng._alert("reassign_proposal", [
        f"ИИ просит разрешения взять сделку {link}: сменить ответственного с {old} на {html.escape(str(plan['to_name']))}. "
        "Подтвердите во вкладке «Пилот» панели Team."], dedupe=f"reassign_proposal:{row['ref']}", ping=False)
    return True


# =============================================================================================
# устаревание
# =============================================================================================

def _stale_reason(row: dict[str, Any]) -> tuple[str, str] | None:
    """Устарело ли открытое предложение без сети: (новое состояние, причина) или None. Протухшие настройки ничего не снимают."""
    mark = marks.get(int(row["deal_id"]))                         # отметки людей (`ai_engine_marks`) от настроек панели не зависят
    if mark is not None and (mark["kind"] == marks.KIND_REMOVED or float(mark["at"]) >= float(row["proposed_at"])):
        return CANCELLED, "сделку убрали отметкой человека" + (" («удалить»)" if mark["kind"] == marks.KIND_REMOVED else " («не требует ответа»)")
    if not settings.is_fresh():
        return None
    if settings.effective_mode() == "off" or settings.active_mode_id() != "full":
        return CANCELLED, "режим ИИ сменили или выключили"
    ex = filters.existing_config()
    if ex is None or ex["policy"] != "redistribute":
        return CANCELLED, "«перераспределить существующие» выключили"
    if filters.reassign_disabled():
        return CANCELLED, "смена ответственного выключена аварийным флагом AI_REASSIGN_DISABLED"
    last = filters.manager_last_out(row["chat_key"])
    if last is not None and last > float(row["proposed_at"]):
        return STALE, "менеджер написал клиенту после предложения"
    if store.get_owner(int(row["deal_id"])) is not None:
        return CANCELLED, "по сделке уже есть владелец в базе движка"
    return None


async def _finish_session(eng, row: dict[str, Any], now: float) -> None:
    """Предложение закрыто без взятия сделки: чат из ожидания уходит в пропуск, накопленные сообщения выбрасываются (отвечает менеджер, как было)."""
    s = await eng._db(store.get_session, row["chat_key"]) if row.get("chat_key") else None
    if s is not None and s["state"] == AWAITING:
        await eng._db(store.put_session, row["chat_key"], state=store.S_SKIP, skip_until=now + cfg.SKIP_TTL_S, next_at=None)
        await eng._db(store.discard_pending, row["chat_key"])


async def _close(eng, row: dict[str, Any], state: str, note: str, now: float) -> bool:
    if await eng._db(transition, row["ref"], state, note=note, now=now):
        logger.info("ai_engine: предложение %s по сделке %s закрыто (%s): %s", row["ref"], row["deal_id"], state, note)
        await _finish_session(eng, row, now)
        return True
    return False


async def sweep(eng, now: float) -> None:
    rows = await eng._db(list_open)
    for row in rows:
        why = _stale_reason(row)
        if why is None:
            continue
        async with eng._get_lock():
            cur = await eng._db(get, row["ref"])
            if cur is not None and cur["state"] in OPEN_STATES and cur["ref"] not in _deciding:      # идущее подтверждение само проверит актуальность
                await _close(eng, cur, why[0], why[1], now)


async def cancel_for_deal(eng, deal_id: int, note: str) -> bool:
    """Открытое предложение по сделке снимается решением человека или движка (отметки «удалить» и «не требует ответа», `ai_engine.mark_deal`). Идущее подтверждение
    не трогаем: оно само проверит актуальность (`_stale_reason` видит отметку). True: предложение закрыто."""
    row = await eng._db(open_for_deal, int(deal_id))
    if row is not None and row["ref"] not in _deciding:
        return await _close(eng, row, CANCELLED, note, eng._now())
    return False


async def cancel_for_closed_deal(eng, deal_id: int) -> None:
    """Сделку закрыли (событие сделки из amo): открытое предложение по ней снимается."""
    await cancel_for_deal(eng, deal_id, "сделка закрыта")


async def _fix_orphans(eng, now: float) -> None:
    """Чат в ожидании без открытого предложения (предложение закрыли, а чат не успели отпустить): отпустить, иначе ИИ молчал бы в нём дольше нужного."""
    for s in await eng._db(store.sessions_by_state, AWAITING):
        deal_id = s.get("deal_id")
        if deal_id is not None and await eng._db(open_for_deal, int(deal_id)) is not None:
            continue
        await eng._db(store.put_session, s["chat_key"], state=store.S_SKIP, skip_until=now + cfg.SKIP_TTL_S, next_at=None)
        await eng._db(store.discard_pending, s["chat_key"])


async def tick(eng, now: float) -> None:
    """Раз в проход таймеров: устаревание, доставка в панель, чаты в ожидании без предложения. Без открытых предложений это один лёгкий запрос."""
    global _last_orphans
    if await eng._db(count_open):
        await sweep(eng, now)
        await _schedule_delivery(eng, now)
    if now - _last_orphans >= 60:
        _last_orphans = now
        await _fix_orphans(eng, now)


# =============================================================================================
# решение человека
# =============================================================================================

_STATE_OUT = {APPROVED: "approved", REJECTED: "rejected", STALE: "stale", CANCELLED: "stale", FAILED: "failed", PENDING: "failed"}
_DONE_TEXT = {
    APPROVED: "Решение уже принято: ответственный изменён, ИИ ведёт сделку.",
    REJECTED: "Решение уже принято: предложение отклонено, сделка остаётся у менеджеров.",
    STALE: "Предложение устарело",
    CANCELLED: "Предложение устарело",
}


def _answer(row: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Ответ по уже закрытому предложению: текущее состояние, без действий (повторное решение безопасно)."""
    state = row["state"]
    detail = _DONE_TEXT.get(state, "Предложение ещё открыто")
    if state in (STALE, CANCELLED) and row.get("note"):
        detail = f"{detail}: {row['note']}"
    return 200, {"ok": True, "state": _STATE_OUT.get(state, state), "detail": detail}


def _out(state: str, detail: str) -> tuple[int, dict[str, Any]]:
    return 200, {"ok": True, "state": state, "detail": detail}


async def decide(eng, ref: str, deal_id: int, decision: str, decided_by: str) -> tuple[int, dict[str, Any]]:
    """Решение человека по предложению (`/internal/ai/v1/reassign/decision`). Возвращает (HTTP-код, тело)."""
    if decision not in ("approve", "reject"):
        return 422, {"detail": "bad_decision"}
    if not ref or len(ref) > 64:
        return 404, {"detail": "unknown_proposal"}
    by = " ".join(str(decided_by or "").split())[:80] or "не указан"
    row = await eng._db(get, ref)
    if row is None:
        return 404, {"detail": "unknown_proposal"}
    if int(row["deal_id"]) != int(deal_id):
        return 409, {"detail": "deal_mismatch"}
    if row["state"] not in OPEN_STATES:
        return _answer(row)                                         # идемпотентность: решение уже принято
    if ref in _deciding:
        return 409, {"detail": "in_progress"}                        # то же предложение сейчас решается другим запросом
    _deciding.add(ref)
    try:
        return await (_reject(eng, row, by) if decision == "reject" else _approve(eng, row, by))
    finally:
        _deciding.discard(ref)


async def _reject(eng, row: dict[str, Any], by: str) -> tuple[int, dict[str, Any]]:
    async with eng._get_lock():
        cur = await eng._db(get, row["ref"])
        if cur is None or cur["state"] not in OPEN_STATES:
            return _answer(cur or row)
        now = eng._now()
        await eng._db(transition, row["ref"], REJECTED, decided_by=by, note="отклонено человеком", now=now)
        await _finish_session(eng, cur, now)
    logger.info("ai_engine: предложение %s по сделке %s отклонено (%s)", row["ref"], row["deal_id"], by)
    return _out("rejected", "Предложение отклонено: сделка остаётся у менеджеров, новое по ней не раньше чем через 24 часа.")


async def _stale(eng, row: dict[str, Any], state: str, note: str, by: str) -> tuple[int, dict[str, Any]]:
    """Предложение при проверке перед исполнением оказалось неактуальным: закрыть."""
    async with eng._get_lock():
        cur = await eng._db(get, row["ref"])
        if cur is not None and cur["state"] in OPEN_STATES:
            now = eng._now()
            await eng._db(transition, row["ref"], state, decided_by=by, note=note, now=now)
            await _finish_session(eng, cur, now)
            logger.info("ai_engine: предложение %s по сделке %s устарело при подтверждении: %s", row["ref"], row["deal_id"], note)
        else:
            return _answer(cur or row)
    return _out("stale", f"Предложение устарело: {note}.")


async def _failed(eng, row: dict[str, Any], note: str, detail: str, by: str) -> tuple[int, dict[str, Any]]:
    """Исполнить не вышло по временной причине: предложение остаётся открытым (`failed`), панель может нажать ещё раз."""
    async with eng._get_lock():
        cur = await eng._db(get, row["ref"])
        if cur is None or cur["state"] not in OPEN_STATES:
            return _answer(cur or row)
        await eng._db(transition, row["ref"], FAILED, decided_by=by, note=note, now=eng._now())
    logger.warning("ai_engine: предложение %s по сделке %s не исполнено: %s", row["ref"], row["deal_id"], note)
    return _out("failed", detail)


def _precheck(row: dict[str, Any]) -> tuple[str, str] | None:
    """Проверка актуальности без сети. Настройки протухли: это сбой, а не устаревание (повторить позже)."""
    if not settings.is_fresh():
        return FAILED, "настройки панели устарели"
    return _stale_reason(row)


async def _approve(eng, row: dict[str, Any], by: str) -> tuple[int, dict[str, Any]]:
    deal_id = int(row["deal_id"])
    why = _precheck(row)
    if why is not None:
        if why[0] == FAILED:
            return await _failed(eng, row, why[1], "Настройки панели устарели, попробуйте через минуту.", by)
        return await _stale(eng, row, why[0], why[1], by)
    user = filters.ai_user()
    if user is None or int(user["id"]) != int(row["to_user_id"]):
        return await _stale(eng, row, CANCELLED, "пользователь ИИ в настройках изменился", by)
    try:
        lead = await asyncio.wait_for(amo_service.get_lead_full(deal_id, with_=("contacts",)), timeout=AMO_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: сделка %s не прочиталась при подтверждении", deal_id)
        lead = None
    if not lead:
        return await _failed(eng, row, "amo не ответила", "amoCRM не ответила, попробуйте ещё раз.", by)
    if not dedup.is_open(lead):
        return await _stale(eng, row, CANCELLED, "сделка закрыта", by)
    if not settings.pipeline_allowed(lead.get("pipeline_id")):
        # Пока предложение ждало человека, воронка сделки лишилась промпта (или режим перестал её пускать): ИИ в ней не пишет, ответственного не меняем.
        return await _stale(eng, row, STALE, "воронка сделки больше не ведётся ИИ", by)
    try:
        current = int(lead.get("responsible_user_id") or 0)
    except (TypeError, ValueError):
        current = 0
    if current not in (int(row["from_user_id"] or 0), int(row["to_user_id"])):
        return await _stale(eng, row, STALE, "ответственного уже сменили", by)
    explicit = settings.contacts_restricted() and settings.contact_allowed(dedup.contact_ids_of(lead), [row["phone"], row["chat_id"]])
    try:
        refusal = await filters.recheck_for_approval(lead, explicit=explicit)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: перепроверка фильтров по сделке %s не удалась", deal_id)
        return await _failed(eng, row, "сбой перепроверки фильтров", "Не удалось перепроверить сделку, попробуйте ещё раз.", by)
    if refusal:
        return await _stale(eng, row, STALE, f"сделка больше не подходит ({refusal})", by)

    async with eng._get_lock():
        cur = await eng._db(get, row["ref"])
        if cur is None or cur["state"] not in OPEN_STATES:
            return _answer(cur or row)
        why = _stale_reason(cur)                                     # пока ходили в amo и МойСклад, мог написать менеджер или смениться режим
        if why is not None:
            now = eng._now()
            await eng._db(transition, row["ref"], why[0], decided_by=by, note=why[1], now=now)
            await _finish_session(eng, cur, now)
            return _out("stale", f"Предложение устарело: {why[1]}.")
        if eng._autopilot_busy(row["chat_id"], row["phone"], deal_id):
            now = eng._now()
            await eng._db(transition, row["ref"], STALE, decided_by=by, note="сделку ведёт авто-режим", now=now)
            await _finish_session(eng, cur, now)
            return _out("stale", "Предложение устарело: сделку ведёт авто-режим.")
        if not await eng._ai_is_up():
            await eng._db(transition, row["ref"], FAILED, decided_by=by, note="ИИ-сервис не отвечает", now=eng._now())
            return _out("failed", "ИИ-сервис не отвечает, попробуйте позже.")
        if current != int(row["to_user_id"]):
            plan = {"to_id": int(row["to_user_id"]), "to_name": row["to_user_name"], "from_id": current or None}
            if not await filters.reassign_responsible(deal_id, plan, confirmed_by=by):
                await eng._db(transition, row["ref"], FAILED, decided_by=by, note="не удалась запись ответственного в amo", now=eng._now())
                return _out("failed", "Не удалось записать ответственного в amoCRM, попробуйте ещё раз.")
        now = eng._now()
        mode_id = settings.active_mode_id()
        chat = {"chat_id": row["chat_id"], "chat_type": row["chat_type"], "channel_id": row["channel_id"], "chat_key": row["chat_key"]}
        await eng._db(eng.silence.clear, deal_id)
        swept = row.get("source") == SOURCE_SWEEP
        pending = bool(await eng._db(store.pending_inbox, row["chat_key"]))
        if swept and not pending:
            # Предложение создал подбор существующих сделок, клиент с тех пор не писал: хода по сообщениям нет, ИИ получает `deal_given` и сам читает историю
            # (как после кнопки «Дать сделку ИИ»). Задачи amo при этом не закрываются (подбор их не трогает).
            await eng._db(transition, row["ref"], APPROVED, decided_by=by, note="подтверждено человеком", now=now)
            await eng._hand_over(lead, chat=chat, phone=row["phone"], contact_name=row["contact_name"], mode_id=mode_id, forced=False,
                                 reason="reassign_approved", note=eng.sweep.SWEEP_NOTE, release_tasks=False)
        else:
            owner = await eng._db(store.set_owner, deal_id, store.OWNER_AI, reason="reassign_approved", chat=chat,
                                  pipeline_id=int(lead.get("pipeline_id") or 0), contact_ids=dedup.contact_ids_of(lead), mode=mode_id)
            await eng._db(filters.guard_on_take, deal_id)               # существующая сделка: инициатива ждёт первого сообщения ИИ клиенту
            await eng._db(transition, row["ref"], APPROVED, decided_by=by, note="подтверждено человеком", now=now)
            s = await eng._db(store.put_session, row["chat_key"], channel_id=row["channel_id"], chat_id=row["chat_id"], chat_type=row["chat_type"],
                              phone=row["phone"], contact_name=row["contact_name"], state=store.S_COLLECTING, deal_id=deal_id,
                              batch_start=now, first_batch=1, last_client_at=now, has_media=0, next_at=None, skip_until=None)
            await eng._enqueue_turn(s, owner, settings.effective_mode())
            if not swept:
                eng._spawn(eng.ai_engine_tasks.release_intake(deal_id))     # системные «Связаться»/«Новый заказ с сайта»: теперь клиентом занят ИИ
    logger.info("ai_engine: предложение %s по сделке %s подтверждено (%s): ответственный изменён, ИИ взял сделку", row["ref"], deal_id, by)
    if swept and not pending:
        return _out("approved", "Ответственный изменён, ИИ взял сделку, перечитает переписку и продолжит диалог.")
    return _out("approved", "Ответственный изменён, ИИ взял сделку и отвечает клиенту." if pending
                else "Ответственный изменён, ИИ взял сделку и ответит на следующее сообщение клиента.")
