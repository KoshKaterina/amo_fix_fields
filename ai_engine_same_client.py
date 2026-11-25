"""Сообщение менеджера из ДРУГОГО чата того же клиента (просьба Кати 25.11.2026: «живой менеджер отправил сообщение» это вмешательство).

Зачем. `ai_engine._on_outbound` отпускал сделку, только если сам чат, куда написал менеджер, вёл ИИ. Менеджер, ответивший в WhatsApp клиенту, с которым ИИ переписывается в Telegram, для движка ничего не делал.

Кто «тот же клиент». Два вида ключей, и они никогда не смешиваются:
  • телефон: последние 10 цифр. Из `chat_id` чатов WhatsApp (`PHONE_CHAT_TYPES`, тот же список, что в панели), из `contact.phone` сообщения и сессии, из полей «Телефон» контактов сделки;
  • Telegram ID: точное равенство. Из `chat_id` чатов Telegram и из поля контакта `TelegramId_WZ` (571839).
Анонимный Telegram ID бывает десятизначным: телефоном он не считается нигде (в том числе когда Wazzup кладёт его же в `contact.phone`). Группы и прочие мессенджеры не участвуют.

Где смотрим. Сначала по базе движка (чаты и сессии сделки): сразу, под тем же замком. Для остальных сделок ИИ по карточкам их контактов из amo (`GET /api/v4/contacts?filter[id][]`), фоновой задачей: без карточек пара
«Telegram и WhatsApp» почти не сходится, Telegram редко отдаёт телефон. Ключи контактов помнятся в памяти `CACHE_TTL_S`; amo не ответила: берётся прежнее значение, непрочитанные контакты пропускаются (лог).
Ключ, совпавший больше чем у `MAGNET_LIMIT` контактов сделок ИИ, никого не снимает (общий или офисный номер).

Действие: как сообщение в чате ИИ (`_to_human`, `manager_message`, то же примечание). Только сделки, которые ИИ взял ДО этого сообщения.

Возврат после «Выключено» (`ai_engine_mode_return`) спрашивает о том же: ключи сообщений людей клиентам запоминаются на трое суток (`manager_out_ident`), в том числе при «Выключено».

Видны только каналы движка (`AI_ENGINE_CHANNELS`) и чаты перехваченных сделок: сообщения с других номеров и аккаунтов движок по-прежнему не видит.

Рубильник `AI_SAME_CLIENT_TAKEOVER=0` в .env и перезапуск контейнера. В лог идут номер сделки и вид ключа, сами телефоны и ID не пишутся.
"""
import asyncio
import logging
from typing import Any, Iterable

import ai_engine_calls as calls
import ai_engine_config as cfg
import ai_engine_filters as filters
import ai_engine_modes as modes
import ai_engine_settings as settings
import ai_engine_store as store
import amo_service
from shared.phones import digits_only

logger = logging.getLogger("uvicorn")

KIND_PHONE, KIND_TELEGRAM = "phone", "telegram"
KIND_WORDS = {KIND_PHONE: "телефон", KIND_TELEGRAM: "Telegram ID"}
PHONE_CHAT_TYPES = frozenset({"whatsapp", "wapi"})      # у этих чатов `chat_id` это телефон; правило панели (`client_events.PHONE_CHAT_TYPES`)
TELEGRAM_CHAT_TYPE = "telegram"
FIELD_TELEGRAM_ID = 571839                              # «TelegramId_WZ» в контакте amo
CACHE_TTL_S = 3600
MAGNET_LIMIT = 5                                        # тот же порог, что у панели
BATCH = 250
AMO_TIMEOUT_S = 15

Ident = tuple[str, str]

_SCHEMA = "CREATE TABLE IF NOT EXISTS manager_out_ident (kind TEXT NOT NULL, value TEXT NOT NULL, at REAL NOT NULL, PRIMARY KEY (kind, value))"

_cache: dict[int, tuple[float, frozenset]] = {}         # контакт -> (когда прочитан, его ключи)
_fetch_lock: asyncio.Lock | None = None


class _Db:
    def __enter__(self):
        self._cm = store._connect()
        conn = self._cm.__enter__()
        conn.execute(_SCHEMA)
        return conn

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


def enabled() -> bool:
    return bool(cfg.SAME_CLIENT_ENABLED)


# =============================================================================================
# ключи клиента
# =============================================================================================

def phone_ident(raw: Any) -> Ident | None:
    key = settings.phone_key(raw)                       # цифры, российская 8 -> 7, короче десяти цифр ключа нет
    return (KIND_PHONE, key[-10:]) if key else None


def chat_idents(chat_type: Any, chat_id: Any, phone: Any = "") -> set[Ident]:
    """Ключи одного чата. Telegram: `chat_id` это только ID, никогда не телефон. Прочие типы (группы и т.п.): ключей нет."""
    kind = str(chat_type or "").strip().lower()
    chat_id, phone = str(chat_id or "").strip(), str(phone or "")
    out: set[Ident] = set()
    if kind in PHONE_CHAT_TYPES:
        found = [phone_ident(chat_id), phone_ident(phone)]
    elif kind == TELEGRAM_CHAT_TYPE:
        if chat_id:
            out.add((KIND_TELEGRAM, chat_id))
        found = [phone_ident(phone)] if digits_only(phone) != digits_only(chat_id) else []
    else:
        return out
    out.update(i for i in found if i)
    return out


def message_idents(m: dict[str, Any]) -> set[Ident]:
    contact = m.get("contact") if isinstance(m.get("contact"), dict) else {}
    return chat_idents(m.get("chatType"), m.get("chatId"), contact.get("phone"))


def contact_idents(contact: dict[str, Any]) -> frozenset:
    """Ключи из карточки контакта amo: все телефоны и `TelegramId_WZ`."""
    out: set[Ident] = set()
    for field in contact.get("custom_fields_values") or []:
        values = [str(v.get("value") or "").strip() for v in field.get("values") or []]
        if field.get("field_code") == "PHONE":
            out.update(i for i in map(phone_ident, values) if i)
        elif field.get("field_id") == FIELD_TELEGRAM_ID:
            out.update((KIND_TELEGRAM, v) for v in values if v)
    return frozenset(out)


def local_idents(rows: list[dict[str, Any]]) -> dict[int, set[Ident]]:
    """Ключи сделок по базе движка: чат записи владельца и все сессии сделки."""
    out = {int(r["deal_id"]): chat_idents(r.get("chat_type"), r.get("chat_id")) for r in rows}
    ids = sorted(out)
    with store._connect() as conn:
        for i in range(0, len(ids), 500):
            chunk = ids[i:i + 500]
            for s in conn.execute(f"SELECT deal_id, chat_type, chat_id, phone FROM chat_session WHERE deal_id IN ({','.join('?' * len(chunk))})", chunk):
                out[int(s["deal_id"])] |= chat_idents(s["chat_type"], s["chat_id"], s["phone"])
    return out


def cached(contact_ids: Iterable[int]) -> set[Ident]:
    out: set[Ident] = set()
    for cid in contact_ids:
        entry = _cache.get(int(cid))
        if entry:
            out |= entry[1]
    return out


async def warm(eng, contact_ids: Iterable[int]) -> bool:
    """Прочитать карточки контактов, которых нет в памяти или чьи ключи устарели. False: amo не ответила (что было в памяти, остаётся)."""
    global _fetch_lock
    if _fetch_lock is None:
        _fetch_lock = asyncio.Lock()
    async with _fetch_lock:
        now = eng._now()
        need = sorted(c for c in {int(c) for c in contact_ids} if c not in _cache or now - _cache[c][0] >= CACHE_TTL_S)
        for i in range(0, len(need), BATCH):
            batch = need[i:i + BATCH]
            try:
                data = await asyncio.wait_for(amo_service._do_get(
                    "/api/v4/contacts", [("filter[id][]", str(c)) for c in batch] + [("limit", str(BATCH))]), timeout=AMO_TIMEOUT_S)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.warning("ai_engine_same_client: контакты не прочитаны (%s)", exc.__class__.__name__)
                return False
            if data is None:
                return False
            got = {int(c["id"]): contact_idents(c) for c in (data.get("_embedded") or {}).get("contacts") or [] if c.get("id") is not None}
            for c in batch:
                _cache[c] = (now, got.get(c, frozenset()))
    return True


# =============================================================================================
# сообщения менеджера для возврата после «Выключено»
# =============================================================================================

def note_manager_out(idents: set[Ident], now: float) -> None:
    if not idents:
        return
    with _Db() as conn:
        for kind, value in idents:
            conn.execute("INSERT INTO manager_out_ident (kind, value, at) VALUES (?,?,?) ON CONFLICT(kind, value) DO UPDATE SET at=excluded.at", (kind, value, float(now)))
        conn.execute("DELETE FROM manager_out_ident WHERE at<?", (float(now) - filters.MANAGER_OUT_KEEP_S,))


def wrote_after(row: dict[str, Any], since: float, contact_ids: Iterable[int] = ()) -> bool:
    """Писал ли менеджер этому клиенту позже `since` в любом видимом чате. Ключи: база движка и карточки, уже прочитанные `warm`."""
    idents = local_idents([row])[int(row["deal_id"])] | cached(calls.contact_ids_of_row(row) | {int(c) for c in contact_ids})
    if not idents:
        return False
    with _Db() as conn:
        return any(conn.execute("SELECT 1 FROM manager_out_ident WHERE kind=? AND value=? AND at>?", (kind, value, float(since))).fetchone()
                   for kind, value in idents)


# =============================================================================================
# снятие
# =============================================================================================

async def _release(eng, deal_id: int, at: float, hit: set[Ident]) -> bool:
    """Снять ИИ со сделки того же клиента. Зовётся под общим замком движка, всё проверяет заново."""
    row = await eng._db(store.get_owner, deal_id)
    if (row is None or row["owner"] != store.OWNER_AI or row["closed"] or modes.is_stale(row, settings.active_mode_id())
            or calls.since_ts(row) > at):
        return False                                        # ИИ взял (получил обратно) сделку уже после этого сообщения
    s = await eng._db(store.session_for_deal, deal_id)
    await eng._to_human(s, "manager_message", deal_id=deal_id, alert=None)
    words = ", ".join(sorted({KIND_WORDS[kind] for kind, _ in hit}))
    logger.info("ai_engine_same_client: сделка %s: менеджер написал клиенту в другом чате (совпал %s), ИИ приостановил ведение", deal_id, words)
    return True


async def _by_contacts(eng, idents: set[Ident], rows: list[dict[str, Any]], at: float) -> None:
    """Сделки, не совпавшие по базе движка: сверка с карточками их контактов. Сеть без общего замка. Не бросает (кроме отмены)."""
    try:
        wanted: set[int] = set()
        for row in rows:
            wanted |= calls.contact_ids_of_row(row)
        if not await warm(eng, wanted):
            logger.warning("ai_engine_same_client: amo не ответила по контактам, сверяю только с уже прочитанными")
        owners: dict[Ident, set[int]] = {}
        for cid in wanted:
            for key in cached([cid]) & idents:
                owners.setdefault(key, set()).add(cid)
        if not owners:
            return
        magnets = {key for key, contacts in owners.items() if len(contacts) > MAGNET_LIMIT}
        if magnets:
            logger.warning("ai_engine_same_client: ключ совпал больше чем у %s контактов, по нему никого не снимаю", MAGNET_LIMIT)
        async with eng._get_lock():
            for row in rows:
                hit = (cached(calls.contact_ids_of_row(row)) & idents) - magnets
                if hit:
                    await _release(eng, int(row["deal_id"]), at, hit)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_same_client: сверка по контактам упала")


async def on_manager_message(eng, m: dict[str, Any]) -> None:
    """Человек написал клиенту в чат, где ИИ сделку не ведёт: снять ИИ с его сделок в других чатах. Зовётся под общим замком движка. Не бросает (кроме отмены)."""
    try:
        if not enabled():
            return
        idents = message_idents(m)
        if not idents:
            return
        at = eng._now()
        active = settings.active_mode_id()
        rows = [r for r in await eng._db(store.list_owned, store.OWNER_AI) if not modes.is_stale(r, active)]
        if not rows:
            return
        local = await eng._db(local_idents, rows)
        rest: list[dict[str, Any]] = []
        for row in rows:
            hit = local.get(int(row["deal_id"]), set()) & idents
            if hit:
                await _release(eng, int(row["deal_id"]), at, hit)
            elif calls.contact_ids_of_row(row):
                rest.append(row)
        if rest:
            eng._spawn(_by_contacts(eng, idents, rest, at))
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_same_client: не проверил другие чаты клиента")


def reset_for_tests() -> None:
    global _fetch_lock
    _fetch_lock = None
    _cache.clear()
