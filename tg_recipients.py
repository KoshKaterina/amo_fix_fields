"""Общие получатели Telegram-алертов отдела продаж и логика тега ответственного.

Единая точка правды для uis_missed_call (пропущенные звонки) и wazzup_sla
(сообщения без ответа), а также office_transfer / ozon_invoice: куда слать
и кого тегать. Маршрут и тег считаются одинаково — по responsible_user_id.

Правило тега (одно на оба сценария):
  • ответственный по сделке — наш МОП (есть в WAZZUP_TG_HANDLES) → тегаем ЕГО +
    WAZZUP_ALWAYS_TAG (Саша/Гладков);
  • ответственный не наш МОП / не определён / сделка не найдена → тегаем всю
    смену MANAGERS_ON_SHIFT (Саша в неё уже входит).
"""

import logging
import os

from waybill_config import WAZZUP_ALWAYS_TAG, WAZZUP_TG_HANDLES

logger = logging.getLogger("uvicorn")

# Супергруппа ОП «Store [Отдел продаж]», топик РОЗНИЦА (thread 2). None → General.
NOTIFY_CHAT_ID = -1003680811996
NOTIFY_THREAD_ID: int | None = 2

# --- Отдельный маршрут для ОПТ/B2B (решение Кати 01.08.2026) ----------------
# Алерты по сделкам Артёма Коннова уходят в его супергруппу ОПТ, а не в топик
# РОЗНИЦА: рознице они не нужны, а самого Артёма в розничном топике тегать
# бесполезно — он там не читает.
#
# chat_id/thread берутся из ссылки на ЛЮБОЕ сообщение нужной группы:
#   t.me/c/<N>/<msg>            → chat_id = -100<N>, thread = None (обычная группа)
#   t.me/c/<N>/<thread>/<msg>   → chat_id = -100<N>, thread = <thread> (форум)
# Так же в своё время сняли id топика РОЗНИЦА (t.me/c/3680811996/2/5994).
# Пока id не задан, алерты Артёма идут по общему маршруту — молча не теряем.
OPT_MANAGER_IDS = {13822630}   # Артём Коннов (B2B, воронка ОПТ 10131762)


def _env_int(name: str) -> int | None:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("tg_recipients: %s=%r — не число, игнорирую", name, raw)
        return None


OPT_CHAT_ID: int | None = _env_int("TG_OPT_CHAT_ID")
OPT_THREAD_ID: int | None = _env_int("TG_OPT_THREAD_ID")

_warned_no_opt_chat = False


def route_for(responsible_id) -> tuple[int | None, int | None]:
    """(chat_id, message_thread_id), куда слать алерт по ответственному сделки.

    Ответственный из OPT_MANAGER_IDS и задан TG_OPT_CHAT_ID → супергруппа ОПТ.
    Иначе — общий маршрут (ОП / РОЗНИЦА). Если id группы ОПТ не настроен,
    сознательно отдаём общий маршрут: лучше алерт «не туда», чем никуда."""
    global _warned_no_opt_chat
    try:
        rid = int(responsible_id) if responsible_id is not None else None
    except (TypeError, ValueError):
        rid = None
    if rid in OPT_MANAGER_IDS:
        if OPT_CHAT_ID is not None:
            return OPT_CHAT_ID, OPT_THREAD_ID
        if not _warned_no_opt_chat:
            _warned_no_opt_chat = True
            logger.warning(
                "tg_recipients: TG_OPT_CHAT_ID не задан — алерты ОПТ (user %s) "
                "идут в общий чат ОП/РОЗНИЦА", rid,
            )
    return NOTIFY_CHAT_ID, NOTIFY_THREAD_ID


# Вся смена — фолбэк, когда ответственного-МОПа определить не удалось.
# ⚠️ ВРЕМЕННОЕ: фикс.список хендлов. TODO: динамика «кто на смене».
MANAGERS_ON_SHIFT = "@offf1cer @egorkonsss @kathrina_bistraya @gladkov_369"

# Доп. тег ТОЛЬКО для алертов о пропущенных звонках (не для wazzup SLA):
# @thebarsa1 (Игорь) подмешиваем ТОЛЬКО в фолбэке — когда ответственного-МОПа
# определить не удалось (нет сделки / тех.аккаунт / нет хендла). На живого
# ответственного (в т.ч. самого Игоря на его сделках) его не добавляем.
MISSED_CALL_FALLBACK_TAG = "@thebarsa1"


def mentions_for(responsible_id) -> str:
    """Строка @-тегов для алерта по ответственному сделки.
    Наш МОП → «@его @gladkov_369»; иначе → вся смена (в ней Гладков уже есть)."""
    handle = None
    try:
        handle = WAZZUP_TG_HANDLES.get(int(responsible_id)) if responsible_id is not None else None
    except (TypeError, ValueError):
        handle = None
    if not handle:
        return MANAGERS_ON_SHIFT
    parts = [handle]
    if WAZZUP_ALWAYS_TAG and WAZZUP_ALWAYS_TAG != handle:
        parts.append(WAZZUP_ALWAYS_TAG)
    return " ".join(parts)


def missed_call_mentions(responsible_id) -> str:
    """Теги для алерта о ПРОПУЩЕННОМ звонке.
    Ответственный — живой МОП → тегаем только его (mentions_for), Игоря не
    подмешиваем. Ответственного нет / тех.аккаунт / нет хендла → base == вся
    смена, тогда дополнительно тегаем @thebarsa1."""
    base = mentions_for(responsible_id)
    if base == MANAGERS_ON_SHIFT and MISSED_CALL_FALLBACK_TAG not in base.split():
        return f"{base} {MISSED_CALL_FALLBACK_TAG}"
    return base
