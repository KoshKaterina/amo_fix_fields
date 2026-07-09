"""Общие получатели Telegram-алертов отдела продаж и логика тега ответственного.

Единая точка правды для uis_missed_call (пропущенные звонки) и wazzup_sla
(сообщения без ответа): куда слать (супергруппа ОП, топик РОЗНИЦА) и кого тегать.

Правило тега (одно на оба сценария):
  • ответственный по сделке — наш МОП (есть в WAZZUP_TG_HANDLES) → тегаем ЕГО +
    WAZZUP_ALWAYS_TAG (Саша/Гладков);
  • ответственный не наш МОП / не определён / сделка не найдена → тегаем всю
    смену MANAGERS_ON_SHIFT (Саша в неё уже входит).
"""

from waybill_config import WAZZUP_ALWAYS_TAG, WAZZUP_TG_HANDLES

# Супергруппа ОП «Store [Отдел продаж]», топик РОЗНИЦА (thread 2). None → General.
NOTIFY_CHAT_ID = -1003680811996
NOTIFY_THREAD_ID: int | None = 2

# Вся смена — фолбэк, когда ответственного-МОПа определить не удалось.
# ⚠️ ВРЕМЕННОЕ: фикс.список хендлов. TODO: динамика «кто на смене».
MANAGERS_ON_SHIFT = "@offf1cer @egorkonsss @kathrina_bistraya @gladkov_369"


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
