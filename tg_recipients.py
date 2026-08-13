"""Общие получатели Telegram-алертов отдела продаж и логика тега ответственного.

Единая точка правды для uis_missed_call (пропущенные звонки) и wazzup_sla
(сообщения без ответа): куда слать (супергруппа ОП, топик РОЗНИЦА) и кого тегать.

Правило тега (одно на оба сценария):
  • ответственный по сделке — наш МОП (есть в WAZZUP_TG_HANDLES) → тегаем ЕГО
    (плюс WAZZUP_ALWAYS_TAG, если он не пуст);
  • ответственный не наш МОП / не определён / сделка не найдена → тегаем всю
    смену MANAGERS_ON_SHIFT.

⚠️ 13.08.2026 надзорный тег снят: WAZZUP_ALWAYS_TAG пуст, Саша (РОП) убран и из
смены, и из карты ответственных — ушёл в отпуск и попросил убрать его из
уведомлений. Возвращать будем не тегом РОПа, а задачей про точки контроля.
"""

from waybill_config import WAZZUP_ALWAYS_TAG, WAZZUP_TG_HANDLES

# Супергруппа ОП «Store [Отдел продаж]», топик УВЕДОМЛЕНИЯ (thread 10479). None → General.
NOTIFY_CHAT_ID = -1003680811996
NOTIFY_THREAD_ID: int | None = 10479

# Вся смена — фолбэк, когда ответственного-МОПа определить не удалось.
# ⚠️ ВРЕМЕННОЕ: фикс.список хендлов. TODO: динамика «кто на смене».
# 13.08.2026: @gladkov_369 (Саша, РОП) убран — отпуск, его же просьба.
MANAGERS_ON_SHIFT = "@offf1cer @egorkonsss @kathrina_bistraya"

# Доп. тег ТОЛЬКО для алертов о пропущенных звонках (не для wazzup SLA):
# @thebarsa1 (Игорь) подмешиваем ТОЛЬКО в фолбэке — когда ответственного-МОПа
# определить не удалось (нет сделки / тех.аккаунт / нет хендла). На живого
# ответственного (в т.ч. самого Игоря на его сделках) его не добавляем.
MISSED_CALL_FALLBACK_TAG = "@thebarsa1"

# Алерт «новый заказ с самовывозом → записать в шоурум» (showroom_alert).
# Та же супергруппа ОП, но СВОЙ топик ШОУРУМ и один адресат — Катя-офис.
# Топик «Магазин, ШОУРУМ» = thread 4083 (снято 07.08.2026 из адреса веб-телеграма
# web.telegram.org/a/#-1003680811996_4083). В закрепе топика ветка адресована
# @offf1cer и @kathrina_bistraya — оба в супергруппе, тег уведомит.
# None → алерт НЕ шлётся (в General сыпать не будем), в логе — предупреждение.
SHOWROOM_ALERT_THREAD_ID: int | None = 4083
SHOWROOM_ALERT_TAG = "@kathrina_bistraya"


def mentions_for(responsible_id) -> str:
    """Строка @-тегов для алерта по ответственному сделки.
    Наш МОП → «@его» (+ WAZZUP_ALWAYS_TAG, если он задан); иначе → вся смена."""
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
