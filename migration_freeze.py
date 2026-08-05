"""Заморозка сделок на время миграции воронок (решение Кати 03.08.2026).

Задача: почистить amoCRM от лишних воронок. Перед удалением воронки её сделки
переносятся в основную — и каждый такой перенос выглядит для наших интеграций
как обычная продажа: office_transfer тащит сделку дальше в Офис/Фулфилмент,
Метрика получает конверсию, Woo-заказ уезжает в completed, гейт КОНТРОЛЬ идёт
проверять склад. На боевом тесте 03.08 одно движение этапа подняло 4 системы.

Решение: скрипт переноса вешает каждой перенесённой сделке тег
`MIGRATION_FREEZE_TAG` («перенесено из старой воронки» — название для
менеджеров, им потом с этими сделками работать). Пока идёт окно
[FROM, TO], наши обработчики такие сделки не трогают вовсе.

Важно: заморозка ВРЕМЕННАЯ. После окна тег остаётся на сделке (менеджер видит,
откуда она взялась), но блокировка снимается — сделка работает как обычная.
Совсем отключать нельзя: с перенесёнными потом работают.

Что тег НЕ останавливает (это не наш код, живёт в Цифровой воронке amo):
  - виджет выгрузки в МойСклад (871693) — двигает статус заказа на складе;
  - боты 7167 «Калькуляция ОП» и 7213.
Их на время переноса снимают с этапа руками, если у переносимых сделок есть
поля МойСклада.
"""

import logging
import time

import amo_service
from waybill_config import (
    MIGRATION_BULK_PAUSE,
    MIGRATION_FREEZE_FROM_TS,
    MIGRATION_FREEZE_TAG,
    MIGRATION_FREEZE_TO_TS,
    MIGRATION_SOURCE_PIPELINES,
    PIPELINE_CLEVER_MAIN,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
)

logger = logging.getLogger("uvicorn")  # канон проекта: хендлеры только у uvicorn-логгера

# lead_id → (когда проверили, заморожена ли). Вебхуки на одну сделку прилетают
# пачкой (перенос + правка полей + эхо нашего же PATCH), дочитывать её каждый
# раз незачем.
_cache: dict[int, tuple[float, bool]] = {}
_CACHE_TTL_S = 300
_CACHE_MAX = 20000


def window_active(now: float | None = None) -> bool:
    """Идёт ли сейчас окно миграции. Без тега или без границы TO — выключено."""
    if not MIGRATION_FREEZE_TAG or not MIGRATION_FREEZE_TO_TS:
        return False
    now = time.time() if now is None else now
    return MIGRATION_FREEZE_FROM_TS <= now <= MIGRATION_FREEZE_TO_TS


def bulk_skip(pipeline_id, status_id, now: float | None = None) -> bool:
    """Точечная отсечка вебхуков на время МАССОВОГО прогона (легаси-воронка).

    Обычный гейт по тегу дочитывает каждую сделку — на ста тысячах это упирается
    в лимит amo и очередь разгребается часами. Поэтому на время прогона гасим
    ТОЛЬКО тот поток, который прогон и создаёт, не читая сделку:
      • вебхуки из воронок-источников (первая волна, простановка тега);
      • вебхуки о заходе в 142/143 основной воронки (вторая волна, сам перенос).

    Всё остальное обрабатывается штатно: накладные СДЭК, счёт Ozon, гейт КОНТРОЛЬ,
    новые заказы. Боевые сделки, закрытые в эти минуты, не теряются — их подберёт
    reconciliation office_transfer (раз в 2 минуты) и сверки Метрики и Woo.

    Флаг действует только ВНУТРИ окна миграции: забыть выключить и остаться без
    обработки невозможно.
    """
    if not (MIGRATION_BULK_PAUSE and window_active(now)):
        return False
    try:
        pid = int(pipeline_id) if pipeline_id is not None else None
        sid = int(status_id) if status_id is not None else None
    except (TypeError, ValueError):
        return False

    if pid in MIGRATION_SOURCE_PIPELINES:
        return True
    return pid == PIPELINE_CLEVER_MAIN and sid in (STATUS_SUCCESS, STATUS_CLOSED_LOST)


def is_bulk_move_event(before_pipeline_id, now: float | None = None) -> bool:
    """Событие входа в 142/143 создано МАССОВЫМ ПРОГОНОМ, а не менеджером?

    Отличаем по тому, ОТКУДА сделка приехала: прогон тащит её из воронки-
    источника, а живая продажа закрывается внутри основной воронки. Это поле
    (`value_before`) уже лежит в ответе `/api/v4/events`, который reconciliation
    и так читает, — дочитывать сделку ради тега не нужно. Именно дочитывание
    съедало лимит amo 04.08 (537 запросов за 5 минут).

    Осторожность намеренная: миграцией считаем ТОЛЬКО приезд из перечисленных
    в MIGRATION_SOURCE_PIPELINES воронок. Ручной перевод менеджером из любой
    другой воронки останется боевым и будет обработан.

    ⚠️ Новую воронку в перенос — сразу в MIGRATION_SOURCE_PIPELINES, иначе её
    поток пойдёт в обычную обработку и опять начнёт есть лимит.
    """
    if not (MIGRATION_BULK_PAUSE and window_active(now)):
        return False
    try:
        pid = int(before_pipeline_id) if before_pipeline_id is not None else None
    except (TypeError, ValueError):
        return False
    return pid in MIGRATION_SOURCE_PIPELINES


async def is_frozen(lead_id, lead: dict | None = None) -> bool:
    """Сделка заморожена на время миграции?

    lead передан → тег смотрим в нём (лишнего запроса нет). Иначе дочитываем
    сделку: amo не шлёт теги в вебхук (та же причина, что в unmiss_tag).
    """
    if not window_active():
        return False
    if lead is not None:
        return amo_service.has_tag(lead, MIGRATION_FREEZE_TAG)

    try:
        lid = int(lead_id)
    except (TypeError, ValueError):
        return False

    now = time.time()
    hit = _cache.get(lid)
    if hit is not None and now - hit[0] < _CACHE_TTL_S:
        return hit[1]

    fetched = await amo_service.get_lead_full(lid, with_=())
    frozen = bool(fetched) and amo_service.has_tag(fetched, MIGRATION_FREEZE_TAG)
    if len(_cache) > _CACHE_MAX:
        _cache.clear()
    _cache[lid] = (now, frozen)
    return frozen


async def skip(lead_id, where: str, lead: dict | None = None) -> bool:
    """is_frozen + строка в лог. Возвращает True, если обработчику надо выйти."""
    if not await is_frozen(lead_id, lead):
        return False
    logger.info(
        "%s: сделка %s с тегом «%s» — окно миграции, пропускаем",
        where, lead_id, MIGRATION_FREEZE_TAG,
    )
    return True
