"""Статусы заказа МойСклад → этап КОПИИ в воронке Фулфилмент.

Зачем. Интеграция amgroup привязана к ОРИГИНАЛУ сделки (CLEVER) своей внутренней
привязкой и при смене статуса заказа тащит ОРИГИНАЛ по этапам обрабатывающей
воронки — а копию в Фулфилменте, которая для этого и нужна, не ведёт.
Переключить amgroup на копию снаружи нельзя (проверено: «Ссылку на сделку»
он игнорирует). Поэтому КОПИЮ ведём сами: читаем статус заказа МойСклад и
ставим ФФ-копии тот же этап. Оригинал не трогаем (он замирает в 142 после
обнуления строк воронки Фулфилмент в настройках amgroup — отдельный ручной шаг).

Канал — фоновый ОПРОС (вебхуки МС капризные, не используем):
  раз в MS_SYNC_POLL_INTERVAL_S берём заказы, изменённые в окне, и
  досинхронизируем ФФ-копии. Идемпотентно: не трогаем, если этап уже верный.
  Окно перекрывается (interval + буфер), первый проход — глубже (lookback).

Маппинг МС-статус → этап ФФ: по совпадению ИМЁН («00. Обрабатывается» →
этап «00. Обрабатывается», и т.д.), плюс «Завершён» → «УР». Этап «КОНТРОЛЬ»
складом не управляется (в МС такого статуса нет) — копию в нём не двигаем,
пока склад не перейдёт в один из маппящихся статусов.

Затрагивает ТОЛЬКО Фулфилмент: заказы Офиса (и прочих) пропускаются — у них
нет копии в воронке Фулфилмент, а офисные статусы МС сюда не маппятся.
"""

import asyncio
import datetime
import logging

import amo_service
import ms_client
from waybill_config import (
    FIELD_MOYSKLAD_ORDER_UUID,
    MS_SYNC_LOOKBACK_MIN,
    MS_SYNC_POLL_INTERVAL_S,
    MS_TOKEN,
    PIPELINE_FULFILLMENT,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
)

logger = logging.getLogger("uvicorn")
_MSK = datetime.timezone(datetime.timedelta(hours=3))

_enabled = False
_poll_task: asyncio.Task | None = None
_ms_state_name: dict[str, str] = {}   # uuid статуса заказа МС → имя
_seen: dict[str, str] = {}            # id заказа МС → последний обработанный 'updated' (дедуп окна)
_first_poll = True


def is_enabled() -> bool:
    return _enabled


async def _alert(text: str) -> None:
    logger.error(text)
    try:
        from telegram_bot import send_alert
        await send_alert(text)
    except Exception:
        logger.exception("MS sync alert failed: %s", text)


async def init() -> None:
    """Требует уже прогретый amo_service.warm_pipeline_cache()."""
    global _enabled, _poll_task
    if not MS_TOKEN:
        logger.warning("MS sync: MS_TOKEN не задан — синхронизация ВЫКЛЮЧЕНА")
        return

    ms_client.init()
    meta = await ms_client.get("entity/customerorder/metadata")
    if not meta or "states" not in meta:
        await _alert("MS sync: не получил метаданные статусов заказа МС — ВЫКЛЮЧЕНА")
        await ms_client.aclose()
        return
    for s in meta.get("states") or []:
        if s.get("id"):
            _ms_state_name[s["id"]] = (s.get("name") or "").strip()

    # Санити-чек: видим ли этапы ФФ по именам
    probe = amo_service.resolve_status_id_by_name(PIPELINE_FULFILLMENT, "00. Обрабатывается")
    if probe is None:
        logger.warning(
            "MS sync: в воронке Фулфилмент (%s) не нашёл этап «00. Обрабатывается» — "
            "проверь имена этапов/кэш воронок", PIPELINE_FULFILLMENT,
        )

    _enabled = True
    logger.info(
        "MS sync включён: %s статусов заказа МС, опрос каждые %sс (lookback %s мин)",
        len(_ms_state_name), MS_SYNC_POLL_INTERVAL_S, MS_SYNC_LOOKBACK_MIN,
    )
    _poll_task = asyncio.create_task(_poll_loop())


async def shutdown() -> None:
    global _enabled, _poll_task
    _enabled = False
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
    _seen.clear()
    await ms_client.aclose()
    logger.info("MS sync stopped")


# ---------------------------------------------------------------------------
# Маппинг и поиск
# ---------------------------------------------------------------------------

def _ff_stage_for(ms_status_name: str | None) -> int | None:
    """Имя статуса заказа МС → status_id этапа Фулфилмента (или None, если складом
    этот статус ФФ-копию не двигает)."""
    if not ms_status_name:
        return None
    name = ms_status_name.strip()
    if name.lower() in ("завершён", "завершен"):
        for cand in ("УР", "Успешно реализовано"):
            sid = amo_service.resolve_status_id_by_name(PIPELINE_FULFILLMENT, cand)
            if sid is not None:
                return sid
        return None
    return amo_service.resolve_status_id_by_name(PIPELINE_FULFILLMENT, name)


def _state_uuid(order: dict) -> str | None:
    href = ((order.get("state") or {}).get("meta") or {}).get("href", "")
    return href.rstrip("/").split("/")[-1] if href else None


async def _find_ff_copies(ms_order_id: str) -> list[dict]:
    """ФФ-копии заказа: сделки с полем 576689 == id заказа МС И воронка Фулфилмент."""
    copies: dict[int, dict] = {}
    for lead in await amo_service.find_leads_by_query(ms_order_id):
        if lead.get("pipeline_id") != PIPELINE_FULFILLMENT:
            continue
        val = amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID)
        if val is not None and str(val).strip() == str(ms_order_id):
            lid = lead.get("id")
            if lid is not None:
                copies[int(lid)] = lead
    if len(copies) > 1:
        logger.warning("MS sync: по заказу %s найдено %s ФФ-копий: %s",
                       ms_order_id, len(copies), sorted(copies))
    return list(copies.values())


def _stage_name(status_id) -> str:
    info = amo_service._status_info.get((PIPELINE_FULFILLMENT, status_id))
    return (info or {}).get("name", str(status_id))


async def _move_copy(copy: dict, target_status_id: int, ms_status_name: str) -> bool:
    cid = copy.get("id")
    # Финализированные копии не трогаем (Успешно реализовано / Закрыто и не
    # реализовано) — иначе движение склада «оживляло» бы закрытые сделки,
    # напр. закрытые вручную дубли с причиной «дубль».
    if copy.get("status_id") in (STATUS_SUCCESS, STATUS_CLOSED_LOST):
        return False
    if copy.get("status_id") == target_status_id:
        return False  # уже на месте — идемпотентно
    res = await amo_service.patch_lead(cid, status_id=target_status_id, pipeline_id=PIPELINE_FULFILLMENT)
    if res.get("ok"):
        logger.info("MS sync: копия %s «%s» → «%s» (склад: «%s»)",
                    cid, _stage_name(copy.get("status_id")), _stage_name(target_status_id), ms_status_name)
        return True
    logger.error("MS sync: не удалось двинуть копию %s → «%s»: %s", cid, ms_status_name, res)
    return False


async def _process_order(order: dict) -> None:
    oid = order.get("id")
    if not oid:
        return
    name = _ms_state_name.get(_state_uuid(order) or "")
    target = _ff_stage_for(name)
    if target is None:
        return  # статус не ведёт ФФ-копию (офисный/системный/не наш)
    for copy in await _find_ff_copies(oid):
        await _move_copy(copy, target, name or "")


async def reconcile_order(ms_order_id: str) -> dict:
    """Разово подравнять ФФ-копию одного заказа под текущий статус склада.

    Для ручной сверки/теста. Возвращает краткий отчёт.
    """
    order = await ms_client.get(f"entity/customerorder/{ms_order_id}")
    if not order:
        return {"ok": False, "error": "заказ МС не найден"}
    name = _ms_state_name.get(_state_uuid(order) or "")
    target = _ff_stage_for(name)
    copies = await _find_ff_copies(ms_order_id)
    moved = []
    for copy in copies:
        if target is not None and await _move_copy(copy, target, name or ""):
            moved.append(copy.get("id"))
    return {
        "ok": True, "ms_status": name, "target_stage": _stage_name(target) if target else None,
        "copies": [c.get("id") for c in copies], "moved": moved,
    }


# ---------------------------------------------------------------------------
# Фоновый опрос
# ---------------------------------------------------------------------------

async def _poll_once() -> None:
    global _first_poll
    lookback_s = MS_SYNC_LOOKBACK_MIN * 60 if _first_poll else (MS_SYNC_POLL_INTERVAL_S + 120)
    _first_poll = False
    since = (datetime.datetime.now(_MSK) - datetime.timedelta(seconds=lookback_s)).strftime("%Y-%m-%d %H:%M:%S")

    offset, total = 0, 0
    while True:
        data = await ms_client.get("entity/customerorder", {
            "filter": f"updated>={since}", "order": "updated,asc", "limit": 100, "offset": offset,
        })
        if not data:
            break
        rows = data.get("rows") or []
        for order in rows:
            oid, upd = order.get("id"), order.get("updated")
            if oid and _seen.get(oid) == upd:
                continue  # эту версию заказа уже обработали (перекрытие окна)
            try:
                await _process_order(order)
                if oid:
                    _seen[oid] = upd
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("MS sync: ошибка обработки заказа %s", oid)
        total += len(rows)
        if len(rows) < 100:
            break
        offset += 100
        if offset >= 1000:
            logger.warning("MS sync: >1000 изменённых заказов в окне с %s — обрезаю", since)
            break

    if len(_seen) > 5000:
        _seen.clear()
    if total:
        logger.info("MS sync poll: просмотрено %s изменённых заказов (с %s)", total, since)


async def _poll_loop() -> None:
    while True:
        try:
            await _poll_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MS sync: ошибка фонового опроса")
        await asyncio.sleep(MS_SYNC_POLL_INTERVAL_S)
