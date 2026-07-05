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
    FIELD_FF_TREK,
    FIELD_MOYSKLAD_ORDER_UUID,
    MS_API_URL,
    MS_ATTR_TREK,
    MS_RECONCILE_HOUR_MSK,
    MS_SYNC_LOOKBACK_MIN,
    MS_SYNC_POLL_INTERVAL_S,
    MS_TOKEN,
    PIPELINE_FULFILLMENT,
    STATUS_CLOSED_LOST,
    STATUS_SUCCESS,
)

# Имя этапа/статуса «обработка» — единственная точка обратной синхронизации amo→МС.
PROCESSING_NAME = "00. Обрабатывается"

logger = logging.getLogger("uvicorn")
_MSK = datetime.timezone(datetime.timedelta(hours=3))

_enabled = False
_poll_task: asyncio.Task | None = None
_nightly_task: asyncio.Task | None = None   # ночная полная сверка ФФ (страховка)
_ms_state_name: dict[str, str] = {}   # uuid статуса заказа МС → имя
_seen: dict[str, str] = {}            # id заказа МС → последний обработанный 'updated' (дедуп окна)
_first_poll = True

# Обратная синхронизация amo→МС — ТОЛЬКО для этапа «00. Обрабатывается».
_ff_processing_status_id: int | None = None   # id этапа «00. Обрабатывается» в воронке ФФ
_ms_processing_state_uuid: str | None = None  # uuid статуса «00. Обрабатывается» в МС
_bg_tasks: set = set()                        # держим ссылки на фоновые задачи amo→МС


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

    # Этап/статус «00. Обрабатывается» — единственная точка обратной синхронизации amo→МС.
    global _ff_processing_status_id, _ms_processing_state_uuid
    _ff_processing_status_id = amo_service.resolve_status_id_by_name(PIPELINE_FULFILLMENT, PROCESSING_NAME)
    _ms_processing_state_uuid = next((u for u, n in _ms_state_name.items() if n == PROCESSING_NAME), None)
    if _ff_processing_status_id is None:
        logger.warning("MS sync: в воронке Фулфилмент нет этапа «%s» — проверь имена/кэш", PROCESSING_NAME)
    if _ms_processing_state_uuid is None:
        logger.warning("MS sync: в МС нет статуса «%s» — обратная синхр. amo→МС(00) отключена", PROCESSING_NAME)

    _enabled = True
    logger.info(
        "MS sync включён: %s статусов заказа МС, опрос каждые %sс (lookback %s мин)",
        len(_ms_state_name), MS_SYNC_POLL_INTERVAL_S, MS_SYNC_LOOKBACK_MIN,
    )
    _poll_task = asyncio.create_task(_poll_loop())

    global _nightly_task
    _nightly_task = asyncio.create_task(_nightly_loop())
    logger.info("MS sync: ночная полная сверка ФФ в %02d:00 МСК", MS_RECONCILE_HOUR_MSK)


async def shutdown() -> None:
    global _enabled, _poll_task, _nightly_task
    _enabled = False
    if _poll_task is not None:
        _poll_task.cancel()
        try:
            await _poll_task
        except asyncio.CancelledError:
            pass
        _poll_task = None
    if _nightly_task is not None:
        _nightly_task.cancel()
        try:
            await _nightly_task
        except asyncio.CancelledError:
            pass
        _nightly_task = None
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


async def _order_trek(order: dict) -> str | None:
    """Трек-номер заказа МС. Берём из атрибутов заказа; если список их не вернул —
    дозапрашиваем заказ целиком."""
    attrs = order.get("attributes")
    if attrs is None:
        full = await ms_client.get(f"entity/customerorder/{order.get('id')}")
        attrs = (full or {}).get("attributes") or []
    for a in attrs:
        if a.get("id") == MS_ATTR_TREK:
            v = a.get("value")
            return str(v).strip() if v not in (None, "") else None
    return None


async def _sync_trek(copy: dict, trek: str) -> None:
    cid = copy.get("id")
    cur = str(amo_service.get_custom_field_value(copy, FIELD_FF_TREK) or "").strip()
    if cur == trek:
        return  # уже совпадает — идемпотентно
    res = await amo_service.patch_lead(cid, custom_fields={FIELD_FF_TREK: trek})
    if res.get("ok"):
        logger.info("MS sync: копия %s трек-номер → %s", cid, trek)
    else:
        logger.error("MS sync: трек копии %s не записан: %s", cid, res)


async def _process_order(order: dict) -> None:
    oid = order.get("id")
    if not oid:
        return
    name = _ms_state_name.get(_state_uuid(order) or "")
    target = _ff_stage_for(name)
    trek = await _order_trek(order)
    if target is None and not trek:
        return  # ни этапа, ни трека — нечего синкать
    for copy in await _find_ff_copies(oid):
        # финализированные/закрытые (в т.ч. дубли) не трогаем — ни этап, ни трек
        if copy.get("status_id") in (STATUS_SUCCESS, STATUS_CLOSED_LOST):
            continue
        if target is not None:
            await _move_copy(copy, target, name or "")
        if trek:
            await _sync_trek(copy, trek)


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
# Обратная синхронизация amo → МС (ТОЛЬКО этап «00. Обрабатывается»)
# ---------------------------------------------------------------------------

def push_processing_bg(lead_id, status_id) -> None:
    """Вызов из вебхука /lead_change (должен быть быстрым). Если ФФ-сделку
    перевели в этап «00. Обрабатывается» — в фоне проставляем «00» заказу в МС.
    Только этот этап; дальше склад ведёт amo (МС→amo)."""
    if not _enabled or _ff_processing_status_id is None:
        return
    try:
        if int(status_id) != _ff_processing_status_id:
            return
    except (TypeError, ValueError):
        return
    t = asyncio.create_task(_push_processing(lead_id))
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)


async def _push_processing(lead_id) -> None:
    try:
        if _ms_processing_state_uuid is None:
            return
        lead = await amo_service.get_lead_full(lead_id, with_=())
        if not lead or lead.get("pipeline_id") != PIPELINE_FULFILLMENT:
            return
        if lead.get("status_id") != _ff_processing_status_id:
            return  # этап успел измениться — не наш случай
        uuid = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
        if not uuid:
            return
        order = await ms_client.get(f"entity/customerorder/{uuid}")
        if not order:
            return
        cur = ((order.get("state") or {}).get("meta") or {}).get("href", "").rstrip("/").split("/")[-1]
        if cur == _ms_processing_state_uuid:
            return  # уже «00» — идемпотентно (и гасит петлю МС→amo→МС)
        body = {"state": {"meta": {
            "href": f"{MS_API_URL}/entity/customerorder/metadata/states/{_ms_processing_state_uuid}",
            "type": "state", "mediaType": "application/json",
        }}}
        res = await ms_client.put(f"entity/customerorder/{uuid}", body)
        if res is not None:
            logger.info("MS sync amo→склад: заказ %s (сделка %s) → «%s»", uuid, lead_id, PROCESSING_NAME)
        else:
            logger.error("MS sync amo→склад: не удалось проставить «%s» заказу %s", PROCESSING_NAME, uuid)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("MS sync amo→склад: ошибка для сделки %s", lead_id)


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


# ---------------------------------------------------------------------------
# Ночная ПОЛНАЯ сверка (страховка) — amo-driven, не зависит от окна `updated`
# ---------------------------------------------------------------------------
# Живой опрос ловит только заказы, изменённые в узком окне (interval+буфер). Если
# сервис стоял/подвисал/деплоился дольше окна — изменение статуса МС теряется
# навсегда (первый-проход lookback короче истории). Раз в сутки перебираем ВСЕ
# открытые ФФ-сделки и подгоняем под текущий статус заказа МС напрямую — это
# гарантирует итоговую согласованность независимо от промахов живого окна.
# (У metrika/woo такая ночная сверка есть; у ms её не было — корень рассинхрона.)

async def reconcile_all_ff() -> dict:
    """Полная сверка воронки Фулфилмент под текущие статусы заказов МС. Идемпотентно."""
    leads = await amo_service.get_leads_updated_since(PIPELINE_FULFILLMENT, 0)
    moved = 0
    for lead in leads:
        if lead.get("status_id") in (STATUS_SUCCESS, STATUS_CLOSED_LOST):
            continue  # финализированные (в т.ч. закрытые дубли) не трогаем
        uuid = str(amo_service.get_custom_field_value(lead, FIELD_MOYSKLAD_ORDER_UUID) or "").strip()
        if not uuid:
            continue
        try:
            order = await ms_client.get(f"entity/customerorder/{uuid}")
            if not order:
                continue
            name = _ms_state_name.get(_state_uuid(order) or "")
            target = _ff_stage_for(name)
            if target is not None and target != lead.get("status_id"):
                if await _move_copy(lead, target, name or ""):
                    moved += 1
            trek = await _order_trek(order)
            if trek:
                await _sync_trek(lead, trek)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MS reconcile: ошибка по сделке %s", lead.get("id"))
    logger.info("MS reconcile (полная ФФ): двинуто %s из %s сделок", moved, len(leads))
    return {"total": len(leads), "moved": moved}


def _seconds_until_next(hour_msk: int) -> float:
    now = datetime.datetime.now(_MSK)
    nxt = now.replace(hour=hour_msk, minute=0, second=0, microsecond=0)
    if nxt <= now:
        nxt += datetime.timedelta(days=1)
    return (nxt - now).total_seconds()


async def _nightly_loop() -> None:
    while True:
        try:
            await asyncio.sleep(_seconds_until_next(MS_RECONCILE_HOUR_MSK))
        except asyncio.CancelledError:
            raise
        try:
            await reconcile_all_ff()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MS sync: ночная полная сверка упала")
