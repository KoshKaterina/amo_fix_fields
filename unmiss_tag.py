"""Автоснятие тега «пропущенный» при дозвоне (реконсиляция по дочитыванию).

UIS вешает «пропущенный» на СДЕЛКУ и КОНТАКТ при потерянном входящем звонке, а
«Успешный звонок» — при успешном (вх./исх.). Когда до клиента ДОЗВОНИЛИСЬ, снимаем
«пропущенный» со сделки и её контактов.

⚠️ Почему реконсиляция, а не разбор тегов из пейлоада: amo НЕ присылает теги в
вебхук /lead_change (только поля — проверено на тест-сделке 06.07: showroom по полю
сработал, снятие по тегам — нет), а UIS ставит теги как СИСТЕМА (created_by=0).
Поэтому на любом изменении сделки в фоне ДОЧИТЫВАЕМ её теги и сверяем.

Идемпотентно и без петли: PATCH только когда на сделке ОДНОВРЕМЕННО «Успешный
звонок» И «пропущенный»; после снятия «пропущенный» повторная сверка = no-op.
Фоновая работа не блокирует ответ вебхука. Дешёвый short-circuit: если нет
«Успешный звонок» — сразу выходим (один GET), PATCH не делаем.

Дебаунс (08.07.2026): сверка не чаще раза в UNMISS_DEBOUNCE_SECONDS на сделку.
Замер за 10.5 ч: 347 сверок (по GET на каждый вебхук) при 0 срабатываний —
~треть трафика к amo впустую, в основном пачки echo-вебхуков одной правки.
Дебаунс «с хвостом»: события внутри окна не теряются — планируется ОДНА
отложенная сверка на конец окна, она видит финальное состояние тегов. Худшая
задержка снятия тега = UNMISS_DEBOUNCE_SECONDS (было ~10 с).
"""

import asyncio
import logging
import os
import time

import amo_service
from waybill_config import TAG_MISSED_NAME, TAG_SUCCESS_CALL_NAME

logger = logging.getLogger("uvicorn")

UNMISS_DEBOUNCE_SECONDS = float(os.getenv("UNMISS_DEBOUNCE_SECONDS", "120"))

_bg_tasks: set = set()
# lead_id → monotonic-время последней сверки; отдельно — сделки, по которым уже
# ждёт отложенная («хвостовая») сверка.
_last_check: dict[str, float] = {}
_tail_scheduled: set[str] = set()
# Взводится на shutdown: спящие хвосты просыпаются и досверяют немедленно,
# пока API-пайплайн ещё жив — иначе отложенная сверка молча терялась бы при
# деплое, и тег «пропущенный» на затихшей сделке висел бы навсегда.
_shutdown_event = asyncio.Event()


def maybe_remove_bg(lead_id) -> None:
    """На изменении сделки — в фоне сверить теги и снять «пропущенный», если
    дозвонились. Быстрый: планирует фон и сразу возвращает. Внутри окна дебаунса
    повторные вебхуки схлопываются в одну отложенную сверку."""
    if lead_id is None:
        return
    key = str(lead_id)
    now = time.monotonic()
    last = _last_check.get(key)
    if last is not None and now - last < UNMISS_DEBOUNCE_SECONDS:
        if key not in _tail_scheduled:
            _tail_scheduled.add(key)
            _spawn(_tail_apply(key, lead_id, UNMISS_DEBOUNCE_SECONDS - (now - last)))
        return
    _last_check[key] = now
    _cleanup_last_check(now)
    _spawn(_apply(lead_id))


def _spawn(coro) -> None:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)


async def _tail_apply(key: str, lead_id, delay: float) -> None:
    """Отложенная сверка на конец окна дебаунса — видит финальное состояние.
    На shutdown не ждёт остатка окна, а досверяет сразу (см. _shutdown_event)."""
    try:
        await asyncio.wait_for(_shutdown_event.wait(), timeout=max(delay, 0.0))
    except asyncio.TimeoutError:
        pass
    finally:
        _tail_scheduled.discard(key)
    _last_check[key] = time.monotonic()
    await _apply(lead_id)


async def shutdown() -> None:
    """Досверить хвосты дебаунса перед остановкой. Звать из lifespan ДО остановки
    API-пайплайна (сверкам нужен живой amo-клиент)."""
    _shutdown_event.set()
    pending = [t for t in _bg_tasks if not t.done()]
    if not pending:
        return
    done, still_pending = await asyncio.wait(pending, timeout=15)
    if still_pending:
        logger.warning(
            "unmiss: %d фоновых сверок не успели на shutdown (отложенные сделки: %s)",
            len(still_pending), ", ".join(sorted(_tail_scheduled)) or "—",
        )
        for t in still_pending:
            t.cancel()


def _cleanup_last_check(now: float) -> None:
    if len(_last_check) < 5000:
        return
    stale = [k for k, v in _last_check.items() if now - v >= UNMISS_DEBOUNCE_SECONDS]
    for k in stale:
        del _last_check[k]


async def _apply(lead_id) -> None:
    try:
        lead = await amo_service.get_lead_full(lead_id, with_=("contacts",))
        if not lead:
            return
        # Снимаем ТОЛЬКО когда дозвонились (есть «Успешный звонок») И ещё висит
        # «пропущенный». Иначе — выходим без записи (это и гасит эхо своего PATCH).
        if not amo_service.has_tag(lead, TAG_SUCCESS_CALL_NAME):
            return
        if not amo_service.has_tag(lead, TAG_MISSED_NAME):
            return
        await amo_service.remove_tag(lead_id, TAG_MISSED_NAME, lead=lead)
        logger.info("Автоснятие «пропущенный»: снят со сделки %s (дозвон)", lead_id)
        # Контакты сделки: в embed сделки тегов контакта нет → дочитываем контакт.
        for contact in (lead.get("_embedded") or {}).get("contacts") or []:
            cid = contact.get("id")
            if cid is None:
                continue
            full = await amo_service.get_contact_by_id(cid)
            if full and amo_service.has_tag(full, TAG_MISSED_NAME):
                await amo_service.remove_contact_tag(cid, TAG_MISSED_NAME, contact=full)
                logger.info("Автоснятие «пропущенный»: снят с контакта %s (сделка %s)", cid, lead_id)
    except Exception:
        logger.exception("Автоснятие «пропущенный»: ошибка на сделке %s", lead_id)
