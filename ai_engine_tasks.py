"""Задачи amo при передаче диалога человеку, которую делает сам движок (сторона `amo_fix_fields` ИИ-менеджера).

Когда диалог уходит человеку по решению движка (клиент прислал медиа, у клиента несколько сделок, ИИ-сервис отказался или ход провалился),
кроме алерта в чат ОП ставится срочная задача «Связаться с клиентом» на ответственного по сделке, как и у передачи самим ИИ (там её ставит
тулза `handoff` ИИ-сервиса). Когда менеджер возвращает диалог ИИ (реакция на алерт или ручка `return_dialog`), открытые задачи передачи
закрываются: ИИ снова ведёт диалог, и менеджеру незачем «связываться».

Что считается задачей передачи: открытая задача сделки, текст которой начинается с `[handoff:` (так помечают и ИИ-сервис, и движок). Чужие
задачи не трогаем. Любой сбой amo здесь не должен ломать разбор: пишем в лог и живём дальше (алерт в чат ОП уже ушёл).
Тесты: test_ai_engine_tasks.py.
"""
import logging
import time
from typing import Any

import ai_engine_config as cfg
import api as amo_api
import amo_service
from api import BASE_URL

logger = logging.getLogger("uvicorn")

MARKER = "[handoff:"
TASK_URGENT = 4239498          # тип задачи «Срочно» в amo (тот же, что у передачи самим ИИ)
DUE_S = 3600                   # срок: час


async def _get_open(deal_id: int) -> list[dict] | None:
    """Открытые задачи сделки. None - amo не ответил (молчание нельзя читать как «задач нет»)."""
    url = f"{BASE_URL}/api/v4/tasks?filter[entity_id]={int(deal_id)}&filter[entity_type]=leads&filter[is_completed]=0&limit=50"
    data = await amo_api._request_json("GET", url, what=f"ai_handoff_tasks[{deal_id}]")
    if data is None:
        return None
    return ((data.get("_embedded") or {}).get("tasks")) or []


async def _responsible(deal_id: int) -> int | None:
    lead = await amo_service.get_lead_full(deal_id, with_=())
    uid = (lead or {}).get("responsible_user_id")
    return int(uid) if uid else None


async def _create(deal_id: int, text: str, responsible_user_id: int, due_ts: int) -> bool:
    return await amo_api.create_task(deal_id, text, responsible_user_id, due_ts, TASK_URGENT)


async def _complete(task_ids: list[int], result: str) -> bool:
    body = [{"id": int(i), "is_completed": True, "result": {"text": result}} for i in task_ids]
    data = await amo_api._request_json("PATCH", f"{BASE_URL}/api/v4/tasks", body=body, what="ai_handoff_tasks_complete")
    return data is not None


def _is_handoff(task: dict[str, Any]) -> bool:
    return str(task.get("text") or "").startswith(MARKER)


async def create_contact_task(deal_id: int, reason: str, why: str) -> bool:
    """Срочная задача «Связаться с клиентом» на ответственного. Не ставит вторую по той же причине. True - поставлена."""
    if not cfg.HANDOFF_TASKS:
        return False
    try:
        marker = f"{MARKER}{reason}]"
        open_tasks = await _get_open(deal_id)
        if open_tasks is None:
            logger.warning("ai_engine_tasks: amo не ответил по задачам сделки %s, задачу не ставлю", deal_id)
            return False
        if any(str(t.get("text") or "").startswith(marker) for t in open_tasks):
            return False
        uid = await _responsible(deal_id)
        if not uid:
            logger.info("ai_engine_tasks: у сделки %s нет ответственного, задачи не будет (остался алерт)", deal_id)
            return False
        text = f"{marker} Связаться с клиентом, срочно: {why.strip()}"[:500]
        return bool(await _create(deal_id, text, uid, int(time.time()) + DUE_S))
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_tasks: задача передачи по сделке %s не поставилась", deal_id)
        return False


async def release(deal_id: int) -> int:
    """Диалог вернули ИИ: закрыть открытые задачи передачи сделки. Возвращает, сколько закрыто."""
    if not cfg.HANDOFF_TASKS:
        return 0
    try:
        open_tasks = await _get_open(deal_id)
        mine = [int(t["id"]) for t in (open_tasks or []) if _is_handoff(t)]
        if mine and await _complete(mine, "Диалог возвращён ИИ"):
            return len(mine)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine_tasks: задачи передачи по сделке %s не закрылись", deal_id)
    return 0
