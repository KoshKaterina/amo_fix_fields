"""Внутренний API движка для ИИ-сервиса: `/internal/ai/v1/permit`, `/result`, плюс две ручки человека.

⚠️ Сервис доступен снаружи через `team.sunscrypt.ru/amo/` (см. AGENTS.md: три вебхука без секрета уже
открыты, нового открытого маршрута мы добавлять не хотим). Поэтому защита здесь жёсткая:

  • без `AI_ENGINE_ENABLED=1` или без `AI_ENGINE_TOKEN` ручки `permit`/`result` отвечают 503;
  • токен сверяется в постоянное время (`hmac.compare_digest`), версия контракта обязательна (426);
  • ручки человека (`return_dialog`, `state`) закрыты ОТДЕЛЬНЫМ `AI_ADMIN_TOKEN`: у ИИ-сервиса его нет, и
    сервис не может сам забрать назад сделку, которую у него отняли.

Рекомендация к выкатке: закрыть `/amo/internal/` в nginx (сервис ходит по docker-сети напрямую на
`amo-fix-fields:8000`, публичный путь ему не нужен). Токены это защита второго эшелона, а не замена.
"""
import hmac
import logging
from typing import Any

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import ai_engine
import ai_engine_config as cfg
import ai_engine_store as store

logger = logging.getLogger("uvicorn")

router = APIRouter(prefix="/internal/ai/v1")


class PermitIn(BaseModel):
    deal_id: int
    epoch: int
    turn_id: int
    up_to_message_id: str = ""


class ResultIn(BaseModel):
    turn_id: int
    deal_id: int
    epoch: int
    outcome: str
    reason: str | None = None
    pending_draft: dict[str, Any] | None = None


class ReturnIn(BaseModel):
    deal_id: int


OUTCOMES = {"continue", "handoff_to_human", "release_to_engine", "aborted", "failed"}


def _deny(status: int, detail: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=status)


def _check_service(token: str, contract: str) -> JSONResponse | None:
    if not cfg.ENABLED or not cfg.ENGINE_TOKEN:
        return _deny(503, "ai_engine_disabled")
    if contract != cfg.CONTRACT_VERSION:
        return _deny(426, "contract_version")
    if not hmac.compare_digest(token.encode("utf-8"), cfg.ENGINE_TOKEN.encode("utf-8")):
        return _deny(403, "forbidden")
    return None


def _check_admin(token: str) -> JSONResponse | None:
    if not cfg.ENABLED or not cfg.ADMIN_TOKEN:
        return _deny(503, "ai_engine_admin_disabled")
    if not hmac.compare_digest(token.encode("utf-8"), cfg.ADMIN_TOKEN.encode("utf-8")):
        return _deny(403, "forbidden")
    return None


@router.post("/permit")
async def permit(body: PermitIn, x_service_token: str = Header(default=""), x_contract: str = Header(default="")):
    bad = _check_service(x_service_token, x_contract)
    if bad is not None:
        return bad
    try:
        code, out = ai_engine.decide_permit(body.deal_id, body.epoch, body.turn_id, body.up_to_message_id)
    except Exception:  # noqa: BLE001
        # Недоступное решение - отказ, а не разрешение: сервис читает не-200 как «нельзя».
        logger.exception("ai_engine: permit не посчитался")
        return JSONResponse({"allowed": False, "reason": "engine_error"}, status_code=500)
    return JSONResponse(out, status_code=code)


@router.post("/result")
async def result(body: ResultIn, x_service_token: str = Header(default=""), x_contract: str = Header(default="")):
    bad = _check_service(x_service_token, x_contract)
    if bad is not None:
        return bad
    if body.outcome not in OUTCOMES:
        return _deny(422, "unknown_outcome")
    try:
        code, out = await ai_engine.handle_result(
            body.turn_id, body.deal_id, body.epoch, body.outcome, body.reason, body.pending_draft,
        )
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: итог хода %s не обработан", body.turn_id)
        return _deny(500, "engine_error")
    return JSONResponse(out, status_code=code)


@router.post("/return_dialog")
async def return_dialog(body: ReturnIn, x_admin_token: str = Header(default="")):
    """Человек вернул диалог ИИ. Движок ставит `owner = ai` и шлёт ходом событие `dialog_returned`."""
    bad = _check_admin(x_admin_token)
    if bad is not None:
        return bad
    code, out = await ai_engine.return_dialog(body.deal_id)
    return JSONResponse(out, status_code=code)


@router.get("/state/{deal_id}")
async def state(deal_id: int, x_admin_token: str = Header(default="")):
    """Что движок знает о сделке: владелец, эпоха, живой ход. Без текстов клиента."""
    bad = _check_admin(x_admin_token)
    if bad is not None:
        return bad
    owner = store.get_owner(deal_id)
    if owner is None:
        return _deny(404, "unknown_deal")
    turn = store.active_turn_for_deal(deal_id)
    return {"owner": {k: owner[k] for k in ("deal_id", "owner", "epoch", "since", "reason", "closed", "pipeline_id")},
            "active_turn": None if turn is None else {k: turn[k] for k in ("turn_id", "state", "reason", "epoch")}}
