"""Внутренний API движка для ИИ-сервиса: `/internal/ai/v1/permit`, `/result`, плюс две ручки человека.

⚠️ Сервис доступен снаружи через `team.sunscrypt.ru/amo/` (см. AGENTS.md: три вебхука без секрета уже
открыты, нового открытого маршрута мы добавлять не хотим). Поэтому защита здесь жёсткая:

  • без `AI_ENGINE_ENABLED=1` или без `AI_ENGINE_TOKEN` ручки `permit`/`result` отвечают 503;
  • токен сверяется в постоянное время (`hmac.compare_digest`), версия контракта обязательна (426);
  • ручки человека (`return_dialog`, `state`) закрыты ОТДЕЛЬНЫМ `AI_ADMIN_TOKEN`: у ИИ-сервиса его нет, и
    сервис не может сам забрать назад сделку, которую у него отняли;
  • кнопка «Дать сделку ИИ» в панели Team ходит со своим узким `X-Give-Token` (`AI_GIVE_TOKEN`): он открывает только `/give_deal`,
    админский токен в панель не кладётся.

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
import ai_engine_filters
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


class GiveIn(BaseModel):
    """Человек отдаёт ИИ существующую сделку (`ai_engine.give_deal`): чат клиента задаёт тот, кто отдаёт."""
    deal_id: int
    chat_id: str
    chat_type: str
    channel_id: str
    phone: str = ""
    contact_name: str = ""
    force: bool = False         # вопреки правилам дублей A, C, D (решение человека)
    retrigger: bool = False     # сделку ИИ уже ведёт: поставить ход-событие заново


class ReassignDecisionIn(BaseModel):
    """Решение человека по предложению смены ответственного (`ai_engine_approval`): подтвердить или отклонить. Имена и формы согласованы с панелью."""
    ref: str
    deal_id: int
    decision: str               # approve | reject
    decided_by: str = ""


class SendFailedIn(BaseModel):
    """ИИ-сервис не смог отправить сообщение клиенту: `code` короткий код причины (для дедупликации), `reason` читаемая причина по-русски."""
    deal_id: int
    turn_id: int = 0
    code: str
    reason: str


class FollowupIn(BaseModel):
    """Отложенное касание, о котором клиент попросил сам (`ai_engine_followup`). `due_at_ts` это unix-секунды."""
    deal_id: int
    epoch: int
    turn_id: int
    up_to_message_id: str = ""
    due_at_ts: float
    reason: str = ""
    draft: str = ""


class TeamAlertIn(BaseModel):
    """Дежурный алерт ИИ-сервиса (`ai_engine.duty_alert`): `event` - ключ события панели (`ai_engine.DUTY_EVENTS`), `text` до 500 знаков."""
    deal_id: int
    event: str
    text: str


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


def _check_give(token: str) -> JSONResponse | None:
    """Узкий токен панели для `/give_deal`. Другие ручки человека и ручки сервиса его не принимают."""
    if not cfg.ENABLED or not cfg.GIVE_TOKEN:
        return _deny(503, "ai_engine_give_disabled")
    if not hmac.compare_digest(token.encode("utf-8"), cfg.GIVE_TOKEN.encode("utf-8")):
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
    if out.get("allowed"):
        ai_engine_filters.mark_first_ai_message(body.deal_id)    # первое сообщение ИИ клиенту: охрана инициативы по «существующей» сделке снимается
        await ai_engine.silence_arm(body.deal_id)          # ИИ сейчас напишет клиенту: идёт отсчёт тишины
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


@router.post("/followup")
async def followup(body: FollowupIn, x_service_token: str = Header(default=""), x_contract: str = Header(default="")):
    bad = _check_service(x_service_token, x_contract)
    if bad is not None:
        return bad
    try:
        code, out = await ai_engine.schedule_followup(
            body.deal_id, body.epoch, body.turn_id, body.up_to_message_id, body.due_at_ts, body.reason, body.draft)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: отложенное касание по сделке %s не зарегистрировано", body.deal_id)
        return JSONResponse({"allowed": False, "reason": "engine_error"}, status_code=500)
    return JSONResponse(out, status_code=code)


@router.post("/send_failed")
async def send_failed(body: SendFailedIn, x_service_token: str = Header(default=""), x_contract: str = Header(default="")):
    """Отказ отправки клиенту: примечание, алерт и срочная задача (`ai_engine.send_failed`). Токен сервиса, как у `permit`."""
    bad = _check_service(x_service_token, x_contract)
    if bad is not None:
        return bad
    try:
        out = await ai_engine.send_failed(body.deal_id, body.code, body.reason)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: отказ отправки по сделке %s не обработан", body.deal_id)
        return JSONResponse({"detail": "engine_error"}, status_code=500)
    return JSONResponse(out)


@router.post("/team_alert")
async def team_alert(body: TeamAlertIn, x_service_token: str = Header(default=""), x_contract: str = Header(default="")):
    """Дежурный алерт ИИ-сервиса по событию панели (`ai_engine.duty_alert`). Токен сервиса, как у `send_failed`.

    Ответ `{"status": "sent" | "disabled" | "duplicate"}`, Телеграм не принял - 502 `{"status": "failed"}`. Незнакомое событие,
    пустой текст или сделка не больше нуля - 422; текст длиннее 500 знаков обрезается."""
    bad = _check_service(x_service_token, x_contract)
    if bad is not None:
        return bad
    if body.event not in ai_engine.DUTY_EVENTS:
        return _deny(422, "unknown_event")
    if body.deal_id <= 0:
        return _deny(422, "deal_id")
    text = " ".join(body.text.split())
    if not text:
        return _deny(422, "text")
    if len(text) > ai_engine.DUTY_TEXT_MAX:
        text = text[:ai_engine.DUTY_TEXT_MAX - 1].rstrip() + "…"
    status = await ai_engine.duty_alert(body.deal_id, body.event, text)
    return JSONResponse({"status": status}, status_code=502 if status == "failed" else 200)


@router.post("/return_dialog")
async def return_dialog(body: ReturnIn, x_admin_token: str = Header(default="")):
    """Человек вернул диалог ИИ. Движок ставит `owner = ai` и шлёт ходом событие `dialog_returned`."""
    bad = _check_admin(x_admin_token)
    if bad is not None:
        return bad
    code, out = await ai_engine.return_dialog(body.deal_id)
    return JSONResponse(out, status_code=code)


@router.post("/give_deal")
async def give_deal(body: GiveIn, x_admin_token: str = Header(default=""), x_give_token: str = Header(default="")):
    """Человек отдал ИИ сделку: движок ставит `owner = ai` и шлёт ходом событие `deal_given`. Закрыта админским токеном, как `return_dialog`.

    Панель Team (кнопка «Дать сделку ИИ») ходит сюда со своим узким токеном `X-Give-Token` (`AI_GIVE_TOKEN`): он подходит только к этой ручке."""
    bad = _check_give(x_give_token) if x_give_token else _check_admin(x_admin_token)
    if bad is not None:
        return bad
    try:
        code, out = await ai_engine.give_deal(body.deal_id, body.chat_id, body.chat_type, body.channel_id, body.phone, body.contact_name, body.force, body.retrigger)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: сделка %s не передана ИИ", body.deal_id)
        return JSONResponse({"detail": "engine_error"}, status_code=500)
    return JSONResponse(out, status_code=code)


@router.post("/reassign/decision")
async def reassign_decision(body: ReassignDecisionIn, x_admin_token: str = Header(default=""), x_give_token: str = Header(default="")):
    """Решение человека по предложению «сменить ответственного и взять сделку»: движок исполняет (`ai_engine_approval.decide`). Тот же узкий токен панели
    `X-Give-Token` (`AI_GIVE_TOKEN`), что у `give_deal`: он открывает ТОЛЬКО эти две ручки; админский токен тоже подходит. Ответ 200 `{ok, state, detail}`;
    повтор по решённому предложению безопасен (текущее состояние без действий). 403: токен не тот, 404: нет такого предложения, 409: сделка не та."""
    bad = _check_give(x_give_token) if x_give_token else _check_admin(x_admin_token)
    if bad is not None:
        return bad
    try:
        code, out = await ai_engine.reassign_decision(body.ref, body.deal_id, body.decision, body.decided_by)
    except Exception:  # noqa: BLE001
        logger.exception("ai_engine: решение по предложению %s не обработано", body.ref)
        return JSONResponse({"detail": "engine_error"}, status_code=500)
    return JSONResponse(out, status_code=code)


class AlertIn(BaseModel):
    key: str = "manual"
    lines: list[str]


@router.post("/alert")
async def alert(body: AlertIn, x_admin_token: str = Header(default="")):
    """Алерт ИИ-менеджера в чат ОП по требованию человека (шапка и пинги, как у обычных алертов). Закрыт админским токеном."""
    bad = _check_admin(x_admin_token)
    if bad is not None:
        return bad
    await ai_engine.raise_alert(body.key, body.lines[:8])
    return {"ok": True}


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
    return {"owner": {k: owner[k] for k in ("deal_id", "owner", "epoch", "since", "reason", "closed", "pipeline_id", "mode")},
            "active_turn": None if turn is None else {k: turn[k] for k in ("turn_id", "state", "reason", "epoch")}}
