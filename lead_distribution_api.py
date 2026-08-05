"""Админ-эндпоинты конструктора профилей распределения лидов (lead_distribution.py).

Без UI — эндпоинты под будущую панель (Тиана попросила именно API в этом
раунде). Защита — секрет в пути, по образцу /wazzup/{secret}, /uis/{secret}:
пустой LEAD_DISTRIBUTION_ADMIN_SECRET делает все роуты недоступными (403 на
любой переданный секрет), это безопасное состояние по умолчанию.
"""

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

import amo_service
import lead_distribution as ld
from waybill_config import LEAD_DISTRIBUTION_ADMIN_SECRET

logger = logging.getLogger("uvicorn")

router = APIRouter(prefix="/admin/lead-distribution/{secret}")


def _check_secret(secret: str) -> bool:
    return bool(LEAD_DISTRIBUTION_ADMIN_SECRET) and secret == LEAD_DISTRIBUTION_ADMIN_SECRET


def _forbidden() -> JSONResponse:
    return JSONResponse({"detail": "forbidden"}, status_code=403)


def _conflict(exc: ld.ProfileConflictError) -> JSONResponse:
    return JSONResponse(
        {
            "detail": str(exc),
            "conflicting_profile_id": exc.conflicting_profile_id,
            "conflicting_source_ids": sorted(exc.conflicting_source_ids),
        },
        status_code=409,
    )


@router.get("/profiles")
async def list_profiles_route(secret: str):
    if not _check_secret(secret):
        return _forbidden()
    return {"profiles": [p.to_dict() for p in ld.list_profiles()]}


@router.post("/profiles")
async def create_profile_route(secret: str, request: Request):
    if not _check_secret(secret):
        return _forbidden()
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"detail": "невалидный JSON"}, status_code=400)
    try:
        profile = ld.create_profile(data)
    except ld.ProfileValidationError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except ld.ProfileConflictError as exc:
        return _conflict(exc)
    return JSONResponse(profile.to_dict(), status_code=201)


@router.get("/profiles/{profile_id}")
async def get_profile_route(secret: str, profile_id: str):
    if not _check_secret(secret):
        return _forbidden()
    profile = ld.get_profile(profile_id)
    if profile is None:
        return JSONResponse({"detail": "not found"}, status_code=404)
    return profile.to_dict()


@router.patch("/profiles/{profile_id}")
async def patch_profile_route(secret: str, profile_id: str, request: Request):
    if not _check_secret(secret):
        return _forbidden()
    try:
        patch = await request.json()
    except Exception:
        return JSONResponse({"detail": "невалидный JSON"}, status_code=400)
    try:
        profile = ld.update_profile(profile_id, patch)
    except KeyError:
        return JSONResponse({"detail": "not found"}, status_code=404)
    except ld.ProfileValidationError as exc:
        return JSONResponse({"detail": str(exc)}, status_code=400)
    except ld.ProfileConflictError as exc:
        return _conflict(exc)
    return profile.to_dict()


@router.delete("/profiles/{profile_id}")
async def delete_profile_route(secret: str, profile_id: str):
    if not _check_secret(secret):
        return _forbidden()
    ok = ld.delete_profile(profile_id)
    if not ok:
        return JSONResponse({"detail": "not found"}, status_code=404)
    return {"ok": True}


@router.get("/pipelines")
async def pipelines_route(secret: str):
    """Для дропдауна «воронка → этапы» (точка входа профиля)."""
    if not _check_secret(secret):
        return _forbidden()
    raw = await amo_service.get_pipelines_with_statuses()
    return {
        "pipelines": [
            {
                "id": p.get("id"),
                "name": p.get("name"),
                "statuses": [
                    {"id": s.get("id"), "name": s.get("name")}
                    for s in (p.get("_embedded") or {}).get("statuses") or []
                ],
            }
            for p in raw
        ]
    }


@router.get("/sources")
async def sources_route(secret: str, pipeline_id: int, days: int = 30):
    """Для дропдауна источников (фильтр профиля). Прямой /api/v4/sources на
    этом аккаунте пуст — каталог строим по факту из недавних сделок воронки."""
    if not _check_secret(secret):
        return _forbidden()
    return {"sources": await amo_service.get_recent_lead_sources(pipeline_id, days=days)}


@router.get("/employees")
async def employees_route(secret: str):
    """Для дропдауна участников/дежурного с поиском (поиск — на стороне
    будущего UI, здесь просто полный список активных пользователей)."""
    if not _check_secret(secret):
        return _forbidden()
    users = await amo_service.get_users()
    return {
        "employees": [
            {"id": u.get("id"), "name": u.get("name"), "is_active": not u.get("is_disabled", False)}
            for u in users
        ]
    }


@router.get("/state")
async def state_route(secret: str):
    """Отладочный срез сегодняшних счётчиков — «почему сделка ушла именно
    этому человеку», без захода на сервер руками."""
    if not _check_secret(secret):
        return _forbidden()
    return ld.debug_state()
