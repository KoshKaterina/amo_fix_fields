"""Админ-эндпоинты конструктора профилей распределения лидов (lead_distribution.py).

CRUD профилей (POST/PATCH/DELETE/GET /profiles) переехал в team-panel 09.08.2026 —
там теперь единственное хранилище (Postgres) и валидация, см. app/lead_distribution/
service.py в team-panel и lead_distribution_profiles_client.py здесь (write-through
кэш, которым эти эндпоинты больше не занимаются). Остаются только точки, которым
нужен живой доступ к amoCRM API (его нет у team-panel) — дропдауны конструктора и
отладочный /state.

Защита — секрет в пути, по образцу /wazzup/{secret}, /uis/{secret}: пустой
LEAD_DISTRIBUTION_ADMIN_SECRET делает все роуты недоступными (403 на любой
переданный секрет), это безопасное состояние по умолчанию.
"""

import logging

from fastapi import APIRouter
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
