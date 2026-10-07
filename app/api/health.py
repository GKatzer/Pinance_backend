"""Health & readiness endpoints.

Разделяем намеренно:
- /health  — процесс жив, отвечает (для Docker healthcheck и UptimeRobot)
- /ready   — БД и Redis доступны (для оркестратора, если когда-нибудь будет)
"""
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis import get_redis
from app.db.session import get_session

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/ready")
async def ready(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str]:
    # Postgres
    try:
        await session.execute(text("SELECT 1"))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"postgres unavailable: {e!s}",
        ) from e

    # Redis
    try:
        await get_redis().ping()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"redis unavailable: {e!s}",
        ) from e

    return {"status": "ready"}