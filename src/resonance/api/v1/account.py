"""Account API routes — profile and service connection management."""

from __future__ import annotations

import uuid  # noqa: TC003 - runtime import required for FastAPI dependency resolution
from typing import Annotated

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dependencies as deps_module
import resonance.models.user as user_models

router = fastapi.APIRouter(prefix="/account", tags=["account"])


@router.get("")
async def get_profile(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str | None]:
    """Return the authenticated user's profile."""
    stmt = sa.select(user_models.User).where(user_models.User.id == user_id)
    result = await db.execute(stmt)
    user = result.scalar_one_or_none()
    if user is None:
        raise fastapi.HTTPException(status_code=404, detail="User not found")
    return {
        "id": str(user.id),
        "display_name": user.display_name,
        "email": user.email,
    }


@router.get("/connections")
async def list_connections(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[dict[str, str]]:
    """Return all service connections for the authenticated user."""
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id
    )
    result = await db.execute(stmt)
    connections = result.scalars().all()
    return [
        {
            "id": str(conn.id),
            "service_type": conn.service_type.value
            if hasattr(conn.service_type, "value")
            else str(conn.service_type),
            "external_user_id": conn.external_user_id,
            "connected_at": str(conn.connected_at),
        }
        for conn in connections
    ]


@router.delete("/connections/{connection_id}")
async def unlink_connection(
    connection_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Unlink a service connection, unless it's the user's last one."""
    # Count user's connections
    count_stmt = sa.select(sa.func.count()).select_from(
        sa.select(user_models.ServiceConnection)
        .where(user_models.ServiceConnection.user_id == user_id)
        .subquery()
    )
    count_result = await db.execute(count_stmt)
    count = count_result.scalar_one()

    if count <= 1:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Cannot unlink last connected service",
        )

    # Find the specific connection
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.id == connection_id,
        user_models.ServiceConnection.user_id == user_id,
    )
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if connection is None:
        raise fastapi.HTTPException(status_code=404, detail="Connection not found")

    await db.delete(connection)
    await db.commit()

    return {"status": "unlinked"}
