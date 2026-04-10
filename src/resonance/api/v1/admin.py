"""Admin API routes."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.crypto as crypto_module
import resonance.dependencies as deps_module
import resonance.models.user as user_models
import resonance.types as types_module

router = fastapi.APIRouter(prefix="/admin", tags=["admin"])


@router.post("/test/connect")
async def connect_test_service(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Admin-only: instantly connect the test service."""
    user_role = request.state.session.get("user_role", "user")
    if user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403, detail="Admin access required")

    # Check if already connected
    existing = (
        await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_id,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.TEST,
            )
        )
    ).scalar_one_or_none()

    if existing:
        return {"status": "already_connected"}

    settings = request.app.state.settings
    connection = user_models.ServiceConnection(
        user_id=user_id,
        service_type=types_module.ServiceType.TEST,
        external_user_id="test",
        encrypted_access_token=crypto_module.encrypt_token(
            "test-token", settings.token_encryption_key
        ),
    )
    db.add(connection)
    await db.commit()

    return {"status": "connected"}
