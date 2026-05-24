"""Account and merge routes."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dependencies as deps_module
import resonance.merge as merge_module
import resonance.middleware.session as session_module
import resonance.models.user as user_models
import resonance.ui.common as common

router = fastapi.APIRouter(tags=["ui"])


# ---------------------------------------------------------------------------
# Account page
# ---------------------------------------------------------------------------


@router.get("/account", response_model=None)
async def account_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render account page with profile and connection management."""
    user_result = await db.execute(
        sa.select(user_models.User).where(user_models.User.id == user_id)
    )
    user = user_result.scalar_one_or_none()

    if user is None:
        request.state.session.clear()
        return fastapi.responses.RedirectResponse(
            url="/login?prompt=select", status_code=307
        )

    connections_result = await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_id
        )
    )
    connections = connections_result.scalars().all()

    ctx = common.base_context(request)
    ctx.update(
        user=user,
        connections=connections,
        state="button",
    )

    return common.templates.TemplateResponse(request, "account.html", ctx)


# ---------------------------------------------------------------------------
# Merge flow
# ---------------------------------------------------------------------------


@router.get("/merge", response_model=None)
async def merge_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render merge confirmation page with source account data summary."""
    session = request.state.session
    source_user_id = session.get("merge_source_user_id")
    if not source_user_id:
        return fastapi.responses.RedirectResponse(url="/account", status_code=307)

    source_summary = await merge_module.get_account_summary(
        db, uuid.UUID(source_user_id)
    )

    service_type = session.get("merge_service_type", "unknown")

    ctx = common.base_context(request)
    ctx.update(
        source_summary=source_summary,
        service_type=service_type,
    )

    return common.templates.TemplateResponse(request, "merge.html", ctx)


@router.post("/merge", response_model=None)
async def merge_confirm(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.RedirectResponse:
    """Execute account merge and redirect to account page."""
    session = request.state.session
    source_user_id = session.get("merge_source_user_id")
    if not source_user_id:
        return fastapi.responses.RedirectResponse(url="/account", status_code=307)

    await merge_module.merge_accounts(db, user_id, uuid.UUID(source_user_id))
    await db.commit()

    redis: session_module.RedisClient = request.app.state.redis
    await session_module.invalidate_user_sessions(redis, source_user_id)

    session["merge_source_user_id"] = None
    session["merge_service_type"] = None
    session["merge_connection_id"] = None

    return fastapi.responses.RedirectResponse(url="/account", status_code=303)
