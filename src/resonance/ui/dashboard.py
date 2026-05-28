"""Dashboard and login routes."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.dependencies as deps_module
import resonance.models.music as music_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module
import resonance.ui.common as common

router = fastapi.APIRouter(tags=["ui"])


def _get_authn_connectors(
    request: fastapi.Request,
) -> list[base_module.BaseConnector]:
    """Return all connectors with AUTHN capability, ordered for the login page."""
    registry: registry_module.ConnectorRegistry = request.app.state.connector_registry
    return registry.get_by_capability(base_module.ConnectorCapability.AUTHN)


@router.get("/login", response_class=fastapi.responses.HTMLResponse)
async def login(
    request: fastapi.Request,
    prompt: str | None = None,
) -> fastapi.responses.Response:
    """Render the login page, or auto-redirect to the last auth service.

    If the user already has a valid session, redirect straight to the
    dashboard.  If a ``last_auth_service`` cookie is set (and
    ``prompt`` is not ``"select"``), redirect to that service's OAuth
    flow so expired sessions re-authenticate automatically.
    """
    if request.state.session.get("user_id"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    authn_connectors = _get_authn_connectors(request)
    authn_services = {c.service_type.value for c in authn_connectors}

    if prompt != "select":
        last_service = request.cookies.get("last_auth_service")
        if last_service and last_service in authn_services:
            return fastapi.responses.RedirectResponse(
                url=f"/api/v1/auth/{last_service}", status_code=307
            )

    return common.templates.TemplateResponse(
        request,
        "login.html",
        {"request": request, "authn_connectors": authn_connectors},
    )


@router.post("/view-as", response_class=fastapi.responses.HTMLResponse)
async def set_view_as(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    role: str = "",
) -> fastapi.responses.Response:
    """Set or clear view-as role impersonation (admin/owner only)."""
    session = request.state.session
    actual_role = session.get("user_role", "user")

    if actual_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    if not role or role == "reset":
        session.pop("view_as", None)
    elif common._ROLE_HIERARCHY.get(role, 0) < common._ROLE_HIERARCHY.get(
        actual_role, 0
    ):
        session["view_as"] = role
    else:
        raise fastapi.HTTPException(
            status_code=400, detail="Can only view as a lower role"
        )

    referer = request.headers.get("referer", "/")
    if not referer.startswith("/") or referer.startswith("//"):
        referer = "/"
    return fastapi.responses.RedirectResponse(url=referer, status_code=303)


@router.get("/", response_model=None)
async def dashboard(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Render dashboard with stats and sync controls."""
    artist_count = await common.count_rows(db, music_models.Artist)
    track_count = await common.count_rows(db, music_models.Track)
    event_count = await common.count_rows(
        db,
        music_models.ListeningEvent,
        music_models.ListeningEvent.user_id == user_id,
    )

    connections_result = await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_id
        )
    )
    connections = connections_result.scalars().all()

    latest_sync_result = await db.execute(
        sa.select(task_models.Task)
        .where(
            task_models.Task.user_id == user_id,
            task_models.Task.task_type.in_(
                [
                    types_module.TaskType.SYNC_JOB,
                    types_module.TaskType.CALENDAR_SYNC,
                    types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
                ]
            ),
        )
        .order_by(task_models.Task.created_at.desc())
        .limit(1)
    )
    latest_sync: task_models.Task | None = latest_sync_result.scalar_one_or_none()

    conn_ids = [conn.id for conn in connections]
    active_syncs: dict[str, task_models.Task] = {}
    if conn_ids:
        active_stmt = sa.select(task_models.Task).where(
            task_models.Task.user_id == user_id,
            task_models.Task.service_connection_id.in_(conn_ids),
            task_models.Task.status.in_(
                [
                    types_module.SyncStatus.PENDING,
                    types_module.SyncStatus.RUNNING,
                    types_module.SyncStatus.DEFERRED,
                ]
            ),
        )
        active_result = await db.execute(active_stmt)
        for active_task in active_result.scalars().all():
            active_syncs[str(active_task.service_connection_id)] = active_task

    registry: registry_module.ConnectorRegistry = request.app.state.connector_registry
    syncable_services = {
        c.service_type
        for c in registry.all()
        if c.connection_config().sync_function is not None
    }

    ctx = common.base_context(request)
    ctx.update(
        artist_count=artist_count,
        track_count=track_count,
        event_count=event_count,
        connections=connections,
        latest_sync=latest_sync,
        active_syncs=active_syncs,
        syncable_services=syncable_services,
    )

    return common.templates.TemplateResponse(request, "dashboard.html", ctx)
