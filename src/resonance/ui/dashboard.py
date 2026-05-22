"""Dashboard and login routes."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dependencies as deps_module
import resonance.models.music as music_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module
import resonance.ui.common as common

router = fastapi.APIRouter(tags=["ui"])


@router.get("/login", response_class=fastapi.responses.HTMLResponse)
async def login(request: fastapi.Request) -> fastapi.responses.HTMLResponse:
    """Render the login page."""
    return common.templates.TemplateResponse(request, "login.html")


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

    ctx = common.base_context(request)
    ctx.update(
        artist_count=artist_count,
        track_count=track_count,
        event_count=event_count,
        connections=connections,
        latest_sync=latest_sync,
        active_syncs=active_syncs,
    )

    return common.templates.TemplateResponse(request, "dashboard.html", ctx)
