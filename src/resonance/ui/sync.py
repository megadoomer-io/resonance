"""Sync routes: Songkick connect, Concert Archives, sync status."""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated

import fastapi
import fastapi.responses
import httpx
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.dependencies as deps_module
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module
import resonance.ui.common as common

router = fastapi.APIRouter(tags=["ui"])


# ---------------------------------------------------------------------------
# Songkick connect flow
# ---------------------------------------------------------------------------


@router.get("/partials/songkick-connect", response_model=None)
async def songkick_connect_button(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
) -> fastapi.responses.HTMLResponse:
    """Return the Songkick connect button partial."""
    return common.templates.TemplateResponse(
        request,
        "partials/songkick_connect.html",
        {"state": "button"},
    )


@router.get("/partials/songkick-lookup", response_model=None)
async def songkick_lookup_form(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
) -> fastapi.responses.HTMLResponse:
    """Return the Songkick username lookup form partial."""
    return common.templates.TemplateResponse(
        request,
        "partials/songkick_connect.html",
        {"state": "form"},
    )


@router.post("/partials/songkick-lookup", response_model=None)
async def songkick_lookup_submit(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
) -> fastapi.responses.HTMLResponse:
    """Validate a Songkick username and return confirm or error state."""
    form = await request.form()
    username = str(form.get("username", "")).strip()
    if not username:
        return common.templates.TemplateResponse(
            request,
            "partials/songkick_connect.html",
            {"state": "error", "error_message": "Please enter a username."},
        )

    base = f"https://www.songkick.com/users/{username}/calendars.ics"
    try:
        async with httpx.AsyncClient() as client:
            att_resp = await client.get(f"{base}?filter=attendance")
            att_resp.raise_for_status()
            trk_resp = await client.get(f"{base}?filter=tracked_artist")
            trk_resp.raise_for_status()
    except httpx.HTTPStatusError:
        return common.templates.TemplateResponse(
            request,
            "partials/songkick_connect.html",
            {"state": "error"},
        )
    except httpx.ConnectError:
        return common.templates.TemplateResponse(
            request,
            "partials/songkick_connect.html",
            {
                "state": "error",
                "error_message": (
                    "Could not connect to Songkick. Please try again later."
                ),
            },
        )

    return common.templates.TemplateResponse(
        request,
        "partials/songkick_connect.html",
        {
            "state": "confirm",
            "username": username,
            "plans_count": att_resp.text.count("BEGIN:VEVENT"),
            "tracked_artist_count": trk_resp.text.count("BEGIN:VEVENT"),
        },
    )


@router.post("/partials/songkick-confirm", response_model=None)
async def songkick_confirm(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Create a Songkick ServiceConnection and reload the page."""
    form = await request.form()
    username = str(form.get("username", "")).strip()
    if not username:
        return fastapi.responses.HTMLResponse("")

    dup_stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type == types_module.ServiceType.SONGKICK,
        user_models.ServiceConnection.external_user_id == username,
    )
    dup_result = await db.execute(dup_stmt)
    if dup_result.scalar_one_or_none() is not None:
        msg = "Songkick connection already exists for this username."
        return fastapi.responses.HTMLResponse(f"<p><mark>{msg}</mark></p>")

    conn = user_models.ServiceConnection(
        user_id=user_id,
        service_type=types_module.ServiceType.SONGKICK,
        external_user_id=username,
        enabled=True,
    )
    db.add(conn)
    await db.commit()

    return fastapi.responses.HTMLResponse("<script>location.reload()</script>")


@router.post("/partials/songkick-sync/{username}", response_model=None)
async def songkick_sync_trigger(
    username: str,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Trigger sync for a Songkick connection by username."""
    result = await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_id,
            user_models.ServiceConnection.service_type
            == types_module.ServiceType.SONGKICK,
            user_models.ServiceConnection.external_user_id == username,
        )
    )
    connection = result.scalar_one_or_none()
    if connection is None:
        raise fastapi.HTTPException(status_code=404)

    task = task_models.Task(
        user_id=user_id,
        service_connection_id=connection.id,
        task_type=types_module.TaskType.CALENDAR_SYNC,
        status=types_module.SyncStatus.PENDING,
    )
    db.add(task)
    await db.flush()

    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "sync_calendar_feed",
        str(connection.id),
        str(task.id),
        _job_id=f"sync_calendar_feed:{task.id}",
    )
    await db.commit()

    return fastapi.responses.HTMLResponse("")


# ---------------------------------------------------------------------------
# Concert Archives connect
# ---------------------------------------------------------------------------


@router.get("/partials/concert-archives-connect", response_model=None)
async def concert_archives_connect_button(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
) -> fastapi.responses.HTMLResponse:
    """Return the Concert Archives connect button partial."""
    return common.templates.TemplateResponse(
        request,
        "partials/concert_archives_connect.html",
        {"state": "button"},
    )


@router.get("/partials/concert-archives-upload", response_model=None)
async def concert_archives_upload_form(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
) -> fastapi.responses.HTMLResponse:
    """Return the Concert Archives CSV upload form partial."""
    return common.templates.TemplateResponse(
        request,
        "partials/concert_archives_connect.html",
        {"state": "form"},
    )


# ---------------------------------------------------------------------------
# Sync status
# ---------------------------------------------------------------------------


@router.get("/partials/sync-status", response_model=None)
async def sync_status_partial(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Return the sync status partial for HTMX polling."""
    sync_jobs_result = await db.execute(
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
        .options(
            sa_orm.joinedload(task_models.Task.service_connection),
            sa_orm.subqueryload(task_models.Task.children),
        )
        .limit(5)
    )
    sync_jobs = sync_jobs_result.scalars().all()

    for job in sync_jobs:
        if job.status in (
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.RUNNING,
            types_module.SyncStatus.DEFERRED,
        ):
            child_total = sum(child.progress_current for child in job.children)
            if child_total:
                job.progress_current = int(child_total)

    has_active_sync = any(
        j.status
        in (
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.RUNNING,
            types_module.SyncStatus.DEFERRED,
        )
        for j in sync_jobs
    )

    return common.templates.TemplateResponse(
        request,
        "partials/sync_status.html",
        {
            "user_tz": request.state.session.get("user_tz"),
            "sync_jobs": sync_jobs,
            "has_active_sync": has_active_sync,
            "now": datetime.datetime.now(datetime.UTC),
        },
    )
