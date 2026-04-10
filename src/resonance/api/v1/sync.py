"""Sync API routes — trigger and monitor data synchronization jobs."""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated, Any

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.dependencies as deps_module
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module

logger = structlog.get_logger()

router = fastapi.APIRouter(prefix="/sync", tags=["sync"])


class SyncRequest(pydantic.BaseModel):
    """Optional body for POST /sync/{service}."""

    sync_from: str | None = None
    """ISO 8601 date/datetime or unix timestamp. Overrides the watermark
    so the sync fetches data from this point forward. If set to the
    empty string or "full", clears all watermarks for a full re-sync."""


def _parse_service_type(service: str) -> types_module.ServiceType:
    """Parse a service string to ServiceType enum, raising 404 if unknown."""
    try:
        return types_module.ServiceType(service)
    except ValueError as exc:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Unknown service: {service}"
        ) from exc


@router.post("/{service}")
async def trigger_sync(
    service: str,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    body: SyncRequest | None = None,
) -> dict[str, str]:
    """Trigger a data sync for the given service.

    Optionally accepts a JSON body with ``sync_from`` to override the
    stored watermark.  Pass ``"full"`` or ``""`` to clear all watermarks
    and force a complete re-sync.

    Args:
        service: The service name (e.g., "spotify").
        request: The FastAPI request object.
        user_id: The authenticated user's ID.
        db: The async database session.
        body: Optional sync request body with watermark override.

    Returns:
        A dict with status and sync_task_id.

    Raises:
        HTTPException: 404 if unknown service, 400 if no connection,
            409 if sync already running.
    """
    service_type = _parse_service_type(service)

    # Find user's ServiceConnection for this service
    conn_stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type == service_type,
    )
    conn_result = await db.execute(conn_stmt)
    connection = conn_result.scalar_one_or_none()

    if connection is None:
        raise fastapi.HTTPException(
            status_code=400,
            detail=f"No connection found for service {service}",
        )

    # Check for already-running sync (PENDING or RUNNING)
    running_stmt = sa.select(task_models.SyncTask).where(
        task_models.SyncTask.user_id == user_id,
        task_models.SyncTask.service_connection_id == connection.id,
        task_models.SyncTask.task_type == types_module.SyncTaskType.SYNC_JOB,
        task_models.SyncTask.status.in_(
            [
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
                types_module.SyncStatus.DEFERRED,
            ]
        ),
    )
    running_result = await db.execute(running_stmt)
    existing_job = running_result.scalar_one_or_none()

    if existing_job is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="A sync is already running for this service",
        )

    # Handle watermark override
    if body is not None and body.sync_from is not None:
        _apply_watermark_override(connection, body.sync_from)

    # Create SyncTask
    task = task_models.SyncTask(
        user_id=user_id,
        service_connection_id=connection.id,
        task_type=types_module.SyncTaskType.SYNC_JOB,
        status=types_module.SyncStatus.PENDING,
    )
    db.add(task)
    await db.commit()

    # Enqueue arq job for background processing
    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "plan_sync", str(task.id), _job_id=f"plan_sync:{task.id}"
    )

    return {"status": "started", "sync_task_id": str(task.id)}


def _apply_watermark_override(
    connection: user_models.ServiceConnection, sync_from: str
) -> None:
    """Clear or override the connection's sync watermark.

    Args:
        connection: The service connection to modify.
        sync_from: Either "full"/"" for a complete reset, or an ISO 8601
            date/datetime/unix timestamp to set as the new watermark.
    """
    if sync_from in ("", "full"):
        connection.sync_watermark = {}
        logger.info(
            "watermark_cleared",
            connection_id=str(connection.id),
        )
        return

    # Parse as unix timestamp or ISO 8601
    watermark_ts: int | None = None
    try:
        watermark_ts = int(sync_from)
    except ValueError:
        try:
            dt = datetime.datetime.fromisoformat(sync_from)
            watermark_ts = int(dt.timestamp())
        except ValueError:
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Invalid sync_from value: {sync_from!r}. "
                "Use ISO 8601 date, unix timestamp, or 'full'.",
            ) from None

    # Build service-appropriate watermark
    if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
        connection.sync_watermark = {
            "listens": {"last_listened_at": watermark_ts},
        }
    elif connection.service_type == types_module.ServiceType.SPOTIFY:
        iso_str = datetime.datetime.fromtimestamp(
            watermark_ts, tz=datetime.UTC
        ).isoformat()
        connection.sync_watermark = {
            "recently_played": {"last_played_at": iso_str},
            "saved_tracks": {"last_saved_at": iso_str},
            # followed_artists intentionally omitted — always full-fetches
        }
    else:
        connection.sync_watermark = {}

    logger.info(
        "watermark_overridden",
        connection_id=str(connection.id),
        sync_from=sync_from,
    )


@router.post("/cancel/{job_id}")
async def cancel_sync(
    job_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Cancel a pending or running sync task."""
    stmt = sa.select(task_models.SyncTask).where(
        task_models.SyncTask.id == job_id,
        task_models.SyncTask.user_id == user_id,
    )
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()

    if job is None:
        raise fastapi.HTTPException(status_code=404, detail="Sync task not found")

    if job.status not in (
        types_module.SyncStatus.PENDING,
        types_module.SyncStatus.RUNNING,
        types_module.SyncStatus.DEFERRED,
    ):
        raise fastapi.HTTPException(status_code=400, detail="Job is already finished")

    job.status = types_module.SyncStatus.FAILED
    job.error_message = "Cancelled by user"
    job.completed_at = datetime.datetime.now(datetime.UTC)
    await db.commit()

    return {"status": "cancelled", "job_id": str(job_id)}


@router.get("/status")
async def sync_status(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[dict[str, Any]]:
    """Get recent sync task status for the authenticated user.

    Args:
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A list of sync task status dicts, most recent first.
    """
    stmt = (
        sa.select(task_models.SyncTask)
        .where(
            task_models.SyncTask.user_id == user_id,
            task_models.SyncTask.task_type == types_module.SyncTaskType.SYNC_JOB,
        )
        .order_by(task_models.SyncTask.created_at.desc())
        .limit(10)
    )
    result = await db.execute(stmt)
    jobs = result.scalars().all()

    return [
        {
            "id": str(job.id),
            "status": str(job.status),
            "task_type": str(job.task_type),
            "progress_current": job.progress_current,
            "progress_total": job.progress_total,
            "items_created": job.result.get("items_created", 0)
            if isinstance(job.result, dict)
            else 0,
            "items_updated": job.result.get("items_updated", 0)
            if isinstance(job.result, dict)
            else 0,
            "description": job.description,
            "deferred_until": (
                job.deferred_until.isoformat()
                if job.deferred_until is not None
                else None
            ),
            "error_message": job.error_message,
            "started_at": (
                job.started_at.isoformat() if job.started_at is not None else None
            ),
            "completed_at": (
                job.completed_at.isoformat() if job.completed_at is not None else None
            ),
        }
        for job in jobs
    ]
