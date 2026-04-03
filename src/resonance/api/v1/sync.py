"""Sync API routes — trigger and monitor data synchronization jobs."""

from __future__ import annotations

import datetime
import uuid  # noqa: TC003 - runtime import required for FastAPI dependency resolution
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.dependencies as deps_module
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module

logger = structlog.get_logger()

router = fastapi.APIRouter(prefix="/sync", tags=["sync"])


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
) -> dict[str, str]:
    """Trigger a full data sync for the given service.

    Args:
        service: The service name (e.g., "spotify").
        request: The FastAPI request object.
        user_id: The authenticated user's ID.
        db: The async database session.

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
    await arq_redis.enqueue_job("plan_sync", str(task.id))

    return {"status": "started", "sync_task_id": str(task.id)}


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
