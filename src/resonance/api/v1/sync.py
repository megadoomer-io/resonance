"""Sync API routes — trigger and monitor data synchronization jobs."""

from __future__ import annotations

import asyncio
import datetime
import logging
import uuid
from typing import Annotated, Any

import fastapi
import httpx
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.crypto as crypto_module
import resonance.dependencies as deps_module
import resonance.models.sync as sync_models
import resonance.models.user as user_models
import resonance.sync.runner as runner_module
import resonance.types as types_module

logger = logging.getLogger(__name__)

router = fastapi.APIRouter(prefix="/sync", tags=["sync"])

# Set of strong references to background sync tasks to prevent garbage collection.
_background_tasks: set[asyncio.Task[None]] = set()


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
        A dict with status and sync_job_id.

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
    running_stmt = sa.select(sync_models.SyncJob).where(
        sync_models.SyncJob.user_id == user_id,
        sync_models.SyncJob.service_connection_id == connection.id,
        sync_models.SyncJob.status.in_(
            [
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
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

    # Create SyncJob
    job = sync_models.SyncJob(
        id=uuid.uuid4(),
        user_id=user_id,
        service_connection_id=connection.id,
        sync_type=types_module.SyncType.FULL,
        status=types_module.SyncStatus.PENDING,
    )
    db.add(job)
    await db.commit()

    # Decrypt access token
    settings = request.app.state.settings
    access_token = crypto_module.decrypt_token(
        connection.encrypted_access_token, settings.token_encryption_key
    )

    # Get connector from registry
    registry: registry_module.ConnectorRegistry = request.app.state.connector_registry
    connector = registry.get(service_type)
    if connector is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"No connector registered for {service_type.value}",
        )

    # Refresh token if expired
    if (
        connection.token_expires_at is not None
        and connection.token_expires_at < datetime.datetime.now(datetime.UTC)
        and connection.encrypted_refresh_token is not None
        and hasattr(connector, "refresh_access_token")
    ):
        refresh_token = crypto_module.decrypt_token(
            connection.encrypted_refresh_token, settings.token_encryption_key
        )
        try:
            new_tokens: base_module.TokenResponse = (
                await connector.refresh_access_token(refresh_token)
            )
            access_token = new_tokens.access_token
            connection.encrypted_access_token = crypto_module.encrypt_token(
                new_tokens.access_token, settings.token_encryption_key
            )
            if new_tokens.refresh_token:
                connection.encrypted_refresh_token = crypto_module.encrypt_token(
                    new_tokens.refresh_token, settings.token_encryption_key
                )
            if new_tokens.expires_in:
                connection.token_expires_at = datetime.datetime.now(
                    datetime.UTC
                ) + datetime.timedelta(seconds=new_tokens.expires_in)
            await db.commit()
            logger.info("Refreshed expired token for %s", service)
        except httpx.HTTPStatusError:
            logger.warning(
                "Failed to refresh token for %s — using existing token",
                service,
            )

    # Launch background task with its own db session
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = (
        request.app.state.session_factory
    )
    job_id = job.id

    async def _run_background_sync() -> None:
        async with session_factory() as bg_session:
            bg_stmt = sa.select(sync_models.SyncJob).where(
                sync_models.SyncJob.id == job_id
            )
            bg_result = await bg_session.execute(bg_stmt)
            bg_job = bg_result.scalar_one_or_none()
            if bg_job is not None:
                await runner_module.run_sync(
                    bg_job,
                    connector,  # type: ignore[arg-type]
                    bg_session,
                    access_token,
                )

    task = asyncio.create_task(_run_background_sync())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)

    return {"status": "started", "sync_job_id": str(job_id)}


@router.post("/cancel/{job_id}")
async def cancel_sync(
    job_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Cancel a pending or running sync job."""
    import datetime

    stmt = sa.select(sync_models.SyncJob).where(
        sync_models.SyncJob.id == job_id,
        sync_models.SyncJob.user_id == user_id,
    )
    result = await db.execute(stmt)
    job = result.scalar_one_or_none()

    if job is None:
        raise fastapi.HTTPException(status_code=404, detail="Sync job not found")

    if job.status not in (
        types_module.SyncStatus.PENDING,
        types_module.SyncStatus.RUNNING,
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
    """Get recent sync job status for the authenticated user.

    Args:
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A list of sync job status dicts, most recent first.
    """
    stmt = (
        sa.select(sync_models.SyncJob)
        .where(sync_models.SyncJob.user_id == user_id)
        .order_by(sync_models.SyncJob.created_at.desc())
        .limit(10)
    )
    result = await db.execute(stmt)
    jobs = result.scalars().all()

    return [
        {
            "id": str(job.id),
            "status": str(job.status),
            "sync_type": str(job.sync_type),
            "progress_current": job.progress_current,
            "progress_total": job.progress_total,
            "items_created": job.items_created,
            "items_updated": job.items_updated,
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
