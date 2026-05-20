"""Concert Archives CSV upload API route."""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated, cast

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.concerts.concert_archives as concert_archives_module
import resonance.dependencies as deps_module
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module

logger = structlog.get_logger()

router = fastapi.APIRouter(
    prefix="/connections/concert-archives", tags=["concert-archives"]
)

_MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB


@router.post(
    "/upload",
    summary="Upload Concert Archives CSV",
    description=(
        "Upload a Concert Archives CSV export file to import concert history."
        " Creates a ServiceConnection if one does not exist, then enqueues"
        " a background import task. Requires session authentication."
    ),
)
async def upload_csv(
    request: fastapi.Request,
    file: fastapi.UploadFile,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    export_date: Annotated[str | None, fastapi.Form()] = None,
) -> dict[str, str]:
    """Upload a Concert Archives CSV export and start an import task.

    Args:
        request: The FastAPI request object (for arq_redis access).
        file: The uploaded CSV file.
        user_id: The authenticated user's ID.
        db: The async database session.
        export_date: Optional ISO date string for the export date.
            Falls back to filename detection, then today.

    Returns:
        A dict with status and task_id.

    Raises:
        HTTPException: 413 if file too large, 422 if invalid CSV or
            encoding, 409 if stale export or concurrent import.
    """
    # 1. Read file content and enforce size limit
    raw_content = await file.read()
    if len(raw_content) > _MAX_FILE_SIZE:
        raise fastapi.HTTPException(
            status_code=413,
            detail=(
                f"File too large. Maximum size is {_MAX_FILE_SIZE // (1024 * 1024)} MB."
            ),
        )

    # 2. Decode as UTF-8
    try:
        csv_text = raw_content.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise fastapi.HTTPException(
            status_code=422,
            detail="File is not valid UTF-8 text.",
        ) from exc

    # 3. Validate CSV by parsing
    try:
        parse_result = concert_archives_module.parse_csv(csv_text)
    except ValueError as exc:
        raise fastapi.HTTPException(
            status_code=422,
            detail=f"Invalid CSV: {exc}",
        ) from exc

    # 4. Resolve export date: form field > filename > today
    resolved_date = _resolve_export_date(export_date, file.filename)

    # 5. Find existing CONCERT_ARCHIVES connection for this user
    conn_stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type
        == types_module.ServiceType.CONCERT_ARCHIVES,
    )
    conn_result = await db.execute(conn_stmt)
    connection = conn_result.scalar_one_or_none()

    if connection is not None:
        # 6. Check for stale export
        # Concert Archives uses a flat watermark structure (not nested
        # like incremental sync watermarks), so we cast the value.
        last_export_raw = connection.sync_watermark.get("last_export_date")
        last_export = str(last_export_raw) if last_export_raw is not None else None
        if last_export is not None:
            last_date = datetime.date.fromisoformat(last_export)
            if resolved_date < last_date:
                raise fastapi.HTTPException(
                    status_code=409,
                    detail=(
                        "Stale export: uploaded file date"
                        f" ({resolved_date.isoformat()})"
                        f" is older than last import"
                        f" ({last_export})."
                        " Upload a newer export."
                    ),
                )
    else:
        # 7. Create new ServiceConnection
        urls = [e.external_url for e in parse_result.events if e.external_url]
        username = concert_archives_module.parse_username(file.filename or "", urls)
        connection = user_models.ServiceConnection(
            user_id=user_id,
            service_type=types_module.ServiceType.CONCERT_ARCHIVES,
            external_user_id=username,
        )
        db.add(connection)

    # 8. Check for concurrent import
    running_stmt = sa.select(task_models.Task).where(
        task_models.Task.user_id == user_id,
        task_models.Task.service_connection_id == connection.id,
        task_models.Task.task_type == types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
        task_models.Task.status.in_(
            [
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
            ]
        ),
    )
    running_result = await db.execute(running_stmt)
    if running_result.scalar_one_or_none() is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="An import is already in progress for Concert Archives.",
        )

    # 9. Update sync_watermark
    # Concert Archives uses a flat watermark (str values, not nested
    # dicts like incremental sync services).  The JSON column accepts
    # any shape at runtime; we cast to satisfy the Mapped type.
    connection.sync_watermark = cast(
        "dict[str, dict[str, object]]",
        {"last_export_date": resolved_date.isoformat()},
    )

    # 10. Create Task
    task = task_models.Task(
        user_id=user_id,
        service_connection_id=connection.id,
        task_type=types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
        status=types_module.SyncStatus.PENDING,
    )
    db.add(task)
    await db.commit()

    # 11. Enqueue arq job
    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "sync_concert_archives",
        str(task.id),
        csv_text,
        _job_id=f"sync_concert_archives:{task.id}",
    )

    logger.info(
        "concert_archives_import_started",
        task_id=str(task.id),
        connection_id=str(connection.id),
        export_date=resolved_date.isoformat(),
    )

    return {"status": "started", "task_id": str(task.id)}


def _resolve_export_date(form_value: str | None, filename: str | None) -> datetime.date:
    """Resolve the export date from form field, filename, or today.

    Args:
        form_value: The export_date form field value, if provided.
        filename: The uploaded filename for date detection.

    Returns:
        The resolved export date.
    """
    if form_value:
        try:
            return datetime.date.fromisoformat(form_value)
        except ValueError:
            pass

    if filename:
        detected = concert_archives_module.parse_export_date(filename)
        if detected is not None:
            return detected

    return datetime.date.today()
