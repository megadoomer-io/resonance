from __future__ import annotations

import pathlib
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import fastapi
import fastapi.requests
import fastapi.responses
import fastapi.templating
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.models.music as music_models
import resonance.models.sync as sync_models
import resonance.models.user as user_models
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

_TEMPLATE_DIR = pathlib.Path(__file__).resolve().parent.parent / "templates"
templates = fastapi.templating.Jinja2Templates(directory=str(_TEMPLATE_DIR))

router = fastapi.APIRouter(tags=["ui"])


@asynccontextmanager
async def _get_db(
    request: fastapi.Request,
) -> AsyncIterator[sa_async.AsyncSession]:
    """Yield a DB session from the app's session factory."""
    factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = (
        request.app.state.session_factory
    )
    async with factory() as db:
        yield db


async def _count(
    db: sa_async.AsyncSession,
    model: type[sa.orm.DeclarativeBase],
    *filters: sa.ColumnElement[bool],
) -> int:
    """Return the row count for *model*, optionally filtered."""
    stmt = sa.select(sa.func.count()).select_from(model)
    for f in filters:
        stmt = stmt.where(f)
    result = await db.execute(stmt)
    return int(result.scalar_one())


@router.get("/login", response_class=fastapi.responses.HTMLResponse)
async def login(request: fastapi.Request) -> fastapi.responses.HTMLResponse:
    """Render the login page."""
    return templates.TemplateResponse(request, "login.html")


@router.get("/", response_model=None)
async def dashboard(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render dashboard with stats and sync controls, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        artist_count = await _count(db, music_models.Artist)
        track_count = await _count(db, music_models.Track)
        event_count = await _count(
            db,
            music_models.ListeningEvent,
            music_models.ListeningEvent.user_id == user_uuid,
        )

        connections_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_uuid
            )
        )
        connections: Sequence[user_models.ServiceConnection] = (
            connections_result.scalars().all()
        )

        latest_sync_result = await db.execute(
            sa.select(sync_models.SyncJob)
            .where(sync_models.SyncJob.user_id == user_uuid)
            .order_by(sync_models.SyncJob.created_at.desc())
            .limit(1)
        )
        latest_sync: sync_models.SyncJob | None = (
            latest_sync_result.scalar_one_or_none()
        )

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "user_id": user_id,
            "artist_count": artist_count,
            "track_count": track_count,
            "event_count": event_count,
            "connections": connections,
            "latest_sync": latest_sync,
        },
    )


@router.get("/partials/sync-status", response_model=None)
async def sync_status_partial(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Return the sync status partial for HTMX polling."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        sync_jobs_result = await db.execute(
            sa.select(sync_models.SyncJob)
            .where(sync_models.SyncJob.user_id == user_uuid)
            .order_by(sync_models.SyncJob.created_at.desc())
            .limit(5)
        )
        sync_jobs: Sequence[sync_models.SyncJob] = sync_jobs_result.scalars().all()

    has_active_sync = any(
        j.status in (types_module.SyncStatus.PENDING, types_module.SyncStatus.RUNNING)
        for j in sync_jobs
    )

    return templates.TemplateResponse(
        request,
        "partials/sync_status.html",
        {"sync_jobs": sync_jobs, "has_active_sync": has_active_sync},
    )
