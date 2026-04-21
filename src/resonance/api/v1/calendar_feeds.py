"""Calendar feed management API routes."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.task as task_models
import resonance.types as types_module

logger = structlog.get_logger()

router = fastapi.APIRouter(prefix="/calendar-feeds", tags=["calendar-feeds"])


# ---------------------------------------------------------------------------
# Pydantic request/response models
# ---------------------------------------------------------------------------


class SongkickFeedRequest(pydantic.BaseModel):
    """Request body for creating Songkick calendar feeds."""

    username: str


class GenericFeedRequest(pydantic.BaseModel):
    """Request body for creating a generic iCal feed."""

    url: str
    label: str | None = None


class FeedResponse(pydantic.BaseModel):
    """Response model for a calendar feed."""

    id: str
    feed_type: str
    url: str
    label: str | None
    enabled: bool
    last_synced_at: str | None


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _feed_to_response(feed: concert_models.UserCalendarFeed) -> FeedResponse:
    """Convert a UserCalendarFeed ORM object to a FeedResponse.

    Args:
        feed: The ORM model instance.

    Returns:
        A serialisable FeedResponse.
    """
    return FeedResponse(
        id=str(feed.id),
        feed_type=str(feed.feed_type),
        url=feed.url,
        label=feed.label,
        enabled=feed.enabled if feed.enabled is not None else True,
        last_synced_at=(
            feed.last_synced_at.isoformat() if feed.last_synced_at is not None else None
        ),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/songkick",
    summary="Add Songkick calendar feeds",
    description=(
        "Creates attendance and tracked-artist calendar feeds "
        "for the given Songkick username. Requires session authentication."
    ),
)
async def add_songkick_feeds(
    body: SongkickFeedRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[FeedResponse]:
    """Create two Songkick calendar feeds (attendance + tracked artist).

    Args:
        body: Request containing the Songkick username.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A list of two created FeedResponse objects.

    Raises:
        HTTPException: 409 if feeds already exist for these URLs.
    """
    base = f"https://www.songkick.com/users/{body.username}/calendars.ics"
    feed_specs: list[tuple[types_module.FeedType, str]] = [
        (types_module.FeedType.SONGKICK_ATTENDANCE, f"{base}?filter=attendance"),
        (
            types_module.FeedType.SONGKICK_TRACKED_ARTIST,
            f"{base}?filter=tracked_artist",
        ),
    ]

    # Check for duplicates
    for _feed_type, url in feed_specs:
        stmt = sa.select(concert_models.UserCalendarFeed).where(
            concert_models.UserCalendarFeed.user_id == user_id,
            concert_models.UserCalendarFeed.url == url,
        )
        result = await db.execute(stmt)
        if result.scalar_one_or_none() is not None:
            raise fastapi.HTTPException(
                status_code=409,
                detail="Songkick feeds already exist for this username",
            )

    # Create feeds
    feeds: list[concert_models.UserCalendarFeed] = []
    for feed_type, url in feed_specs:
        feed = concert_models.UserCalendarFeed(
            user_id=user_id,
            feed_type=feed_type,
            url=url,
        )
        db.add(feed)
        feeds.append(feed)

    await db.commit()
    return [_feed_to_response(f) for f in feeds]


@router.post(
    "/ical",
    summary="Add generic iCal feed",
    description=(
        "Creates a generic iCal calendar feed from a URL. "
        "Requires session authentication."
    ),
)
async def add_generic_feed(
    body: GenericFeedRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> FeedResponse:
    """Create a generic iCal calendar feed.

    Args:
        body: Request containing the feed URL and optional label.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        The created FeedResponse.

    Raises:
        HTTPException: 409 if a feed already exists for this URL.
    """
    # Check for duplicate
    stmt = sa.select(concert_models.UserCalendarFeed).where(
        concert_models.UserCalendarFeed.user_id == user_id,
        concert_models.UserCalendarFeed.url == body.url,
    )
    result = await db.execute(stmt)
    if result.scalar_one_or_none() is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="A feed already exists for this URL",
        )

    feed = concert_models.UserCalendarFeed(
        user_id=user_id,
        feed_type=types_module.FeedType.ICAL_GENERIC,
        url=body.url,
        label=body.label,
    )
    db.add(feed)
    await db.commit()

    return _feed_to_response(feed)


@router.get(
    "",
    summary="List calendar feeds",
    description=(
        "Returns all calendar feeds for the authenticated user. "
        "Requires session authentication."
    ),
)
async def list_feeds(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[FeedResponse]:
    """List all calendar feeds for the authenticated user.

    Args:
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A list of FeedResponse objects.
    """
    stmt = sa.select(concert_models.UserCalendarFeed).where(
        concert_models.UserCalendarFeed.user_id == user_id,
    )
    result = await db.execute(stmt)
    feeds = result.scalars().all()
    return [_feed_to_response(feed) for feed in feeds]


@router.delete(
    "/{feed_id}",
    summary="Delete calendar feed",
    description=(
        "Deletes a calendar feed. Returns 404 if feed is not found "
        "or not owned by the authenticated user. "
        "Requires session authentication."
    ),
)
async def delete_feed(
    feed_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Delete a calendar feed owned by the authenticated user.

    Args:
        feed_id: The UUID of the feed to delete.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with status "deleted".

    Raises:
        HTTPException: 404 if feed not found or not owned by user.
    """
    stmt = sa.select(concert_models.UserCalendarFeed).where(
        concert_models.UserCalendarFeed.id == feed_id,
        concert_models.UserCalendarFeed.user_id == user_id,
    )
    result = await db.execute(stmt)
    feed = result.scalar_one_or_none()

    if feed is None:
        raise fastapi.HTTPException(status_code=404, detail="Feed not found")

    await db.delete(feed)
    await db.commit()

    return {"status": "deleted"}


@router.post(
    "/{feed_id}/sync",
    summary="Trigger calendar feed sync",
    description=(
        "Creates a task and enqueues a background job to sync a calendar feed. "
        "Returns 409 if a sync is already running for this feed. "
        "Requires session authentication."
    ),
)
async def trigger_feed_sync(
    feed_id: uuid.UUID,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Trigger a sync for a calendar feed.

    Args:
        feed_id: The UUID of the feed to sync.
        request: The FastAPI request object.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with status "started" and the task_id.

    Raises:
        HTTPException: 404 if feed not found or not owned by user,
            409 if sync already running.
    """
    # Find feed
    stmt = sa.select(concert_models.UserCalendarFeed).where(
        concert_models.UserCalendarFeed.id == feed_id,
        concert_models.UserCalendarFeed.user_id == user_id,
    )
    result = await db.execute(stmt)
    feed = result.scalar_one_or_none()

    if feed is None:
        raise fastapi.HTTPException(status_code=404, detail="Feed not found")

    # Check for already-running sync
    running_stmt = sa.select(task_models.Task).where(
        task_models.Task.user_id == user_id,
        task_models.Task.task_type == types_module.TaskType.CALENDAR_SYNC,
        task_models.Task.status.in_(
            [
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
                types_module.SyncStatus.DEFERRED,
            ]
        ),
        task_models.Task.params["feed_id"].as_string() == str(feed_id),
    )
    running_result = await db.execute(running_stmt)
    existing_job = running_result.scalar_one_or_none()

    if existing_job is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="A sync is already running for this feed",
        )

    # Create Task
    task = task_models.Task(
        user_id=user_id,
        task_type=types_module.TaskType.CALENDAR_SYNC,
        status=types_module.SyncStatus.PENDING,
        params={"feed_id": str(feed_id)},
    )
    db.add(task)
    await db.commit()

    # Enqueue arq job
    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "sync_calendar_feed",
        str(feed_id),
        _job_id=f"sync_calendar_feed:{feed_id}",
    )

    return {"status": "started", "task_id": str(task.id)}
