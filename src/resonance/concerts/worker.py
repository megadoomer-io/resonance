"""Calendar feed sync task for the arq worker."""

from __future__ import annotations

import datetime
import typing
import uuid
from typing import Any

import httpx
import sqlalchemy as sa
import structlog

import resonance.concerts.ical as ical_module
import resonance.concerts.sync as concert_sync
import resonance.connectors.songkick as songkick_module
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module

if typing.TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

# Feed type to use when parsing each Songkick URL (positional — matches
# the order returned by derive_songkick_urls: attendance first, tracked second).
_SONGKICK_FEED_TYPES: list[types_module.FeedType] = [
    types_module.FeedType.SONGKICK_ATTENDANCE,
    types_module.FeedType.SONGKICK_TRACKED_ARTIST,
]


async def sync_calendar_feed(
    ctx: dict[str, Any], connection_id: str, task_id: str
) -> None:
    """Fetch, parse, and sync calendar feed(s) into the database.

    This is the main arq task for calendar feed syncing. For Songkick
    connections it fetches and processes both the attendance and
    tracked-artist feeds.  For iCal connections it fetches the single URL.

    Args:
        ctx: arq worker context dict (contains session_factory).
        connection_id: UUID string of the ServiceConnection.
        task_id: UUID string of the Task tracking this sync.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    log = logger.bind(connection_id=connection_id, task_id=task_id)

    async with session_factory() as session:
        task: task_models.Task | None = None
        try:
            # Load tracking task
            task = await _load_task(session, task_id)
            if task is None:
                log.error("calendar_sync_task_not_found")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            # Load the service connection
            connection = await _load_connection(session, connection_id)
            if connection is None:
                log.error("service_connection_not_found")
                await lifecycle_module.fail_task(
                    session,
                    task,
                    f"ServiceConnection {connection_id} not found",
                )
                await session.commit()
                return

            log = log.bind(
                user_id=str(connection.user_id),
                service_type=connection.service_type.value,
            )

            # Check enabled
            if not connection.enabled:
                log.info("connection_disabled_skip")
                await lifecycle_module.complete_task(
                    session, task, {"skipped": "connection disabled"}
                )
                await session.commit()
                return

            # Determine feed URLs and their corresponding FeedTypes
            source_service = connection.service_type
            feed_items: list[tuple[str, types_module.FeedType]] = []

            if source_service == types_module.ServiceType.SONGKICK:
                if connection.external_user_id is None:
                    await lifecycle_module.fail_task(
                        session,
                        task,
                        "Songkick connection has no external_user_id",
                    )
                    await session.commit()
                    return
                urls = songkick_module.derive_songkick_urls(connection.external_user_id)
                for url, feed_type in zip(urls, _SONGKICK_FEED_TYPES, strict=True):
                    feed_items.append((url, feed_type))
            elif source_service == types_module.ServiceType.ICAL:
                if connection.url is None:
                    await lifecycle_module.fail_task(
                        session, task, "iCal connection has no URL"
                    )
                    await session.commit()
                    return
                feed_items.append((connection.url, types_module.FeedType.ICAL_GENERIC))
            else:
                await lifecycle_module.fail_task(
                    session,
                    task,
                    f"Unsupported service type for calendar sync: "
                    f"{source_service.value}",
                )
                await session.commit()
                return

            # Process each feed URL
            log.info("calendar_feed_sync_started", feed_count=len(feed_items))
            events_created = 0
            events_updated = 0
            candidates_created = 0
            candidates_matched = 0
            total_events = 0

            async with httpx.AsyncClient() as client:
                for url, feed_type in feed_items:
                    log.info("fetching_feed", url=url, feed_type=feed_type.value)
                    response = await client.get(url)
                    response.raise_for_status()

                    parsed_events = ical_module.parse_ical_feed(
                        response.text, feed_type
                    )
                    total_events += len(parsed_events)

                    for parsed in parsed_events:
                        # Venue
                        venue = None
                        if parsed.venue is not None:
                            venue = await concert_sync.upsert_venue(
                                session, parsed.venue
                            )

                        # Event
                        event, created = await concert_sync.upsert_event(
                            session, parsed, source_service, venue
                        )
                        if created:
                            events_created += 1
                        else:
                            events_updated += 1

                        # Artist candidates
                        if parsed.artist_candidates:
                            new_candidates = await concert_sync.upsert_candidates(
                                session, event, parsed.artist_candidates
                            )
                            candidates_created += new_candidates

                        # Attendance
                        if parsed.attendance_status is not None:
                            await concert_sync.upsert_attendance(
                                session,
                                connection.user_id,
                                event,
                                parsed.attendance_status,
                                source_service,
                            )

                        # Match candidates to existing artists
                        matched = await concert_sync.match_candidates_to_artists(
                            session, event
                        )
                        candidates_matched += matched

            # Update connection last_synced_at
            connection.last_synced_at = datetime.datetime.now(datetime.UTC)

            # Build result summary
            result: dict[str, object] = {
                "events_created": events_created,
                "events_updated": events_updated,
                "candidates_created": candidates_created,
                "candidates_matched": candidates_matched,
                "total_events": total_events,
            }

            # Mark task completed via lifecycle helper
            await lifecycle_module.complete_task(session, task, result)
            await session.commit()

            log.info("calendar_feed_sync_completed", **result)

        except Exception:
            log.exception("calendar_feed_sync_failed")
            if task is not None:
                import traceback

                await lifecycle_module.fail_task(session, task, traceback.format_exc())
                await session.commit()


async def _load_task(
    session: sa_async.AsyncSession,
    task_id: str,
) -> task_models.Task | None:
    """Load a Task by ID."""
    result = await session.execute(
        sa.select(task_models.Task).where(task_models.Task.id == uuid.UUID(task_id))
    )
    return result.scalar_one_or_none()


async def _load_connection(
    session: sa_async.AsyncSession,
    connection_id: str,
) -> user_models.ServiceConnection | None:
    """Load a ServiceConnection by ID.

    Args:
        session: Active database session.
        connection_id: UUID string of the connection.

    Returns:
        The ServiceConnection, or None if not found.
    """
    result = await session.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.id == uuid.UUID(connection_id)
        )
    )
    return result.scalar_one_or_none()
