"""Calendar feed and Concert Archives sync tasks for the arq worker."""

from __future__ import annotations

import dataclasses
import datetime
import math
import typing
import uuid
from typing import Any

import httpx
import sqlalchemy as sa
import structlog

import resonance.concerts.concert_archives as concert_archives_module
import resonance.concerts.ical as ical_module
import resonance.concerts.sync as concert_sync
import resonance.connectors.songkick as songkick_module
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.normalize as normalize_module
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module

if typing.TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

_CHUNK_SIZE = 25


@dataclasses.dataclass
class EventProcessingResult:
    """Counters from processing a batch of parsed events."""

    events_created: int = 0
    events_updated: int = 0
    candidates_created: int = 0
    candidates_matched: int = 0


# Feed type to use when parsing each Songkick URL (positional — matches
# the order returned by derive_songkick_urls: attendance first, tracked second).
_SONGKICK_FEED_TYPES: list[types_module.FeedType] = [
    types_module.FeedType.SONGKICK_ATTENDANCE,
    types_module.FeedType.SONGKICK_TRACKED_ARTIST,
]


def _venue_external_id(venue_data: ical_module.VenueData) -> str:
    """Generate a deterministic external ID for a venue candidate."""
    name = normalize_module.normalize_name(venue_data.name)
    city = normalize_module.normalize_name(venue_data.city or "")
    return f"{name}_{city}" if city else name


async def _process_parsed_events(
    session: sa_async.AsyncSession,
    events: list[ical_module.ParsedEvent],
    source_service: types_module.ServiceType,
    user_id: uuid.UUID,
) -> EventProcessingResult:
    """Process a list of parsed events into the database.

    Shared by calendar feed sync and Concert Archives chunk processing.
    Upserts venues, events, artist candidates, attendance, and matches.
    """
    result = EventProcessingResult()

    for parsed in events:
        venue = None
        venue_candidate = None
        if parsed.venue is not None:
            venue_ext_id = _venue_external_id(parsed.venue)
            venue_candidate = await concert_sync.upsert_venue_candidate(
                session, parsed.venue, source_service, venue_ext_id
            )
            venue = await concert_sync.resolve_venue_candidate(session, venue_candidate)

        event_candidate = await concert_sync.upsert_event_candidate(
            session, parsed, source_service, venue_candidate
        )
        event, created = await concert_sync.resolve_event_candidate(
            session, event_candidate, venue
        )
        if created:
            result.events_created += 1
        else:
            result.events_updated += 1

        if parsed.artist_candidates:
            new_candidates = await concert_sync.upsert_candidates(
                session, event, parsed.artist_candidates
            )
            result.candidates_created += new_candidates

        if parsed.attendance_status is not None:
            await concert_sync.upsert_attendance(
                session, user_id, event, parsed.attendance_status, source_service
            )

        matched = await concert_sync.match_candidates_to_artists(session, event)
        result.candidates_matched += matched

    return result


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

            # Check if this task or its parent was cancelled
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("calendar_sync_cancelled")
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
            totals = EventProcessingResult()
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

                    batch = await _process_parsed_events(
                        session, parsed_events, source_service, connection.user_id
                    )
                    totals.events_created += batch.events_created
                    totals.events_updated += batch.events_updated
                    totals.candidates_created += batch.candidates_created
                    totals.candidates_matched += batch.candidates_matched

            # Update connection last_synced_at
            connection.last_synced_at = datetime.datetime.now(datetime.UTC)

            # Build result summary
            result: dict[str, object] = {
                "events_created": totals.events_created,
                "events_updated": totals.events_updated,
                "candidates_created": totals.candidates_created,
                "candidates_matched": totals.candidates_matched,
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


async def sync_concert_archives(
    ctx: dict[str, Any], task_id: str, csv_content: str | None = None
) -> None:
    """Plan a Concert Archives CSV import by creating chunk children.

    Parses the CSV, stores parsed events in the parent task's params,
    creates CONCERT_ARCHIVES_CHUNK children, and enqueues the first one.
    Each chunk processes a batch of events, freeing the worker slot between
    chunks so other sync jobs can run.

    When ``csv_content`` is ``None`` (orphan recovery), checks whether
    children already exist. If so, re-enqueues the first pending child.
    Otherwise, the CSV is lost and the task fails gracefully.

    Args:
        ctx: arq worker context dict (contains session_factory, arq_redis).
        task_id: UUID string of the Task tracking this import.
        csv_content: Raw CSV file content, or None on orphan recovery.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    arq_redis = ctx["redis"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_models.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("concert_archives_import_task_not_found")
                return

            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("concert_archives_import_cancelled")
                return

            # Check if children already exist (orphan recovery of a planned parent)
            children_result = await session.execute(
                sa.select(task_models.Task)
                .where(task_models.Task.parent_id == task.id)
                .order_by(task_models.Task.created_at)
            )
            existing_children = children_result.scalars().all()

            if existing_children:
                first_pending = next(
                    (
                        c
                        for c in existing_children
                        if c.status == types_module.SyncStatus.PENDING
                    ),
                    None,
                )
                if first_pending is not None:
                    await arq_redis.enqueue_job(
                        "sync_concert_archives_chunk",
                        str(first_pending.id),
                        _job_id=f"sync_concert_archives_chunk:{first_pending.id}",
                    )
                    log.info(
                        "orphan_recovery_resumed_chunk",
                        chunk_task_id=str(first_pending.id),
                    )
                else:
                    log.info("orphan_recovery_all_children_terminal")
                return

            if csv_content is None:
                log.warning("concert_archives_csv_unavailable", task_id=task_id)
                await lifecycle_module.fail_task(
                    session,
                    task,
                    "CSV content unavailable — please re-upload the file",
                )
                await session.commit()
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            connection = await _load_connection(
                session, str(task.service_connection_id)
            )
            if connection is None:
                log.error("service_connection_not_found")
                await lifecycle_module.fail_task(
                    session,
                    task,
                    f"ServiceConnection {task.service_connection_id} not found",
                )
                await session.commit()
                return

            log = log.bind(
                user_id=str(connection.user_id),
                service_type=connection.service_type.value,
            )

            if not connection.enabled:
                log.info("connection_disabled_skip")
                await lifecycle_module.complete_task(
                    session, task, {"skipped": "connection disabled"}
                )
                await session.commit()
                return

            log.info("concert_archives_import_planning")
            parse_result = concert_archives_module.parse_csv(csv_content)
            total_events = len(parse_result.events)

            # Store parsed events and warnings in parent task params
            task.params = {
                **(task.params or {}),
                "parsed_events": [
                    e.model_dump(mode="json") for e in parse_result.events
                ],
                "warnings": parse_result.warnings,
            }
            task.progress_total = total_events

            # Create chunk children
            num_chunks = max(1, math.ceil(total_events / _CHUNK_SIZE))
            first_child: task_models.Task | None = None
            for i in range(num_chunks):
                child = task_models.Task(
                    task_type=types_module.TaskType.CONCERT_ARCHIVES_CHUNK,
                    status=types_module.SyncStatus.PENDING,
                    user_id=task.user_id,
                    service_connection_id=task.service_connection_id,
                    parent_id=task.id,
                    params={"chunk_index": i, "chunk_size": _CHUNK_SIZE},
                    description=f"Import chunk {i + 1}/{num_chunks}",
                )
                session.add(child)
                if i == 0:
                    first_child = child

            await session.commit()

            # Enqueue the first chunk (sequential dispatch)
            if first_child is not None:
                await arq_redis.enqueue_job(
                    "sync_concert_archives_chunk",
                    str(first_child.id),
                    _job_id=f"sync_concert_archives_chunk:{first_child.id}",
                )

            log.info(
                "concert_archives_import_planned",
                total_events=total_events,
                num_chunks=num_chunks,
            )

        except Exception:
            log.exception("concert_archives_import_planning_failed")
            if task is not None:
                import traceback

                await lifecycle_module.fail_task(session, task, traceback.format_exc())
                await session.commit()


async def sync_concert_archives_chunk(ctx: dict[str, Any], task_id: str) -> None:
    """Process one chunk of a Concert Archives import.

    Reads the chunk's slice of parsed events from the parent task's
    params, processes them, and calls _check_parent_completion to
    dispatch the next chunk or complete the parent.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the chunk Task.
    """
    from resonance.worker import _check_parent_completion

    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    arq_redis = ctx["redis"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_models.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("concert_archives_chunk_task_not_found")
                return

            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            # Load parent to get parsed events
            parent = await _load_task(session, str(task.parent_id))
            if parent is None:
                log.error("chunk_parent_not_found")
                await lifecycle_module.fail_task(session, task, "Parent task not found")
                await session.commit()
                return

            parent_params = parent.params or {}
            raw = parent_params.get("parsed_events", [])
            parsed_events_raw: list[dict[str, Any]] = (
                list(raw) if isinstance(raw, list) else []
            )
            chunk_params = task.params or {}
            chunk_index = int(str(chunk_params.get("chunk_index", 0)))
            chunk_size = int(str(chunk_params.get("chunk_size", _CHUNK_SIZE)))

            start = chunk_index * chunk_size
            end = start + chunk_size
            chunk_data = parsed_events_raw[start:end]

            # Deserialize back to ParsedEvent objects
            chunk_events = [
                ical_module.ParsedEvent.model_validate(d) for d in chunk_data
            ]

            connection = await _load_connection(
                session, str(task.service_connection_id)
            )
            if connection is None:
                log.error("service_connection_not_found")
                await lifecycle_module.fail_task(
                    session, task, "ServiceConnection not found"
                )
                await session.commit()
                return

            log = log.bind(
                chunk_index=chunk_index,
                chunk_events=len(chunk_events),
                user_id=str(connection.user_id),
            )
            log.info("concert_archives_chunk_started")

            result = await _process_parsed_events(
                session,
                chunk_events,
                types_module.ServiceType.CONCERT_ARCHIVES,
                connection.user_id,
            )

            task_result: dict[str, object] = {
                "events_created": result.events_created,
                "events_updated": result.events_updated,
                "candidates_created": result.candidates_created,
                "candidates_matched": result.candidates_matched,
                "total_events": len(chunk_events),
            }

            await lifecycle_module.complete_task(session, task, task_result)
            await session.commit()

            log.info("concert_archives_chunk_completed", **task_result)

            await _check_parent_completion(session, task, arq_redis, log)

        except Exception:
            log.exception("concert_archives_chunk_failed")
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
