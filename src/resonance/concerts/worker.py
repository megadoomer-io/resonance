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
import resonance.models.concert as concert_models
import resonance.types as types_module

if typing.TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

# Map feed_type to source_service
_FEED_TYPE_TO_SERVICE: dict[types_module.FeedType, types_module.ServiceType] = {
    types_module.FeedType.SONGKICK_ATTENDANCE: types_module.ServiceType.SONGKICK,
    types_module.FeedType.SONGKICK_TRACKED_ARTIST: types_module.ServiceType.SONGKICK,
    types_module.FeedType.ICAL_GENERIC: types_module.ServiceType.ICAL,
}


async def sync_calendar_feed(ctx: dict[str, Any], feed_id: str) -> None:
    """Fetch, parse, and sync a calendar feed into the database.

    This is the main arq task for calendar feed syncing. It loads the feed
    configuration, fetches the iCal data via HTTP, parses it, and upserts
    venues, events, artist candidates, and attendance records.

    Args:
        ctx: arq worker context dict (contains session_factory).
        feed_id: UUID string of the UserCalendarFeed to sync.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    log = logger.bind(feed_id=feed_id)

    async with session_factory() as session:
        try:
            # 1. Load feed
            feed = await _load_feed(session, feed_id)
            if feed is None:
                log.error("calendar_feed_not_found")
                return

            log = log.bind(
                user_id=str(feed.user_id),
                feed_type=feed.feed_type.value,
            )

            # 2. Check enabled
            if not feed.enabled:
                log.info("calendar_feed_disabled_skip")
                return

            # 3. HTTP GET the feed URL
            log.info("calendar_feed_sync_started", url=feed.url)
            async with httpx.AsyncClient() as client:
                response = await client.get(feed.url)
                response.raise_for_status()

            # 4. Parse iCal
            parsed_events = ical_module.parse_ical_feed(response.text, feed.feed_type)

            # 5. Determine source service
            source_service = _FEED_TYPE_TO_SERVICE[feed.feed_type]

            # 6. Process each parsed event
            events_created = 0
            events_updated = 0
            candidates_created = 0
            candidates_matched = 0

            for parsed in parsed_events:
                # 6a. Venue
                venue: concert_models.Venue | None = None
                if parsed.venue is not None:
                    venue = await concert_sync.upsert_venue(session, parsed.venue)

                # 6b. Event
                event, created = await concert_sync.upsert_event(
                    session, parsed, source_service, venue
                )
                if created:
                    events_created += 1
                else:
                    events_updated += 1

                # 6c. Artist candidates
                if parsed.artist_candidates:
                    new_candidates = await concert_sync.upsert_candidates(
                        session, event, parsed.artist_candidates
                    )
                    candidates_created += new_candidates

                # 6d. Attendance
                if parsed.attendance_status is not None:
                    await concert_sync.upsert_attendance(
                        session,
                        feed.user_id,
                        event,
                        parsed.attendance_status,
                        source_service,
                    )

                # 6e. Match candidates to existing artists
                matched = await concert_sync.match_candidates_to_artists(session, event)
                candidates_matched += matched

            # 7. Update last_synced_at
            feed.last_synced_at = datetime.datetime.now(datetime.UTC)

            # 8. Commit
            await session.commit()

            # 9. Log summary
            log.info(
                "calendar_feed_sync_completed",
                events_created=events_created,
                events_updated=events_updated,
                candidates_created=candidates_created,
                candidates_matched=candidates_matched,
                total_events=len(parsed_events),
            )

        except Exception:
            log.exception("calendar_feed_sync_failed")


async def _load_feed(
    session: sa_async.AsyncSession,
    feed_id: str,
) -> concert_models.UserCalendarFeed | None:
    """Load a UserCalendarFeed by ID.

    Args:
        session: Active database session.
        feed_id: UUID string of the feed.

    Returns:
        The UserCalendarFeed, or None if not found.
    """
    result = await session.execute(
        sa.select(concert_models.UserCalendarFeed).where(
            concert_models.UserCalendarFeed.id == uuid.UUID(feed_id)
        )
    )
    return result.scalar_one_or_none()
