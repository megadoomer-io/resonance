"""ListenBrainz sync strategy implementation."""

from __future__ import annotations

import asyncio
import datetime
from typing import TYPE_CHECKING

import httpx
import structlog

import resonance.connectors.base as connector_base
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.models.task as task_module
import resonance.sync.base as sync_base
import resonance.sync.runner as runner_module
import resonance.types as types_module

if TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

    import resonance.models.user as user_models

logger = structlog.get_logger()

MAX_PAGES = 5000
_DEFAULT_PAGE_SIZE = 1000
_MIN_PAGE_SIZE = 100
_ADAPTIVE_BACKOFF_BASE = 5.0  # seconds — scales with reduction depth
_GAP_SKIP_WINDOW = 90 * 86400  # 90 days in seconds
_GAP_SKIP_LOWER_BOUND = 946684800  # 2000-01-01T00:00:00Z


class ListenBrainzSyncStrategy(sync_base.SyncStrategy):
    """Sync strategy for ListenBrainz listening history."""

    concurrency = "sequential"

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: connector_base.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        """Plan ListenBrainz sync tasks with two-ended watermark support.

        Handles three watermark scenarios:

        1. No watermark: full sync (one task, min_ts=None).
        2. Legacy watermark (``last_listened_at`` only) or new watermark with
           only ``newest_synced_at``: incremental sync (one task from that
           timestamp upward).
        3. Two-ended watermark (``newest_synced_at`` + ``oldest_synced_at``):
           plans two tasks -- one for new listens above the newest boundary,
           and one to resume backfill below the oldest boundary. If the
           previous sync was actually complete, the backfill task will fetch
           zero listens and finish immediately.

        Args:
            session: Active database session.
            connection: The user's ListenBrainz service connection.
            connector: The ListenBrainz connector instance.

        Returns:
            A list of TIME_RANGE task descriptors.
        """
        lb_connector = _cast_connector(connector)
        username = connection.external_user_id

        # Get listen count for progress tracking
        progress_total: int | None = None
        try:
            progress_total = await lb_connector.get_listen_count(username)
        except (httpx.HTTPError, connector_base.RateLimitExceededError):  # fmt: skip
            logger.warning("could_not_fetch_listen_count", username=username)

        listens_watermark = connection.sync_watermark.get("listens", {})

        # Read two-ended watermark, with backward compat for legacy format
        newest_synced_at: int | None = None
        oldest_synced_at: int | None = None

        raw_newest = listens_watermark.get("newest_synced_at")
        raw_oldest = listens_watermark.get("oldest_synced_at")
        raw_legacy = listens_watermark.get("last_listened_at")

        if raw_newest is not None:
            newest_synced_at = int(str(raw_newest))
            oldest_synced_at = int(str(raw_oldest)) if raw_oldest is not None else None
        elif raw_legacy is not None:
            # Legacy format: treat as complete sync up to this point
            newest_synced_at = int(str(raw_legacy))

        descriptors: list[sync_base.SyncTaskDescriptor] = []

        if newest_synced_at is not None:
            # Task 1: new listens since last sync
            listened_at_dt = datetime.datetime.fromtimestamp(
                newest_synced_at, tz=datetime.UTC
            )
            date_str = listened_at_dt.date().isoformat()
            descriptors.append(
                sync_base.SyncTaskDescriptor(
                    task_type=types_module.TaskType.TIME_RANGE,
                    params={"username": username, "min_ts": newest_synced_at},
                    progress_total=progress_total,
                    description=f"Syncing new listens since {date_str}",
                )
            )

            # Task 2: remaining backfill if sync was potentially interrupted
            if oldest_synced_at is not None:
                descriptors.append(
                    sync_base.SyncTaskDescriptor(
                        task_type=types_module.TaskType.TIME_RANGE,
                        params={
                            "username": username,
                            "max_ts": oldest_synced_at,
                            "min_ts": None,
                        },
                        progress_total=progress_total,
                        description="Resuming listening history backfill",
                    )
                )
        else:
            # No watermark at all — full sync
            descriptors.append(
                sync_base.SyncTaskDescriptor(
                    task_type=types_module.TaskType.TIME_RANGE,
                    params={"username": username, "min_ts": None},
                    progress_total=progress_total,
                    description="Syncing listening history",
                )
            )

        return descriptors

    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.Task,
        connector: connector_base.BaseConnector,
        connection: user_models.ServiceConnection,
    ) -> dict[str, object]:
        """Paginate through ListenBrainz listens and upsert into the database.

        Fetches listens page by page using max_ts/min_ts pagination. Upserts
        artists, tracks, and listening events for each listen. Commits per
        page, updates task progress, and writes the sync watermark
        incrementally so crash recovery loses at most one page.

        Args:
            session: Active database session.
            task: The TIME_RANGE task being executed.
            connector: The ListenBrainz connector instance.
            connection: The ServiceConnection whose sync_watermark is
                updated incrementally after each page commit.

        Returns:
            Dict with items_created count, last_listened_at timestamp,
            and watermark dict with newest_synced_at/oldest_synced_at.

        Raises:
            DeferRequest: When a RateLimitExceededError is encountered.
        """
        assert task.user_id is not None
        lb_connector = _cast_connector(connector)
        username: str = str(task.params.get("username", ""))
        min_ts_param = task.params.get("min_ts")
        min_ts: int | None = (
            int(str(min_ts_param)) if min_ts_param is not None else None
        )
        max_ts_param = task.params.get("max_ts")
        max_ts: int | None = (
            int(str(max_ts_param)) if max_ts_param is not None else None
        )
        items_created = int(str(task.params.get("items_so_far", 0)))
        pages_fetched = int(str(task.params.get("pages_fetched", 0)))
        # Preserve watermark across deferral/resume cycles
        last_listened_at_param = task.params.get("last_listened_at")
        last_listened_at: int | None = (
            int(str(last_listened_at_param))
            if last_listened_at_param is not None
            else None
        )
        page_limit_reached = False
        page_size = _DEFAULT_PAGE_SIZE

        while True:
            # Check for graceful shutdown between pages
            if sync_base.shutdown_requested.is_set():
                raise sync_base.ShutdownRequest(
                    resume_params={
                        "max_ts": max_ts,
                        "items_so_far": items_created,
                        "pages_fetched": pages_fetched,
                        "last_listened_at": last_listened_at,
                    }
                )

            if pages_fetched >= MAX_PAGES:
                page_limit_reached = True
                logger.warning(
                    "page_limit_reached",
                    username=username,
                    pages_fetched=pages_fetched,
                    items_created=items_created,
                )
                break

            try:
                listens, page_size = await _adaptive_fetch(
                    lb_connector,
                    username,
                    max_ts=max_ts,
                    min_ts=min_ts,
                    page_size=page_size,
                )
            except connector_base.RateLimitExceededError as exc:
                raise sync_base.DeferRequest(
                    retry_after=exc.retry_after,
                    resume_params={
                        "max_ts": max_ts,
                        "items_so_far": items_created,
                        "pages_fetched": pages_fetched,
                        "last_listened_at": last_listened_at,
                    },
                ) from exc

            if not listens:
                break

            pages_fetched += 1

            # Track last_listened_at from the first page's first listen
            if last_listened_at is None:
                last_listened_at = listens[0].listened_at

            # Bulk pre-fetch existing records
            service_key = types_module.ServiceType.LISTENBRAINZ.value
            artist_ids = {
                listen.track.artist_external_id
                for listen in listens
                if listen.track.artist_external_id
            }
            track_ids = {
                listen.track.external_id
                for listen in listens
                if listen.track.external_id
            }
            artist_cache = await runner_module.bulk_fetch_artists(
                session, service_key, artist_ids
            )
            track_cache = await runner_module.bulk_fetch_tracks(
                session, service_key, track_ids
            )

            # Pass 1: artists
            for listen in listens:
                with session.no_autoflush:
                    await runner_module._upsert_artist_from_track(
                        session, listen.track, artist_cache=artist_cache
                    )
            await session.flush()

            # Pass 2: tracks
            for listen in listens:
                with session.no_autoflush:
                    await runner_module._upsert_track(
                        session, listen.track, track_cache=track_cache
                    )
            await session.flush()

            # Pass 3: events
            for listen in listens:
                played_at = datetime.datetime.fromtimestamp(
                    listen.listened_at, tz=datetime.UTC
                ).isoformat()
                await runner_module._upsert_listening_event(
                    session, task.user_id, listen.track, played_at
                )
                items_created += 1

            # Use the oldest listen's timestamp for next page
            max_ts = listens[-1].listened_at
            task.progress_current = items_created

            # Update watermark incrementally
            updated_watermarks = dict(connection.sync_watermark)
            updated_watermarks["listens"] = {
                "newest_synced_at": last_listened_at,
                "oldest_synced_at": max_ts,
            }
            connection.sync_watermark = updated_watermarks

            await session.commit()

        # Clear oldest_synced_at when backfill completes normally.
        # A backfill task has max_ts in its original params.  If we exited
        # the loop without hitting the page limit, the backfill is done.
        if max_ts_param is not None and not page_limit_reached:
            updated_watermarks = dict(connection.sync_watermark)
            listens_wm = dict(updated_watermarks.get("listens", {}))
            if "oldest_synced_at" in listens_wm:
                listens_wm.pop("oldest_synced_at")
                updated_watermarks["listens"] = listens_wm
                connection.sync_watermark = updated_watermarks
                await session.commit()
                logger.info("backfill_complete_cleared_oldest_synced_at")

        backfill_complete = max_ts_param is not None and not page_limit_reached

        result: dict[str, object] = {"items_created": items_created}
        if last_listened_at is not None:
            result["last_listened_at"] = last_listened_at
            watermark: dict[str, object] = {
                "newest_synced_at": last_listened_at,
            }
            # Only include oldest_synced_at if the backfill is NOT done.
            # When backfill completes, omitting it signals the worker to
            # stop creating backfill tasks on future syncs.
            if not backfill_complete:
                watermark["oldest_synced_at"] = (
                    max_ts if max_ts is not None else last_listened_at
                )
            result["watermark"] = watermark
        if page_limit_reached:
            result["page_limit_reached"] = True
        return result


async def _adaptive_fetch(
    connector: listenbrainz_module.ListenBrainzConnector,
    username: str,
    *,
    max_ts: int | None,
    min_ts: int | None,
    page_size: int,
) -> tuple[list[listenbrainz_module.ListenBrainzListenItem], int]:
    """Fetch listens with adaptive page size and unified backoff.

    Tries the request at the current page_size. On timeout or server
    disconnect, halves the page size and waits before retrying. On
    success, grows the page size back toward the default.

    Uses max_retries=1 on the connector so each attempt fails fast,
    letting this function control the backoff with page size reduction.

    Args:
        connector: The ListenBrainz connector.
        username: ListenBrainz username.
        max_ts: Upper bound timestamp for pagination.
        min_ts: Lower bound timestamp (watermark).
        page_size: Current page size to try.

    Returns:
        Tuple of (listens, updated_page_size) for the caller to use
        on the next iteration.

    Raises:
        httpx.RemoteProtocolError: If fetch fails at minimum page size.
        httpx.ReadTimeout: If fetch fails at minimum page size.
        connector_base.RateLimitExceededError: Propagated for deferral.
    """
    current_size = page_size
    reduction_depth = 0

    while True:
        try:
            listens = await connector.get_listens(
                username,
                max_ts=max_ts,
                min_ts=min_ts,
                count=current_size,
                max_retries=1,
            )
        except (httpx.RemoteProtocolError, httpx.ReadTimeout):  # fmt: skip
            if current_size <= _MIN_PAGE_SIZE:
                if max_ts is None:
                    logger.error(
                        "adaptive_page_size_exhausted",
                        page_size=current_size,
                        max_ts=max_ts,
                        username=username,
                    )
                    raise
                # Page size exhausted with a max_ts bound — likely a
                # multi-year gap.  Probe bounded time windows to skip
                # past it rather than failing the entire sync.
                return await _skip_gap(
                    connector, username, max_ts=max_ts, min_ts=min_ts
                ), current_size

            old_size = current_size
            current_size = max(_MIN_PAGE_SIZE, current_size // 2)
            reduction_depth += 1
            backoff = _ADAPTIVE_BACKOFF_BASE * reduction_depth
            logger.warning(
                "adaptive_page_size_reduced",
                old_size=old_size,
                new_size=current_size,
                backoff_seconds=round(backoff, 1),
                max_ts=max_ts,
                username=username,
            )
            await asyncio.sleep(backoff)
            continue

        # Success — grow page size back toward default for next call
        next_page_size = current_size
        if current_size < _DEFAULT_PAGE_SIZE:
            next_page_size = min(_DEFAULT_PAGE_SIZE, current_size * 2)
            logger.info(
                "adaptive_page_size_increased",
                old_size=current_size,
                new_size=next_page_size,
            )

        return listens, next_page_size


async def _skip_gap(
    connector: listenbrainz_module.ListenBrainzConnector,
    username: str,
    *,
    max_ts: int,
    min_ts: int | None,
) -> list[listenbrainz_module.ListenBrainzListenItem]:
    """Skip past a gap in listening history by probing bounded time windows.

    When ``_adaptive_fetch`` exhausts page-size reduction, the problem is
    likely a multi-year gap rather than a large result set.  This function
    probes 90-day bounded windows stepping backward from *max_ts* until it
    finds listens or reaches the year-2000 lower bound.

    Each probe uses ``count=1`` with both ``min_ts`` and ``max_ts`` set so
    the API only has to search a small, bounded range.

    Args:
        connector: The ListenBrainz connector.
        username: ListenBrainz username.
        max_ts: Upper bound where the gap was detected.
        min_ts: Global lower bound from the task (may be ``None``).

    Returns:
        A non-empty list if listens were found after the gap, or an empty
        list if no listens exist before the lower bound (backfill complete).
    """
    logger.warning(
        "gap_detected_starting_skip",
        max_ts=max_ts,
        username=username,
    )

    probe_max = max_ts

    while probe_max > _GAP_SKIP_LOWER_BOUND:
        probe_min = probe_max - _GAP_SKIP_WINDOW
        if probe_min < _GAP_SKIP_LOWER_BOUND:
            probe_min = _GAP_SKIP_LOWER_BOUND

        # Respect the task's global lower bound if set
        if min_ts is not None and probe_min < min_ts:
            probe_min = min_ts

        try:
            listens = await connector.get_listens(
                username,
                max_ts=probe_max,
                min_ts=probe_min,
                count=1,
                max_retries=1,
            )
        except httpx.RemoteProtocolError, httpx.ReadTimeout:
            logger.warning(
                "gap_skip_probe_failed",
                probe_max=probe_max,
                probe_min=probe_min,
                username=username,
            )
            probe_max = probe_min
            continue

        if listens:
            logger.info(
                "gap_skip_found_listens",
                probe_max=probe_max,
                probe_min=probe_min,
                listened_at=listens[0].listened_at,
                username=username,
            )
            return listens

        logger.debug(
            "gap_skip_window_empty",
            probe_max=probe_max,
            probe_min=probe_min,
            username=username,
        )
        probe_max = probe_min

        # If we've reached the task's lower bound, stop
        if min_ts is not None and probe_max <= min_ts:
            break

    logger.info(
        "gap_skip_exhausted_backfill_complete",
        username=username,
    )
    return []


def _cast_connector(
    connector: connector_base.BaseConnector,
) -> listenbrainz_module.ListenBrainzConnector:
    """Cast a BaseConnector to ListenBrainzConnector with a runtime check.

    Args:
        connector: The connector to cast.

    Returns:
        The connector as a ListenBrainzConnector.

    Raises:
        TypeError: If the connector is not a ListenBrainzConnector.
    """
    if not isinstance(connector, listenbrainz_module.ListenBrainzConnector):
        msg = f"Expected ListenBrainzConnector, got {type(connector).__name__}"
        raise TypeError(msg)
    return connector
