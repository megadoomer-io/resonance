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


class ListenBrainzSyncStrategy(sync_base.SyncStrategy):
    """Sync strategy for ListenBrainz listening history."""

    concurrency = "parallel"

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: connector_base.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        """Plan a ListenBrainz sync by creating a single TIME_RANGE task.

        Checks for a watermark from the most recent completed sync to enable
        incremental sync. Fetches listen count for progress tracking.

        Args:
            session: Active database session.
            connection: The user's ListenBrainz service connection.
            connector: The ListenBrainz connector instance.

        Returns:
            A single-element list with a TIME_RANGE task descriptor.
        """
        lb_connector = _cast_connector(connector)
        username = connection.external_user_id

        # Get listen count for progress tracking
        progress_total: int | None = None
        try:
            progress_total = await lb_connector.get_listen_count(username)
        except (httpx.HTTPError, connector_base.RateLimitExceededError):  # fmt: skip
            logger.warning("could_not_fetch_listen_count", username=username)

        # Read watermark from connection
        listens_watermark = connection.sync_watermark.get("listens", {})
        watermark: int | None = None
        raw = listens_watermark.get("last_listened_at")
        if raw is not None:
            watermark = int(str(raw))

        if watermark is not None:
            listened_at_dt = datetime.datetime.fromtimestamp(watermark, tz=datetime.UTC)
            date_str = listened_at_dt.date().isoformat()
            description = f"Syncing new listens since {date_str}"
        else:
            description = "Syncing listening history"

        return [
            sync_base.SyncTaskDescriptor(
                task_type=types_module.SyncTaskType.TIME_RANGE,
                params={"username": username, "min_ts": watermark},
                progress_total=progress_total,
                description=description,
            )
        ]

    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: connector_base.BaseConnector,
    ) -> dict[str, object]:
        """Paginate through ListenBrainz listens and upsert into the database.

        Fetches listens page by page using max_ts/min_ts pagination. Upserts
        artists, tracks, and listening events for each listen. Commits per
        page and updates task progress.

        Args:
            session: Active database session.
            task: The TIME_RANGE task being executed.
            connector: The ListenBrainz connector instance.

        Returns:
            Dict with items_created count and last_listened_at timestamp.

        Raises:
            DeferRequest: When a RateLimitExceededError is encountered.
        """
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
            await session.commit()

        result: dict[str, object] = {"items_created": items_created}
        if last_listened_at is not None:
            result["last_listened_at"] = last_listened_at
            result["watermark"] = {"last_listened_at": last_listened_at}
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
                logger.error(
                    "adaptive_page_size_exhausted",
                    page_size=current_size,
                    max_ts=max_ts,
                    username=username,
                )
                raise

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
