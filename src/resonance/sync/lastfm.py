"""Last.fm sync strategy — plans and executes Last.fm data sync tasks."""

from __future__ import annotations

import datetime
import hashlib
from typing import TYPE_CHECKING, Any

import structlog

import resonance.connectors.base as connector_base
import resonance.connectors.lastfm as lastfm_module
import resonance.crypto as crypto_module
import resonance.models.task as task_module
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.sync.runner as runner_module
import resonance.types as types_module

if TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

MAX_PAGES = 5000

_DATA_TYPE_DESCRIPTIONS: dict[str, str] = {
    "recent_tracks": "Fetching your scrobble history",
    "loved_tracks": "Fetching your loved tracks",
}


class LastFmSyncStrategy(sync_base.SyncStrategy):
    """Sync strategy for Last.fm — sequential to respect rate limits."""

    concurrency = "sequential"

    def __init__(self, token_encryption_key: str) -> None:
        self._token_encryption_key = token_encryption_key

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: connector_base.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        """Create descriptors for recent_tracks and loved_tracks sync.

        Args:
            session: Active database session.
            connection: The user's Last.fm service connection.
            connector: The Last.fm connector instance.

        Returns:
            A list of TIME_RANGE task descriptors.
        """
        watermarks = connection.sync_watermark
        descriptors: list[sync_base.SyncTaskDescriptor] = []

        for data_type, base_description in _DATA_TYPE_DESCRIPTIONS.items():
            wm = watermarks.get(data_type, {})
            params: dict[str, object] = {
                "data_type": data_type,
                "username": connection.external_user_id,
            }

            if data_type == "recent_tracks":
                from_ts = wm.get("last_scrobbled_at")
                if from_ts is not None:
                    params["from_ts"] = int(str(from_ts))

            non_key_fields = ("data_type", "username")
            has_watermark = any(
                v is not None for k, v in params.items() if k not in non_key_fields
            )
            description = (
                f"Fetching new {data_type.replace('_', ' ')}"
                if has_watermark
                else base_description
            )

            descriptors.append(
                sync_base.SyncTaskDescriptor(
                    task_type=types_module.TaskType.TIME_RANGE,
                    params=params,
                    description=description,
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
        """Execute a Last.fm sync child task.

        Args:
            session: Async database session for persistence operations.
            task: The Task being executed.
            connector: The BaseConnector (must be a LastFmConnector).
            connection: The ServiceConnection whose sync_watermark
                is updated incrementally.

        Returns:
            Dict with items_created, items_updated, and watermark.
        """
        assert task.user_id is not None
        assert task.service_connection_id is not None
        lfm_connector = _cast_connector(connector)
        data_type = str(task.params.get("data_type", ""))

        # Decrypt session key at execute-time to validate it's available
        # (the sync helpers don't need it — Last.fm API calls use username)
        crypto_module.decrypt_token(
            connection.encrypted_access_token, self._token_encryption_key
        )
        username = str(task.params.get("username", connection.external_user_id))

        items_created = 0
        items_updated = 0
        watermark: dict[str, object] = {}

        try:
            if data_type == "recent_tracks":
                items_created, watermark = await _sync_recent_tracks(
                    session,
                    task,
                    lfm_connector,
                    username,
                    connection=connection,
                )
            elif data_type == "loved_tracks":
                items_created, items_updated = await _sync_loved_tracks(
                    session,
                    task,
                    lfm_connector,
                    username,
                )
            else:
                logger.warning("unknown_lastfm_data_type", data_type=data_type)
        except connector_base.RateLimitExceededError as exc:
            raise sync_base.DeferRequest(
                retry_after=exc.retry_after,
                resume_params={
                    "data_type": data_type,
                    "items_created": items_created,
                    "items_updated": items_updated,
                },
            ) from exc

        await session.commit()
        result: dict[str, object] = {
            "items_created": items_created,
            "items_updated": items_updated,
            "watermark": watermark,
        }
        logger.info(
            "lastfm_range_completed",
            data_type=data_type,
            items_created=items_created,
            items_updated=items_updated,
        )
        return result


def _cast_connector(
    connector: connector_base.BaseConnector,
) -> lastfm_module.LastFmConnector:
    """Cast a BaseConnector to LastFmConnector with a runtime check."""
    if not isinstance(connector, lastfm_module.LastFmConnector):
        msg = f"Expected LastFmConnector, got {type(connector).__name__}"
        raise TypeError(msg)
    return connector


def _parse_recent_track(
    raw: dict[str, Any],
) -> tuple[connector_base.TrackData, int] | None:
    """Parse a single track from a Last.fm getRecentTracks response.

    Skips currently-playing tracks (those with ``@attr.nowplaying``).
    In getRecentTracks, artist name is in ``artist["#text"]``.

    Args:
        raw: A single track dict from the API response.

    Returns:
        Tuple of (TrackData, unix_timestamp), or None if now-playing.
    """
    if raw.get("@attr", {}).get("nowplaying") == "true":
        return None

    artist_name = raw["artist"]["#text"]
    artist_mbid = raw["artist"].get("mbid", "")
    track_name = raw["name"]
    track_mbid = raw.get("mbid", "")
    uts = int(raw["date"]["uts"])

    # Generate a stable external_id when no mbid is available
    external_id = (
        track_mbid if track_mbid else _generate_track_id(artist_name, track_name)
    )
    artist_external_id = (
        artist_mbid if artist_mbid else _generate_artist_id(artist_name)
    )

    # Last.fm returns duration in seconds; convert to milliseconds
    raw_duration = int(raw.get("duration", 0))
    duration_ms = raw_duration * 1000 if raw_duration > 0 else None

    track_data = connector_base.TrackData(
        external_id=external_id,
        title=track_name,
        artist_external_id=artist_external_id,
        artist_name=artist_name,
        service=types_module.ServiceType.LASTFM,
        duration_ms=duration_ms,
    )
    return track_data, uts


def _parse_loved_track(
    raw: dict[str, Any],
) -> connector_base.TrackData:
    """Parse a single track from a Last.fm getLovedTracks response.

    In getLovedTracks, artist name is in ``artist["name"]``.

    Args:
        raw: A single track dict from the API response.

    Returns:
        TrackData for the loved track.
    """
    artist_name = raw["artist"]["name"]
    artist_mbid = raw["artist"].get("mbid", "")
    track_name = raw["name"]
    track_mbid = raw.get("mbid", "")

    external_id = (
        track_mbid if track_mbid else _generate_track_id(artist_name, track_name)
    )
    artist_external_id = (
        artist_mbid if artist_mbid else _generate_artist_id(artist_name)
    )

    # Last.fm returns duration in seconds; convert to milliseconds
    raw_duration = int(raw.get("duration", 0))
    duration_ms = raw_duration * 1000 if raw_duration > 0 else None

    return connector_base.TrackData(
        external_id=external_id,
        title=track_name,
        artist_external_id=artist_external_id,
        artist_name=artist_name,
        service=types_module.ServiceType.LASTFM,
        duration_ms=duration_ms,
    )


def _generate_track_id(artist_name: str, track_name: str) -> str:
    """Generate a stable external ID for tracks without an MBID.

    Uses a hash of the lowercase artist and track name to produce
    a deterministic identifier.

    Args:
        artist_name: The artist name.
        track_name: The track title.

    Returns:
        A stable hex digest string prefixed with ``lastfm:``.
    """
    key = f"{artist_name.lower()}:{track_name.lower()}"
    return f"lastfm:{hashlib.sha256(key.encode()).hexdigest()[:16]}"


def _generate_artist_id(artist_name: str) -> str:
    """Generate a stable external ID for artists without an MBID.

    Args:
        artist_name: The artist name.

    Returns:
        A stable hex digest string prefixed with ``lastfm-artist:``.
    """
    key = artist_name.lower()
    return f"lastfm-artist:{hashlib.sha256(key.encode()).hexdigest()[:16]}"


async def _sync_recent_tracks(
    session: sa_async.AsyncSession,
    task: task_module.Task,
    connector: lastfm_module.LastFmConnector,
    username: str,
    *,
    connection: user_models.ServiceConnection | None = None,
) -> tuple[int, dict[str, object]]:
    """Paginate through Last.fm recent tracks and upsert into the database.

    Last.fm returns tracks in reverse chronological order. The highest
    timestamp from the first page becomes the watermark for next sync.

    Args:
        session: Active database session.
        task: The TIME_RANGE task being executed.
        connector: The Last.fm connector instance.
        username: Last.fm username.
        connection: The ServiceConnection for incremental watermark updates.

    Returns:
        Tuple of (items_created, watermark_dict).
    """
    assert task.user_id is not None
    from_ts_param = task.params.get("from_ts")
    from_ts: int | None = int(str(from_ts_param)) if from_ts_param is not None else None
    items_created = int(str(task.params.get("items_so_far", 0)))
    page = int(str(task.params.get("page", 1)))
    last_scrobbled_at: int | None = None
    # Preserve watermark across deferral/resume
    last_scrobbled_param = task.params.get("last_scrobbled_at")
    if last_scrobbled_param is not None:
        last_scrobbled_at = int(str(last_scrobbled_param))
    pages_fetched = 0

    while True:
        # Check for graceful shutdown between pages
        if sync_base.shutdown_requested.is_set():
            raise sync_base.ShutdownRequest(
                resume_params={
                    "page": page,
                    "items_so_far": items_created,
                    "last_scrobbled_at": last_scrobbled_at,
                }
            )

        if pages_fetched >= MAX_PAGES:
            logger.warning(
                "lastfm_page_limit_reached",
                username=username,
                pages_fetched=pages_fetched,
                items_created=items_created,
            )
            break

        data = await connector.get_recent_tracks(
            username,
            page=page,
            from_ts=from_ts,
        )

        recent_tracks = data.get("recenttracks", {})
        raw_tracks: list[dict[str, Any]] = recent_tracks.get("track", [])
        attrs = recent_tracks.get("@attr", {})
        total_pages = int(attrs.get("totalPages", "1"))

        if not raw_tracks:
            break

        # Parse tracks, skipping now-playing
        parsed: list[tuple[connector_base.TrackData, int]] = []
        for raw in raw_tracks:
            result = _parse_recent_track(raw)
            if result is not None:
                parsed.append(result)

        if not parsed:
            page += 1
            pages_fetched += 1
            if page > total_pages:
                break
            continue

        # Track watermark from the highest timestamp (first page, first track)
        if last_scrobbled_at is None:
            last_scrobbled_at = parsed[0][1]
        else:
            last_scrobbled_at = max(last_scrobbled_at, parsed[0][1])

        # Bulk pre-fetch existing records
        service_key = types_module.ServiceType.LASTFM.value
        artist_ids = {
            td.artist_external_id for td, _ in parsed if td.artist_external_id
        }
        track_ids = {td.external_id for td, _ in parsed if td.external_id}
        artist_cache = await runner_module.bulk_fetch_artists(
            session, service_key, artist_ids
        )
        track_cache = await runner_module.bulk_fetch_tracks(
            session, service_key, track_ids
        )

        # Pass 1: artists
        for track_data, _ in parsed:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(
                    session, track_data, artist_cache=artist_cache
                )
        await session.flush()

        # Pass 2: tracks
        for track_data, _ in parsed:
            with session.no_autoflush:
                await runner_module._upsert_track(
                    session, track_data, track_cache=track_cache
                )
        await session.flush()

        # Pass 3: listening events
        for track_data, uts in parsed:
            played_at = datetime.datetime.fromtimestamp(
                uts, tz=datetime.UTC
            ).isoformat()
            await runner_module._upsert_listening_event(
                session, task.user_id, track_data, played_at
            )
            items_created += 1

        task.progress_current = items_created

        # Update watermark incrementally
        if connection is not None and last_scrobbled_at is not None:
            updated_watermarks = dict(connection.sync_watermark)
            updated_watermarks["recent_tracks"] = {
                "last_scrobbled_at": last_scrobbled_at,
            }
            connection.sync_watermark = updated_watermarks

        await session.commit()

        pages_fetched += 1
        page += 1
        if page > total_pages:
            break

    watermark: dict[str, object] = {}
    if last_scrobbled_at is not None:
        watermark["last_scrobbled_at"] = last_scrobbled_at

    return items_created, watermark


async def _sync_loved_tracks(
    session: sa_async.AsyncSession,
    task: task_module.Task,
    connector: lastfm_module.LastFmConnector,
    username: str,
) -> tuple[int, int]:
    """Paginate through Last.fm loved tracks and upsert into the database.

    Creates UserTrackRelation with LIKE type for each loved track.

    Args:
        session: Active database session.
        task: The TIME_RANGE task being executed.
        connector: The Last.fm connector instance.
        username: Last.fm username.

    Returns:
        Tuple of (items_created, items_updated).
    """
    assert task.user_id is not None
    assert task.service_connection_id is not None
    items_created = 0
    items_updated = 0
    page = 1
    pages_fetched = 0

    while True:
        if sync_base.shutdown_requested.is_set():
            raise sync_base.ShutdownRequest(
                resume_params={
                    "page": page,
                    "items_so_far": items_created,
                }
            )

        if pages_fetched >= MAX_PAGES:
            logger.warning(
                "lastfm_loved_page_limit_reached",
                username=username,
                pages_fetched=pages_fetched,
            )
            break

        data = await connector.get_loved_tracks(username, page=page)

        loved_tracks = data.get("lovedtracks", {})
        raw_tracks: list[dict[str, Any]] = loved_tracks.get("track", [])
        attrs = loved_tracks.get("@attr", {})
        total_pages = int(attrs.get("totalPages", "1"))

        if not raw_tracks:
            break

        # Parse tracks
        parsed: list[connector_base.TrackData] = []
        for raw in raw_tracks:
            parsed.append(_parse_loved_track(raw))

        # Bulk pre-fetch existing records
        service_key = types_module.ServiceType.LASTFM.value
        artist_ids = {td.artist_external_id for td in parsed if td.artist_external_id}
        track_ids = {td.external_id for td in parsed if td.external_id}
        artist_cache = await runner_module.bulk_fetch_artists(
            session, service_key, artist_ids
        )
        track_cache = await runner_module.bulk_fetch_tracks(
            session, service_key, track_ids
        )

        # Pass 1: artists
        for track_data in parsed:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(
                    session, track_data, artist_cache=artist_cache
                )
        await session.flush()

        # Pass 2: tracks
        for track_data in parsed:
            with session.no_autoflush:
                was_created = await runner_module._upsert_track(
                    session, track_data, track_cache=track_cache
                )
                if was_created:
                    items_created += 1
                else:
                    items_updated += 1
        await session.flush()

        # Pass 3: user-track relations (LIKE)
        for track_data in parsed:
            await runner_module._upsert_user_track_relation(
                session, task.user_id, track_data, task.service_connection_id
            )

        task.progress_current = items_created + items_updated
        await session.commit()

        pages_fetched += 1
        page += 1
        if page > total_pages:
            break

    return items_created, items_updated
