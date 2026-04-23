"""Spotify sync strategy — plans and executes Spotify data sync tasks."""

from __future__ import annotations

import datetime

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.connectors.base as connector_base
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
import resonance.models.task as task_module
import resonance.models.taste as taste_module
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.sync.runner as runner_module
import resonance.types as types_module

logger = structlog.get_logger()

_DATA_TYPE_DESCRIPTIONS: dict[str, str] = {
    "followed_artists": "Fetching your followed artists",
    "saved_tracks": "Fetching your saved tracks",
    "recently_played": "Fetching your recent plays",
}


class SpotifySyncStrategy(sync_base.SyncStrategy):
    """Sync strategy for Spotify — sequential to avoid rate limits."""

    concurrency = "sequential"

    def __init__(self, token_encryption_key: str) -> None:
        self._token_encryption_key = token_encryption_key

    async def _get_access_token(
        self,
        session: sa_async.AsyncSession,
        task: task_module.Task,
        connector: spotify_module.SpotifyConnector,
    ) -> str:
        """Load the connection, refresh if expired, return the token."""
        conn_result = await session.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id == task.service_connection_id
            )
        )
        connection = conn_result.scalar_one()
        assert connection.encrypted_access_token is not None, (
            "Spotify connection requires an access token"
        )
        access_token = crypto_module.decrypt_token(
            connection.encrypted_access_token, self._token_encryption_key
        )

        # Refresh token if expired
        if (
            connection.token_expires_at is not None
            and connection.token_expires_at <= datetime.datetime.now(datetime.UTC)
            and connection.encrypted_refresh_token is not None
        ):
            refresh_token = crypto_module.decrypt_token(
                connection.encrypted_refresh_token, self._token_encryption_key
            )
            token_response = await connector.refresh_access_token(refresh_token)
            access_token = token_response.access_token
            connection.encrypted_access_token = crypto_module.encrypt_token(
                access_token, self._token_encryption_key
            )
            if token_response.expires_in is not None:
                connection.token_expires_at = datetime.datetime.now(
                    datetime.UTC
                ) + datetime.timedelta(seconds=token_response.expires_in)
            await session.commit()
            logger.info("spotify_token_refreshed")

        return access_token

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: connector_base.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        """Create descriptors for followed_artists, saved_tracks, recently_played."""
        watermarks = connection.sync_watermark
        descriptors: list[sync_base.SyncTaskDescriptor] = []

        for data_type, base_description in _DATA_TYPE_DESCRIPTIONS.items():
            wm = watermarks.get(data_type, {})
            params: dict[str, object] = {"data_type": data_type}

            if data_type == "recently_played":
                params["last_played_at"] = wm.get("last_played_at")
            elif data_type == "saved_tracks":
                params["last_saved_at"] = wm.get("last_saved_at")
            elif data_type == "followed_artists":
                params["after_cursor"] = wm.get("after_cursor")

            has_watermark = any(
                v is not None for k, v in params.items() if k != "data_type"
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
        """Execute a Spotify sync child task.

        Args:
            session: Async database session for persistence operations.
            task: The Task being executed, containing params like
                ``data_type`` that select which helper to dispatch.
            connector: The BaseConnector (must be a SpotifyConnector).
            connection: The ServiceConnection whose ``sync_watermark``
                is updated incrementally after each page commit so that
                progress survives crashes mid-sync.

        Loads the service connection to decrypt the access token at
        execute-time, avoiding plaintext token storage in task.params.
        """
        assert task.user_id is not None
        assert task.service_connection_id is not None
        sp_connector = _cast_connector(connector)
        data_type = str(task.params.get("data_type", ""))

        if data_type not in _DATA_TYPE_DESCRIPTIONS:
            logger.warning("unknown_spotify_data_type", data_type=data_type)

        # Decrypt token at execute-time, refreshing if expired
        access_token = await self._get_access_token(session, task, sp_connector)

        items_created = 0
        items_updated = 0
        watermark: dict[str, object] = {}

        try:
            if data_type == "followed_artists":
                items_created, items_updated, watermark = await _sync_followed_artists(
                    session, task, sp_connector, access_token
                )
            elif data_type == "saved_tracks":
                items_created, items_updated, watermark = await _sync_saved_tracks(
                    session,
                    task,
                    sp_connector,
                    access_token,
                    connection=connection,
                    data_type=data_type,
                )
            elif data_type == "recently_played":
                items_created, watermark = await _sync_recently_played(
                    session, task, sp_connector, access_token
                )
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
            "spotify_range_completed",
            data_type=data_type,
            items_created=items_created,
            items_updated=items_updated,
        )
        return result


def _cast_connector(
    connector: connector_base.BaseConnector,
) -> spotify_module.SpotifyConnector:
    """Cast a BaseConnector to SpotifyConnector with a runtime check."""
    if not isinstance(connector, spotify_module.SpotifyConnector):
        msg = f"Expected SpotifyConnector, got {type(connector).__name__}"
        raise TypeError(msg)
    return connector


async def _sync_followed_artists(
    session: sa_async.AsyncSession,
    task: task_module.Task,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> tuple[int, int, dict[str, object]]:
    """Fetch all followed artists and upsert into the database.

    Always does a full fetch (no cursor resume) because the list is
    typically small (1-3 API calls) and Spotify's cursor ordering
    doesn't guarantee new follows appear after the stored cursor.
    """
    assert task.user_id is not None
    assert task.service_connection_id is not None
    artists = await connector.get_followed_artists(access_token)
    logger.info("spotify_artists_fetched", count=len(artists))
    created = 0
    updated = 0

    # Bulk pre-fetch existing artists
    service_key = types_module.ServiceType.SPOTIFY.value
    artist_ids = {a.external_id for a in artists if a.external_id}
    artist_cache = await runner_module.bulk_fetch_artists(
        session, service_key, artist_ids
    )

    # Pass 1: upsert artists
    for artist_data in artists:
        with session.no_autoflush:
            was_created = await runner_module._upsert_artist(
                session, artist_data, artist_cache=artist_cache
            )
            if was_created:
                created += 1
            else:
                updated += 1
    await session.flush()

    # Pass 2: user-artist relations
    for artist_data in artists:
        await runner_module._upsert_user_artist_relation(
            session, task.user_id, artist_data, task.service_connection_id
        )
    return created, updated, {}


async def _sync_saved_tracks(
    session: sa_async.AsyncSession,
    task: task_module.Task,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
    *,
    connection: user_models.ServiceConnection | None = None,
    data_type: str = "saved_tracks",
) -> tuple[int, int, dict[str, object]]:
    """Fetch saved tracks page-by-page with stop-early and fast-finish."""
    assert task.user_id is not None
    assert task.service_connection_id is not None
    created = 0
    updated = 0
    watermark: dict[str, object] = {}
    next_url: str | None = None
    first_page = True

    while True:
        page = await connector.get_saved_tracks_page(access_token, url=next_url)

        if first_page and page.items:
            watermark["last_saved_at"] = page.items[0].added_at

        if first_page:
            first_page = False
            # Fast-finish check: if total matches existing count, skip sync
            existing_count_result = await session.execute(
                sa.select(sa.func.count()).where(
                    taste_module.UserTrackRelation.source_connection_id
                    == task.service_connection_id,
                    taste_module.UserTrackRelation.relation_type
                    == types_module.TrackRelationType.LIKE,
                )
            )
            existing_count = existing_count_result.scalar_one()
            if page.total == existing_count:
                logger.info("saved_tracks_fast_finish", total=page.total)
                task.progress_total = page.total
                task.progress_current = page.total
                return created, updated, watermark
            task.progress_total = page.total

        if not page.items:
            break

        # Bulk pre-fetch existing records for this page
        service_key = types_module.ServiceType.SPOTIFY.value
        artist_ids = {
            item.track.artist_external_id
            for item in page.items
            if item.track.artist_external_id
        }
        track_ids = {
            item.track.external_id for item in page.items if item.track.external_id
        }
        artist_cache = await runner_module.bulk_fetch_artists(
            session, service_key, artist_ids
        )
        track_cache = await runner_module.bulk_fetch_tracks(
            session, service_key, track_ids
        )

        # Pass 1: artists
        for item in page.items:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(
                    session, item.track, artist_cache=artist_cache
                )
        await session.flush()

        # Pass 2: tracks
        page_all_duplicates = True
        for item in page.items:
            with session.no_autoflush:
                was_created = await runner_module._upsert_track(
                    session, item.track, track_cache=track_cache
                )
                if was_created:
                    created += 1
                    page_all_duplicates = False
                else:
                    updated += 1
        await session.flush()

        # Pass 3: user-track relations
        for item in page.items:
            await runner_module._upsert_user_track_relation(
                session, task.user_id, item.track, task.service_connection_id
            )

        task.progress_current = created + updated

        # Incrementally persist the watermark on the connection so progress
        # survives crashes between pages.
        if connection is not None and watermark:
            updated_watermarks = dict(connection.sync_watermark)
            updated_watermarks[data_type] = dict(watermark)
            connection.sync_watermark = updated_watermarks

        await session.commit()

        if page_all_duplicates:
            logger.info("saved_tracks_stop_early", created=created, updated=updated)
            break

        next_url = page.next_url
        if next_url is None:
            break

    return created, updated, watermark


async def _sync_recently_played(
    session: sa_async.AsyncSession,
    task: task_module.Task,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> tuple[int, dict[str, object]]:
    """Fetch recently played tracks and upsert into the database."""
    assert task.user_id is not None
    after_param = task.params.get("last_played_at")
    after = str(after_param) if after_param is not None else None
    played_items = await connector.get_recently_played(access_token, after=after)
    logger.info("spotify_recent_fetched", count=len(played_items))
    created = 0
    watermark: dict[str, object] = {}
    if played_items:
        watermark["last_played_at"] = played_items[0].played_at

    if played_items:
        # Bulk pre-fetch existing records
        service_key = types_module.ServiceType.SPOTIFY.value
        artist_ids = {
            item.track.artist_external_id
            for item in played_items
            if item.track.artist_external_id
        }
        track_ids = {
            item.track.external_id for item in played_items if item.track.external_id
        }
        artist_cache = await runner_module.bulk_fetch_artists(
            session, service_key, artist_ids
        )
        track_cache = await runner_module.bulk_fetch_tracks(
            session, service_key, track_ids
        )

        # Pass 1: artists
        for played_item in played_items:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(
                    session, played_item.track, artist_cache=artist_cache
                )
        await session.flush()

        # Pass 2: tracks
        for played_item in played_items:
            with session.no_autoflush:
                await runner_module._upsert_track(
                    session, played_item.track, track_cache=track_cache
                )
        await session.flush()

        # Pass 3: events
        for played_item in played_items:
            await runner_module._upsert_listening_event(
                session, task.user_id, played_item.track, played_item.played_at
            )
            created += 1

    return created, watermark
