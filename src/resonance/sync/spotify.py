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
        task: task_module.SyncTask,
        connector: spotify_module.SpotifyConnector,
    ) -> str:
        """Load the connection, refresh if expired, return the token."""
        conn_result = await session.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id == task.service_connection_id
            )
        )
        connection = conn_result.scalar_one()
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
                    task_type=types_module.SyncTaskType.TIME_RANGE,
                    params=params,
                    description=description,
                )
            )
        return descriptors

    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: connector_base.BaseConnector,
    ) -> dict[str, object]:
        """Execute a Spotify sync child task.

        Loads the service connection to decrypt the access token at
        execute-time, avoiding plaintext token storage in task.params.
        """
        sp_connector = _cast_connector(connector)
        data_type = str(task.params.get("data_type", ""))

        if data_type not in _DATA_TYPE_DESCRIPTIONS:
            logger.warning("unknown_spotify_data_type", data_type=data_type)

        # Decrypt token at execute-time, refreshing if expired
        access_token = await self._get_access_token(session, task, sp_connector)

        items_created = 0
        items_updated = 0

        try:
            if data_type == "followed_artists":
                items_created, items_updated = await _sync_followed_artists(
                    session, task, sp_connector, access_token
                )
            elif data_type == "saved_tracks":
                items_created, items_updated = await _sync_saved_tracks(
                    session, task, sp_connector, access_token
                )
            elif data_type == "recently_played":
                items_created = await _sync_recently_played(
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
    task: task_module.SyncTask,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> tuple[int, int]:
    """Fetch followed artists and upsert into the database."""
    artists = await connector.get_followed_artists(access_token)
    logger.info("spotify_artists_fetched", count=len(artists))
    created = 0
    updated = 0
    for artist_data in artists:
        with session.no_autoflush:
            was_created = await runner_module._upsert_artist(session, artist_data)
            await session.flush()
            if was_created:
                created += 1
            else:
                updated += 1
            await runner_module._upsert_user_artist_relation(
                session, task.user_id, artist_data, task.service_connection_id
            )
    return created, updated


async def _sync_saved_tracks(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> tuple[int, int]:
    """Fetch saved tracks and upsert into the database."""
    tracks = await connector.get_saved_tracks(access_token)
    logger.info("spotify_tracks_fetched", count=len(tracks))
    created = 0
    updated = 0
    for track_data in tracks:
        with session.no_autoflush:
            await runner_module._upsert_artist_from_track(session, track_data)
            await session.flush()
            was_created = await runner_module._upsert_track(session, track_data)
            await session.flush()
            if was_created:
                created += 1
            else:
                updated += 1
            await runner_module._upsert_user_track_relation(
                session, task.user_id, track_data, task.service_connection_id
            )
    return created, updated


async def _sync_recently_played(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> int:
    """Fetch recently played tracks and upsert into the database."""
    played_items = await connector.get_recently_played(access_token)
    logger.info("spotify_recent_fetched", count=len(played_items))
    created = 0
    for played_item in played_items:
        with session.no_autoflush:
            await runner_module._upsert_artist_from_track(session, played_item.track)
            await session.flush()
            await runner_module._upsert_track(session, played_item.track)
            await session.flush()
            await runner_module._upsert_listening_event(
                session, task.user_id, played_item.track, played_item.played_at
            )
        created += 1
    return created
