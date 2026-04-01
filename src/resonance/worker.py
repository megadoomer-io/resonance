"""Arq worker with plan_sync and sync_range task functions."""

from __future__ import annotations

import datetime
import traceback
import typing
import uuid
from typing import Any

import arq
import arq.connections as arq_connections
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.config as config_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.registry as registry_module
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
import resonance.database as database_module
import resonance.models.task as task_module
import resonance.models.user as user_models
import resonance.sync.runner as runner_module
import resonance.types as types_module

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# plan_sync: top-level entry point for a sync job
# ---------------------------------------------------------------------------


async def plan_sync(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Load a SYNC_JOB task, mark it RUNNING, and create child tasks.

    Routes to the appropriate planner based on service type (ListenBrainz
    or Spotify), then enqueues the resulting child tasks.

    Args:
        ctx: arq worker context dict (contains session_factory, settings, etc.).
        sync_task_id: UUID string of the SYNC_JOB SyncTask.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    settings: config_module.Settings = ctx["settings"]
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        try:
            task = await _load_task(session, sync_task_id)
            if task is None:
                log.error("plan_sync_task_not_found")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            # Load the service connection
            conn_result = await session.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.id == task.service_connection_id
                )
            )
            connection = conn_result.scalar_one()
            log = log.bind(
                service=connection.service_type.value,
                user_id=str(task.user_id),
            )
            log.info("plan_sync_started")

            if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
                await _plan_listenbrainz_sync(
                    ctx, session, task, connection, settings, log
                )
            elif connection.service_type == types_module.ServiceType.SPOTIFY:
                await _plan_spotify_sync(ctx, session, task, connection, settings, log)
            else:
                log.error("unsupported_service_type")
                task.status = types_module.SyncStatus.FAILED
                task.error_message = (
                    f"Unsupported service type: {connection.service_type}"
                )
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()

        except Exception:
            log.exception("plan_sync_failed")
            # Re-fetch task in case the session was invalidated
            task_reload = await _load_task(session, sync_task_id)
            if task_reload is not None:
                task_reload.status = types_module.SyncStatus.FAILED
                task_reload.error_message = traceback.format_exc()
                task_reload.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()


# ---------------------------------------------------------------------------
# Planner helpers (create child TIME_RANGE tasks)
# ---------------------------------------------------------------------------


async def _plan_listenbrainz_sync(
    ctx: dict[str, Any],
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connection: user_models.ServiceConnection,
    settings: config_module.Settings,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Create a single TIME_RANGE child task for ListenBrainz sync.

    Checks for a watermark (most recent completed TIME_RANGE task for this
    connection) to enable incremental sync.

    Args:
        ctx: arq worker context dict.
        session: Active database session.
        task: The parent SYNC_JOB task.
        connection: The user's ListenBrainz service connection.
        settings: Application settings.
        log: Bound structured logger.
    """
    connector_registry: registry_module.ConnectorRegistry = ctx["connector_registry"]
    connector = connector_registry.get(types_module.ServiceType.LISTENBRAINZ)
    if connector is None:
        log.error("listenbrainz_connector_not_registered")
        task.status = types_module.SyncStatus.FAILED
        task.error_message = "ListenBrainz connector not registered"
        task.completed_at = datetime.datetime.now(datetime.UTC)
        await session.commit()
        return

    lb_connector: listenbrainz_module.ListenBrainzConnector = connector  # type: ignore[assignment]
    username = connection.external_user_id

    # Get listen count for progress tracking
    try:
        total = await lb_connector.get_listen_count(username)
        task.progress_total = total
        await session.commit()
        log.info("listenbrainz_listen_count", total=total)
    except Exception:
        log.warning("could_not_fetch_listen_count")

    # Check for watermark (incremental sync)
    min_ts = await _get_watermark(session, connection.id)
    if min_ts is not None:
        log.info("listenbrainz_incremental_sync", min_ts=min_ts)

    # Create child TIME_RANGE task
    child = task_module.SyncTask(
        id=uuid.uuid4(),
        user_id=task.user_id,
        service_connection_id=task.service_connection_id,
        parent_id=task.id,
        task_type=types_module.SyncTaskType.TIME_RANGE,
        status=types_module.SyncStatus.PENDING,
        params={"username": username, "min_ts": min_ts},
    )
    session.add(child)
    await session.commit()

    # Enqueue the child task
    arq_redis: arq.ArqRedis = ctx["arq_redis"]
    await arq_redis.enqueue_job("sync_range", str(child.id))
    log.info("listenbrainz_child_enqueued", child_id=str(child.id))


async def _plan_spotify_sync(
    ctx: dict[str, Any],
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connection: user_models.ServiceConnection,
    settings: config_module.Settings,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Create three TIME_RANGE child tasks for Spotify sync.

    Creates one child for each data type: followed_artists, saved_tracks,
    and recently_played. Each child receives the decrypted access token.

    Args:
        ctx: arq worker context dict.
        session: Active database session.
        task: The parent SYNC_JOB task.
        connection: The user's Spotify service connection.
        settings: Application settings.
        log: Bound structured logger.
    """
    access_token = crypto_module.decrypt_token(
        connection.encrypted_access_token, settings.token_encryption_key
    )

    data_types = ["followed_artists", "saved_tracks", "recently_played"]
    arq_redis: arq.ArqRedis = ctx["arq_redis"]

    for data_type in data_types:
        child = task_module.SyncTask(
            id=uuid.uuid4(),
            user_id=task.user_id,
            service_connection_id=task.service_connection_id,
            parent_id=task.id,
            task_type=types_module.SyncTaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": data_type, "access_token": access_token},
        )
        session.add(child)

    await session.commit()

    # Re-query children to get their IDs after commit
    children_result = await session.execute(
        sa.select(task_module.SyncTask).where(task_module.SyncTask.parent_id == task.id)
    )
    children = children_result.scalars().all()

    for child in children:
        await arq_redis.enqueue_job("sync_range", str(child.id))
        log.info(
            "spotify_child_enqueued",
            child_id=str(child.id),
            data_type=child.params.get("data_type"),
        )


# ---------------------------------------------------------------------------
# sync_range: execute a TIME_RANGE task
# ---------------------------------------------------------------------------


async def sync_range(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Execute a TIME_RANGE task that fetches and upserts data.

    Routes to the appropriate runner based on service type, then marks
    the task COMPLETED or FAILED and checks parent completion.

    Args:
        ctx: arq worker context dict.
        sync_task_id: UUID string of the TIME_RANGE SyncTask.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    settings: config_module.Settings = ctx["settings"]
    connector_registry: registry_module.ConnectorRegistry = ctx["connector_registry"]
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        try:
            task = await _load_task(session, sync_task_id)
            if task is None:
                log.error("sync_range_task_not_found")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            # Load the service connection
            conn_result = await session.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.id == task.service_connection_id
                )
            )
            connection = conn_result.scalar_one()
            log = log.bind(
                service=connection.service_type.value,
                user_id=str(task.user_id),
            )
            log.info("sync_range_started")

            if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
                await _run_listenbrainz_range(
                    session, task, connection, connector_registry, log
                )
            elif connection.service_type == types_module.ServiceType.SPOTIFY:
                await _run_spotify_range(
                    session, task, connection, connector_registry, settings, log
                )

            task.status = types_module.SyncStatus.COMPLETED
            task.completed_at = datetime.datetime.now(datetime.UTC)
            await session.commit()
            log.info("sync_range_completed", result=task.result)

        except Exception:
            log.exception("sync_range_failed")
            task_reload = await _load_task(session, sync_task_id)
            if task_reload is not None:
                task_reload.status = types_module.SyncStatus.FAILED
                task_reload.error_message = traceback.format_exc()
                task_reload.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                task = task_reload

        # Always check parent completion
        if task is not None:
            await _check_parent_completion(session, task, log)


# ---------------------------------------------------------------------------
# Range runners (do the actual data fetching and upserting)
# ---------------------------------------------------------------------------


async def _run_listenbrainz_range(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connection: user_models.ServiceConnection,
    connector_registry: registry_module.ConnectorRegistry,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Paginate through ListenBrainz listens and upsert into the database.

    Uses max_ts/min_ts from task params to control pagination range.
    Updates task.progress_current and commits per page.

    Args:
        session: Active database session.
        task: The TIME_RANGE task being executed.
        connection: The user's ListenBrainz service connection.
        connector_registry: Registry to look up the LB connector.
        log: Bound structured logger.
    """
    connector = connector_registry.get(types_module.ServiceType.LISTENBRAINZ)
    if connector is None:
        raise RuntimeError("ListenBrainz connector not registered")

    lb_connector: listenbrainz_module.ListenBrainzConnector = connector  # type: ignore[assignment]
    username: str = str(task.params.get("username", connection.external_user_id))
    min_ts_param = task.params.get("min_ts")
    min_ts: int | None = int(str(min_ts_param)) if min_ts_param is not None else None
    max_ts: int | None = None
    items_created = 0
    page = 0

    while True:
        listens = await lb_connector.get_listens(
            username, max_ts=max_ts, min_ts=min_ts, count=100
        )
        if not listens:
            break
        page += 1

        for listen in listens:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(session, listen.track)
                await session.flush()
                await runner_module._upsert_track(session, listen.track)
                await session.flush()
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
        log.info(
            "listenbrainz_page_synced",
            page=page,
            listens_in_page=len(listens),
            total_created=items_created,
            max_ts=max_ts,
        )

    task.result = {"items_created": items_created}


async def _run_spotify_range(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connection: user_models.ServiceConnection,
    connector_registry: registry_module.ConnectorRegistry,
    settings: config_module.Settings,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Fetch Spotify data for a single data_type and upsert into the database.

    Reads data_type and access_token from task params. Refreshes the token
    if the connection has expired. Routes to the appropriate connector method.

    Args:
        session: Active database session.
        task: The TIME_RANGE task being executed.
        connection: The user's Spotify service connection.
        connector_registry: Registry to look up the Spotify connector.
        settings: Application settings (for token encryption key).
        log: Bound structured logger.
    """
    connector = connector_registry.get(types_module.ServiceType.SPOTIFY)
    if connector is None:
        raise RuntimeError("Spotify connector not registered")

    sp_connector: spotify_module.SpotifyConnector = connector  # type: ignore[assignment]
    data_type: str = str(task.params.get("data_type", ""))
    access_token: str = str(task.params.get("access_token", ""))

    # Refresh token if expired
    if (
        connection.token_expires_at is not None
        and connection.token_expires_at <= datetime.datetime.now(datetime.UTC)
        and connection.encrypted_refresh_token is not None
    ):
        refresh_token = crypto_module.decrypt_token(
            connection.encrypted_refresh_token, settings.token_encryption_key
        )
        token_response = await sp_connector.refresh_access_token(refresh_token)
        access_token = token_response.access_token
        connection.encrypted_access_token = crypto_module.encrypt_token(
            access_token, settings.token_encryption_key
        )
        if token_response.expires_in is not None:
            connection.token_expires_at = datetime.datetime.now(
                datetime.UTC
            ) + datetime.timedelta(seconds=token_response.expires_in)
        await session.commit()
        log.info("spotify_token_refreshed")

    items_created = 0
    items_updated = 0

    if data_type == "followed_artists":
        artists = await sp_connector.get_followed_artists(access_token)
        log.info("spotify_artists_fetched", count=len(artists))
        for artist_data in artists:
            with session.no_autoflush:
                created = await runner_module._upsert_artist(session, artist_data)
                await session.flush()
                if created:
                    items_created += 1
                else:
                    items_updated += 1
                await runner_module._upsert_user_artist_relation(
                    session,
                    task.user_id,
                    artist_data,
                    task.service_connection_id,
                )

    elif data_type == "saved_tracks":
        tracks = await sp_connector.get_saved_tracks(access_token)
        log.info("spotify_tracks_fetched", count=len(tracks))
        for track_data in tracks:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(session, track_data)
                await session.flush()
                created = await runner_module._upsert_track(session, track_data)
                await session.flush()
                if created:
                    items_created += 1
                else:
                    items_updated += 1
                await runner_module._upsert_user_track_relation(
                    session,
                    task.user_id,
                    track_data,
                    task.service_connection_id,
                )

    elif data_type == "recently_played":
        played_items = await sp_connector.get_recently_played(access_token)
        log.info("spotify_recent_fetched", count=len(played_items))
        for played_item in played_items:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(
                    session, played_item.track
                )
                await session.flush()
                await runner_module._upsert_track(session, played_item.track)
                await session.flush()
                await runner_module._upsert_listening_event(
                    session,
                    task.user_id,
                    played_item.track,
                    played_item.played_at,
                )
            items_created += 1

    await session.commit()
    task.result = {"items_created": items_created, "items_updated": items_updated}
    log.info(
        "spotify_range_completed",
        data_type=data_type,
        items_created=items_created,
        items_updated=items_updated,
    )


# ---------------------------------------------------------------------------
# Parent completion check
# ---------------------------------------------------------------------------


async def _check_parent_completion(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Check if all sibling tasks are done; if so, mark the parent complete.

    When all children of a parent task have reached a terminal state
    (COMPLETED or FAILED), the parent is marked COMPLETED (or FAILED if
    any child failed). Result counters are aggregated from children.

    Args:
        session: Active database session.
        task: The child task that just completed.
        log: Bound structured logger.
    """
    if task.parent_id is None:
        return

    # Count siblings (including self) that are NOT in a terminal state
    pending_count_result = await session.execute(
        sa.select(sa.func.count()).where(
            task_module.SyncTask.parent_id == task.parent_id,
            task_module.SyncTask.status.notin_(
                [types_module.SyncStatus.COMPLETED, types_module.SyncStatus.FAILED]
            ),
        )
    )
    pending_count: int = pending_count_result.scalar_one()

    if pending_count > 0:
        log.info("parent_still_pending", pending_children=pending_count)
        return

    # All children are done — load parent and aggregate results
    parent = await _load_task(session, str(task.parent_id))
    if parent is None:
        log.error("parent_task_not_found", parent_id=str(task.parent_id))
        return

    # Check if any children failed
    failed_count_result = await session.execute(
        sa.select(sa.func.count()).where(
            task_module.SyncTask.parent_id == task.parent_id,
            task_module.SyncTask.status == types_module.SyncStatus.FAILED,
        )
    )
    failed_count: int = failed_count_result.scalar_one()

    # Aggregate results from all children
    children_result = await session.execute(
        sa.select(task_module.SyncTask).where(
            task_module.SyncTask.parent_id == task.parent_id
        )
    )
    children = children_result.scalars().all()

    total_created = 0
    total_updated = 0
    for child in children:
        child_result = child.result or {}
        total_created += int(str(child_result.get("items_created", 0)))
        total_updated += int(str(child_result.get("items_updated", 0)))

    parent.result = {
        "items_created": total_created,
        "items_updated": total_updated,
        "children_completed": len(children) - failed_count,
        "children_failed": failed_count,
    }

    if failed_count > 0:
        parent.status = types_module.SyncStatus.FAILED
        parent.error_message = f"{failed_count} child task(s) failed"
    else:
        parent.status = types_module.SyncStatus.COMPLETED

    parent.completed_at = datetime.datetime.now(datetime.UTC)
    await session.commit()
    log.info(
        "parent_completed",
        parent_id=str(parent.id),
        status=parent.status.value,
        result=parent.result,
    )


# ---------------------------------------------------------------------------
# Watermark lookup (for incremental sync)
# ---------------------------------------------------------------------------


async def _get_watermark(
    session: sa_async.AsyncSession,
    connection_id: uuid.UUID,
) -> int | None:
    """Find the most recent completed TIME_RANGE task's max listened_at timestamp.

    Used for incremental ListenBrainz sync — only fetches listens newer
    than the watermark.

    Args:
        session: Active database session.
        connection_id: The service connection ID.

    Returns:
        Unix timestamp (int) of the watermark, or None for full sync.
    """
    result = await session.execute(
        sa.select(task_module.SyncTask)
        .where(
            task_module.SyncTask.service_connection_id == connection_id,
            task_module.SyncTask.task_type == types_module.SyncTaskType.TIME_RANGE,
            task_module.SyncTask.status == types_module.SyncStatus.COMPLETED,
        )
        .order_by(task_module.SyncTask.completed_at.desc())
        .limit(1)
    )
    last_task = result.scalar_one_or_none()
    if last_task is None:
        return None

    # The watermark is stored in the task's result as the last max_ts
    # processed, or we can use progress_current as a proxy.  For now,
    # we look for a "last_listened_at" key in result.
    task_result = last_task.result or {}
    watermark = task_result.get("last_listened_at")
    if watermark is not None:
        return int(str(watermark))
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _load_task(
    session: sa_async.AsyncSession, sync_task_id: str
) -> task_module.SyncTask | None:
    """Load a SyncTask by ID.

    Args:
        session: Active database session.
        sync_task_id: UUID string of the task.

    Returns:
        The SyncTask, or None if not found.
    """
    result = await session.execute(
        sa.select(task_module.SyncTask).where(
            task_module.SyncTask.id == uuid.UUID(sync_task_id)
        )
    )
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# arq startup / shutdown hooks
# ---------------------------------------------------------------------------


async def startup(ctx: dict[str, Any]) -> None:
    """Initialize database engine, session factory, and connector registry.

    Called by arq when the worker process starts. Stores shared resources
    in the worker context dict for use by task functions.

    Args:
        ctx: arq worker context dict.
    """
    settings = config_module.Settings()
    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)

    connector_registry = registry_module.ConnectorRegistry()
    connector_registry.register(spotify_module.SpotifyConnector(settings=settings))
    connector_registry.register(
        listenbrainz_module.ListenBrainzConnector(settings=settings)
    )

    ctx["settings"] = settings
    ctx["engine"] = engine
    ctx["session_factory"] = session_factory
    ctx["connector_registry"] = connector_registry

    logger.info("worker_started")


async def shutdown(ctx: dict[str, Any]) -> None:
    """Dispose of the database engine.

    Called by arq when the worker process shuts down.

    Args:
        ctx: arq worker context dict.
    """
    engine: sa_async.AsyncEngine = ctx["engine"]
    await engine.dispose()
    logger.info("worker_shutdown")


# ---------------------------------------------------------------------------
# arq WorkerSettings
# ---------------------------------------------------------------------------


class WorkerSettings:
    """arq worker configuration.

    arq discovers this class by convention. It defines the task functions,
    lifecycle hooks, concurrency limits, and Redis connection settings.
    """

    functions: typing.ClassVar[list[typing.Any]] = [plan_sync, sync_range]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 10
    job_timeout = 300

    @staticmethod
    def redis_settings() -> arq_connections.RedisSettings:
        """Build Redis connection settings from app config.

        Returns:
            arq RedisSettings for connecting to the task queue.
        """
        settings = config_module.Settings()
        return arq_connections.RedisSettings(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password or None,
        )
