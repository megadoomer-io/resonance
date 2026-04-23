"""Arq worker with plan_sync and sync_range task functions."""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import traceback
import typing
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import collections.abc

import arq
import arq.connections as arq_connections
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.concerts.worker as concert_worker
import resonance.config as config_module
import resonance.connectors.ical as ical_module
import resonance.connectors.lastfm as lastfm_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.registry as registry_module
import resonance.connectors.songkick as songkick_module
import resonance.connectors.spotify as spotify_module
import resonance.connectors.test as test_connector_module
import resonance.database as database_module
import resonance.heartbeat as heartbeat_module
import resonance.logging as logging_module
import resonance.models.task as task_module
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.sync.lastfm as lastfm_sync
import resonance.sync.listenbrainz as lb_sync
import resonance.sync.spotify as spotify_sync
import resonance.sync.test as test_sync
import resonance.types as types_module

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Orphan recovery dispatch map
# ---------------------------------------------------------------------------
# Maps each TaskType to (arq_job_name, args_builder). The args_builder
# receives a Task and returns the positional args tuple for enqueue_job.

_TASK_DISPATCH: dict[
    types_module.TaskType,
    tuple[str, collections.abc.Callable[[task_module.Task], tuple[str, ...]]],
] = {
    types_module.TaskType.SYNC_JOB: ("plan_sync", lambda t: (str(t.id),)),
    types_module.TaskType.TIME_RANGE: ("sync_range", lambda t: (str(t.id),)),
    types_module.TaskType.CALENDAR_SYNC: (
        "sync_calendar_feed",
        lambda t: (str(t.params.get("feed_id", "")), str(t.id)),
    ),
    types_module.TaskType.BULK_JOB: ("run_bulk_job", lambda t: (str(t.id),)),
}


class WorkerContext(typing.TypedDict):
    """Typed arq worker context dict.

    arq passes ``dict[str, Any]`` at runtime; this TypedDict lets mypy
    catch key-name typos and wrong value types at check time.
    """

    settings: config_module.Settings
    engine: sa_async.AsyncEngine
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession]
    connector_registry: registry_module.ConnectorRegistry
    strategies: dict[types_module.ServiceType, sync_base.SyncStrategy]
    redis: arq.ArqRedis


# ---------------------------------------------------------------------------
# plan_sync: top-level entry point for a sync job
# ---------------------------------------------------------------------------


async def plan_sync(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Load a SYNC_JOB task, mark it RUNNING, and create child tasks.

    Routes to the appropriate planner based on service type (ListenBrainz
    or Spotify), then enqueues the resulting child tasks.

    Args:
        ctx: arq worker context dict (contains session_factory, settings, etc.).
        sync_task_id: UUID string of the SYNC_JOB Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
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

            # Look up strategy
            strategy = wctx["strategies"].get(connection.service_type)
            if strategy is None:
                task.status = types_module.SyncStatus.FAILED
                task.error_message = (
                    f"No sync strategy for {connection.service_type.value}"
                )
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                return

            # Look up connector (must be a full BaseConnector for sync)
            connector = wctx["connector_registry"].get_base_connector(
                connection.service_type
            )
            if connector is None:
                task.status = types_module.SyncStatus.FAILED
                task.error_message = f"No connector for {connection.service_type.value}"
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                return

            # Plan
            descriptors = await strategy.plan(session, connection, connector)

            if not descriptors:
                task.status = types_module.SyncStatus.COMPLETED
                task.result = {"items_created": 0, "items_updated": 0}
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                log.info("plan_sync_no_work")
                return

            # Create child tasks from descriptors
            arq_redis = wctx["redis"]
            parent_step_mode = bool(task.params and task.params.get("step_mode"))
            children: list[task_module.Task] = []
            for desc in descriptors:
                child_params = dict(desc.params) if desc.params else {}
                if parent_step_mode:
                    child_params["step_mode"] = True
                child = task_module.Task(
                    id=uuid.uuid4(),
                    user_id=task.user_id,
                    service_connection_id=task.service_connection_id,
                    parent_id=task.id,
                    task_type=desc.task_type,
                    status=types_module.SyncStatus.PENDING,
                    params=child_params,
                    progress_total=desc.progress_total,
                    description=desc.description,
                )
                session.add(child)
                children.append(child)
            await session.commit()

            # Enqueue based on concurrency policy
            if strategy.concurrency == "parallel":
                for child in children:
                    await arq_redis.enqueue_job(
                        "sync_range",
                        str(child.id),
                        _job_id=f"sync_range:{child.id}",
                    )
                    log.info("child_enqueued", child_id=str(child.id))
            else:
                # Sequential: enqueue only the first
                await arq_redis.enqueue_job(
                    "sync_range",
                    str(children[0].id),
                    _job_id=f"sync_range:{children[0].id}",
                )
                log.info(
                    "child_enqueued",
                    child_id=str(children[0].id),
                    mode="sequential",
                )

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
# sync_range: execute a TIME_RANGE task
# ---------------------------------------------------------------------------


async def sync_range(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Execute a TIME_RANGE task using the appropriate sync strategy.

    Delegates to the strategy's execute() method, handling completion,
    deferral (DeferRequest), and failure. Always checks parent completion
    afterward to cascade status or enqueue the next sequential sibling.

    Args:
        ctx: arq worker context dict.
        sync_task_id: UUID string of the TIME_RANGE Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    connector_registry = wctx["connector_registry"]
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, sync_task_id)
            if task is None:
                log.error("sync_range_task_not_found")
                return

            # Detect retry: task is still RUNNING from a previous crashed attempt
            is_retry = task.status == types_module.SyncStatus.RUNNING

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

            # On retry, resume from watermark to avoid re-processing pages
            if is_retry:
                _apply_watermark_resume(task, connection)

            log.info("sync_range_started")

            strategy = wctx["strategies"].get(connection.service_type)
            connector = connector_registry.get_base_connector(connection.service_type)
            if strategy is None or connector is None:
                raise RuntimeError(
                    f"No strategy/connector for {connection.service_type.value}"
                )

            try:
                result = await strategy.execute(session, task, connector, connection)
                task.status = types_module.SyncStatus.COMPLETED
                task.result = result
                task.completed_at = datetime.datetime.now(datetime.UTC)

                # Write watermark back to connection
                watermark = result.get("watermark")
                if watermark and isinstance(watermark, dict):
                    data_type = str(task.params.get("data_type", ""))
                    if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
                        data_type_key = "listens"
                    else:
                        data_type_key = data_type
                    updated_watermarks = dict(connection.sync_watermark)
                    updated_watermarks[data_type_key] = watermark
                    connection.sync_watermark = updated_watermarks

                await session.commit()
                log.info("sync_range_completed", result=task.result)
            except sync_base.DeferRequest as defer:
                task.status = types_module.SyncStatus.DEFERRED
                task.params = {**task.params, **defer.resume_params}
                task.deferred_until = datetime.datetime.now(
                    datetime.UTC
                ) + datetime.timedelta(seconds=defer.retry_after)
                await session.commit()
                arq_redis_defer = wctx["redis"]
                await arq_redis_defer.enqueue_job(
                    "sync_range",
                    str(task.id),
                    _job_id=f"sync_range:{task.id}",
                    _defer_by=datetime.timedelta(seconds=defer.retry_after),
                )
                log.info(
                    "sync_range_deferred",
                    retry_after=defer.retry_after,
                    deferred_until=str(task.deferred_until),
                )
            except sync_base.ShutdownRequest as shutdown_req:
                task.status = types_module.SyncStatus.PENDING
                task.params = {**task.params, **shutdown_req.resume_params}
                task.started_at = None
                await session.commit()
                log.info(
                    "sync_range_shutdown_checkpoint",
                    resume_params=shutdown_req.resume_params,
                )
                return

        except Exception:
            log.exception("sync_range_failed")
            task_reload = await _load_task(session, sync_task_id)
            if task_reload is not None:
                task_reload.status = types_module.SyncStatus.FAILED
                task_reload.error_message = traceback.format_exc()
                task_reload.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                task = task_reload

        # Check parent completion (may enqueue next sibling)
        # In step_mode, skip auto-advance so user manually triggers next step
        if task is not None:
            # Check step_mode on this task's params
            step_mode = bool(task.params and task.params.get("step_mode"))

            if step_mode and task.status == types_module.SyncStatus.COMPLETED:
                log.info("step_mode_paused", task_id=str(task.id))
            else:
                arq_redis = wctx["redis"]
                await _check_parent_completion(session, task, arq_redis, log)


# ---------------------------------------------------------------------------
# Bulk job execution
# ---------------------------------------------------------------------------

_BULK_OPERATIONS: dict[str, str] = {
    "dedup_artists": "find_and_merge_duplicate_artists",
    "dedup_tracks": "find_and_merge_duplicate_tracks",
    "dedup_events": "delete_cross_service_duplicate_events",
}


async def run_bulk_job(ctx: dict[str, Any], task_id: str) -> None:
    """Execute a BULK_JOB task (dedup, future bulk operations).

    Reads ``params["operation"]`` to dispatch to the correct function
    in the dedup module. Updates task status through the standard
    PENDING -> RUNNING -> COMPLETED/FAILED lifecycle.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the BULK_JOB Task.
    """
    import resonance.dedup as dedup_module

    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("bulk_job_task_not_found")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            operation = str(task.params.get("operation", ""))
            log = log.bind(operation=operation)
            log.info("bulk_job_started")

            if operation == "dedup_artists":
                stats = await dedup_module.find_and_merge_duplicate_artists(session)
                task.result = {
                    "artists_merged": stats.artists_merged,
                    "tracks_repointed": stats.tracks_repointed,
                    "relations_repointed": stats.artist_relations_repointed,
                    "relations_deleted": stats.artist_relations_deleted,
                }
            elif operation == "dedup_tracks":
                stats = await dedup_module.find_and_merge_duplicate_tracks(session)
                task.result = {
                    "tracks_merged": stats.tracks_merged,
                    "events_repointed": stats.events_repointed,
                    "relations_repointed": stats.track_relations_repointed,
                    "relations_deleted": stats.track_relations_deleted,
                }
            elif operation == "dedup_events":
                deleted = await dedup_module.delete_cross_service_duplicate_events(
                    session
                )
                task.result = {"events_deleted": deleted}
            elif operation == "dedup_all":
                task.result = {**await dedup_module.dedup_all(session)}
            else:
                msg = f"Unknown bulk operation: {operation}"
                raise ValueError(msg)

            task.status = types_module.SyncStatus.COMPLETED
            task.completed_at = datetime.datetime.now(datetime.UTC)
            await session.commit()
            log.info("bulk_job_completed", result=task.result)

        except Exception:
            log.exception("bulk_job_failed")
            if task is not None:
                task.status = types_module.SyncStatus.FAILED
                task.error_message = traceback.format_exc()
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()


# ---------------------------------------------------------------------------
# Parent completion check
# ---------------------------------------------------------------------------


async def _check_parent_completion(
    session: sa_async.AsyncSession,
    task: task_module.Task,
    arq_redis: arq.ArqRedis,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Check sibling tasks; enqueue next pending sibling or mark parent done.

    After a child task completes, this function checks if there are pending
    siblings to enqueue (sequential execution for rate-limit-sensitive
    services like Spotify) or if all children are done (cascade completion
    to parent).

    Args:
        session: Active database session.
        task: The child task that just completed.
        arq_redis: arq Redis pool for enqueuing jobs.
        log: Bound structured logger.
    """
    if task.parent_id is None:
        return

    # Count siblings (including self) that are NOT in a terminal state
    pending_count_result = await session.execute(
        sa.select(sa.func.count()).where(
            task_module.Task.parent_id == task.parent_id,
            task_module.Task.status.notin_(
                [types_module.SyncStatus.COMPLETED, types_module.SyncStatus.FAILED]
            ),
        )
    )
    pending_count: int = pending_count_result.scalar_one()

    if pending_count > 0:
        # Enqueue the next PENDING sibling (sequential execution)
        next_pending_result = await session.execute(
            sa.select(task_module.Task)
            .where(
                task_module.Task.parent_id == task.parent_id,
                task_module.Task.status == types_module.SyncStatus.PENDING,
            )
            .order_by(task_module.Task.created_at)
            .limit(1)
        )
        next_pending = next_pending_result.scalar_one_or_none()
        if next_pending is not None:
            await arq_redis.enqueue_job(
                "sync_range",
                str(next_pending.id),
                _job_id=f"sync_range:{next_pending.id}",
            )
            log.info(
                "next_sibling_enqueued",
                next_task_id=str(next_pending.id),
                remaining=pending_count,
            )
        else:
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
            task_module.Task.parent_id == task.parent_id,
            task_module.Task.status == types_module.SyncStatus.FAILED,
        )
    )
    failed_count: int = failed_count_result.scalar_one()

    # Aggregate results from all children
    children_result = await session.execute(
        sa.select(task_module.Task).where(task_module.Task.parent_id == task.parent_id)
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

    # After a successful sync, run cross-service event dedup
    if parent.status == types_module.SyncStatus.COMPLETED:
        dedup_task = task_module.Task(
            task_type=types_module.TaskType.BULK_JOB,
            status=types_module.SyncStatus.PENDING,
            params={"operation": "dedup_all"},
            description="Post-sync entity resolution",
        )
        session.add(dedup_task)
        await session.commit()
        await arq_redis.enqueue_job(
            "run_bulk_job",
            str(dedup_task.id),
            _job_id=f"bulk:{dedup_task.id}",
        )
        log.info("post_sync_dedup_enqueued", task_id=str(dedup_task.id))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _load_task(
    session: sa_async.AsyncSession, sync_task_id: str
) -> task_module.Task | None:
    """Load a Task by ID.

    Args:
        session: Active database session.
        sync_task_id: UUID string of the task.

    Returns:
        The Task, or None if not found.
    """
    result = await session.execute(
        sa.select(task_module.Task).where(
            task_module.Task.id == uuid.UUID(sync_task_id)
        )
    )
    return result.scalar_one_or_none()


def _apply_watermark_resume(
    task: task_module.Task,
    connection: user_models.ServiceConnection,
) -> None:
    """Inject watermark position into task params for crash recovery.

    For ListenBrainz TIME_RANGE tasks, reads ``oldest_synced_at`` from
    the connection's sync watermark and sets it as ``max_ts`` so the
    task resumes from where the previous run left off instead of
    re-processing all pages.

    Args:
        task: The orphaned task being re-enqueued.
        connection: The task's ServiceConnection with current watermark.
    """
    if connection.service_type != types_module.ServiceType.LISTENBRAINZ:
        return

    listens_watermark = connection.sync_watermark.get("listens", {})
    oldest_synced_at = listens_watermark.get("oldest_synced_at")
    if oldest_synced_at is not None:
        items_so_far = int(task.progress_current or 0)
        task.params = {
            **task.params,
            "max_ts": int(str(oldest_synced_at)),
            "items_so_far": items_so_far,
        }
        logger.info(
            "watermark_resume_applied",
            task_id=str(task.id),
            max_ts=oldest_synced_at,
            items_so_far=items_so_far,
        )


async def _reenqueue_orphaned_tasks(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
) -> None:
    """Re-enqueue orphaned tasks on worker startup.

    Finds tasks stuck in PENDING, expired DEFERRED, or RUNNING status
    (from crashes or ungraceful shutdowns) and re-enqueues them. arq jobs
    in Redis expire after ~1 day, so if the worker was down during that
    window the Task row remains but the arq job is gone.
    """
    async with session_factory() as session:
        now = datetime.datetime.now(datetime.UTC)

        # Find PENDING tasks (orphaned — their arq job likely expired).
        # Exclude children whose parent already completed or failed —
        # those are stale leftovers, not actionable orphans.
        parent_alias = sa.orm.aliased(task_module.Task)
        pending_result = await session.execute(
            sa.select(task_module.Task)
            .outerjoin(
                parent_alias,
                task_module.Task.parent_id == parent_alias.id,
            )
            .where(
                task_module.Task.status == types_module.SyncStatus.PENDING,
                sa.or_(
                    task_module.Task.parent_id.is_(None),
                    parent_alias.status.notin_(
                        [
                            types_module.SyncStatus.COMPLETED,
                            types_module.SyncStatus.FAILED,
                        ]
                    ),
                ),
            )
        )
        pending_tasks = list(pending_result.scalars().all())

        # Find DEFERRED tasks whose deferred_until has passed
        deferred_result = await session.execute(
            sa.select(task_module.Task).where(
                task_module.Task.status == types_module.SyncStatus.DEFERRED,
                sa.or_(
                    task_module.Task.deferred_until <= now,
                    task_module.Task.deferred_until.is_(None),
                ),
            )
        )
        deferred_tasks = list(deferred_result.scalars().all())

        # Mark stale children of terminal parents as FAILED
        stale_result = await session.execute(
            sa.select(task_module.Task)
            .join(
                parent_alias,
                task_module.Task.parent_id == parent_alias.id,
            )
            .where(
                task_module.Task.status == types_module.SyncStatus.PENDING,
                parent_alias.status.in_(
                    [
                        types_module.SyncStatus.COMPLETED,
                        types_module.SyncStatus.FAILED,
                    ]
                ),
            )
        )
        stale_tasks = list(stale_result.scalars().all())
        for task in stale_tasks:
            task.status = types_module.SyncStatus.FAILED
            task.error_message = "Parent task already terminal"
            task.completed_at = now
        if stale_tasks:
            await session.commit()
            logger.info("cleaned_stale_orphans", count=len(stale_tasks))

        # Find RUNNING tasks (interrupted by crash/restart)
        running_result = await session.execute(
            sa.select(task_module.Task)
            .outerjoin(
                parent_alias,
                task_module.Task.parent_id == parent_alias.id,
            )
            .where(
                task_module.Task.status == types_module.SyncStatus.RUNNING,
                sa.or_(
                    task_module.Task.parent_id.is_(None),
                    parent_alias.status.notin_(
                        [
                            types_module.SyncStatus.COMPLETED,
                            types_module.SyncStatus.FAILED,
                        ]
                    ),
                ),
            )
        )
        running_tasks = list(running_result.scalars().all())

        # Reset deferred tasks back to PENDING before re-enqueueing
        for task in deferred_tasks:
            task.status = types_module.SyncStatus.PENDING
        if deferred_tasks:
            await session.commit()

        # Reset RUNNING tasks back to PENDING, with watermark resume.
        # Preserve started_at so the UI shows continuous elapsed time.
        for task in running_tasks:
            task.status = types_module.SyncStatus.PENDING

            # Attempt watermark-based resume for TIME_RANGE tasks
            if task.task_type == types_module.TaskType.TIME_RANGE:
                conn_result = await session.execute(
                    sa.select(user_models.ServiceConnection).where(
                        user_models.ServiceConnection.id == task.service_connection_id
                    )
                )
                connection = conn_result.scalar_one_or_none()
                if connection is not None:
                    _apply_watermark_resume(task, connection)

        if running_tasks:
            await session.commit()
            logger.info("reset_running_orphans", count=len(running_tasks))

        all_tasks = pending_tasks + deferred_tasks + running_tasks
        if not all_tasks:
            return

        enqueued = 0
        for task in all_tasks:
            if task.task_type == types_module.TaskType.SYNC_JOB:
                # Skip SYNC_JOBs that already have children — re-planning
                # would create duplicate child tasks.
                children_count_result = await session.execute(
                    sa.select(sa.func.count()).where(
                        task_module.Task.parent_id == task.id,
                    )
                )
                if children_count_result.scalar_one() > 0:
                    logger.info(
                        "skipped_sync_job_with_children",
                        task_id=str(task.id),
                    )
                    continue

            dispatch = _TASK_DISPATCH.get(task.task_type)
            if dispatch is None:
                logger.warning(
                    "no_dispatch_for_task_type",
                    task_id=str(task.id),
                    task_type=task.task_type.value,
                )
                continue

            job_name, args_builder = dispatch
            args = args_builder(task)
            await arq_redis.enqueue_job(
                job_name, *args, _job_id=f"{job_name}:{task.id}"
            )
            enqueued += 1

        if enqueued:
            logger.info("reenqueued_orphaned_tasks", count=enqueued)


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
    wctx = typing.cast("WorkerContext", ctx)
    settings = config_module.Settings()
    logging_module.configure_logging(settings.log_level)

    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)

    connector_registry = registry_module.ConnectorRegistry()
    connector_registry.register(spotify_module.SpotifyConnector(settings=settings))
    connector_registry.register(
        listenbrainz_module.ListenBrainzConnector(settings=settings)
    )
    connector_registry.register(lastfm_module.LastFmConnector(settings=settings))
    connector_registry.register(test_connector_module.TestConnector())
    connector_registry.register(songkick_module.SongkickConnector())
    connector_registry.register(ical_module.ICalConnector())

    wctx["settings"] = settings
    wctx["engine"] = engine
    wctx["session_factory"] = session_factory
    wctx["connector_registry"] = connector_registry
    wctx["strategies"] = {
        types_module.ServiceType.SPOTIFY: spotify_sync.SpotifySyncStrategy(
            token_encryption_key=settings.token_encryption_key
        ),
        types_module.ServiceType.LISTENBRAINZ: lb_sync.ListenBrainzSyncStrategy(),
        types_module.ServiceType.LASTFM: lastfm_sync.LastFmSyncStrategy(
            token_encryption_key=settings.token_encryption_key
        ),
        types_module.ServiceType.TEST: test_sync.TestSyncStrategy(),
    }

    # Register this worker and clean up stale locks from dead workers
    await heartbeat_module.register_worker(wctx["redis"])
    ctx["_idle_heartbeat"] = heartbeat_module.start_idle_heartbeat(wctx["redis"])
    cleaned = await heartbeat_module.cleanup_stale_locks(wctx["redis"])
    if cleaned:
        logger.info("startup_cleaned_stale_locks", count=cleaned)

    # Re-enqueue orphaned tasks that lost their arq jobs
    await _reenqueue_orphaned_tasks(session_factory, wctx["redis"])

    logger.info("worker_started")


async def shutdown(ctx: dict[str, Any]) -> None:
    """Signal graceful shutdown, then dispose of resources.

    Called by arq when the worker process shuts down. Sets the
    shutdown_requested event so in-flight sync tasks can checkpoint
    their progress before the process exits.

    Args:
        ctx: arq worker context dict.
    """
    sync_base.shutdown_requested.set()

    wctx = typing.cast("WorkerContext", ctx)

    # Cancel idle heartbeat and unregister from worker registry
    idle_heartbeat: asyncio.Task[None] | None = ctx.get("_idle_heartbeat")
    if idle_heartbeat is not None:
        idle_heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await idle_heartbeat

    await heartbeat_module.unregister_worker(wctx["redis"])

    engine = wctx["engine"]
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

    functions: typing.ClassVar[list[typing.Any]] = [
        arq.func(heartbeat_module.with_heartbeat(plan_sync), timeout=3600),
        arq.func(heartbeat_module.with_heartbeat(sync_range), timeout=3600),
        arq.func(heartbeat_module.with_heartbeat(run_bulk_job), timeout=3600),
        arq.func(
            heartbeat_module.with_heartbeat(concert_worker.sync_calendar_feed),
            timeout=3600,
        ),
    ]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 10
    job_timeout = 300  # default for future leaf tasks (e.g., page_fetch)
    # arq reads redis_settings as a class attribute (not a method call).
    # Settings() reads env vars, which are available at import time in K8s.
    _cfg = config_module.Settings()
    redis_settings = arq_connections.RedisSettings(
        host=_cfg.redis_host,
        port=_cfg.redis_port,
        password=_cfg.redis_password or None,
    )


def main() -> None:
    """Run the arq worker.

    Python 3.14 removed the implicit event loop from
    asyncio.get_event_loop(), which arq 0.27 calls in Worker.__init__.
    This entrypoint creates a loop first so the Worker can find it.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    arq.run_worker(WorkerSettings)  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
