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
import resonance.database as database_module
import resonance.logging as logging_module
import resonance.models.task as task_module
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.sync.listenbrainz as lb_sync
import resonance.sync.spotify as spotify_sync
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
            strategies: dict[types_module.ServiceType, sync_base.SyncStrategy] = ctx[
                "strategies"
            ]
            strategy = strategies.get(connection.service_type)
            if strategy is None:
                task.status = types_module.SyncStatus.FAILED
                task.error_message = (
                    f"No sync strategy for {connection.service_type.value}"
                )
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                return

            # Look up connector
            connector = ctx["connector_registry"].get(connection.service_type)
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
            arq_redis: arq.ArqRedis = ctx["redis"]
            children: list[task_module.SyncTask] = []
            for desc in descriptors:
                child = task_module.SyncTask(
                    id=uuid.uuid4(),
                    user_id=task.user_id,
                    service_connection_id=task.service_connection_id,
                    parent_id=task.id,
                    task_type=desc.task_type,
                    status=types_module.SyncStatus.PENDING,
                    params=desc.params,
                    progress_total=desc.progress_total,
                    description=desc.description,
                )
                session.add(child)
                children.append(child)
            await session.commit()

            # Enqueue based on concurrency policy
            if strategy.concurrency == "parallel":
                for child in children:
                    await arq_redis.enqueue_job("sync_range", str(child.id))
                    log.info("child_enqueued", child_id=str(child.id))
            else:
                # Sequential: enqueue only the first
                await arq_redis.enqueue_job("sync_range", str(children[0].id))
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
        sync_task_id: UUID string of the TIME_RANGE SyncTask.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    connector_registry: registry_module.ConnectorRegistry = ctx["connector_registry"]
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        task: task_module.SyncTask | None = None
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

            strategies: dict[types_module.ServiceType, sync_base.SyncStrategy] = ctx[
                "strategies"
            ]
            strategy = strategies.get(connection.service_type)
            connector = connector_registry.get(connection.service_type)
            if strategy is None or connector is None:
                raise RuntimeError(
                    f"No strategy/connector for {connection.service_type.value}"
                )

            try:
                result = await strategy.execute(session, task, connector)
                task.status = types_module.SyncStatus.COMPLETED
                task.result = result
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                log.info("sync_range_completed", result=task.result)
            except sync_base.DeferRequest as defer:
                task.status = types_module.SyncStatus.DEFERRED
                task.params = {**task.params, **defer.resume_params}
                task.deferred_until = datetime.datetime.now(
                    datetime.UTC
                ) + datetime.timedelta(seconds=defer.retry_after)
                await session.commit()
                arq_redis_defer: arq.ArqRedis = ctx["redis"]
                await arq_redis_defer.enqueue_job(
                    "sync_range",
                    str(task.id),
                    _defer_by=datetime.timedelta(seconds=defer.retry_after),
                )
                log.info(
                    "sync_range_deferred",
                    retry_after=defer.retry_after,
                    deferred_until=str(task.deferred_until),
                )

        except Exception:
            log.exception("sync_range_failed")
            task_reload = await _load_task(session, sync_task_id)
            if task_reload is not None:
                task_reload.status = types_module.SyncStatus.FAILED
                task_reload.error_message = traceback.format_exc()
                task_reload.completed_at = datetime.datetime.now(datetime.UTC)
                await session.commit()
                task = task_reload

        # Always check parent completion (may enqueue next sibling)
        if task is not None:
            arq_redis: arq.ArqRedis = ctx["redis"]
            await _check_parent_completion(session, task, arq_redis, log)


# ---------------------------------------------------------------------------
# Parent completion check
# ---------------------------------------------------------------------------


async def _check_parent_completion(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
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
            task_module.SyncTask.parent_id == task.parent_id,
            task_module.SyncTask.status.notin_(
                [types_module.SyncStatus.COMPLETED, types_module.SyncStatus.FAILED]
            ),
        )
    )
    pending_count: int = pending_count_result.scalar_one()

    if pending_count > 0:
        # Enqueue the next PENDING sibling (sequential execution)
        next_pending_result = await session.execute(
            sa.select(task_module.SyncTask)
            .where(
                task_module.SyncTask.parent_id == task.parent_id,
                task_module.SyncTask.status == types_module.SyncStatus.PENDING,
            )
            .order_by(task_module.SyncTask.created_at)
            .limit(1)
        )
        next_pending = next_pending_result.scalar_one_or_none()
        if next_pending is not None:
            await arq_redis.enqueue_job("sync_range", str(next_pending.id))
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


async def _reenqueue_orphaned_tasks(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
) -> None:
    """Re-enqueue PENDING and expired DEFERRED tasks on worker startup.

    arq jobs in Redis expire after ~1 day. If the worker was down during
    that window, the SyncTask row remains PENDING/DEFERRED but the arq
    job is gone. This function finds those orphaned tasks and re-enqueues
    them.
    """
    async with session_factory() as session:
        now = datetime.datetime.now(datetime.UTC)

        # Find PENDING tasks (orphaned — their arq job likely expired)
        pending_result = await session.execute(
            sa.select(task_module.SyncTask).where(
                task_module.SyncTask.status == types_module.SyncStatus.PENDING,
                task_module.SyncTask.task_type.in_(
                    [
                        types_module.SyncTaskType.SYNC_JOB,
                        types_module.SyncTaskType.TIME_RANGE,
                    ]
                ),
            )
        )
        pending_tasks = list(pending_result.scalars().all())

        # Find DEFERRED tasks whose deferred_until has passed
        deferred_result = await session.execute(
            sa.select(task_module.SyncTask).where(
                task_module.SyncTask.status == types_module.SyncStatus.DEFERRED,
                sa.or_(
                    task_module.SyncTask.deferred_until <= now,
                    task_module.SyncTask.deferred_until.is_(None),
                ),
            )
        )
        deferred_tasks = list(deferred_result.scalars().all())

        # Reset deferred tasks back to PENDING before re-enqueueing
        for task in deferred_tasks:
            task.status = types_module.SyncStatus.PENDING
        if deferred_tasks:
            await session.commit()

        all_tasks = pending_tasks + deferred_tasks
        if not all_tasks:
            return

        for task in all_tasks:
            if task.task_type == types_module.SyncTaskType.SYNC_JOB:
                await arq_redis.enqueue_job("plan_sync", str(task.id))
            else:
                await arq_redis.enqueue_job("sync_range", str(task.id))

        logger.info("reenqueued_orphaned_tasks", count=len(all_tasks))


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
    logging_module.configure_logging(settings.log_level)

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
    ctx["strategies"] = {
        types_module.ServiceType.SPOTIFY: spotify_sync.SpotifySyncStrategy(
            token_encryption_key=settings.token_encryption_key
        ),
        types_module.ServiceType.LISTENBRAINZ: lb_sync.ListenBrainzSyncStrategy(),
    }

    # Re-enqueue orphaned tasks that lost their arq jobs
    await _reenqueue_orphaned_tasks(session_factory, ctx["redis"])

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

    functions: typing.ClassVar[list[typing.Any]] = [
        arq.func(plan_sync, timeout=86400),  # 24h — orchestrator, duration varies
        arq.func(sync_range, timeout=86400),  # 24h — duration depends on user data
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
