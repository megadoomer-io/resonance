"""Type-agnostic task lifecycle helpers."""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import datetime
import enum
import functools
import typing
import uuid
from typing import TYPE_CHECKING, Any

import arq.jobs as arq_jobs
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as orm
import structlog

import resonance.models.task as task_models
import resonance.sync.dispatch as dispatch_module
import resonance.types as types_module

if TYPE_CHECKING:
    import collections.abc

    import arq
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Delivery: handing a task row to arq
# ---------------------------------------------------------------------------


def new_job_id(job_name: str, task_id: uuid.UUID) -> str:
    """A fresh arq job id for one delivery of a task.

    arq silently drops ``enqueue_job`` while a job with the same id (or its
    result key) exists, so every delivery gets its own id.
    """
    return f"{job_name}:{task_id}:{uuid.uuid4().hex}"


@dataclasses.dataclass(frozen=True)
class Delivery:
    """One arq job for a task, stamped on the row and enqueued after commit."""

    task_id: uuid.UUID
    job_name: str
    args: tuple[object, ...]
    job_id: str
    defer_by: datetime.timedelta | None = None


def _check_job_args(
    task: task_models.Task, job_name: str, args: tuple[object, ...]
) -> None:
    """Fail at enqueue, not in the worker, when the job can't find its task."""
    if task.id is None or any(arg is None or arg == "None" for arg in args):
        msg = f"{job_name}: None job arg for task {task.id}: {args!r}"
        raise ValueError(msg)
    if str(task.id) not in args:
        msg = f"{job_name}: job args {args!r} don't include task id {task.id}"
        raise ValueError(msg)


def stamp_delivery(
    task: task_models.Task,
    job_name: str,
    *args: object,
    defer_by: datetime.timedelta | None = None,
    job_id: str | None = None,
) -> Delivery:
    """Record a delivery on the row; the caller commits, then calls ``deliver``.

    Stamping in the same transaction that inserts or advances the row means a
    crash between commit and enqueue leaves a row whose job arq doesn't know,
    which the reaper re-delivers.
    """
    _check_job_args(task, job_name, args)
    delivery = Delivery(
        task_id=task.id,
        job_name=job_name,
        args=args,
        job_id=job_id or new_job_id(job_name, task.id),
        defer_by=defer_by,
    )
    task.enqueued_at = datetime.datetime.now(datetime.UTC)
    task.arq_job_id = delivery.job_id
    return delivery


async def deliver(arq_redis: arq.ArqRedis | None, delivery: Delivery) -> bool:
    """Enqueue a stamped delivery. Never raises: the reaper covers a failure."""
    log = logger.bind(
        task_id=str(delivery.task_id),
        job=delivery.job_name,
        job_id=delivery.job_id,
    )
    if arq_redis is None:
        log.warning("task_enqueue_skipped_no_redis")
        return False
    try:
        job = await arq_redis.enqueue_job(
            delivery.job_name,
            *delivery.args,
            _job_id=delivery.job_id,
            _defer_by=delivery.defer_by,
        )
    except Exception:
        log.exception("task_enqueue_failed")
        return False
    if job is None:
        log.warning("task_enqueue_dropped")
        return False
    return True


async def enqueue_task(
    session: AsyncSession,
    arq_redis: arq.ArqRedis | None,
    task: task_models.Task,
    job_name: str,
    *args: object,
    defer_by: datetime.timedelta | None = None,
    job_id: str | None = None,
) -> Delivery:
    """Stamp a delivery on *task*, commit the session, then enqueue the job."""
    delivery = stamp_delivery(task, job_name, *args, defer_by=defer_by, job_id=job_id)
    await session.commit()
    await deliver(arq_redis, delivery)
    return delivery


async def complete_task(
    session: AsyncSession,
    task: task_models.Task,
    result: dict[str, object],
) -> None:
    """Mark a task completed.

    Sets status to COMPLETED, records the result dict, and timestamps
    the completion.  Parent-completion cascading is handled by the
    caller (e.g. ``worker._check_parent_completion``) so that
    worker-specific concerns (sequential sibling enqueue, post-sync
    dedup, step-mode) stay in one place.
    """
    task.status = types_module.SyncStatus.COMPLETED
    task.result = result
    task.completed_at = datetime.datetime.now(datetime.UTC)


async def recover_failed_session(session: AsyncSession) -> None:
    """Roll back a failed job's transaction so its session can record FAILED.

    Call at the top of a job's failure handler. The failure may have left a
    transaction that refuses every further statement: a connection the server
    ended (``idle_in_transaction_session_timeout``), a failed flush, or a core
    statement the server rejected (the session still reports active then). So
    it always rolls back, dropping the job's uncommitted work, then reloads
    the task rows the session holds so the handler can keep using them
    (an async session can't lazy-load the expired attributes).
    """
    if not isinstance(session, sa_async.AsyncSession):
        return  # a test double has no transaction
    if not session.in_transaction():
        return
    await session.rollback()
    for obj in list(session.identity_map.values()):
        if isinstance(obj, task_models.Task):
            try:
                await session.refresh(obj)
            except sa.exc.InvalidRequestError:
                session.expunge(obj)  # the row is gone


async def fail_task(
    session: AsyncSession,
    task: task_models.Task,
    error_message: str,
) -> None:
    """Mark a task as failed."""
    task.status = types_module.SyncStatus.FAILED
    task.error_message = error_message
    task.completed_at = datetime.datetime.now(datetime.UTC)


async def cancel_task(
    session: AsyncSession,
    task: task_models.Task,
    reason: str,
) -> None:
    """Mark a task cancelled, recording why in ``error_message``."""
    task.status = types_module.SyncStatus.CANCELLED
    task.error_message = reason
    task.completed_at = datetime.datetime.now(datetime.UTC)


def is_terminal(task: task_models.Task) -> bool:
    """Whether the task's row is in a terminal status (will never run again)."""
    return task.status in types_module.TERMINAL_STATUSES


def is_duplicate_delivery(task: task_models.Task, job: str) -> bool:
    """True (and logged) when the row is already terminal.

    arq can deliver a job more than once (SIGTERM retry, re-enqueue). A delivery
    whose task already finished must not run the work again.
    """
    if not is_terminal(task):
        return False
    logger.info(
        "duplicate_delivery",
        job=job,
        task_id=str(task.id),
        status=task.status.value,
    )
    return True


# ---------------------------------------------------------------------------
# Claims: one delivery at a time owns a task row
# ---------------------------------------------------------------------------

_CLAIMABLE = (types_module.SyncStatus.PENDING, types_module.SyncStatus.DEFERRED)
# A lost race re-reads the row and tries again; more than a couple of rounds
# means it keeps changing under us, and the reaper will redeliver if needed.
_CLAIM_ROUNDS = 3


@dataclasses.dataclass(frozen=True)
class Claim:
    """A won claim: the attempt this delivery owns and the status it took over."""

    attempt: int
    previous_status: types_module.SyncStatus


def _claimable(task: task_models.Task, job_id: str | None) -> bool:
    if task.status in _CLAIMABLE:
        return True
    # A RUNNING row is only taken over by a delivery with the same job id: arq's
    # retry of the job that claimed it. arq's in-progress key normally keeps
    # that id from running twice at once, but heartbeat.py rewrites it with a
    # 60s TTL, so a loop stalled longer lets another worker start the same id.
    # The claim then fences the stale run's DB writes; its external side
    # effects can still happen twice. Legacy RUNNING rows with no arq_job_id
    # never match and wait for startup recovery (worker._recover_running).
    return (
        task.status == types_module.SyncStatus.RUNNING
        and job_id is not None
        and task.arq_job_id == job_id
    )


def _remember_claim(task_id: uuid.UUID, attempt: int) -> None:
    claims = task_models.claimed_attempts.get()
    if claims is None:
        claims = {}
        task_models.claimed_attempts.set(claims)
    claims[task_id] = attempt


async def claim(
    session: AsyncSession,
    task: task_models.Task,
    job_id: str | None,
) -> Claim | None:
    """Move *task* to RUNNING for this delivery, or return None if it can't.

    One conditional UPDATE on the (status, attempt) this delivery read, so of
    two concurrent claimers exactly one matches. The winner bumps ``attempt``,
    which fences every earlier claimer's writes, and records its job id so a
    stray delivery can't take over a live RUNNING row. Commits on success.
    """
    for _ in range(_CLAIM_ROUNDS):
        if not _claimable(task, job_id):
            return None
        seen_status, seen_attempt = task.status, task.attempt
        now = datetime.datetime.now(datetime.UTC)
        row = (
            await session.execute(
                sa.update(task_models.Task)
                .where(
                    task_models.Task.id == task.id,
                    task_models.Task.status == seen_status,
                    task_models.Task.attempt == seen_attempt,
                )
                .values(
                    status=types_module.SyncStatus.RUNNING,
                    attempt=task_models.Task.attempt + 1,
                    started_at=now,
                    arq_job_id=sa.func.coalesce(job_id, task_models.Task.arq_job_id),
                    lease_expires_at=task_models.db_clock_offset(LEASE_TTL),
                )
                .returning(task_models.Task.attempt, task_models.Task.arq_job_id)
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
        if row is None:
            await session.rollback()
            await session.refresh(task)
            continue
        await session.commit()
        for key, value in (
            ("status", types_module.SyncStatus.RUNNING),
            ("attempt", row.attempt),
            ("started_at", now),
            ("arq_job_id", row.arq_job_id),
        ):
            orm.attributes.set_committed_value(task, key, value)
        _remember_claim(task.id, row.attempt)
        return Claim(attempt=row.attempt, previous_status=seen_status)
    return None


async def claim_delivery(
    session: AsyncSession,
    task: task_models.Task,
    job: str,
    job_id: str | None,
) -> Claim | None:
    """Entry guard for every task job: claim the row or say why not (logged).

    A terminal row is a duplicate delivery and touches nothing. An active row
    another delivery owns, or one that changed while claiming, loses.
    """
    if is_duplicate_delivery(task, job):
        return None
    won = await claim(session, task, job_id)
    if won is None:
        logger.info(
            "task_claim_lost",
            job=job,
            task_id=str(task.id),
            status=task.status.value,
            attempt=task.attempt,
            job_id=job_id,
            owner_job_id=task.arq_job_id,
        )
    return won


async def release_for_redelivery(
    session: AsyncSession,
    task: task_models.Task,
) -> bool:
    """Reset a recovered row to PENDING and fence whoever held it.

    Recovery (reaper, startup) calls this before stamping a new delivery. The
    bump means a stalled-but-alive holder can't write past this point, even
    before the new delivery claims. Conditional on the attempt read, so a claim
    that lands in between wins; the caller commits.

    Returns:
        False when the row changed since it was read; skip redelivery.
    """
    row = (
        await session.execute(
            sa.update(task_models.Task)
            .where(
                task_models.Task.id == task.id,
                task_models.Task.status == task.status,
                task_models.Task.attempt == task.attempt,
            )
            .values(
                status=types_module.SyncStatus.PENDING,
                attempt=task_models.Task.attempt + 1,
                lease_expires_at=None,
            )
            .returning(task_models.Task.attempt)
            .execution_options(synchronize_session=False)
        )
    ).scalar_one_or_none()
    if row is None:
        return False
    orm.attributes.set_committed_value(task, "status", types_module.SyncStatus.PENDING)
    orm.attributes.set_committed_value(task, "attempt", row)
    orm.attributes.set_committed_value(task, "lease_expires_at", None)
    return True


# ---------------------------------------------------------------------------
# Leases: a claimed delivery proves it is alive by renewing its row
# ---------------------------------------------------------------------------

# Four renewals per TTL, so one or two slow or failed renewals (a DB blip, a
# GC pause) don't expire a live run; the reaper adds its own grace on top.
LEASE_TTL = datetime.timedelta(seconds=120)
LEASE_RENEW_INTERVAL = datetime.timedelta(seconds=30)
# The row lock a renewal waits on is almost always its own handler's open
# transaction, so a wait is no sign of death. Bounded well under the renew
# interval so a cycle never stacks behind the previous one; the next cycle
# retries, and three more attempts fit in the TTL.
LEASE_RENEW_LOCK_TIMEOUT = datetime.timedelta(seconds=2)


ALIVE_KEY_PREFIX = "resonance:task-alive:"


def alive_key(task_id: uuid.UUID | str, attempt: int) -> str:
    """Redis key a live holder of (task, attempt) keeps fresh.

    Separate from the DB lease because the holder's own open transaction can
    hold the row lock and block DB renewal; this key proves the process is
    alive regardless.
    """
    return f"{ALIVE_KEY_PREFIX}{task_id}:{attempt}"


async def _mark_alive(redis: Any, claims: dict[uuid.UUID, int]) -> None:
    ttl_ms = int(LEASE_TTL.total_seconds() * 1000)
    for task_id, attempt in list(claims.items()):
        try:
            await redis.psetex(alive_key(task_id, attempt), ttl_ms, b"1")
        except Exception:
            logger.warning(
                "task_alive_refresh_failed", task_id=str(task_id), attempt=attempt
            )


class LeaseRenewBlocked(Exception):  # noqa: N818 — a retryable outcome, not a bug
    """A renewal gave up waiting on the row lock; retry next cycle."""


async def renew_lease(
    session_factory: async_sessionmaker[AsyncSession],
    task_id: uuid.UUID,
    attempt: int,
) -> bool:
    """Extend this delivery's lease, in a session of its own.

    Only the claimer's attempt on a RUNNING row matches, so a fenced run can't
    renew. Never the handler's session: committing there would commit the
    handler's half-done state.

    Returns:
        False once the lease is gone: fenced (logged ``task_lease_lost``), or
        the row left RUNNING under this attempt.

    Raises:
        LeaseRenewBlocked: the row lock wasn't free within
            ``LEASE_RENEW_LOCK_TIMEOUT``.
    """
    async with session_factory() as session:
        if session.bind.dialect.name == "postgresql":
            timeout_ms = int(LEASE_RENEW_LOCK_TIMEOUT.total_seconds() * 1000)
            await session.execute(sa.text(f"SET LOCAL lock_timeout = {timeout_ms}"))
        try:
            renewed = await _renew_row(session, task_id, attempt)
        except sa.exc.DBAPIError as exc:
            if getattr(exc.orig, "sqlstate", None) != _LOCK_NOT_AVAILABLE:
                raise
            await session.rollback()
            raise LeaseRenewBlocked from exc
        await session.commit()
        if renewed is not None:
            return True
        current = (
            await session.execute(
                sa.select(task_models.Task.attempt).where(
                    task_models.Task.id == task_id
                )
            )
        ).scalar_one_or_none()
    if current != attempt:
        logger.warning(
            "task_lease_lost",
            task_id=str(task_id),
            attempt=attempt,
            current_attempt=current,
        )
    return False


_LOCK_NOT_AVAILABLE = "55P03"


async def _renew_row(
    session: AsyncSession, task_id: uuid.UUID, attempt: int
) -> uuid.UUID | None:
    # Lock first: an UPDATE computes its new values before it waits on a row
    # lock (and keeps them unless the row changed), so a renewal that queued
    # behind its handler would write a lease measured from before the wait.
    await session.execute(
        sa.select(task_models.Task.id)
        .where(task_models.Task.id == task_id)
        .with_for_update(key_share=True)
    )
    return (
        await session.execute(
            sa.update(task_models.Task)
            .where(
                task_models.Task.id == task_id,
                task_models.Task.attempt == attempt,
                task_models.Task.status == types_module.SyncStatus.RUNNING,
            )
            .values(lease_expires_at=task_models.db_clock_offset(LEASE_TTL))
            .returning(task_models.Task.id)
            .execution_options(synchronize_session=False)
        )
    ).scalar_one_or_none()


async def release_leases(
    session_factory: async_sessionmaker[AsyncSession],
    claims: dict[uuid.UUID, int],
    redis: Any = None,
) -> None:
    """Drop the leases a finished job still holds; a fenced attempt matches none.

    A row can stay RUNNING after its job returns (a parent waiting on its
    children). Nobody holds it then, so its lease must not expire into a
    reaper recovery.
    """
    if not claims:
        return
    async with session_factory() as session:
        await session.execute(
            sa.update(task_models.Task)
            .where(
                sa.tuple_(task_models.Task.id, task_models.Task.attempt).in_(
                    list(claims.items())
                )
            )
            .values(lease_expires_at=None)
            .execution_options(synchronize_session=False)
        )
        await session.commit()
    if redis is not None:
        await redis.delete(*(alive_key(t, a) for t, a in claims.items()))


# Set when the job's own session commits, so the DB lease catches up right
# after a long locked section instead of up to an interval later.
_job_committed: contextvars.ContextVar[asyncio.Event | None] = contextvars.ContextVar(
    "job_committed", default=None
)


@sa.event.listens_for(orm.Session, "after_commit")
def _wake_renewer(_session: orm.Session) -> None:
    event = _job_committed.get()
    if event is not None:
        event.set()


async def _until_next_renewal(committed: asyncio.Event | None) -> None:
    interval = LEASE_RENEW_INTERVAL.total_seconds()
    if committed is None:
        await asyncio.sleep(interval)
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(committed.wait(), timeout=interval)
    committed.clear()


async def _renew_leases_forever(
    session_factory: async_sessionmaker[AsyncSession],
    claims: dict[uuid.UUID, int],
    redis: Any = None,
    committed: asyncio.Event | None = None,
) -> None:
    # This task copied the job's context; its own renewal commits mustn't wake it.
    _job_committed.set(None)
    lost: set[tuple[uuid.UUID, int]] = set()
    while True:
        await _until_next_renewal(committed)
        # First and independent of the DB: it must stay fresh while the DB
        # renewal is blocked on this job's own row lock.
        if redis is not None:
            await _mark_alive(
                redis, {t: a for t, a in claims.items() if (t, a) not in lost}
            )
        for task_id, attempt in list(claims.items()):
            if (task_id, attempt) in lost:
                continue
            try:
                if not await renew_lease(session_factory, task_id, attempt):
                    lost.add((task_id, attempt))
            except LeaseRenewBlocked:
                logger.info(
                    "task_lease_renew_blocked", task_id=str(task_id), attempt=attempt
                )
            except Exception:
                logger.warning(
                    "task_lease_renew_failed", task_id=str(task_id), attempt=attempt
                )


async def _reap_renewer(renewer: asyncio.Task[None]) -> None:
    """Wait out the renewer's own cancellation, but not one aimed at this job."""
    try:
        await renewer
    except asyncio.CancelledError:
        current = asyncio.current_task()
        if current is not None and current.cancelling():
            raise


def _session_factory_of(
    args: tuple[object, ...],
) -> async_sessionmaker[AsyncSession] | None:
    ctx = args[0] if args else None
    if isinstance(ctx, dict):
        factory = ctx.get("session_factory")
        if factory is not None:
            return typing.cast("async_sessionmaker[AsyncSession]", factory)
    return None


def _redis_of(args: tuple[object, ...]) -> Any:
    ctx = args[0] if args else None
    return ctx.get("redis") if isinstance(ctx, dict) else None


def task_job[**P](
    fn: collections.abc.Callable[P, collections.abc.Awaitable[None]],
) -> collections.abc.Callable[P, collections.abc.Coroutine[Any, Any, None]]:
    """Wrap an arq task job: scope its claims, keep their leases, stop once fenced.

    A fenced write means a newer delivery owns the row; this one exits without
    touching it again (the session rolls back as the exception unwinds). While
    the job runs, its claims' leases are renewed from the worker context's
    session factory; when it ends, they are released.
    """

    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        claims: dict[uuid.UUID, int] = {}
        token = task_models.claimed_attempts.set(claims)
        session_factory = _session_factory_of(args)
        redis = _redis_of(args)
        committed = asyncio.Event()
        committed_token = _job_committed.set(committed)
        renewer = (
            asyncio.create_task(
                _renew_leases_forever(session_factory, claims, redis, committed)
            )
            if session_factory is not None
            else None
        )
        cancelled = False
        try:
            await fn(*args, **kwargs)
        except asyncio.CancelledError:
            # arq retries or aborts a cancelled job. Leave its leases to expire
            # so the reaper still covers a retry that never comes.
            cancelled = True
            raise
        except task_models.TaskFenced as fenced:
            logger.warning(
                "task_write_fenced",
                job=fn.__name__,
                task_id=str(fenced.task_id),
                attempt=fenced.mine,
                current_attempt=fenced.current,
            )
        finally:
            task_models.claimed_attempts.reset(token)
            _job_committed.reset(committed_token)
            if renewer is not None and session_factory is not None:
                renewer.cancel()
                await _reap_renewer(renewer)
            if session_factory is not None and not cancelled:
                try:
                    await release_leases(session_factory, claims, redis)
                except Exception:
                    logger.warning("task_lease_release_failed", claims=len(claims))

    return wrapper


async def is_cancelled(
    session: AsyncSession,
    task: task_models.Task,
) -> bool:
    """Check whether this task or any ancestor has been cancelled.

    Walks up the parent chain so nested children (grandchildren, etc.) also
    detect cancellation.

    Args:
        session: Active database session.
        task: The task to check.

    Returns:
        True if this task or any ancestor was cancelled.
    """
    if task.status == types_module.SyncStatus.CANCELLED:
        return True

    current_parent_id = task.parent_id
    visited: set[object] = set()

    while current_parent_id is not None and current_parent_id not in visited:
        visited.add(current_parent_id)
        parent_result = await session.execute(
            sa.select(task_models.Task).where(task_models.Task.id == current_parent_id)
        )
        parent = parent_result.scalar_one_or_none()
        if parent is None:
            break

        if parent.status == types_module.SyncStatus.CANCELLED:
            logger.info(
                "cancellation_detected",
                task_id=str(task.id),
                cancelled_ancestor_id=str(current_parent_id),
            )
            return True

        current_parent_id = parent.parent_id

    return False


async def cancel_active_children(
    session: AsyncSession,
    parent_id: object,
) -> list[tuple[task_models.Task, types_module.SyncStatus]]:
    """Cancel every non-terminal child of a cancelled parent.

    PENDING and DEFERRED children are never picked up afterwards. A RUNNING
    child is marked too; ``request_cancel`` aborts its arq job.

    Args:
        session: Active database session.
        parent_id: The parent task ID whose children should be cancelled.

    Returns:
        Each cancelled child with the status it had before.
    """
    active_result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.parent_id == parent_id,
            task_models.Task.status.in_(types_module.ACTIVE_STATUSES),
        )
    )
    cancelled = [(child, child.status) for child in active_result.scalars().all()]
    for child, _ in cancelled:
        await cancel_task(session, child, "Parent task was cancelled")
    await _fence_out_holders(session, [child for child, _ in cancelled])
    if cancelled:
        logger.info(
            "active_children_cancelled",
            parent_id=str(parent_id),
            count=len(cancelled),
        )
    return cancelled


async def _fence_out_holders(
    session: AsyncSession, tasks: list[task_models.Task]
) -> None:
    """Bump ``attempt`` on rows cancelled from outside their job.

    A run whose abort missed would otherwise still pass the fence, and its
    DEFERRED or PENDING write (both active, so ``_keep_cancelled`` lets them
    through) would revive the task. Not part of ``cancel_task``, which a
    claimer also calls on its own row.
    """
    if not tasks:
        return
    bumped = (
        await session.execute(
            sa.update(task_models.Task)
            .where(task_models.Task.id.in_([t.id for t in tasks]))
            .values(attempt=task_models.Task.attempt + 1)
            .returning(task_models.Task.id, task_models.Task.attempt)
            .execution_options(synchronize_session=False)
        )
    ).tuples()
    by_id = {t.id: t for t in tasks}
    for task_id, attempt in bumped:
        orm.attributes.set_committed_value(by_id[task_id], "attempt", attempt)


class CancelOutcome(enum.StrEnum):
    """Result of a cancel request."""

    CANCELLED = "cancelled"
    ALREADY_TERMINAL = "already_terminal"
    NOT_FOUND = "not_found"


@dataclasses.dataclass(frozen=True)
class CancelResult:
    """Outcome of ``request_cancel`` plus the task (when found)."""

    outcome: CancelOutcome
    task: task_models.Task | None = None
    children_cancelled: int = 0


async def request_cancel(
    session: AsyncSession,
    task_id: uuid.UUID,
    user_id: uuid.UUID,
    reason: str = "Cancelled by user",
    arq_redis: arq.ArqRedis | None = None,
) -> CancelResult:
    """Cancel a user's task and its non-terminal children, then commit.

    Shared by the API and UI cancel routes. Only the owning user's tasks are
    visible; another user's task reads as not found. After the commit, the
    arq jobs of anything that was RUNNING are aborted (bounded wait).

    Args:
        session: Active database session.
        task_id: The task to cancel.
        user_id: The requesting user (ownership scope).
        reason: Stored in ``error_message`` on the cancelled task.
        arq_redis: arq pool used to abort running jobs; ``None`` skips aborts.

    Returns:
        A ``CancelResult``: CANCELLED, ALREADY_TERMINAL (task untouched), or
        NOT_FOUND.
    """
    result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.id == task_id,
            task_models.Task.user_id == user_id,
        )
    )
    task = result.scalar_one_or_none()
    if task is None:
        return CancelResult(CancelOutcome.NOT_FOUND)
    if is_terminal(task):
        return CancelResult(CancelOutcome.ALREADY_TERMINAL, task=task)

    was_running = task.status == types_module.SyncStatus.RUNNING
    await cancel_task(session, task, reason)
    await _fence_out_holders(session, [task])
    children = await cancel_active_children(session, task.id)
    await session.commit()
    logger.info(
        "task_cancelled",
        task_id=str(task.id),
        children_cancelled=len(children),
    )

    running = [task] if was_running else []
    running += [c for c, prior in children if prior == types_module.SyncStatus.RUNNING]
    await asyncio.gather(
        *(
            abort_job(arq_redis, row.id, row.arq_job_id)
            for row in running
            if row.arq_job_id is not None
        )
    )
    if was_running and task.parent_id is not None:
        await _redeliver_to_advance_parent(session, arq_redis, task)
    return CancelResult(
        CancelOutcome.CANCELLED, task=task, children_cancelled=len(children)
    )


async def _redeliver_to_advance_parent(
    session: AsyncSession,
    arq_redis: arq.ArqRedis | None,
    task: task_models.Task,
) -> None:
    """Hand an aborted child back to arq so its pipeline doesn't stall.

    An aborted job exits with CancelledError and never reaches its sibling
    advance. A fresh delivery hits the entry guard, which sees CANCELLED and
    advances the next sibling or settles the parent.
    """
    job = dispatch_module.job_for(task)
    if job is None:
        return
    await enqueue_task(session, arq_redis, task, job[0], *job[1])
    logger.info("cancelled_child_redelivered_to_advance", task_id=str(task.id))


# arq's abort waits for the job's result unless bounded; a cancel request must
# return promptly. The row is already CANCELLED, so a slow abort only delays
# when the job notices.
ABORT_TIMEOUT_SECONDS = 2.0
_ABORT_POLL_SECONDS = 0.25


async def abort_job(
    arq_redis: arq.ArqRedis | None, task_id: uuid.UUID, job_id: str
) -> bool:
    """Ask arq to abort a running job; never raises.

    Returns:
        True if arq confirmed the abort within the timeout.
    """
    log = logger.bind(task_id=str(task_id), job_id=job_id)
    if arq_redis is None:
        log.warning("task_abort_skipped_no_redis")
        return False
    job = arq_jobs.Job(job_id, arq_redis, _queue_name=arq_redis.default_queue_name)
    try:
        aborted = await asyncio.wait_for(
            job.abort(timeout=ABORT_TIMEOUT_SECONDS, poll_delay=_ABORT_POLL_SECONDS),
            timeout=ABORT_TIMEOUT_SECONDS + 1,
        )
    except TimeoutError:
        log.info("task_abort_pending")
        return False
    except Exception:
        log.exception("task_abort_failed")
        return False
    log.info("task_abort_requested", confirmed=aborted)
    return aborted
