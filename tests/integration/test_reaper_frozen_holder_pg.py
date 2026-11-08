"""Reaper handling of an expired row still locked by a frozen holder, on real
Postgres as a non-superuser role."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import arq.jobs as arq_jobs
import pytest
import sqlalchemy as sa

import resonance.models.task as task_models
import resonance.sync.holder as holder_module
import resonance.sync.lifecycle as lifecycle_module
import resonance.sync.reaper as reaper_module
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import arq

    from tests.conftest import TaskDB
    from tests.integration.conftest import AppRole

pytestmark = pytest.mark.integration

S = types_module.SyncStatus
Mode = reaper_module.LeaseRecovery
Action = reaper_module.Action
WORKER_APP = "resonance-worker:test:1"


@pytest.fixture(autouse=True)
def short_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lifecycle_module, "LEASE_TTL", datetime.timedelta(seconds=1))


@pytest.fixture
async def redis(arq_redis: arq.ArqRedis) -> AsyncIterator[arq.ArqRedis]:
    async def clear() -> None:
        keys = await arq_redis.keys(f"{reaper_module.SEEN_PREFIX}*")
        await arq_redis.delete(
            reaper_module.DECISIONS_KEY, reaper_module.LAST_TICK_KEY, *keys
        )

    await clear()
    yield arq_redis
    await clear()


async def _expired_running(db: TaskDB) -> task_models.Task:
    task = await db.add(status=S.PENDING)

    async def claim() -> None:
        # In its own task, so the claim it records for the fence doesn't leak
        # into the test's context.
        async with db.factory() as session:
            row = await session.get(task_models.Task, task.id)
            assert row is not None
            assert await lifecycle_module.claim(session, row, "stalled") is not None

    await asyncio.create_task(claim())
    async with db.factory() as session:
        past = (await session.execute(sa.select(sa.func.now()))).scalar_one()
    await db.update(task.id, lease_expires_at=past - reaper_module.LEASE_GRACE * 4)
    return await db.get(task.id)


@contextlib.asynccontextmanager
async def _frozen_holder(
    role: AppRole, task_id: uuid.UUID, application_name: str = WORKER_APP
) -> AsyncIterator[tuple[int, sa.ext.asyncio.AsyncConnection]]:
    """A worker connection that flushed a write to its row and went idle."""
    engine = role.engine(application_name)
    conn = await engine.connect()
    try:
        pid = (await conn.execute(sa.select(sa.func.pg_backend_pid()))).scalar_one()
        await conn.execute(
            sa.update(task_models.Task)
            .where(task_models.Task.id == task_id)
            .values(progress_current=7)
        )
        yield pid, conn
    finally:
        with contextlib.suppress(Exception):
            await conn.rollback()
            await conn.close()
        await engine.dispose()


async def _reap(
    role: AppRole, redis: arq.ArqRedis, mode: Mode
) -> list[reaper_module.Decision]:
    with patch.object(
        reaper_module,
        "job_status",
        AsyncMock(return_value=arq_jobs.JobStatus.in_progress),
    ):
        return await asyncio.wait_for(
            reaper_module.reap(role.factory, redis, lease_recovery=mode), timeout=15
        )


async def _backend_alive(db: TaskDB, pid: int) -> bool:
    async with db.factory() as session:
        return bool(
            (
                await session.execute(
                    sa.text("SELECT count(*) FROM pg_stat_activity WHERE pid = :p"),
                    {"p": pid},
                )
            ).scalar_one()
        )


def _only(
    decisions: list[reaper_module.Decision], action: Action
) -> reaper_module.Decision:
    found = [d for d in decisions if d.action == action]
    assert len(found) == 1, [d.action for d in decisions]
    return found[0]


async def test_log_mode_records_frozen_holder_once(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    task = await _expired_running(pg_task_db)
    async with _frozen_holder(pg_app_role, task.id) as (pid, _):
        await asyncio.sleep(1.5)
        first = await _reap(pg_app_role, redis, Mode.LOG)
        second = await _reap(pg_app_role, redis, Mode.LOG)
        assert await _backend_alive(pg_task_db, pid)

    frozen = _only(first, Action.HOLDER_FROZEN)
    assert frozen.detail is not None
    assert frozen.detail["pid"] == pid
    assert frozen.detail["state"] == "idle in transaction"
    assert frozen.detail["application_name"] == WORKER_APP
    assert float(str(frozen.detail["idle_seconds"])) > 1
    assert Action.HOLDER_FROZEN in [d.action for d in second]
    state = await reaper_module.read_state(redis)
    assert sorted(d["action"] for d in state.decisions) == [
        "holder_frozen",
        "lease_expired",
    ]
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.RUNNING, 1)


async def test_active_mode_terminates_then_next_tick_recovers(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    task = await _expired_running(pg_task_db)
    async with _frozen_holder(pg_app_role, task.id) as (pid, _):
        await asyncio.sleep(1.5)
        first = await _reap(pg_app_role, redis, Mode.ACTIVE)
        assert [d.action for d in first] == [Action.HOLDER_TERMINATED]
        assert first[0].detail is not None
        assert first[0].detail["pid"] == pid
        assert not await _backend_alive(pg_task_db, pid)
        row = await pg_task_db.get(task.id)
        assert (row.status, row.attempt, row.progress_current) == (S.RUNNING, 1, 0)

        second = await _reap(pg_app_role, redis, Mode.ACTIVE)

    assert [d.action for d in second] == [Action.LEASE_RECOVERED]
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.PENDING, 2)
    assert row.arq_job_id == second[0].job_id
    assert await reaper_module.job_status(redis, row.arq_job_id) == (
        arq_jobs.JobStatus.queued
    )


async def test_active_holder_is_not_terminated(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    task = await _expired_running(pg_task_db)
    async with _frozen_holder(pg_app_role, task.id) as (pid, conn):
        await asyncio.sleep(1.5)
        busy = asyncio.create_task(conn.execute(sa.text("SELECT pg_sleep(3)")))
        await asyncio.sleep(0.3)
        decisions = await _reap(pg_app_role, redis, Mode.ACTIVE)
        await busy

    locked = _only(decisions, Action.HOLDER_LOCKED)
    assert locked.detail is not None
    assert locked.detail["reason"] == "not_idle_in_transaction"
    assert locked.detail["pid"] == pid
    assert (await pg_task_db.get(task.id)).status == S.RUNNING


async def test_non_worker_holder_is_not_terminated(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    task = await _expired_running(pg_task_db)
    async with _frozen_holder(pg_app_role, task.id, "psql") as (pid, _):
        await asyncio.sleep(1.5)
        decisions = await _reap(pg_app_role, redis, Mode.ACTIVE)
        assert await _backend_alive(pg_task_db, pid)

    locked = _only(decisions, Action.HOLDER_LOCKED)
    assert locked.detail is not None
    assert locked.detail["reason"] == "not_worker"


async def test_holder_idle_under_ttl_is_not_terminated(
    pg_task_db: TaskDB,
    pg_app_role: AppRole,
    redis: arq.ArqRedis,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(lifecycle_module, "LEASE_TTL", datetime.timedelta(minutes=5))
    task = await _expired_running(pg_task_db)
    async with _frozen_holder(pg_app_role, task.id) as (pid, _):
        decisions = await _reap(pg_app_role, redis, Mode.ACTIVE)
        assert await _backend_alive(pg_task_db, pid)

    locked = _only(decisions, Action.HOLDER_LOCKED)
    assert locked.detail is not None
    assert locked.detail["reason"] == "idle_under_ttl"


async def test_live_holder_key_skips_holder_check(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    task = await _expired_running(pg_task_db)
    key = lifecycle_module.alive_key(task.id, 1)
    await redis.psetex(key, 60_000, b"1")
    try:
        async with _frozen_holder(pg_app_role, task.id) as (pid, _):
            await asyncio.sleep(1.5)
            decisions = await _reap(pg_app_role, redis, Mode.ACTIVE)
            assert await _backend_alive(pg_task_db, pid)
    finally:
        await redis.delete(key)

    assert [d.action for d in decisions] == [Action.HOLDER_ALIVE]


async def test_terminate_privilege_error_is_recorded(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    """The holder runs as a superuser role the reaper can't signal; the tick
    records the refusal and carries on."""
    task = await _expired_running(pg_task_db)
    other = await pg_task_db.add(status=S.PENDING)
    engine = sa.ext.asyncio.create_async_engine(
        pg_task_db.factory.kw["bind"].url,
        connect_args={
            "server_settings": {
                "application_name": WORKER_APP,
                "search_path": (await _schema(pg_task_db)),
            }
        },
    )
    real_find = holder_module.find_holder

    async def visible_state(*args: Any) -> holder_module.Lookup:
        # A non-superuser can't read another role's state; pretend it can so
        # the privilege check on termination is what's exercised.
        lookup = await real_find(*args)
        assert lookup.holder is not None
        holder = dataclasses.replace(
            lookup.holder, state="idle in transaction", idle_seconds=60.0
        )
        return holder_module.Lookup(holder)

    try:
        async with engine.connect() as conn:
            await conn.execute(
                sa.update(task_models.Task)
                .where(task_models.Task.id == task.id)
                .values(progress_current=7)
            )
            await asyncio.sleep(1.5)
            with (
                patch.object(holder_module, "find_holder", visible_state),
                patch.object(
                    holder_module,
                    "_TERMINATE",
                    sa.text("SELECT pg_terminate_backend(:pid, :timeout_ms)"),
                ),
            ):
                decisions = await _reap(pg_app_role, redis, Mode.ACTIVE)
            await conn.rollback()
    finally:
        await engine.dispose()

    failed = _only(decisions, Action.TERMINATE_FAILED)
    assert failed.detail is not None
    assert failed.detail["reason"] == "insufficient_privilege"
    assert (await pg_task_db.get(task.id)).status == S.RUNNING
    assert (await pg_task_db.get(other.id)).status == S.PENDING


async def _schema(db: TaskDB) -> str:
    async with db.factory() as session:
        return str(
            (await session.execute(sa.text("SELECT current_schema()"))).scalar_one()
        )


async def _insert_child(
    conn: sa.ext.asyncio.AsyncConnection, parent: uuid.UUID
) -> None:
    await conn.execute(
        sa.insert(task_models.Task).values(
            id=uuid.uuid4(),
            task_type=types_module.TaskType.TIME_RANGE,
            status=S.PENDING,
            parent_id=parent,
        )
    )


async def test_two_live_multixact_members_are_not_terminated(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    """Two open child inserts key-share lock the parent: xmax is a multixact
    with two live members, so no single holder is singled out."""
    task = await _expired_running(pg_task_db)
    conns = [await pg_app_role.engine(WORKER_APP).connect() for _ in range(2)]
    try:
        for conn in conns:
            await _insert_child(conn, task.id)
        await asyncio.sleep(1.5)
        async with pg_app_role.factory() as session:
            lookup = await holder_module.find_holder(session, task.id)
    finally:
        for conn in conns:
            await conn.rollback()
            await conn.close()

    assert lookup.holder is None
    assert (lookup.reason, lookup.multixact) == ("multiple_holders", True)


async def test_frozen_holder_behind_multixact_is_terminated(
    pg_task_db: TaskDB, pg_app_role: AppRole, redis: arq.ArqRedis
) -> None:
    """A multixact xmax whose only live member is the frozen worker (the
    other locker already ended) still identifies that worker."""
    task = await _expired_running(pg_task_db)
    frozen = await pg_app_role.engine(WORKER_APP).connect()
    done = await pg_app_role.engine("resonance-worker:test:2").connect()
    try:
        pid = (await frozen.execute(sa.select(sa.func.pg_backend_pid()))).scalar_one()
        await _insert_child(frozen, task.id)
        await _insert_child(done, task.id)
        await done.rollback()
        await asyncio.sleep(1.5)
        async with pg_app_role.factory() as session:
            lookup = await holder_module.find_holder(session, task.id)
        decisions = await _reap(pg_app_role, redis, Mode.ACTIVE)
    finally:
        await done.close()
        with contextlib.suppress(Exception):
            await frozen.rollback()
            await frozen.close()

    assert lookup.holder is not None
    assert (lookup.holder.pid, lookup.multixact) == (pid, True)
    assert [d.action for d in decisions] == [Action.HOLDER_TERMINATED]
    assert not await _backend_alive(pg_task_db, pid)
