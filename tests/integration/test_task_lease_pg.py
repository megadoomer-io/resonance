"""Row leases and reaper recovery of expired RUNNING rows, on real Postgres."""

from __future__ import annotations

import asyncio
import datetime
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import arq.jobs as arq_jobs
import pytest
import sqlalchemy as sa

import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.sync.reaper as reaper_module
import resonance.types as types_module
import resonance.worker as worker_module

if TYPE_CHECKING:
    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration

S = types_module.SyncStatus
Mode = reaper_module.LeaseRecovery
Action = reaper_module.Action


async def _db_now(db: TaskDB) -> datetime.datetime:
    async with db.factory() as session:
        return (await session.execute(sa.select(sa.func.now()))).scalar_one()


async def _claim(db: TaskDB, task_id: uuid.UUID, job_id: str) -> int:
    """Claim as another delivery would: in its own task, so the claim it
    records for the fence doesn't leak into the test's context."""

    async def claim() -> int:
        async with db.factory() as session:
            task = await session.get(task_models.Task, task_id)
            assert task is not None
            won = await lifecycle_module.claim(session, task, job_id)
            assert won is not None
            return won.attempt

    return await asyncio.create_task(claim())


async def _expired_running(db: TaskDB, **fields: Any) -> task_models.Task:
    """A RUNNING row whose holder stopped renewing well past TTL + grace."""
    task = await db.add(status=S.PENDING, **fields)
    await _claim(db, task.id, "stalled")
    past = await _db_now(db) - reaper_module.LEASE_GRACE * 4
    await db.update(task.id, lease_expires_at=past)
    return await db.get(task.id)


async def _reap(db: TaskDB, mode: Mode) -> list[reaper_module.Decision]:
    arq = AsyncMock()
    arq.default_queue_name = "arq:queue"
    arq.exists.return_value = 0
    with patch.object(
        reaper_module,
        "job_status",
        AsyncMock(return_value=arq_jobs.JobStatus.in_progress),
    ):
        return await reaper_module.reap(db.factory, arq, lease_recovery=mode)


async def test_claim_sets_lease_from_db_clock(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)
    before = await _db_now(pg_task_db)

    await _claim(pg_task_db, task.id, "job")

    row = await pg_task_db.get(task.id)
    assert row.lease_expires_at is not None
    ttl = row.lease_expires_at - before
    assert lifecycle_module.LEASE_TTL - datetime.timedelta(seconds=5) < ttl
    assert ttl <= lifecycle_module.LEASE_TTL + datetime.timedelta(seconds=5)


async def test_renewal_extends_the_lease(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)
    attempt = await _claim(pg_task_db, task.id, "job")
    soon = await _db_now(pg_task_db) + datetime.timedelta(seconds=5)
    await pg_task_db.update(task.id, lease_expires_at=soon)

    assert await lifecycle_module.renew_lease(pg_task_db.factory, task.id, attempt)

    row = await pg_task_db.get(task.id)
    assert row.lease_expires_at is not None and row.lease_expires_at > soon


async def test_fenced_attempt_cannot_renew(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)
    old_attempt = await _claim(pg_task_db, task.id, "a")
    async with pg_task_db.factory() as session:
        loaded = await session.get(task_models.Task, task.id)
        assert loaded is not None
        assert await lifecycle_module.release_for_redelivery(session, loaded)
        await session.commit()
    await _claim(pg_task_db, task.id, "b")
    new_lease = (await pg_task_db.get(task.id)).lease_expires_at

    with patch.object(lifecycle_module, "logger") as log:
        renewed = await lifecycle_module.renew_lease(
            pg_task_db.factory, task.id, old_attempt
        )

    assert not renewed
    assert log.warning.call_args.args[0] == "task_lease_lost"
    assert (await pg_task_db.get(task.id)).lease_expires_at == new_lease


async def test_release_for_redelivery_clears_lease(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)
    await _claim(pg_task_db, task.id, "a")
    async with pg_task_db.factory() as session:
        loaded = await session.get(task_models.Task, task.id)
        assert loaded is not None
        assert await lifecycle_module.release_for_redelivery(session, loaded)
        await session.commit()

    row = await pg_task_db.get(task.id)
    assert (row.status, row.lease_expires_at) == (S.PENDING, None)


@pytest.mark.parametrize(
    "transition",
    ["complete", "fail", "cancel", "defer"],
)
async def test_leaving_running_clears_lease(
    pg_task_db: TaskDB, transition: str
) -> None:
    task = await pg_task_db.add(status=S.PENDING)

    @lifecycle_module.task_job
    async def job(_ctx: dict[str, Any]) -> None:
        async with pg_task_db.factory() as session:
            loaded = await session.get(task_models.Task, task.id)
            assert loaded is not None
            assert await lifecycle_module.claim(session, loaded, "a") is not None
            # The handler's in-memory copy predates any renewal.
            if transition == "complete":
                await lifecycle_module.complete_task(session, loaded, {})
            elif transition == "fail":
                await lifecycle_module.fail_task(session, loaded, "boom")
            elif transition == "cancel":
                await lifecycle_module.cancel_task(session, loaded, "x")
            else:
                loaded.status = S.DEFERRED
            await session.commit()
            assert (await pg_task_db.get(task.id)).lease_expires_at is None

    await job({})


async def test_job_end_releases_lease_of_row_left_running(pg_task_db: TaskDB) -> None:
    """A parent left RUNNING for its children has no holder once its job ends."""
    task = await pg_task_db.add(status=S.PENDING)

    @lifecycle_module.task_job
    async def job(_ctx: dict[str, Any]) -> None:
        await _claim_in_job(pg_task_db, task.id)

    await job({"session_factory": pg_task_db.factory})

    row = await pg_task_db.get(task.id)
    assert (row.status, row.lease_expires_at) == (S.RUNNING, None)


async def _claim_in_job(db: TaskDB, task_id: uuid.UUID) -> None:
    async with db.factory() as session:
        loaded = await session.get(task_models.Task, task_id)
        assert loaded is not None
        assert await lifecycle_module.claim(session, loaded, "a") is not None


async def test_cancelled_job_keeps_lease_to_expire(pg_task_db: TaskDB) -> None:
    """On SIGTERM arq cancels the job; its lease stays so the reaper covers it."""
    task = await pg_task_db.add(status=S.PENDING)
    claimed = asyncio.Event()

    @lifecycle_module.task_job
    async def job(_ctx: dict[str, Any]) -> None:
        await _claim_in_job(pg_task_db, task.id)
        claimed.set()
        await asyncio.Event().wait()

    run = asyncio.create_task(job({"session_factory": pg_task_db.factory}))
    await claimed.wait()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run

    assert (await pg_task_db.get(task.id)).lease_expires_at is not None


async def test_heartbeat_renews_while_job_runs(
    pg_task_db: TaskDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        lifecycle_module, "LEASE_RENEW_INTERVAL", datetime.timedelta(seconds=0.05)
    )
    task = await pg_task_db.add(status=S.PENDING)
    seen: list[datetime.datetime | None] = []

    @lifecycle_module.task_job
    async def job(_ctx: dict[str, Any]) -> None:
        await _claim_in_job(pg_task_db, task.id)
        expired = await _db_now(pg_task_db) - datetime.timedelta(minutes=10)
        await pg_task_db.update(task.id, lease_expires_at=expired)
        await asyncio.sleep(0.3)
        seen.append((await pg_task_db.get(task.id)).lease_expires_at)

    await job({"session_factory": pg_task_db.factory})

    assert seen[0] is not None and seen[0] > await _db_now(pg_task_db)


@pytest.mark.parametrize("mode", [Mode.LOG, Mode.ACTIVE])
async def test_reaper_ignores_live_and_null_leases(
    pg_task_db: TaskDB, mode: Mode
) -> None:
    live = await pg_task_db.add(status=S.PENDING)
    await _claim(pg_task_db, live.id, "live")
    legacy = await pg_task_db.add(
        status=S.RUNNING, attempt=1, arq_job_id="old-pod", lease_expires_at=None
    )

    decisions = await _reap(pg_task_db, mode)

    assert decisions == []
    for task_id in (live.id, legacy.id):
        assert (await pg_task_db.get(task_id)).status == S.RUNNING


async def test_reaper_honours_grace_past_ttl(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)
    await _claim(pg_task_db, task.id, "slow")
    just_expired = await _db_now(pg_task_db) - reaper_module.LEASE_GRACE / 2
    await pg_task_db.update(task.id, lease_expires_at=just_expired)

    assert await _reap(pg_task_db, Mode.ACTIVE) == []


async def test_log_mode_records_without_acting(pg_task_db: TaskDB) -> None:
    task = await _expired_running(pg_task_db)

    decisions = await _reap(pg_task_db, Mode.LOG)

    assert [d.action for d in decisions] == [Action.LEASE_EXPIRED]
    assert decisions[0].job_id == "stalled"
    assert Action.LEASE_EXPIRED in reaper_module._RECORDED_ACTIONS
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt, row.arq_job_id) == (S.RUNNING, 1, "stalled")


async def test_active_mode_recovers_and_fences_stale_holder(
    pg_task_db: TaskDB,
) -> None:
    task = await pg_task_db.add(status=S.PENDING)
    started, resume = asyncio.Event(), asyncio.Event()

    @lifecycle_module.task_job
    async def stalled(_ctx: dict[str, Any]) -> None:
        async with pg_task_db.factory() as session:
            loaded = await session.get(task_models.Task, task.id)
            assert loaded is not None
            assert await lifecycle_module.claim(session, loaded, "stalled")
            started.set()
            await resume.wait()
            await lifecycle_module.complete_task(session, loaded, {"stale": True})
            await session.commit()

    run = asyncio.create_task(stalled({}))
    await started.wait()
    past = await _db_now(pg_task_db) - reaper_module.LEASE_GRACE * 4
    await pg_task_db.update(task.id, lease_expires_at=past)

    decisions = await _reap(pg_task_db, Mode.ACTIVE)
    resume.set()
    await run

    assert [d.action for d in decisions] == [Action.LEASE_RECOVERED]
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt, row.lease_expires_at) == (S.PENDING, 2, None)
    assert row.result == {}
    assert row.arq_job_id is not None and row.arq_job_id != "stalled"
    assert decisions[0].job_id == row.arq_job_id


async def test_active_recovery_applies_watermark_resume(pg_task_db: TaskDB) -> None:
    user = user_models.User(id=uuid.uuid4(), display_name="u")
    connection = user_models.ServiceConnection(
        id=uuid.uuid4(),
        user_id=user.id,
        service_type=types_module.ServiceType.LISTENBRAINZ,
        external_user_id="lb",
        sync_watermark={"listens": {"oldest_synced_at": 1700500000}},
    )
    async with pg_task_db.factory() as session:
        session.add_all([user, connection])
        await session.commit()
    task = await _expired_running(
        pg_task_db,
        user_id=user.id,
        service_connection_id=connection.id,
        progress_current=900,
        params={"username": "lb", "min_ts": 1700000000},
    )

    await _reap(pg_task_db, Mode.ACTIVE)

    row = await pg_task_db.get(task.id)
    assert row.params == {
        "username": "lb",
        "min_ts": 1700000000,
        "max_ts": 1700500000,
        "items_so_far": 900,
    }


async def test_reaper_skips_row_reclaimed_after_scan(pg_task_db: TaskDB) -> None:
    task = await _expired_running(pg_task_db)
    real_recover = reaper_module._recover_expired_lease

    async def reclaimed_first(*args: Any, **kwargs: Any) -> reaper_module.Decision:
        # arq retried the job meanwhile: a fresh claim with a live lease.
        await pg_task_db.update(task.id, status=S.PENDING)
        await _claim(pg_task_db, task.id, "stalled")
        return await real_recover(*args, **kwargs)

    with patch.object(reaper_module, "_recover_expired_lease", reclaimed_first):
        decisions = await _reap(pg_task_db, Mode.ACTIVE)

    assert [d.action for d in decisions] == [Action.ROW_CHANGED]
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.RUNNING, 2)


async def test_reaper_and_startup_recovery_race_one_winner(
    pg_task_db: TaskDB,
) -> None:
    task = await _expired_running(pg_task_db)
    arq = AsyncMock()
    arq.default_queue_name = "arq:queue"
    arq.exists.return_value = 0

    with patch.object(
        reaper_module,
        "job_status",
        AsyncMock(return_value=arq_jobs.JobStatus.not_found),
    ):
        await asyncio.gather(
            reaper_module.reap(pg_task_db.factory, arq, lease_recovery=Mode.ACTIVE),
            worker_module._reenqueue_orphaned_tasks(pg_task_db.factory, arq),
        )

    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.PENDING, 2)
    assert arq.enqueue_job.await_count == 1


async def _db_wall_clock(db: TaskDB) -> datetime.datetime:
    async with db.factory() as session:
        return (
            await session.execute(sa.select(sa.func.clock_timestamp()))
        ).scalar_one()


async def _hold_row_lock(
    db: TaskDB, task_id: uuid.UUID, held: asyncio.Event, release: asyncio.Event
) -> None:
    """A handler's own transaction holding its row lock (flushed, not committed)."""
    async with db.factory() as session:
        await session.execute(
            sa.select(task_models.Task.id)
            .where(task_models.Task.id == task_id)
            .with_for_update(key_share=True)
        )
        held.set()
        await release.wait()
        await session.commit()


async def test_renewal_that_waited_on_the_lock_writes_a_future_lease(
    pg_task_db: TaskDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The lease runs from when the renewal lands, not from when its
    transaction started waiting on the handler's lock."""
    ttl = datetime.timedelta(seconds=5)
    monkeypatch.setattr(lifecycle_module, "LEASE_TTL", ttl)
    monkeypatch.setattr(
        lifecycle_module, "LEASE_RENEW_LOCK_TIMEOUT", datetime.timedelta(seconds=10)
    )
    task = await pg_task_db.add(status=S.PENDING)
    attempt = await _claim(pg_task_db, task.id, "a")
    held, release = asyncio.Event(), asyncio.Event()
    holder = asyncio.create_task(_hold_row_lock(pg_task_db, task.id, held, release))
    await held.wait()

    renewal = asyncio.create_task(
        lifecycle_module.renew_lease(pg_task_db.factory, task.id, attempt)
    )
    await asyncio.sleep(2)
    released_at = await _db_wall_clock(pg_task_db)
    release.set()
    await holder
    assert await renewal

    row = await pg_task_db.get(task.id)
    assert row.lease_expires_at is not None
    # A lease from the transaction's start would be about 2s short of this.
    assert row.lease_expires_at >= released_at + ttl - datetime.timedelta(seconds=0.5)


async def test_blocked_renewal_times_out_and_renewer_keeps_going(
    pg_task_db: TaskDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        lifecycle_module, "LEASE_RENEW_LOCK_TIMEOUT", datetime.timedelta(seconds=0.3)
    )
    monkeypatch.setattr(
        lifecycle_module, "LEASE_RENEW_INTERVAL", datetime.timedelta(seconds=0.1)
    )
    task = await pg_task_db.add(status=S.PENDING)
    attempt = await _claim(pg_task_db, task.id, "a")
    held, release = asyncio.Event(), asyncio.Event()
    holder = asyncio.create_task(_hold_row_lock(pg_task_db, task.id, held, release))
    await held.wait()

    loop = asyncio.get_running_loop()
    started = loop.time()
    with pytest.raises(lifecycle_module.LeaseRenewBlocked):
        await lifecycle_module.renew_lease(pg_task_db.factory, task.id, attempt)
    assert loop.time() - started < 2

    renewer = asyncio.create_task(
        lifecycle_module._renew_leases_forever(pg_task_db.factory, {task.id: attempt})
    )
    await asyncio.sleep(0.8)
    assert not renewer.done()
    release.set()
    await holder
    stale = await _db_wall_clock(pg_task_db) - datetime.timedelta(minutes=5)
    await pg_task_db.update(task.id, lease_expires_at=stale)
    await asyncio.sleep(0.5)
    renewer.cancel()

    row = await pg_task_db.get(task.id)
    assert row.lease_expires_at is not None and row.lease_expires_at > stale


async def test_db_clock_offsets_on_one_statement_shape_stay_distinct(
    pg_task_db: TaskDB,
) -> None:
    """The offset is a parameter, so the compiled cache can't mix up +120s
    and -30s on the same statement shape."""

    async def offset_seconds(offset: datetime.timedelta) -> float:
        async with pg_task_db.factory() as session:
            value = (
                await session.execute(
                    sa.select(
                        task_models.db_clock_offset(offset) - sa.func.clock_timestamp()
                    )
                )
            ).scalar_one()
            return float(value.total_seconds())

    assert abs(await offset_seconds(datetime.timedelta(seconds=120)) - 120) < 5
    assert abs(await offset_seconds(datetime.timedelta(seconds=-30)) + 30) < 5


async def test_log_mode_records_each_expired_attempt_once(
    pg_task_db: TaskDB, arq_redis: Any
) -> None:
    task = await _expired_running(pg_task_db)
    seen_key = f"{reaper_module.LEASE_EXPIRED_SEEN_PREFIX}{task.id}:1"
    await arq_redis.delete(reaper_module.DECISIONS_KEY, seen_key)
    try:
        with patch.object(
            reaper_module,
            "job_status",
            AsyncMock(return_value=arq_jobs.JobStatus.in_progress),
        ):
            for _ in range(3):
                decisions = await reaper_module.reap(
                    pg_task_db.factory, arq_redis, lease_recovery=Mode.LOG
                )
                assert [d.action for d in decisions] == [Action.LEASE_EXPIRED]

        state = await reaper_module.read_state(arq_redis)
        assert [d["action"] for d in state.decisions] == ["lease_expired"]
        assert state.last_tick is not None
        assert state.last_tick["lease_expired"] == 1
    finally:
        await arq_redis.delete(
            reaper_module.DECISIONS_KEY, reaper_module.LAST_TICK_KEY, seen_key
        )


@pytest.mark.parametrize(
    ("arq_status", "action"),
    [
        (arq_jobs.JobStatus.queued, Action.ARQ_HOLDS_JOB),
        (arq_jobs.JobStatus.deferred, Action.ARQ_HOLDS_JOB),
        (arq_jobs.JobStatus.in_progress, Action.LEASE_RECOVERED),
        (arq_jobs.JobStatus.not_found, Action.LEASE_RECOVERED),
    ],
)
async def test_active_recovery_leaves_jobs_arq_will_run(
    pg_task_db: TaskDB, arq_status: arq_jobs.JobStatus, action: Action
) -> None:
    """A queued/deferred job (arq's SIGTERM retry) reclaims the row itself; an
    in_progress one with an expired lease is a stalled worker."""
    task = await _expired_running(pg_task_db)
    arq = AsyncMock()
    arq.default_queue_name = "arq:queue"
    arq.exists.return_value = 0
    with patch.object(reaper_module, "job_status", AsyncMock(return_value=arq_status)):
        decisions = await reaper_module.reap(
            pg_task_db.factory, arq, lease_recovery=Mode.ACTIVE
        )

    assert [d.action for d in decisions] == [action]
    row = await pg_task_db.get(task.id)
    expected = S.RUNNING if action == Action.ARQ_HOLDS_JOB else S.PENDING
    assert row.status == expected


async def test_cancel_during_renewer_teardown_reaches_the_job(
    pg_task_db: TaskDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only the renewer's own cancellation is absorbed, not one aimed at the job."""
    tearing_down = asyncio.Event()

    async def slow_to_stop(*_: Any) -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            tearing_down.set()
            await asyncio.sleep(1)
            raise

    monkeypatch.setattr(lifecycle_module, "_renew_leases_forever", slow_to_stop)
    release = AsyncMock()
    monkeypatch.setattr(lifecycle_module, "release_leases", release)

    @lifecycle_module.task_job
    async def job(_ctx: dict[str, Any]) -> None:
        await asyncio.sleep(0)

    run = asyncio.create_task(job({"session_factory": pg_task_db.factory}))
    await tearing_down.wait()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    release.assert_not_called()


async def test_live_holder_blocking_its_own_renewal_is_not_recovered(
    pg_task_db: TaskDB, arq_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The live failure: a handler holds its row lock past TTL + grace, so the
    DB lease expires; it commits, and the next reaper tick must not recover it."""
    monkeypatch.setattr(lifecycle_module, "LEASE_TTL", datetime.timedelta(seconds=2))
    monkeypatch.setattr(
        lifecycle_module, "LEASE_RENEW_INTERVAL", datetime.timedelta(seconds=0.3)
    )
    monkeypatch.setattr(
        lifecycle_module, "LEASE_RENEW_LOCK_TIMEOUT", datetime.timedelta(seconds=0.2)
    )
    monkeypatch.setattr(reaper_module, "LEASE_GRACE", datetime.timedelta(seconds=1))
    task = await pg_task_db.add(status=S.PENDING)
    committed = asyncio.Event()
    finish = asyncio.Event()

    @lifecycle_module.task_job
    async def job(_ctx: dict[str, Any]) -> None:
        async with pg_task_db.factory() as session:
            loaded = await session.get(task_models.Task, task.id)
            assert loaded is not None
            assert await lifecycle_module.claim(session, loaded, "live")
            loaded.progress_current = 1
            await session.flush()
            await asyncio.sleep(4)
            await session.commit()
            committed.set()
            await finish.wait()
            await lifecycle_module.complete_task(session, loaded, {"ok": True})
            await session.commit()

    run = asyncio.create_task(
        job({"session_factory": pg_task_db.factory, "redis": arq_redis})
    )
    await committed.wait()
    assert (await pg_task_db.get(task.id)).lease_expires_at < await _db_wall_clock(
        pg_task_db
    ) - reaper_module.LEASE_GRACE

    with patch.object(
        reaper_module,
        "job_status",
        AsyncMock(return_value=arq_jobs.JobStatus.in_progress),
    ):
        decisions = await reaper_module.reap(
            pg_task_db.factory, arq_redis, lease_recovery=Mode.ACTIVE
        )
    finish.set()
    await run

    assert Action.LEASE_RECOVERED not in [d.action for d in decisions]
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.COMPLETED, 1)
    assert not await arq_redis.exists(lifecycle_module.alive_key(task.id, 1))


async def test_expired_lease_without_liveness_key_is_recovered(
    pg_task_db: TaskDB, arq_redis: Any
) -> None:
    task = await _expired_running(pg_task_db)
    await arq_redis.delete(lifecycle_module.alive_key(task.id, 1))
    with patch.object(
        reaper_module,
        "job_status",
        AsyncMock(return_value=arq_jobs.JobStatus.in_progress),
    ):
        decisions = await reaper_module.reap(
            pg_task_db.factory, arq_redis, lease_recovery=Mode.ACTIVE
        )

    assert [d.action for d in decisions] == [Action.LEASE_RECOVERED]
    assert (await pg_task_db.get(task.id)).status == S.PENDING


async def test_expired_lease_with_liveness_key_is_left_alone(
    pg_task_db: TaskDB, arq_redis: Any
) -> None:
    task = await _expired_running(pg_task_db)
    key = lifecycle_module.alive_key(task.id, 1)
    await arq_redis.psetex(key, 60_000, b"1")
    try:
        decisions = await _reap_with(pg_task_db, arq_redis)
    finally:
        await arq_redis.delete(key)

    assert [d.action for d in decisions] == [Action.HOLDER_ALIVE]
    assert (await pg_task_db.get(task.id)).status == S.RUNNING


async def _reap_with(db: TaskDB, arq: Any) -> list[reaper_module.Decision]:
    with patch.object(
        reaper_module,
        "job_status",
        AsyncMock(return_value=arq_jobs.JobStatus.in_progress),
    ):
        return await reaper_module.reap(db.factory, arq, lease_recovery=Mode.ACTIVE)


async def test_liveness_check_error_never_recovers(pg_task_db: TaskDB) -> None:
    task = await _expired_running(pg_task_db)
    arq = AsyncMock()
    arq.default_queue_name = "arq:queue"
    arq.exists.side_effect = ConnectionError("redis down")

    decisions = await _reap_with(pg_task_db, arq)

    assert [d.action for d in decisions] == [Action.LIVENESS_ERROR]
    assert (await pg_task_db.get(task.id)).status == S.RUNNING


async def test_locked_expired_row_is_not_recovered_this_tick(
    pg_task_db: TaskDB,
) -> None:
    """The lock holder isn't a worker connection, so it is left alone."""
    task = await _expired_running(pg_task_db)
    arq = AsyncMock()
    arq.default_queue_name = "arq:queue"
    arq.exists.return_value = 0
    held, release = asyncio.Event(), asyncio.Event()
    holder = asyncio.create_task(_hold_row_lock(pg_task_db, task.id, held, release))
    await held.wait()
    try:
        decisions = await asyncio.wait_for(_reap_with(pg_task_db, arq), timeout=5)
    finally:
        release.set()
        await holder

    assert [d.action for d in decisions] == [Action.HOLDER_LOCKED]
    assert decisions[0].detail is not None
    assert decisions[0].detail["reason"] == "not_worker"
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.RUNNING, 1)


async def test_job_commit_renews_the_lease_promptly(
    pg_task_db: TaskDB, arq_redis: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A commit wakes the renewer, so the lease catches up without waiting a
    full interval (and its own renewals don't keep waking it)."""
    monkeypatch.setattr(
        lifecycle_module, "LEASE_RENEW_INTERVAL", datetime.timedelta(seconds=60)
    )
    task = await pg_task_db.add(status=S.PENDING)
    renew = AsyncMock(wraps=lifecycle_module.renew_lease)
    monkeypatch.setattr(lifecycle_module, "renew_lease", renew)

    @lifecycle_module.task_job
    async def job(_ctx: dict[str, Any]) -> None:
        async with pg_task_db.factory() as session:
            loaded = await session.get(task_models.Task, task.id)
            assert loaded is not None
            assert await lifecycle_module.claim(session, loaded, "a")
            await asyncio.sleep(0.3)
            renew.reset_mock()
            loaded.progress_current = 3
            await session.commit()
            await asyncio.sleep(0.5)

    await job({"session_factory": pg_task_db.factory, "redis": arq_redis})

    assert renew.await_count == 1
    assert not await arq_redis.exists(lifecycle_module.alive_key(task.id, 1))
