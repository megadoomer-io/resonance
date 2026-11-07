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
