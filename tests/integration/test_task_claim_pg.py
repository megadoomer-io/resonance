"""Attempt claims and fenced writes against real Postgres."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import arq.jobs as arq_jobs
import pytest

import resonance.models.task as task_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.sync.reaper as reaper_module
import resonance.types as types_module
import resonance.worker as worker_module
from tests.conftest import TaskDB, minutes_ago

if TYPE_CHECKING:
    import uuid
    from collections.abc import Awaitable, Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    Finish = Callable[[AsyncSession, task_models.Task], Awaitable[None]]

pytestmark = pytest.mark.integration

S = types_module.SyncStatus


async def _claim_in_new_session(
    db: TaskDB, task_id: uuid.UUID, job_id: str | None
) -> lifecycle_module.Claim | None:
    async with db.factory() as session:
        task = await session.get(task_models.Task, task_id)
        assert task is not None
        return await lifecycle_module.claim(session, task, job_id)


@pytest.mark.parametrize("status", [S.PENDING, S.DEFERRED])
async def test_claim_wins_and_bumps_attempt(pg_task_db: TaskDB, status: S) -> None:
    task = await pg_task_db.add(status=status, arq_job_id="old")

    claim = await _claim_in_new_session(pg_task_db, task.id, "job-1")

    assert claim == lifecycle_module.Claim(attempt=1, previous_status=status)
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt, row.arq_job_id) == (S.RUNNING, 1, "job-1")
    assert row.started_at is not None


async def test_claim_loses_on_terminal_and_foreign_running(
    pg_task_db: TaskDB,
) -> None:
    done = await pg_task_db.add(status=S.COMPLETED)
    running = await pg_task_db.add(status=S.RUNNING, attempt=3, arq_job_id="owner")

    assert await _claim_in_new_session(pg_task_db, done.id, "x") is None
    assert await _claim_in_new_session(pg_task_db, running.id, "dup") is None

    row = await pg_task_db.get(running.id)
    assert (row.status, row.attempt, row.arq_job_id) == (S.RUNNING, 3, "owner")


async def test_arq_retry_of_owning_job_reclaims_running(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.RUNNING, attempt=2, arq_job_id="owner")

    claim = await _claim_in_new_session(pg_task_db, task.id, "owner")

    assert claim == lifecycle_module.Claim(attempt=3, previous_status=S.RUNNING)


async def test_concurrent_claims_have_one_winner(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)

    results = await asyncio.gather(
        *(_claim_in_new_session(pg_task_db, task.id, f"job-{i}") for i in range(8))
    )

    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    row = await pg_task_db.get(task.id)
    assert row.attempt == 1
    assert row.arq_job_id == f"job-{results.index(winners[0])}"


async def test_claim_rereads_a_row_that_moved(pg_task_db: TaskDB) -> None:
    """A row that changed between load and claim (still claimable) is re-read."""
    task = await pg_task_db.add(status=S.DEFERRED)
    async with pg_task_db.factory() as session:
        loaded = await session.get(task_models.Task, task.id)
        assert loaded is not None
        await session.commit()
        await pg_task_db.update(task.id, status=S.PENDING, attempt=4)

        claim = await lifecycle_module.claim(session, loaded, "job")

    assert claim == lifecycle_module.Claim(attempt=5, previous_status=S.PENDING)


async def _run_claimed(
    db: TaskDB,
    task_id: uuid.UUID,
    job_id: str,
    started: asyncio.Event,
    resume: asyncio.Event,
    finish: Finish,
) -> None:
    """A job body: claim, stall while another delivery acts, then write."""

    @lifecycle_module.task_job
    async def job() -> None:
        async with db.factory() as session:
            task = await session.get(task_models.Task, task_id)
            assert task is not None
            assert await lifecycle_module.claim(session, task, job_id) is not None
            started.set()
            await resume.wait()
            try:
                await finish(session, task)
                await session.commit()
            except Exception:
                await lifecycle_module.fail_task(session, task, "handler error")
                await session.commit()

    await job()


async def _fenced_after_reclaim(db: TaskDB, finish: Finish) -> task_models.Task:
    task = await db.add(status=S.PENDING)
    started, resume = asyncio.Event(), asyncio.Event()
    old = asyncio.create_task(_run_claimed(db, task.id, "a", started, resume, finish))
    await started.wait()
    # Released and claimed by a newer delivery while "a" is stalled.
    await db.update(task.id, status=S.PENDING)
    assert await _claim_in_new_session(db, task.id, "b") is not None
    resume.set()
    await old
    return await db.get(task.id)


async def test_complete_from_fenced_attempt_is_dropped(pg_task_db: TaskDB) -> None:
    async def finish(session: AsyncSession, task: task_models.Task) -> None:
        await lifecycle_module.complete_task(session, task, {"stale": True})

    with patch.object(lifecycle_module, "logger") as log:
        row = await _fenced_after_reclaim(pg_task_db, finish)

    assert (row.status, row.attempt, row.arq_job_id) == (S.RUNNING, 2, "b")
    assert row.result == {}
    log.warning.assert_called_once()
    assert log.warning.call_args.args[0] == "task_write_fenced"


async def test_fail_from_fenced_attempt_is_dropped(pg_task_db: TaskDB) -> None:
    async def finish(session: AsyncSession, task: task_models.Task) -> None:
        raise RuntimeError("boom")

    row = await _fenced_after_reclaim(pg_task_db, finish)

    # The handler's except-Exception fail_task is fenced too: never FAILED.
    assert (row.status, row.error_message) == (S.RUNNING, None)


async def test_progress_from_fenced_attempt_is_dropped(pg_task_db: TaskDB) -> None:
    async def finish(session: AsyncSession, task: task_models.Task) -> None:
        task.progress_current = 99
        task.result = {"phase": "stale"}

    row = await _fenced_after_reclaim(pg_task_db, finish)

    assert (row.progress_current, row.result) == (0, {})


async def test_unfenced_writes_still_apply(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)

    @lifecycle_module.task_job
    async def job() -> None:
        async with pg_task_db.factory() as session:
            loaded = await session.get(task_models.Task, task.id)
            assert loaded is not None
            await lifecycle_module.claim(session, loaded, "a")
            loaded.progress_current = 5
            await session.commit()
            await lifecycle_module.complete_task(session, loaded, {"ok": True})
            await session.commit()

    await job()

    row = await pg_task_db.get(task.id)
    assert (row.status, row.progress_current) == (S.COMPLETED, 5)
    assert row.result == {"ok": True}


async def test_cancel_still_wins_over_the_claimer(pg_task_db: TaskDB) -> None:
    """Cancel doesn't bump attempt, so the claimer isn't fenced; the
    keep-CANCELLED listener still keeps its terminal write from landing."""
    task = await pg_task_db.add(status=S.PENDING)

    @lifecycle_module.task_job
    async def job() -> None:
        async with pg_task_db.factory() as session:
            loaded = await session.get(task_models.Task, task.id)
            assert loaded is not None
            await lifecycle_module.claim(session, loaded, "a")
            await pg_task_db.update(task.id, status=S.CANCELLED, error_message="user")
            await lifecycle_module.complete_task(session, loaded, {"late": True})
            await session.commit()

    await job()

    row = await pg_task_db.get(task.id)
    assert (row.status, row.error_message) == (S.CANCELLED, "user")


async def test_release_fences_the_old_attempt(pg_task_db: TaskDB) -> None:
    """Recovery bumps attempt as it resets the row, so the old holder is fenced
    even before the new delivery claims."""
    task = await pg_task_db.add(status=S.PENDING)
    started, resume = asyncio.Event(), asyncio.Event()

    async def finish(session: AsyncSession, loaded: task_models.Task) -> None:
        await lifecycle_module.complete_task(session, loaded, {"stale": True})

    old = asyncio.create_task(
        _run_claimed(pg_task_db, task.id, "a", started, resume, finish)
    )
    await started.wait()
    async with pg_task_db.factory() as session:
        loaded = await session.get(task_models.Task, task.id)
        assert loaded is not None
        assert await lifecycle_module.release_for_redelivery(session, loaded)
        await session.commit()
    resume.set()
    await old

    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.PENDING, 2)
    assert await _claim_in_new_session(pg_task_db, task.id, "b") is not None


async def test_release_loses_to_a_claim_in_between(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(status=S.PENDING)
    async with pg_task_db.factory() as session:
        loaded = await session.get(task_models.Task, task.id)
        assert loaded is not None
        await session.commit()
        assert await _claim_in_new_session(pg_task_db, task.id, "b") is not None

        assert not await lifecycle_module.release_for_redelivery(session, loaded)

    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.RUNNING, 1)


async def test_reaper_redelivery_bumps_attempt(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(
        status=S.DEFERRED,
        attempt=2,
        enqueued_at=minutes_ago(5),
        arq_job_id="lost",
    )
    with patch.object(
        reaper_module,
        "job_status",
        AsyncMock(return_value=arq_jobs.JobStatus.not_found),
    ):
        decisions = await reaper_module.reap(pg_task_db.factory, AsyncMock())

    assert [d.action for d in decisions] == [reaper_module.Action.REDELIVERED]
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.PENDING, 3)


def _ctx(db: TaskDB, job_id: str) -> dict[str, Any]:
    return {"session_factory": db.factory, "redis": AsyncMock(), "job_id": job_id}


async def test_duplicate_delivery_while_running_does_not_run(
    pg_task_db: TaskDB,
) -> None:
    """A second arq delivery of a live RUNNING task exits without touching it."""
    task = await pg_task_db.add(
        task_type=types_module.TaskType.BULK_JOB,
        status=S.RUNNING,
        attempt=1,
        arq_job_id="owner",
        params={"operation": "nope"},
    )

    await lifecycle_module.task_job(worker_module.run_bulk_job)(
        _ctx(pg_task_db, "dup"), str(task.id)
    )

    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt, row.error_message) == (S.RUNNING, 1, None)


async def test_job_entry_point_claims_then_writes(pg_task_db: TaskDB) -> None:
    task = await pg_task_db.add(
        task_type=types_module.TaskType.BULK_JOB,
        status=S.PENDING,
        params={"operation": "nope"},
    )

    await lifecycle_module.task_job(worker_module.run_bulk_job)(
        _ctx(pg_task_db, "job-1"), str(task.id)
    )

    row = await pg_task_db.get(task.id)
    # The unknown operation fails the task; the claimer's own write applies.
    assert (row.status, row.attempt, row.arq_job_id) == (S.FAILED, 1, "job-1")
