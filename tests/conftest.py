"""Shared fixtures for task recovery tests."""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.models as models
import resonance.models.task as task_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


class TaskDB:
    """An in-memory SQLite task table with helpers to seed and read rows."""

    def __init__(
        self, factory: sa_async.async_sessionmaker[sa_async.AsyncSession]
    ) -> None:
        self.factory = factory

    async def add(self, **fields: Any) -> task_models.Task:
        fields.setdefault("id", uuid.uuid4())
        fields.setdefault("task_type", types_module.TaskType.TIME_RANGE)
        fields.setdefault("status", types_module.SyncStatus.PENDING)
        task = task_models.Task(**fields)
        async with self.factory() as session:
            session.add(task)
            await session.commit()
        return task

    async def get(self, task_id: uuid.UUID) -> task_models.Task:
        async with self.factory() as session:
            task = (
                await session.execute(
                    sa.select(task_models.Task).where(task_models.Task.id == task_id)
                )
            ).scalar_one()
            return task

    async def update(self, task_id: uuid.UUID, **values: Any) -> None:
        async with self.factory() as session:
            await session.execute(
                sa.update(task_models.Task)
                .where(task_models.Task.id == task_id)
                .values(**values)
            )
            await session.commit()


def minutes_ago(minutes: float) -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=minutes)


@pytest.fixture
async def task_db() -> AsyncIterator[TaskDB]:
    engine = sa_async.create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    try:
        yield TaskDB(sa_async.async_sessionmaker(engine, expire_on_commit=False))
    finally:
        await engine.dispose()


@pytest.fixture(autouse=True)
def _simulated_claim_on_mock_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Win the claim in memory when a job runs against a mocked session.

    Mocked-session tests script each ``session.execute`` result for the job's
    own queries; the claim's UPDATE isn't part of that script. Real sessions
    (SQLite ``task_db``, Postgres) still go through the real claim.
    """
    real_claim = lifecycle_module.claim

    async def claim(
        session: Any, task: task_models.Task, job_id: str | None
    ) -> lifecycle_module.Claim | None:
        if not isinstance(session, mock.Mock):
            return await real_claim(session, task, job_id)
        if not lifecycle_module._claimable(task, job_id):
            return None
        previous = task.status
        task.status = types_module.SyncStatus.RUNNING
        task.attempt = (task.attempt or 0) + 1
        task.started_at = datetime.datetime.now(datetime.UTC)
        await session.commit()
        return lifecycle_module.Claim(attempt=task.attempt, previous_status=previous)

    monkeypatch.setattr(lifecycle_module, "claim", claim)
