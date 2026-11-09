"""Shared fixtures for task recovery tests."""

from __future__ import annotations

import asyncio
import datetime
import uuid
import weakref
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as orm

import resonance.models as models
import resonance.models.task as task_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator


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
    """Win the claim (and cancel's attempt bump) in memory on fake sessions.

    Mock and fake-session tests script each ``session.execute`` result for the job's
    own queries; these UPDATEs aren't part of that script. Recording the claim
    for the fence would be pointless there: a fake session never flushes, so
    the ``before_flush`` fence can't run. Real sessions (SQLite ``task_db``,
    Postgres) use the real code, and the fence is tested on Postgres.
    """
    real_claim = lifecycle_module.claim

    async def claim(
        session: Any, task: task_models.Task, job_id: str | None
    ) -> lifecycle_module.Claim | None:
        if isinstance(session, sa_async.AsyncSession):
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

    # Cancel's attempt bump is the same kind of unscripted UPDATE.
    real_fence_out = lifecycle_module._fence_out_holders

    async def fence_out(session: Any, tasks: list[task_models.Task]) -> None:
        if isinstance(session, sa_async.AsyncSession):
            await real_fence_out(session, tasks)
            return
        for task in tasks:
            task.attempt = (task.attempt or 0) + 1

    monkeypatch.setattr(lifecycle_module, "_fence_out_holders", fence_out)


class _WritesAcrossHttp:
    """State for :func:`no_writes_across_http`."""

    def __init__(self) -> None:
        self.dirty: weakref.WeakKeyDictionary[orm.Session, asyncio.Task[Any] | None] = (
            weakref.WeakKeyDictionary()
        )
        self.violations: list[str] = []

    def mark(self, session: orm.Session) -> None:
        try:
            owner: asyncio.Task[Any] | None = asyncio.current_task()
        except RuntimeError:
            owner = None
        self.dirty.setdefault(session, owner)

    def clear(self, session: orm.Session) -> None:
        self.dirty.pop(session, None)

    def check(self, request: httpx.Request) -> None:
        current = asyncio.current_task()
        if any(owner is current for owner in self.dirty.values()):
            self.violations.append(f"{request.method} {request.url}")


@pytest.fixture
def no_writes_across_http(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[_WritesAcrossHttp]:
    """Fail the test if an HTTP request is sent while its task has uncommitted writes.

    A session counts as written once it flushes, or once it executes a core
    INSERT/UPDATE/DELETE through ``session.execute`` (which never flushes), and
    stops counting at commit or rollback. Every outbound call (connectors, the
    artist-tags client, ``fetch_feed``) goes through ``httpx.AsyncClient.send``,
    which is patched to record a violation when a written session belongs to the
    sending asyncio task (other tasks, like the lease renewer, are ignored).

    Opt-in, not global: paged syncs (ListenBrainz, Last.fm, Spotify) flush a
    page's rows and fetch the next page before their per-page commit, which
    this would flag. Tests must route stub connectors through a real
    ``httpx.AsyncClient`` (e.g. ``httpx.MockTransport``) for the check to see them.
    """
    state = _WritesAcrossHttp()

    def on_flush(session: orm.Session, _ctx: Any) -> None:
        state.mark(session)

    def on_execute(orm_state: orm.ORMExecuteState) -> None:
        if orm_state.is_insert or orm_state.is_update or orm_state.is_delete:
            state.mark(orm_state.session)

    def on_end(session: orm.Session, *_: Any) -> None:
        state.clear(session)

    listeners: list[tuple[str, Any]] = [
        ("after_flush", on_flush),
        ("do_orm_execute", on_execute),
        ("after_commit", on_end),
        ("after_rollback", on_end),
    ]
    for name, fn in listeners:
        sa.event.listen(orm.Session, name, fn)

    real_send = httpx.AsyncClient.send

    async def send(
        self: httpx.AsyncClient, request: httpx.Request, **kwargs: Any
    ) -> httpx.Response:
        state.check(request)
        return await real_send(self, request, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "send", send)
    try:
        yield state
    finally:
        for name, fn in listeners:
            sa.event.remove(orm.Session, name, fn)
    assert not state.violations, (
        f"HTTP sent with uncommitted writes: {state.violations}"
    )
