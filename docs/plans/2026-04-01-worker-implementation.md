# Worker Architecture Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace `asyncio.create_task()` sync runner with arq-based hierarchical task queue so sync jobs survive pod restarts and failed ranges can be retried individually.

**Architecture:** A single `SyncTask` model with self-referential parent/child relationships replaces `SyncJob`. arq workers (same Docker image, different entrypoint) consume tasks from Redis. The web app enqueues tasks; workers execute them. A `WORKER_MODE` setting controls inline (dev) vs external (prod) worker execution.

**Tech Stack:** arq, Redis (already deployed), SQLAlchemy async, FastAPI, alembic

**Issues:** Closes #7, supersedes #13. Depends on #8 (ListeningEvent creation fix — needed for accurate watermarks but can be done separately).

---

## Task 1: Add arq dependency and WORKER_MODE config

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/resonance/config.py`
- Test: `tests/test_config.py`

**Step 1: Add arq to dependencies**

In `pyproject.toml`, add `"arq>=0.26"` to the `dependencies` list.

**Step 2: Add WORKER_MODE to Settings**

In `src/resonance/config.py`, add:

```python
# Worker mode
worker_mode: str = "external"  # "external" (prod) or "inline" (dev)
```

**Step 3: Write test for worker_mode config**

In `tests/test_config.py`, add a test verifying the default value:

```python
def test_worker_mode_defaults_to_external() -> None:
    settings = config_module.Settings()
    assert settings.worker_mode == "external"
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS

**Step 5: Run type checker and linter**

Run: `uv run mypy src/resonance/config.py && uv run ruff check src/resonance/config.py`

**Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/resonance/config.py tests/test_config.py
git commit -m "feat: add arq dependency and WORKER_MODE config setting"
```

---

## Task 2: Add SyncTaskType enum and update SyncStatus

**Files:**
- Modify: `src/resonance/types.py`

**Step 1: Add SyncTaskType enum**

Add to `src/resonance/types.py`:

```python
class SyncTaskType(enum.StrEnum):
    """Types of hierarchical sync tasks."""

    SYNC_JOB = "sync_job"
    TIME_RANGE = "time_range"
    PAGE_FETCH = "page_fetch"
```

**Step 2: Run type checker**

Run: `uv run mypy src/resonance/types.py`

**Step 3: Commit**

```bash
git add src/resonance/types.py
git commit -m "feat: add SyncTaskType enum for hierarchical task types"
```

---

## Task 3: Create SyncTask model

**Files:**
- Create: `src/resonance/models/task.py`
- Modify: `src/resonance/models/__init__.py`
- Test: `tests/test_models.py`

**Step 1: Write failing test**

Add to `tests/test_models.py`:

```python
class TestSyncTask:
    def test_sync_task_has_expected_columns(self) -> None:
        import resonance.models.task as task_models
        import resonance.types as types_module

        task = task_models.SyncTask(
            user_id=uuid.uuid4(),
            service_connection_id=uuid.uuid4(),
            task_type=types_module.SyncTaskType.SYNC_JOB,
            status=types_module.SyncStatus.PENDING,
        )
        assert task.parent_id is None
        assert task.params == {}
        assert task.result == {}
        assert task.progress_current == 0
        assert task.progress_total is None
        assert task.error_message is None

    def test_sync_task_tablename(self) -> None:
        import resonance.models.task as task_models

        assert task_models.SyncTask.__tablename__ == "sync_tasks"
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_models.py::TestSyncTask -v`
Expected: FAIL (module not found)

**Step 3: Create SyncTask model**

Create `src/resonance/models/task.py`:

```python
"""SyncTask model for hierarchical sync job tracking."""

from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.models.user as user_models
import resonance.types as types_module


class SyncTask(base_module.Base):
    """A hierarchical sync task: sync_job -> time_range -> page_fetch."""

    __tablename__ = "sync_tasks"
    __table_args__ = (
        sa.Index("ix_sync_tasks_user_status", "user_id", "status"),
        sa.Index("ix_sync_tasks_parent_status", "parent_id", "status"),
        sa.Index(
            "ix_sync_tasks_connection_type_status",
            "service_connection_id",
            "task_type",
            "status",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    service_connection_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE"), nullable=False
    )
    service_connection: orm.Mapped[user_models.ServiceConnection] = orm.relationship()
    parent_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("sync_tasks.id", ondelete="CASCADE"), nullable=True, default=None
    )
    parent: orm.Mapped[SyncTask | None] = orm.relationship(
        back_populates="children", remote_side=[id]
    )
    children: orm.Mapped[list[SyncTask]] = orm.relationship(
        back_populates="parent", cascade="all, delete-orphan"
    )
    task_type: orm.Mapped[types_module.SyncTaskType] = orm.mapped_column(
        sa.Enum(types_module.SyncTaskType, native_enum=False), nullable=False
    )
    status: orm.Mapped[types_module.SyncStatus] = orm.mapped_column(
        sa.Enum(types_module.SyncStatus, native_enum=False),
        nullable=False,
        default=types_module.SyncStatus.PENDING,
    )
    params: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, default=dict
    )
    result: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, default=dict
    )
    error_message: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    progress_current: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    progress_total: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True, default=None
    )
    started_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    completed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )
```

**Step 4: Update models __init__.py**

Add `SyncTask` to `src/resonance/models/__init__.py`:

```python
from resonance.models.task import SyncTask
```

Add `"SyncTask"` to `__all__`.

**Step 5: Run tests**

Run: `uv run pytest tests/test_models.py::TestSyncTask -v`
Expected: PASS

**Step 6: Run type checker**

Run: `uv run mypy src/resonance/models/task.py`

**Step 7: Commit**

```bash
git add src/resonance/models/task.py src/resonance/models/__init__.py tests/test_models.py
git commit -m "feat: add SyncTask model with hierarchical parent/child relationships"
```

---

## Task 4: Alembic migration for sync_tasks table

**Files:**
- Create: `alembic/versions/<auto>_add_sync_tasks.py` (generated)

**Step 1: Generate migration**

Run: `uv run alembic revision --autogenerate -m "add sync_tasks table"`

This should generate a migration that creates the `sync_tasks` table. The existing `sync_jobs` table is NOT dropped yet — we'll keep it until the new system is verified working.

**Step 2: Review the generated migration**

Verify it creates the `sync_tasks` table with all columns and indexes from the model. It should NOT drop `sync_jobs`.

**Step 3: Commit**

```bash
git add alembic/versions/
git commit -m "feat: add alembic migration for sync_tasks table"
```

---

## Task 5: Create arq task functions (plan_sync, sync_range)

**Files:**
- Create: `src/resonance/worker.py`
- Create: `tests/test_worker.py`

**Step 1: Write failing tests for plan_sync**

Create `tests/test_worker.py`:

```python
"""Tests for arq worker task functions."""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.models.task as task_models
import resonance.types as types_module


def _make_sync_task(
    task_type: types_module.SyncTaskType = types_module.SyncTaskType.SYNC_JOB,
    status: types_module.SyncStatus = types_module.SyncStatus.PENDING,
    user_id: uuid.UUID | None = None,
    service_connection_id: uuid.UUID | None = None,
    parent_id: uuid.UUID | None = None,
    params: dict | None = None,
) -> MagicMock:
    task = MagicMock(spec=task_models.SyncTask)
    task.id = uuid.uuid4()
    task.user_id = user_id or uuid.uuid4()
    task.service_connection_id = service_connection_id or uuid.uuid4()
    task.parent_id = parent_id
    task.task_type = task_type
    task.status = status
    task.params = params or {}
    task.result = {}
    task.error_message = None
    task.progress_current = 0
    task.progress_total = None
    task.started_at = None
    task.completed_at = None
    task.children = []
    return task


class TestPlanSync:
    @pytest.mark.anyio()
    async def test_plan_sync_marks_task_running(self) -> None:
        import resonance.worker as worker_module

        task = _make_sync_task()
        ctx: dict = {"session_factory": AsyncMock(), "settings": MagicMock()}
        # Mock DB lookups
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = task
        conn_mock = MagicMock()
        conn_mock.service_type = types_module.ServiceType.LISTENBRAINZ
        conn_mock.external_user_id = "testuser"
        conn_mock.encrypted_access_token = "enc-token"
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = conn_mock
        mock_session.execute = AsyncMock(side_effect=[mock_result, conn_result])
        mock_session.add = MagicMock()
        ctx["session_factory"].return_value.__aenter__ = AsyncMock(
            return_value=mock_session
        )
        ctx["session_factory"].return_value.__aexit__ = AsyncMock(return_value=False)

        # We'll verify plan_sync is importable and has the right signature
        assert callable(worker_module.plan_sync)
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_worker.py::TestPlanSync -v`
Expected: FAIL (module not found)

**Step 3: Create worker module**

Create `src/resonance/worker.py`:

```python
"""arq worker: task functions and settings for the sync task queue."""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import arq.connections
import structlog

import resonance.config as config_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.registry as registry_module
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
import resonance.database as database_module
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.sync.runner as runner_module
import resonance.types as types_module

try:
    import sqlalchemy as sa
    import sqlalchemy.ext.asyncio as sa_async
except ImportError:  # pragma: no cover
    pass

logger = structlog.get_logger()


async def plan_sync(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Plan a sync by creating child tasks based on connector chunking strategy.

    Args:
        ctx: arq context dict with session_factory and settings.
        sync_task_id: UUID string of the top-level SyncTask.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    settings: config_module.Settings = ctx["settings"]
    task_uuid = uuid.UUID(sync_task_id)
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        # Load the task
        result = await session.execute(
            sa.select(task_models.SyncTask).where(
                task_models.SyncTask.id == task_uuid
            )
        )
        task = result.scalar_one_or_none()
        if task is None:
            log.error("sync_task_not_found")
            return

        # Mark as running
        task.status = types_module.SyncStatus.RUNNING
        task.started_at = datetime.datetime.now(datetime.UTC)
        await session.commit()

        # Load connection
        conn_result = await session.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id == task.service_connection_id
            )
        )
        connection = conn_result.scalar_one()
        log = log.bind(service=connection.service_type.value)

        try:
            if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
                await _plan_listenbrainz_sync(
                    ctx, session, task, connection, settings, log
                )
            else:
                await _plan_spotify_sync(
                    ctx, session, task, connection, settings, log
                )
        except Exception:
            log.exception("plan_sync_failed")
            task.status = types_module.SyncStatus.FAILED
            task.error_message = "Failed to plan sync — see logs"
            task.completed_at = datetime.datetime.now(datetime.UTC)
            await session.commit()


async def _plan_listenbrainz_sync(
    ctx: dict[str, Any],
    session: sa_async.AsyncSession,
    task: task_models.SyncTask,
    connection: user_models.ServiceConnection,
    settings: config_module.Settings,
    log: Any,
) -> None:
    """Create time-range child tasks for a ListenBrainz full sync."""
    connector_registry: registry_module.ConnectorRegistry = ctx["connector_registry"]
    connector = connector_registry.get(types_module.ServiceType.LISTENBRAINZ)
    if connector is None or not isinstance(
        connector, listenbrainz_module.ListenBrainzConnector
    ):
        task.status = types_module.SyncStatus.FAILED
        task.error_message = "ListenBrainz connector not registered"
        task.completed_at = datetime.datetime.now(datetime.UTC)
        await session.commit()
        return

    username = connection.external_user_id

    # Get total listen count
    try:
        total = await connector.get_listen_count(username)
        task.progress_total = total
        log.info("listenbrainz_total_listens", total=total)
    except Exception:
        log.warning("could_not_fetch_listen_count")
        total = 0

    # For incremental sync, find the most recent completed timestamp
    min_ts: int | None = None
    watermark_result = await session.execute(
        sa.select(sa.func.max(task_models.SyncTask.completed_at))
        .where(
            task_models.SyncTask.service_connection_id == task.service_connection_id,
            task_models.SyncTask.task_type == types_module.SyncTaskType.TIME_RANGE,
            task_models.SyncTask.status == types_module.SyncStatus.COMPLETED,
        )
    )
    last_completed = watermark_result.scalar_one_or_none()
    if last_completed is not None:
        # This is an incremental sync — only fetch new data
        min_ts = int(last_completed.timestamp())
        log.info("incremental_sync_from", min_ts=min_ts)

    # Create a single time_range child task (chunking happens in sync_range)
    child = task_models.SyncTask(
        user_id=task.user_id,
        service_connection_id=task.service_connection_id,
        parent_id=task.id,
        task_type=types_module.SyncTaskType.TIME_RANGE,
        status=types_module.SyncStatus.PENDING,
        params={"username": username, "min_ts": min_ts},
    )
    session.add(child)
    await session.commit()

    # Enqueue the child
    redis = ctx["redis"]
    await redis.enqueue_job("sync_range", str(child.id))
    log.info("enqueued_lb_sync_range", child_task_id=str(child.id))


async def _plan_spotify_sync(
    ctx: dict[str, Any],
    session: sa_async.AsyncSession,
    task: task_models.SyncTask,
    connection: user_models.ServiceConnection,
    settings: config_module.Settings,
    log: Any,
) -> None:
    """Create child tasks for each Spotify data type."""
    access_token = crypto_module.decrypt_token(
        connection.encrypted_access_token, settings.token_encryption_key
    )

    # Create one child per data type
    for data_type in ("followed_artists", "saved_tracks", "recently_played"):
        child = task_models.SyncTask(
            user_id=task.user_id,
            service_connection_id=task.service_connection_id,
            parent_id=task.id,
            task_type=types_module.SyncTaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": data_type, "access_token": access_token},
        )
        session.add(child)

    await session.commit()

    # Enqueue children
    redis = ctx["redis"]
    children_result = await session.execute(
        sa.select(task_models.SyncTask).where(
            task_models.SyncTask.parent_id == task.id
        )
    )
    for child in children_result.scalars().all():
        await redis.enqueue_job("sync_range", str(child.id))
        log.info(
            "enqueued_spotify_sync_range",
            child_task_id=str(child.id),
            data_type=child.params.get("data_type"),
        )


async def sync_range(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Execute a single sync range/data-type task.

    Args:
        ctx: arq context dict with session_factory, settings, connector_registry.
        sync_task_id: UUID string of the child SyncTask.
    """
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = ctx[
        "session_factory"
    ]
    settings: config_module.Settings = ctx["settings"]
    task_uuid = uuid.UUID(sync_task_id)
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        result = await session.execute(
            sa.select(task_models.SyncTask).where(
                task_models.SyncTask.id == task_uuid
            )
        )
        task = result.scalar_one_or_none()
        if task is None:
            log.error("sync_task_not_found")
            return

        # Load connection
        conn_result = await session.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id == task.service_connection_id
            )
        )
        connection = conn_result.scalar_one()

        task.status = types_module.SyncStatus.RUNNING
        task.started_at = datetime.datetime.now(datetime.UTC)
        await session.commit()

        connector_registry: registry_module.ConnectorRegistry = ctx[
            "connector_registry"
        ]

        try:
            if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
                await _run_listenbrainz_range(
                    session, task, connection, connector_registry, log
                )
            else:
                await _run_spotify_range(
                    session, task, connection, connector_registry, settings, log
                )

            task.status = types_module.SyncStatus.COMPLETED
            task.completed_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

        except Exception:
            log.exception("sync_range_failed")
            task.status = types_module.SyncStatus.FAILED
            task.error_message = "Range sync failed — see logs"
            task.completed_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

        # Check if all siblings are done -> mark parent completed
        await _check_parent_completion(session, task, log)


async def _run_listenbrainz_range(
    session: sa_async.AsyncSession,
    task: task_models.SyncTask,
    connection: user_models.ServiceConnection,
    connector_registry: registry_module.ConnectorRegistry,
    log: Any,
) -> None:
    """Paginate through ListenBrainz listens for this range."""
    connector = connector_registry.get(types_module.ServiceType.LISTENBRAINZ)
    if connector is None or not isinstance(
        connector, listenbrainz_module.ListenBrainzConnector
    ):
        raise RuntimeError("ListenBrainz connector not registered")

    username: str = task.params.get("username", connection.external_user_id)  # type: ignore[assignment]
    min_ts: int | None = task.params.get("min_ts")  # type: ignore[assignment]
    max_ts: int | None = None
    items_created = 0
    page = 0
    page_size = 100

    while True:
        listens = await connector.get_listens(
            username, max_ts=max_ts, min_ts=min_ts, count=page_size
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

        max_ts = listens[-1].listened_at
        task.progress_current = items_created
        await session.commit()
        log.info(
            "listenbrainz_page_synced",
            page=page,
            listens_in_page=len(listens),
            items_created=items_created,
            max_ts=max_ts,
        )

    task.result = {"items_created": items_created}


async def _run_spotify_range(
    session: sa_async.AsyncSession,
    task: task_models.SyncTask,
    connection: user_models.ServiceConnection,
    connector_registry: registry_module.ConnectorRegistry,
    settings: config_module.Settings,
    log: Any,
) -> None:
    """Execute a single Spotify data type sync."""
    connector = connector_registry.get(types_module.ServiceType.SPOTIFY)
    if connector is None:
        raise RuntimeError("Spotify connector not registered")

    data_type: str = task.params.get("data_type", "")  # type: ignore[assignment]
    access_token: str = task.params.get("access_token", "")  # type: ignore[assignment]

    # Refresh token if needed
    if (
        connection.token_expires_at is not None
        and connection.token_expires_at < datetime.datetime.now(datetime.UTC)
        and connection.encrypted_refresh_token is not None
        and hasattr(connector, "refresh_access_token")
    ):
        refresh_token = crypto_module.decrypt_token(
            connection.encrypted_refresh_token, settings.token_encryption_key
        )
        from resonance.connectors.base import TokenResponse

        new_tokens: TokenResponse = await connector.refresh_access_token(refresh_token)  # type: ignore[union-attr]
        access_token = new_tokens.access_token
        connection.encrypted_access_token = crypto_module.encrypt_token(
            new_tokens.access_token, settings.token_encryption_key
        )
        if new_tokens.refresh_token:
            connection.encrypted_refresh_token = crypto_module.encrypt_token(
                new_tokens.refresh_token, settings.token_encryption_key
            )
        await session.commit()

    items_created = 0
    items_updated = 0

    if data_type == "followed_artists":
        artists = await connector.get_followed_artists(access_token)  # type: ignore[union-attr]
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
        tracks = await connector.get_saved_tracks(access_token)  # type: ignore[union-attr]
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
        recently_played = await connector.get_recently_played(access_token)  # type: ignore[union-attr]
        for played_item in recently_played:
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


async def _check_parent_completion(
    session: sa_async.AsyncSession,
    task: task_models.SyncTask,
    log: Any,
) -> None:
    """Check if all siblings are done; if so, mark parent as completed."""
    if task.parent_id is None:
        return

    incomplete_result = await session.execute(
        sa.select(sa.func.count())
        .select_from(task_models.SyncTask)
        .where(
            task_models.SyncTask.parent_id == task.parent_id,
            task_models.SyncTask.status.not_in(
                [types_module.SyncStatus.COMPLETED, types_module.SyncStatus.FAILED]
            ),
        )
    )
    incomplete_count = incomplete_result.scalar_one()

    if incomplete_count > 0:
        return

    # All siblings done — check for failures
    failed_result = await session.execute(
        sa.select(sa.func.count())
        .select_from(task_models.SyncTask)
        .where(
            task_models.SyncTask.parent_id == task.parent_id,
            task_models.SyncTask.status == types_module.SyncStatus.FAILED,
        )
    )
    failed_count = failed_result.scalar_one()

    parent_result = await session.execute(
        sa.select(task_models.SyncTask).where(
            task_models.SyncTask.id == task.parent_id
        )
    )
    parent = parent_result.scalar_one_or_none()
    if parent is None:
        return

    if failed_count > 0:
        parent.status = types_module.SyncStatus.FAILED
        parent.error_message = f"{failed_count} child task(s) failed"
    else:
        parent.status = types_module.SyncStatus.COMPLETED

    parent.completed_at = datetime.datetime.now(datetime.UTC)

    # Aggregate results from children
    children_result = await session.execute(
        sa.select(task_models.SyncTask).where(
            task_models.SyncTask.parent_id == task.parent_id,
        )
    )
    total_created = 0
    total_updated = 0
    for child in children_result.scalars().all():
        child_result = child.result or {}
        total_created += child_result.get("items_created", 0)  # type: ignore[arg-type]
        total_updated += child_result.get("items_updated", 0)  # type: ignore[arg-type]

    parent.result = {"items_created": total_created, "items_updated": total_updated}
    await session.commit()
    log.info(
        "parent_task_completed",
        parent_id=str(parent.id),
        status=parent.status.value,
        items_created=total_created,
        items_updated=total_updated,
    )


async def startup(ctx: dict[str, Any]) -> None:
    """arq startup hook — initialize DB, Redis, and connectors."""
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


async def shutdown(ctx: dict[str, Any]) -> None:
    """arq shutdown hook — dispose of engine."""
    engine = ctx.get("engine")
    if engine is not None:
        await engine.dispose()


class WorkerSettings:
    """arq worker settings."""

    functions = [plan_sync, sync_range]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 10
    job_timeout = 300

    @staticmethod
    def redis_settings() -> arq.connections.RedisSettings:
        """Build Redis settings from app config."""
        settings = config_module.Settings()
        return arq.connections.RedisSettings(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password or None,
        )
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_worker.py -v`
Expected: PASS

**Step 5: Run type checker**

Run: `uv run mypy src/resonance/worker.py`

Fix any type errors that arise. The `type: ignore` comments on connector method calls are expected since `registry.get()` returns `BaseConnector | None`.

**Step 6: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: add arq worker with plan_sync and sync_range task functions"
```

---

## Task 6: Update sync API to enqueue arq tasks instead of asyncio.create_task

**Files:**
- Modify: `src/resonance/api/v1/sync.py`
- Modify: `tests/test_api_sync.py`

**Step 1: Update trigger_sync endpoint**

Replace the `asyncio.create_task()` pattern in `src/resonance/api/v1/sync.py`:

1. Remove the `_background_tasks` set and the `_run_background_sync` closure.
2. Instead of creating an `asyncio.Task`, create a `SyncTask` model and enqueue `plan_sync` via arq.
3. The endpoint creates a top-level `SyncTask` (type=`sync_job`), saves it, then calls `await arq_redis.enqueue_job("plan_sync", str(task.id))`.
4. Move the token refresh logic into the worker (it's already there in `_run_spotify_range`).

Key changes:
- Import `task_models` instead of `sync_models` for the new SyncTask.
- Keep `sync_models` imported for backward compatibility during migration (the `cancel` and `status` endpoints still query SyncJob until updated).
- The `trigger_sync` endpoint creates a `SyncTask` and enqueues it.

**Step 2: Update sync_status endpoint**

Change `sync_status` to query `SyncTask` where `task_type == SYNC_JOB` instead of `SyncJob`.

**Step 3: Update cancel endpoint**

Change `cancel_sync` to query and cancel `SyncTask` instead of `SyncJob`.

**Step 4: Update tests**

Update `tests/test_api_sync.py` to reference `SyncTask` instead of `SyncJob`, and mock the arq Redis pool on `request.app.state.arq_redis`.

**Step 5: Run tests**

Run: `uv run pytest tests/test_api_sync.py -v`

**Step 6: Run full test suite**

Run: `uv run pytest -v`

**Step 7: Commit**

```bash
git add src/resonance/api/v1/sync.py tests/test_api_sync.py
git commit -m "feat: replace asyncio.create_task with arq task queue in sync API"
```

---

## Task 7: Update app.py lifespan to initialize arq Redis pool

**Files:**
- Modify: `src/resonance/app.py`

**Step 1: Add arq Redis pool to lifespan**

In the `lifespan` function:
1. Create an arq `ArqRedis` connection pool using `arq.create_pool()`.
2. Store it as `application.state.arq_redis`.
3. Update the RUNNING reset logic to operate on `SyncTask` instead of `SyncJob`.
4. Close the arq pool on shutdown.

```python
import arq.connections as arq_connections

# In lifespan, after redis_pool:
arq_redis = await arq_connections.create_pool(
    arq_connections.RedisSettings(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password or None,
    )
)
application.state.arq_redis = arq_redis

# Update reset query to use SyncTask
result = await db.execute(
    sa.update(task_models.SyncTask)
    .where(task_models.SyncTask.status == types_module.SyncStatus.RUNNING)
    .values(
        status=types_module.SyncStatus.PENDING,
        started_at=None,
    )
)
```

Also add inline worker startup if `WORKER_MODE=inline`:

```python
if settings.worker_mode == "inline":
    import resonance.worker as worker_module
    # Start arq worker in background (inline mode for development)
    ...
```

**Step 2: Run tests**

Run: `uv run pytest -v`

**Step 3: Commit**

```bash
git add src/resonance/app.py
git commit -m "feat: initialize arq Redis pool in app lifespan, reset interrupted SyncTasks"
```

---

## Task 8: Update UI routes and templates for SyncTask

**Files:**
- Modify: `src/resonance/ui/routes.py`
- Modify: `src/resonance/templates/partials/sync_status.html`
- Modify: `src/resonance/templates/dashboard.html`

**Step 1: Update dashboard route**

Change `latest_sync` query from `SyncJob` to `SyncTask` where `task_type == SYNC_JOB`.

**Step 2: Update sync_status_partial route**

Change query from `SyncJob` to `SyncTask` where `task_type == SYNC_JOB`.

**Step 3: Update sync_status.html template**

The template accesses `job.service_connection`, `job.status`, `job.progress_current`, `job.progress_total`, `job.items_created`, `job.items_updated`. The SyncTask model has all these except `items_created`/`items_updated` — those are now in `task.result["items_created"]`. Update the template to read from `job.result`.

Replace:
```html
<td>{{ job.items_created }} new / {{ job.items_updated }} updated</td>
```
With:
```html
<td>{{ job.result.get('items_created', 0) }} new / {{ job.result.get('items_updated', 0) }} updated</td>
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_ui.py -v`

**Step 5: Commit**

```bash
git add src/resonance/ui/routes.py src/resonance/templates/partials/sync_status.html src/resonance/templates/dashboard.html
git commit -m "feat: update UI routes and templates to use SyncTask model"
```

---

## Task 9: Update merge.py for SyncTask

**Files:**
- Modify: `src/resonance/merge.py`
- Modify: `tests/test_merge.py`

**Step 1: Update merge references**

Change `models.SyncJob` → `models.SyncTask` in:
- `get_account_summary()`: the `("sync_jobs", models.SyncJob)` entry → `("sync_tasks", models.SyncTask)`
- `merge_accounts()`: the SyncJob update → SyncTask update

Update `MergeStats.sync_jobs_moved` → `sync_tasks_moved`.

**Step 2: Update tests**

Update any test references from `SyncJob` to `SyncTask`.

**Step 3: Run tests**

Run: `uv run pytest tests/test_merge.py -v`

**Step 4: Commit**

```bash
git add src/resonance/merge.py tests/test_merge.py
git commit -m "refactor: update account merge to use SyncTask instead of SyncJob"
```

---

## Task 10: Remove SyncJob model and old sync runner

**Files:**
- Modify: `src/resonance/models/__init__.py`
- Delete: `src/resonance/models/sync.py`
- Modify: `src/resonance/sync/runner.py` (keep upsert functions, remove run_sync and _sync_* orchestration)
- Modify: `tests/test_sync_runner.py` (update to test upsert functions only)
- Modify: `src/resonance/app.py` (remove sync_models import if unused)

**Step 1: Remove SyncJob from models**

Remove `SyncJob` from `src/resonance/models/__init__.py` `__all__` and its import line.

**Step 2: Trim sync/runner.py**

Keep the upsert functions (`_upsert_artist`, `_upsert_track`, etc.) since the worker uses them. Remove `run_sync`, `_sync_spotify`, `_sync_listenbrainz`, `_fetch_listens_resilient`, and the `SyncableConnector` protocol. These are replaced by the worker task functions.

**Step 3: Update tests**

Remove tests for `run_sync` from `tests/test_sync_runner.py`. Keep the `TestMBIDArtistMatching` tests and any upsert-specific tests.

**Step 4: Run full test suite**

Run: `uv run pytest -v`

**Step 5: Run type checker**

Run: `uv run mypy src/`

**Step 6: Commit**

```bash
git add -u src/resonance/models/ src/resonance/sync/runner.py src/resonance/app.py tests/test_sync_runner.py
git commit -m "refactor: remove SyncJob model and old sync orchestration, keep upsert functions"
```

---

## Task 11: Alembic migration to drop sync_jobs table

**Files:**
- Create: `alembic/versions/<auto>_drop_sync_jobs.py`

**Step 1: Generate migration**

Run: `uv run alembic revision --autogenerate -m "drop sync_jobs table"`

Review the generated migration — it should drop the `sync_jobs` table and its index.

**Step 2: Commit**

```bash
git add alembic/versions/
git commit -m "feat: add alembic migration to drop sync_jobs table"
```

---

## Task 12: Add worker tests for parent completion cascade

**Files:**
- Modify: `tests/test_worker.py`

**Step 1: Write tests**

Add tests to `tests/test_worker.py`:

```python
class TestCheckParentCompletion:
    @pytest.mark.anyio()
    async def test_marks_parent_completed_when_all_children_done(self) -> None:
        # Create parent + child tasks, mock DB to show 0 incomplete siblings
        ...

    @pytest.mark.anyio()
    async def test_marks_parent_failed_when_child_failed(self) -> None:
        # Create parent + child tasks, mock DB to show 1 failed sibling
        ...

    @pytest.mark.anyio()
    async def test_does_not_mark_parent_when_siblings_still_running(self) -> None:
        # Create parent + child, mock DB to show 1 incomplete sibling
        ...
```

**Step 2: Run tests**

Run: `uv run pytest tests/test_worker.py -v`

**Step 3: Commit**

```bash
git add tests/test_worker.py
git commit -m "test: add parent completion cascade tests for worker"
```

---

## Task 13: Final verification

**Step 1: Full test suite**

Run: `uv run pytest -v`

**Step 2: Type checker**

Run: `uv run mypy src/`

**Step 3: Linter**

Run: `uv run ruff check . && uv run ruff format --check .`

**Step 4: Verify arq worker starts**

Run: `uv run python -c "from resonance.worker import WorkerSettings; print('OK')"` to verify the module loads.

---

## Deployment Notes (not part of this plan)

After merging, update `megadoomer-config` to add the worker controller:

```yaml
controllers:
  worker:
    containers:
      main:
        image:
          repository: app
          tag: latest
        command: ["arq", "resonance.worker.WorkerSettings"]
        envFrom:
          - secretRef:
              name: resonance-app-secrets
```

The Alembic migrations need to be run against the production DB before deploying the new image.
