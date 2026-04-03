# Sync Orchestration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace hardcoded per-service sync functions in worker.py with pluggable SyncStrategy classes, add task deferral for long rate limits, and improve sync UX.

**Architecture:** Strategy pattern — each service gets a SyncStrategy class (in `sync/`) that owns planning (task decomposition, watermarks) and execution (fetching, upserting). The worker becomes a thin dispatcher. Deferral uses a DeferRequest exception that the worker catches and converts to a delayed arq job.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 (async), arq, Pydantic, Jinja2/HTMX

**Design doc:** `docs/plans/2026-04-03-sync-orchestration-design.md`

**Key references:**
- Current worker: `src/resonance/worker.py`
- Current sync runner: `src/resonance/sync/runner.py`
- SyncTask model: `src/resonance/models/task.py`
- Types/enums: `src/resonance/types.py`
- Connectors: `src/resonance/connectors/base.py`, `spotify.py`, `listenbrainz.py`
- Sync API: `src/resonance/api/v1/sync.py`
- UI routes: `src/resonance/ui/routes.py`
- UI templates: `src/resonance/templates/partials/sync_status.html`, `dashboard.html`

**Run commands:**
- Tests: `uv run pytest`
- Single test: `uv run pytest tests/test_file.py::TestClass::test_name -v`
- Lint: `uv run ruff check .`
- Format: `uv run ruff format .`
- Type check: `uv run mypy src/`
- All checks: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`

---

## Task 1: Add DEFERRED status and model columns

**Files:**
- Modify: `src/resonance/types.py:47-54`
- Modify: `src/resonance/models/task.py:23-90`
- Modify: `tests/test_models.py` (if enum tests exist)
- Create: `alembic/versions/<auto>_add_deferred_status_and_columns.py`

**Step 1: Add DEFERRED to SyncStatus enum**

In `src/resonance/types.py`, add `DEFERRED` after `FAILED`:

```python
class SyncStatus(enum.StrEnum):
    """Status of a synchronization job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEFERRED = "deferred"
```

**Step 2: Add description and deferred_until columns to SyncTask**

In `src/resonance/models/task.py`, add two columns after `completed_at` (line ~85):

```python
    description: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    deferred_until: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
```

**Step 3: Run type checks and tests**

Run: `uv run mypy src/ && uv run pytest`
Expected: PASS (no tests depend on exact SyncStatus members or SyncTask columns)

**Step 4: Write Alembic migration**

Create a new migration file. Use the pattern from `6ac8ba60ca95`:

```python
"""add deferred status, description and deferred_until columns

Revision ID: <generate>
Revises: 6ac8ba60ca95
Create Date: 2026-04-03

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "<generate>"
down_revision: str | None = "6ac8ba60ca95"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Add new columns
    op.add_column("sync_tasks", sa.Column("description", sa.Text(), nullable=True))
    op.add_column(
        "sync_tasks",
        sa.Column("deferred_until", sa.DateTime(timezone=True), nullable=True),
    )

    # Update status CHECK constraint to include 'deferred'
    op.drop_constraint("ck_sync_tasks_status", "sync_tasks", type_="check")
    op.create_check_constraint(
        "ck_sync_tasks_status",
        "sync_tasks",
        "status IN ('pending', 'running', 'completed', 'failed', 'deferred')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_sync_tasks_status", "sync_tasks", type_="check")
    op.create_check_constraint(
        "ck_sync_tasks_status",
        "sync_tasks",
        "status IN ('pending', 'running', 'completed', 'failed')",
    )
    op.drop_column("sync_tasks", "deferred_until")
    op.drop_column("sync_tasks", "description")
```

**IMPORTANT:** The existing CHECK constraint uses uppercase values (`'PENDING'`, `'RUNNING'`, etc.) but SyncStatus is a StrEnum with lowercase values (`"pending"`, `"running"`, etc.). Check the actual stored values in the database before writing the migration. If the DB stores lowercase, the existing constraint may be silently ineffective and this migration should use lowercase. If uppercase, match that. The migration above uses lowercase to match StrEnum `.value`.

**Step 5: Run all checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/types.py src/resonance/models/task.py alembic/versions/*deferred*
git commit -m "feat: add DEFERRED sync status, description and deferred_until columns"
```

---

## Task 2: Create SyncStrategy ABC and supporting types

**Files:**
- Create: `src/resonance/sync/base.py`
- Create: `tests/test_sync_base.py`

**Step 1: Write tests for SyncTaskDescriptor and DeferRequest**

Create `tests/test_sync_base.py`:

```python
"""Tests for sync strategy base classes."""

import pytest
import pydantic

import resonance.sync.base as sync_base
import resonance.types as types_module


class TestSyncTaskDescriptor:
    """Tests for SyncTaskDescriptor data model."""

    def test_required_fields(self) -> None:
        desc = sync_base.SyncTaskDescriptor(
            task_type=types_module.SyncTaskType.TIME_RANGE,
            params={"data_type": "saved_tracks"},
        )
        assert desc.task_type == types_module.SyncTaskType.TIME_RANGE
        assert desc.params == {"data_type": "saved_tracks"}
        assert desc.progress_total is None
        assert desc.description == ""

    def test_optional_fields(self) -> None:
        desc = sync_base.SyncTaskDescriptor(
            task_type=types_module.SyncTaskType.TIME_RANGE,
            params={"data_type": "saved_tracks"},
            progress_total=500,
            description="Fetching your saved tracks",
        )
        assert desc.progress_total == 500
        assert desc.description == "Fetching your saved tracks"

    def test_missing_required_field_raises(self) -> None:
        with pytest.raises(pydantic.ValidationError):
            sync_base.SyncTaskDescriptor(params={"x": 1})  # type: ignore[call-arg]


class TestDeferRequest:
    """Tests for DeferRequest exception."""

    def test_stores_retry_after_and_resume_params(self) -> None:
        exc = sync_base.DeferRequest(
            retry_after=3600.0,
            resume_params={"max_ts": 12345, "items_so_far": 100},
        )
        assert exc.retry_after == 3600.0
        assert exc.resume_params == {"max_ts": 12345, "items_so_far": 100}

    def test_is_exception(self) -> None:
        exc = sync_base.DeferRequest(retry_after=60.0, resume_params={})
        assert isinstance(exc, Exception)

    def test_message_includes_retry_after(self) -> None:
        exc = sync_base.DeferRequest(retry_after=3600.0, resume_params={})
        assert "3600" in str(exc)


class TestSyncStrategyABC:
    """Tests for SyncStrategy abstract base class."""

    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            sync_base.SyncStrategy()  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_plan_and_execute(self) -> None:
        class IncompleteStrategy(sync_base.SyncStrategy):
            concurrency = "parallel"

        with pytest.raises(TypeError):
            IncompleteStrategy()  # type: ignore[abstract]
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_base.py -v`
Expected: FAIL (module not found)

**Step 3: Write sync/base.py**

Create `src/resonance/sync/base.py`:

```python
"""Base classes for sync strategies."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING

import pydantic

import resonance.types as types_module  # noqa: TC001 — Pydantic needs at runtime

if TYPE_CHECKING:
    import resonance.connectors.base as connector_base
    import resonance.models.task as task_module
    import resonance.models.user as user_models
    import sqlalchemy.ext.asyncio as sa_async


class SyncTaskDescriptor(pydantic.BaseModel):
    """Lightweight description of a child task to create."""

    task_type: types_module.SyncTaskType
    params: dict[str, object]
    progress_total: int | None = None
    description: str = ""


class DeferRequest(Exception):
    """Raised by execute() when a rate limit exceeds acceptable wait time.

    Args:
        retry_after: Seconds until the request can be retried.
        resume_params: State to merge into task.params for resumption.
    """

    def __init__(self, retry_after: float, resume_params: dict[str, object]) -> None:
        self.retry_after = retry_after
        self.resume_params = resume_params
        super().__init__(
            f"Sync deferred for {retry_after:.0f}s"
        )


class SyncStrategy(abc.ABC):
    """Defines how a service plans and executes sync tasks.

    Attributes:
        concurrency: "sequential" or "parallel" — controls whether the
            worker enqueues all children at once or one-at-a-time.
    """

    concurrency: str

    @abc.abstractmethod
    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: connector_base.BaseConnector,
    ) -> list[SyncTaskDescriptor]:
        """Return child task descriptors for a sync job.

        Receives the session to query watermarks and history. Must only
        return data — do not create database rows.
        """
        ...

    @abc.abstractmethod
    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: connector_base.BaseConnector,
    ) -> dict[str, object]:
        """Execute a single child task.

        Receives the session to upsert data and update task progress.
        Returns a result dict (e.g., items_created, items_updated).
        May raise DeferRequest to pause and resume later.
        """
        ...
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_sync_base.py -v`
Expected: PASS

**Step 5: Run all checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/sync/base.py tests/test_sync_base.py
git commit -m "feat: add SyncStrategy ABC, SyncTaskDescriptor, and DeferRequest"
```

---

## Task 3: Implement ListenBrainzSyncStrategy

**Files:**
- Create: `src/resonance/sync/listenbrainz.py`
- Create: `tests/test_sync_listenbrainz.py`

**Step 1: Write tests for ListenBrainzSyncStrategy.plan()**

Create `tests/test_sync_listenbrainz.py`:

```python
"""Tests for ListenBrainz sync strategy."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.sync.listenbrainz as lb_sync
import resonance.types as types_module


def _mock_connection(external_user_id: str = "testuser") -> MagicMock:
    conn = MagicMock()
    conn.id = uuid.uuid4()
    conn.external_user_id = external_user_id
    conn.service_type = types_module.ServiceType.LISTENBRAINZ
    return conn


class TestListenBrainzPlan:
    """Tests for ListenBrainzSyncStrategy.plan()."""

    @pytest.mark.asyncio
    async def test_returns_single_descriptor(self) -> None:
        strategy = lb_sync.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connector = AsyncMock()
        connector.get_listen_count = AsyncMock(return_value=5000)
        connection = _mock_connection()

        # No watermark (no previous completed tasks)
        watermark_result = MagicMock()
        watermark_result.scalar_one_or_none.return_value = None
        session.execute.return_value = watermark_result

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        assert descriptors[0].task_type == types_module.SyncTaskType.TIME_RANGE
        assert descriptors[0].params["username"] == "testuser"
        assert descriptors[0].params["min_ts"] is None
        assert descriptors[0].progress_total == 5000

    @pytest.mark.asyncio
    async def test_uses_watermark_for_incremental(self) -> None:
        strategy = lb_sync.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connector = AsyncMock()
        connector.get_listen_count = AsyncMock(return_value=100)
        connection = _mock_connection()

        # Previous task with watermark
        last_task = MagicMock()
        last_task.result = {"last_listened_at": 1700000000}
        watermark_result = MagicMock()
        watermark_result.scalar_one_or_none.return_value = last_task
        session.execute.return_value = watermark_result

        descriptors = await strategy.plan(session, connection, connector)

        assert descriptors[0].params["min_ts"] == 1700000000
        assert "since" in descriptors[0].description.lower()

    @pytest.mark.asyncio
    async def test_description_for_full_sync(self) -> None:
        strategy = lb_sync.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connector = AsyncMock()
        connector.get_listen_count = AsyncMock(return_value=5000)
        connection = _mock_connection()

        watermark_result = MagicMock()
        watermark_result.scalar_one_or_none.return_value = None
        session.execute.return_value = watermark_result

        descriptors = await strategy.plan(session, connection, connector)

        assert "listening history" in descriptors[0].description.lower()

    def test_concurrency_is_parallel(self) -> None:
        assert lb_sync.ListenBrainzSyncStrategy.concurrency == "parallel"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_listenbrainz.py -v`
Expected: FAIL (module not found)

**Step 3: Implement ListenBrainzSyncStrategy**

Create `src/resonance/sync/listenbrainz.py`:

```python
"""ListenBrainz sync strategy."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.connectors.base as base_module
import resonance.models.task as task_module
import resonance.sync.base as sync_base
import resonance.sync.runner as runner_module
import resonance.types as types_module

if TYPE_CHECKING:
    import resonance.connectors.listenbrainz as lb_module
    import resonance.models.user as user_models
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()


class ListenBrainzSyncStrategy(sync_base.SyncStrategy):
    """Sync strategy for ListenBrainz listening history."""

    concurrency = "parallel"

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: base_module.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        lb_connector: lb_module.ListenBrainzConnector = connector  # type: ignore[assignment]
        username = connection.external_user_id

        # Fetch listen count for progress tracking
        progress_total: int | None = None
        try:
            progress_total = await lb_connector.get_listen_count(username)
        except Exception:
            logger.warning("could_not_fetch_listen_count", username=username)

        # Query watermark from last completed TIME_RANGE task
        min_ts = await self._get_watermark(session, connection.id)

        if min_ts is not None:
            ts_date = datetime.datetime.fromtimestamp(min_ts, tz=datetime.UTC)
            description = f"Syncing new listens since {ts_date.strftime('%b %d, %Y')}"
        else:
            description = "Syncing listening history"

        return [
            sync_base.SyncTaskDescriptor(
                task_type=types_module.SyncTaskType.TIME_RANGE,
                params={"username": username, "min_ts": min_ts},
                progress_total=progress_total,
                description=description,
            ),
        ]

    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: base_module.BaseConnector,
    ) -> dict[str, object]:
        lb_connector: lb_module.ListenBrainzConnector = connector  # type: ignore[assignment]
        username = str(task.params.get("username", ""))
        min_ts_param = task.params.get("min_ts")
        min_ts: int | None = int(str(min_ts_param)) if min_ts_param is not None else None
        max_ts: int | None = None
        items_created = 0
        page = 0
        last_listened_at: int | None = None

        while True:
            try:
                listens = await lb_connector.get_listens(
                    username, max_ts=max_ts, min_ts=min_ts, count=100
                )
            except base_module.RateLimitExceededError as exc:
                raise sync_base.DeferRequest(
                    retry_after=exc.retry_after,
                    resume_params={"max_ts": max_ts, "items_so_far": items_created},
                ) from exc

            if not listens:
                break
            page += 1

            # Track the most recent listen for watermark
            if last_listened_at is None:
                last_listened_at = listens[0].listened_at

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
            logger.info(
                "listenbrainz_page_synced",
                page=page,
                listens_in_page=len(listens),
                total_created=items_created,
                max_ts=max_ts,
            )

        result: dict[str, object] = {"items_created": items_created}
        if last_listened_at is not None:
            result["last_listened_at"] = last_listened_at
        return result

    @staticmethod
    async def _get_watermark(
        session: sa_async.AsyncSession,
        connection_id: object,
    ) -> int | None:
        """Find the most recent listened_at timestamp from completed tasks."""
        result = await session.execute(
            sa.select(task_module.SyncTask)
            .where(
                task_module.SyncTask.service_connection_id == connection_id,
                task_module.SyncTask.task_type == types_module.SyncTaskType.TIME_RANGE,
                task_module.SyncTask.status == types_module.SyncStatus.COMPLETED,
            )
            .order_by(task_module.SyncTask.completed_at.desc())
            .limit(1)
        )
        last_task = result.scalar_one_or_none()
        if last_task is None:
            return None

        task_result = last_task.result or {}
        watermark = task_result.get("last_listened_at")
        if watermark is not None:
            return int(str(watermark))
        return None
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_sync_listenbrainz.py -v`
Expected: PASS

**Step 5: Run all checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/sync/listenbrainz.py tests/test_sync_listenbrainz.py
git commit -m "feat: add ListenBrainzSyncStrategy with watermark-based incremental sync"
```

---

## Task 4: Implement SpotifySyncStrategy

**Files:**
- Create: `src/resonance/sync/spotify.py`
- Create: `tests/test_sync_spotify_strategy.py`

**Step 1: Write tests for SpotifySyncStrategy.plan()**

Create `tests/test_sync_spotify_strategy.py`:

```python
"""Tests for Spotify sync strategy."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.connectors.base as base_module
import resonance.sync.spotify as spotify_sync
import resonance.types as types_module


def _mock_connection(
    access_token: str = "test-token",
    token_expired: bool = False,
) -> MagicMock:
    import datetime

    import resonance.crypto as crypto_module

    conn = MagicMock()
    conn.id = uuid.uuid4()
    conn.service_type = types_module.ServiceType.SPOTIFY
    conn.encrypted_access_token = crypto_module.encrypt_token(
        access_token, "dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk="
    )
    conn.encrypted_refresh_token = None
    if token_expired:
        conn.token_expires_at = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
    else:
        conn.token_expires_at = datetime.datetime(2030, 1, 1, tzinfo=datetime.UTC)
    return conn


class TestSpotifyPlan:
    """Tests for SpotifySyncStrategy.plan()."""

    @pytest.mark.asyncio
    async def test_returns_three_descriptors(self) -> None:
        strategy = spotify_sync.SpotifySyncStrategy(
            token_encryption_key="dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk="
        )
        session = AsyncMock()
        connector = AsyncMock()
        connection = _mock_connection()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 3
        data_types = [d.params["data_type"] for d in descriptors]
        assert "followed_artists" in data_types
        assert "saved_tracks" in data_types
        assert "recently_played" in data_types

    @pytest.mark.asyncio
    async def test_descriptors_have_descriptions(self) -> None:
        strategy = spotify_sync.SpotifySyncStrategy(
            token_encryption_key="dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk="
        )
        session = AsyncMock()
        connector = AsyncMock()
        connection = _mock_connection()

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert desc.description != ""

    @pytest.mark.asyncio
    async def test_descriptors_include_access_token(self) -> None:
        strategy = spotify_sync.SpotifySyncStrategy(
            token_encryption_key="dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk="
        )
        session = AsyncMock()
        connector = AsyncMock()
        connection = _mock_connection(access_token="my-token")

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert desc.params["access_token"] == "my-token"

    def test_concurrency_is_sequential(self) -> None:
        assert spotify_sync.SpotifySyncStrategy.concurrency == "sequential"


class TestSpotifyExecute:
    """Tests for SpotifySyncStrategy.execute()."""

    @pytest.mark.asyncio
    async def test_followed_artists_calls_connector(self) -> None:
        strategy = spotify_sync.SpotifySyncStrategy(
            token_encryption_key="dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk="
        )
        session = AsyncMock()
        connector = AsyncMock()
        connector.get_followed_artists = AsyncMock(return_value=[])

        task = MagicMock()
        task.params = {"data_type": "followed_artists", "access_token": "tok"}
        task.user_id = uuid.uuid4()
        task.service_connection_id = uuid.uuid4()

        result = await strategy.execute(session, task, connector)

        connector.get_followed_artists.assert_called_once_with("tok")
        assert "items_created" in result

    @pytest.mark.asyncio
    async def test_rate_limit_raises_defer_request(self) -> None:
        strategy = spotify_sync.SpotifySyncStrategy(
            token_encryption_key="dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk="
        )
        session = AsyncMock()
        connector = AsyncMock()
        connector.get_followed_artists = AsyncMock(
            side_effect=base_module.RateLimitExceededError(
                retry_after=3600.0, max_wait=120.0
            )
        )

        task = MagicMock()
        task.params = {"data_type": "followed_artists", "access_token": "tok"}
        task.user_id = uuid.uuid4()
        task.service_connection_id = uuid.uuid4()

        import resonance.sync.base as sync_base

        with pytest.raises(sync_base.DeferRequest) as exc_info:
            await strategy.execute(session, task, connector)

        assert exc_info.value.retry_after == 3600.0
        assert exc_info.value.resume_params["data_type"] == "followed_artists"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_spotify_strategy.py -v`
Expected: FAIL (module not found)

**Step 3: Implement SpotifySyncStrategy**

Create `src/resonance/sync/spotify.py`:

```python
"""Spotify sync strategy."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import structlog

import resonance.connectors.base as base_module
import resonance.crypto as crypto_module
import resonance.models.task as task_module
import resonance.sync.base as sync_base
import resonance.sync.runner as runner_module
import resonance.types as types_module

if TYPE_CHECKING:
    import resonance.connectors.spotify as spotify_module
    import resonance.models.user as user_models
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

_DATA_TYPE_DESCRIPTIONS: dict[str, str] = {
    "followed_artists": "Fetching your followed artists",
    "saved_tracks": "Fetching your saved tracks",
    "recently_played": "Fetching your recent plays",
}


class SpotifySyncStrategy(sync_base.SyncStrategy):
    """Sync strategy for Spotify library data."""

    concurrency = "sequential"

    def __init__(self, token_encryption_key: str) -> None:
        self._token_encryption_key = token_encryption_key

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: base_module.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        access_token = crypto_module.decrypt_token(
            connection.encrypted_access_token, self._token_encryption_key
        )

        descriptors: list[sync_base.SyncTaskDescriptor] = []
        for data_type, description in _DATA_TYPE_DESCRIPTIONS.items():
            descriptors.append(
                sync_base.SyncTaskDescriptor(
                    task_type=types_module.SyncTaskType.TIME_RANGE,
                    params={"data_type": data_type, "access_token": access_token},
                    description=description,
                ),
            )
        return descriptors

    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: base_module.BaseConnector,
    ) -> dict[str, object]:
        sp_connector: spotify_module.SpotifyConnector = connector  # type: ignore[assignment]
        data_type = str(task.params.get("data_type", ""))
        access_token = str(task.params.get("access_token", ""))

        items_created = 0
        items_updated = 0

        try:
            if data_type == "followed_artists":
                items_created, items_updated = await self._sync_followed_artists(
                    session, task, sp_connector, access_token
                )
            elif data_type == "saved_tracks":
                items_created, items_updated = await self._sync_saved_tracks(
                    session, task, sp_connector, access_token
                )
            elif data_type == "recently_played":
                items_created = await self._sync_recently_played(
                    session, task, sp_connector, access_token
                )
        except base_module.RateLimitExceededError as exc:
            raise sync_base.DeferRequest(
                retry_after=exc.retry_after,
                resume_params={
                    "data_type": data_type,
                    "items_created": items_created,
                    "items_updated": items_updated,
                },
            ) from exc

        await session.commit()
        result: dict[str, object] = {
            "items_created": items_created,
            "items_updated": items_updated,
        }
        logger.info(
            "spotify_range_completed",
            data_type=data_type,
            items_created=items_created,
            items_updated=items_updated,
        )
        return result

    @staticmethod
    async def _sync_followed_artists(
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: spotify_module.SpotifyConnector,
        access_token: str,
    ) -> tuple[int, int]:
        artists = await connector.get_followed_artists(access_token)
        logger.info("spotify_artists_fetched", count=len(artists))
        created = 0
        updated = 0
        for artist_data in artists:
            with session.no_autoflush:
                was_created = await runner_module._upsert_artist(session, artist_data)
                await session.flush()
                if was_created:
                    created += 1
                else:
                    updated += 1
                await runner_module._upsert_user_artist_relation(
                    session, task.user_id, artist_data, task.service_connection_id
                )
        return created, updated

    @staticmethod
    async def _sync_saved_tracks(
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: spotify_module.SpotifyConnector,
        access_token: str,
    ) -> tuple[int, int]:
        tracks = await connector.get_saved_tracks(access_token)
        logger.info("spotify_tracks_fetched", count=len(tracks))
        created = 0
        updated = 0
        for track_data in tracks:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(session, track_data)
                await session.flush()
                was_created = await runner_module._upsert_track(session, track_data)
                await session.flush()
                if was_created:
                    created += 1
                else:
                    updated += 1
                await runner_module._upsert_user_track_relation(
                    session, task.user_id, track_data, task.service_connection_id
                )
        return created, updated

    @staticmethod
    async def _sync_recently_played(
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: spotify_module.SpotifyConnector,
        access_token: str,
    ) -> int:
        played_items = await connector.get_recently_played(access_token)
        logger.info("spotify_recent_fetched", count=len(played_items))
        created = 0
        for played_item in played_items:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(session, played_item.track)
                await session.flush()
                await runner_module._upsert_track(session, played_item.track)
                await session.flush()
                await runner_module._upsert_listening_event(
                    session, task.user_id, played_item.track, played_item.played_at
                )
            created += 1
        return created
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_sync_spotify_strategy.py -v`
Expected: PASS

**Step 5: Run all checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/sync/spotify.py tests/test_sync_spotify_strategy.py
git commit -m "feat: add SpotifySyncStrategy with deferral on rate limit"
```

---

## Task 5: Refactor worker to use strategy dispatch

**Files:**
- Modify: `src/resonance/worker.py`
- Modify: `tests/test_worker.py`

This is the largest task. The worker's `plan_sync` and `sync_range` become generic dispatchers, and the `_plan_*`/`_run_*` functions are deleted.

**Step 1: Update worker tests for strategy dispatch**

Modify `tests/test_worker.py`. Add/update tests:

```python
# Add to imports:
import resonance.sync.base as sync_base

# Add new test class after existing TestPlanSync:

class TestPlanSyncStrategyDispatch:
    """Tests for plan_sync strategy dispatch."""

    @pytest.mark.asyncio
    async def test_no_strategy_marks_task_failed(self) -> None:
        """When no strategy is registered, task is marked FAILED."""
        session = AsyncMock()
        task = _make_task(status=types_module.SyncStatus.PENDING)
        connection = MagicMock()
        connection.service_type = types_module.ServiceType.SPOTIFY

        # Load task -> task, commit (set RUNNING), load connection
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection
        session.execute.side_effect = [task_result, conn_result]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "settings": MagicMock(),
            "connector_registry": MagicMock(),
            "strategies": {},  # empty — no strategy
            "redis": AsyncMock(),
        }

        await worker_module.plan_sync(ctx, str(task.id))

        assert task.status == types_module.SyncStatus.FAILED
        assert "strategy" in str(task.error_message).lower()


class TestSyncRangeDeferral:
    """Tests for sync_range deferral handling."""

    @pytest.mark.asyncio
    async def test_defer_request_sets_deferred_status(self) -> None:
        """When strategy raises DeferRequest, task is set to DEFERRED."""
        session = AsyncMock()
        task = _make_task(status=types_module.SyncStatus.PENDING)
        task.params = {"data_type": "saved_tracks"}
        connection = MagicMock()
        connection.service_type = types_module.ServiceType.SPOTIFY

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection
        session.execute.side_effect = [task_result, conn_result]

        # Strategy that raises DeferRequest
        strategy = MagicMock()
        strategy.execute = AsyncMock(
            side_effect=sync_base.DeferRequest(
                retry_after=3600.0,
                resume_params={"data_type": "saved_tracks", "offset": 50},
            )
        )

        connector = MagicMock()
        registry = MagicMock()
        registry.get.return_value = connector

        arq_redis = AsyncMock()

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "settings": MagicMock(),
            "connector_registry": registry,
            "strategies": {types_module.ServiceType.SPOTIFY: strategy},
            "redis": arq_redis,
        }

        await worker_module.sync_range(ctx, str(task.id))

        assert task.status == types_module.SyncStatus.DEFERRED
        assert task.deferred_until is not None
        assert task.params["offset"] == 50
        arq_redis.enqueue_job.assert_called()
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_worker.py::TestPlanSyncStrategyDispatch -v`
Expected: FAIL (strategies not in ctx)

**Step 3: Refactor worker.py**

This is a major rewrite of `worker.py`. The key changes:

1. `startup()` registers strategies in `ctx["strategies"]`
2. `plan_sync()` looks up strategy, calls `strategy.plan()`, creates rows, enqueues
3. `sync_range()` looks up strategy, calls `strategy.execute()`, handles `DeferRequest`
4. Delete: `_plan_listenbrainz_sync`, `_plan_spotify_sync`, `_run_listenbrainz_range`, `_run_spotify_range`, `_get_watermark`
5. `_check_parent_completion` updated to treat `DEFERRED` as non-terminal

Here's the refactored `worker.py` in full (replace the entire file):

- Remove imports: `resonance.connectors.listenbrainz`, `resonance.connectors.spotify`, `resonance.sync.runner`
- Add imports: `resonance.sync.base`, `resonance.sync.listenbrainz`, `resonance.sync.spotify`
- `plan_sync`:
  - After loading connection, look up `strategy = ctx["strategies"].get(connection.service_type)`
  - If None, fail the task with "No sync strategy registered for {service_type}"
  - Look up connector from registry
  - Call `descriptors = await strategy.plan(session, connection, connector)`
  - Create SyncTask rows from each descriptor, setting `description`, `progress_total`, `params`
  - Enqueue based on `strategy.concurrency` ("parallel" = all, "sequential" = first only)
- `sync_range`:
  - After loading connection, look up strategy and connector
  - Wrap `strategy.execute(session, task, connector)` call:
    - On success: set COMPLETED, store result
    - On `DeferRequest`: set DEFERRED, merge resume_params, set deferred_until, enqueue delayed job
    - On other Exception: set FAILED (existing behavior)
- `_check_parent_completion`:
  - Change the `notin_` filter from `[COMPLETED, FAILED]` to `[COMPLETED, FAILED, DEFERRED]` — wait, actually DEFERRED is non-terminal, so it should NOT be in the "done" list. The current code counts tasks NOT in terminal states. DEFERRED is non-terminal (task still pending retry), so it correctly blocks parent completion already by not being in `[COMPLETED, FAILED]`. No change needed here.
  - BUT: we do need to avoid enqueuing the next sibling when a sibling is DEFERRED. The current logic enqueues next PENDING sibling when `pending_count > 0`. A DEFERRED task is not PENDING, so it won't be picked as "next". But the flow is: pending count includes DEFERRED tasks (they're not COMPLETED/FAILED), so `pending_count > 0` is true, then it looks for a PENDING sibling. If the only remaining task is DEFERRED, `next_pending` will be None. This case is already handled by the `else` branch ("parent_still_pending"). So actually no code change is needed in `_check_parent_completion`.
- `startup`:
  - After creating connector_registry, build strategies dict:
    ```python
    ctx["strategies"] = {
        types_module.ServiceType.SPOTIFY: spotify_sync.SpotifySyncStrategy(
            token_encryption_key=settings.token_encryption_key
        ),
        types_module.ServiceType.LISTENBRAINZ: lb_sync.ListenBrainzSyncStrategy(),
    }
    ```

**Step 4: Run tests**

Run: `uv run pytest tests/test_worker.py -v`
Expected: PASS

**Step 5: Run all checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "refactor: replace per-service sync functions with strategy dispatch

Worker's plan_sync and sync_range now use SyncStrategy classes instead of
hardcoded _plan_*/_run_* functions. Adds DeferRequest handling for rate
limit deferral with delayed arq re-enqueue."
```

---

## Task 6: Update sync API guard for DEFERRED status

**Files:**
- Modify: `src/resonance/api/v1/sync.py:72-91`
- Modify: `tests/test_api_sync.py`

**Step 1: Write test for DEFERRED blocking new sync**

Add to `tests/test_api_sync.py` in `TestSyncTrigger`:

```python
    async def test_deferred_task_returns_409(self) -> None:
        """A DEFERRED task blocks new sync trigger."""
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        fake_conn = MagicMock(spec=user_models.ServiceConnection)
        fake_conn.id = conn_id
        fake_conn.user_id = user_id
        fake_conn.service_type = types_module.ServiceType.SPOTIFY

        fake_deferred_task = MagicMock(spec=task_models.SyncTask)
        fake_deferred_task.id = uuid.uuid4()
        fake_deferred_task.status = types_module.SyncStatus.DEFERRED

        db_session = FakeAsyncSession()
        db_session.set_execute_results(
            [
                FakeScalarResult(fake_conn),
                FakeScalarResult(fake_deferred_task),
            ]
        )

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/sync/spotify")

        assert response.status_code == 409
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_api_sync.py::TestSyncTrigger::test_deferred_task_returns_409 -v`
Expected: FAIL (DEFERRED not in status filter)

**Step 3: Update sync API to include DEFERRED in guard**

In `src/resonance/api/v1/sync.py`, update the `running_stmt` filter (line ~77):

```python
    running_stmt = sa.select(task_models.SyncTask).where(
        task_models.SyncTask.user_id == user_id,
        task_models.SyncTask.service_connection_id == connection.id,
        task_models.SyncTask.task_type == types_module.SyncTaskType.SYNC_JOB,
        task_models.SyncTask.status.in_(
            [
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
                types_module.SyncStatus.DEFERRED,
            ]
        ),
    )
```

Also update `cancel_sync` to accept DEFERRED tasks (line ~127):

```python
    if job.status not in (
        types_module.SyncStatus.PENDING,
        types_module.SyncStatus.RUNNING,
        types_module.SyncStatus.DEFERRED,
    ):
```

And add a test for cancelling DEFERRED tasks.

**Step 4: Run tests**

Run: `uv run pytest tests/test_api_sync.py -v`
Expected: PASS

**Step 5: Run all checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/api/v1/sync.py tests/test_api_sync.py
git commit -m "fix: include DEFERRED status in sync guard and cancel eligibility"
```

---

## Task 7: Update sync status API response for new fields

**Files:**
- Modify: `src/resonance/api/v1/sync.py:141-189`
- Modify: `tests/test_api_sync.py`

**Step 1: Add test for description and deferred_until in status response**

Add to `TestSyncStatus` in `tests/test_api_sync.py`:

```python
    async def test_returns_description_and_deferred_until(self) -> None:
        user_id = uuid.uuid4()

        fake_task = MagicMock(spec=task_models.SyncTask)
        fake_task.id = uuid.uuid4()
        fake_task.status = types_module.SyncStatus.DEFERRED
        fake_task.task_type = types_module.SyncTaskType.SYNC_JOB
        fake_task.progress_current = 50
        fake_task.progress_total = 200
        fake_task.result = {}
        fake_task.error_message = None
        fake_task.description = "Fetching your saved tracks"
        fake_task.deferred_until = datetime.datetime(
            2026, 4, 3, 4, 0, 0, tzinfo=datetime.UTC
        )
        fake_task.started_at = datetime.datetime(
            2026, 4, 2, 22, 0, 0, tzinfo=datetime.UTC
        )
        fake_task.completed_at = None

        db_session = FakeAsyncSession()
        scalars_result = FakeScalarsResult([fake_task])
        execute_result = MagicMock()
        execute_result.scalars.return_value = scalars_result
        db_session.set_execute_results([execute_result])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.get("/api/v1/sync/status")

        assert response.status_code == 200
        data = response.json()
        assert data[0]["description"] == "Fetching your saved tracks"
        assert data[0]["deferred_until"] is not None
        assert data[0]["status"] == "deferred"
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_api_sync.py::TestSyncStatus::test_returns_description_and_deferred_until -v`
Expected: FAIL (fields not in response)

**Step 3: Update sync_status response**

In `src/resonance/api/v1/sync.py`, update the response dict in `sync_status()`:

```python
    return [
        {
            "id": str(job.id),
            "status": str(job.status),
            "task_type": str(job.task_type),
            "description": job.description,
            "progress_current": job.progress_current,
            "progress_total": job.progress_total,
            "items_created": job.result.get("items_created", 0)
            if isinstance(job.result, dict)
            else 0,
            "items_updated": job.result.get("items_updated", 0)
            if isinstance(job.result, dict)
            else 0,
            "error_message": job.error_message,
            "deferred_until": (
                job.deferred_until.isoformat()
                if job.deferred_until is not None
                else None
            ),
            "started_at": (
                job.started_at.isoformat() if job.started_at is not None else None
            ),
            "completed_at": (
                job.completed_at.isoformat() if job.completed_at is not None else None
            ),
        }
        for job in jobs
    ]
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_api_sync.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/api/v1/sync.py tests/test_api_sync.py
git commit -m "feat: include description and deferred_until in sync status API"
```

---

## Task 8: Update UI templates for new progress display and conditional sync button

**Files:**
- Modify: `src/resonance/templates/partials/sync_status.html`
- Modify: `src/resonance/templates/dashboard.html`
- Modify: `src/resonance/ui/routes.py:275-323`

**Step 1: Update sync_status_partial route to include child task descriptions**

In `src/resonance/ui/routes.py`, update `sync_status_partial` to also load child tasks for active syncs, so we can show per-child descriptions and progress:

Update the query to also fetch children for display. After the existing child progress aggregation loop, also collect active child descriptions:

```python
        # Collect active child info for display
        for job in sync_jobs:
            if job.status in (
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
                types_module.SyncStatus.DEFERRED,
            ):
                children_result = await db.execute(
                    sa.select(task_models.SyncTask)
                    .where(task_models.SyncTask.parent_id == job.id)
                    .order_by(task_models.SyncTask.created_at)
                )
                job._active_children = list(children_result.scalars().all())
            else:
                job._active_children = []
```

Also update the `has_active_sync` check to include DEFERRED:

```python
    has_active_sync = any(
        j.status in (
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.RUNNING,
            types_module.SyncStatus.DEFERRED,
        )
        for j in sync_jobs
    )
```

**Step 2: Update sync_status.html template**

Replace `src/resonance/templates/partials/sync_status.html` with a redesigned version:

```html
{% if sync_jobs %}
{% if has_active_sync %}
<div hx-get="/partials/sync-status" hx-trigger="every 3s" hx-swap="outerHTML">
{% endif %}
<figure>
    <table>
        <thead>
            <tr>
                <th>Service</th>
                <th>Status</th>
                <th>Progress</th>
                <th>Time</th>
                <th></th>
            </tr>
        </thead>
        <tbody>
            {% for job in sync_jobs %}
            <tr>
                <td>{{ job.service_connection.service_type.value | capitalize }}</td>
                <td>
                    {% if job.status.value == 'deferred' %}
                    <span>Deferred{% if job.deferred_until %} until {{ job.deferred_until.strftime('%H:%M') }}{% endif %}</span>
                    {% elif job.status.value in ('pending', 'running') %}
                    <span aria-busy="true">{{ job.status.value | capitalize }}</span>
                    {% else %}
                    {{ job.status.value | capitalize }}
                    {% endif %}
                </td>
                <td>
                    {% if job.status.value in ('pending', 'running', 'deferred') %}
                        {% set active_children = job._active_children | default([]) %}
                        {% for child in active_children %}
                            {% if child.status.value in ('running', 'deferred') %}
                            <div style="margin-bottom: 0.25rem;">
                                <small>{{ child.description or 'Processing...' }}</small>
                                {% if child.progress_total %}
                                <progress value="{{ child.progress_current }}" max="{{ child.progress_total }}" style="width: 100%;"></progress>
                                <small>{{ child.progress_current }} of {{ child.progress_total }}
                                {% if child.started_at and child.progress_current > 0 and child.progress_total %}
                                    {% set elapsed = (now - child.started_at).total_seconds() %}
                                    {% set rate = child.progress_current / elapsed if elapsed > 0 else 0 %}
                                    {% set remaining = ((child.progress_total - child.progress_current) / rate) if rate > 0 else 0 %}
                                    {% if remaining > 60 %}
                                    (~{{ (remaining / 60) | int }}m remaining)
                                    {% elif remaining > 0 %}
                                    (~{{ remaining | int }}s remaining)
                                    {% endif %}
                                {% endif %}
                                </small>
                                {% else %}
                                <small>{{ child.progress_current }} so far</small>
                                {% endif %}
                            </div>
                            {% elif child.status.value == 'pending' %}
                            <div style="margin-bottom: 0.25rem;">
                                <small>{{ child.description or 'Waiting...' }}</small>
                            </div>
                            {% elif child.status.value == 'completed' %}
                            <div style="margin-bottom: 0.25rem;">
                                <small>{{ child.description or 'Done' }} -- {{ child.result.get('items_created', 0) }} new, {{ child.result.get('items_updated', 0) }} updated</small>
                            </div>
                            {% endif %}
                        {% endfor %}
                    {% elif job.status.value == 'completed' %}
                        {{ job.result.get('items_created', 0) }} new / {{ job.result.get('items_updated', 0) }} updated
                    {% elif job.status.value == 'failed' %}
                        {% if job.error_message %}
                        <small>{{ job.error_message[:80] }}</small>
                        {% else %}
                        Failed
                        {% endif %}
                    {% else %}
                    --
                    {% endif %}
                </td>
                <td>
                    {% if job.completed_at %}
                    {{ job.completed_at.strftime('%H:%M:%S') }}
                    {% elif job.started_at %}
                    Started {{ job.started_at.strftime('%H:%M:%S') }}
                    {% else %}
                    {{ job.created_at.strftime('%H:%M') }}
                    {% endif %}
                </td>
                <td>
                    {% if job.status.value in ('pending', 'running', 'deferred') %}
                    <button
                        hx-post="/api/v1/sync/cancel/{{ job.id }}"
                        hx-target="#sync-status"
                        hx-swap="innerHTML"
                        hx-confirm="Cancel this sync job?"
                        class="outline secondary"
                        style="padding: 0.25rem 0.5rem; font-size: 0.8rem;"
                    >Cancel</button>
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
{% if has_active_sync %}
</div>
{% endif %}
{% else %}
<p>No sync jobs yet. Connect a service and click <strong>Sync Now</strong> to get started.</p>
{% endif %}
```

**Step 3: Update dashboard.html sync button to be conditional**

In `src/resonance/templates/dashboard.html`, the Sync Now button should check for active tasks. Update the route to pass active task info per connection, then update the template:

In `src/resonance/ui/routes.py`, update the dashboard route to query active tasks per connection:

```python
        # Build active sync lookup for conditional button
        active_syncs: dict[str, task_models.SyncTask] = {}
        for conn in connections:
            active_stmt = sa.select(task_models.SyncTask).where(
                task_models.SyncTask.user_id == user_uuid,
                task_models.SyncTask.service_connection_id == conn.id,
                task_models.SyncTask.task_type == types_module.SyncTaskType.SYNC_JOB,
                task_models.SyncTask.status.in_([
                    types_module.SyncStatus.PENDING,
                    types_module.SyncStatus.RUNNING,
                    types_module.SyncStatus.DEFERRED,
                ]),
            )
            active_result = await db.execute(active_stmt)
            active_task = active_result.scalar_one_or_none()
            if active_task is not None:
                active_syncs[str(conn.id)] = active_task
```

Pass `active_syncs` to the template context. Then update `dashboard.html`:

```html
                <td>
                    {% set active_task = active_syncs.get(conn.id | string) %}
                    {% if active_task %}
                        {% if active_task.status.value == 'deferred' %}
                        <span>Deferred{% if active_task.deferred_until %} until {{ active_task.deferred_until.strftime('%H:%M') }}{% endif %}</span>
                        {% else %}
                        <span aria-busy="true">Syncing...</span>
                        {% endif %}
                        <button
                            hx-post="/api/v1/sync/cancel/{{ active_task.id }}"
                            hx-swap="none"
                            hx-confirm="Cancel sync?"
                            hx-on::after-request="location.reload()"
                            class="outline secondary"
                            style="padding: 0.25rem 0.5rem; font-size: 0.8rem;"
                        >Cancel</button>
                    {% else %}
                    <button
                        hx-post="/api/v1/sync/{{ conn.service_type.value }}"
                        hx-swap="none"
                        hx-on::after-request="htmx.trigger('#sync-status', 'load'); this.textContent='Syncing...'; this.disabled=true"
                    >Sync Now</button>
                    {% endif %}
                </td>
```

**Step 4: Pass `now` to template context for ETA calculation**

In `sync_status_partial`, add `datetime.datetime.now(datetime.UTC)` to the context:

```python
    return templates.TemplateResponse(
        request,
        "partials/sync_status.html",
        {"sync_jobs": sync_jobs, "has_active_sync": has_active_sync, "now": datetime.datetime.now(datetime.UTC)},
    )
```

Add `import datetime` at the top of `routes.py` if not already present.

**Step 5: Run all checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: PASS

**Step 6: Commit**

```bash
git add src/resonance/ui/routes.py src/resonance/templates/partials/sync_status.html src/resonance/templates/dashboard.html
git commit -m "feat: unified progress display with ETA, conditional sync/cancel button

Shows per-child task descriptions and progress during active sync.
Replaces Sync Now button with status + Cancel when sync is active.
Adds estimated time remaining for tasks with known progress_total."
```

---

## Task 9: Final integration test and cleanup

**Files:**
- Modify: `tests/test_worker.py` (ensure old test patterns still pass)
- Review all files for leftover references

**Step 1: Verify no references to deleted functions**

Search for any remaining references to the deleted functions:

```bash
uv run ruff check . && uv run ruff format --check .
```

Then grep for any stale references:

```
_plan_listenbrainz_sync
_plan_spotify_sync
_run_listenbrainz_range
_run_spotify_range
_get_watermark (in worker.py only — sync/listenbrainz.py has its own)
```

Remove or update any found.

**Step 2: Run full test suite**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`
Expected: ALL PASS

**Step 3: Commit any cleanup**

```bash
git add -u
git commit -m "chore: remove stale references to deleted worker functions"
```

(Skip if no changes needed.)

---

## Implementation order and dependencies

```
Task 1: Model changes (DEFERRED status, columns)
  └── Task 2: SyncStrategy ABC
       ├── Task 3: ListenBrainzSyncStrategy
       ├── Task 4: SpotifySyncStrategy
       └── (both must be done before Task 5)
            └── Task 5: Worker refactor (largest task)
                 ├── Task 6: Sync API guard update
                 ├── Task 7: Sync status API update
                 └── Task 8: UI templates
                      └── Task 9: Integration test and cleanup
```

Tasks 3 and 4 can be done in parallel.
Tasks 6, 7, and 8 can be done in parallel after Task 5.
