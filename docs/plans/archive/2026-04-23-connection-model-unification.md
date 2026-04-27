# Connection Model Unification Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Unify `ServiceConnection` and `UserCalendarFeed` into a single `Connection` model with type-agnostic task lifecycle, fixing calendar sync bugs and eliminating duplicated UI code.

**Architecture:** Three-phase incremental migration. Phase 1 widens `service_connections` and migrates data. Phase 2 generalizes the task lifecycle and worker dispatch. Phase 3 unifies the UI layer and removes old code. Each phase is independently deployable.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 (async), PostgreSQL, Alembic, arq, Jinja2, HTMX

**Design Doc:** [docs/plans/2026-04-23-connection-model-unification-design.md](2026-04-23-connection-model-unification-design.md)

---

## Phase 1: Schema + Data Migration

### Task 1: Widen service_connections Table

Add new columns and make OAuth-specific columns nullable so the table can hold both OAuth connections and calendar feeds.

**Files:**
- Create: `alembic/versions/o2j3k4l5m6n7_widen_service_connections.py`

**Step 1: Write the migration**

```python
"""widen service_connections for unified connection model

Add url, label, enabled, last_synced_at columns. Make encrypted_access_token
nullable for non-OAuth connections. Rename last_used_at to last_synced_at
(keep old column during transition).

Revision ID: o2j3k4l5m6n7
Revises: n1i2j3k4l5m6
Create Date: 2026-04-23

"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "o2j3k4l5m6n7"
down_revision: str | None = "n1i2j3k4l5m6"


def upgrade() -> None:
    # New columns for feed connections
    op.add_column(
        "service_connections",
        sa.Column("url", sa.String(2048), nullable=True),
    )
    op.add_column(
        "service_connections",
        sa.Column("label", sa.String(256), nullable=True),
    )
    op.add_column(
        "service_connections",
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default="true"),
    )
    op.add_column(
        "service_connections",
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Copy last_used_at values to last_synced_at for existing OAuth connections
    op.execute(
        "UPDATE service_connections SET last_synced_at = last_used_at "
        "WHERE last_used_at IS NOT NULL"
    )

    # Make encrypted_access_token nullable (feed connections have no tokens)
    op.alter_column(
        "service_connections",
        "encrypted_access_token",
        existing_type=sa.Text(),
        nullable=True,
    )

    # Make external_user_id nullable (generic iCal feeds may not have one)
    op.alter_column(
        "service_connections",
        "external_user_id",
        existing_type=sa.String(255),
        nullable=True,
    )

    # Add partial unique index for URL-based connections
    op.create_index(
        "ix_service_connections_user_url",
        "service_connections",
        ["user_id", "url"],
        unique=True,
        postgresql_where=sa.text("url IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_service_connections_user_url", "service_connections")
    op.alter_column(
        "service_connections",
        "external_user_id",
        existing_type=sa.String(255),
        nullable=False,
    )
    op.alter_column(
        "service_connections",
        "encrypted_access_token",
        existing_type=sa.Text(),
        nullable=False,
    )
    op.drop_column("service_connections", "last_synced_at")
    op.drop_column("service_connections", "enabled")
    op.drop_column("service_connections", "label")
    op.drop_column("service_connections", "url")
```

**Step 2: Run lint/type check**

Run: `uv run ruff check alembic/versions/o2j3k4l5m6n7_widen_service_connections.py && uv run mypy alembic/versions/o2j3k4l5m6n7_widen_service_connections.py`
Expected: PASS

**Step 3: Commit**

```bash
git add alembic/versions/o2j3k4l5m6n7_widen_service_connections.py
git commit -m "feat: widen service_connections table for unified connection model"
```

---

### Task 2: Update ServiceConnection Model

Update the SQLAlchemy model to reflect the new columns and nullable changes.

**Files:**
- Modify: `src/resonance/models/user.py:60-125`

**Step 1: Write tests for model changes**

Add tests to `tests/test_models.py` verifying:
- A ServiceConnection can be created with `encrypted_access_token=None` (feed)
- A ServiceConnection can be created with `url` set (iCal feed)
- A ServiceConnection can be created with `external_user_id=None` (generic iCal)
- The `enabled` field defaults to True
- The `last_synced_at` field is accessible

```python
class TestServiceConnectionUnified:

    async def test_feed_connection_no_token(self, session):
        user = User(id=uuid.uuid4(), display_name="test")
        session.add(user)
        await session.flush()
        conn = ServiceConnection(
            user_id=user.id,
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id="michael-dougherty",
            enabled=True,
        )
        session.add(conn)
        await session.flush()
        assert conn.encrypted_access_token is None
        assert conn.enabled is True

    async def test_ical_connection_with_url(self, session):
        user = User(id=uuid.uuid4(), display_name="test")
        session.add(user)
        await session.flush()
        conn = ServiceConnection(
            user_id=user.id,
            service_type=types_module.ServiceType.ICAL,
            url="https://example.com/feed.ics",
            label="My Calendar",
            enabled=True,
        )
        session.add(conn)
        await session.flush()
        assert conn.url == "https://example.com/feed.ics"
        assert conn.external_user_id is None

    async def test_enabled_defaults_true(self, session):
        user = User(id=uuid.uuid4(), display_name="test")
        session.add(user)
        await session.flush()
        conn = ServiceConnection(
            user_id=user.id,
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id="test-user",
        )
        session.add(conn)
        await session.flush()
        assert conn.enabled is True

    async def test_last_synced_at(self, session):
        user = User(id=uuid.uuid4(), display_name="test")
        session.add(user)
        await session.flush()
        now = datetime.datetime.now(datetime.UTC)
        conn = ServiceConnection(
            user_id=user.id,
            service_type=types_module.ServiceType.SPOTIFY,
            external_user_id="spotify-user",
            encrypted_access_token="token",
            last_synced_at=now,
        )
        session.add(conn)
        await session.flush()
        assert conn.last_synced_at == now
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_models.py::TestServiceConnectionUnified -v`
Expected: FAIL (new columns don't exist on model yet)

**Step 3: Update the model**

In `src/resonance/models/user.py`, update `ServiceConnection`:

- Change docstring from "OAuth connection" to "Connection to an external service"
- Make `encrypted_access_token` nullable: `orm.Mapped[str | None]` with `nullable=True, default=None`
- Make `external_user_id` nullable: `orm.Mapped[str | None]` with `nullable=True, default=None`
- Add `url` column: `orm.Mapped[str | None]` with `sa.String(2048), nullable=True, default=None`
- Add `label` column: `orm.Mapped[str | None]` with `sa.String(256), nullable=True, default=None`
- Add `enabled` column: `orm.Mapped[bool]` with `sa.Boolean, nullable=False, default=True`
- Add `last_synced_at` column: `orm.Mapped[datetime.datetime | None]` with `sa.DateTime(timezone=True), nullable=True, default=None`
- Keep `last_used_at` for now (removed in Phase 3)

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_models.py::TestServiceConnectionUnified -v`
Expected: PASS

**Step 5: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All existing tests still pass

**Step 6: Commit**

```bash
git add src/resonance/models/user.py tests/test_models.py
git commit -m "feat: update ServiceConnection model with feed connection fields"
```

---

### Task 3: Migrate Calendar Feed Data

Create an Alembic migration that copies `user_calendar_feeds` rows into `service_connections`, collapsing Songkick feeds by username.

**Files:**
- Create: `alembic/versions/p3k4l5m6n7o8_migrate_calendar_feeds.py`

**Step 1: Write the migration**

```python
"""migrate calendar feed data into service_connections

Copy UserCalendarFeed rows into service_connections:
- Songkick feeds: one row per username (collapse attendance + tracked_artist)
- iCal feeds: one row per URL

Revision ID: p3k4l5m6n7o8
Revises: o2j3k4l5m6n7
Create Date: 2026-04-23

"""

from __future__ import annotations

from alembic import op

revision: str = "p3k4l5m6n7o8"
down_revision: str | None = "o2j3k4l5m6n7"


def upgrade() -> None:
    # Migrate Songkick feeds: one connection per distinct username.
    # Extract username from URL pattern:
    #   https://www.songkick.com/users/{username}/calendars.ics?filter=...
    # Use the earliest created_at and latest last_synced_at from the pair.
    op.execute("""
        INSERT INTO service_connections (
            id, user_id, service_type, external_user_id,
            url, enabled, last_synced_at,
            connected_at, created_at, updated_at, sync_watermark
        )
        SELECT
            gen_random_uuid(),
            user_id,
            'SONGKICK',
            split_part(split_part(url, '/users/', 2), '/', 1),
            NULL,
            bool_and(enabled),
            MAX(last_synced_at),
            MIN(created_at),
            MIN(created_at),
            NOW(),
            '{}'
        FROM user_calendar_feeds
        WHERE feed_type IN ('SONGKICK_ATTENDANCE', 'SONGKICK_TRACKED_ARTIST')
        GROUP BY user_id, split_part(split_part(url, '/users/', 2), '/', 1)
    """)

    # Migrate generic iCal feeds: one connection per URL
    op.execute("""
        INSERT INTO service_connections (
            id, user_id, service_type, external_user_id,
            url, label, enabled, last_synced_at,
            connected_at, created_at, updated_at, sync_watermark
        )
        SELECT
            gen_random_uuid(),
            user_id,
            'ICAL',
            NULL,
            url,
            label,
            enabled,
            last_synced_at,
            created_at,
            created_at,
            NOW(),
            '{}'
        FROM user_calendar_feeds
        WHERE feed_type = 'ICAL_GENERIC'
    """)


def downgrade() -> None:
    # Remove migrated feed connections
    op.execute(
        "DELETE FROM service_connections "
        "WHERE service_type IN ('SONGKICK', 'ICAL') "
        "AND encrypted_access_token IS NULL"
    )
```

**Step 2: Run lint/type check**

Run: `uv run ruff check alembic/versions/p3k4l5m6n7o8_migrate_calendar_feeds.py && uv run mypy alembic/versions/p3k4l5m6n7o8_migrate_calendar_feeds.py`
Expected: PASS

**Step 3: Commit**

```bash
git add alembic/versions/p3k4l5m6n7o8_migrate_calendar_feeds.py
git commit -m "feat: migrate calendar feed data into service_connections"
```

---

### Task 4: Update Sync Tasks to Reference Unified Connections

Create a migration that links existing CALENDAR_SYNC tasks to their new service_connections rows.

**Files:**
- Create: `alembic/versions/q4l5m6n7o8p9_link_calendar_tasks.py`

**Step 1: Write the migration**

```python
"""link calendar sync tasks to unified connections

Update sync_tasks.service_connection_id for CALENDAR_SYNC tasks to
point to the new service_connections rows created from calendar feeds.

Revision ID: q4l5m6n7o8p9
Revises: p3k4l5m6n7o8
Create Date: 2026-04-23

"""

from __future__ import annotations

from alembic import op

revision: str = "q4l5m6n7o8p9"
down_revision: str | None = "p3k4l5m6n7o8"


def upgrade() -> None:
    # Link Songkick CALENDAR_SYNC tasks to unified connections.
    # The task's params->'feed_id' references a user_calendar_feeds row.
    # Find the feed's username, then find the matching service_connection.
    op.execute("""
        UPDATE sync_tasks st
        SET service_connection_id = sc.id
        FROM user_calendar_feeds ucf
        JOIN service_connections sc ON (
            sc.user_id = ucf.user_id
            AND sc.service_type = 'SONGKICK'
            AND sc.external_user_id = split_part(
                split_part(ucf.url, '/users/', 2), '/', 1
            )
        )
        WHERE st.task_type = 'CALENDAR_SYNC'
        AND st.service_connection_id IS NULL
        AND st.params->>'feed_id' = ucf.id::text
        AND ucf.feed_type IN ('SONGKICK_ATTENDANCE', 'SONGKICK_TRACKED_ARTIST')
    """)

    # Link iCal CALENDAR_SYNC tasks similarly
    op.execute("""
        UPDATE sync_tasks st
        SET service_connection_id = sc.id
        FROM user_calendar_feeds ucf
        JOIN service_connections sc ON (
            sc.user_id = ucf.user_id
            AND sc.service_type = 'ICAL'
            AND sc.url = ucf.url
        )
        WHERE st.task_type = 'CALENDAR_SYNC'
        AND st.service_connection_id IS NULL
        AND st.params->>'feed_id' = ucf.id::text
        AND ucf.feed_type = 'ICAL_GENERIC'
    """)


def downgrade() -> None:
    op.execute(
        "UPDATE sync_tasks SET service_connection_id = NULL "
        "WHERE task_type = 'CALENDAR_SYNC'"
    )
```

**Step 2: Run lint/type check**

Run: `uv run ruff check alembic/versions/q4l5m6n7o8p9_link_calendar_tasks.py && uv run mypy alembic/versions/q4l5m6n7o8p9_link_calendar_tasks.py`
Expected: PASS

**Step 3: Commit**

```bash
git add alembic/versions/q4l5m6n7o8p9_link_calendar_tasks.py
git commit -m "feat: link calendar sync tasks to unified connection rows"
```

---

## Phase 2: Task Lifecycle + Worker

### Task 5: Add ConnectionConfig to Connector Framework

Add a `ConnectionConfig` dataclass and implement it on each connector.

**Files:**
- Modify: `src/resonance/connectors/base.py`
- Modify: `src/resonance/connectors/spotify.py`
- Modify: `src/resonance/connectors/listenbrainz.py`
- Modify: `src/resonance/connectors/lastfm.py`
- Modify: `src/resonance/connectors/test.py`
- Create: `src/resonance/connectors/songkick.py`
- Create: `src/resonance/connectors/ical.py`
- Modify: `src/resonance/connectors/registry.py`
- Create: `tests/test_connector_config.py`

**Step 1: Write failing tests**

Test that every registered connector provides a `ConnectionConfig`, and that the config contains required fields.

```python
"""Tests for connector ConnectionConfig declarations."""

import resonance.connectors.base as base_module
import resonance.types as types_module


class TestConnectionConfig:

    def test_config_has_required_fields(self):
        config = base_module.ConnectionConfig(
            auth_type="oauth",
            sync_function="plan_sync",
            sync_style="incremental",
        )
        assert config.auth_type == "oauth"
        assert config.sync_function == "plan_sync"
        assert config.sync_style == "incremental"
        assert config.derive_urls is None

    def test_songkick_config(self):
        from resonance.connectors.songkick import SongkickConnector
        config = SongkickConnector.connection_config()
        assert config.auth_type == "username"
        assert config.sync_function == "sync_calendar_feed"
        assert config.sync_style == "full"
        assert config.derive_urls is not None
        urls = config.derive_urls("michael-dougherty")
        assert len(urls) == 2
        assert "attendance" in urls[0]
        assert "tracked_artist" in urls[1]

    def test_ical_config(self):
        from resonance.connectors.ical import ICalConnector
        config = ICalConnector.connection_config()
        assert config.auth_type == "url"
        assert config.sync_function == "sync_calendar_feed"
        assert config.sync_style == "full"

    def test_spotify_config(self):
        from resonance.connectors.spotify import SpotifyConnector
        config = SpotifyConnector.connection_config()
        assert config.auth_type == "oauth"
        assert config.sync_function == "plan_sync"
        assert config.sync_style == "incremental"

    def test_all_connectors_have_config(self, connector_registry):
        for connector in connector_registry.all():
            config = connector.connection_config()
            assert isinstance(config, base_module.ConnectionConfig)
            assert config.auth_type in ("oauth", "username", "url")
            assert config.sync_function
            assert config.sync_style in ("incremental", "full")
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_connector_config.py -v`
Expected: FAIL (ConnectionConfig doesn't exist yet)

**Step 3: Implement ConnectionConfig**

In `src/resonance/connectors/base.py`, add before `BaseConnector`:

```python
import dataclasses
from collections.abc import Callable

@dataclasses.dataclass(frozen=True)
class ConnectionConfig:
    """Declares how a connector authenticates and syncs."""

    auth_type: str  # "oauth", "username", "url"
    sync_function: str  # arq job name
    sync_style: str  # "incremental" or "full"
    derive_urls: Callable[[str], list[str]] | None = None
```

Add abstract method to `BaseConnector`:

```python
@staticmethod
@abc.abstractmethod
def connection_config() -> ConnectionConfig:
    """Return the connection configuration for this connector type."""
    ...
```

**Step 4: Implement on each connector**

Spotify (`src/resonance/connectors/spotify.py`):
```python
@staticmethod
def connection_config() -> base_module.ConnectionConfig:
    return base_module.ConnectionConfig(
        auth_type="oauth",
        sync_function="plan_sync",
        sync_style="incremental",
    )
```

ListenBrainz (`src/resonance/connectors/listenbrainz.py`):
```python
@staticmethod
def connection_config() -> base_module.ConnectionConfig:
    return base_module.ConnectionConfig(
        auth_type="oauth",
        sync_function="plan_sync",
        sync_style="incremental",
    )
```

Last.fm (`src/resonance/connectors/lastfm.py`):
```python
@staticmethod
def connection_config() -> base_module.ConnectionConfig:
    return base_module.ConnectionConfig(
        auth_type="oauth",
        sync_function="plan_sync",
        sync_style="incremental",
    )
```

Test (`src/resonance/connectors/test.py`):
```python
@staticmethod
def connection_config() -> base_module.ConnectionConfig:
    return base_module.ConnectionConfig(
        auth_type="oauth",
        sync_function="plan_sync",
        sync_style="incremental",
    )
```

Create `src/resonance/connectors/songkick.py`:
```python
"""Songkick connector — username-based calendar feed sync."""

from __future__ import annotations

import resonance.connectors.base as base_module
import resonance.types as types_module


def derive_songkick_urls(username: str) -> list[str]:
    """Generate Songkick iCal feed URLs from a username."""
    base = f"https://www.songkick.com/users/{username}/calendars.ics"
    return [
        f"{base}?filter=attendance",
        f"{base}?filter=tracked_artist",
    ]


class SongkickConnector:
    """Minimal connector for Songkick calendar feed connections."""

    service_type = types_module.ServiceType.SONGKICK

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        return base_module.ConnectionConfig(
            auth_type="username",
            sync_function="sync_calendar_feed",
            sync_style="full",
            derive_urls=derive_songkick_urls,
        )
```

Create `src/resonance/connectors/ical.py`:
```python
"""Generic iCal connector — URL-based calendar feed sync."""

from __future__ import annotations

import resonance.connectors.base as base_module
import resonance.types as types_module


class ICalConnector:
    """Minimal connector for generic iCal feed connections."""

    service_type = types_module.ServiceType.ICAL

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        return base_module.ConnectionConfig(
            auth_type="url",
            sync_function="sync_calendar_feed",
            sync_style="full",
        )
```

**Step 5: Register new connectors**

Find where connectors are registered (in `src/resonance/app.py` or worker startup) and register `SongkickConnector` and `ICalConnector`. Since these are lightweight connectors that don't inherit from `BaseConnector` (no HTTP client, no OAuth), they may need a lighter registration. Check the registry — it accepts anything with a `service_type` attribute. Add a `get_config` method to `ConnectorRegistry`:

```python
def get_config(
    self, service_type: types_module.ServiceType
) -> base_module.ConnectionConfig | None:
    connector = self._connectors.get(service_type)
    if connector is None:
        return None
    return connector.connection_config()
```

**Step 6: Run tests**

Run: `uv run pytest tests/test_connector_config.py -v`
Expected: PASS

**Step 7: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All tests pass

**Step 8: Commit**

```bash
git add src/resonance/connectors/base.py src/resonance/connectors/spotify.py \
  src/resonance/connectors/listenbrainz.py src/resonance/connectors/lastfm.py \
  src/resonance/connectors/test.py src/resonance/connectors/songkick.py \
  src/resonance/connectors/ical.py src/resonance/connectors/registry.py \
  tests/test_connector_config.py
git commit -m "feat: add ConnectionConfig to connector framework"
```

---

### Task 6: Extract Parent Completion Helper

Extract the parent-completion logic from the inline Spotify sync code into a shared helper.

**Files:**
- Create: `src/resonance/sync/lifecycle.py`
- Create: `tests/test_task_lifecycle.py`
- Modify: `src/resonance/worker.py` (replace inline logic)

**Step 1: Write failing tests**

```python
"""Tests for type-agnostic task lifecycle helpers."""

import datetime
import uuid

import resonance.sync.lifecycle as lifecycle_module
import resonance.models.task as task_models
import resonance.types as types_module


class TestCompleteTask:

    async def test_standalone_task_completes(self, session):
        """A task with no parent marks itself completed."""
        task = task_models.Task(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            task_type=types_module.TaskType.CALENDAR_SYNC,
            status=types_module.SyncStatus.RUNNING,
        )
        session.add(task)
        await session.flush()

        await lifecycle_module.complete_task(
            session, task, {"events_created": 5}
        )

        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result == {"events_created": 5}
        assert task.completed_at is not None

    async def test_child_task_propagates_to_parent(self, session):
        """When the last child completes, the parent also completes."""
        parent = task_models.Task(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.RUNNING,
        )
        session.add(parent)
        await session.flush()

        child = task_models.Task(
            id=uuid.uuid4(),
            user_id=parent.user_id,
            parent_id=parent.id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.RUNNING,
        )
        session.add(child)
        await session.flush()

        await lifecycle_module.complete_task(
            session, child, {"items_created": 10}
        )

        assert child.status == types_module.SyncStatus.COMPLETED
        await session.refresh(parent)
        assert parent.status == types_module.SyncStatus.COMPLETED

    async def test_parent_stays_running_with_pending_children(self, session):
        """Parent does not complete while other children are still running."""
        parent = task_models.Task(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.RUNNING,
        )
        session.add(parent)
        await session.flush()

        child1 = task_models.Task(
            id=uuid.uuid4(),
            user_id=parent.user_id,
            parent_id=parent.id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.RUNNING,
        )
        child2 = task_models.Task(
            id=uuid.uuid4(),
            user_id=parent.user_id,
            parent_id=parent.id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
        )
        session.add_all([child1, child2])
        await session.flush()

        await lifecycle_module.complete_task(
            session, child1, {"items_created": 5}
        )

        assert child1.status == types_module.SyncStatus.COMPLETED
        await session.refresh(parent)
        assert parent.status == types_module.SyncStatus.RUNNING


class TestFailTask:

    async def test_standalone_task_fails(self, session):
        task = task_models.Task(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            task_type=types_module.TaskType.CALENDAR_SYNC,
            status=types_module.SyncStatus.RUNNING,
        )
        session.add(task)
        await session.flush()

        await lifecycle_module.fail_task(session, task, "Something broke")

        assert task.status == types_module.SyncStatus.FAILED
        assert task.error_message == "Something broke"
        assert task.completed_at is not None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_task_lifecycle.py -v`
Expected: FAIL (lifecycle module doesn't exist)

**Step 3: Implement the lifecycle module**

Create `src/resonance/sync/lifecycle.py`:

```python
"""Type-agnostic task lifecycle helpers."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import sqlalchemy as sa

import resonance.models.task as task_models
import resonance.types as types_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


async def complete_task(
    session: AsyncSession,
    task: task_models.Task,
    result: dict[str, object],
) -> None:
    """Mark a task completed and propagate to parent if applicable."""
    task.status = types_module.SyncStatus.COMPLETED
    task.result = result
    task.completed_at = datetime.datetime.now(datetime.UTC)

    if task.parent_id is not None:
        await _check_parent_completion(session, task.parent_id)


async def fail_task(
    session: AsyncSession,
    task: task_models.Task,
    error_message: str,
) -> None:
    """Mark a task as failed."""
    task.status = types_module.SyncStatus.FAILED
    task.error_message = error_message
    task.completed_at = datetime.datetime.now(datetime.UTC)


async def _check_parent_completion(
    session: AsyncSession,
    parent_id: object,
) -> None:
    """Check if all children of a parent are done; if so, complete the parent."""
    parent_result = await session.execute(
        sa.select(task_models.Task).where(task_models.Task.id == parent_id)
    )
    parent = parent_result.scalar_one_or_none()
    if parent is None:
        return

    children_result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.parent_id == parent_id,
        )
    )
    children = children_result.scalars().all()

    completed = sum(
        1 for c in children
        if c.status in (types_module.SyncStatus.COMPLETED, types_module.SyncStatus.FAILED)
    )

    if completed == len(children):
        failed = sum(
            1 for c in children if c.status == types_module.SyncStatus.FAILED
        )
        children_completed = completed - failed

        parent.status = types_module.SyncStatus.COMPLETED
        parent.completed_at = datetime.datetime.now(datetime.UTC)
        parent.result = {
            "children_completed": children_completed,
            "children_failed": failed,
        }
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_task_lifecycle.py -v`
Expected: PASS

**Step 5: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/resonance/sync/lifecycle.py tests/test_task_lifecycle.py
git commit -m "feat: extract type-agnostic task lifecycle helpers"
```

---

### Task 7: Make Orphan Recovery Type-Agnostic

Update `_reenqueue_orphaned_tasks` in the worker to handle all task types.

**Files:**
- Modify: `src/resonance/worker.py:598-769`
- Modify: `tests/test_worker.py` (add CALENDAR_SYNC orphan tests)

**Step 1: Write failing tests**

Add tests verifying that CALENDAR_SYNC tasks stuck in PENDING or RUNNING are recovered on worker startup.

```python
class TestReenqueueOrphanedCalendarSync:

    async def test_pending_calendar_sync_reenqueued(self, session, arq_redis):
        """A PENDING CALENDAR_SYNC task is re-enqueued on startup."""
        conn = ServiceConnection(
            id=uuid.uuid4(),
            user_id=test_user_id,
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id="test-user",
        )
        session.add(conn)
        task = task_models.Task(
            id=uuid.uuid4(),
            user_id=test_user_id,
            service_connection_id=conn.id,
            task_type=types_module.TaskType.CALENDAR_SYNC,
            status=types_module.SyncStatus.PENDING,
        )
        session.add(task)
        await session.commit()

        await _reenqueue_orphaned_tasks(session_factory, arq_redis)

        # Verify task was re-enqueued
        # (check arq_redis for enqueued job)

    async def test_running_calendar_sync_reset_to_pending(self, session, arq_redis):
        """A RUNNING CALENDAR_SYNC task is reset to PENDING and re-enqueued."""
        # Similar to above but with RUNNING status
```

**Step 2: Implement the changes**

The key change in `_reenqueue_orphaned_tasks`:

1. Remove the `task_type.in_([SYNC_JOB, TIME_RANGE])` filter from the PENDING and RUNNING queries
2. Build a dispatch map: `task_type → arq_job_name + args_builder` using connector configs
3. Use the dispatch map to re-enqueue any orphaned task regardless of type

The dispatch map:
```python
_TASK_REENQUEUE_MAP: dict[types_module.TaskType, tuple[str, Callable]] = {
    types_module.TaskType.SYNC_JOB: ("plan_sync", lambda t: (str(t.id),)),
    types_module.TaskType.TIME_RANGE: ("sync_range", lambda t: (str(t.id),)),
    types_module.TaskType.CALENDAR_SYNC: (
        "sync_calendar_feed",
        lambda t: (str(t.service_connection_id), str(t.id)),
    ),
    types_module.TaskType.BULK_JOB: ("run_bulk_job", lambda t: (str(t.id),)),
}
```

**Step 3: Run tests**

Run: `uv run pytest tests/test_worker.py -v -k orphan`
Expected: PASS

**Step 4: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All tests pass

**Step 5: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: make orphan recovery type-agnostic for all task types"
```

---

### Task 8: Fix arq Job Dedup and Update Calendar Sync Worker

Fix the arq job ID collision and update the calendar sync worker to use `service_connection_id` instead of `feed_id`.

**Files:**
- Modify: `src/resonance/concerts/worker.py`
- Modify: `src/resonance/ui/routes.py` (sync trigger)
- Modify: `src/resonance/api/v1/calendar_feeds.py` (sync trigger)
- Modify: `tests/test_concert_worker.py`

**Step 1: Write failing tests**

Test that the calendar sync worker:
- Accepts a `connection_id` instead of `feed_id`
- Derives feed URLs from the connection's `external_user_id` (for Songkick)
- Uses the connection's `url` directly (for iCal)
- Updates `connection.last_synced_at` instead of `feed.last_synced_at`
- Uses `lifecycle_module.complete_task` for status updates

**Step 2: Update the calendar sync worker**

Key changes to `sync_calendar_feed`:
- Change signature: `async def sync_calendar_feed(ctx, connection_id: str, task_id: str)`
- Load `ServiceConnection` instead of `UserCalendarFeed`
- Use connector config's `derive_urls` to get feed URLs for Songkick
- Use `connection.url` for iCal
- Call `lifecycle_module.complete_task()` and `lifecycle_module.fail_task()` for status
- Update `connection.last_synced_at` instead of `feed.last_synced_at`

**Step 3: Update sync triggers**

In `src/resonance/ui/routes.py` (songkick sync trigger):
- Look up the `ServiceConnection` by `(user_id, service_type=SONGKICK, external_user_id=username)`
- Create one task with `service_connection_id=connection.id`
- Enqueue with `_job_id=f"sync_calendar_feed:{task.id}"` (unique per task)

In `src/resonance/api/v1/calendar_feeds.py`:
- Update or deprecate the `/{feed_id}/sync` endpoint
- Use `_job_id=f"sync_calendar_feed:{task.id}"` instead of `:{feed_id}`

**Step 4: Run tests**

Run: `uv run pytest tests/test_concert_worker.py -v`
Expected: PASS

**Step 5: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All tests pass

**Step 6: Commit**

```bash
git add src/resonance/concerts/worker.py src/resonance/ui/routes.py \
  src/resonance/api/v1/calendar_feeds.py tests/test_concert_worker.py
git commit -m "fix: use connection_id for calendar sync, fix arq job dedup"
```

---

### Task 9: Wire Lifecycle Helpers Into Existing Sync Workers

Replace inline parent-completion logic in the Spotify/ListenBrainz sync path with `lifecycle_module`.

**Files:**
- Modify: `src/resonance/worker.py` (sync_range function)
- Modify: `tests/test_worker.py`

**Step 1: Find and replace inline parent completion**

In `src/resonance/worker.py`, the `sync_range` function has inline parent-completion logic after a child task completes. Replace it with:

```python
await lifecycle_module.complete_task(session, task, result)
```

And in the error path:
```python
await lifecycle_module.fail_task(session, task, error_message)
```

**Step 2: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All tests pass (behavior should be identical)

**Step 3: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "refactor: use lifecycle helpers in existing sync workers"
```

---

## Phase 3: UI Cutover + Cleanup

### Task 10: Unify Dashboard Route

Replace the two-model query/render pattern with a single `Connection` query.

**Files:**
- Modify: `src/resonance/ui/routes.py` (dashboard function)
- Modify: `src/resonance/templates/dashboard.html`

**Step 1: Update the dashboard route**

Replace the separate `ServiceConnection` + `UserCalendarFeed` queries with a single query:

```python
connections_result = await db.execute(
    sa.select(user_models.ServiceConnection)
    .where(user_models.ServiceConnection.user_id == user_uuid)
    .order_by(user_models.ServiceConnection.connected_at)
)
connections = connections_result.scalars().all()

# Build active_syncs for ALL connections (one dict, one query)
active_tasks_result = await db.execute(
    sa.select(task_models.Task).where(
        task_models.Task.user_id == user_uuid,
        task_models.Task.service_connection_id.isnot(None),
        task_models.Task.status.in_([
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.RUNNING,
            types_module.SyncStatus.DEFERRED,
        ]),
    )
)
active_syncs = {
    str(t.service_connection_id): t
    for t in active_tasks_result.scalars().all()
}
```

Pass `connections` and `active_syncs` to the template. Remove `songkick_accounts`, `active_feed_syncs`.

**Step 2: Update the dashboard template**

Single loop for all connections:

```html
{% for conn in connections %}
<tr>
    <td>{{ conn.service_type.value | capitalize }}</td>
    <td>{{ conn.external_user_id or conn.label or conn.url }}</td>
    <td>{{ (conn.last_synced_at | localtime(user_tz)).strftime('%Y-%m-%d %H:%M') if conn.last_synced_at else 'Never' }}</td>
    <td>
        {% set active_task = active_syncs.get(conn.id | string) %}
        {% if active_task %}
            <span aria-busy="true">Syncing...</span>
        {% else %}
            <button hx-post="/api/v1/sync/{{ conn.id }}" hx-swap="none"
                hx-on::after-request="htmx.trigger('#sync-status', 'load'); this.textContent='Syncing...'; this.disabled=true"
            >Sync Now</button>
        {% endif %}
    </td>
</tr>
{% endfor %}
```

**Step 3: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All tests pass

**Step 4: Manual test in browser**

Start the dev server and verify the dashboard renders all connections in one table.

**Step 5: Commit**

```bash
git add src/resonance/ui/routes.py src/resonance/templates/dashboard.html
git commit -m "feat: unify dashboard to single connection query and template loop"
```

---

### Task 11: Unify Account Page Route

Same pattern as dashboard — one query, one template loop.

**Files:**
- Modify: `src/resonance/ui/routes.py` (account function)
- Modify: `src/resonance/templates/account.html`

**Step 1: Update the account route and template**

Replace separate ServiceConnection + Songkick queries with single Connection query. Update template to one loop with unified disconnect endpoint.

**Step 2: Run tests and manually verify**

**Step 3: Commit**

```bash
git add src/resonance/ui/routes.py src/resonance/templates/account.html
git commit -m "feat: unify account page to single connection model"
```

---

### Task 12: Unified Sync Trigger Endpoint

Create a single sync endpoint that works for all connection types.

**Files:**
- Modify: `src/resonance/api/v1/sync.py`
- Create: `tests/test_api_unified_sync.py`

**Step 1: Write failing tests**

Test that `POST /api/v1/sync/{connection_id}`:
- Creates a task and enqueues the correct arq job for OAuth connections (plan_sync)
- Creates a task and enqueues the correct arq job for Songkick connections (sync_calendar_feed)
- Returns 404 for unknown connection_id
- Returns 409 if sync already running for this connection

**Step 2: Implement the unified endpoint**

```python
@router.post("/{connection_id}")
async def trigger_sync(
    connection_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    request: fastapi.Request,
) -> dict[str, str]:
    # Load connection
    conn = await _load_connection(db, connection_id, user_id)
    # Get connector config from registry
    config = request.app.state.connector_registry.get_config(conn.service_type)
    # Check for already-running sync
    # Create task with service_connection_id=conn.id
    # Enqueue config.sync_function with _job_id=f"{config.sync_function}:{task.id}"
    return {"status": "started", "task_id": str(task.id)}
```

**Step 3: Run tests**

Run: `uv run pytest tests/test_api_unified_sync.py -v`
Expected: PASS

**Step 4: Commit**

```bash
git add src/resonance/api/v1/sync.py tests/test_api_unified_sync.py
git commit -m "feat: add unified sync trigger endpoint for all connection types"
```

---

### Task 13: Update Sync Status Partial

Remove `task_type == calendar_sync` branching from the sync status partial.

**Files:**
- Modify: `src/resonance/templates/partials/sync_status.html`

**Step 1: Simplify the template**

Replace conditional service name rendering with a single path using `task.service_connection.service_type.value`. Render `task.result` generically or with minimal type-specific formatting.

**Step 2: Manual test in browser**

**Step 3: Commit**

```bash
git add src/resonance/templates/partials/sync_status.html
git commit -m "refactor: remove task_type branching from sync status partial"
```

---

### Task 14: Remove Old Calendar Feed Code

Drop `UserCalendarFeed` model, `FeedType` enum, old API endpoints, and the `user_calendar_feeds` table.

**Files:**
- Create: `alembic/versions/r5m6n7o8p9q0_drop_user_calendar_feeds.py`
- Modify: `src/resonance/models/concert.py` (remove UserCalendarFeed class)
- Modify: `src/resonance/types.py` (remove FeedType enum)
- Modify: `src/resonance/api/v1/calendar_feeds.py` (remove or redirect old endpoints)
- Remove: tests referencing old models
- Modify: `src/resonance/concerts/worker.py` (remove old _load_feed, _FEED_TYPE_TO_SERVICE)

**Step 1: Create the drop-table migration**

```python
def upgrade() -> None:
    op.drop_table("user_calendar_feeds")
    # Drop last_used_at column (fully replaced by last_synced_at)
    op.drop_column("service_connections", "last_used_at")
```

**Step 2: Remove FeedType enum and UserCalendarFeed model**

**Step 3: Update/remove old API endpoints**

The Songkick connect/disconnect endpoints in `calendar_feeds.py` need to be rewritten to create/delete `ServiceConnection` rows instead. The lookup endpoint stays. The per-feed sync endpoint is replaced by the unified sync endpoint.

**Step 4: Clean up old tests**

**Step 5: Run full test suite**

Run: `uv run pytest --tb=short -q`
Expected: All tests pass

**Step 6: Commit**

```bash
git add -A  # Careful: review staged files
git commit -m "chore: remove UserCalendarFeed model and user_calendar_feeds table"
```

---

### Task 15: Final Validation

**Step 1: Run full test suite**

Run: `uv run pytest --tb=short -q`

**Step 2: Run linting and type checking**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 3: Manual end-to-end test**

1. Start dev server
2. Connect a Songkick username
3. Trigger sync
4. Verify events appear on the events page
5. Verify sync status updates correctly (no stuck "Syncing...")
6. Disconnect the Songkick account
7. Verify it's removed from dashboard and account page

**Step 4: Commit any fixes**
