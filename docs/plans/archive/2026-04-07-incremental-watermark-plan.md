# Incremental Watermark Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Update sync watermarks incrementally per-page so crash recovery only loses one page of progress.

**Architecture:** Add `connection` parameter to `SyncStrategy.execute()`, update `sync_watermark` on each page commit inside the strategy, and teach `plan_sync` to read two-ended watermarks (`newest_synced_at`/`oldest_synced_at`) for interrupted-sync recovery.

**Tech Stack:** Python, SQLAlchemy async, arq worker, pytest

---

### Task 1: Update `SyncStrategy.execute()` signature

**Files:**
- Modify: `src/resonance/sync/base.py:65-73`
- Test: `tests/test_sync_base.py`

**Step 1: Write the failing test**

In `tests/test_sync_base.py`, add a test that verifies the abstract `execute()` method accepts a `connection` parameter. Since `SyncStrategy` is abstract, test via a concrete subclass:

```python
class TestSyncStrategySignature:
    def test_execute_accepts_connection_parameter(self) -> None:
        """execute() signature includes connection parameter."""
        import inspect
        sig = inspect.signature(sync_base.SyncStrategy.execute)
        params = list(sig.parameters.keys())
        assert "connection" in params
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_sync_base.py::TestSyncStrategySignature -v`
Expected: FAIL — `connection` not in params

**Step 3: Update the abstract method signature**

In `src/resonance/sync/base.py`, update `execute()`:

```python
@abc.abstractmethod
async def execute(
    self,
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: connector_base.BaseConnector,
    connection: user_models.ServiceConnection,
) -> dict[str, Any]:
    """Execute a single child task. May raise DeferRequest."""
    ...
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_sync_base.py::TestSyncStrategySignature -v`
Expected: PASS

**Step 5: Run type checker**

Run: `uv run mypy src/resonance/sync/base.py`
Expected: PASS (or errors in downstream files that we'll fix in subsequent tasks)

**Step 6: Commit**

```bash
git add src/resonance/sync/base.py tests/test_sync_base.py
git commit -m "refactor: add connection parameter to SyncStrategy.execute() signature"
```

---

### Task 2: Thread `connection` through `worker.sync_range`

**Files:**
- Modify: `src/resonance/worker.py:228`
- Test: `tests/test_worker.py`

**Step 1: Write the failing test**

Add a test in `TestSyncRangeWatermarkWrite` (or a new class) that verifies `strategy.execute` is called with the `connection` object:

```python
class TestSyncRangePassesConnection:
    @pytest.mark.asyncio
    async def test_execute_receives_connection(self) -> None:
        """strategy.execute() is called with the connection object."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        task = task_module.SyncTask(
            id=task_id,
            user_id=uuid.uuid4(),
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.SyncTaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.LISTENBRAINZ
        connection.id = conn_id
        connection.sync_watermark = {}

        session = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection
        # _check_parent_completion mocks
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = _make_task(
            task_id=parent_id, status=types_module.SyncStatus.RUNNING
        )
        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0
        children_scalars = MagicMock()
        children_scalars.all.return_value = [task]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            task_result, conn_result,
            pending_result, parent_result, failed_result, children_result,
        ]

        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.return_value = {"items_created": 0, "items_updated": 0}

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.LISTENBRAINZ: mock_strategy},
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        mock_strategy.execute.assert_called_once_with(
            session, task, mock_connector, connection
        )
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_worker.py::TestSyncRangePassesConnection -v`
Expected: FAIL — execute called without connection arg

**Step 3: Update worker.py to pass connection**

In `src/resonance/worker.py`, line 228, change:

```python
result = await strategy.execute(session, task, connector)
```

to:

```python
result = await strategy.execute(session, task, connector, connection)
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_worker.py::TestSyncRangePassesConnection -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "refactor: pass connection to strategy.execute() in sync_range"
```

---

### Task 3: ListenBrainz per-page watermark update

**Files:**
- Modify: `src/resonance/sync/listenbrainz.py:89-240`
- Test: `tests/test_sync_listenbrainz.py`

**Step 1: Write the failing test — watermark updated per page**

```python
class TestIncrementalWatermark:
    @pytest.mark.asyncio
    async def test_watermark_updated_after_each_page(self) -> None:
        """connection.sync_watermark is updated after each page commit."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connection = _make_connection()
        connector = _make_lb_connector()

        page1 = [_make_listen(1700000100), _make_listen(1700000050)]
        page2 = [_make_listen(1700000020), _make_listen(1700000010)]

        connector.get_listens = AsyncMock(side_effect=[page1, page2, []])

        watermark_snapshots: list[dict[str, object]] = []

        original_commit = session.commit

        async def capture_watermark() -> None:
            await original_commit()
            if connection.sync_watermark:
                watermark_snapshots.append(dict(connection.sync_watermark.get("listens", {})))

        session.commit = AsyncMock(side_effect=capture_watermark)

        with (
            patch.object(lb_sync_module.runner_module, "bulk_fetch_artists", new_callable=AsyncMock, return_value={}),
            patch.object(lb_sync_module.runner_module, "bulk_fetch_tracks", new_callable=AsyncMock, return_value={}),
            patch.object(lb_sync_module.runner_module, "_upsert_artist_from_track", new_callable=AsyncMock),
            patch.object(lb_sync_module.runner_module, "_upsert_track", new_callable=AsyncMock),
            patch.object(lb_sync_module.runner_module, "_upsert_listening_event", new_callable=AsyncMock),
        ):
            result = await strategy.execute(session, task, connector, connection)

        # After page 1: oldest is 1700000050
        assert watermark_snapshots[0]["newest_synced_at"] == 1700000100
        assert watermark_snapshots[0]["oldest_synced_at"] == 1700000050
        # After page 2: oldest is 1700000010
        assert watermark_snapshots[1]["newest_synced_at"] == 1700000100
        assert watermark_snapshots[1]["oldest_synced_at"] == 1700000010
```

**Step 2: Write the failing test — result watermark uses new structure**

```python
    @pytest.mark.asyncio
    async def test_result_watermark_uses_two_ended_structure(self) -> None:
        """Result watermark has newest_synced_at and oldest_synced_at."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connection = _make_connection()
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100)
        listen2 = _make_listen(1700000050)
        connector.get_listens = AsyncMock(side_effect=[[listen1, listen2], []])

        with (
            patch.object(lb_sync_module.runner_module, "bulk_fetch_artists", new_callable=AsyncMock, return_value={}),
            patch.object(lb_sync_module.runner_module, "bulk_fetch_tracks", new_callable=AsyncMock, return_value={}),
            patch.object(lb_sync_module.runner_module, "_upsert_artist_from_track", new_callable=AsyncMock),
            patch.object(lb_sync_module.runner_module, "_upsert_track", new_callable=AsyncMock),
            patch.object(lb_sync_module.runner_module, "_upsert_listening_event", new_callable=AsyncMock),
        ):
            result = await strategy.execute(session, task, connector, connection)

        assert result["watermark"] == {
            "newest_synced_at": 1700000100,
            "oldest_synced_at": 1700000050,
        }
```

**Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_listenbrainz.py::TestIncrementalWatermark -v`
Expected: FAIL

**Step 4: Implement per-page watermark update**

In `src/resonance/sync/listenbrainz.py`:

1. Add `connection` parameter to `execute()` signature (line 89-93):

```python
async def execute(
    self,
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: connector_base.BaseConnector,
    connection: user_models.ServiceConnection,
) -> dict[str, object]:
```

2. After the page commit (line 230-232), add watermark update:

```python
            # Use the oldest listen's timestamp for next page
            max_ts = listens[-1].listened_at
            task.progress_current = items_created

            # Update watermark incrementally
            updated_watermarks = dict(connection.sync_watermark)
            updated_watermarks["listens"] = {
                "newest_synced_at": last_listened_at,
                "oldest_synced_at": max_ts,
            }
            connection.sync_watermark = updated_watermarks

            await session.commit()
```

3. Update the result watermark (lines 234-237) to use the new structure:

```python
        result: dict[str, object] = {"items_created": items_created}
        if last_listened_at is not None:
            result["last_listened_at"] = last_listened_at
            result["watermark"] = {
                "newest_synced_at": last_listened_at,
                "oldest_synced_at": max_ts if max_ts is not None else last_listened_at,
            }
```

**Step 5: Update existing tests**

All existing `execute()` tests need the `connection` argument added. Update every `strategy.execute(session, task, connector)` call to `strategy.execute(session, task, connector, connection)`, creating a connection mock via `_make_connection()`.

**Step 6: Run all ListenBrainz tests**

Run: `uv run pytest tests/test_sync_listenbrainz.py -v`
Expected: ALL PASS

**Step 7: Run type checker**

Run: `uv run mypy src/resonance/sync/listenbrainz.py`
Expected: PASS

**Step 8: Commit**

```bash
git add src/resonance/sync/listenbrainz.py tests/test_sync_listenbrainz.py
git commit -m "feat: per-page watermark updates in ListenBrainz sync strategy"
```

---

### Task 4: Spotify strategy — add `connection` parameter

**Files:**
- Modify: `src/resonance/sync/spotify.py:117-176`
- Test: `tests/test_sync_spotify_strategy.py`

**Step 1: Update `execute()` signature**

Add `connection` parameter to `SpotifySyncStrategy.execute()`:

```python
async def execute(
    self,
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: connector_base.BaseConnector,
    connection: user_models.ServiceConnection,
) -> dict[str, object]:
```

Also add per-page watermark update in `_sync_saved_tracks` (the only Spotify function with a page loop). After the existing `await session.commit()` at line 320:

```python
        # Update watermark incrementally
        updated_watermarks = dict(connection.sync_watermark)
        updated_watermarks[data_type] = dict(watermark)
        connection.sync_watermark = updated_watermarks
```

This requires passing `connection` and `data_type` through to `_sync_saved_tracks`. Update the helper signature and call site accordingly.

**Step 2: Update existing tests**

Update all `execute()` calls in `tests/test_sync_spotify_strategy.py` to include the `connection` parameter.

**Step 3: Run all Spotify tests**

Run: `uv run pytest tests/test_sync_spotify_strategy.py -v`
Expected: ALL PASS

**Step 4: Run type checker**

Run: `uv run mypy src/resonance/sync/spotify.py`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/sync/spotify.py tests/test_sync_spotify_strategy.py
git commit -m "refactor: add connection parameter to Spotify sync strategy"
```

---

### Task 5: Backward-compatible watermark reading in `plan()`

**Files:**
- Modify: `src/resonance/sync/listenbrainz.py:37-87` (plan method)
- Test: `tests/test_sync_listenbrainz.py`

**Step 1: Write the failing test — legacy watermark**

```python
class TestPlanBackwardCompatibility:
    @pytest.mark.asyncio
    async def test_legacy_last_listened_at_treated_as_newest_synced_at(self) -> None:
        """Legacy watermark with last_listened_at is treated as complete sync."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={"listens": {"last_listened_at": 1700000000}},
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        desc = descriptors[0]
        # Should use last_listened_at as min_ts (incremental from there)
        assert desc.params["min_ts"] == 1700000000
```

**Step 2: Write the failing test — new watermark complete sync**

```python
    @pytest.mark.asyncio
    async def test_new_watermark_complete_sync_uses_newest_as_min_ts(self) -> None:
        """Complete two-ended watermark: plans one task from newest_synced_at."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={"listens": {
                "newest_synced_at": 1700000000,
                "oldest_synced_at": 1650000000,
            }},
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        desc = descriptors[0]
        assert desc.params["min_ts"] == 1700000000
```

**Step 3: Write the failing test — interrupted sync produces two tasks**

```python
    @pytest.mark.asyncio
    async def test_interrupted_sync_plans_two_tasks(self) -> None:
        """Interrupted sync with gap: plans new-listens + remaining-backfill tasks."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        # Watermark shows we synced from 1700000000 down to 1680000000,
        # but original sync started from epoch (no prior min_ts)
        connection = _make_connection(
            sync_watermark={"listens": {
                "newest_synced_at": 1700000000,
                "oldest_synced_at": 1680000000,
            }},
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 2
        # Task 1: new listens since newest_synced_at
        assert descriptors[0].params["min_ts"] == 1700000000
        # Task 2: remaining backfill below oldest_synced_at
        assert descriptors[1].params["max_ts"] == 1680000000
        assert descriptors[1].params.get("min_ts") is None
```

**Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_listenbrainz.py::TestPlanBackwardCompatibility -v`
Expected: FAIL (at least the two-task test)

**Step 5: Implement backward-compatible watermark reading**

Update `plan()` in `src/resonance/sync/listenbrainz.py`:

```python
async def plan(
    self,
    session: sa_async.AsyncSession,
    connection: user_models.ServiceConnection,
    connector: connector_base.BaseConnector,
) -> list[sync_base.SyncTaskDescriptor]:
    lb_connector = _cast_connector(connector)
    username = connection.external_user_id

    progress_total: int | None = None
    try:
        progress_total = await lb_connector.get_listen_count(username)
    except (httpx.HTTPError, connector_base.RateLimitExceededError):
        logger.warning("could_not_fetch_listen_count", username=username)

    listens_watermark = connection.sync_watermark.get("listens", {})

    # Read two-ended watermark, with backward compat for legacy format
    newest_synced_at: int | None = None
    oldest_synced_at: int | None = None

    raw_newest = listens_watermark.get("newest_synced_at")
    raw_oldest = listens_watermark.get("oldest_synced_at")
    raw_legacy = listens_watermark.get("last_listened_at")

    if raw_newest is not None:
        newest_synced_at = int(str(raw_newest))
        oldest_synced_at = int(str(raw_oldest)) if raw_oldest is not None else None
    elif raw_legacy is not None:
        # Legacy format: treat as complete sync up to this point
        newest_synced_at = int(str(raw_legacy))

    descriptors: list[sync_base.SyncTaskDescriptor] = []

    if newest_synced_at is not None:
        # Task 1: new listens since last sync
        listened_at_dt = datetime.datetime.fromtimestamp(
            newest_synced_at, tz=datetime.UTC
        )
        date_str = listened_at_dt.date().isoformat()
        descriptors.append(
            sync_base.SyncTaskDescriptor(
                task_type=types_module.SyncTaskType.TIME_RANGE,
                params={"username": username, "min_ts": newest_synced_at},
                progress_total=progress_total,
                description=f"Syncing new listens since {date_str}",
            )
        )

        # Task 2: remaining backfill if interrupted
        if oldest_synced_at is not None:
            descriptors.append(
                sync_base.SyncTaskDescriptor(
                    task_type=types_module.SyncTaskType.TIME_RANGE,
                    params={
                        "username": username,
                        "max_ts": oldest_synced_at,
                        "min_ts": None,
                    },
                    description="Resuming listening history backfill",
                )
            )
    else:
        # No watermark at all — full sync
        descriptors.append(
            sync_base.SyncTaskDescriptor(
                task_type=types_module.SyncTaskType.TIME_RANGE,
                params={"username": username, "min_ts": None},
                progress_total=progress_total,
                description="Syncing listening history",
            )
        )

    return descriptors
```

**Step 6: Update existing plan tests**

Existing tests for `test_incremental_sync_with_watermark` and `test_incremental_description_says_since` use the legacy `last_listened_at` format — they should still pass without changes (backward compat).

**Step 7: Run all ListenBrainz tests**

Run: `uv run pytest tests/test_sync_listenbrainz.py -v`
Expected: ALL PASS

**Step 8: Run type checker**

Run: `uv run mypy src/resonance/sync/listenbrainz.py`
Expected: PASS

**Step 9: Commit**

```bash
git add src/resonance/sync/listenbrainz.py tests/test_sync_listenbrainz.py
git commit -m "feat: two-ended watermark reading with interrupted sync recovery in plan()"
```

---

### Task 6: Full integration — run all tests, lint, type check

**Step 1: Run full test suite**

Run: `uv run pytest -v`
Expected: ALL PASS

**Step 2: Run linter**

Run: `uv run ruff check .`
Expected: PASS

**Step 3: Run formatter check**

Run: `uv run ruff format --check .`
Expected: PASS

**Step 4: Run type checker**

Run: `uv run mypy src/`
Expected: PASS

**Step 5: Commit any remaining fixes**

If any issues were found, fix and commit.
