# Graceful Shutdown Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure in-flight HTTP requests, worker tasks, and migrations survive pod restarts without data loss or stale state.

**Architecture:** Add a `ShutdownRequest` control-flow signal (parallel to existing `DeferRequest`) that sync strategies raise when a module-level `shutdown_requested` Event is set. The worker catches it, saves progress, and marks the task PENDING for resumption on next startup. Web server gets a preStop hook and uvicorn graceful timeout. Migrations get a longer grace period.

**Tech Stack:** arq, threading.Event, uvicorn, Kubernetes lifecycle hooks

---

### Task 1: Add ShutdownRequest and shutdown_requested Event

**Files:**
- Modify: `src/resonance/sync/base.py`

**Step 1: Write the test**

In `tests/test_sync_base.py` (create if needed):

```python
import threading
import resonance.sync.base as sync_base

class TestShutdownRequest:
    def test_is_exception(self) -> None:
        exc = sync_base.ShutdownRequest(
            resume_params={"offset": 100, "items_so_far": 50}
        )
        assert isinstance(exc, Exception)
        assert exc.resume_params == {"offset": 100, "items_so_far": 50}

class TestShutdownEvent:
    def test_not_set_by_default(self) -> None:
        assert not sync_base.shutdown_requested.is_set()

    def test_can_be_set_and_cleared(self) -> None:
        sync_base.shutdown_requested.set()
        assert sync_base.shutdown_requested.is_set()
        sync_base.shutdown_requested.clear()
        assert not sync_base.shutdown_requested.is_set()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_sync_base.py -v`
Expected: FAIL — `ShutdownRequest` and `shutdown_requested` don't exist yet

**Step 3: Implement**

Add to `src/resonance/sync/base.py`:

```python
import threading

# Module-level event for coordinating graceful shutdown.
# Set by the arq shutdown hook; checked by strategies between pages.
shutdown_requested = threading.Event()


class ShutdownRequest(Exception):  # noqa: N818 — control-flow signal, not an error
    """Raised by execute() when graceful shutdown is requested."""

    def __init__(self, resume_params: dict[str, Any]) -> None:
        self.resume_params = resume_params
        super().__init__("Shutdown requested, checkpointing progress")
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_sync_base.py -v`
Expected: PASS

**Step 5: Run full suite + type check**

Run: `uv run pytest && uv run mypy src/`

**Step 6: Commit**

```
git add src/resonance/sync/base.py tests/test_sync_base.py
git commit -m "feat: add ShutdownRequest signal and shutdown_requested event"
```

---

### Task 2: Handle ShutdownRequest in sync_range

**Files:**
- Modify: `src/resonance/worker.py:219-243` (sync_range try/except block)
- Test: `tests/test_worker.py`

**Step 1: Write the test**

Add to `tests/test_worker.py` in a new class:

```python
class TestSyncRangeShutdown:
    """Tests for graceful shutdown checkpoint in sync_range."""

    async def test_shutdown_request_marks_task_pending(self) -> None:
        """When strategy raises ShutdownRequest, task is marked PENDING with saved params."""
        task_id = uuid.uuid4()
        task = _make_task(
            task_id,
            parent_id=uuid.uuid4(),
            task_type=types_module.SyncTaskType.TIME_RANGE,
            status=types_module.SyncStatus.RUNNING,
        )

        mock_session = _mock_session(task)
        mock_strategy = AsyncMock()
        mock_strategy.execute = AsyncMock(
            side_effect=sync_base.ShutdownRequest(
                resume_params={"max_ts": 12345, "items_so_far": 50}
            )
        )

        ctx = _make_ctx(mock_session, strategies={types_module.ServiceType.LISTENBRAINZ: mock_strategy})
        await worker_module.sync_range(ctx, str(task_id))

        assert task.status == types_module.SyncStatus.PENDING
        assert task.params["max_ts"] == 12345
        assert task.params["items_so_far"] == 50
        assert task.started_at is None
```

Note: Adapt this test to match the existing test patterns in `test_worker.py` — use the same `_make_task`, `_mock_session`, `_make_ctx` helpers already defined there.

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_worker.py::TestSyncRangeShutdown -v`
Expected: FAIL — ShutdownRequest not caught yet

**Step 3: Implement**

In `src/resonance/worker.py`, add a handler for `ShutdownRequest` in the `sync_range` function, parallel to the existing `DeferRequest` handler (lines 226-243):

```python
except sync_base.ShutdownRequest as shutdown:
    task.status = types_module.SyncStatus.PENDING
    task.params = {**task.params, **shutdown.resume_params}
    task.started_at = None
    await session.commit()
    log.info(
        "sync_range_shutdown_checkpoint",
        resume_params=shutdown.resume_params,
    )
```

Add the import: `import resonance.sync.base as sync_base` (already imported).

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_worker.py::TestSyncRangeShutdown -v`
Expected: PASS

**Step 5: Run full suite + type check**

Run: `uv run pytest && uv run mypy src/`

**Step 6: Commit**

```
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: handle ShutdownRequest in sync_range with checkpoint"
```

---

### Task 3: Worker shutdown hook — set event, reset RUNNING tasks, close Redis

**Files:**
- Modify: `src/resonance/worker.py:562-573` (shutdown function)
- Test: `tests/test_worker.py`

**Step 1: Write the test**

```python
class TestWorkerShutdown:
    """Tests for worker shutdown hook."""

    async def test_shutdown_sets_event(self) -> None:
        import resonance.sync.base as sync_base
        sync_base.shutdown_requested.clear()

        mock_engine = AsyncMock()
        ctx: dict[str, Any] = {"engine": mock_engine}
        await worker_module.shutdown(ctx)

        assert sync_base.shutdown_requested.is_set()
        sync_base.shutdown_requested.clear()  # cleanup

    async def test_shutdown_disposes_engine(self) -> None:
        mock_engine = AsyncMock()
        ctx: dict[str, Any] = {"engine": mock_engine}

        import resonance.sync.base as sync_base
        sync_base.shutdown_requested.clear()
        await worker_module.shutdown(ctx)

        mock_engine.dispose.assert_awaited_once()
        sync_base.shutdown_requested.clear()
```

**Step 2: Run test to verify it fails**

Expected: FAIL — shutdown doesn't set the event yet

**Step 3: Implement**

Update the `shutdown` function in `worker.py`:

```python
async def shutdown(ctx: dict[str, Any]) -> None:
    """Signal graceful shutdown, then dispose of resources."""
    sync_base.shutdown_requested.set()

    wctx = typing.cast("WorkerContext", ctx)
    engine = wctx["engine"]
    await engine.dispose()
    logger.info("worker_shutdown")
```

Add import at top of worker.py: `import resonance.sync.base as sync_base`

**Step 4: Run test to verify it passes**

**Step 5: Run full suite + type check**

**Step 6: Commit**

```
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: set shutdown_requested event in worker shutdown hook"
```

---

### Task 4: Extend _reenqueue_orphaned_tasks to handle RUNNING tasks

**Files:**
- Modify: `src/resonance/worker.py:400-516`
- Test: `tests/test_worker.py`

**Step 1: Write the test**

Add to `TestReenqueueOrphanedTasks`:

```python
async def test_reenqueues_running_task(self) -> None:
    """RUNNING tasks should be reset to PENDING and re-enqueued."""
    task_id = uuid.uuid4()
    task = _make_task(
        task_id,
        task_type=types_module.SyncTaskType.TIME_RANGE,
        status=types_module.SyncStatus.RUNNING,
    )
    # ... adapt to match existing test patterns in the class
    # Verify: task.status changed to PENDING, started_at cleared, enqueue_job called
```

**Step 2: Run test to verify it fails**

**Step 3: Implement**

In `_reenqueue_orphaned_tasks`, add a query for RUNNING tasks after the existing PENDING/DEFERRED queries:

```python
# Find RUNNING tasks (interrupted by crash/restart)
running_result = await session.execute(
    sa.select(task_module.SyncTask)
    .outerjoin(
        parent_alias,
        task_module.SyncTask.parent_id == parent_alias.id,
    )
    .where(
        task_module.SyncTask.status == types_module.SyncStatus.RUNNING,
        task_module.SyncTask.task_type.in_(
            [
                types_module.SyncTaskType.SYNC_JOB,
                types_module.SyncTaskType.TIME_RANGE,
            ]
        ),
        sa.or_(
            task_module.SyncTask.parent_id.is_(None),
            parent_alias.status.notin_(
                [
                    types_module.SyncStatus.COMPLETED,
                    types_module.SyncStatus.FAILED,
                ]
            ),
        ),
    )
)
running_tasks = list(running_result.scalars().all())

# Reset RUNNING tasks back to PENDING
for task in running_tasks:
    task.status = types_module.SyncStatus.PENDING
    task.started_at = None
if running_tasks:
    await session.commit()
    logger.info("reset_running_orphans", count=len(running_tasks))
```

Then add `running_tasks` to `all_tasks`:

```python
all_tasks = pending_tasks + deferred_tasks + running_tasks
```

**Step 4: Run tests**

**Step 5: Run full suite + type check**

**Step 6: Commit**

```
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: re-enqueue RUNNING tasks as orphans on worker startup"
```

---

### Task 5: ListenBrainz shutdown checkpoint

**Files:**
- Modify: `src/resonance/sync/listenbrainz.py:127-179` (page loop)
- Test: `tests/test_sync_listenbrainz.py` (or `tests/test_worker.py`)

**Step 1: Write the test**

```python
async def test_shutdown_requested_raises_shutdown_request(self) -> None:
    """When shutdown_requested is set, the strategy raises ShutdownRequest between pages."""
    import resonance.sync.base as sync_base

    sync_base.shutdown_requested.set()
    try:
        # Set up a mock connector that returns one page of listens
        # Execute the strategy
        # Assert ShutdownRequest is raised with resume params (max_ts, items_so_far, etc.)
        ...
    finally:
        sync_base.shutdown_requested.clear()
```

**Step 2: Run test to verify it fails**

**Step 3: Implement**

In `src/resonance/sync/listenbrainz.py`, add a shutdown check at the top of the page loop (after line 127 `while True:`) and after the per-page commit (after line 179):

```python
# Check for graceful shutdown between pages
if sync_base.shutdown_requested.is_set():
    raise sync_base.ShutdownRequest(
        resume_params={
            "max_ts": max_ts,
            "items_so_far": items_created,
            "pages_fetched": pages_fetched,
            "last_listened_at": last_listened_at,
        }
    )
```

Add import: `import resonance.sync.base as sync_base`

**Step 4: Run tests**

**Step 5: Run full suite + type check**

**Step 6: Commit**

```
git add src/resonance/sync/listenbrainz.py tests/...
git commit -m "feat: add shutdown checkpoint to ListenBrainz page loop"
```

---

### Task 6: Dockerfile — uvicorn graceful shutdown timeout

**Files:**
- Modify: `Dockerfile:29`

**Step 1: Update CMD**

Change:
```dockerfile
CMD ["uvicorn", "resonance.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
```

To:
```dockerfile
CMD ["uvicorn", "resonance.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--timeout-graceful-shutdown", "30"]
```

**Step 2: Verify image builds**

Run: `docker build -t resonance-test .` (or just verify syntax)

**Step 3: Commit**

```
git add Dockerfile
git commit -m "feat: add 30s graceful shutdown timeout to uvicorn"
```

---

### Task 7: Kubernetes config — grace periods and preStop hook

**Files:**
- Modify: `~/src/github.com/megadoomer-io/megadoomer-config/applications/resonance/resonance/do/helm-values.yaml`

**This is in the megadoomer-config repo, not resonance.**

**Step 1: Add terminationGracePeriodSeconds and preStop to main controller**

Under `controllers.main`, add:

```yaml
  main:
    pod:
      terminationGracePeriodSeconds: 60
    initContainers:
      migrations:
        # ... existing config ...
        # Add termination grace for init container
    containers:
      main:
        # ... existing config ...
        lifecycle:
          preStop:
            exec:
              command: ["sleep", "5"]
```

**Step 2: Add terminationGracePeriodSeconds to worker controller**

Under `controllers.worker`, add:

```yaml
  worker:
    pod:
      terminationGracePeriodSeconds: 45
```

**Step 3: Commit in megadoomer-config repo**

```
git add applications/resonance/resonance/do/helm-values.yaml
git commit -m "feat: add graceful shutdown config for resonance pods"
```

---

## Task Summary

| Task | Component | Description |
|------|-----------|-------------|
| 1 | sync/base.py | ShutdownRequest exception + shutdown_requested Event |
| 2 | worker.py | Catch ShutdownRequest in sync_range, save checkpoint |
| 3 | worker.py | Set shutdown_requested in shutdown hook |
| 4 | worker.py | Re-enqueue RUNNING orphans on startup |
| 5 | sync/listenbrainz.py | Check shutdown between pages, raise ShutdownRequest |
| 6 | Dockerfile | Add --timeout-graceful-shutdown 30 |
| 7 | megadoomer-config | terminationGracePeriodSeconds + preStop hook |

Tasks 1-5 are in the resonance repo (application code + tests).
Task 6 is in resonance (Dockerfile).
Task 7 is in megadoomer-config (Kubernetes config).
