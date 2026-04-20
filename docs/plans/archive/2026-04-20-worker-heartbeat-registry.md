# Worker Heartbeat & Registry Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate stale arq job locks after worker pod restarts by adding heartbeat-based lock renewal, a worker registry, and startup stale-lock cleanup. Fixes #48.

**Architecture:** A `@with_heartbeat` decorator refreshes arq's `in-progress` lock keys with short TTLs (60s) instead of relying on arq's 24-hour timeout. A Redis-based worker registry tracks live workers so startup can identify and clear locks from dead workers. An idle heartbeat task keeps the worker registered between jobs.

**Tech Stack:** Python asyncio, Redis (via arq's `ArqRedis`), decorator pattern

---

### Task 1: Worker identity helper

**Files:**
- Create: `src/resonance/heartbeat.py`
- Test: `tests/test_heartbeat.py`

**Step 1: Write the failing test**

```python
"""Tests for the worker heartbeat and registry module."""

from __future__ import annotations

import resonance.heartbeat as heartbeat_module


class TestWorkerIdentity:
    """Tests for get_worker_id()."""

    def test_returns_string_with_hostname_and_pid(self) -> None:
        worker_id = heartbeat_module.get_worker_id()
        assert worker_id.startswith("worker:")
        parts = worker_id.split(":")
        assert len(parts) == 3  # "worker", hostname, pid
        assert parts[2].isdigit()

    def test_is_stable_across_calls(self) -> None:
        assert heartbeat_module.get_worker_id() == heartbeat_module.get_worker_id()
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_heartbeat.py::TestWorkerIdentity -v`
Expected: FAIL with `ModuleNotFoundError`

**Step 3: Write minimal implementation**

Create `src/resonance/heartbeat.py`:

```python
"""Worker heartbeat, registry, and stale lock cleanup for arq."""

from __future__ import annotations

import os
import socket

_worker_id: str | None = None


def get_worker_id() -> str:
    """Return a stable unique identifier for this worker process.

    Format: ``worker:{hostname}:{pid}``. In Kubernetes the hostname
    is the pod name, which changes on every restart.
    """
    global _worker_id  # noqa: PLW0603
    if _worker_id is None:
        _worker_id = f"worker:{socket.gethostname()}:{os.getpid()}"
    return _worker_id
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_heartbeat.py::TestWorkerIdentity -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/heartbeat.py tests/test_heartbeat.py
git commit -m "feat: add worker identity helper for heartbeat system

Part of #48"
```

---

### Task 2: `@with_heartbeat` decorator

**Files:**
- Modify: `src/resonance/heartbeat.py`
- Test: `tests/test_heartbeat.py`

**Step 1: Write the failing tests**

Add to `tests/test_heartbeat.py`:

```python
import asyncio
from unittest.mock import AsyncMock, patch

import pytest

import resonance.heartbeat as heartbeat_module


class TestWithHeartbeat:
    """Tests for the @with_heartbeat decorator."""

    @pytest.mark.asyncio
    async def test_decorated_function_runs_normally(self) -> None:
        """The wrapped function executes and returns normally."""
        @heartbeat_module.with_heartbeat(interval=0.1, ttl=1)
        async def my_task(ctx: dict, task_id: str) -> str:
            return "done"

        ctx: dict = {"redis": AsyncMock()}
        result = await my_task(ctx, "test-task-id")
        assert result == "done"

    @pytest.mark.asyncio
    async def test_refreshes_lock_during_execution(self) -> None:
        """The heartbeat refreshes the in-progress key while the job runs."""
        mock_redis = AsyncMock()

        @heartbeat_module.with_heartbeat(interval=0.05, ttl=1)
        async def slow_task(ctx: dict, task_id: str) -> str:
            await asyncio.sleep(0.15)  # long enough for ~2 heartbeats
            return "done"

        ctx: dict = {"redis": mock_redis}
        await slow_task(ctx, "job123")

        # Should have called psetex for both the job lock and worker key
        psetex_calls = mock_redis.psetex.call_args_list
        job_lock_refreshes = [
            c for c in psetex_calls
            if c.args[0] == b"arq:in-progress:job123"
        ]
        assert len(job_lock_refreshes) >= 1

    @pytest.mark.asyncio
    async def test_heartbeat_cancelled_on_error(self) -> None:
        """Heartbeat task is cancelled even if the job raises."""
        mock_redis = AsyncMock()

        @heartbeat_module.with_heartbeat(interval=0.05, ttl=1)
        async def failing_task(ctx: dict, task_id: str) -> None:
            raise ValueError("boom")

        ctx: dict = {"redis": mock_redis}
        with pytest.raises(ValueError, match="boom"):
            await failing_task(ctx, "job456")

    @pytest.mark.asyncio
    async def test_stores_worker_id_in_lock_value(self) -> None:
        """Lock value contains the worker identity, not just b'1'."""
        mock_redis = AsyncMock()

        @heartbeat_module.with_heartbeat(interval=0.05, ttl=1)
        async def quick_task(ctx: dict, task_id: str) -> None:
            await asyncio.sleep(0.08)

        ctx: dict = {"redis": mock_redis}
        await quick_task(ctx, "job789")

        psetex_calls = mock_redis.psetex.call_args_list
        job_lock_calls = [
            c for c in psetex_calls
            if c.args[0] == b"arq:in-progress:job789"
        ]
        assert len(job_lock_calls) >= 1
        lock_value = job_lock_calls[0].args[2]
        assert lock_value.startswith(b"worker:")
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_heartbeat.py::TestWithHeartbeat -v`
Expected: FAIL with `AttributeError: module has no attribute 'with_heartbeat'`

**Step 3: Write the implementation**

Add to `src/resonance/heartbeat.py`:

```python
import asyncio
import functools
from typing import Any

import structlog

logger = structlog.get_logger()

_LOCK_KEY_PREFIX = b"arq:in-progress:"
_WORKER_KEY_PREFIX = "arq:worker:"


def with_heartbeat(
    fn: Any = None,
    *,
    interval: float = 30.0,
    ttl: float = 60.0,
) -> Any:
    """Decorator that refreshes an arq job's in-progress lock while it runs.

    Spawns a background asyncio task that periodically refreshes the
    ``arq:in-progress:{job_id}`` key with a short TTL and the worker's
    identity as the value.  Also refreshes the worker registry key.

    The heartbeat is cancelled in a ``finally`` block so it stops on
    success, error, or cancellation.

    Can be used bare (``@with_heartbeat``) or with arguments
    (``@with_heartbeat(interval=15, ttl=30)``).

    Args:
        fn: The async task function to wrap.
        interval: Seconds between heartbeat refreshes.
        ttl: TTL in seconds for the lock key.
    """
    def decorator(func: Any) -> Any:
        @functools.wraps(func)
        async def wrapper(ctx: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
            redis = ctx["redis"]
            worker_id = get_worker_id()
            worker_id_bytes = worker_id.encode()

            # Derive job_id from arq's context
            job_id: str = ctx.get("job_id", "")
            lock_key = _LOCK_KEY_PREFIX + job_id.encode()
            worker_key = _WORKER_KEY_PREFIX + worker_id
            ttl_ms = int(ttl * 1000)

            async def _heartbeat_loop() -> None:
                while True:
                    await asyncio.sleep(interval)
                    try:
                        await redis.psetex(lock_key, ttl_ms, worker_id_bytes)
                        await redis.psetex(worker_key, ttl_ms, b"1")
                    except Exception:
                        logger.warning(
                            "heartbeat_refresh_failed",
                            job_id=job_id,
                            worker_id=worker_id,
                        )

            heartbeat_task = asyncio.create_task(_heartbeat_loop())
            try:
                return await func(ctx, *args, **kwargs)
            finally:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass

        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_heartbeat.py::TestWithHeartbeat -v`
Expected: PASS

**Step 5: Run linting and type checks**

Run: `uv run ruff check src/resonance/heartbeat.py && uv run mypy src/resonance/heartbeat.py`

**Step 6: Commit**

```bash
git add src/resonance/heartbeat.py tests/test_heartbeat.py
git commit -m "feat: add @with_heartbeat decorator for arq lock renewal

Refreshes arq in-progress lock keys with short TTLs (60s) and stores
worker identity in the lock value instead of b'1'.

Part of #48"
```

---

### Task 3: Worker registry (startup/shutdown + idle heartbeat)

**Files:**
- Modify: `src/resonance/heartbeat.py`
- Test: `tests/test_heartbeat.py`

**Step 1: Write the failing tests**

Add to `tests/test_heartbeat.py`:

```python
class TestWorkerRegistry:
    """Tests for register/unregister and idle heartbeat."""

    @pytest.mark.asyncio
    async def test_register_writes_worker_key(self) -> None:
        mock_redis = AsyncMock()
        await heartbeat_module.register_worker(mock_redis)

        worker_id = heartbeat_module.get_worker_id()
        expected_key = f"arq:worker:{worker_id}"
        mock_redis.psetex.assert_called_once_with(
            expected_key, 60000, b"1"
        )

    @pytest.mark.asyncio
    async def test_unregister_deletes_worker_key(self) -> None:
        mock_redis = AsyncMock()
        await heartbeat_module.unregister_worker(mock_redis)

        worker_id = heartbeat_module.get_worker_id()
        expected_key = f"arq:worker:{worker_id}"
        mock_redis.delete.assert_called_once_with(expected_key)

    @pytest.mark.asyncio
    async def test_idle_heartbeat_refreshes_worker_key(self) -> None:
        mock_redis = AsyncMock()
        task = heartbeat_module.start_idle_heartbeat(mock_redis, interval=0.05, ttl=1)
        await asyncio.sleep(0.12)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        worker_id = heartbeat_module.get_worker_id()
        expected_key = f"arq:worker:{worker_id}"
        refresh_calls = [
            c for c in mock_redis.psetex.call_args_list
            if c.args[0] == expected_key
        ]
        assert len(refresh_calls) >= 1
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_heartbeat.py::TestWorkerRegistry -v`
Expected: FAIL

**Step 3: Write the implementation**

Add to `src/resonance/heartbeat.py`:

```python
async def register_worker(
    redis: Any,
    *,
    ttl: float = 60.0,
) -> None:
    """Register this worker in the Redis worker registry.

    Writes a TTL'd key so other workers can detect whether this
    worker is alive.

    Args:
        redis: arq ArqRedis connection.
        ttl: TTL in seconds for the worker key.
    """
    worker_id = get_worker_id()
    key = _WORKER_KEY_PREFIX + worker_id
    await redis.psetex(key, int(ttl * 1000), b"1")
    logger.info("worker_registered", worker_id=worker_id)


async def unregister_worker(redis: Any) -> None:
    """Remove this worker from the Redis worker registry.

    Best-effort cleanup on graceful shutdown.

    Args:
        redis: arq ArqRedis connection.
    """
    worker_id = get_worker_id()
    key = _WORKER_KEY_PREFIX + worker_id
    await redis.delete(key)
    logger.info("worker_unregistered", worker_id=worker_id)


def start_idle_heartbeat(
    redis: Any,
    *,
    interval: float = 30.0,
    ttl: float = 60.0,
) -> asyncio.Task[None]:
    """Start a background task that keeps the worker registry key alive.

    This runs between jobs so the worker stays registered even when
    idle.  The task should be cancelled in the shutdown hook.

    Args:
        redis: arq ArqRedis connection.
        interval: Seconds between refreshes.
        ttl: TTL in seconds for the worker key.

    Returns:
        The asyncio Task (caller should cancel on shutdown).
    """
    worker_id = get_worker_id()
    key = _WORKER_KEY_PREFIX + worker_id
    ttl_ms = int(ttl * 1000)

    async def _loop() -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await redis.psetex(key, ttl_ms, b"1")
            except Exception:
                logger.warning("idle_heartbeat_refresh_failed", worker_id=worker_id)

    return asyncio.create_task(_loop())
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_heartbeat.py::TestWorkerRegistry -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/heartbeat.py tests/test_heartbeat.py
git commit -m "feat: add worker registry with idle heartbeat

Workers register in Redis on startup and refresh via a background
task. Other workers can check if a peer is alive before clearing
its locks.

Part of #48"
```

---

### Task 4: Stale lock cleanup on startup

**Files:**
- Modify: `src/resonance/heartbeat.py`
- Test: `tests/test_heartbeat.py`

**Step 1: Write the failing tests**

Add to `tests/test_heartbeat.py`:

```python
class TestStaleLockCleanup:
    """Tests for cleaning stale arq locks on startup."""

    @pytest.mark.asyncio
    async def test_clears_lock_from_dead_worker(self) -> None:
        """Locks from workers not in the registry are deleted."""
        mock_redis = AsyncMock()
        # One stale lock from a dead worker
        mock_redis.keys.return_value = [b"arq:in-progress:sync_range:abc123"]
        mock_redis.get.return_value = b"worker:dead-pod:999"
        mock_redis.exists.return_value = 0  # worker key does not exist

        cleaned = await heartbeat_module.cleanup_stale_locks(mock_redis)

        mock_redis.delete.assert_called_once_with(b"arq:in-progress:sync_range:abc123")
        assert cleaned == 1

    @pytest.mark.asyncio
    async def test_preserves_lock_from_live_worker(self) -> None:
        """Locks from workers still in the registry are kept."""
        mock_redis = AsyncMock()
        mock_redis.keys.return_value = [b"arq:in-progress:sync_range:abc123"]
        mock_redis.get.return_value = b"worker:live-pod:1"
        mock_redis.exists.return_value = 1  # worker key exists

        cleaned = await heartbeat_module.cleanup_stale_locks(mock_redis)

        mock_redis.delete.assert_not_called()
        assert cleaned == 0

    @pytest.mark.asyncio
    async def test_clears_legacy_lock_without_worker_id(self) -> None:
        """Locks with value b'1' (pre-heartbeat) are treated as stale."""
        mock_redis = AsyncMock()
        mock_redis.keys.return_value = [b"arq:in-progress:sync_range:abc123"]
        mock_redis.get.return_value = b"1"

        cleaned = await heartbeat_module.cleanup_stale_locks(mock_redis)

        mock_redis.delete.assert_called_once_with(b"arq:in-progress:sync_range:abc123")
        assert cleaned == 1

    @pytest.mark.asyncio
    async def test_no_locks_returns_zero(self) -> None:
        mock_redis = AsyncMock()
        mock_redis.keys.return_value = []

        cleaned = await heartbeat_module.cleanup_stale_locks(mock_redis)
        assert cleaned == 0
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_heartbeat.py::TestStaleLockCleanup -v`
Expected: FAIL

**Step 3: Write the implementation**

Add to `src/resonance/heartbeat.py`:

```python
async def cleanup_stale_locks(redis: Any) -> int:
    """Delete arq in-progress locks held by dead workers.

    Scans all ``arq:in-progress:*`` keys, reads the worker ID from
    each lock's value, and checks whether that worker is still
    registered.  Locks from unregistered workers (or legacy locks
    with value ``b'1'``) are deleted.

    Args:
        redis: arq ArqRedis connection.

    Returns:
        Number of stale locks cleaned up.
    """
    lock_keys: list[bytes] = await redis.keys(_LOCK_KEY_PREFIX + b"*")
    if not lock_keys:
        return 0

    cleaned = 0
    for lock_key in lock_keys:
        value: bytes | None = await redis.get(lock_key)
        if value is None:
            continue

        value_str = value.decode()
        if not value_str.startswith("worker:"):
            # Legacy lock (b'1') — no worker ID, treat as stale
            await redis.delete(lock_key)
            cleaned += 1
            logger.info(
                "cleaned_legacy_stale_lock",
                lock_key=lock_key.decode(),
            )
            continue

        # Check if the owning worker is still registered
        worker_key = _WORKER_KEY_PREFIX + value_str
        worker_alive = await redis.exists(worker_key)
        if not worker_alive:
            await redis.delete(lock_key)
            cleaned += 1
            logger.info(
                "cleaned_stale_lock",
                lock_key=lock_key.decode(),
                dead_worker=value_str,
            )

    if cleaned:
        logger.info("stale_lock_cleanup_complete", cleaned=cleaned)

    return cleaned
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_heartbeat.py::TestStaleLockCleanup -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/heartbeat.py tests/test_heartbeat.py
git commit -m "feat: add stale lock cleanup using worker registry

On startup, scans arq in-progress locks and deletes those held by
workers no longer in the registry. Handles legacy b'1' locks from
before the heartbeat system.

Part of #48"
```

---

### Task 5: Integrate into worker.py

**Files:**
- Modify: `src/resonance/worker.py`
- Modify: `tests/test_worker.py`

**Step 1: Update WorkerSettings to use `with_heartbeat` and lower timeouts**

In `src/resonance/worker.py`, add import and modify WorkerSettings:

```python
import resonance.heartbeat as heartbeat_module
```

Change the `functions` list (around line 846):

```python
functions: typing.ClassVar[list[typing.Any]] = [
    arq.func(heartbeat_module.with_heartbeat(plan_sync), timeout=3600),
    arq.func(heartbeat_module.with_heartbeat(sync_range), timeout=3600),
    arq.func(heartbeat_module.with_heartbeat(run_bulk_job), timeout=3600),
]
```

**Step 2: Update startup() to register worker and clean stale locks**

In `startup()` (around line 771), add after creating the redis connection and before `_reenqueue_orphaned_tasks`:

```python
# Register this worker and clean up stale locks from dead workers
await heartbeat_module.register_worker(wctx["redis"])
wctx["_idle_heartbeat"] = heartbeat_module.start_idle_heartbeat(wctx["redis"])
cleaned = await heartbeat_module.cleanup_stale_locks(wctx["redis"])
if cleaned:
    logger.info("startup_cleaned_stale_locks", count=cleaned)
```

**Step 3: Update shutdown() to unregister worker and cancel idle heartbeat**

In `shutdown()` (around line 816), add before engine disposal:

```python
# Cancel idle heartbeat and unregister from worker registry
idle_heartbeat = wctx.get("_idle_heartbeat")
if idle_heartbeat is not None:
    idle_heartbeat.cancel()
    try:
        await idle_heartbeat
    except asyncio.CancelledError:
        pass

await heartbeat_module.unregister_worker(wctx["redis"])
```

Add `import asyncio` to the imports at the top of worker.py if not already present.

**Step 4: Update tests in test_worker.py**

Update `TestWorkerSettings.test_functions_registered` — the coroutines will be the decorated versions. The test should check that the functions are present (the decorator wraps them, so check by name or just verify count):

```python
def test_functions_registered(self) -> None:
    funcs = worker_module.WorkerSettings.functions
    assert len(funcs) == 3
    names = {f.name for f in funcs}
    assert names == {"plan_sync", "sync_range", "run_bulk_job"}
```

Update `test_job_timeout` if it checks the per-function timeout (verify 3600 instead of 86400).

**Step 5: Run all tests**

Run: `uv run pytest tests/test_worker.py tests/test_heartbeat.py -v`
Expected: PASS

**Step 6: Run full quality checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`

**Step 7: Commit**

```bash
git add src/resonance/worker.py src/resonance/heartbeat.py tests/test_worker.py tests/test_heartbeat.py
git commit -m "feat: integrate heartbeat system into arq worker

- Task functions wrapped with @with_heartbeat for lock renewal
- Worker registers/unregisters in Redis on startup/shutdown
- Idle heartbeat keeps worker registered between jobs
- Stale locks cleaned on startup before orphan re-enqueue
- Function timeouts reduced from 86400 to 3600 (heartbeat handles the rest)

Fixes #48"
```

---

### Task 6: Final verification

**Step 1: Run full test suite**

Run: `uv run pytest -v`

**Step 2: Run quality checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 3: Verify the plan file is not staged**

The plan file (`docs/plans/2026-04-20-worker-heartbeat-registry.md`) should NOT be committed — it lives in the repo's `docs/plans/` but is a planning artifact.

**Step 4: Create PR or merge directly**

Based on user preference. PR title: "Add worker heartbeat and registry to prevent stale arq locks"
Reference: Fixes #48
