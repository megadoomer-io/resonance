# Goal-Oriented Sync Orchestration Design

Redesign sync orchestration to use pluggable, per-connector strategy classes
instead of hardcoded per-service functions in the worker. Co-designed with
incremental sync (#13) and deferred arq jobs (#18).

Related issues: #17, #13, #18, #20, #15

## Problem

The worker (`worker.py`) contains hardcoded `if/elif` branches for each
service in both `plan_sync` (planning) and `sync_range` (execution). Each
service has bespoke `_plan_*` and `_run_*` functions. Adding a new service
means adding more conditionals and duplicating orchestration patterns.

Additionally:
- Rate limit backoffs block a worker slot (sleeping for hours on 429s)
- No way to defer and resume a task after a long rate limit window
- Users can trigger overlapping syncs for the same service
- Progress reporting conflates page progress with item counts
- No user-facing task descriptions

## Design

### Core abstractions (`sync/base.py`)

A `SyncStrategy` ABC defines how a service plans and executes sync tasks.
Strategies live in `sync/` alongside the existing `runner.py` upsert functions.

```python
class SyncTaskDescriptor(pydantic.BaseModel):
    """Lightweight description of a child task to create."""
    task_type: SyncTaskType
    params: dict[str, object]
    progress_total: int | None = None
    description: str = ""

class DeferRequest(Exception):
    """Raised by execute() when rate-limited beyond acceptable wait time."""
    def __init__(self, retry_after: float, resume_params: dict[str, object]) -> None:
        self.retry_after = retry_after
        self.resume_params = resume_params

class SyncStrategy(abc.ABC):
    """Defines how a service plans and executes sync tasks."""

    concurrency: str  # "sequential" or "parallel"

    @abc.abstractmethod
    async def plan(
        self,
        session: AsyncSession,
        connection: ServiceConnection,
        connector: BaseConnector,
    ) -> list[SyncTaskDescriptor]:
        """Return child task descriptors for this sync job.

        Receives the session to query watermarks and other state, but only
        returns data -- does not create database rows.
        """
        ...

    @abc.abstractmethod
    async def execute(
        self,
        session: AsyncSession,
        task: SyncTask,
        connector: BaseConnector,
    ) -> dict[str, object]:
        """Execute a single child task.

        Receives the session to upsert data and update progress. Returns
        a result dict. May raise DeferRequest to pause and resume later.
        """
        ...
```

### Key design decisions

**Strategies are separate from connectors.** Connectors are pure API clients
(HTTP, auth, parsing). Strategies own sync orchestration (task decomposition,
watermarks, concurrency, deferral). A connector can exist without a strategy
(e.g., a service you can authenticate with but haven't built sync for yet).

**Strategies are registered in a plain dict**, not a registry class. The lookup
is always `ServiceType -> SyncStrategy`, one-to-one. Built in worker
`startup()` and stored in the arq context as `ctx["strategies"]`.

**`plan()` returns data, worker persists.** The strategy returns a list of
`SyncTaskDescriptor` objects. The worker creates `SyncTask` rows and handles
enqueuing based on the concurrency policy. This makes strategies easy to test:
give it inputs, assert on the returned descriptors, no database fixtures
needed for planning tests.

**`execute()` receives the session directly.** Execution is inherently
side-effectful -- paginating through an API, upserting per page, updating
progress. The strategy calls existing `sync/runner.py` upsert functions.
Testing can mock the session.

**Concurrency is a simple string attribute** (`"sequential"` or `"parallel"`).
The worker reads this when deciding whether to enqueue all children at once or
one-at-a-time via `_check_parent_completion`. Covers both existing services
and likely near-term additions.

**Strategies own watermark logic.** Each strategy's `plan()` method queries
whatever it needs to determine where to resume. Watermark semantics are
service-specific (unix timestamps for ListenBrainz, cursor/offset for Spotify,
none for recently_played).

**Deferral uses an exception pattern.** When `execute()` hits a rate limit
too long to wait for, it raises `DeferRequest` with `retry_after` and
`resume_params`. The worker catches it, sets the task to `DEFERRED`, stores
`deferred_until`, and enqueues a delayed arq job. The strategy never touches
arq directly.

### Model changes

**`types.py`** -- Add `DEFERRED` to `SyncStatus`:

```python
class SyncStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEFERRED = "deferred"
```

**`models/task.py`** -- Add two columns to `SyncTask`:

- `description: Mapped[str | None]` -- nullable text, user-facing task label
  (e.g., "Fetching your saved tracks")
- `deferred_until: Mapped[datetime | None]` -- nullable datetime with timezone,
  when a deferred task will be retried

**Alembic migration:**

- Add `description` and `deferred_until` columns
- Update the status CHECK constraint to include `deferred`

### Strategy implementations

**`sync/spotify.py` -- `SpotifySyncStrategy`**

```
concurrency = "sequential"
```

- `plan()`: Creates three descriptors:
  - `"Fetching your followed artists"` (data_type: followed_artists)
  - `"Fetching your saved tracks"` (data_type: saved_tracks)
  - `"Fetching your recent plays"` (data_type: recently_played)
  - Decrypts access token and includes in params
  - No watermark logic initially (Spotify's API doesn't support incremental
    well -- issue #20 will refine this)
- `execute()`: Dispatches by `data_type` param. Calls connector fetch methods,
  upserts via `sync/runner.py`. On `RateLimitExceededError`, raises
  `DeferRequest` with current data_type and progress offset as resume_params.

**`sync/listenbrainz.py` -- `ListenBrainzSyncStrategy`**

```
concurrency = "parallel"
```

- `plan()`: Queries watermark (most recent `listened_at` from completed tasks
  for this connection). Creates one descriptor:
  - `"Syncing listening history"` (or `"Syncing new listens since <date>"`)
  - Sets `min_ts` to watermark, fetches listen count for `progress_total`
- `execute()`: Paginates through listens using `max_ts`/`min_ts`, upserts per
  page, updates `task.progress_current`. Tracks `max_ts` cursor for
  `resume_params` on deferral. Writes `last_listened_at` to `task.result` for
  future watermark queries.

### Worker changes

**`plan_sync`** becomes a generic dispatcher:

1. Load task and connection (unchanged)
2. Look up strategy from `ctx["strategies"]`
3. Check for existing non-terminal task for this connection (sync guard)
4. Call `strategy.plan(session, connection, connector)` to get descriptors
5. Create `SyncTask` rows from descriptors (with description, progress_total)
6. Enqueue based on `strategy.concurrency`:
   - `"parallel"`: enqueue all children
   - `"sequential"`: enqueue only the first child

**`sync_range`** adds deferral handling:

1. Load task and connection (unchanged)
2. Look up strategy
3. Call `strategy.execute(session, task, connector)`
4. On success: set `COMPLETED`, store result
5. On `DeferRequest`: set `DEFERRED`, merge resume_params into task.params,
   set `deferred_until`, enqueue delayed arq job with `_defer_by`
6. On other exception: set `FAILED` (unchanged)
7. Call `_check_parent_completion` (unchanged, but DEFERRED is non-terminal)

**`_check_parent_completion`** -- `DEFERRED` added to non-terminal status list.
A deferred sibling blocks parent completion and next-sibling enqueuing.

**Deleted functions:**

- `_plan_listenbrainz_sync`
- `_plan_spotify_sync`
- `_run_listenbrainz_range`
- `_run_spotify_range`
- `_get_watermark`

### Sync guard and cancellation

**API sync endpoint** -- Before creating a new `SYNC_JOB`, query for existing
tasks on this connection with status in (`PENDING`, `RUNNING`, `DEFERRED`).
If found, return HTTP 409 with the existing task's ID and status.

**Cancel endpoint** -- `POST /api/v1/sync/{task_id}/cancel`:

- Loads the task, verifies it belongs to the requesting user
- Sets the task and all non-terminal children to `FAILED` with
  `error_message = "Cancelled by user"`, `completed_at = now`

**UI** -- Sync button per service becomes conditional:

- No active task: "Sync" button
- `PENDING`/`RUNNING`: status text with task description + "Cancel" button
- `DEFERRED`: "Rate limited -- retrying at HH:MM" + "Cancel" button

### Progress reporting

Unified single progress display per task:

- When `progress_total` is known: progress bar with
  `"description -- N of M"` and estimated time remaining
- When `progress_total` is unknown: running count with `"description -- N so far"`
- ETA computed at render time from `started_at`, `progress_current`, and
  `progress_total` (only shown when total is known)
- Item counts (`items_created`, `items_updated`) shown only in the completed
  result summary, not during active sync

## File changes summary

**New files:**

- `sync/base.py` -- SyncStrategy ABC, SyncTaskDescriptor, DeferRequest
- `sync/spotify.py` -- SpotifySyncStrategy
- `sync/listenbrainz.py` -- ListenBrainzSyncStrategy
- Alembic migration for description, deferred_until, and status constraint

**Modified files:**

- `types.py` -- Add DEFERRED status
- `models/task.py` -- Add description and deferred_until columns
- `worker.py` -- Replace per-service functions with strategy dispatch
- `api/v1/sync.py` -- Add sync guard (409) and cancel endpoint
- UI templates -- Conditional sync/cancel button, unified progress, ETA

**Unchanged:**

- `sync/runner.py` -- All upsert functions stay as-is
- `connectors/*` -- All connector classes stay as pure API clients
- `connectors/registry.py` -- No changes
- `_load_task` helper -- Unchanged

## Future extension points

These are noted to avoid designing ourselves into a corner. Not built now.

1. **`finalize()` hook on SyncStrategy** -- Post-completion logic after all
   children finish (e.g., summary stats, cleanup). Can be added as a default
   no-op on the ABC without breaking existing strategies.

2. **Custom enqueuing policies** -- If a future service needs more than
   sequential/parallel (e.g., "2 at a time", "conditional on previous
   result"), the concurrency attribute can evolve into a richer type.

3. **Watermark override / backfill** -- A "sync from date X" option that
   passes an override timestamp to `strategy.plan()`, bypassing the stored
   watermark. Useful for re-scanning historical data after a ListenBrainz
   import.

4. **Cross-service entity resolution** -- A higher-level orchestration phase
   that runs after all per-service syncs complete, deduplicating
   artists/tracks across services by MBID or name matching.

5. **OpenAPI spec enrichment** -- Invest in Pydantic response models and route
   docstrings so FastAPI's auto-generated spec is comprehensive.
