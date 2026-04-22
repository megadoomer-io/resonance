# Worker Architecture: Hierarchical Task Queue

## Overview

Replace the current `asyncio.create_task()` sync runner with an arq-based worker system. Tasks are organized hierarchically (sync_job → time_range → page_fetch) using a single self-referential `SyncTask` model. Workers run as a separate Kubernetes deployment using the same Docker image with a different entrypoint.

## Goals

- Sync jobs survive pod restarts
- Failed ranges can be retried individually without re-running the entire sync
- Incremental sync: subsequent syncs only fetch new data
- Scales to multiple worker replicas if needed
- Simple UX: user sees overall progress, can retry failures with one click

## Tech Stack

- **arq** — async-native task queue built on Redis
- **Redis** — already deployed, used as arq broker
- **Same Docker image** — web uses uvicorn, worker uses `arq resonance.worker.WorkerSettings`

## Data Model

### SyncTask (replaces SyncJob)

Single model with self-referential parent/child relationships:

```
SyncTask
  id (UUID, PK)
  user_id (FK users)
  service_connection_id (FK service_connections)
  parent_id (FK sync_tasks, nullable — NULL = top-level sync)
  task_type (enum: sync_job, time_range, page_fetch)
  status (enum: pending, running, completed, failed)
  params (JSON — chunking strategy params, pagination params, etc.)
  result (JSON — items_created, items_updated, skipped info, etc.)
  error_message (Text, nullable)
  progress_current (Integer)
  progress_total (Integer, nullable)
  created_at, started_at, completed_at (DateTime)
```

**Relationships:**
- `children: list[SyncTask]` — child tasks
- `parent: SyncTask | None` — parent task

**Indexes:**
- `(user_id, status)` — dashboard queries
- `(parent_id, status)` — child completion checks
- `(service_connection_id, task_type, status)` — incremental sync watermark

### Migration

- Alembic migration to create `sync_tasks` table
- Migrate existing `sync_jobs` data (optional — could just start fresh)
- Drop `sync_jobs` table after migration

## Task Hierarchy

### ListenBrainz Full Sync Example (123K listens)

```
SyncTask (type=sync_job, parent=None)
├── SyncTask (type=time_range, params={min_ts=1711900000, max_ts=1714500000})
│   ├── SyncTask (type=page_fetch, params={max_ts=1714500000, count=100})
│   ├── SyncTask (type=page_fetch, params={max_ts=1714490000, count=100})
│   └── ... (~10 pages per range)
├── SyncTask (type=time_range, params={min_ts=1709300000, max_ts=1711900000})
│   └── ...
└── ... (~125 time range tasks targeting ~1000 listens each)
```

### Spotify Sync

Spotify's API doesn't support time-range pagination the same way. The chunking strategy is per-connector:

- **Followed artists:** Single task, paginated by cursor
- **Saved tracks:** Single task, paginated by offset
- **Recently played:** Single task, max 50 items (no pagination needed)

Each becomes a `time_range`-equivalent task under the top-level `sync_job`.

## arq Task Functions

### `plan_sync(ctx, sync_task_id)`

Called when user triggers a sync. Runs as the top-level orchestrator:

1. Load the SyncTask from DB
2. Get the connector for this service
3. Determine chunking strategy based on connector type:
   - **ListenBrainz:** Query listen count, compute time range boundaries targeting ~1000 listens per chunk, create child SyncTasks
   - **Spotify:** Create one child task per data type (artists, tracks, recent)
4. Enqueue each child task as `sync_range`
5. Mark top-level task as RUNNING

### `sync_range(ctx, sync_task_id)`

Executes a single time range or data type:

1. Load the SyncTask and its params
2. Paginate through the API (creating page_fetch children as it goes)
3. Upsert data into DB
4. Mark task as COMPLETED (or FAILED with error details)
5. Check if all siblings are done → if so, mark parent as COMPLETED

### Task completion cascade

When a child task completes:
1. Query `SELECT count(*) FROM sync_tasks WHERE parent_id = ? AND status NOT IN ('completed')`
2. If 0 remaining → mark parent as COMPLETED
3. If parent has failed children → mark parent's result with partial completion info
4. Cascade up the tree

## Worker Configuration

### `resonance/worker.py`

```python
from arq import cron
from arq.connections import RedisSettings

class WorkerSettings:
    functions = [plan_sync, sync_range]
    redis_settings = RedisSettings(...)  # from app config
    max_jobs = 10  # concurrent tasks per worker
    job_timeout = 300  # 5 min per task (individual pages, not full syncs)
```

### WORKER_MODE

- `WORKER_MODE=external` (default, production): Web app only enqueues tasks. Separate worker pod processes them.
- `WORKER_MODE=inline` (development): Web app starts an arq worker in a background thread on startup. Single process, easy to debug.

## Kubernetes Deployment

Same Docker image, second controller in app-template:

```yaml
controllers:
  worker:
    containers:
      main:
        image:
          repository: app
          tag: latest
        command: ["arq", "resonance.worker.WorkerSettings"]
        # Same env vars as web app (DB, Redis, secrets)
        env: ...
        envFrom:
          - secretRef:
              name: resonance-app-secrets
        # No HTTP probes — arq has its own health check
        # Or use a simple exec probe: arq --check resonance.worker.WorkerSettings
        resources:
          limits:
            cpu: 200m
            memory: 512Mi
```

## Incremental Sync

### First sync

`plan_sync` creates ~125 time range tasks covering the full listen history.

### Subsequent syncs

`plan_sync` checks the most recent completed page_fetch task's timestamp for this service connection. Creates a single time range task: `last_completed_timestamp → now`.

### Retry failed ranges

User clicks "Retry missing ranges." The UI creates new child tasks only for previously failed time ranges (using the `params` from the failed tasks) and enqueues them.

## UX

### Dashboard

- Shows top-level sync status (same as today): progress bar, service name, timestamps
- Progress calculated from children: `completed_children / total_children`

### Partial completion

- If any children failed: "Some data couldn't be fetched. [Retry missing ranges]"
- Button creates new tasks for failed ranges only

### View details (optional, expandable)

- Shows which time ranges succeeded/failed
- Failed ranges show error message and timestamp boundaries
- Useful for bug reports

## Rate Limiting

The existing `RateLimitBudget` stays as-is. Each worker processes tasks sequentially within a service (arq's `max_jobs` handles concurrency). The budget manager on each connector instance paces requests.

For multiple workers hitting the same service: the budget manager is per-process, so two workers could double the request rate. Mitigation: set `max_jobs=1` for sync tasks, or use a shared Redis-backed rate limiter (future enhancement).

## Not In Scope

- Scheduled/recurring syncs (cron-based — future, easy to add with arq's cron support)
- Multi-worker rate limit coordination (single worker is fine for now)
- Real-time WebSocket progress updates (future, complements this work)
- Spotify chunking optimization (simple single-task approach works for now)
