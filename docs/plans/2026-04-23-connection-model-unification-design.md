# Connection Model Unification Design

**Goal:** Unify `ServiceConnection` (OAuth) and `UserCalendarFeed` (calendar feeds) into a single `Connection` abstraction, fix calendar sync task lifecycle bugs, and eliminate duplicated UI code paths.

**GitHub Issue:** #53

---

## Problem

`ServiceConnection` and `UserCalendarFeed` represent the same user-facing concept ("connected service") but use different models, API paths, sync mechanisms, and task lifecycle patterns. This causes:

- **Task lifecycle bugs** — calendar sync tasks don't get orphan recovery, deferred handling, or consistent status tracking because the worker code is hardcoded to `SYNC_JOB`/`TIME_RANGE` types
- **arq dedup collisions** — calendar sync uses `feed_id`-based job IDs that collide across syncs of the same feed, causing jobs to be silently dropped
- **Duplicated UI logic** — dashboard and account routes query/render two separate models with separate grouping logic
- **NULL service_connection_id** — calendar sync tasks have no connection FK, breaking joins and queries that assume it exists

## Design Decisions

### Unified Connection Model (Single Table)

The `service_connections` table is widened to absorb `user_calendar_feeds`. No discriminator column — `service_type` already identifies the connection kind, and each connector declares its own authentication style.

**New columns added to `service_connections`:**
- `url` (String, nullable) — for generic iCal feeds; Songkick URLs derived from `external_user_id`
- `label` (String, nullable) — user-facing label for feed connections
- `enabled` (Boolean, default True) — explicit enable/disable for all connections
- `last_synced_at` (DateTime, nullable) — replaces `last_used_at`; "last synced" is the meaningful concept for all types

**Existing columns reused:**
- `external_user_id` — Spotify user ID for OAuth, Songkick username for feeds
- `sync_watermark` — available for future feed watermarking, NULL for now
- `service_type` — already includes `songkick` and `ical`

**Songkick connections are one row per username.** The two feed URLs (attendance + tracked_artist) are derived from `external_user_id`. This eliminates the grouping-by-username logic in routes and templates.

**Future consideration:** If many service types accumulate with divergent type-specific fields, migrate to table inheritance (base `connections` + child tables for type-specific columns). For now, the nullable column approach is sufficient.

### Type-Agnostic Task Lifecycle

The task tree is generic — any task can be a root or child, a leaf or branch. Lifecycle management works identically for all tasks:

- **Leaf tasks** (no children) do actual work
- **Branch tasks** (has children) aggregate progress and complete when all children finish
- **Root leaf tasks** (no parent, no children) are standalone — they do work and are done
- **`task_type`** tells the worker what function to call, not how to manage lifecycle

**Parent completion helper** — extracted from inline Spotify sync logic into a shared function:

```python
async def complete_task(session, task, result):
    task.status = SyncStatus.COMPLETED
    task.result = result
    task.completed_at = datetime.now(UTC)
    if task.parent_id is not None:
        await _check_parent_completion(session, task.parent_id)
```

**Orphan recovery** — `_reenqueue_orphaned_tasks` drops the `task_type.in_([SYNC_JOB, TIME_RANGE])` filter. Any task stuck in PENDING/RUNNING gets recovered. Re-enqueue uses a registry mapping `task_type → arq_job_name`.

**arq dedup fix** — all jobs use `_job_id=f"{job_name}:{task_id}"` (unique per task), matching the pattern ServiceConnection syncs already use.

**service_connection_id** — calendar sync tasks now set this FK (pointing to the unified Connection row) instead of leaving it NULL.

### Connector-Declared Configuration

Each connector declares its own connection config rather than the worker/routes hardcoding behavior per type:

```python
@dataclass
class ConnectionConfig:
    auth_type: str           # "oauth", "username", "url"
    sync_function: str       # arq job name
    sync_style: str          # "incremental" or "full"
    derive_urls: Callable | None  # username → feed URLs (optional)
```

This means:
- Adding a new service only requires writing a connector
- Service auth changes (e.g., Songkick adds OAuth) are connector changes, not schema migrations
- Worker dispatch uses `connection.service_type → connector → sync_function` instead of if/else branches

### Unified UI

Dashboard and account pages collapse to one query/render path:

- One `connections` query per route
- One `active_syncs` dict keyed by `connection.id`
- One template loop rendering all connections
- One sync trigger endpoint: `POST /api/v1/sync/{connection_id}`
- One disconnect endpoint: `DELETE /api/v1/connections/{connection_id}`

The sync status partial drops `if task_type == calendar_sync` branching — result formatting uses the connector config or renders `task.result` generically.

## Migration Phases

### Phase 1: Schema + Data Migration

- Add new columns to `service_connections` (`url`, `label`, `enabled`, `last_synced_at`)
- Rename `last_used_at` → `last_synced_at` (or keep both during transition)
- Migrate `user_calendar_feeds` rows into `service_connections`:
  - One row per Songkick username (collapse two feed rows)
  - One row per generic iCal feed
- Update `sync_tasks.service_connection_id` for existing CALENDAR_SYNC tasks
- Both old and new models coexist; app code still reads from old models

### Phase 2: Task Lifecycle + Worker

- Extract parent completion helper
- Make orphan recovery type-agnostic
- Fix arq dedup to use task_id-based job IDs for calendar sync
- Add `ConnectionConfig` to connectors
- Update worker dispatch to use connector registry
- Calendar sync tasks now set `service_connection_id`

### Phase 3: UI Cutover + Cleanup

- Unify dashboard/account routes to query `Connection` only
- Collapse templates to single loops
- Unify sync trigger endpoint
- Remove or redirect calendar feed API endpoints
- Drop `user_calendar_feeds` table
- Remove `FeedType` enum

Each phase ships to main, deploys, and gets validated before the next begins.
