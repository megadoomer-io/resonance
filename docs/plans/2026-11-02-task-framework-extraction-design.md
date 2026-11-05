# Task Framework Extraction: Design

**Date:** 2026-11-02 (revised 2026-11-04)
**Status:** Reliability-first slice approved for build (five PRs, below). The full framework
is the target design, deferred until a queue spike decides whether to build it, adopt a
library, or drop it (see `TODOS.md`).

## Summary

Resonance's background work runs on arq, but the reliability layer around arq is
written into the job functions themselves. Status transitions, deferral, cancellation
checks, parent/child completion and crash recovery are repeated across a 3,363-line
`worker.py` and tied to the `Task` model and resonance's `TaskType` values.

This design separates the two concerns:

- **A task framework** (`resonance.taskkit`) that owns durability, leasing, cancellation,
  deferral, resumable checkpoints, fan-out, recovery and scheduling. It knows nothing
  about music, connectors or users.
- **Business handlers** that only do the work: sync a range, import a CSV, build a
  playlist. A handler receives a context, does its job, and returns, raises, defers or
  checkpoints through that context.

The goal is a framework another service can lift. The immediate motivation is Joey (a
Kubernetes controller at work) which will eventually need longer-running and scheduled
jobs. Lifting is not part of this work, but the import boundary is designed so it stays
possible.

## Motivation

What the coupling costs today, with evidence:

| Problem | Where |
|---|---|
| Cancellation is encoded as `status=FAILED` plus the string "Cancelled" in `error_message` | `sync/lifecycle.py:96` (`_is_cancel_status`) |
| Every job function does its own status bookkeeping and calls `_check_parent_completion` by hand | `worker.py:341`, `:444`, `:1180`, `:1211`, `:1314` |
| Deferral is hand-rolled per job: set DEFERRED, merge resume params, re-enqueue with `_defer_by` | `worker.py:395-406`, `:1231-1241` |
| Three parallel registries must stay in sync: `_TASK_DISPATCH`, `WorkerSettings.functions`, and the enqueue call sites | `worker.py:89`, `:3305`; 26 `enqueue_job` calls across `api/v1/` and the workers |
| Recovery runs only at worker startup, so a job lost while the worker stays up is never reclaimed | `worker.py:3031` (`_reenqueue_orphaned_tasks`) |
| Stale-lock cleanup scans with `KEYS` | `heartbeat.py:148` |
| No scheduled jobs exist, so recurring work has no home | Issues #120 (background crawler), #149 (auto-run backfills after sync) |
| No first-class cancel for long jobs | Issue #173 (cancel a stuck Spotify export) |

The heartbeat module (`heartbeat.py`, ~180 lines) is the one piece already written
generically. It exists because arq has no visibility timeout and its in-progress lock
lasts about a day (issue #48).

## Build now: reliability-first slice

Every stuck-task bug found so far is fixed by a smaller change than the full framework.
These five PRs ship first; the framework sections after this one are deferred.

### arq behavior the slice depends on

- `enqueue_job` with a job id that already exists is silently dropped while the job or its
  result key is retained (results are kept for 3600s). The in-job defer re-enqueues reuse
  the running job's id, so a deferred task can vanish.
- On SIGTERM, arq cancels in-flight jobs before `on_shutdown` runs, and with
  `retry_jobs=True` the cancelled job stays queued under the same id and runs again on the
  next worker.
- `is_cancelled` is checked only at job entry (14 entry points). Nothing in `sync/` or
  `connectors/` checks it mid-run, so cancelling a RUNNING task today is overwritten when
  the run finishes.
- `Job.abort` waits for the job's result by default; callers must pass a short timeout.

### PR1: status-partial authorization

`export_status_partial` and `generating_status_partial` (`ui/playlists.py`) gain
`require_user`, filter tasks by `Task.user_id`, and skip malformed task ids. Tests cover
unauthenticated, other-user and malformed-id requests for both.

### PR2: unique ids on deferral re-enqueues

The two in-job defer re-enqueues (`worker.py`, in sync_range and discover_tracks)
use a unique job id so arq cannot drop them. Adds an `integration` pytest marker, Postgres
and Redis services to the lint-and-test workflow, and a real-Redis test of the deferral.

### PR3: characterization tests

Three new worker tests pin behavior PR4 and PR5 must preserve: step_mode pause, stale-child
cleanup, and a parent with children not being re-planned on recovery. Existing tests
already cover `started_at` preservation, TIME_RANGE watermark resume and sync_range
RUNNING-retry resume.

### PR4: CANCELLED, cancel, export idempotency

- A real `CANCELLED` state, plus shared TERMINAL and ACTIVE status sets used at every
  status check (including `concert_archives.py` and `playlist_export.py`).
- Migration: widen the CHECK constraint first, then rewrite legacy "FAILED + Cancelled"
  rows to `CANCELLED`.
- The existing user-scoped `POST /api/v1/sync/cancel/{task_id}` sets `CANCELLED` and
  cancels the task's non-terminal children. Adds `resonance-api cancel <task_id>`.
- A terminal-row guard at all 14 job entry points: a delivery whose row is already
  terminal logs `duplicate_delivery` and exits.
- Export persists the Spotify playlist id immediately after creating it, so a retry reuses
  the playlist instead of creating a second one.
- `plan_sync` skips planning when the parent already has children. Today that guard exists
  only in startup recovery, so an arq re-delivery of `plan_sync` creates duplicate
  children; the entry guard doesn't catch it because the parent is RUNNING, not terminal.
- Delete the unused `sync/lifecycle._check_parent_completion` and its tests; the worker's
  version is the only live one.
- No arq abort yet. Cancelling a RUNNING task through the API, CLI or dashboard still flips
  back when the run finishes (existing behavior, documented in the PR); PR5 fixes it.

UI in PR4:

- **Stop per Spotify connection.** Each in-progress export row on the playlist detail page
  and the exporting page has its own Stop control, shown only for PENDING or DEFERRED
  tasks. A RUNNING row shows "Exporting…" with "Can't be stopped once it has started."
- **Stop posts to a UI route,** `POST /playlists/{playlist_id}/export/{task_id}/cancel`,
  which uses the same cancel logic as the API and returns that connection's row partial.
  Outcomes: "Stopped"; the row's real state plus "This export had already finished."; or
  the button stays with an inline "Couldn't stop the export. Try again." No toast. One row
  partial serves both pages.
- **Exporting page layout.** A stable frame (playlist name, one row per connection, an
  always-visible "← Back to playlist" link). Only each row's status span polls, via
  `hx-swap-oob`; buttons never sit inside a replaced region.
- **Terminal rule.** Polling stops when every task is completed, failed or cancelled.
  Rows read "Exported · N tracks · Open in Spotify" (with "M not found on Spotify" when
  tracks were skipped), "Stopped" (muted), or "Failed: <error>". Headline precedence:
  any failed, then any stopped, then complete. No tasks found renders "Export not found"
  with a back link and no polling.
- **Other pollers.** The generating partial, the CLI's `_poll_task` and the lineup
  builder's enrich poll treat any status outside pending/running/deferred as terminal.
  Cancelled copy: "Generation was cancelled." with "Start over"; "Enrichment was
  cancelled."; the CLI prints CANCELLED and exits 1. Unknown terminal statuses render like
  failed, showing the raw status.
- **Accessibility.** The rows container is `aria-live="polite"`; only status spans change.
  After Stop, focus moves to the row's status element. The generating page gets the same
  stable live region.
- **Sync history Cancel** stops swapping the JSON response into `#sync-status`: it uses
  `hx-swap="none"`, reloads the status on success, and shows an inline error otherwise.

### PR5: reaper, startup recovery, hard cancel, health

- `enqueued_at` and `arq_job_id` columns, stamped in the same transaction that inserts or
  advances the row, with a partial index built `CONCURRENTLY` (dropping any invalid index
  of the same name first).
- An `enqueue_task` helper replaces the 26 direct `enqueue_job` calls and gives every
  delivery a unique job id.
- **Reaper** (arq cron, `unique=True`): re-delivers PENDING and DEFERRED rows only when
  arq reports their job as not found or complete, and re-reads the row inside its
  transaction before delivering. Siblings and step_mode children that were never enqueued
  are left alone, as are children whose parent is already terminal (today's stale-child
  cleanup handles only PENDING children, so a DEFERRED orphan would otherwise be
  re-delivered). Startup recovery calls the same function.
- **Startup RUNNING recovery** checks `Job.status(arq_job_id)` first. Queued, deferred or
  in-progress jobs are left for arq's own SIGTERM retry; otherwise every deploy would
  double-run interrupted tasks.
- Delete the `app.py` lifespan block that resets every RUNNING row to PENDING on each API
  pod start (it discards resume state and races the worker).
- **Hard cancel:** `allow_abort_jobs`, and cancel aborts `row.arq_job_id` with a short
  timeout. Stop appears on RUNNING export rows, confirming "Stop export? Tracks already
  added stay on Spotify."
- `GET /api/v1/admin/tasks/health` and `resonance-api tasks health`: reaper last tick
  (STALE past twice the cron interval), oldest pending task, zero-filled counts per status,
  running tasks with age and progress, and recent reaper decisions. `--json` prints the raw
  response; 403 prints "Admin role required".

### Order and known gaps

PR1 and PR2 are independent and run in parallel; PR3 follows PR2 (integration marker). PR4
and PR5 both touch `worker.py`, `cli.py` and the alembic head, so they land in sequence.

Accepted until the post-spike decision: if two deliveries of one task are both RUNNING (a
stalled but alive worker), the task runs twice. The entry guard covers only rows that are
already terminal; startup and the guard log the case, so it is not silent. The
`ShutdownRequest` checkpoint never fires on SIGTERM (see the arq notes above) and is tracked
in `TODOS.md`.

## Target design (deferred until the queue spike)

The rest of this document is the full framework. It is not scheduled; `TODOS.md` records
what the spike compares (procrastinate, PgQueuer, DBOS, and arq with this layer) and what
gets re-decided.

### Principle: the task row is the truth, arq is the transport

Postgres already holds the durable record (`sync_tasks`). Keep that, and make it the
framework's contract: a task exists when its row exists, and its state is the row's state.
arq only delivers "run task X now / at time T". If an arq job is lost, the row still says
the task is owed, and recovery re-delivers it. This is already how recovery works; the
design makes it the only rule rather than a startup special case.

### Layout and the import boundary

```
src/resonance/taskkit/        # framework: no imports from resonance.* outside taskkit
  model.py                    # TaskState enum, TaskRecord protocol
  store.py                    # TaskStore protocol (claim, heartbeat, transition, checkpoint)
  registry.py                 # @handler registration
  context.py                  # TaskContext passed to handlers
  runner.py                   # the single arq entry point that wraps every handler
  recovery.py                 # periodic reaper
  schedule.py                 # cron registration
  stores/postgres.py          # SQLAlchemy adapter over sync_tasks
src/resonance/jobs/           # business handlers, split by domain (sync, concerts, playlists, backfill)
```

Enforce the boundary mechanically (an import-linter contract, or ruff `banned-api` scoped
to `taskkit/`) so `taskkit` cannot import `resonance.models`, `resonance.connectors`,
settings, or sessions. The Postgres adapter is the only place that knows the table, and it
receives a session factory and a mapped class by injection. This boundary is what makes the
package liftable later; without the lint rule it will erode.

### State machine

```
PENDING -> RUNNING -> COMPLETED
                   -> FAILED
                   -> DEFERRED -> PENDING (when due)
                   -> CANCELLED
PENDING/DEFERRED   -> CANCELLED
```

- Add a real `CANCELLED` state and a `cancel_requested_at` column. Migrate existing rows
  that match the "FAILED + 'Cancelled'" convention.
- Transitions happen only in `runner.py` and `store.py`. Handlers never set status.

### Handler contract

```python
@handler("sync_range", timeout=3600, retry=Retry(max_attempts=3, backoff="exponential"))
async def sync_range(ctx: TaskContext[SyncRangeParams]) -> SyncRangeResult:
    for page in ...:
        await ctx.check_cancelled()          # raises Cancelled if requested (task or ancestor)
        ...
        await ctx.checkpoint({"watermark": ts})   # persisted; available as ctx.resume_state on retry
    if rate_limited:
        raise ctx.defer(retry_after=seconds)      # framework sets DEFERRED and schedules re-delivery
    return SyncRangeResult(...)
```

- **Typed params and results.** Each handler declares pydantic models for `params` and
  `result`; the framework validates on submit and on load. This replaces untyped
  `dict[str, object]` access in job bodies.
- **Outcomes map to states:** return to `COMPLETED`; `ctx.defer(...)` to `DEFERRED`;
  `Cancelled` to `CANCELLED`; any other exception to retry per policy, then `FAILED`.
- **Resumability** generalizes the ListenBrainz watermark resume
  (`docs/architecture.md`, Crash Recovery): `ctx.checkpoint(state)` writes to the row, and
  a re-delivered task starts with `ctx.resume_state`. Handlers that don't checkpoint simply
  restart.
- **Progress** (`progress_current` / `progress_total`) moves to `ctx.progress(n, total)`.

### Submitting work

One API replaces the 26 direct `enqueue_job` calls:

```python
task = await tasks.submit(
    "sync_range", SyncRangeParams(...),
    parent=parent_task, dedup_key=f"sync_range:{connection_id}:{range}", run_at=None,
)
```

`submit` inserts the row and enqueues after commit. If the process dies between commit and
enqueue, the row is `PENDING` with no arq job, which is exactly what the reaper re-delivers,
so no outbox table is needed. `dedup_key` (a unique partial index over non-terminal rows)
replaces the API-level 409 checks for mutually exclusive work.

### Leasing and crash recovery

- **Claim:** the runner moves `PENDING -> RUNNING` with a conditional update
  (`WHERE status = 'pending'`) and stamps `worker_id` and `lease_expires_at`. A second
  delivery of the same task loses the claim and exits. This gives at-most-one execution
  without relying on arq's job-id dedup.
- **Heartbeat:** extend `lease_expires_at` on an interval (the existing `with_heartbeat`
  loop, retargeted from the arq lock key to the row).
- **Reaper:** a periodic arq cron job, not only a startup hook, that:
  - re-delivers `RUNNING` rows whose lease expired (worker died mid-job),
  - re-delivers `PENDING` rows older than a grace period (arq job lost),
  - moves due `DEFERRED` rows back to `PENDING` and delivers them.
- With the lease on the row, the Redis worker registry and the `KEYS` scan in
  `cleanup_stale_locks` can be removed.

### Fan-out

Parent/child completion becomes a framework feature. A parent submits children with a
concurrency mode (`sequential` or `parallel`, today declared on `SyncStrategy`), and the
runner, on each child's terminal transition, advances the parent: enqueue the next
sequential child, or complete/fail the parent when all children are terminal. Cancelling a
parent cancels its non-terminal children (today `cancel_pending_children`).

### Scheduling

Register recurring handlers with `@scheduled("backfill_mbids", cron="0 * * * *")`. The
scheduler uses arq's `cron_jobs` and submits a normal task (row plus delivery), so scheduled
work gets the same durability, dedup and visibility as any other task. This is the home for
#120 and #149.

### Observability

Framework-level structured logs and metrics per handler: submitted, started, completed,
failed, deferred, cancelled, retried, duration, queue lag (`started_at - run_at`), and
reaper re-deliveries. Business handlers keep their own domain logs.

## Migration plan (deferred)

Each step ships on its own and leaves the system working. The reliability-first slice
delivers step 1 except `cancel_requested_at`; the rest waits on the spike.

1. **Real cancellation.** Add `CANCELLED` and `cancel_requested_at`; migrate the string
   convention; keep `is_cancelled` working. Unblocks #173.
2. **Runner wrapper.** Introduce `runner.py` wrapping the existing job functions; move
   status transitions and `_check_parent_completion` calls into it. Job bodies shrink, no
   behavior change.
3. **Registry.** `@handler` replaces `_TASK_DISPATCH` and `WorkerSettings.functions`
   (derive the arq function list from the registry).
4. **Submit API.** Replace the direct `enqueue_job` calls; add `dedup_key`.
5. **Row leases and the periodic reaper.** Retire the arq-lock heartbeat, worker registry
   and `KEYS` scan.
6. **Typed params, checkpoints, `ctx.defer`.** Port TIME_RANGE's watermark resume and the
   two hand-rolled deferrals.
7. **Split `worker.py`** into `jobs/` by domain.
8. **Scheduling.** Add `@scheduled`; land #149's auto-backfill as the first user.
9. **Boundary enforcement.** Turn on the import-linter contract (earlier if convenient; it
   is cheapest before handlers move).

## Non-goals

- Replacing arq. It stays the transport; the design narrows what it is trusted with.
- Publishing `taskkit` as a separate package now. It lives in the resonance tree behind an
  enforced boundary until a second consumer exists.
- Changing what any job does.

## Notes for a future lift (Joey)

Joey runs as a single-active leader with Redis/Memorystore and no Postgres, and most of
its work can be recomputed from Kubernetes state. For it to reuse `taskkit`:

- the `TaskStore` protocol needs a Redis-backed implementation (or a store-less mode for
  work that is safe to lose and re-derive),
- the reaper would run on leader acquisition and on an interval under the lease,
- the handler, context, retry, defer, checkpoint and schedule APIs should carry over unchanged.

Keeping `TaskStore` free of SQLAlchemy types is what keeps that option open.

## Open questions

- Retry policy defaults: retry transient errors (HTTP 5xx, timeouts) automatically and
  leave domain errors to fail fast, or make every handler opt in?
- Should `dedup_key` collisions return the existing task (idempotent submit) or raise? The
  API's current 409 suggests raise for user actions and return for internal fan-out.
- Lease length versus the longest handler timeout (3600s): heartbeat interval and lease TTL
  need to tolerate a busy event loop without false reclaims.
