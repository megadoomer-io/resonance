# Graceful Shutdown Design

**Issue**: [#28](https://github.com/megadoomer-io/resonance/issues/28)
**Date**: 2026-04-06

## Problem

Deployments can interrupt in-flight work at three layers: Alembic migrations (init container), arq worker tasks, and HTTP requests. This causes data loss, stale task status in the UI, and wasted sync progress.

## Web Server

### Goal

Ensure in-flight HTTP requests complete before pod termination, and prevent new requests from arriving during shutdown.

### Changes

**megadoomer-config** (`applications/resonance/resonance/do/helm-values.yaml`):
- `terminationGracePeriodSeconds: 60` on the main controller
- `preStop` lifecycle hook: `sleep 5` — gives Kubernetes time to remove the pod from Service endpoints before uvicorn starts draining

**Dockerfile**: Update `CMD` to include `--timeout-graceful-shutdown 30` — uvicorn waits up to 30 seconds for in-flight requests to finish after receiving SIGTERM.

### Timing Sequence

1. Pod receives SIGTERM
2. `preStop` hook runs: 5s sleep (pod removed from endpoints during this time)
3. SIGTERM delivered to uvicorn: starts draining, rejects new connections, waits up to 30s for in-flight requests
4. Lifespan shutdown: closes Redis, DB connections
5. Pod exits (well within the 60s grace period)

No application code changes needed.

## Worker

### Goal

On SIGTERM, stop accepting new jobs, let in-flight tasks checkpoint their progress, and mark them PENDING so they resume on next startup.

### Changes

**Shutdown signal** (`src/resonance/worker.py`):
- Module-level `threading.Event` called `shutdown_requested`
- In the arq `on_shutdown` hook: set the event, then reset any still-RUNNING tasks to PENDING with current progress saved to `params`
- Pass the event through the worker context dict (`WorkerContext`) so task functions can access it
- Close `arq_redis` explicitly in shutdown hook

**Checkpoint loop** (`sync_range` task):
- Between pages, check `shutdown_requested.is_set()`
- If set: save current offset/watermark to `task.params`, commit progress, mark task PENDING, and return early (similar to how `DeferRequest` already interrupts the loop for rate limits)
- The task resumes from the saved offset on next startup via `_reenqueue_orphaned_tasks`

**Orphan re-enqueue** (`_reenqueue_orphaned_tasks`):
- Add handling for tasks stuck in RUNNING status — reset them to PENDING and re-enqueue
- Covers the crash case where shutdown didn't complete cleanly

**`plan_sync` tasks**: No checkpoint needed — they're fast (just create child tasks). On shutdown, the shutdown hook resets them to PENDING and they re-plan on restart.

**megadoomer-config**: `terminationGracePeriodSeconds: 45` on the worker controller — enough time for a page fetch + DB commit to complete before SIGKILL.

## Migrations

### Goal

Prevent deployments from killing in-progress Alembic migrations.

### Changes

**megadoomer-config**: `terminationGracePeriodSeconds: 300` on the init container (5 minutes) — enough for any well-structured migration to complete.

No application code changes. Existing safeguards are sufficient:
- CLAUDE.md mandates multi-step migrations (never drop+create in one migration)
- Alembic runs in a transaction — if killed, the transaction rolls back cleanly
- The 5-minute grace period is generous for single-step migrations

### Future Improvement

Run migrations as a separate Kubernetes Job, decoupled from the deployment rollout. This fully eliminates the risk but adds operational complexity (ordering, health gating, cleanup). Filed as a follow-up issue.

## Testing

- Unit tests for checkpoint/resume logic: simulate `shutdown_requested` being set mid-sync, verify task is marked PENDING with saved offset
- Unit tests for `_reenqueue_orphaned_tasks` handling RUNNING status
- Manual verification: deploy, trigger a sync, then push a new image and confirm the task resumes from checkpoint after the new pod starts

## Out of Scope

- Migration as a Kubernetes Job (follow-up issue)
- Worker health checks / liveness probes
- WebSocket or long-lived connection draining
