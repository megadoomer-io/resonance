# Async Bulk Tasks Design

Issue: #44

## Problem

Bulk admin operations (dedup-tracks, dedup-artists) run inline in HTTP handlers and can exceed gateway timeouts. Track dedup hit a 504 scanning ~30K tracks.

## Solution

Run bulk operations as arq tasks with CLI polling for results.

## Model Changes

Rename `SyncTask` → `Task` in Python. The table stays `sync_tasks` (historical name, documented in CLAUDE.md). Rename `SyncTaskType` → `TaskType`, keep `SyncStatus` as-is.

Add `BULK_JOB` to `TaskType`. Make `user_id` and `service_connection_id` nullable for bulk tasks that don't belong to a user or service.

Migration: ALTER nullable columns + add enum value. No table rename.

## Worker

New arq function `run_bulk_job(ctx, task_id)` in `worker.py`. Loads the task, reads `params["operation"]` to dispatch:

- `dedup_artists` → `dedup.find_and_merge_duplicate_artists(session, task)`
- `dedup_tracks` → `dedup.find_and_merge_duplicate_tracks(session, task)`
- `dedup_events` → `dedup.delete_cross_service_duplicate_events(session)`

Follows the same lifecycle as `sync_range`: PENDING → RUNNING → COMPLETED/FAILED. Dedup functions accept an optional `Task` parameter for progress updates.

Register `run_bulk_job` in `WorkerSettings.functions`.

## API

Dedup POST endpoints change from inline execution to:

1. Create `Task` with `task_type=BULK_JOB`, `status=PENDING`, `params={"operation": "..."}`
2. Enqueue `run_bulk_job` to arq
3. Return `{"task_id": "<uuid>", "status": "started"}`

New endpoint: `GET /admin/tasks/{task_id}` returns task status, progress, result, and error.

## CLI

`_cmd_dedup` becomes a two-phase flow: POST to start, then poll `GET /admin/tasks/{id}` every 3 seconds.

Display adapts to output context:
- **TTY**: Overwrite single line with `\r` showing progress. Print final JSON on completion.
- **Piped**: Append timestamped lines each poll. Final JSON on its own line.

New flags and commands:
- `dedup <type> --no-wait`: Start task, print ID, exit.
- `task <task_id>`: One-shot status check for any task.

`dedup all` runs the three operations sequentially, each as its own task.

## Code Extraction

Move the dedup-events SQL from `ui/routes.py` into `dedup.delete_cross_service_duplicate_events(session)` for reuse by both route handlers and the worker.

## Future

This pattern applies to any bulk/batch operation, not just admin tasks. Future bulk operations should follow the same model: create a BULK_JOB task, enqueue to arq, poll for results.
