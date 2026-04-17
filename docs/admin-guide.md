# Admin Guide

Operator reference for Resonance administration. Covers the role system, admin panel, CLI tool, deduplication, and the test connector.

## Role System

Resonance has three roles, listed from least to most privileged:

| Role    | Access                                                                 |
|---------|------------------------------------------------------------------------|
| `user`  | Default. Dashboard, personal sync controls, listening history.         |
| `admin` | Everything `user` can do, plus admin panel and bulk operations.        |
| `owner` | Everything `admin` can do, plus changing other users' roles.           |

Key restrictions:

- Only an `owner` can promote a user to `owner`.
- Admins can change roles to `user` or `admin`, but not to `owner`.
- No one can change their own role through the admin panel.

### Assigning Roles

Roles are assigned with the CLI `set-role` command, which writes directly to the database. This is the only way to bootstrap the first admin/owner account and serves as a disaster-recovery escape hatch when the web UI is inaccessible.

```bash
uv run resonance-api set-role <user_id> <role>
```

Valid roles: `user`, `admin`, `owner`.

To find a user's ID, query the database directly or use the admin panel's user list (once you have an admin account).


## Admin Panel

The admin panel is available at `/admin` and is visible in the navigation bar for users with the `admin` or `owner` role. Non-admin users are redirected to the dashboard.

### User Management

Lists all registered users with their display names, creation dates, and current roles. Owners can change any user's role via dropdown. Admins can assign `user` or `admin` roles but cannot promote to `owner`.

### Sync Status Overview

Shows the 20 most recent top-level tasks across all users, with their status, associated service connection, and creation time. This gives a quick view of sync activity across the entire instance.

The API equivalent (`/admin/status`) returns the 10 most recent sync jobs with child task details including type, status, progress, and any error messages.

### Database Statistics

Available via the admin panel or the `stats` CLI command. Reports:

- Total artist, track, and event counts
- Track duration coverage (how many tracks have duration metadata)
- Event counts broken down by service
- Duplicate group counts for artists and tracks

### Track Search

Fuzzy search across the track library by title. Returns up to 20 matches with:

- Track title, artist name, and duration
- Service links (external IDs from each connected service)
- Event counts per service
- Five most recent listening events with timestamps

### Dedup Controls

Three buttons to trigger deduplication jobs:

- **Dedup Events** -- removes duplicate listening events
- **Dedup Artists** -- merges duplicate artist records
- **Dedup Tracks** -- merges duplicate track records

Each button creates an asynchronous bulk task. Progress can be monitored via the `task` CLI command or by refreshing the admin panel.

### Task Cloning

Clone any sync task from the task list. Useful for debugging a failed or unusual sync by re-running it. Options:

- **Standard clone** -- creates a copy of the task with the same parameters and enqueues it for execution.
- **Step-through mode** -- adds `step_mode: true` to the cloned task's parameters. The task pauses (enters `deferred` status) after each step, letting you inspect intermediate state before continuing.

### Task Resume

Advance a deferred step-mode task to its next step. When a task is in `deferred` status, the resume button re-enqueues it for the next processing step. When all steps are complete, the parent task is marked as completed.

This is also used to resume any task that has entered `deferred` status for other reasons.


## CLI Tool (`resonance-api`)

The `resonance-api` CLI provides admin operations via bearer token authentication. Most commands call the same API endpoints as the admin panel.

### Environment Setup

```bash
export RESONANCE_URL=https://resonance.megadoomer.io
export RESONANCE_API_TOKEN=<bearer-token>
```

The token value is the `ADMIN_API_TOKEN` configured on the server. If these environment variables are not set, the CLI falls back to the application's `Settings` defaults.

### Command Reference

#### `healthz` -- Verify Deployment

```bash
uv run resonance-api healthz
```

Returns the health check response including the deployed git revision. Use this to confirm a deployment completed successfully.

#### `status` -- Recent Sync Jobs

```bash
uv run resonance-api status
```

Displays the 10 most recent sync jobs with their status, service, creation time, and child task progress. Output includes progress counters and error messages for failed tasks.

#### `stats` -- Database Statistics

```bash
uv run resonance-api stats
```

Shows counts for artists, tracks (with/without duration metadata), and events broken down by service. Also reports duplicate artist and track group counts when duplicates exist.

#### `sync` -- Trigger a Sync

```bash
# Incremental sync (default -- only fetches new data since last sync)
uv run resonance-api sync spotify

# Full re-sync (fetches all data from the beginning)
uv run resonance-api sync spotify --full
```

Triggers a sync for the specified service. The service name must match a connected service (e.g., `spotify`, `listenbrainz`, `lastfm`, `test`).

#### `dedup` -- Run Deduplication

```bash
# Dedup a specific type
uv run resonance-api dedup events
uv run resonance-api dedup artists
uv run resonance-api dedup tracks

# Run all three in sequence (artists, tracks, events)
uv run resonance-api dedup all

# Fire-and-forget (don't wait for completion)
uv run resonance-api dedup events --no-wait
```

By default, the CLI polls the task until completion, showing a progress indicator in TTY mode. Use `--no-wait` to submit the job and return immediately.

When running `dedup all`, jobs execute in sequence: artists first, then tracks, then events.

#### `task` -- Monitor a Task

```bash
uv run resonance-api task <task_id>
```

Returns the full status of a task including progress counters, result data, error messages, and timestamps. Useful for checking on tasks started with `--no-wait`.

#### `track` -- Search Tracks

```bash
uv run resonance-api track "bohemian rhapsody"
```

Searches tracks by title (case-insensitive substring match). For each match, displays:

- Title, artist, and duration
- Track ID
- Service links (external IDs)
- Event counts per service
- Recent listening events with timestamps

#### `set-role` -- Assign User Role

```bash
uv run resonance-api set-role <user_id> <role>
```

Connects directly to the database (not via API) to set a user's role. This is the disaster-recovery command for bootstrapping admin access or fixing role issues when the web UI is unavailable.

Valid roles: `user`, `admin`, `owner`.


## Deduplication

### Why Duplicates Occur

Duplicates appear for several reasons:

- **Cross-service overlap** -- The same track listened to on Spotify and scrobbled to Last.fm creates separate artist/track records for each service.
- **Re-syncs** -- A full re-sync may re-import events that already exist.
- **Name variations** -- Slight differences in artist or track names across services (e.g., trailing whitespace, unicode normalization) can create separate records.

### Dedup Types

**Event dedup** removes duplicate listening events. A duplicate is defined as the same user + track + timestamp combination. This is the most common type of duplicate and is automatically cleaned up after every successful sync.

**Artist dedup** merges artist records that share the same name (case-insensitive). When merging, all tracks, relations, and references are moved to the surviving record and the duplicate is deleted.

**Track dedup** merges track records that share the same title (case-insensitive) and artist. Service links, events, and relations are consolidated onto the surviving record.

### Auto-Dedup

Event deduplication runs automatically after each successful sync completes. This is handled by the worker -- after all sync ranges finish and the parent task is marked completed, a `dedup_events` bulk job is enqueued.

### Manual Dedup

Run deduplication manually when:

- You want to clean up artist or track duplicates (these are not auto-triggered).
- You suspect the auto-dedup missed events (e.g., after a database restore or manual data import).
- The `stats` command shows non-zero duplicate group counts.

Manual dedup is available through:

- The admin panel dedup buttons
- The CLI: `resonance-api dedup <type>`


## Test Connector

The test connector is a mock service connector for development and testing. It implements the `LISTENING_HISTORY` capability with no rate limiting.

### How to Use

1. Navigate to the account page.
2. Connect the "test" service -- it connects instantly without OAuth or credentials.
3. Trigger a sync via the dashboard or CLI: `resonance-api sync test`.

### What It Is For

- **Sync pipeline testing** -- Verify that the sync flow (plan, enqueue, fetch, upsert) works end-to-end without hitting external APIs or dealing with rate limits.
- **Task cloning and step-through** -- Clone a test sync task with step-through mode enabled to inspect how the worker processes each step. Useful for debugging task state transitions.
- **Admin panel walkthrough** -- Generate tasks and data to exercise admin panel features (status, stats, dedup) without needing real service credentials.

The test connector does not require any API keys or environment variables.
