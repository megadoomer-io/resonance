# Architecture

Technical reference for the Resonance system as built. This document describes
the components, data model, and runtime behavior of the application.

For setup and deployment instructions, see [self-hosting.md](self-hosting.md).
For Spotify-specific API constraints, see [spotify-api-constraints.md](spotify-api-constraints.md).

---

## Overview

Resonance is a personal media discovery platform that aggregates music data from
external services (Spotify, Last.fm, ListenBrainz) into a unified data model.
It normalizes listening history, artist follows, and track ratings across
services, enabling cross-service analytics and curated playlist generation.

The data model is multi-user-ready -- each entity is scoped to a user via
foreign keys -- but the current deployment is single-user. The application lives
under the `megadoomer-io` GitHub organization and runs on the `megadoomer-do`
Kubernetes cluster.

**Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 (async), PostgreSQL, Redis, arq,
Jinja2 + HTMX, structlog.

---

## System Components

```
                        +------------------+
                        |   Browser / CLI  |
                        +--------+---------+
                                 |
                    HTTP (session cookies / bearer token)
                                 |
                        +--------v---------+
                        |  FastAPI Server   |
                        |  (uvicorn)        |
                        |                   |
                        |  API routes       |
                        |  UI routes        |
                        |  /healthz         |
                        +--+------+------+--+
                           |      |      |
              +------------+      |      +-------------+
              |                   |                     |
     +--------v-------+  +-------v--------+   +--------v-----------+
     |   PostgreSQL    |  |     Redis      |   |  arq Worker        |
     |                 |  |                |   |                     |
     |  users          |  |  sessions      |   |  plan_sync          |
     |  service_conns  |  |  task queue    |   |  sync_range         |
     |  artists        |  |  rate limit    |   |  sync_cal_feed      |
     |  tracks         |  |  coordination  |   |  run_bulk_job       |
     |  events         |  +----------------+   |  generate_playlist  |
     |  relations      |                       |  discover_tracks    |
     |  sync_tasks     |                       |  score_and_build    |
     |  playlists      |                       +---+------------+---+
     |  gen_profiles   |                           |            |
     +-----------------+                  +--------v---+  +-----v---------+
                                          | PostgreSQL |  | External APIs |
                                          | (read/write)|  |               |
                                          +------------+  | Spotify       |
                                                          | Last.fm       |
                                                          | ListenBrainz  |
                                                          | Songkick      |
                                                          +---------------+
```

**FastAPI web server** serves both the REST API (`/api/v1/`) and the
server-rendered UI (Jinja2 + HTMX). It handles authentication, enqueues
background jobs, and reads data from PostgreSQL for display.

**arq background worker** runs as a separate process. The web server enqueues
jobs to Redis; the worker picks them up and executes sync operations and bulk
tasks. The worker reads/writes PostgreSQL directly and calls external service
APIs through the connector system.

**PostgreSQL** stores all persistent state: users, service connections, music
entities (artists, tracks, listening events), taste signals (follows, likes),
and task tracking.

**Redis** serves three roles:
1. **Session store** -- server-side session data, keyed by signed cookie.
2. **Task queue** -- arq job queue for background work.
3. **Rate limit coordination** -- connectors track request budgets via
   in-process state (not shared across processes).

---

## Data Model

All models use UUID primary keys and include `created_at`/`updated_at`
timestamps (via `TimestampMixin`). Enum columns use `native_enum=False`
(stored as varchar with CHECK constraints).

### User

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| display_name | String(255) | |
| email | String(255) | Nullable |
| timezone | String(63) | Nullable, IANA timezone |
| role | UserRole enum | `user`, `admin`, `owner` |

### ServiceConnection

Unified connection model for all external services. OAuth connections store
encrypted tokens; username-based connections (Songkick) store an external user
ID; URL-based connections (generic iCal) store the feed URL directly.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| service_type | ServiceType enum | `spotify`, `lastfm`, `listenbrainz`, etc. |
| external_user_id | String(255) | Service-specific user identifier |
| encrypted_access_token | Text | Fernet-encrypted |
| encrypted_refresh_token | Text | Nullable, Fernet-encrypted |
| token_expires_at | DateTime(tz) | Nullable |
| scopes | Text | Nullable, granted OAuth scopes |
| url | String(2048) | Nullable, for URL-based connections (iCal) |
| label | String(256) | Nullable, user-facing label |
| enabled | Boolean | Whether the connection is active |
| connected_at | DateTime(tz) | |
| last_synced_at | DateTime(tz) | Nullable |
| sync_watermark | JSON | `{data_type: {key: value}}` for incremental sync |

Unique constraint: `(user_id, service_type, external_user_id)`.

### Artist

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| name | String(512) | |
| service_links | JSON | Nullable. Maps ServiceType value to external ID |

### Track

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| title | String(512) | |
| artist_id | UUID FK | References artists (CASCADE) |
| duration_ms | Integer | Nullable |
| service_links | JSON | Nullable. Maps ServiceType value to external ID |

### ListeningEvent

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| track_id | UUID FK | References tracks |
| source_service | ServiceType enum | Which service reported this listen |
| listened_at | DateTime(tz) | When the listen occurred |

Unique constraint: `(user_id, track_id, listened_at)`.
Index: `(user_id, listened_at)`.

### UserArtistRelation

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| artist_id | UUID FK | References artists |
| relation_type | ArtistRelationType | `follow`, `favorite` |
| source_service | ServiceType enum | |
| source_connection_id | UUID FK | References service_connections |
| discovered_at | DateTime(tz) | |

Unique constraint: `(user_id, artist_id, relation_type, source_service)`.

### UserTrackRelation

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| track_id | UUID FK | References tracks |
| relation_type | TrackRelationType | `like`, `love` |
| source_service | ServiceType enum | |
| source_connection_id | UUID FK | References service_connections |
| discovered_at | DateTime(tz) | |

Unique constraint: `(user_id, track_id, relation_type, source_service)`.

### Task

Stored in the `sync_tasks` table (historical name). Tracks sync jobs, bulk
operations, and other async work. Tasks form a hierarchy via `parent_id`.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | Nullable (bulk jobs may have no user) |
| service_connection_id | UUID FK | Nullable |
| parent_id | UUID FK | Self-referential, nullable |
| task_type | TaskType enum | `sync_job`, `time_range`, `page_fetch`, `bulk_job`, `calendar_sync` |
| status | SyncStatus enum | `pending`, `running`, `completed`, `failed`, `deferred` |
| params | JSON | Task-specific parameters |
| result | JSON | Task output on completion |
| error_message | Text | Nullable, set on failure |
| progress_current | BigInteger | Current progress count |
| progress_total | BigInteger | Nullable, expected total |
| description | Text | Nullable, human-readable |
| started_at | DateTime(tz) | Nullable |
| completed_at | DateTime(tz) | Nullable |
| deferred_until | DateTime(tz) | Nullable, for rate-limit deferral |
| created_at | DateTime(tz) | Server default |

Indexes: `(user_id, status)`, `(parent_id, status)`,
`(service_connection_id, task_type, status)`.

### Playlist

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| name | String(512) | |
| description | Text | Nullable |
| track_count | Integer | Default 0 |
| is_pinned | Boolean | Default false |

### PlaylistTrack

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| playlist_id | UUID FK | References playlists (CASCADE) |
| track_id | UUID FK | References tracks (CASCADE) |
| position | Integer | 1-indexed order within playlist |
| score | Float | Nullable, composite score from scoring engine |
| source | String(64) | `library` or `discovery` |

### GeneratorProfile

A saved playlist generation recipe. Stores the generator type, input
references (e.g., event ID), and parameter values that control scoring.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| name | String(512) | |
| generator_type | GeneratorType enum | `concert_prep`, `artist_deep_dive`, etc. |
| input_references | JSON | `{input_name: id}` (e.g., `{"event_id": "..."}`) |
| parameter_values | JSON | `{param_name: value}` (e.g., `{"familiarity": 70}`) |
| auto_sync_targets | JSON | Nullable, future use |

### GenerationRecord

Links a Playlist to the GeneratorProfile run that produced it. Snapshots
the parameters used so the playlist is reproducible.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| profile_id | UUID FK | References generator_profiles (CASCADE) |
| playlist_id | UUID FK | References playlists (CASCADE) |
| parameter_snapshot | JSON | Frozen copy of parameter values at generation time |
| freshness_target | Integer | Nullable, requested freshness percentage (0-100) |
| freshness_actual | Float | Nullable, actual freshness achieved |
| generation_duration_ms | Integer | Nullable |
| track_sources_summary | JSON | Nullable, `{source: count}` |

### Cross-Service Entity Resolution

Artists and tracks use `service_links` (a JSON column) to map between external
service identifiers. The structure is `{service_type_value: external_id}`, e.g.:

```json
{
  "spotify": "4Z8W4fKeB5YxbusRsdQVPb",
  "listenbrainz": "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d"
}
```

When syncing from a new service, the system looks up existing entities by
service link. If a match is found, the new service's external ID is added to
`service_links`. If no match is found, a new entity is created. A post-sync
dedup process merges entities that were initially created separately but later
identified as the same artist or track.

---

## Connector System

Connectors are pluggable adapters for external music services. Each connector
declares its capabilities and handles authentication, API communication, and
data normalization.

### BaseConnector ABC

All connectors extend `BaseConnector` and must implement:

- `get_auth_url(state) -> str` -- Build the OAuth authorization URL.
- `exchange_code(code) -> TokenResponse` -- Exchange auth code for tokens.
- `get_current_user(access_token) -> dict[str, str]` -- Get user profile (`id`, `display_name`).

The base class provides `_request()`, which handles:
- Rate limit pacing via `RateLimitBudget`
- Automatic retry on 429 responses (respects `Retry-After`)
- Exponential backoff on transient errors (timeouts, disconnects)
- Up to 5 transient retries; 120s maximum rate-limit wait before failing

### ConnectorCapability Enum

Ten capabilities that connectors can declare:

| Capability | Description |
|------------|-------------|
| `AUTHENTICATION` | OAuth login flow |
| `LISTENING_HISTORY` | Fetch scrobbles / recently played |
| `RECOMMENDATIONS` | Get recommended tracks |
| `PLAYLIST_WRITE` | Create or modify playlists |
| `ARTIST_DATA` | Fetch artist metadata |
| `EVENTS` | Live event / concert data |
| `FOLLOWS` | Artist follows / subscriptions |
| `TRACK_RATINGS` | Likes, loves, saved tracks |
| `NEW_RELEASES` | New album / single releases |
| `TRACK_DISCOVERY` | Discover tracks for an artist via external APIs |

### Current Connectors

| Connector | Service | Auth Type | Capabilities |
|-----------|---------|-----------|-------------|
| `SpotifyConnector` | Spotify | OAuth | `AUTHENTICATION`, `LISTENING_HISTORY`, `FOLLOWS`, `TRACK_RATINGS` |
| `LastFmConnector` | Last.fm | OAuth | `AUTHENTICATION`, `LISTENING_HISTORY`, `TRACK_RATINGS` |
| `ListenBrainzConnector` | ListenBrainz | OAuth | `AUTHENTICATION`, `LISTENING_HISTORY`, `TRACK_DISCOVERY` |
| `SongkickConnector` | Songkick | Username | Lightweight — calendar feed sync only |
| `ICalConnector` | iCal | URL | Lightweight — calendar feed sync only |
| `TestConnector` | Test (mock) | OAuth | `LISTENING_HISTORY` |

### ConnectorRegistry

`ConnectorRegistry` stores connector instances keyed by `ServiceType`. It
supports lookup by service type (`get()`) or by capability
(`get_by_capability()`). Both the web server and the arq worker maintain
their own registry instance, populated at startup.

### ConnectionConfig

Each connector declares a `ConnectionConfig` (frozen dataclass) that tells the
system how to authenticate and sync:

| Field | Type | Description |
|-------|------|-------------|
| `auth_type` | str | `"oauth"`, `"username"`, or `"url"` |
| `sync_function` | str | arq job name to enqueue (e.g., `"plan_sync"`, `"sync_calendar_feed"`) |
| `sync_style` | str | `"incremental"` (watermark-based) or `"full"` (re-fetch everything) |
| `derive_urls` | Callable or None | For username-based connectors: generates feed URLs from a username |

This enables generic sync dispatch — the unified sync endpoint, orphan recovery,
and UI rendering all use `ConnectionConfig` instead of hardcoding per-service
logic. Full `BaseConnector` subclasses and lightweight connectors both provide
this via the `Connectable` Protocol.

### RateLimitBudget

Each connector has a `RateLimitBudget` that tracks remaining API quota from
response headers and computes paced request intervals. Features:

- **Paced intervals** -- spreads requests evenly across the rate limit window
  rather than bursting.
- **Priority lanes** -- `high_priority` requests (auth flows) skip pacing when
  budget is available; `normal` priority (sync) is always paced.
- **Rolling window** -- optional secondary budget ceiling (used by Spotify:
  180 requests per 30s window).
- **Header-driven** -- updates remaining/reset from `X-RateLimit-Remaining`,
  `X-RateLimit-Reset`, `Retry-After` headers.

### Adding a New Connector

**OAuth service (full connector):**

1. Create `src/resonance/connectors/<service>.py` extending `BaseConnector`.
2. Set `service_type`, `capabilities`, and implement `connection_config()`.
3. Implement `get_auth_url()`, `exchange_code()`, `get_current_user()`.
4. Add data-fetching methods for declared capabilities.
5. Register in both `app.py` and `worker.py`.
6. Create a `SyncStrategy` in `src/resonance/sync/<service>.py` if the
   connector supports `LISTENING_HISTORY`.
7. Add the service to the `ServiceType` enum in `src/resonance/types.py`.

**Username/URL service (lightweight connector):**

1. Create `src/resonance/connectors/<service>.py` with a class implementing
   the `Connectable` Protocol (`service_type` attribute + `connection_config()` method).
2. Implement `connection_config()` returning a `ConnectionConfig`.
3. Register in both `app.py` and `worker.py`.
4. Add the service to the `ServiceType` enum.
5. No `BaseConnector` subclassing needed — no OAuth, no HTTP client.

---

## Sync Pipeline

The sync pipeline imports data from external services into the local database.
It uses a hierarchical task model with three levels.

### Trigger

A sync is triggered by:
- `POST /api/v1/sync/connection/{connection_id}` (unified endpoint — works for all connection types)
- `POST /api/v1/sync/{service}` (legacy endpoint — OAuth services only)
- `resonance-api sync <service> [--full]` (CLI command)

The unified endpoint uses the connector's `ConnectionConfig` to determine which
arq job to enqueue (`plan_sync` for incremental, `sync_calendar_feed` for full).

### Flow

```
API/CLI
  |
  v
Create SYNC_JOB task (PENDING)
  |
  v
Enqueue plan_sync to arq
  |
  v
plan_sync (worker):
  - Mark SYNC_JOB as RUNNING
  - Load ServiceConnection and SyncStrategy
  - Call strategy.plan() -> list of SyncTaskDescriptors
  - Create TIME_RANGE child tasks
  - Enqueue children (parallel or sequential per strategy)
  |
  v
sync_range (worker, per TIME_RANGE):
  - Mark task RUNNING
  - Call strategy.execute() -> fetches pages, upserts data
  - On success: mark COMPLETED, write watermark
  - On 429 exceeding max wait: mark DEFERRED, re-enqueue with delay
  - On shutdown signal: checkpoint progress, mark PENDING
  - Check parent completion -> enqueue next sibling or cascade
  |
  v
Parent completion:
  - Aggregate results from all children
  - Mark parent COMPLETED or FAILED
  - On success: auto-enqueue post-sync event dedup (BULK_JOB)
```

### Incremental vs Full Sync

Each `ServiceConnection` maintains a `sync_watermark` JSON column that records
the most recent synced timestamp per data type. On incremental sync, the
strategy uses the watermark as a starting point, fetching only newer data. A
full sync ignores the watermark and fetches everything from the beginning.

### Task Hierarchy

- **SYNC_JOB** -- top-level job for OAuth services. Has zero or more
  TIME_RANGE children.
- **TIME_RANGE** -- a time-bounded chunk of data to fetch. The strategy
  decides how to partition (e.g., Last.fm uses monthly ranges for large
  histories).
- **CALENDAR_SYNC** -- standalone task for calendar feed sync (Songkick, iCal).
  No children — fetches and parses iCal feeds in a single operation.
- **PAGE_FETCH** -- (defined in TaskType but not currently used as a separate
  task; page fetching happens within sync_range execution).
- **BULK_JOB** -- standalone task for bulk operations (dedup, etc.).
- **PLAYLIST_GENERATION** -- top-level task for playlist generation. Has
  zero or more TRACK_DISCOVERY children and one TRACK_SCORING child.
- **TRACK_DISCOVERY** -- discovers tracks for a single artist via a
  connector with `TRACK_DISCOVERY` capability (e.g., ListenBrainz/MusicBrainz).
- **TRACK_SCORING** -- scores all candidate tracks and builds the final
  playlist. Always the last child of a PLAYLIST_GENERATION task.

### Concurrency

Each `SyncStrategy` declares a `concurrency` mode:
- **`sequential`** -- children execute one at a time; the next is enqueued
  only after the previous completes. Used by Spotify (strict rate limits).
- **`parallel`** -- all children are enqueued immediately. Used by
  ListenBrainz (generous rate limits).

### Crash Recovery

On worker startup, `_reenqueue_orphaned_tasks()` finds tasks stuck in
PENDING (lost arq job), RUNNING (interrupted by crash), or expired DEFERRED
status and re-enqueues them. Recovery is type-agnostic — the `_TASK_DISPATCH`
map determines the arq job name and arguments for each task type. For
ListenBrainz TIME_RANGE tasks, watermark-based resume is applied so the task
picks up where it left off rather than re-processing from the beginning.

### Task Lifecycle Helpers

`sync/lifecycle.py` provides `complete_task()` and `fail_task()` for consistent
status management. All task types use these helpers instead of setting status
fields inline. The helpers set `status`, `result`/`error_message`, and
`completed_at` atomically.

---

## Authentication and Authorization

### OAuth Login Flow

Users authenticate by connecting an external service via OAuth. Any service
with the `AUTHENTICATION` capability can serve as the login provider. The flow:

1. `GET /api/v1/auth/{service}/login` -- generates state token, stores in
   session, redirects to service's auth URL.
2. Service redirects back to `GET /api/v1/auth/{service}/callback` with
   auth code.
3. Server exchanges code for tokens, encrypts them (Fernet), creates or
   updates `User` and `ServiceConnection`.
4. User ID and role are stored in the Redis-backed session.

### Sessions

Sessions use Redis-backed server-side storage with signed cookies
(`itsdangerous` signer). The `SessionMiddleware` intercepts every request,
loads session data from Redis by cookie value, and saves it back if modified.

### Role System

Three roles on the `User` model:

| Role | Access |
|------|--------|
| `user` | Standard access to own data |
| `admin` | Admin API endpoints, test service connection |
| `owner` | Full access; bearer token API resolves to this user |

### Two Auth Modes

1. **Session cookies** -- used by the browser UI. OAuth login sets the session.
2. **Bearer token** -- used by the CLI and programmatic API access. The token
   is compared against the `ADMIN_API_TOKEN` setting; if valid, the request
   is resolved to the `owner` user.

The `get_current_user_id` dependency tries session first, then bearer token.
The `verify_admin_access` dependency checks bearer token first, then session
role.

---

## Bulk Operations

Long-running administrative operations run as `BULK_JOB` tasks in the arq
worker.

### Current Operations

All bulk operations are deduplication tasks:

| Operation | Function | Description |
|-----------|----------|-------------|
| `dedup_artists` | `find_and_merge_duplicate_artists` | Merge duplicate Artist records, repoint tracks and relations |
| `dedup_tracks` | `find_and_merge_duplicate_tracks` | Merge duplicate Track records, repoint events and relations |
| `dedup_events` | `delete_cross_service_duplicate_events` | Remove duplicate ListeningEvents from different sources |

### Task Lifecycle

1. Client sends `POST /api/v1/sync/dedup/{type}` or runs `resonance-api dedup <type>`.
2. Server creates a `BULK_JOB` task and enqueues `run_bulk_job` to arq.
3. Worker executes the operation, updating task status through
   `PENDING -> RUNNING -> COMPLETED/FAILED`.
4. Results (counts of merged/deleted entities) are stored in `task.result`.

### CLI Polling

The `resonance-api dedup` command supports:
- **Polling mode** (default) -- polls the task status endpoint with
  TTY-aware progress display.
- **Fire-and-forget** (`--no-wait`) -- returns immediately after enqueuing.

### Auto-Dedup

After a successful sync job completes (all children finished), a
`dedup_events` bulk job is automatically enqueued to clean up cross-service
duplicate listening events.

---

## Generator System

The generator system produces curated playlists from stored recipes. A
**GeneratorProfile** defines what to generate (generator type, input references
like an event ID, and parameter values). Running a profile produces a versioned
**Playlist** through a hierarchical task pipeline.

For the full design rationale, see
[docs/plans/2026-04-27-playlist-generation-design.md](plans/2026-04-27-playlist-generation-design.md).

### Parameter Registry

Parameters are defined in code (`generators/parameters.py`), not in the
database. Each parameter has a name, scale type, default value, and endpoint
labels.

Two scale types:

| Scale Type | Range | Meaning |
|------------|-------|---------|
| **Bipolar** | 0-100 | 50 is neutral; 0 and 100 are opposite extremes |
| **Unipolar** | 0-100 | 0 is "none", 100 is "maximum" |

Current parameters:

| Parameter | Scale | Default | Labels |
|-----------|-------|---------|--------|
| `familiarity` | Bipolar | 50 | All Discovery / All Known Tracks |
| `hit_depth` | Bipolar | 50 | Deep Cuts / Big Hits |
| `similar_artist_ratio` | Unipolar | 0 | Target Artists Only / Heavy Adjacent Artists |

Each generator type declares its **featured parameters** (the subset of
parameters relevant to that generator) and **required inputs** via
`GENERATOR_TYPE_CONFIG` in `generators/parameters.py`.

### Task Hierarchy

Playlist generation uses the same task infrastructure as sync jobs, with three
task types dispatched sequentially:

```
API/CLI
  |
  v
Create PLAYLIST_GENERATION task (PENDING)
  |
  v
Enqueue generate_playlist to arq
  |
  v
generate_playlist (worker):
  - Resolve event artists (EventArtist + accepted EventArtistCandidate)
  - Check library coverage per artist
  - Create TRACK_DISCOVERY child for each artist with < 5 library tracks
  - Create TRACK_SCORING child (always, runs last)
  - Enqueue first child (sequential dispatch)
  |
  v
discover_tracks_for_artist (worker, per TRACK_DISCOVERY):
  - Call connector with TRACK_DISCOVERY capability (ListenBrainz/MusicBrainz)
  - Upsert discovered tracks and service links
  - On completion: enqueue next sibling or TRACK_SCORING
  |
  v
score_and_build_playlist (worker, TRACK_SCORING):
  - Load all candidate tracks (library + discovered)
  - Score each track via composite_score()
  - Apply freshness filtering (if previous generation exists)
  - Create Playlist + PlaylistTrack rows
  - Create GenerationRecord linking profile to playlist
  - Mark parent PLAYLIST_GENERATION complete
```

Children are dispatched **sequentially** (one at a time) to respect MusicBrainz
rate limits. The TRACK_SCORING task is always created last so it runs after all
discovery is complete.

### Scoring

The scoring engine (`generators/scoring.py`) computes a composite score for
each candidate track based on three signals:

1. **Familiarity** -- logarithmic curve over listen count + library membership.
   Higher value = more familiar track.
2. **Popularity** -- linear mapping from the external 0-100 popularity score.
   Higher value = more popular track.
3. **Artist relevance** -- 1.0 for target artists, 0.0 for adjacent artists.

Bipolar parameters shift the score: a `familiarity` value of 80 favors known
tracks, while 20 favors discovery. The composite score is clamped to [0.0, 1.0].

### Concert Prep Data Flow

The concert prep generator (`generators/concert_prep.py`) follows this flow:

1. **Resolve artists** -- load confirmed EventArtist rows + accepted
   EventArtistCandidate rows for the event.
2. **Library pass** -- check how many tracks exist per artist in the user's
   listening history.
3. **Discovery pass** -- for artists below the library threshold (< 5 tracks),
   create TRACK_DISCOVERY tasks that query MusicBrainz for top recordings.
4. **Scoring** -- score all candidate tracks (library + discovered) using the
   profile's parameter values.
5. **Playlist creation** -- take the top N scored tracks, create a Playlist
   with PlaylistTrack rows, and record a GenerationRecord.

---

## API Layer

### Route Groups

All API routes are versioned under `/api/v1/`:

| Prefix | Module | Purpose |
|--------|--------|---------|
| `/api/v1/auth` | `auth.py` | OAuth login/callback, logout |
| `/api/v1/account` | `account.py` | User profile, service connections |
| `/api/v1/sync` | `sync.py` | Trigger syncs, check status, dedup, stats |
| `/api/v1/admin` | `admin.py` | Admin operations (test service connect) |
| `/api/v1/generator-profiles` | `generators.py` | CRUD profiles, trigger generation |
| `/api/v1/playlists` | `playlists.py` | List, detail, diff playlists |

### Authentication

- **User-facing endpoints** (account, sync trigger) use session auth via the
  `get_current_user_id` dependency.
- **Admin endpoints** (dedup, stats, test connect) use the `verify_admin_access`
  dependency, which accepts either admin/owner session or bearer token.

### Interactive Docs

FastAPI auto-generates OpenAPI docs at `/docs` (Swagger UI). This is only
enabled when `DEBUG=true` in the application settings.

### Health Check

`GET /healthz` returns `{"status": "ok", "revision": "<git-sha>"}`. The
revision comes from the `GIT_SHA` environment variable set at Docker build
time.

---

## Deployment

Resonance runs on the `megadoomer-do` DigitalOcean Kubernetes cluster, managed
by ArgoCD.

### Build

The `Dockerfile` uses a multi-stage build:
1. **Builder stage** (`uv:python3.14-bookworm-slim`) -- installs dependencies
   via `uv sync`, copies source and Alembic config.
2. **Runtime stage** (`python:3.14-slim-bookworm`) -- copies the virtual
   environment from builder. Runs as `nobody` user on port 8000.

The `GIT_SHA` build arg is baked into the image for the `/healthz` endpoint.

### CI/CD Pipeline

On push to `main`:
1. GitHub Actions builds and pushes the Docker image to
   `ghcr.io/megadoomer-io/resonance` with a timestamped tag.
2. A second job updates the image tag in the `megadoomer-config` repo via
   `kustomize edit set image`.
3. ArgoCD auto-syncs the change to the cluster within ~5 minutes.

### Database Migrations

Alembic migrations run as a Kubernetes init container on every deploy
(`alembic upgrade head`). This ensures the database schema is current before
the application starts.

### Runtime Processes

Two containers (configured in `megadoomer-config`):
- **Web server** -- `uvicorn resonance.app:create_app --factory` with
  30-second graceful shutdown timeout (the Dockerfile default CMD).
- **Worker** -- `python -m resonance.worker` running the arq worker process
  (command override in the deployment config).

Both connect to the same PostgreSQL and Redis instances.

For setup and configuration details, see [self-hosting.md](self-hosting.md).
