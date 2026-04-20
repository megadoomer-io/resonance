# Documentation Refresh Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refresh stale docs and create missing docs so the project is well-documented for both development (Claude + owner) and showcase (tech-savvy friends/family). Relates to #43.

**Architecture:** Text-only documentation. README is the showcase entry point. `docs/` contains detailed guides. CLAUDE.md stays focused on development workflow. OpenAPI annotations replace a standalone API reference.

**Tech Stack:** Markdown, FastAPI route annotations (Python)

---

### Task 1: Archive the original design doc

**Files:**
- Move: `docs/design.md` → `docs/plans/archive/design-2026-03-30.md`

**Step 1: Create the archive directory**

Run: `mkdir -p docs/plans/archive`

**Step 2: Move the design doc**

Run: `git mv docs/design.md docs/plans/archive/design-2026-03-30.md`

**Step 3: Commit**

```bash
git add docs/plans/archive/design-2026-03-30.md
git commit -m "docs: archive original design doc to plans/archive

The design doc from 2026-03-30 is being replaced by docs/architecture.md
which describes the system as-built rather than as-planned.

Relates to #43"
```

---

### Task 2: Write README.md

**Files:**
- Modify: `README.md`

**Step 1: Write the new README**

Replace the current README with the following structure and content. Read the current codebase state to fill in accurate details:

- **Title + one-liner:** "Resonance — Personal music discovery platform that aggregates listening data from multiple services into a unified music profile."
- **What it does:** 3-4 sentences. Connects to Spotify, Last.fm, and ListenBrainz via OAuth. Syncs listening history in the background with incremental updates. Deduplicates and cross-references artists and tracks across services. Building toward automated playlist generation.
- **Features:** Bulleted list organized by category:
  - *Services:* Spotify (history, follows, ratings, releases), Last.fm (history, ratings), ListenBrainz (history)
  - *Data:* Unified artist/track library with cross-service entity resolution, automatic deduplication, listening history aggregation
  - *Sync:* Background sync via arq worker, incremental + full sync modes, real-time progress tracking, auto-dedup after sync
  - *UI:* Dashboard with sync controls, artist/track browsing and search, listening history, account/connection management, HTMX-powered real-time updates
  - *Admin:* Role-based access (user/admin/owner), CLI tooling for operations, bulk dedup tools, test connector for development, task cloning and step-through debugging
- **Tech stack:** Python 3.14 · FastAPI · SQLAlchemy 2.0 (async) · PostgreSQL · Redis · arq · Jinja2 · HTMX
- **Quick start:** Commands to clone, install (`uv sync --all-extras`), set up env vars (reference self-hosting.md), run migrations, run the app, run the worker
- **Documentation:** Links to architecture.md, user-guide.md, admin-guide.md, self-hosting.md, spotify-api-constraints.md
- **Roadmap:** 3-4 bullets: playlist generation engine, additional service connectors, concert discovery, extended media types
- **Built with Claude:** Brief note — "Resonance is a claude-first project — built collaboratively with Claude as the primary developer. Human provides direction and review; Claude does the implementation."

**Step 2: Verify markdown renders correctly**

Run: `cat README.md` and visually inspect structure.

**Step 3: Commit**

```bash
git add README.md
git commit -m "docs: rewrite README as project showcase

Replaces the scaffolding-era README with a complete project overview
including feature list, tech stack, quick start, and documentation links.

Relates to #43"
```

---

### Task 3: Write docs/architecture.md

**Files:**
- Create: `docs/architecture.md`

**Step 1: Write the architecture document**

Read all source files referenced below for accuracy. The doc should describe the system as it exists today, not aspirational design.

Sections:

1. **Overview** — What Resonance does. Single-user design with multi-user data model. Mention the megadoomer-io org context briefly.

2. **System Components** — Text diagram showing:
   - FastAPI web server (API + UI routes)
   - arq background worker (sync jobs, bulk operations)
   - PostgreSQL (persistent state)
   - Redis (sessions, task queue, rate limit coordination)
   - External service APIs (Spotify, Last.fm, ListenBrainz)
   
   Describe how they connect: web server enqueues jobs to Redis → arq worker picks them up → worker reads/writes PostgreSQL and calls external APIs.

3. **Data Model** — Describe the core entities and their relationships. Reference `src/resonance/models/`:
   - `User` — identity, role, timezone
   - `ServiceConnection` — OAuth credentials per service per user, sync watermarks
   - `Artist` — name + `service_links` JSON for cross-service resolution
   - `Track` — title, duration, artist FK, `service_links` JSON
   - `ListeningEvent` — user + track + timestamp + source service
   - `UserArtistRelation` / `UserTrackRelation` — follows, favorites, likes, loves
   - `Task` — hierarchical job tracking (parent/children), used for both sync jobs and bulk operations
   
   Explain the cross-service entity resolution: `service_links` is a JSON dict mapping `ServiceType` → external ID, allowing the same artist/track to be recognized across services.

4. **Connector System** — Reference `src/resonance/connectors/`:
   - `BaseConnector` ABC with `ConnectorCapability` enum (9 capabilities)
   - `ConnectorRegistry` for lookup by service type or capability
   - Current connectors: Spotify (8 capabilities), Last.fm (3), ListenBrainz (2), Test (mock)
   - Rate limit budget manager with priority lanes (auth=high, sync=normal)
   - How to add a new connector: implement `BaseConnector`, register in `ConnectorRegistry`

5. **Sync Pipeline** — Reference `src/resonance/worker.py` and `src/resonance/sync/`:
   - Trigger: API POST `/api/v1/sync/{service}` or CLI `resonance-api sync <service>`
   - Flow: creates SYNC_JOB task → enqueues `plan_sync` to arq → worker creates TIME_RANGE children → each range runs `sync_range` → data upserted via service-specific `SyncStrategy` → auto-dedup on completion
   - Incremental vs full: watermark tracking on `ServiceConnection.sync_watermark`
   - Task hierarchy: SYNC_JOB → TIME_RANGE → PAGE_FETCH (three levels)

6. **Authentication & Authorization** — Reference `src/resonance/api/v1/auth.py`, `src/resonance/middleware/`:
   - OAuth flow for login (any connected service works)
   - Redis-backed server-side sessions
   - Role system: user, admin, owner (stored on User model)
   - Two auth modes: session cookies (UI/browser) and bearer token (CLI/API)

7. **Bulk Operations** — Reference `src/resonance/worker.py`:
   - Async task system for long-running operations (dedup)
   - BULK_JOB task type with operation-specific logic
   - CLI polling with TTY-aware progress display
   - Fire-and-forget mode (`--no-wait`)

8. **API Layer** — Reference `src/resonance/api/v1/`:
   - Versioned under `/api/v1/` with four route groups: auth, account, sync, admin
   - Session auth for user-facing endpoints, bearer token for admin/CLI
   - Interactive API docs at `/docs` (FastAPI auto-generated OpenAPI)

9. **Deployment** — Brief:
   - Docker multi-stage build (see `Dockerfile`)
   - ArgoCD auto-sync from `megadoomer-config` repo
   - Alembic migrations run as init container
   - See `docs/self-hosting.md` for setup instructions

**Step 2: Verify no broken internal references**

Run: Review all cross-references to other docs and source files mentioned in the document.

**Step 3: Commit**

```bash
git add docs/architecture.md
git commit -m "docs: add architecture reference describing the system as-built

Covers system components, data model, connector system, sync pipeline,
auth, bulk operations, API layer, and deployment.

Replaces the archived design doc with current-state documentation.

Relates to #43"
```

---

### Task 4: Write docs/user-guide.md

**Files:**
- Create: `docs/user-guide.md`

**Step 1: Write the user guide**

Sections:

1. **Getting Started** — Navigate to the app URL, choose a service to log in with (Spotify, Last.fm, or ListenBrainz), complete OAuth, land on the dashboard.

2. **Dashboard** — What you see: connected services with sync status, quick stats (artist count, track count, listening events). Sync controls to trigger incremental or full syncs. Real-time progress updates via HTMX polling.

3. **Connecting Services** — From the Account page, click a service to initiate OAuth. Explain what each service provides:
   - Spotify: listening history, followed artists, saved tracks, new releases
   - Last.fm: listening history (scrobbles), loved tracks
   - ListenBrainz: listening history
   
   Multiple services can be connected simultaneously. Data is merged and deduplicated automatically.

4. **Syncing Your Data** — How to trigger a sync (dashboard button or CLI). Incremental sync fetches only new data since last sync. Full sync re-fetches everything. Background processing — you can navigate away and check back. Auto-deduplication runs after each successful sync.

5. **Browsing Your Library** — Artists page (alphabetical list, search by name), Tracks page (search by title, shows artist and duration), History page (chronological listening events with service source). All pages are paginated.

6. **Account Management** — View connected services, disconnect a service (blocked if it's your only connection), set timezone for display, account merge (if you logged in via different services and created duplicate accounts).

**Step 2: Commit**

```bash
git add docs/user-guide.md
git commit -m "docs: add user guide for the web UI

Covers getting started, dashboard, connecting services, syncing,
browsing the library, and account management.

Relates to #43"
```

---

### Task 5: Write docs/admin-guide.md

**Files:**
- Create: `docs/admin-guide.md`

**Step 1: Write the admin guide**

Sections:

1. **Role System** — Three roles: user (default), admin, owner. Admin can access admin panel and run bulk operations. Owner can do everything admin can plus change user roles. Roles are assigned via CLI `set-role` command (requires direct database access).

2. **Admin Panel** — Accessible from the navigation bar for admin/owner users. Features:
   - User management: view all users, change roles
   - Sync status overview: recent tasks across all users
   - Database stats: artist, track, event counts and duplicate metrics
   - Track search: fuzzy search across the track library
   - Dedup controls: trigger event/artist/track deduplication
   - Task cloning: clone a sync task for debugging, with optional step-through mode
   - Task resume: advance a deferred step-mode task

3. **CLI Tool (`resonance-api`)** — Environment setup:
   ```bash
   export RESONANCE_URL=https://resonance.megadoomer.io
   export RESONANCE_API_TOKEN=<bearer-token>
   ```
   
   Command reference with examples:
   - `healthz` — verify deployment, shows git revision
   - `status` — recent sync jobs with progress bars
   - `stats` — database statistics (counts, duplicates)
   - `sync <service> [--full]` — trigger sync (incremental by default)
   - `dedup <type> [--no-wait]` — run deduplication (events, artists, tracks, or all)
   - `task <task_id>` — monitor a specific bulk task
   - `track <query>` — search tracks by title
   - `set-role <user_id> <role>` — assign role (direct DB, disaster recovery)

4. **Deduplication** — Why duplicates occur (same track from multiple services, re-syncs). Three dedup types:
   - Event dedup: removes duplicate listening events (same user + track + timestamp)
   - Artist dedup: merges artists with matching names across services
   - Track dedup: merges tracks with matching title + artist across services
   
   Auto-dedup runs after each successful sync. Manual dedup available via admin panel or CLI.

5. **Test Connector** — Mock service for development and testing. Instantly connects without OAuth. Useful for testing sync flow, task cloning, and step-through debugging without hitting external APIs.

**Step 2: Commit**

```bash
git add docs/admin-guide.md
git commit -m "docs: add admin guide covering roles, panel, CLI, and dedup

Relates to #43"
```

---

### Task 6: Write docs/self-hosting.md

**Files:**
- Create: `docs/self-hosting.md`

**Step 1: Write the self-hosting guide**

Read `src/resonance/config.py` for the exact env var names and defaults. Read `Dockerfile` for the container setup.

Sections:

1. **Prerequisites** — Python 3.14+, PostgreSQL 15+, Redis 7+. Or Docker.

2. **OAuth App Registration** — Step-by-step for each service:
   - **Spotify:** Create app at developer.spotify.com → Dashboard → Create App. Set redirect URI to `{BASE_URL}/api/v1/auth/spotify/callback`. Copy client ID and secret. Note: dev mode limits to 25 users (see `docs/spotify-api-constraints.md`).
   - **Last.fm:** Create API account at last.fm/api/account/create. Note API key and shared secret. No redirect URI needed (uses web auth token flow).
   - **ListenBrainz:** Register OAuth app at musicbrainz.org/account/applications. Set redirect URI to `{BASE_URL}/api/v1/auth/listenbrainz/callback`. Copy client ID and secret.

3. **Environment Variables** — Full list organized by category:
   - Database: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`
   - Redis: `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD`
   - Security: `SESSION_SECRET_KEY` (generate with `python -c "import secrets; print(secrets.token_urlsafe(32))"`), `TOKEN_ENCRYPTION_KEY` (generate Fernet key with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)
   - App: `BASE_URL` (public URL, e.g., `http://localhost:8000` for local dev)
   - Spotify: `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`
   - MusicBrainz/ListenBrainz: `MUSICBRAINZ_CLIENT_ID`, `MUSICBRAINZ_CLIENT_SECRET`
   - Last.fm: `LASTFM_API_KEY`, `LASTFM_SHARED_SECRET`
   - Admin: `ADMIN_API_TOKEN` (generate with `python -c "import secrets; print(secrets.token_urlsafe(32))"`)

4. **Database Setup** — Create database, run migrations:
   ```bash
   createdb resonance
   uv run alembic upgrade head
   ```

5. **Running the App** — Two paths:
   - **Direct:**
     ```bash
     uv sync --all-extras
     uv run uvicorn resonance.app:create_app --factory --reload
     ```
   - **Docker:**
     ```bash
     docker build -t resonance .
     docker run -p 8000:8000 --env-file .env resonance
     ```
   
   Note: the arq worker runs separately for background sync:
   ```bash
   uv run arq resonance.worker.WorkerSettings
   ```

6. **First Login & Setup** — Navigate to the app, log in via any configured service. Use `set-role` to make yourself owner:
   ```bash
   uv run resonance-api set-role <your-user-id> owner
   ```
   Your user ID is visible in the admin panel or database.

**Step 2: Commit**

```bash
git add docs/self-hosting.md
git commit -m "docs: add self-hosting guide with setup instructions

Covers prerequisites, OAuth app registration for all three services,
environment variables, database setup, running the app, and first login.

Relates to #43"
```

---

### Task 7: Refresh CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

**Step 1: Update CLAUDE.md**

Make the following changes:

- **Add a Documentation section** near the top (after Project Overview), linking to:
  - `docs/architecture.md` — system architecture and data model
  - `docs/user-guide.md` — end-user walkthrough
  - `docs/admin-guide.md` — admin panel, CLI, and operations
  - `docs/self-hosting.md` — setup and deployment
  - `docs/spotify-api-constraints.md` — Spotify dev mode limitations

- **Remove the Project Structure section** (lines 66-89) — this is now covered in `docs/architecture.md`. Replace with a one-liner: "See [docs/architecture.md](docs/architecture.md) for project structure and system design."

- **Remove the Architecture Principles section** (lines 91-97) — also now in architecture.md.

- **Update the Project Overview** — remove the design doc link (archived), add architecture.md link instead.

- **Keep everything else** — Development Commands, Database Migrations, Database Operations, Code Quality, CLI Tool, CLI Testing Guidelines, Git Workflow, Conventions, Environment Variables. These are operational and belong in CLAUDE.md.

- **Fix Python version** — ensure consistency (3.14 throughout).

**Step 2: Verify CLAUDE.md is coherent**

Run: `cat CLAUDE.md` and check that the document flows well without the removed sections and with the new links.

**Step 3: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: refresh CLAUDE.md, link to new documentation

Removes architecture details now covered by docs/architecture.md.
Adds Documentation section linking to all guides.

Relates to #43"
```

---

### Task 8: Improve OpenAPI annotations

**Files:**
- Modify: `src/resonance/api/v1/auth.py`
- Modify: `src/resonance/api/v1/account.py`
- Modify: `src/resonance/api/v1/sync.py`
- Modify: `src/resonance/api/v1/admin.py`

**Step 1: Add/improve route annotations**

For each route module, ensure every route has:
- `summary` parameter on the decorator (short, for the endpoint list)
- `description` parameter on the decorator (longer, includes auth requirement: "Requires session authentication" or "Requires bearer token authentication")
- `tags` are already set at the router level — verify they're descriptive

Example pattern:
```python
@router.get(
    "/{service}",
    summary="Initiate OAuth flow",
    description="Redirect the user to the external service's OAuth authorization page. Requires session authentication.",
)
```

Do NOT change any logic, signatures, or behavior. Only add/update `summary` and `description` kwargs on route decorators.

**Step 2: Verify the app still passes linting and type checks**

Run: `uv run ruff check src/resonance/api/v1/ && uv run mypy src/resonance/api/v1/`

**Step 3: Commit**

```bash
git add src/resonance/api/v1/auth.py src/resonance/api/v1/account.py src/resonance/api/v1/sync.py src/resonance/api/v1/admin.py
git commit -m "docs: add OpenAPI summaries and descriptions to API routes

Every endpoint now has a summary and description including auth
requirements, improving the auto-generated /docs page.

Relates to #43"
```

---

### Task 9: Final verification and PR

**Step 1: Verify all docs exist and links are valid**

Run:
```bash
ls -la README.md docs/architecture.md docs/user-guide.md docs/admin-guide.md docs/self-hosting.md docs/plans/archive/design-2026-03-30.md
```

**Step 2: Check for broken markdown links**

Grep all `.md` files for internal links and verify targets exist:
```bash
grep -ohE '\[.*?\]\(([^)]+)\)' README.md CLAUDE.md docs/*.md | grep -v '^http' | sort -u
```

**Step 3: Run full quality checks**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src/
```

**Step 4: Run tests**

```bash
uv run pytest
```

**Step 5: Create PR**

Create a PR with title: "Documentation refresh: README, architecture, guides, and OpenAPI annotations"

Reference: Fixes #43
