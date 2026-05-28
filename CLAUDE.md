@./AGENTS.md

# Resonance

Personal media discovery platform — aggregates music data from external services and generates curated playlists.

## Project Overview

- **Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 (async), PostgreSQL, Redis, Jinja2, HTMX
- **Package manager:** uv
- **Architecture:** [docs/architecture.md](docs/architecture.md)
- **Spotify API constraints:** [docs/spotify-api-constraints.md](docs/spotify-api-constraints.md) — Dev mode rate limits, removed endpoints, sync implications
- **Deployment:** ArgoCD on megadoomer-do K8s cluster; config in `megadoomer-config` repo

## Documentation

- [Architecture](docs/architecture.md) — system components, data model, connectors, sync pipeline
- [User Guide](docs/user-guide.md) — end-user walkthrough
- [Admin Guide](docs/admin-guide.md) — admin panel, CLI tool, dedup operations
- [Self-Hosting](docs/self-hosting.md) — setup and deployment
- [Spotify API Constraints](docs/spotify-api-constraints.md) — dev mode limitations

## Local Development

The app requires PostgreSQL and Redis. Docker Compose provides both:

```bash
make dev          # Start PG + Redis, run migrations, launch app
make dev-up       # Start PG + Redis only (background)
make dev-down     # Stop services
make dev-reset    # Stop services and delete all data
make dev-migrate  # Run alembic upgrade head
```

Requires Docker (on macOS: Colima or Docker Desktop).

## Development Commands

```bash
# Install all dependencies (including dev tools: pytest, ruff, mypy)
# IMPORTANT: Dev tools (pytest, ruff, mypy) are in [project.optional-dependencies] dev.
# Plain `uv sync` does NOT install them. Always use --all-extras.
uv sync --all-extras

# Run the app locally (requires PG + Redis running, see Local Development above)
uv run uvicorn resonance.app:create_app --factory --reload

# Run tests
uv run pytest

# Lint and format
uv run ruff check .
uv run ruff format .

# Type checking (strict mode)
uv run mypy src/

# Database migrations
uv run alembic upgrade head        # apply all
uv run alembic revision --autogenerate -m "description"  # create new

# Generator profiles and playlists
uv run resonance-api profile list
uv run resonance-api profile create --type concert_prep --event <id> --name "name"
uv run resonance-api generate <profile-id> [--freshness 50]
uv run resonance-api playlists
uv run resonance-api playlist <playlist-id>
```

## Database Migrations

- **Never drop tables in the same migration that creates their replacement.** Use multi-step migrations:
  1. Create the new table
  2. Migrate data from old table to new table (separate migration)
  3. Update application code to use new table
  4. Drop the old table (separate migration, only after verifying data migration and code cutover)
- Each migration step should be independently deployable and rollback-safe
- Autogenerate requires a live database — write migrations manually when no local DB is available
- Alembic migrations run as an init container on every deploy (`alembic upgrade head`)

## Database Operations

- **Never use raw SQL** for data fixes or ad-hoc queries against production — always use SQLAlchemy ORM or Alembic migrations. Raw SQL bypasses ORM constraints (e.g., enum value casing) and is error-prone.
- For production data fixes, create an Alembic migration with `op.execute()` using proper enum values
- Enum columns use `native_enum=False` (stored as varchar) — always ensure CHECK constraints exist to enforce valid values at the DB level
- **SQLAlchemy stores StrEnum `.name` (UPPERCASE), not `.value` (lowercase).** When writing raw SQL in migrations, use `'RUNNING'` not `'running'`, `'CALENDAR_SYNC'` not `'calendar_sync'`, etc. This applies to CHECK constraints, WHERE clauses, and UPDATE SET values

## Code Quality

- **pre-commit** hooks run ruff (lint + format) and mypy on every commit
- **ruff** for linting and formatting (no black)
- **mypy** in strict mode — zero `type: ignore` suppressions without documented rationale
- **pytest** for all tests — write tests before implementation (TDD)
- All code must pass `ruff check`, `ruff format --check`, and `mypy --strict` before committing

## Project Structure

See [docs/architecture.md](docs/architecture.md) for project structure and system design.

### UI Module Layout

UI routes live in `src/resonance/ui/`, split by domain:

| Module | Scope |
|--------|-------|
| `common.py` | Shared infrastructure: templates singleton, `require_user`/`require_admin` auth deps, `base_context()`, pagination, `escape_ilike`, `count_rows` |
| `htmx.py` | HTMX helpers: `is_htmx_request()`, `render_fragment()`, `trigger_event()` |
| `dashboard.py` | Login + dashboard |
| `artists.py` | Artist list, detail, compare, merge preview |
| `events.py` | Event list, detail, artist management, candidates, attendance, enrichment |
| `tracks.py` | Track list, detail, compare, merge preview, listening history |
| `playlists.py` | Playlist list, detail, new, generation status, export |
| `admin.py` | Admin dashboard, resolution, venue/event management, dedup, tasks, stats |
| `sync.py` | Songkick connect, Concert Archives, sync status |
| `account.py` | Account page, merge flow |
| `playground.py` | Component playground at `/dev/components` (admin-only) |

All route modules use FastAPI dependency injection:
- `Annotated[uuid.UUID, Depends(common.require_user)]` for auth
- `Annotated[AsyncSession, Depends(deps_module.get_db)]` for DB sessions
- `common.base_context(request)` for template context

Macro library: `templates/components/macros.html` (entity_list, action_button, filter_bar, etc.)

## CLI Tool

### `resonance-api`

Unified CLI for admin operations. Uses bearer token auth for API commands;
`set-role` connects directly to the database (disaster recovery).

```bash
# Set environment variables (or use ADMIN_API_TOKEN in app config)
export RESONANCE_URL=https://resonance.megadoomer.io
export RESONANCE_API_TOKEN=<token>   # from 1Password: "Last.fm" item, Private vault

# Available commands
uv run resonance-api healthz                    # Health + deployed revision
uv run resonance-api status                     # Recent sync job overview
uv run resonance-api stats                      # Database statistics
uv run resonance-api sync <service> [--full]    # Trigger a sync
uv run resonance-api dedup <type> [--no-wait]   # Dedup: events|artists|tracks|all
uv run resonance-api task <task_id>             # Check bulk task status
uv run resonance-api track <query>              # Search tracks by title
uv run resonance-api profile list               # List generator profiles
uv run resonance-api profile create ...         # Create a generator profile
uv run resonance-api generate <profile-id>      # Generate playlist from profile
uv run resonance-api playlists                  # List playlists
uv run resonance-api playlist <playlist-id>     # Show playlist details
uv run resonance-api import concert_archives --file <path> [--export-date YYYY-MM-DD] [--wait]
uv run resonance-api api [METHOD] PATH [-d DATA] [-H HDR]  # Raw API request
uv run resonance-api set-role <user_id> <role>  # Set role — direct DB
```

### CLI Testing Guidelines

- **Use the CLI to verify deployments** — `resonance-api healthz` confirms the running revision
- **Use the CLI to trigger admin actions** — dedup, sync triggers, and future admin operations should be testable without a browser
- **When adding new admin features**, ensure they have a corresponding API endpoint and CLI command — not just a UI button
- **The CLI is also useful for Claude** — when debugging in a session, use `resonance-api` to interact with the live app instead of raw curl commands

## Git Workflow

- Feature branches and PRs are not always required — use judgment based on scope
- Small or straightforward changes can go directly to main
- Use a feature branch + PR for larger or riskier changes
- Single maintainer — in-session review is sufficient; no need to wait for async PR review

## Conventions

- **API-first**: all data must be accessible via JSON API endpoints under `/api/v1/` — the UI is a rendering layer on top of the API, not a separate data path
- API versioned under `/api/v1/`
- Connector classes live in `connectors/` and declare capabilities via `ConnectorCapability` enum
- Connectors also declare a `ConnectionConfig` (auth type, sync function, sync style) — used for generic sync dispatch, orphan recovery, and UI rendering
- Lightweight connectors (Songkick, iCal, Concert Archives) implement the `Connectable` Protocol without extending `BaseConnector`
- Concert Archives uses `file_upload` auth type — CSV uploaded via the web UI or CLI, parsed and imported as a background task
- Generator classes live in `generators/` and declare required/optional capabilities
- Generator parameter registry lives in `generators/parameters.py`
- Generator types declared in `generators/parameters.py` via `GENERATOR_TYPE_CONFIG`
- Scoring logic in `generators/scoring.py`
- Generator-specific logic in `generators/<type>.py` (e.g., `concert_prep.py`)
- SQLAlchemy models use UUID primary keys
- OAuth tokens encrypted at rest via Fernet
- All connections (OAuth, username-based, URL-based, file-upload) use the unified `ServiceConnection` model — there is no separate calendar feed or import model
- Task lifecycle helpers (`sync/lifecycle.py`) provide `complete_task`/`fail_task` — use these instead of setting task status inline
- Orphan recovery in the worker is type-agnostic — new task types are handled by adding an entry to `_TASK_DISPATCH`
- Playlist export uses `PLAYLIST_WRITE` capability — connectors declare it to enable export. Export creates a background `PLAYLIST_EXPORT` task per connection, tracked via `playlist.service_links`
- Track matching: export searches Spotify for tracks missing `service_links["spotify"]` and persists matches for future reuse
- Sync staleness: compare `playlist.updated_at` vs `service_links[connection].exported_at` — no external API calls needed
- No deployment manifests in this repo — all K8s config lives in `megadoomer-config`

## Environment Variables

App config is loaded via Pydantic `Settings`. Key variables:

- `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` — PostgreSQL connection components
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASSWORD` — Redis connection components
- `BASE_URL` — Public base URL (e.g., `https://resonance.megadoomer.io`), used to construct OAuth redirect URIs
- `SESSION_SECRET_KEY` — signing key for session cookies
- `TOKEN_ENCRYPTION_KEY` — Fernet encryption key for stored OAuth tokens
- `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET` — Spotify OAuth credentials
- `MUSICBRAINZ_CLIENT_ID`, `MUSICBRAINZ_CLIENT_SECRET` — MusicBrainz OAuth credentials (for ListenBrainz)
- `LASTFM_API_KEY`, `LASTFM_SHARED_SECRET` — Last.fm API credentials
- `ADMIN_API_TOKEN` — Bearer token for admin API access (CLI and programmatic use)
- `DEX_CLIENT_ID`, `DEX_CLIENT_SECRET` — Dex OIDC credentials (for GitHub login via Dex broker)
- `DEX_ISSUER_URL` — Dex issuer URL (e.g., `https://dex.megadoomer.io`). When empty, the GitHub connector is not registered and the login button does not appear.

## Design System

Always read DESIGN.md before making any visual or UI decisions.
All font choices, colors, spacing, and aesthetic direction are defined there.
Do not deviate without explicit user approval.
In QA mode, flag any code that doesn't match DESIGN.md.
