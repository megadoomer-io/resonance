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

## Development Commands

```bash
# Install all dependencies (including dev tools: pytest, ruff, mypy)
# IMPORTANT: Dev tools (pytest, ruff, mypy) are in [project.optional-dependencies] dev.
# Plain `uv sync` does NOT install them. Always use --all-extras.
uv sync --all-extras

# Run the app locally
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

## Code Quality

- **pre-commit** hooks run ruff (lint + format) and mypy on every commit
- **ruff** for linting and formatting (no black)
- **mypy** in strict mode — zero `type: ignore` suppressions without documented rationale
- **pytest** for all tests — write tests before implementation (TDD)
- All code must pass `ruff check`, `ruff format --check`, and `mypy --strict` before committing

## Project Structure

See [docs/architecture.md](docs/architecture.md) for project structure and system design.

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

- API versioned under `/api/v1/`
- Connector classes live in `connectors/` and declare capabilities via `ConnectorCapability` enum
- Generator classes live in `generators/` and declare required/optional capabilities
- SQLAlchemy models use UUID primary keys
- OAuth tokens encrypted at rest via Fernet
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
