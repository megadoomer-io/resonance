# Resonance

Personal media discovery platform — aggregates music data from external services and generates curated playlists.

## Project Overview

- **Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 (async), PostgreSQL, Redis, Jinja2, HTMX
- **Package manager:** uv
- **Design doc:** [docs/design.md](docs/design.md)
- **Deployment:** ArgoCD on megadoomer-do K8s cluster; config in `megadoomer-config` repo

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

## Code Quality

- **pre-commit** hooks run ruff (lint + format) and mypy on every commit
- **ruff** for linting and formatting (no black)
- **mypy** in strict mode — zero `type: ignore` suppressions without documented rationale
- **pytest** for all tests — write tests before implementation (TDD)
- All code must pass `ruff check`, `ruff format --check`, and `mypy --strict` before committing

## Project Structure

```
src/resonance/
  app.py              # FastAPI app factory
  config.py           # Pydantic Settings (env-based config)
  crypto.py           # Fernet token encryption
  database.py         # Async engine + session factory
  dependencies.py     # FastAPI dependency injection
  merge.py            # Account merge logic
  types.py            # Shared enums (ServiceType, SyncStatus, etc.)
  api/v1/             # API route modules (auth, account, sync)
  connectors/         # Service connector plugins (Spotify, ListenBrainz)
    base.py           # BaseConnector ABC, capability enum, data models
    registry.py       # Connector registry
    ratelimit.py      # Rate limit budget manager
    spotify.py        # Spotify connector
    listenbrainz.py   # ListenBrainz connector (MusicBrainz OAuth)
  generators/         # Playlist generator plugins (future)
  middleware/         # Session middleware (Redis-backed)
  models/             # SQLAlchemy async models
  sync/               # Sync job runner + progress tracking
  templates/          # Jinja2 server-rendered UI + HTMX partials
  ui/                 # UI route handlers
```

## Architecture Principles

- **API-first:** Every UI action goes through a REST API call. The UI is a thin template layer.
- **Pluggable connectors:** Each external service is a connector class with declared capabilities. Query capabilities dynamically, never hardcode service names.
- **Pluggable generators:** Each playlist generator is a self-contained module implementing a common interface.
- **Async throughout:** SQLAlchemy async sessions, asyncpg, async connector methods.
- **Rate limit budget management:** Shared `RateLimitBudget` class paces API requests, with priority lanes for auth (high) vs sync (normal).

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
