# Resonance

Personal media discovery platform — aggregates music data from external services and generates curated playlists.

## Project Overview

- **Stack:** Python 3.13+, FastAPI, SQLAlchemy 2.0 (async), PostgreSQL, Redis, Jinja2
- **Package manager:** uv
- **Design doc:** [docs/design.md](docs/design.md)
- **Deployment:** ArgoCD on megadoomer-do K8s cluster; config in `megadoomer-config` repo

## Development Commands

```bash
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

- **ruff** for linting and formatting (no black)
- **mypy** in strict mode — zero `type: ignore` suppressions without documented rationale
- **pytest** for all tests — write tests before implementation (TDD)
- All code must pass `ruff check`, `ruff format --check`, and `mypy --strict` before committing

## Project Structure

```
src/resonance/
  app.py              # FastAPI app factory
  config.py           # Pydantic Settings (env-based config)
  api/v1/             # API route modules
  connectors/         # Service connector plugins (Spotify, Last.fm, etc.)
  generators/         # Playlist generator plugins
  models/             # SQLAlchemy async models
  sync/               # Sync job runner + progress tracking
  templates/          # Jinja2 server-rendered UI
```

## Architecture Principles

- **API-first:** Every UI action goes through a REST API call. The UI is a thin template layer.
- **Pluggable connectors:** Each external service is a connector class with declared capabilities. Query capabilities dynamically, never hardcode service names.
- **Pluggable generators:** Each playlist generator is a self-contained module implementing a common interface.
- **Async throughout:** SQLAlchemy async sessions, asyncpg, async connector methods.

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
- OAuth tokens encrypted at rest
- No deployment manifests in this repo — all K8s config lives in `megadoomer-config`

## Environment Variables

App config is loaded via Pydantic `Settings`. Key variables:

- `DATABASE_URL` — PostgreSQL connection string
- `REDIS_URL` — Redis connection string
- `SESSION_SECRET_KEY` — signing key for session cookies
- `TOKEN_ENCRYPTION_KEY` — encryption key for stored OAuth tokens
- Service-specific OAuth credentials: `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, etc.
