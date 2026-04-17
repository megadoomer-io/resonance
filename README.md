# Resonance

Personal music discovery platform that aggregates listening data from multiple services into a unified music profile.

## What It Does

Resonance connects to Spotify, Last.fm, and ListenBrainz via OAuth and syncs your listening history in the background with incremental updates. It deduplicates and cross-references artists and tracks across services, building a unified view of your music taste. The long-term goal is automated playlist generation — curated playlists assembled from patterns in your aggregated listening data.

## Features

### Services

- Spotify — listening history, followed artists, saved tracks/albums, new releases
- Last.fm — scrobble history, loved tracks
- ListenBrainz — listening history

### Data

- Unified artist and track library with cross-service entity resolution
- Automatic deduplication across services
- Listening history aggregation from all connected accounts

### Sync

- Background sync via arq worker
- Incremental and full sync modes
- Real-time progress tracking
- Auto-dedup after sync completion

### UI

- Dashboard with sync controls and status
- Artist and track browsing with search
- Listening history view
- Account and service connection management
- HTMX-powered real-time updates

### Admin

- Role-based access control (user / admin / owner)
- CLI tooling for operations (`resonance-api`)
- Bulk dedup tools (events, artists, tracks)
- Test connector for development
- Task cloning and step-through debugging

## Tech Stack

Python 3.14 -- FastAPI -- SQLAlchemy 2.0 (async) -- PostgreSQL -- Redis -- arq -- Jinja2 -- HTMX

## Quick Start

```bash
# Clone and install
git clone git@github.com:megadoomer-io/resonance.git
cd resonance
uv sync --all-extras

# Configure environment
# See docs/self-hosting.md for the full list of required env vars.
# At minimum you need PostgreSQL and Redis connection details,
# a session secret, a token encryption key, and OAuth credentials
# for at least one music service.

# Set up the database
uv run alembic upgrade head

# Run the app
uv run uvicorn resonance.app:create_app --factory --reload

# Run the background worker (separate terminal)
uv run arq resonance.worker.WorkerSettings
```

## Documentation

- [Architecture](docs/architecture.md) — system design, data model, connector framework
- [User Guide](docs/user-guide.md) — connecting services, browsing your library, sync controls
- [Admin Guide](docs/admin-guide.md) — CLI reference, role management, bulk operations
- [Self-Hosting](docs/self-hosting.md) — deployment, environment variables, database setup
- [Spotify API Constraints](docs/spotify-api-constraints.md) — dev mode rate limits, removed endpoints, sync implications

## Roadmap

- Playlist generation engine — curated playlists from listening patterns and taste analysis
- Additional service connectors — Bandcamp, SoundCloud, and others
- Concert discovery — upcoming shows based on your taste profile and location
- Extended media types — podcasts, audiobooks, and other listening data

## Built with Claude

Resonance is a claude-first project — built collaboratively with Claude as the primary developer. Human provides direction and review; Claude does the implementation.
