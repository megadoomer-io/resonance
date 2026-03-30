# Resonance

Personal media discovery platform that aggregates data from multiple music services (Spotify, Last.fm, ListenBrainz, Songkick, Bandsintown, Bandcamp, SoundCloud) and generates curated playlists.

## Features (Planned)

- Connect multiple music services via OAuth
- Aggregate listening history, follows, favorites, and ratings across services
- Pluggable playlist generators:
  - Listening History Analysis
  - Concert Prep
  - Local Scene Discovery
  - New Release Radar
  - Discovery from Social Signals
  - Deep Cuts
- Push generated playlists to Spotify
- Upcoming concert discovery based on taste profile and location

## Tech Stack

Python 3.13+ / FastAPI / SQLAlchemy 2.0 (async) / PostgreSQL / Redis / Jinja2

## Development

```bash
# Install dependencies
uv sync

# Run locally
uv run uvicorn resonance.app:create_app --factory --reload

# Run tests
uv run pytest

# Lint, format, type check
uv run ruff check . && uv run ruff format --check . && uv run mypy src/
```

## Documentation

- [Design Document](docs/design.md) — architecture, data model, API design, deployment
