# Resonance — Design Document

**Date:** 2026-03-30
**Status:** Draft
**Repository:** `megadoomer-io/resonance`
**URL:** `resonance.megadoomer.io`

## Overview

Resonance is a personal media discovery platform that aggregates data from multiple external music services and uses it to generate curated playlists. The initial focus is music with Spotify as the primary playback platform, but the architecture supports extension to other media types (TV, movies) and other playback platforms (Apple Music, Google, Amazon) in the future.

This is a greenfield project under the `megadoomer-io` GitHub organization, deployed to the `megadoomer-do` Kubernetes cluster via ArgoCD.

## Tech Stack

| Component | Choice | Notes |
|-----------|--------|-------|
| Language | Python 3.13+ | Modern tooling, uv for package management |
| Web framework | FastAPI (on Starlette) | Async-native, auto OpenAPI docs, Pydantic integration. Can fall back to raw Starlette if needed. |
| ASGI server | uvicorn | |
| Database | PostgreSQL (Bitnami Helm chart, in-cluster) | |
| Cache/sessions | Redis (in-cluster) | Sessions, API response cache, rate limiting, sync job coordination |
| ORM | SQLAlchemy 2.0 + asyncpg | Async-native, modern API with type annotations |
| Migrations | Alembic | Runs as init container before app starts |
| Templates | Jinja2 | Thin server-rendered UI layer |
| Container | Docker (multi-stage build) | |
| Deployment | ArgoCD + app-template Helm chart | Config lives in megadoomer-config |
| CI/CD | GitHub Actions | Build, test, lint, type check, cross-repo image promotion |

## Architecture

```
+-------------------------------------+
|           Web UI (Jinja2)           |
+-------------------------------------+
|          REST API (FastAPI)         |
+----------+----------+---------------+
| Auth &   | Playlist |  Service      |
| Sessions | Engine   |  Connectors   |
+----------+----------+---------------+
|        Data Layer (SQLAlchemy)      |
+----------+--------------------------+
| Postgres |         Redis            |
| (state)  | (sessions, cache, rate)  |
+----------+--------------------------+
```

- **REST API** — the primary interface. Every UI action goes through an API call. OpenAPI docs auto-generated. Versioned under `/api/v1/`.
- **Auth & Sessions** — OAuth flows for external services, Redis-backed session management for app login.
- **Service Connectors** — pluggable adapters for each external service. Common interface with declared capabilities.
- **Playlist Engine** — pluggable generators that consume aggregated data and produce playlists.
- **Data Layer** — SQLAlchemy 2.0 async models. Caches external API responses in Redis to respect rate limits.

The API is the building block for everything. The UI is a thin layer of server-rendered templates calling the same API endpoints.

## Authentication & Identity

### App Authentication

- **Session-based** — server-side sessions stored in Redis, cookie-based.
- No passwords — identity is established entirely through connected external services.
- Any connected service's OAuth can be used to log in.
- Users can connect/disconnect services freely, with one constraint: **unlinking the last connected service is blocked** to prevent lockout.

### Account Recovery (Future)

- Optional email on account for magic-link recovery.
- Not implemented in v1 — noted as a future feature.

### Multi-User

- Designed for single-user initially (just the owner).
- Data model and security support multiple users so the service can be opened to friends or open-sourced later.

## Data Model

### Users & Identity

```
User
+-- id (UUID)
+-- display_name
+-- email (optional, for future recovery)
+-- created_at / updated_at
|
+-- ServiceConnection (one-to-many)
|   +-- service_type (enum: spotify, lastfm, listenbrainz, ...)
|   +-- external_user_id
|   +-- access_token (encrypted at rest)
|   +-- refresh_token (encrypted at rest)
|   +-- token_expires_at
|   +-- scopes
|   +-- connected_at / last_used_at
|
|   Unique constraint: (user_id, service_type, external_user_id)
|   This allows multiple accounts per service.
```

### Music Domain

```
Artist
+-- id (UUID)
+-- name
+-- service_links (artist ID on each external service)
|
+-- ArtistTag (one-to-many)
|   +-- tag (string, e.g. "shoegaze", "post-punk")
|   +-- source_service
|   +-- weight (float, 0-1)
|   +-- fetched_at

Track
+-- id (UUID)
+-- title
+-- artist_id (FK)
+-- service_links (track ID on each external service)

ListeningEvent
+-- user_id (FK)
+-- track_id (FK)
+-- source_service
+-- listened_at

Concert
+-- artist_id (FK)
+-- venue, city, date
+-- source_service
+-- user_rsvp (boolean, per user)

Playlist
+-- user_id (FK)
+-- generator_type (which engine created it)
+-- generator_params (JSON -- inputs used)
+-- spotify_playlist_id
+-- created_at
+-- tracks (ordered list of track IDs)
```

### User Taste Signals

```
UserArtistRelation
+-- user_id (FK)
+-- artist_id (FK)
+-- relation_type (enum: follow, favorite)
+-- source_service
+-- source_connection_id (FK)
+-- discovered_at

UserTrackRelation
+-- user_id (FK)
+-- track_id (FK)
+-- relation_type (enum: like, love, to_listen)
+-- source_service
+-- source_connection_id (FK)
+-- discovered_at
```

Follows, favorites, likes, and loves are gathered during sync from each service. These are stronger taste signals than raw play counts and are weighted accordingly by playlist generators.

### Entity Resolution

The same artist or track appears on multiple services under different IDs. `service_links` on Artist and Track map internal IDs to external ones, allowing data from different sources to merge into a unified view. Spotify IDs are canonical initially; cross-service matching improves progressively.

## Service Connectors

Each external service is implemented as a connector class that conforms to a common interface and declares its capabilities.

### Capability Model

```python
class ConnectorCapability(enum.Enum):
    AUTHENTICATION = "authentication"
    LISTENING_HISTORY = "listening_history"
    RECOMMENDATIONS = "recommendations"
    PLAYLIST_WRITE = "playlist_write"
    ARTIST_DATA = "artist_data"
    EVENTS = "events"
    FOLLOWS = "follows"
    TRACK_RATINGS = "track_ratings"
    NEW_RELEASES = "new_releases"
```

### Service Capability Matrix

| Service | Auth | History | Recs | Playlists | Artists | Events | Follows | Ratings | Releases |
|---------|------|---------|------|-----------|---------|--------|---------|---------|----------|
| Spotify | x | x | x | x | x | ? | x | x | x |
| Last.fm | x | x | x | | x | | | x | |
| ListenBrainz | x | x | x | | x | | | | |
| Songkick | x | | | | | x | | | |
| Bandsintown | x | | | | | x | x | | |
| Bandcamp | x | | | | x | | x | | x |
| SoundCloud | x | x | | | x | | x | | |
| Foopee | | | | | | x | | | |

**Notes:**
- Spotify Events marked `?` -- API support for events needs verification.
- Foopee likely has no API/OAuth -- may require scraping. Events-only, driven by location.

The system queries capabilities dynamically ("which connectors support listening history?") rather than hardcoding service names. Adding a new service means implementing the connector interface and declaring capabilities.

## Playlist Engine

### Generator Interface

```python
class PlaylistGenerator(ABC):
    name: str
    description: str
    required_capabilities: set[ConnectorCapability]
    optional_capabilities: set[ConnectorCapability]

    def is_available(self, user_connections: ...) -> bool:
        """Can this user use this generator given their connections?"""

    async def generate(self, user: User, params: dict) -> list[Track]:
        """Produce an ordered list of tracks."""
```

Each generator is a self-contained module. Adding one means implementing the interface and dropping it into the generators package.

### Input Sources

Playlist generators accept flexible, composable input sources. An input source is a pointer to music data from anywhere:

```
InputSource (value object, stored as JSON in generator params)
+-- source_type (enum)
|   +-- my_history          -> user's own aggregated listening data
|   +-- my_taste_profile    -> computed taste profile (genres, weights)
|   +-- playlist            -> a specific playlist (any user's)
|   +-- artist              -> an artist's catalog
|   +-- external_user       -> another user's profile on a service
|   +-- genre_tag           -> a genre or tag string
|   +-- track_list          -> an ad-hoc set of track IDs
|   +-- album               -> a specific album
+-- service (which service this points at)
+-- reference (the ID, URL, or identifier)
+-- label (user-friendly name)
+-- weight (float, 0-1 -- how much influence this source has)
```

Example generation request:

```json
{
  "generator": "listening_history_analysis",
  "sources": [
    {"source_type": "my_history", "weight": 1.0},
    {"source_type": "playlist", "service": "spotify",
     "reference": "spotify:playlist:abc123",
     "label": "To Listen", "weight": 0.8},
    {"source_type": "external_user", "service": "lastfm",
     "reference": "some_user", "weight": 0.3}
  ],
  "params": {
    "track_count": 30,
    "novelty_bias": 0.7
  }
}
```

Generators declare which source types they consume. Users can combine sources freely and weight them.

### Planned Generators

1. **Listening History Analysis** (first to build)
   - Aggregates play data from all connected history sources
   - Builds taste profile: most-played artists/tracks, genre clustering, recency weighting, gap detection
   - Finds tracks matching the profile that the user hasn't heard
   - Sources candidates from Spotify recs API, Last.fm/ListenBrainz similar artists, deep cuts

2. **Concert Prep** -- top tracks for upcoming show artists
3. **Local Scene Discovery** -- area concerts cross-referenced with taste profile
4. **New Release Radar** -- new releases from followed artists
5. **Discovery from Social Signals** -- artists from followed users' activity
6. **Deep Cuts** -- unheard tracks from known and loved artists

## Sync System

A sync is a long-running job that pulls data from an external service into the local database. Each sync is tracked as a first-class entity with progress reporting.

```
SyncJob
+-- id (UUID)
+-- user_id (FK)
+-- service_connection_id (FK)
+-- sync_type (enum: full, incremental)
+-- status (enum: pending, running, completed, failed)
+-- progress_current (int)
+-- progress_total (int, nullable -- may not be known upfront)
+-- progress_stage (string, e.g. "Fetching listening history", "Resolving artists")
+-- error_message (nullable)
+-- started_at / completed_at
+-- items_created / items_updated (counts)
```

### Behavior

- **First sync** is always full -- pulls complete history. Progress reported in stages: fetching pages -> resolving tracks -> matching artists -> importing tags.
- **Subsequent syncs** are incremental -- only pull data since the last sync's high-water mark.
- **Progress polling** via `GET /api/v1/sync/status` -- UI polls to show progress bar and stage indicator. Can upgrade to SSE/WebSocket later.
- **Rate limiting** -- each connector knows its service's rate limits. Backs off as needed, reflects throttling in progress stage.
- **Concurrency** -- syncs for different services run in parallel. Same-service syncs are queued. Redis-based locking coordinates across replicas.
- **Execution** -- async background tasks within the FastAPI process initially. Can move to a task queue (arq) if needed without changing the data model or API.

## API Design

Versioned under `/api/v1/`. All endpoints return JSON.

### Auth & Account

```
GET    /api/v1/auth/{service}              -> Initiate OAuth flow
GET    /api/v1/auth/{service}/callback     -> OAuth callback
POST   /api/v1/auth/logout                 -> End session

GET    /api/v1/account                     -> Current user profile
GET    /api/v1/account/connections          -> List connected services
DELETE /api/v1/account/connections/{id}    -> Unlink (blocked if last)
```

### Music Data

```
GET /api/v1/artists                        -> Search/browse artists
GET /api/v1/artists/{id}                   -> Artist detail + tags
GET /api/v1/listening-history              -> Aggregated history (paginated)
GET /api/v1/events                         -> Upcoming concerts (filtered by location)
```

### Playlists

```
GET  /api/v1/generators                    -> List available generators for current user
GET  /api/v1/generators/{name}/params      -> Describe parameters for a generator

POST /api/v1/playlists/generate            -> Create playlist (generator + params)
GET  /api/v1/playlists                     -> List generated playlists
GET  /api/v1/playlists/{id}                -> Playlist detail + tracks
POST /api/v1/playlists/{id}/push           -> Push/sync playlist to Spotify
```

### Data Sync

```
POST /api/v1/sync/{service}                -> Trigger data refresh from a service
GET  /api/v1/sync/status                   -> Status of ongoing syncs
```

## Project Structure

```
resonance/
+-- pyproject.toml
+-- Dockerfile
+-- Makefile
+-- alembic/
|   +-- alembic.ini
|   +-- versions/
+-- src/resonance/
|   +-- __init__.py
|   +-- app.py                  # FastAPI app factory
|   +-- config.py               # Pydantic Settings
|   +-- api/
|   |   +-- v1/
|   |       +-- auth.py
|   |       +-- account.py
|   |       +-- playlists.py
|   |       +-- sync.py
|   |       +-- ...
|   +-- connectors/
|   |   +-- base.py             # ABC + capability enum
|   |   +-- spotify.py
|   |   +-- lastfm.py
|   |   +-- ...
|   +-- generators/
|   |   +-- base.py             # Generator ABC + InputSource
|   |   +-- listening_history.py
|   |   +-- ...
|   +-- models/                 # SQLAlchemy models
|   +-- sync/                   # Sync job runner + progress
|   +-- templates/              # Jinja2 templates
+-- tests/
+-- .github/workflows/
```

No `k8s/` directory -- all deployment configuration lives in `megadoomer-config`.

## Deployment

### Kubernetes (megadoomer-config)

```
applications/
+-- services/resonance/do/
    +-- config.json              # ArgoCD discovery
    +-- kustomization.yaml       # app-template + bitnami postgres + redis
    +-- helm-values.yaml
    +-- ingress patches
```

- **app-template** Helm chart (v4.6.2) for the Resonance application.
- **Bitnami PostgreSQL** Helm chart for the database.
- **Redis** via Bitnami chart or app-template.
- Ingress via reusable components from `components/ingress/` with hostname patches for `resonance.megadoomer.io`.
- SealedSecrets for OAuth client credentials, database URL, session signing key, token encryption key.
- Designed for multiple replicas -- sessions in Redis, sync coordination via Redis locking.

### CI/CD (GitHub Actions)

**On PR:**
- Run tests, ruff (lint + format check), mypy (strict)

**On merge to main:**
1. Build Docker image (multi-stage)
2. Tag with `project-version` format: `YYYYMMDDTHHmmSS-<7char-sha>` (e.g. `20260330T143022-a1b2c3d`)
3. Push to container registry
4. **Cross-repo promotion:** Check out `megadoomer-config`, update the image tag in `helm-values.yaml` using `yq`, commit and push
5. ArgoCD detects the config change and deploys

### Configuration

- App config via environment variables (12-factor).
- Pydantic `Settings` class for validation and defaults.
- Secrets via Kubernetes SealedSecrets.
- Non-secret config via ConfigMap.

### Database Migrations

- Alembic migrations run as init container before the app starts.
- Ensures schema is current before traffic is served.

### Health Check

- `/healthz` endpoint for Kubernetes liveness/readiness probes.

## Open Questions

- **Container registry:** Which registry to push images to? (GHCR, DigitalOcean, other)
- **Spotify Events API:** Does the Spotify API expose event/concert data? Needs verification.
- **Foopee integration:** No known API -- likely requires web scraping. Worth investigating feasibility.
- **Scrobbling endpoint:** Should Resonance act as a scrobbling target? (mentioned as a possibility)
- **Email recovery:** Deferred to a future version. Design the email field on User now, implement recovery flow later.
