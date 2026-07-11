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

# Front-end unit tests (vitest) for the lineup builder's core logic.
# Requires Node; `npm ci` once, then:
npm test

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
uv run resonance-api generate <profile-id> [--freshness 50]       # regenerates in place
uv run resonance-api profile exclude-track <profile-id> <track-id>... [--regenerate]
uv run resonance-api enrich <profile-id> --lineup [--n 5]          # top up related artists
uv run resonance-api enrich <profile-id> --seed <artist-id> [--n 5]
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
# RESONANCE_URL and RESONANCE_API_TOKEN are loaded from the user's dotfiles.
# The token is also in 1Password: "resonance.megadoomer.io" item, Private vault.
# Alternatively, read it from the k8s secret backing the ADMIN_API_TOKEN env var.

# Available commands
uv run resonance-api healthz                    # Health + deployed revision
uv run resonance-api status                     # Recent sync job overview
uv run resonance-api stats                      # Database statistics
uv run resonance-api sync <service> [--full]    # Trigger a sync
uv run resonance-api dedup <type> [--no-wait]   # Dedup: events|artists|tracks|all
uv run resonance-api task <task_id>             # Check bulk task status
uv run resonance-api track <query>              # Search tracks by title
uv run resonance-api taste genres [--limit N]   # Top genres across the library
uv run resonance-api taste like <genre-mbid> [--limit N]  # Artists most defined by a genre
uv run resonance-api profile list               # List generator profiles
uv run resonance-api profile create ...         # Create a generator profile
uv run resonance-api generate <profile-id>      # Generate (regenerates in place)
uv run resonance-api profile exclude-track <profile-id> <track-id>... [--regenerate]  # Exclude tracks from the recipe
uv run resonance-api enrich <profile-id> --lineup | --seed <id>  # Add related artists
uv run resonance-api playlists                  # List playlists
uv run resonance-api playlist <playlist-id>     # Show playlist details
uv run resonance-api import concert_archives --file <path> [--export-date YYYY-MM-DD] [--wait]
uv run resonance-api api [METHOD] PATH [-d DATA] [-H HDR]  # Raw API request
uv run resonance-api set-role <user_id> <role>  # Set role — direct DB

# Global flag: --as-user <id> assumes a user identity on user-scoped endpoints
# (admin token only; for agent live testing). Sends the X-Assume-User header.
uv run resonance-api --as-user <user_id> api POST /api/v1/generator-profiles/ -d '{...}'
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
- PRs can be opened ready for review (not draft) — no need to default to draft here
- Use merge commits when merging PRs (not squash or rebase) — keeps related commits grouped so it's clear which commits belong to the same feature
- These overrides supersede the global "always use a feature branch" and "prefer draft PRs" rules

## Conventions

- **API-first**: all data must be accessible via JSON API endpoints under `/api/v1/` — the UI is a rendering layer on top of the API, not a separate data path
- **CLI-to-API mapping**: CLI subcommands map directly to API paths. `resonance-api <command>` hits `/api/v1/<domain>/<command>`. Admin commands use the `/api/v1/admin/` prefix; sync commands use `/api/v1/sync/`; etc. When adding new CLI commands, follow this pattern.
- **OpenAPI spec**: FastAPI serves Swagger UI at `/docs` and the raw spec at `/openapi.json` in all environments. Will be gated behind `debug=True` once feature-complete.
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
- **Track selection is weighted round-robin (reverses #128)**: `concert_prep._select_one_pool` deals each pool artist's tracks round by round until `max_tracks`, so every artist on the bill gets an even share and a heavy-rotation neighbor can't monopolize the playlist. `composite_score` (familiarity/hit_depth) decides WHICH of an artist's tracks fill its slots, round-robin decides HOW MANY. A short band's slack redistributes to the others; `max_tracks < n_artists` keeps the top by score. `weights` (artist_id → tracks per round, default 1 = even) is a real plumbed seam for future seed/headliner emphasis. This reverses #128's round-0 + pure-score fill, which buried the seed band.
- **Lineup builder is a server-backed profile editor (#133)**: `/playlists/new` eagerly creates a `draft` `GeneratorProfile` and redirects to `/playlists/{id}/edit`; the same editor edits existing profiles. Every edit PATCHes `input_references` (and parameters/name) via `/api/v1/generator-profiles/{id}`; "Generate" is a separate action that flips a draft `active`. The profile list shows only `active` profiles. Client state logic is the pure ES module `static/lineup.core.js` (unit-tested with **vitest** under `tests/js/`); `static/lineup.js` is the DOM/network controller. `ui/playlists._hydrate_lineup` turns stored `input_references` into named, grouped rows for the editor.
- **Related-artist enrichment (#133)**: `POST /api/v1/generator-profiles/{id}/enrich {seed_artist_ids: [...] | "lineup", n}` resolves related artists and persists them into the pool as concrete `artist` sources tagged with `via_seed` (`"<seed_id>"` per-seed, `"lineup"` global). It runs as a `RELATED_ARTIST_ENRICHMENT` worker task; re-running a scope **tops up to N active** (counts the non-excluded discoveries already in the scope, fetches only the difference, appends rather than replaces, never re-suggests a globally-excluded artist). A scope already at/over N is a no-op (curated discoveries are never trimmed); connectors running dry is a partial success. There is NO generation-time related expansion anymore (the old `similar_artist_ratio` slider is gone).
- **Profile concurrency (#133)**: `GeneratorProfile.version` is the SQLAlchemy `version_id_col`. The editor sends `expected_version` on PATCH (409 + conflict banner on mismatch); the enrich worker reloads and re-applies on a version conflict so concurrent edits merge instead of clobbering. Generation and enrichment are mutually exclusive per profile (409).
- **Regenerate is in place on the same Playlist row**: `worker.score_and_build_playlist` reuses the profile's current `Playlist` (via the latest `GenerationRecord`) instead of creating a new one, so the row identity — and the Spotify export anchor in `service_links` — survives. Old `PlaylistTrack` rows are replaced; only the first generation (or a deleted playlist) creates a fresh row. UI: a "Regenerate" button on the playlist detail page; API: re-POST `/{id}/generate`. `max_tracks`/`freshness_target` live on the `PLAYLIST_GENERATION` task, so a regenerate must re-pass them.
- **Version history is data-only (#versions, D6)**: each generation records its ordered track list on `GenerationRecord.track_snapshot` (`list[str]` of track uuids). This is the durable per-version history AND the freshness baseline for the next regenerate — `score_and_build` reads the **latest snapshot** for `previous_track_ids`, NOT the live `PlaylistTrack` rows (reading the rows it's about to overwrite would self-poison the freshness target). Browse/restore UI is deferred; the data is captured so it's recoverable later.
- **Track exclusions live on the recipe (#track-exclude)**: `input_references.exclude_track_ids` (parse/serialize via `pool.extract_track_excludes` / `with_track_excludes`; validated on profile PATCH). `pool.py` is artist-only/pre-track, so the exclude is applied as a **candidate filter** in `score_and_build` (an excluded track never becomes a candidate; its band deals its next-best on regenerate). UI: per-row ✕ on the detail page writes it (mark-then-regenerate). CLI: `resonance-api profile exclude-track <id> <track-id>... [--regenerate]`.
- **Artist similarity is durable data, not a cache**: `models.taste.ArtistSimilarity` stores connector-reported artist→neighbor edges (`source_artist_id`, `connector`, `neighbor_name`/`mbid`, `rank`, `fetched_at`). The enrich worker reads stored edges first and falls back to a live fetch, recording the result; `fetched_at` drives refresh-if-old, never eviction.
- **Genre tags are durable data, not a cache (#136)**: `models.music.ArtistTag` stores MusicBrainz tags per artist (`tag`, `genre_mbid`, `count`, `source`, `fetched_at`), fetched by `GENRE_BACKFILL` via the ListenBrainz artist-metadata endpoint. `genre_mbid` is non-NULL only for canonical genres (NULL = free folksonomy tag), so genre-vs-noise is data-driven, not a stoplist. `genre_attempted_at` on the artist is the resume marker. Partial index on `genre_mbid WHERE genre_mbid IS NOT NULL` serves discovery queries.
- **Genre affinity is a pure primitive (#136)**: `generators/genre.py` (`affinity_score`, count-weighted cosine over `genre_mbid` vectors, each seed L2-normalized before aggregation; returns `None` for no-basis vs `0.0` for mismatch). `genre.sort_value` turns an affinity into a rank key that keeps unknown (`None` → neutral `0.0`) distinct from mismatch (`0.0` → sinks to `-1.0`), so an untagged possible match never ranks below a known off-genre one — shared by the disambiguation picker (#136) and the enrich genre guard (#153). Every genre consumer plugs into it. Affinity overlaps only on the *exact* `genre_mbid` (within-genre centrality), NOT cross-genre adjacency — that needs a genre-similarity model (a later arc).
- **On-demand genre tag fetch is a shared primitive (#152)**: `sync/backfill.fetch_and_persist_tags(session, client, artists)` fetches tags for the subset that still need them (`genre_attempted_at IS NULL` + valid MBID) in one call, then wholesale-replaces each artist's `ArtistTag` rows and stamps `genre_attempted_at` (`_persist_artist_tags`, the same per-artist body `run_genre_backfill` uses). No scan/resume loop — the caller passes a bounded set, so it runs on a request hot path or in a worker before the scheduled `GENRE_BACKFILL` reaches those artists. Graceful-degrade: returns `False` (writes nothing) on `ArtistTagsUnavailableError`. Artist search (`api/v1/artists.search_artists`) calls it via `_ensure_seed_tags` for the builder's seeds (bounded to `_SEED_TAG_FETCH_CAP=3`, wrapped in a `_SEED_TAG_FETCH_TIMEOUT=5s` deadline; rolls back + degrades on timeout/HTTP error), so a seed that gained an MBID after the last backfill still ranks candidates by genre.
- **Enrich has a related-expansion genre guard (#153)**: when the enrich worker tops up a scope with imported (not library) artists, `_collect_related` re-ranks the import candidates by genre affinity to the scope's seed profile before importing — off-genre neighbors of a wrong/ambiguous seed sink, no-genre-data stays neutral (never penalized), similarity rank breaks ties (`_rank_candidates_by_genre` + `genre.sort_value`). Import candidates are pre-import `(name, MBID)` pairs with no `ArtistTag` rows, so their tags are fetched by MBID on demand (ephemeral, never persisted) via `_genre_rank_candidates`; the seed profile comes from `_load_genre_pairs`, and the seeds/core are first topped-up via `fetch_and_persist_tags` (pairs with #152). Library-adjacent matches keep provider rank (already high-confidence). Best-effort throughout: any tag-fetch failure degrades to the unguarded similarity order, never blocking enrichment.
- **Genre discovery is read-only, API-first, library-wide (#154 Arc 2 P1)**: `/api/v1/taste` (`taste.py`) + `resonance-api taste`. Three surfaces: top genres (`GET /taste/genres` — one GROUP-BY aggregation that also feeds the browse filter's options), browse/filter (`GET /artists?genre_mbid=...`, repeatable, OR-match via correlated `EXISTS ... IN`), and "more like this genre" (`GET /taste/genres/{mbid}/artists` — ranked by genre centrality, in-library first, reusing `rank_search_candidates`). The reusable `ui.filters.MultiSelectExistsField` (OR-match over a related table) wires genre filtering into the artist filter bar. The shared genre/artist helpers (`load_artist_tags`, `format_artist_summary`, `display_genres`, `genre_pairs`) live in `api/v1/artists.py` as public primitives. UI stats page + in-page genre widget are P1.4 (deferred).
- SQLAlchemy models use UUID primary keys
- OAuth tokens encrypted at rest via Fernet
- All connections (OAuth, username-based, URL-based, file-upload) use the unified `ServiceConnection` model — there is no separate calendar feed or import model
- Task lifecycle helpers (`sync/lifecycle.py`) provide `complete_task`/`fail_task` — use these instead of setting task status inline
- Orphan recovery in the worker is type-agnostic — new task types are handled by adding an entry to `_TASK_DISPATCH`
- Playlist export uses `PLAYLIST_WRITE` capability — connectors declare it to enable export. Export creates a background `PLAYLIST_EXPORT` task per connection, tracked via `playlist.service_links`
- Track matching: export uses two-pass resolution. First, tracks with MusicBrainz recording MBIDs are resolved via MB URL relations (authoritative, community-curated). Second, unresolved tracks fall back to Spotify text search. Matches from both passes are persisted in `service_links["spotify"]` for future reuse.
- Sync staleness: compare `playlist.updated_at` vs `service_links[connection].exported_at` — no external API calls needed
- **Per-track Spotify sync visibility (#spotify-sync-visibility)**: `export_playlist` stamps `PlaylistTrack.spotify_synced_at` after the push — matched tracks get the timestamp, unmatched are cleared (re-match is not monotone). Drives the per-row SP badge on the detail page and the "N of M synced · Exclude unsynced & regenerate" affordance (excludes every unsynced track, then regenerates). A regenerated track starts unsynced (correct — a new version hasn't been exported).
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
- `ADMIN_ASSUME_USER_ENABLED` — when true (default), the admin token may assume a user identity on user-scoped endpoints via the `X-Assume-User` header or `?as_user=` query param (agent-first live testing, #135). Every assumption is audit-logged. Set false to disable.
- `DEX_CLIENT_ID`, `DEX_CLIENT_SECRET` — Dex OIDC credentials (for GitHub login via Dex broker)
- `DEX_ISSUER_URL` — Dex issuer URL (e.g., `https://dex.megadoomer.io`). When empty, the GitHub connector is not registered and the login button does not appear.

## Design System

Always read DESIGN.md before making any visual or UI decisions.
All font choices, colors, spacing, and aesthetic direction are defined there.
Do not deviate without explicit user approval.
In QA mode, flag any code that doesn't match DESIGN.md.
