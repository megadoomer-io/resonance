# Playlist Generation System Design

Design for Resonance's playlist generation system. Focused on the "concert
prep" generator as the first implementation, with a flexible architecture that
supports future generator types.

---

## Core Concepts

The generator system has five main entities:

**GeneratorProfile** -- a saved configuration that produces a playlist. Stores
the generator type, input references, and parameter values. This is the
"recipe" a user can re-run. Profiles accumulate a version history of generated
playlists over time.

**Playlist** -- an ordered track list with metadata. Source-agnostic: a playlist
can be generated from a profile, imported from an external service, manually
curated, or derived from another playlist. Not coupled to the generator system.

**GenerationRecord** -- links a Playlist to the GeneratorProfile run that
created it. Captures a frozen snapshot of parameter values, generation-time
options (like freshness target), measured results (actual freshness), timing,
and track source breakdown. This is the version link that enables history
browsing and diffing.

**GeneratorParameter** -- a named, typed parameter defined in code (not in the
database). Each parameter has a scale type, default value, and display
metadata. The full set of parameters is the system's vocabulary; generator
types declare which are featured.

**GeneratorType** -- an enum of available generator contexts. Each type
declares which parameters are featured (shown by default in the UI) and which
inputs it requires. The full parameter registry is always available via "show
all options."

---

## Generator Parameter Registry

Parameters are defined in code as a registry. Each definition includes:

- `name` -- machine name, used as JSON key in parameter_values
- `display_name` -- human-facing label
- `description` -- tooltip/help text
- `scale_type` -- `bipolar` or `unipolar`
- `default_value` -- 50 for bipolar (neutral center), varies for unipolar
- `labels` -- endpoint labels, e.g., `("Deep Cuts", "Big Hits")`

### Scale Types

**Bipolar** parameters have two opposing concepts on one axis with a meaningful
center point. The center (50) means "agnostic / no preference." Pulling toward
0 actively boosts one quality; pulling toward 100 actively boosts the opposite.

**Unipolar** parameters are a single concept from "none" (0) to "maximum"
(100). There is no opposing concept.

### v1 Parameters (Concert Prep)

| Parameter | Scale | Default | Low end (0) | High end (100) |
|-----------|-------|---------|-------------|----------------|
| `familiarity` | bipolar | 50 | All discovery | All known tracks |
| `hit_depth` | bipolar | 50 | Deep cuts | Big hits |
| `similar_artist_ratio` | unipolar | 0 | Target artists only | Heavy adjacent artists |

### Future Parameters

These are not built in v1 but should be kept in mind for the architecture:

| Parameter | Scale | Likely generator types |
|-----------|-------|-----------------------|
| `era_spread` | bipolar | Artist deep dive, rediscovery -- old catalog vs recent |
| `genre_coherence` | unipolar | Genre-based -- how strictly to stay in genre |
| `mood_consistency` | unipolar | Any -- keep energy/mood uniform vs varied |
| `compilation_inclusion` | unipolar | Discography -- include compilations, live albums |
| `feature_inclusion` | unipolar | Discography -- include tracks where artist is featured |

Adding a new parameter means adding it to the registry and updating the
scoring logic that reads it. No migrations needed since parameter values live
in JSON columns.

---

## Data Model -- New Tables

### Playlist

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| name | String(512) | User-facing name |
| description | Text | Nullable |
| track_count | Integer | Denormalized count |
| is_pinned | Boolean | Whether the user has pinned this as a keeper |

### PlaylistTrack

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| playlist_id | UUID FK | References playlists |
| track_id | UUID FK | References tracks |
| position | Integer | Order in playlist |
| score | Float | Nullable, composite score that placed this track |
| source | String(64) | How this track was sourced: `library`, `discovery`, `manual` |

### GeneratorProfile

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| user_id | UUID FK | References users |
| name | String(512) | User-facing name |
| generator_type | GeneratorType enum | `concert_prep`, `artist_deep_dive`, etc. |
| input_references | JSON | Type-specific: `{"event_id": "..."}` or `{"artist_id": "..."}` |
| parameter_values | JSON | `{"hit_depth": 75, "familiarity": 40, ...}` |
| auto_sync_targets | JSON | Nullable, future: `[{"service": "spotify", "playlist_id": "..."}]` |

### GenerationRecord

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | Primary key |
| profile_id | UUID FK | References generator_profiles |
| playlist_id | UUID FK | References playlists |
| parameter_snapshot | JSON | Frozen copy of parameter values used |
| freshness_target | Integer | Nullable, 0-100 |
| freshness_actual | Float | Nullable, measured result |
| generation_duration_ms | Integer | How long generation took |
| track_sources_summary | JSON | `{"library": 18, "discovery": 12}` |

---

## Connector Changes -- TRACK_DISCOVERY Capability

New capability added to `ConnectorCapability`: `TRACK_DISCOVERY`.

Connectors implementing it provide:

```python
async def discover_tracks(
    self,
    artist_name: str,
    service_links: dict[str, str] | None,
    limit: int = 20,
) -> list[DiscoveredTrack]
```

`DiscoveredTrack` is a lightweight dataclass: title, artist name, duration,
external ID, and a popularity score (0-100, service-defined). These get
matched against existing Track entities or created as new ones during
generation.

### First implementation: ListenBrainz / MusicBrainz

MusicBrainz has artist recordings with metadata. ListenBrainz has listening
statistics that can approximate popularity. The connector:

1. Looks up the artist in MusicBrainz by name or MBID (from `service_links`)
2. Fetches their recordings
3. Optionally enriches with ListenBrainz listen counts for popularity scoring

Spotify is deferred as a `TRACK_DISCOVERY` source due to dev mode rate limits.
Can be added as a second implementation later.

### Multi-source discovery

When multiple connectors support `TRACK_DISCOVERY`, the generator queries all
via `ConnectorRegistry.get_by_capability()` and merges results by matching on
track title + artist. The highest popularity score wins for ranking.

---

## Scoring Algorithm

Each candidate track receives a composite score from 0.0 to 1.0, computed
from signal functions weighted by parameter values.

### Signal Functions (v1)

| Signal | Inputs | Output | Influenced by |
|--------|--------|--------|---------------|
| `familiarity_signal` | Listen count, exists in library | 0.0 (never heard) to 1.0 (most played) | `familiarity` |
| `popularity_signal` | Discovery popularity or listen rank | 0.0 (obscure) to 1.0 (biggest hit) | `hit_depth` |
| `artist_relevance_signal` | Target artist or adjacent? | 1.0 (target) to 0.0 (distant) | `similar_artist_ratio` |

### How Parameter Values Translate to Weights

**Bipolar** parameters map to a weight direction:
- Value 75 -> boost signal by +0.5 (favor high end)
- Value 25 -> boost signal by -0.5 (favor low end, invert signal)
- Value 50 -> signal has zero influence (neutral)

**Unipolar** parameters map to a threshold or blend ratio:
- Value 0 -> exclude that dimension entirely
- Value 30 -> up to ~30% of tracks can come from that source

The composite score is a weighted sum of all signals. Tracks are sorted by
score, the freshness filter removes tracks from the previous version per the
freshness target, and the top N tracks become the playlist.

---

## Task Hierarchy

Playlist generation uses the existing arq task infrastructure with a
parent/child hierarchy for checkpointing and crash recovery.

### New TaskTypes

- `PLAYLIST_GENERATION` -- parent task, orchestrates the full generation
- `TRACK_DISCOVERY` -- child task per artist needing external lookup
- `TRACK_SCORING` -- child task for scoring and selection (runs after all
  discovery children complete)

### Flow

```
PLAYLIST_GENERATION (parent)
  |
  +-- TRACK_DISCOVERY (child, artist A)  -- may hit rate limits, deferred
  +-- TRACK_DISCOVERY (child, artist B)
  +-- TRACK_DISCOVERY (child, artist C)
  |
  +-- TRACK_SCORING (child, runs when all discovery complete)
        |
        +-- Creates Playlist + GenerationRecord
        +-- Marks parent COMPLETED
```

If a discovery task hits a rate limit, it gets deferred and retried -- same
mechanism as sync tasks. The parent monitors children and only proceeds to
scoring when all discovery is complete or has failed gracefully. Crash recovery
works automatically via `_TASK_DISPATCH`.

---

## Data Flow -- Concert Prep Generator

End-to-end walkthrough of the "going to a show" use case:

**Input:** User selects an Event from their synced concerts. The generator
resolves the Event's confirmed artists (via EventArtist) and pending
candidates (EventArtistCandidate with accepted status).

**Track sourcing** happens in two passes:

1. **Library pass** -- query the user's ListeningEvents,
   UserTrackRelations (likes/loves), and UserArtistRelations
   (follows/favorites) for tracks by the event's artists. Each track gets
   scored by familiarity signals: listen count, recency, liked/loved status,
   artist follow status.

2. **Discovery pass** -- for artists where the library pass returned few or
   no tracks, spawn TRACK_DISCOVERY child tasks to fetch external track data
   via connectors implementing `TRACK_DISCOVERY`. ListenBrainz/MusicBrainz
   first. Each discovered track gets scored by popularity signals from the
   external source.

**Scoring and selection:** Every candidate track gets a composite score based
on the GeneratorParameter values. The freshness control filters out tracks
from the previous Playlist version according to the target percentage.

**Output:** An ordered track list becomes a new Playlist, linked to the
profile via a GenerationRecord.

---

## API Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/api/v1/generator-profiles` | Create a new profile |
| `GET` | `/api/v1/generator-profiles` | List user's profiles |
| `GET` | `/api/v1/generator-profiles/{id}` | Profile with generation history |
| `PATCH` | `/api/v1/generator-profiles/{id}` | Update parameters or name |
| `DELETE` | `/api/v1/generator-profiles/{id}` | Delete a profile |
| `POST` | `/api/v1/generator-profiles/{id}/generate` | Trigger generation |
| `GET` | `/api/v1/playlists` | List user's playlists |
| `GET` | `/api/v1/playlists/{id}` | Playlist with tracks |
| `GET` | `/api/v1/playlists/{id}/diff/{other_id}` | Compare two versions |

---

## CLI Commands

```bash
# Profile management
resonance-api profile create --type concert_prep --event <id> --name "Show prep"
resonance-api profile list
resonance-api profile show <profile-id>
resonance-api profile update <profile-id> --param hit_depth=75 --param familiarity=30
resonance-api profile delete <profile-id>

# Generation
resonance-api generate <profile-id> [--freshness 50]

# Playlist inspection
resonance-api playlists
resonance-api playlist <playlist-id>
resonance-api playlist diff <playlist-id> <other-playlist-id>
```

---

## UI -- Minimal Viable Surface

Built with existing Jinja2 + HTMX patterns.

**Generator Profiles list** (`/generators`) -- saved profiles with name, type,
last generated date, and a "Generate" button.

**Profile create/edit** (`/generators/new`, `/generators/{id}/edit`) -- form
with name, generator type selector, type-specific input selector (event
picker for concert prep), featured parameter sliders for the selected type,
"Show all parameters" expandable section, and preset selector.

**Playlist view** (`/playlists/{id}`) -- track list with position, title,
artist, source badge (library/discovery), and score. Header shows generation
metadata: profile link, parameter snapshot, freshness stats, timestamp.
Previous/next version navigation.

**Profile detail** (`/generators/{id}`) -- current parameter values, generation
history as a version list, "Generate now" button with freshness option.

**Integration points:**
- Event detail page: "Create playlist for this show" button
- Dashboard: "Generator Profiles" nav item
- Task status area: generation tasks appear alongside sync tasks

---

## Export Model (Future)

Playlists are internal-first. Export to external services is a separate
action, not part of generation.

**Auto-sync targets** on GeneratorProfile configure automatic push to
external services when a new version is generated. The model is
service-agnostic: `auto_sync_targets` is a JSON array of service + playlist
ID pairs. Spotify is not treated specially in the UX -- any service
implementing `PLAYLIST_WRITE` is an equal export target.

Implementation deferred until the generation output reaches a usable quality
level through CLI/UI iteration.

---

## Future Generator Types

These are noted for future work, not built in v1:

| Type | Input | Description |
|------|-------|-------------|
| `artist_deep_dive` | Artist ID | Deep dive into one artist's catalog with adjacent artists mixed in |
| `rediscovery` | None (uses listening history) | Surface tracks/artists not listened to recently but previously loved |
| `genre_based` | Genre tag(s) | Genre-focused playlist, likely requires MusicBrainz genre metadata |
| `playlist_refresh` | Playlist ID | "I like this playlist but it's getting stale" -- regenerate with freshness control |
| `discography` | Artist ID | Full artist discography in release order |
| `curated_mix` | None (uses recent patterns) | Regularly-generated mix based on recent listening |

### Discography Notes

The discography generator should sort by **original release date** (not
remaster/reissue date). MusicBrainz distinguishes "release group" (the
original work) from individual "releases" (remasters, regional editions).
When albums are eventually modeled in Resonance, both dates should be
captured to enable correct chronological ordering. A 2014 remaster of a 1975
album should slot between the 1973 and 1978 albums.

Parameters for discography are minimal: `compilation_inclusion` and
`feature_inclusion` control whether compilations/live albums and featured
appearances are included.
