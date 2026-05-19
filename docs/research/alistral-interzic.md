# Research: Alistral / Interzic

**Source:** [RustyNova016/Alistral](https://github.com/RustyNova016/Alistral)
**License:** MIT (fully permissive — can borrow algorithms, patterns, and port code with attribution)
**Language:** Rust (concepts and algorithms are portable to Python)
**Date:** 2026-05-18

## What Alistral Is

Alistral is a CLI-based music tool suite built around ListenBrainz and MusicBrainz.
It's a Rust monorepo with several crates:

| Crate | Purpose |
|-------|---------|
| `alistral_cli` | CLI app with radio/playlist generation, stats, daily recaps |
| `alistral_core` | Data structures for listen history, entity-with-listens, ordering |
| `interzic` | **Cross-service ID mapping layer** — maps recordings between services |
| `musicbrainz_db_lite` | Local SQLite cache of MusicBrainz API data |
| `symphonize` | Music provider abstraction (Spotify, YouTube, Subsonic, ListenBrainz) |

## Relevant Findings

### 1. Interzic — Cross-Service ID Mapping via MusicBrainz URL Relations

**The key insight:** MusicBrainz recordings have curated URL relations to their equivalents
on Spotify, YouTube, and other services. Interzic uses these to resolve cross-service IDs
instead of relying on text-search matching.

**How it works:**
1. A `MessyRecording` stores `(title, artist_credits, release, mbid)` — identified by strings
2. The MusicBrainz API is queried with `?inc=url-rels` to get URL relations for a recording
3. URLs are parsed to extract service-specific IDs (e.g., Spotify track ID from a spotify.com URL)
4. Mappings are cached in an `external_id` table: `(recording_id, ext_id, service, user_overwrite)`
5. Users can override bad auto-mappings per service

**Schema (SQLite):**
```sql
CREATE TABLE recording (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    artist_credits TEXT NOT NULL,
    release TEXT,
    mbid TEXT UNIQUE
);
CREATE TABLE external_id (
    id INTEGER PRIMARY KEY,
    recording_id INTEGER REFERENCES recording(id),
    ext_id TEXT NOT NULL,
    service TEXT NOT NULL,
    user_overwrite TEXT DEFAULT ''
);
```

**Relevance to Resonance:**
- We currently use Spotify text search (`track:{title} artist:{artist}`) for playlist export matching
- Text search is prone to false matches (wrong version, live vs studio, covers)
- MusicBrainz URL relations are community-curated and more reliable
- We could use MB URL relations as a first-pass lookup, falling back to text search when no MB relation exists
- This would improve our track matching accuracy significantly

### 2. Radio/Playlist Generation Algorithms

Alistral has four radio algorithms. All follow a common pipeline pattern:
**Seed → Filter → Sort → Collect → Export**

#### a) Artist Circles
Pick a random listen → get its artist → add a random recording by that artist.
Option to blacklist already-listened recordings (discovery mode).

**Pattern:** Stay in the user's taste zone while discovering new tracks by familiar artists.
Similar to our `ARTIST_DEEP_DIVE` generator type concept.

#### b) Underrated Tracks
Score = (rank in user's top 1000) + (user_listens / worldwide_listens * 100).
Finds tracks the user loves that the broader community doesn't listen to.

**Pattern:** Uses ListenBrainz's global listen count API to compare personal vs global popularity.
Could be a compelling "hidden gems" generator type.

#### c) Overdue Listens
Tracks that are "overdue" for a re-listen based on the user's historical listen frequency.
Pipeline: min-listen filter → cooldown filter → timeout filter → sort by overdue factor.

**Pattern:** Maps directly to our `REDISCOVERY` generator type.
The "overdue factor" is: (time since last listen) / (average time between listens).
Tracks with high overdue factors are ones you usually listen to regularly but haven't recently.

#### d) Listen Rate
Orders tracks by listen frequency (listens per time period), surfacing tracks with the lowest
rates — things you've listened to a few times but then forgot about.

**Pattern:** Complementary to overdue — listen rate surfaces "abandoned" tracks, while
overdue surfaces tracks that broke their pattern.

### 3. musicbrainz_db_lite — Local MusicBrainz Cache

Caches MusicBrainz API responses in SQLite to work around the 1 req/sec rate limit.
Uses a simplified version of the official MusicBrainz schema, with fields named after
the API's conventions (not the raw DB schema).

**Relevance:** We fetch from MusicBrainz for artist matching, event lookups, and track
discovery. A caching layer could significantly speed up repeated lookups and reduce
rate limit pressure. Not directly portable (Rust/SQLite) but the caching pattern is sound —
we could implement something similar with Redis or a PostgreSQL table.

### 4. Daily Recap Feature

Shows:
- **Track birthdays:** "You first listened to X exactly N years ago today"
- **Discovery anniversaries:** "You discovered artist Y on this date"
- **Latest releases:** New releases from artists in your listening history

**Relevance:** Engagement feature idea for Resonance. We have listen history from
Last.fm and ListenBrainz — we could compute anniversaries and surface them in the UI.

### 5. Pipeline Architecture Pattern

All radio algorithms share a composable pipeline: `Seeder → Filters → Sorter → Collector`

- **Seeders**: Sources of recordings (user's listen history, specific time range, etc.)
- **Filters**: Composable filters (min listen count, cooldown period, timeouts)
- **Sorters**: Scoring/ordering algorithms (overdue factor, listen rate, underrated score)
- **Collectors**: Output limiters (max count, max duration)

This is conceptually close to our generator/scoring architecture but more composable.
Our generators could benefit from extracting the filtering/sorting into reusable components.

## What We Can't Use

- **The Rust code directly** — different language, different ORM, different async runtime
- **musicbrainz_db_lite as a dependency** — Rust crate, SQLite-based (we use PostgreSQL)
- **symphonize** — early-stage abstraction, not mature enough to learn from

## Comparison with Resonance

| Feature | Alistral | Resonance |
|---------|----------|-----------|
| Track matching | MB URL relations → service IDs | Spotify text search |
| Canonical ID hub | MusicBrainz MBID | None (Spotify-centric) |
| Service link storage | Separate `external_id` table | JSONB `service_links` column |
| Generator pipeline | Seed → Filter → Sort → Collect | Generator types with scoring |
| Listen history source | ListenBrainz only | Last.fm, ListenBrainz, Spotify |
| Event/concert context | None | Core feature (concert_prep) |
| Export targets | ListenBrainz, Subsonic, YouTube | Spotify |
| UI | CLI only | Web (HTMX/Jinja2) |

## Attribution

If we adopt any patterns from Alistral, include attribution in the relevant source file:
```python
# Algorithm inspired by Alistral (https://github.com/RustyNova016/Alistral)
# MIT License, Copyright (c) 2024 RustyNova
```
