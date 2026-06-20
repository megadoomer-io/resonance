"""MusicBrainz MBID backfill core (#71).

Resolves missing MusicBrainz IDs onto library Tracks and Artists and writes them
into ``service_links["musicbrainz"]["id"]``. Phase A (hosted-mapper-first):

- **Tracks** are resolved via the hosted ListenBrainz mapper
  (``MbidMapperClient``), keyed on ``(artist_name, recording_name)``.
- **Artists** are resolved by *harvesting* ``artist_mbids`` from gated track
  matches first (free), then falling back to MusicBrainz ``/ws/2`` artist search
  (the existing connector's ``search_artists``) for any still missing (T1-A).

Shared mechanics live in ``_apply`` (the engine); the two passes differ only in
how they *resolve* an entity to a candidate MBID + name (6A).

Decisions encoded here (from /plan-eng-review):

- **Resume (2A):** each pass loops over rows ``WHERE mb_attempted_at IS NULL`` in
  batches, marking each row attempted and committing per batch. A worker restart
  resumes from the unattempted remainder.
- **Similarity gate (T1-A / 4A):** a match is only written when
  ``normalize.name_similarity(library_name, matched_name) >= threshold``. The
  mapper returns no score of its own, so this is the guard against wrong matches
  (bias: no MBID over wrong MBID). Below the threshold → ``below_similarity``.
- **Write safety (3A):** writes merge into ``service_links`` (sibling keys
  preserved) and never overwrite a *different* existing MBID (``conflict``).
- **Collisions (T5-A):** if two rows in one run resolve to the same MBID, the
  first writes it and later rows are logged + skipped (``collision`` count). They
  are genuine duplicates for ``dedup`` to merge later; not auto-merged here.
- **Transient errors (CRITICAL):** a mapper/search outage leaves
  ``mb_attempted_at`` NULL (NOT ``no_match``) so the row is retried next run.

      TRACK PASS                              ARTIST PASS
   ┌───────────────────────┐              ┌────────────────────────────┐
   │ tracks WHERE attempted │              │ artists WHERE attempted      │
   │   IS NULL (batched)    │   harvest    │   IS NULL (batched)          │
   │ mapper.lookup_recordings│  artist_mbid │ 1. harvested[artist_id]?     │
   │  → gate on artist name │ ───────────► │    → trust (already gated)   │
   │  → write recording mbid│              │ 2. else search_artists +gate │
   └───────────────────────┘              └────────────────────────────┘
"""

from __future__ import annotations

import dataclasses
import datetime
import math
from typing import TYPE_CHECKING, Any

import httpx
import sqlalchemy as sa
import sqlalchemy.orm as orm
import structlog

import resonance.config as config_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.models.music as music_module
import resonance.normalize as normalize_module
import resonance.services.artist_utils as artist_utils
import resonance.services.mbid_mapper as mapper_module
import resonance.types as types_module

if TYPE_CHECKING:
    import uuid

logger = structlog.get_logger()


@dataclasses.dataclass
class BackfillCounts:
    """Per-entity-type outcome tally for one backfill pass."""

    matched: int = 0
    no_match: int = 0
    below_similarity: int = 0
    conflict: int = 0
    collision: int = 0
    transient: int = 0

    @property
    def attempted(self) -> int:
        """Rows whose outcome was recorded (everything except transient)."""
        return (
            self.matched
            + self.no_match
            + self.below_similarity
            + self.conflict
            + self.collision
        )


@dataclasses.dataclass
class Resolution:
    """A candidate resolution for one entity.

    ``mbid`` None means the resolver found no match. ``transient`` True means the
    resolver could not reach its backend for this entity (leave it unattempted).
    ``artist_mbid`` is the harvested artist MBID from a track match (tracks only).
    """

    mbid: str | None = None
    matched_name: str | None = None
    artist_mbid: str | None = None
    transient: bool = False


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC)


def _merge_mbid(service_links: dict[str, Any] | None, mbid: str) -> dict[str, Any]:
    """Return a copy of ``service_links`` with ``musicbrainz.id`` set to ``mbid``.

    Sibling keys (spotify, listenbrainz, ...) and any other musicbrainz sub-keys
    are preserved (3A).
    """
    links: dict[str, Any] = dict(service_links or {})
    existing_mb = links.get("musicbrainz")
    mb: dict[str, Any] = dict(existing_mb) if isinstance(existing_mb, dict) else {}
    mb["id"] = mbid
    links["musicbrainz"] = mb
    return links


def _apply(
    entity: Any,
    library_name: str,
    res: Resolution,
    settings: config_module.Settings,
    seen: dict[str, uuid.UUID],
    counts: BackfillCounts,
) -> None:
    """Apply a resolution to one entity: gate, write (merge-safe), record status.

    Mutates ``entity`` (``service_links``, ``mb_attempted_at``, ``mb_match_status``)
    and ``counts``/``seen`` in place. Does not touch the DB session.
    """
    if res.transient:
        counts.transient += 1
        return  # leave mb_attempted_at NULL -> retried next run (CRITICAL)

    entity.mb_attempted_at = _now()

    if res.mbid is None:
        entity.mb_match_status = types_module.MatchStatus.NO_MATCH
        counts.no_match += 1
        return

    similarity = normalize_module.name_similarity(library_name, res.matched_name or "")
    if similarity < settings.mbid_match_min_similarity:
        entity.mb_match_status = types_module.MatchStatus.BELOW_SIMILARITY
        counts.below_similarity += 1
        logger.info(
            "mbid_backfill_below_similarity",
            entity_id=str(entity.id),
            library_name=library_name,
            matched_name=res.matched_name,
            similarity=round(similarity, 3),
        )
        return

    existing = artist_utils.get_mbid(entity.service_links)
    if existing and existing != res.mbid:
        # Never overwrite a different existing MBID (3A).
        entity.mb_match_status = types_module.MatchStatus.MATCHED
        counts.conflict += 1
        logger.warning(
            "mbid_backfill_conflict",
            entity_id=str(entity.id),
            existing_mbid=existing,
            proposed_mbid=res.mbid,
        )
        return

    claimer = seen.get(res.mbid)
    if claimer is not None and claimer != entity.id:
        # Another row this run already claimed this MBID — a duplicate (T5-A).
        entity.mb_match_status = types_module.MatchStatus.MATCHED
        counts.collision += 1
        logger.info(
            "mbid_backfill_collision",
            mbid=res.mbid,
            first_entity_id=str(claimer),
            duplicate_entity_id=str(entity.id),
        )
        return  # skip the write; dedup merges these later

    entity.service_links = _merge_mbid(entity.service_links, res.mbid)
    entity.mb_match_status = types_module.MatchStatus.MATCHED
    seen[res.mbid] = entity.id
    counts.matched += 1


# Transient errors from the MB /ws/2 search (after the connector exhausts its own
# retries) — treat as "not attempted" for the artist rather than a false no_match.
_SEARCH_TRANSIENT_ERRORS = (httpx.HTTPError,)


async def backfill_tracks(
    session: Any,
    settings: config_module.Settings,
    mapper: mapper_module.MbidMapperClient,
    *,
    harvested: dict[uuid.UUID, str],
) -> BackfillCounts:
    """Resolve recording MBIDs for tracks via the hosted mapper.

    Gates each match on the artist name (the worst failure is a wrong-artist
    recording). On a gated match, harvests the artist MBID into ``harvested`` for
    the artist pass.
    """
    counts = BackfillCounts()
    seen: dict[str, uuid.UUID] = {}
    while True:
        stmt = (
            sa.select(music_module.Track)
            .where(music_module.Track.mb_attempted_at.is_(None))
            .options(orm.selectinload(music_module.Track.artist))
            .limit(settings.mbid_mapper_batch_size)
        )
        tracks = list((await session.execute(stmt)).scalars().all())
        if not tracks:
            break

        queries = [
            mapper_module.RecordingQuery(
                artist_name=t.artist.name, recording_name=t.title
            )
            for t in tracks
        ]
        try:
            matches = await mapper.lookup_recordings(queries)
        except mapper_module.MapperUnavailableError:
            logger.warning("mbid_backfill_tracks_mapper_unavailable", batch=len(tracks))
            counts.transient += len(tracks)
            break  # leave the rest unattempted; retried next run

        for track, match in zip(tracks, matches, strict=True):
            res = Resolution(
                mbid=match.recording_mbid if match else None,
                matched_name=match.artist_credit_name if match else None,
                artist_mbid=(
                    match.artist_mbids[0] if match and match.artist_mbids else None
                ),
            )
            _apply(track, track.artist.name, res, settings, seen, counts)
            if (
                track.mb_match_status == types_module.MatchStatus.MATCHED
                and res.artist_mbid
            ):
                # Harvested from an artist-name-gated track match -> trustworthy.
                harvested.setdefault(track.artist_id, res.artist_mbid)

        await session.commit()

    logger.info("mbid_backfill_tracks_done", **dataclasses.asdict(counts))
    return counts


async def backfill_artists(
    session: Any,
    settings: config_module.Settings,
    connector: listenbrainz_module.ListenBrainzConnector,
    *,
    harvested: dict[uuid.UUID, str],
) -> BackfillCounts:
    """Resolve artist MBIDs: harvested-from-tracks first, then MB /ws/2 search."""
    counts = BackfillCounts()
    seen: dict[str, uuid.UUID] = {}
    while True:
        stmt = (
            sa.select(music_module.Artist)
            .where(music_module.Artist.mb_attempted_at.is_(None))
            .limit(settings.mbid_mapper_batch_size)
        )
        artists = list((await session.execute(stmt)).scalars().all())
        if not artists:
            break

        for artist in artists:
            harvested_mbid = harvested.get(artist.id)
            if harvested_mbid:
                # Already gated during the track pass — trust it (gate passes at 1.0).
                res = Resolution(mbid=harvested_mbid, matched_name=artist.name)
                _apply(artist, artist.name, res, settings, seen, counts)
                continue

            try:
                results = await connector.search_artists(artist.name, limit=1)
            except _SEARCH_TRANSIENT_ERRORS as exc:
                logger.warning(
                    "mbid_backfill_artist_search_unavailable",
                    artist_id=str(artist.id),
                    error=type(exc).__name__,
                )
                _apply(
                    artist,
                    artist.name,
                    Resolution(transient=True),
                    settings,
                    seen,
                    counts,
                )
                continue

            top = results[0] if results else None
            res = Resolution(
                mbid=top.get("mbid") if top else None,
                matched_name=top.get("name") if top else None,
            )
            _apply(artist, artist.name, res, settings, seen, counts)

        await session.commit()

    logger.info("mbid_backfill_artists_done", **dataclasses.asdict(counts))
    return counts


@dataclasses.dataclass
class PopularityBackfillCounts:
    """Outcome tally for a ListenBrainz popularity backfill run."""

    candidates: int = 0
    updated: int = 0
    no_popularity: int = 0
    skipped_no_mbid: int = 0


# Listen-count ceiling for the 0-100 normalization. A global hit recording
# (e.g. "Closer" at ~1.38M listens) saturates the scale; 10^6 gives a clean
# log10 denominator of 6.
_POPULARITY_CEILING = 1_000_000
_POPULARITY_LOG_CEILING = math.log10(_POPULARITY_CEILING)


def normalize_popularity(listen_count: int) -> int:
    """Map a raw ListenBrainz listen count to a 0-100 popularity score.

    Listen counts are heavy-tailed (a handful of recordings dominate), so a
    linear scale would collapse almost everything to 0. A log10 mapping spreads
    the distribution: ``score = 100 * log10(count) / log10(10^6)``, clamped to
    0-100. A recording with ~1M listens saturates to 100; ~1000 listens lands at
    50; a single listen rounds to ~0.

    Args:
        listen_count: ``total_listen_count`` from the LB popularity endpoint.

    Returns:
        An integer popularity score in ``[0, 100]``.
    """
    if listen_count <= 0:
        return 0
    ratio = math.log10(listen_count) / _POPULARITY_LOG_CEILING
    return max(0, min(100, round(ratio * 100)))


async def run_popularity_backfill(
    session: Any,
    settings: config_module.Settings,
    connector: listenbrainz_module.ListenBrainzConnector,
) -> PopularityBackfillCounts:
    """Backfill ``Track.popularity_score`` from ListenBrainz recording popularity.

    Iterates library tracks that carry a MusicBrainz recording MBID
    (``service_links["musicbrainz"]["id"]``), in batches, and fetches each
    recording's global listen count via ``connector.get_recording_popularity``
    (the public ``POST /1/popularity/recording`` endpoint — no auth token). The
    listen count is normalized to a 0-100 score (see ``normalize_popularity``) and
    overwrites any prior discovery-sourced synthetic ``popularity_score``.

    Tracks without a recording MBID are counted under ``skipped_no_mbid`` and left
    untouched — there is no MBID-keyed popularity to fetch for them. Recordings LB
    has no data on are counted under ``no_popularity`` and left untouched. Batched
    and committed per batch so a worker restart re-enters and re-scans (the scan is
    idempotent — re-reading popularity is cheap and converges).
    """
    counts = PopularityBackfillCounts()
    offset = 0
    batch_size = settings.mbid_mapper_batch_size
    while True:
        stmt = (
            sa.select(music_module.Track)
            .order_by(music_module.Track.id)
            .offset(offset)
            .limit(batch_size)
        )
        tracks = list((await session.execute(stmt)).scalars().all())
        if not tracks:
            break
        offset += len(tracks)
        counts.candidates += len(tracks)

        by_recording_mbid: dict[str, list[music_module.Track]] = {}
        for track in tracks:
            recording_mbid = artist_utils.get_mbid(track.service_links)
            if recording_mbid:
                by_recording_mbid.setdefault(recording_mbid, []).append(track)
            else:
                counts.skipped_no_mbid += 1

        if by_recording_mbid:
            popularity = await connector.get_recording_popularity(
                list(by_recording_mbid.keys())
            )
            for recording_mbid, group in by_recording_mbid.items():
                listen_count = popularity.get(recording_mbid)
                if listen_count is None:
                    counts.no_popularity += len(group)
                    continue
                score = normalize_popularity(listen_count)
                for track in group:
                    track.popularity_score = score
                    counts.updated += 1

        await session.commit()

    logger.info("popularity_backfill_done", **dataclasses.asdict(counts))
    return counts


async def run_mbid_backfill(
    session: Any,
    settings: config_module.Settings,
    *,
    mapper: mapper_module.MbidMapperClient,
    connector: listenbrainz_module.ListenBrainzConnector,
    do_tracks: bool = True,
    do_artists: bool = True,
) -> dict[str, BackfillCounts]:
    """Run the track and artist backfill passes (tracks first, to harvest).

    Returns per-entity-type counts: ``{"track": ..., "artist": ...}`` (only the
    passes that ran are present).
    """
    harvested: dict[uuid.UUID, str] = {}
    out: dict[str, BackfillCounts] = {}
    if do_tracks:
        out["track"] = await backfill_tracks(
            session, settings, mapper, harvested=harvested
        )
    if do_artists:
        out["artist"] = await backfill_artists(
            session, settings, connector, harvested=harvested
        )
    return out
