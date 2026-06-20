"""Tests for the MBID backfill core (#71)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

import resonance.config as config_module
import resonance.services.mbid_mapper as mapper_module
import resonance.sync.backfill as backfill_module
import resonance.types as types_module


def _settings() -> config_module.Settings:
    return config_module.Settings(
        mbid_match_min_similarity=0.85, mbid_mapper_batch_size=50
    )


def _entity(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "service_links": None,
        "mb_attempted_at": None,
        "mb_match_status": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class FakeResult:
    """Mimics session.execute(...).scalars().all()."""

    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items


def _session(batches: list[list[Any]]) -> AsyncMock:
    """An AsyncMock session whose execute yields each batch then is consumed."""
    session = AsyncMock()
    session.execute = AsyncMock(side_effect=[FakeResult(b) for b in batches])
    session.commit = AsyncMock()
    return session


# ---------------------------------------------------------------------------
# _apply — the shared engine (pure, no session)
# ---------------------------------------------------------------------------


class TestApply:
    def _run(
        self,
        entity: SimpleNamespace,
        res: backfill_module.Resolution,
        *,
        library_name: str = "Portishead",
        seen: dict[str, uuid.UUID] | None = None,
    ) -> backfill_module.BackfillCounts:
        counts = backfill_module.BackfillCounts()
        backfill_module._apply(
            entity,
            library_name,
            res,
            _settings(),
            seen if seen is not None else {},
            counts,
        )
        return counts

    def test_matched_writes_mbid_and_preserves_siblings(self) -> None:
        entity = _entity(service_links={"spotify": {"id": "abc"}})
        counts = self._run(
            entity,
            backfill_module.Resolution(mbid="MB-1", matched_name="Portishead"),
        )
        assert entity.service_links["musicbrainz"]["id"] == "MB-1"
        assert entity.service_links["spotify"] == {"id": "abc"}  # preserved
        assert entity.mb_match_status == types_module.MatchStatus.MATCHED
        assert entity.mb_attempted_at is not None
        assert counts.matched == 1

    def test_no_match_records_status_no_write(self) -> None:
        entity = _entity()
        counts = self._run(entity, backfill_module.Resolution(mbid=None))
        assert entity.service_links is None
        assert entity.mb_match_status == types_module.MatchStatus.NO_MATCH
        assert entity.mb_attempted_at is not None
        assert counts.no_match == 1

    def test_below_similarity_no_write(self) -> None:
        entity = _entity()
        counts = self._run(
            entity,
            backfill_module.Resolution(
                mbid="MB-1", matched_name="Completely Different"
            ),
        )
        assert entity.service_links is None
        assert entity.mb_match_status == types_module.MatchStatus.BELOW_SIMILARITY
        assert counts.below_similarity == 1

    def test_transient_leaves_unattempted(self) -> None:
        entity = _entity()
        counts = self._run(entity, backfill_module.Resolution(transient=True))
        assert entity.mb_attempted_at is None  # CRITICAL: retried next run
        assert entity.mb_match_status is None
        assert counts.transient == 1

    def test_conflict_keeps_existing_mbid(self) -> None:
        entity = _entity(service_links={"musicbrainz": {"id": "OLD"}})
        counts = self._run(
            entity,
            backfill_module.Resolution(mbid="NEW", matched_name="Portishead"),
        )
        assert entity.service_links["musicbrainz"]["id"] == "OLD"  # not overwritten
        assert counts.conflict == 1
        assert entity.mb_match_status == types_module.MatchStatus.MATCHED

    def test_existing_same_mbid_is_idempotent_match(self) -> None:
        entity = _entity(service_links={"musicbrainz": {"id": "MB-1"}})
        counts = self._run(
            entity, backfill_module.Resolution(mbid="MB-1", matched_name="Portishead")
        )
        assert counts.matched == 1
        assert counts.conflict == 0

    def test_collision_skips_second_write(self) -> None:
        first_id = uuid.uuid4()
        seen: dict[str, uuid.UUID] = {"MB-1": first_id}
        entity = _entity()  # different id
        counts = self._run(
            entity,
            backfill_module.Resolution(mbid="MB-1", matched_name="Portishead"),
            seen=seen,
        )
        assert entity.service_links is None  # write skipped (T5-A)
        assert counts.collision == 1
        assert entity.mb_attempted_at is not None

    def test_first_claim_records_in_seen(self) -> None:
        seen: dict[str, uuid.UUID] = {}
        entity = _entity()
        self._run(
            entity,
            backfill_module.Resolution(mbid="MB-1", matched_name="Portishead"),
            seen=seen,
        )
        assert seen["MB-1"] == entity.id


# ---------------------------------------------------------------------------
# backfill_tracks
# ---------------------------------------------------------------------------


def _track(
    title: str, artist_name: str, artist_id: uuid.UUID | None = None
) -> SimpleNamespace:
    return _entity(
        title=title,
        artist_id=artist_id or uuid.uuid4(),
        artist=SimpleNamespace(name=artist_name),
    )


def _match(
    recording_mbid: str, artist_credit_name: str, artist_mbids: list[str]
) -> mapper_module.RecordingMatch:
    return mapper_module.RecordingMatch(
        recording_mbid=recording_mbid,
        artist_credit_name=artist_credit_name,
        artist_mbids=artist_mbids,
    )


class TestBackfillTracks:
    @pytest.mark.anyio()
    async def test_match_writes_and_harvests_artist_mbid(self) -> None:
        artist_id = uuid.uuid4()
        track = _track("Glory Box", "Portishead", artist_id)
        session = _session([[track], []])
        mapper = AsyncMock()
        mapper.lookup_recordings = AsyncMock(
            return_value=[_match("REC-1", "Portishead", ["ART-1"])]
        )
        harvested: dict[uuid.UUID, str] = {}

        counts = await backfill_module.backfill_tracks(
            session, _settings(), mapper, harvested=harvested
        )

        assert track.service_links["musicbrainz"]["id"] == "REC-1"
        assert harvested[artist_id] == "ART-1"  # harvested for the artist pass
        assert counts.matched == 1
        session.commit.assert_awaited()

    @pytest.mark.anyio()
    async def test_wrong_artist_match_is_below_similarity_no_harvest(self) -> None:
        artist_id = uuid.uuid4()
        track = _track("Glory Box", "Portishead", artist_id)
        session = _session([[track], []])
        mapper = AsyncMock()
        mapper.lookup_recordings = AsyncMock(
            return_value=[_match("REC-X", "Some Other Band", ["ART-X"])]
        )
        harvested: dict[uuid.UUID, str] = {}

        counts = await backfill_module.backfill_tracks(
            session, _settings(), mapper, harvested=harvested
        )

        assert track.mb_match_status == types_module.MatchStatus.BELOW_SIMILARITY
        assert artist_id not in harvested  # nothing harvested from a rejected match
        assert counts.below_similarity == 1

    @pytest.mark.anyio()
    async def test_mapper_unavailable_leaves_unattempted(self) -> None:
        track = _track("Glory Box", "Portishead")
        session = _session([[track], []])
        mapper = AsyncMock()
        mapper.lookup_recordings = AsyncMock(
            side_effect=mapper_module.MapperUnavailableError("down")
        )
        counts = await backfill_module.backfill_tracks(
            session, _settings(), mapper, harvested={}
        )
        assert track.mb_attempted_at is None  # CRITICAL
        assert counts.transient == 1
        session.commit.assert_not_awaited()


# ---------------------------------------------------------------------------
# backfill_artists
# ---------------------------------------------------------------------------


class TestBackfillArtists:
    @pytest.mark.anyio()
    async def test_harvested_path_skips_search(self) -> None:
        artist = _entity(name="Portishead")
        session = _session([[artist], []])
        connector = AsyncMock()
        connector.search_artists = AsyncMock()
        harvested = {artist.id: "ART-1"}

        counts = await backfill_module.backfill_artists(
            session, _settings(), connector, harvested=harvested
        )

        assert artist.service_links["musicbrainz"]["id"] == "ART-1"
        connector.search_artists.assert_not_awaited()
        assert counts.matched == 1

    @pytest.mark.anyio()
    async def test_search_fallback_matches(self) -> None:
        artist = _entity(name="Portishead")
        session = _session([[artist], []])
        connector = AsyncMock()
        connector.search_artists = AsyncMock(
            return_value=[{"mbid": "ART-2", "name": "Portishead"}]
        )
        counts = await backfill_module.backfill_artists(
            session, _settings(), connector, harvested={}
        )
        assert artist.service_links["musicbrainz"]["id"] == "ART-2"
        assert counts.matched == 1

    @pytest.mark.anyio()
    async def test_search_no_results_is_no_match(self) -> None:
        artist = _entity(name="Obscure Local Band")
        session = _session([[artist], []])
        connector = AsyncMock()
        connector.search_artists = AsyncMock(return_value=[])
        counts = await backfill_module.backfill_artists(
            session, _settings(), connector, harvested={}
        )
        assert artist.mb_match_status == types_module.MatchStatus.NO_MATCH
        assert counts.no_match == 1

    @pytest.mark.anyio()
    async def test_search_transient_leaves_unattempted(self) -> None:
        import httpx

        artist = _entity(name="Portishead")
        session = _session([[artist], []])
        connector = AsyncMock()
        connector.search_artists = AsyncMock(side_effect=httpx.ConnectError("down"))
        counts = await backfill_module.backfill_artists(
            session, _settings(), connector, harvested={}
        )
        assert artist.mb_attempted_at is None  # CRITICAL
        assert counts.transient == 1


class TestRunMbidBackfill:
    @pytest.mark.anyio()
    async def test_tracks_then_artists_harvest_flows(self) -> None:
        artist_id = uuid.uuid4()
        track = _track("Glory Box", "Portishead", artist_id)
        artist = _entity(id=artist_id, name="Portishead")
        # execute order: tracks batch, tracks empty, artists batch, artists empty
        session = _session([[track], [], [artist], []])
        mapper = AsyncMock()
        mapper.lookup_recordings = AsyncMock(
            return_value=[_match("REC-1", "Portishead", ["ART-1"])]
        )
        connector = AsyncMock()
        connector.search_artists = AsyncMock()

        out = await backfill_module.run_mbid_backfill(
            session, _settings(), mapper=mapper, connector=connector
        )

        assert track.service_links["musicbrainz"]["id"] == "REC-1"
        # artist filled from harvest, no search call
        assert artist.service_links["musicbrainz"]["id"] == "ART-1"
        connector.search_artists.assert_not_awaited()
        assert out["track"].matched == 1
        assert out["artist"].matched == 1


def _pop_track(spotify_id: str | None, popularity_score: int | None = None) -> Any:
    links = {"spotify": spotify_id} if spotify_id is not None else None
    return _entity(service_links=links, popularity_score=popularity_score)


class TestRunPopularityBackfill:
    @pytest.mark.anyio()
    async def test_overwrites_score_for_spotify_linked_tracks(self) -> None:
        # Spotify is authoritative: overwrite a prior synthetic value (#117).
        a = _pop_track("sp-a", popularity_score=3)
        b = _pop_track("sp-b", popularity_score=None)
        session = _session([[a, b], []])
        connector = AsyncMock()
        connector.get_tracks = AsyncMock(return_value={"sp-a": 71, "sp-b": 12})

        counts = await backfill_module.run_popularity_backfill(
            session, _settings(), connector, access_token="tok"
        )

        assert a.popularity_score == 71  # overwrote synthetic 3
        assert b.popularity_score == 12
        assert counts.candidates == 2
        assert counts.updated == 2
        assert counts.no_popularity == 0
        connector.get_tracks.assert_awaited_once_with("tok", ["sp-a", "sp-b"])

    @pytest.mark.anyio()
    async def test_no_popularity_leaves_score_untouched(self) -> None:
        a = _pop_track("sp-a", popularity_score=5)
        session = _session([[a], []])
        connector = AsyncMock()
        connector.get_tracks = AsyncMock(return_value={})  # Spotify gave nothing

        counts = await backfill_module.run_popularity_backfill(
            session, _settings(), connector, access_token="tok"
        )

        assert a.popularity_score == 5  # untouched
        assert counts.candidates == 1
        assert counts.updated == 0
        assert counts.no_popularity == 1

    @pytest.mark.anyio()
    async def test_dedupes_shared_spotify_id_into_one_lookup(self) -> None:
        # Two tracks pointing at the same Spotify id -> one lookup, both updated.
        a = _pop_track("dup")
        b = _pop_track("dup")
        session = _session([[a, b], []])
        connector = AsyncMock()
        connector.get_tracks = AsyncMock(return_value={"dup": 40})

        counts = await backfill_module.run_popularity_backfill(
            session, _settings(), connector, access_token="tok"
        )

        assert a.popularity_score == 40
        assert b.popularity_score == 40
        assert counts.updated == 2
        connector.get_tracks.assert_awaited_once_with("tok", ["dup"])

    @pytest.mark.anyio()
    async def test_empty_library_no_lookup(self) -> None:
        session = _session([[]])
        connector = AsyncMock()
        connector.get_tracks = AsyncMock()

        counts = await backfill_module.run_popularity_backfill(
            session, _settings(), connector, access_token="tok"
        )

        assert counts.candidates == 0
        assert counts.updated == 0
        connector.get_tracks.assert_not_awaited()
