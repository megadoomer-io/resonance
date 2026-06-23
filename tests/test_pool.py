"""Tests for the pure pool source spec (issue #128)."""

from __future__ import annotations

import uuid

import pytest

import resonance.generators.pool as pool_module


class TestNormalizeSources:
    def test_layered_shape_parses_all_kinds(self) -> None:
        eid = uuid.uuid4()
        aid = uuid.uuid4()
        raw = {
            "sources": [
                {"kind": "event", "event_id": str(eid), "enabled": True},
                {"kind": "artist", "artist_id": str(aid), "enabled": False},
                {"kind": "related", "seed": "target", "amount": 5},
            ]
        }
        sources = pool_module.normalize_sources(raw)
        assert sources == [
            pool_module.EventSource(event_id=eid, enabled=True),
            pool_module.ArtistSource(artist_id=aid, enabled=False),
            pool_module.RelatedSource(amount=5, seed="target", enabled=True),
        ]

    def test_legacy_event_id_shape(self) -> None:
        eid = uuid.uuid4()
        sources = pool_module.normalize_sources({"event_id": str(eid)})
        assert sources == [pool_module.EventSource(event_id=eid, enabled=True)]

    def test_sources_wins_over_legacy_event_id(self) -> None:
        legacy = uuid.uuid4()
        new = uuid.uuid4()
        raw = {
            "event_id": str(legacy),
            "sources": [{"kind": "event", "event_id": str(new)}],
        }
        sources = pool_module.normalize_sources(raw)
        assert sources == [pool_module.EventSource(event_id=new)]

    def test_empty_input_yields_empty_list(self) -> None:
        assert pool_module.normalize_sources({}) == []

    def test_empty_sources_list_yields_empty_list(self) -> None:
        assert pool_module.normalize_sources({"sources": []}) == []

    def test_enabled_defaults_true(self) -> None:
        aid = uuid.uuid4()
        sources = pool_module.normalize_sources(
            {"sources": [{"kind": "artist", "artist_id": str(aid)}]}
        )
        assert sources[0].enabled is True

    def test_unknown_kind_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown source kind"):
            pool_module.normalize_sources({"sources": [{"kind": "genre"}]})

    def test_missing_kind_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown source kind"):
            pool_module.normalize_sources(
                {"sources": [{"event_id": str(uuid.uuid4())}]}
            )

    def test_bad_event_uuid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid UUID for event_id"):
            pool_module.normalize_sources(
                {"sources": [{"kind": "event", "event_id": "not-a-uuid"}]}
            )

    def test_sources_not_a_list_raises(self) -> None:
        with pytest.raises(ValueError, match="'sources' must be a list"):
            pool_module.normalize_sources({"sources": {"kind": "event"}})

    def test_source_entry_not_a_mapping_raises(self) -> None:
        with pytest.raises(ValueError, match="must be an object"):
            pool_module.normalize_sources({"sources": ["event"]})

    def test_related_amount_required_integer(self) -> None:
        with pytest.raises(ValueError, match="'amount' must be an integer"):
            pool_module.normalize_sources(
                {"sources": [{"kind": "related", "amount": "five"}]}
            )

    def test_related_amount_rejects_bool(self) -> None:
        # bool is an int subclass; it must not be accepted as an amount.
        with pytest.raises(ValueError, match="'amount' must be an integer"):
            pool_module.normalize_sources(
                {"sources": [{"kind": "related", "amount": True}]}
            )

    def test_related_amount_non_negative(self) -> None:
        with pytest.raises(ValueError, match="must be non-negative"):
            pool_module.normalize_sources(
                {"sources": [{"kind": "related", "amount": -1}]}
            )

    def test_related_seed_default(self) -> None:
        sources = pool_module.normalize_sources(
            {"sources": [{"kind": "related", "amount": 3}]}
        )
        assert sources[0] == pool_module.RelatedSource(amount=3, seed="target")

    def test_unsupported_seed_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported related 'seed'"):
            pool_module.normalize_sources(
                {"sources": [{"kind": "related", "amount": 3, "seed": "genre"}]}
            )

    def test_enabled_must_be_bool(self) -> None:
        with pytest.raises(ValueError, match="'enabled' must be a boolean"):
            pool_module.normalize_sources(
                {
                    "sources": [
                        {"kind": "artist", "artist_id": str(uuid.uuid4()), "enabled": 1}
                    ]
                }
            )


class TestExtractExcludes:
    def test_missing_yields_empty_set(self) -> None:
        assert pool_module.extract_excludes({}) == set()

    def test_parses_uuid_list(self) -> None:
        a, b = uuid.uuid4(), uuid.uuid4()
        result = pool_module.extract_excludes({"exclude_artist_ids": [str(a), str(b)]})
        assert result == {a, b}

    def test_not_a_list_raises(self) -> None:
        with pytest.raises(ValueError, match="must be a list"):
            pool_module.extract_excludes({"exclude_artist_ids": str(uuid.uuid4())})

    def test_bad_uuid_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid UUID"):
            pool_module.extract_excludes({"exclude_artist_ids": ["nope"]})


class TestBuildPool:
    def test_dedup_first_seen_wins(self) -> None:
        aid = uuid.uuid4()
        resolved = [
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.EVENT),
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.RELATED),
        ]
        pool = pool_module.build_pool(resolved, set())
        assert pool == [
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.EVENT)
        ]

    def test_exclude_applied_last(self) -> None:
        keep = uuid.uuid4()
        drop = uuid.uuid4()
        resolved = [
            pool_module.ResolvedArtist(keep, pool_module.PoolProvenance.EVENT),
            pool_module.ResolvedArtist(drop, pool_module.PoolProvenance.EVENT),
        ]
        pool = pool_module.build_pool(resolved, {drop})
        assert [r.artist_id for r in pool] == [keep]

    def test_exclude_overrides_any_provenance(self) -> None:
        # "this event but not the opener": opener came in via event, still excluded.
        opener = uuid.uuid4()
        resolved = [
            pool_module.ResolvedArtist(opener, pool_module.PoolProvenance.EVENT)
        ]
        assert pool_module.build_pool(resolved, {opener}) == []

    def test_order_preserved(self) -> None:
        ids = [uuid.uuid4() for _ in range(4)]
        resolved = [
            pool_module.ResolvedArtist(i, pool_module.PoolProvenance.ARTIST)
            for i in ids
        ]
        pool = pool_module.build_pool(resolved, set())
        assert [r.artist_id for r in pool] == ids

    def test_provenance_precedence_event_over_related(self) -> None:
        # An artist resolved both as an event act and as a related expansion keeps
        # the event provenance (first-seen), since event sources resolve first.
        aid = uuid.uuid4()
        resolved = [
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.EVENT),
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.RELATED),
        ]
        pool = pool_module.build_pool(resolved, set())
        assert pool[0].via is pool_module.PoolProvenance.EVENT


class TestSerialize:
    def test_round_trip_all_kinds(self) -> None:
        eid = uuid.uuid4()
        aid = uuid.uuid4()
        sources: list[pool_module.PoolSource] = [
            pool_module.EventSource(event_id=eid, enabled=False),
            pool_module.ArtistSource(artist_id=aid),
            pool_module.RelatedSource(amount=7, seed="target", enabled=True),
        ]
        excl = uuid.uuid4()
        stored = pool_module.serialize_input_references(sources, [excl])
        assert pool_module.normalize_sources(stored) == sources
        assert pool_module.extract_excludes(stored) == {excl}

    def test_serialize_input_references_empty(self) -> None:
        stored = pool_module.serialize_input_references([])
        assert stored == {"sources": [], "exclude_artist_ids": []}
