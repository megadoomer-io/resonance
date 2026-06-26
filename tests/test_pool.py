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
            ]
        }
        sources = pool_module.normalize_sources(raw)
        assert sources == [
            pool_module.EventSource(event_id=eid, enabled=True),
            pool_module.ArtistSource(artist_id=aid, enabled=False),
        ]

    def test_related_kind_now_rejected(self) -> None:
        # The "related" source kind was removed in #133 (enrichment persists
        # concrete artist sources instead).
        with pytest.raises(ValueError, match="Unknown source kind"):
            pool_module.normalize_sources(
                {"sources": [{"kind": "related", "amount": 5}]}
            )

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

    def test_enabled_must_be_bool(self) -> None:
        with pytest.raises(ValueError, match="'enabled' must be a boolean"):
            pool_module.normalize_sources(
                {
                    "sources": [
                        {"kind": "artist", "artist_id": str(uuid.uuid4()), "enabled": 1}
                    ]
                }
            )


class TestArtistViaSeed:
    """via_seed provenance on artist sources (#133)."""

    def test_via_seed_defaults_none(self) -> None:
        aid = uuid.uuid4()
        raw = {"sources": [{"kind": "artist", "artist_id": str(aid)}]}
        assert pool_module.normalize_sources(raw) == [
            pool_module.ArtistSource(artist_id=aid, via_seed=None)
        ]

    def test_via_seed_lineup_parsed(self) -> None:
        aid = uuid.uuid4()
        raw = {
            "sources": [{"kind": "artist", "artist_id": str(aid), "via_seed": "lineup"}]
        }
        assert pool_module.normalize_sources(raw) == [
            pool_module.ArtistSource(artist_id=aid, via_seed="lineup")
        ]

    def test_via_seed_artist_id_parsed(self) -> None:
        aid = uuid.uuid4()
        seed = str(uuid.uuid4())
        raw = {"sources": [{"kind": "artist", "artist_id": str(aid), "via_seed": seed}]}
        assert pool_module.normalize_sources(raw) == [
            pool_module.ArtistSource(artist_id=aid, via_seed=seed)
        ]

    def test_via_seed_null_is_none(self) -> None:
        aid = uuid.uuid4()
        raw = {"sources": [{"kind": "artist", "artist_id": str(aid), "via_seed": None}]}
        assert pool_module.normalize_sources(raw)[0] == pool_module.ArtistSource(
            artist_id=aid, via_seed=None
        )

    def test_via_seed_empty_string_raises(self) -> None:
        aid = uuid.uuid4()
        raw = {"sources": [{"kind": "artist", "artist_id": str(aid), "via_seed": ""}]}
        with pytest.raises(ValueError, match="via_seed"):
            pool_module.normalize_sources(raw)

    def test_via_seed_non_string_raises(self) -> None:
        aid = uuid.uuid4()
        raw = {"sources": [{"kind": "artist", "artist_id": str(aid), "via_seed": 3}]}
        with pytest.raises(ValueError, match="via_seed"):
            pool_module.normalize_sources(raw)

    def test_serialize_omits_via_seed_when_none(self) -> None:
        aid = uuid.uuid4()
        out = pool_module.serialize_source(pool_module.ArtistSource(artist_id=aid))
        assert "via_seed" not in out

    def test_serialize_includes_via_seed_when_set(self) -> None:
        aid = uuid.uuid4()
        out = pool_module.serialize_source(
            pool_module.ArtistSource(artist_id=aid, via_seed="lineup")
        )
        assert out["via_seed"] == "lineup"

    def test_round_trip_with_via_seed(self) -> None:
        aid = uuid.uuid4()
        seed = str(uuid.uuid4())
        sources: list[pool_module.PoolSource] = [
            pool_module.ArtistSource(artist_id=aid, via_seed=seed),
            pool_module.ArtistSource(artist_id=uuid.uuid4(), via_seed="lineup"),
            pool_module.ArtistSource(artist_id=uuid.uuid4(), via_seed=None),
        ]
        stored = pool_module.serialize_input_references(sources)
        assert pool_module.normalize_sources(stored) == sources

    def test_via_seed_source_resolves_same_as_plain(self) -> None:
        # via_seed is provenance only -- it must not change how the artist
        # resolves into the pool (same artist_id -> same ResolvedArtist).
        aid = uuid.uuid4()
        plain = pool_module.ArtistSource(artist_id=aid)
        tagged = pool_module.ArtistSource(artist_id=aid, via_seed="lineup")
        resolved = [
            pool_module.ResolvedArtist(
                artist_id=s.artist_id, via=pool_module.PoolSourceKind.ARTIST
            )
            for s in (plain, tagged)
        ]
        # Both resolve to the same artist -> dedup keeps one.
        assert pool_module.build_pool(resolved, set()) == [
            pool_module.ResolvedArtist(
                artist_id=aid, via=pool_module.PoolSourceKind.ARTIST
            )
        ]


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
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.ARTIST),
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

    def test_provenance_precedence_event_over_artist(self) -> None:
        # An artist resolved both as an event act and as a manual artist add keeps
        # the event provenance (first-seen), since event sources resolve first.
        aid = uuid.uuid4()
        resolved = [
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.EVENT),
            pool_module.ResolvedArtist(aid, pool_module.PoolProvenance.ARTIST),
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
        ]
        excl = uuid.uuid4()
        stored = pool_module.serialize_input_references(sources, [excl])
        assert pool_module.normalize_sources(stored) == sources
        assert pool_module.extract_excludes(stored) == {excl}

    def test_serialize_input_references_empty(self) -> None:
        stored = pool_module.serialize_input_references([])
        assert stored == {"sources": [], "exclude_artist_ids": []}


class TestScopeArtistIds:
    """scope_artist_ids (#133)."""

    def test_returns_only_matching_scope(self) -> None:
        a, b, c = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [
                pool_module.ArtistSource(artist_id=a, via_seed="lineup"),
                pool_module.ArtistSource(artist_id=b, via_seed="lineup"),
                pool_module.ArtistSource(artist_id=c, via_seed=str(a)),
            ]
        )
        assert pool_module.scope_artist_ids(refs, "lineup") == [a, b]
        assert pool_module.scope_artist_ids(refs, str(a)) == [c]

    def test_ignores_untagged_and_other_kinds(self) -> None:
        a = uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [
                pool_module.ArtistSource(artist_id=a),  # via_seed=None
                pool_module.EventSource(event_id=uuid.uuid4()),
            ]
        )
        assert pool_module.scope_artist_ids(refs, "lineup") == []


class TestReplaceViaSeedSources:
    """replace_via_seed_sources (#133)."""

    def test_appends_new_scope_sources(self) -> None:
        core = uuid.uuid4()
        n1, n2 = uuid.uuid4(), uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [pool_module.ArtistSource(artist_id=core)]
        )
        out = pool_module.replace_via_seed_sources(refs, "lineup", [n1, n2])
        sources = pool_module.normalize_sources(out)
        assert pool_module.ArtistSource(artist_id=core, via_seed=None) in sources
        assert pool_module.ArtistSource(artist_id=n1, via_seed="lineup") in sources
        assert pool_module.ArtistSource(artist_id=n2, via_seed="lineup") in sources

    def test_replaces_only_target_scope(self) -> None:
        seed_a = uuid.uuid4()
        old_lineup = uuid.uuid4()
        other_scope = uuid.uuid4()
        new = uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [
                pool_module.ArtistSource(artist_id=old_lineup, via_seed="lineup"),
                pool_module.ArtistSource(artist_id=other_scope, via_seed=str(seed_a)),
            ]
        )
        out = pool_module.replace_via_seed_sources(refs, "lineup", [new])
        ids_by_scope = {
            s.via_seed: s.artist_id
            for s in pool_module.normalize_sources(out)
            if isinstance(s, pool_module.ArtistSource)
        }
        assert ids_by_scope["lineup"] == new  # old_lineup dropped
        assert ids_by_scope[str(seed_a)] == other_scope  # other scope untouched

    def test_empty_new_batch_just_drops_scope(self) -> None:
        old = uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [pool_module.ArtistSource(artist_id=old, via_seed="lineup")]
        )
        out = pool_module.replace_via_seed_sources(refs, "lineup", [])
        assert pool_module.normalize_sources(out) == []

    def test_preserves_excludes(self) -> None:
        core = uuid.uuid4()
        excl = uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [pool_module.ArtistSource(artist_id=core)], [excl]
        )
        out = pool_module.replace_via_seed_sources(refs, "lineup", [uuid.uuid4()])
        assert pool_module.extract_excludes(out) == {excl}

    def test_does_not_mutate_input(self) -> None:
        core = uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [pool_module.ArtistSource(artist_id=core)]
        )
        snapshot = {
            "sources": list(refs["sources"]),  # type: ignore[arg-type]
            "exclude_artist_ids": list(refs["exclude_artist_ids"]),  # type: ignore[arg-type]
        }
        pool_module.replace_via_seed_sources(refs, "lineup", [uuid.uuid4()])
        assert refs == snapshot

    def test_preserves_track_excludes(self) -> None:
        """Re-running a scope keeps the global exclude_track_ids set."""
        core = uuid.uuid4()
        t1, t2 = uuid.uuid4(), uuid.uuid4()
        refs = pool_module.serialize_input_references(
            [pool_module.ArtistSource(artist_id=core)],
            exclude_track_ids=[t1, t2],
        )
        out = pool_module.replace_via_seed_sources(refs, "lineup", [uuid.uuid4()])
        assert pool_module.extract_track_excludes(out) == {t1, t2}


class TestExtractTrackExcludes:
    """extract_track_excludes (#track-exclude)."""

    def test_missing_yields_empty_set(self) -> None:
        assert pool_module.extract_track_excludes({}) == set()

    def test_parses_uuid_list(self) -> None:
        a, b = uuid.uuid4(), uuid.uuid4()
        result = pool_module.extract_track_excludes(
            {"exclude_track_ids": [str(a), str(b)]}
        )
        assert result == {a, b}

    def test_not_a_list_raises(self) -> None:
        with pytest.raises(ValueError, match="exclude_track_ids"):
            pool_module.extract_track_excludes({"exclude_track_ids": str(uuid.uuid4())})

    def test_bad_uuid_raises(self) -> None:
        with pytest.raises(ValueError, match="exclude_track_ids"):
            pool_module.extract_track_excludes({"exclude_track_ids": ["nope"]})

    def test_serialize_round_trip(self) -> None:
        """serialize emits exclude_track_ids only when non-empty, round-trips."""
        t1 = uuid.uuid4()
        with_excl = pool_module.serialize_input_references([], exclude_track_ids=[t1])
        assert pool_module.extract_track_excludes(with_excl) == {t1}
        # Empty stays lean (no key) — preserves existing profile shape.
        without = pool_module.serialize_input_references([])
        assert "exclude_track_ids" not in without
