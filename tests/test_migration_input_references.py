"""Regression tests for the {event_id} -> {sources} migration (#128 T11).

These exercise the migration's pure transform helpers and the cross-check that
matters for deploy safety: a concert_prep profile resolves to the same pool
whether or not the migration has run, because pool.normalize_sources tolerates
both shapes. The migration only normalizes stored data; it must never change
which artists a profile resolves to.
"""

from __future__ import annotations

import importlib.util
import pathlib
import uuid

import resonance.generators.pool as pool_module

_MIGRATION_PATH = (
    pathlib.Path(__file__).parent.parent
    / "alembic"
    / "versions"
    / "i0j1k2l3m4n5_migrate_input_references_to_sources.py"
)


def _load_migration() -> object:
    spec = importlib.util.spec_from_file_location("_mig_input_refs", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_mig = _load_migration()


class TestLegacyToLayered:
    def test_basic_event_id(self) -> None:
        eid = str(uuid.uuid4())
        result = _mig._legacy_to_layered({"event_id": eid})
        assert result == {
            "sources": [{"kind": "event", "event_id": eid, "enabled": True}],
            "exclude_artist_ids": [],
        }

    def test_already_layered_returns_none(self) -> None:
        # Idempotent: an already-migrated profile is skipped (re-run safe).
        layered = {"sources": [{"kind": "event", "event_id": "x", "enabled": True}]}
        assert _mig._legacy_to_layered(layered) is None

    def test_no_event_id_returns_none(self) -> None:
        assert _mig._legacy_to_layered({}) is None
        assert _mig._legacy_to_layered({"event_id": ""}) is None

    def test_non_dict_returns_none(self) -> None:
        assert _mig._legacy_to_layered(None) is None
        assert _mig._legacy_to_layered("nope") is None


class TestLayeredToLegacy:
    def test_reverses_clean_single_event(self) -> None:
        eid = str(uuid.uuid4())
        layered = {
            "sources": [{"kind": "event", "event_id": eid, "enabled": True}],
            "exclude_artist_ids": [],
        }
        assert _mig._layered_to_legacy(layered) == {"event_id": eid}

    def test_skips_when_excludes_present(self) -> None:
        layered = {
            "sources": [{"kind": "event", "event_id": "e", "enabled": True}],
            "exclude_artist_ids": ["a1"],
        }
        assert _mig._layered_to_legacy(layered) is None

    def test_skips_multi_source(self) -> None:
        layered = {
            "sources": [
                {"kind": "event", "event_id": "e1", "enabled": True},
                {"kind": "artist", "artist_id": "a1", "enabled": True},
            ],
            "exclude_artist_ids": [],
        }
        assert _mig._layered_to_legacy(layered) is None

    def test_skips_non_event_source(self) -> None:
        layered = {
            "sources": [{"kind": "artist", "artist_id": "a1", "enabled": True}],
            "exclude_artist_ids": [],
        }
        assert _mig._layered_to_legacy(layered) is None


class TestRoundTrip:
    def test_legacy_layered_legacy_is_identity(self) -> None:
        eid = str(uuid.uuid4())
        layered = _mig._legacy_to_layered({"event_id": eid})
        assert layered is not None
        assert _mig._layered_to_legacy(layered) == {"event_id": eid}


class TestBackCompatResolution:
    """The deploy-safety guarantee: both shapes resolve to the same source."""

    def test_migrated_and_legacy_resolve_identically(self) -> None:
        eid = uuid.uuid4()
        legacy = {"event_id": str(eid)}
        migrated = _mig._legacy_to_layered(legacy)
        assert migrated is not None

        legacy_sources = pool_module.normalize_sources(legacy)
        migrated_sources = pool_module.normalize_sources(migrated)

        assert legacy_sources == migrated_sources
        assert len(legacy_sources) == 1
        source = legacy_sources[0]
        assert isinstance(source, pool_module.EventSource)
        assert source.event_id == eid
        assert source.enabled is True
