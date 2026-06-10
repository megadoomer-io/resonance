"""Tests for normalized_raw_name on EventArtistCandidate.

Validates that the @validates hook auto-populates normalized_raw_name,
and that unicode-variant names are treated as the same candidate.
"""
# ruff: noqa: RUF001 — ambiguous unicode characters are intentional test data

from __future__ import annotations

import uuid

import resonance.models.concert as concert_models
import resonance.normalize as normalize_module


class TestValidatesHook:
    """The @validates('raw_name') hook auto-populates normalized_raw_name."""

    def test_ascii_name(self) -> None:
        c = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="Iron Maiden"
        )
        assert c.normalized_raw_name == "iron maiden"
        assert c.raw_name == "Iron Maiden"

    def test_smart_quotes_normalized(self) -> None:
        c = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="KK’s Priest"
        )
        assert c.normalized_raw_name == "kk's priest"

    def test_diacritics_stripped(self) -> None:
        c = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="Motörhead"
        )
        assert c.normalized_raw_name == "motorhead"

    def test_preserves_raw_name(self) -> None:
        original = "Beyoncé"
        c = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name=original
        )
        assert c.raw_name == original
        assert c.normalized_raw_name == "beyonce"

    def test_whitespace_collapsed(self) -> None:
        c = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="  The   Band  "
        )
        assert c.normalized_raw_name == "the band"

    def test_mixed_unicode(self) -> None:
        c = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(),
            raw_name="KK’s Priest at Mötley Crüe Venue",
        )
        assert c.normalized_raw_name == "kk's priest at motley crue venue"

    def test_empty_string(self) -> None:
        c = concert_models.EventArtistCandidate(event_id=uuid.uuid4(), raw_name="")
        assert c.normalized_raw_name == ""

    def test_updates_on_raw_name_change(self) -> None:
        c = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="Old Name"
        )
        assert c.normalized_raw_name == "old name"
        c.raw_name = "Beyoncé"
        assert c.normalized_raw_name == "beyonce"


class TestUnicodeVariantsMatch:
    """Unicode variants of the same name produce identical normalized forms."""

    def test_smart_vs_ascii_apostrophe(self) -> None:
        a = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="KK’s Priest"
        )
        b = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="KK's Priest"
        )
        assert a.normalized_raw_name == b.normalized_raw_name

    def test_accented_vs_plain(self) -> None:
        a = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="Beyoncé"
        )
        b = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="Beyonce"
        )
        assert a.normalized_raw_name == b.normalized_raw_name

    def test_nfc_vs_nfd(self) -> None:
        nfc = "Beyoncé"
        nfd = "Beyoncé"
        a = concert_models.EventArtistCandidate(event_id=uuid.uuid4(), raw_name=nfc)
        b = concert_models.EventArtistCandidate(event_id=uuid.uuid4(), raw_name=nfd)
        assert a.normalized_raw_name == b.normalized_raw_name

    def test_case_variants(self) -> None:
        a = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="IRON MAIDEN"
        )
        b = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="iron maiden"
        )
        assert a.normalized_raw_name == b.normalized_raw_name

    def test_different_names_differ(self) -> None:
        a = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="Iron Maiden"
        )
        b = concert_models.EventArtistCandidate(
            event_id=uuid.uuid4(), raw_name="Judas Priest"
        )
        assert a.normalized_raw_name != b.normalized_raw_name


class TestMigrationNormalization:
    """The inline normalization in the migration matches the app function."""

    def test_migration_function_matches_app(self) -> None:
        import importlib
        import importlib.util
        import sys

        spec = importlib.util.spec_from_file_location(
            "migration",
            "alembic/versions/c4d5e6f7g8h9_add_normalized_raw_name.py",
        )
        assert spec is not None
        assert spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        sys.modules["migration"] = mod
        spec.loader.exec_module(mod)

        test_cases = [
            "Iron Maiden",
            "KK’s Priest",
            "Motörhead",
            "Beyoncé",
            "José González",
            "François",
            "  The   Band  ",
            "",
            "KK’s Priest at Mötley Crüe",
        ]
        for name in test_cases:
            assert mod._normalize_name(name) == normalize_module.normalize_name(name), (
                f"Mismatch for {name!r}"
            )

        del sys.modules["migration"]
