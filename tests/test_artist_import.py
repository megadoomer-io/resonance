"""Tests for the artist-import-by-MBID service (issue #115 Phase 2)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.models.music as music_models
import resonance.services.artist_import as artist_import_module


def _session_returning(scalar: object) -> MagicMock:
    """Build a session whose execute() yields ``scalar`` from scalar_one_or_none."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = scalar
    session = MagicMock()
    session.execute = AsyncMock(return_value=result)
    session.flush = AsyncMock()
    return session


class TestFindLocalArtistByMbid:
    """Dedup lookup by MBID (canonical + legacy storage)."""

    @pytest.mark.anyio()
    async def test_returns_match(self) -> None:
        existing = MagicMock(spec=music_models.Artist)
        session = _session_returning(existing)

        found = await artist_import_module.find_local_artist_by_mbid(session, "mbid-1")

        assert found is existing
        session.execute.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_returns_none_when_absent(self) -> None:
        session = _session_returning(None)

        found = await artist_import_module.find_local_artist_by_mbid(
            session, "mbid-missing"
        )

        assert found is None


class TestImportArtistByMbid:
    """Find-or-create an Artist from an MBID."""

    @pytest.mark.anyio()
    async def test_returns_existing_without_mb_lookup(self) -> None:
        existing = MagicMock(spec=music_models.Artist)
        session = _session_returning(existing)
        connector = AsyncMock()

        result = await artist_import_module.import_artist_by_mbid(
            session, connector, "mbid-existing"
        )

        assert result is existing
        connector.get_artist_by_mbid.assert_not_awaited()
        session.add.assert_not_called()

    @pytest.mark.anyio()
    async def test_creates_artist_from_mb_dict(self) -> None:
        session = _session_returning(None)
        connector = AsyncMock()
        connector.get_artist_by_mbid.return_value = {
            "mbid": "mbid-new",
            "name": "Elder",
            "disambiguation": "stoner rock",
            "artist_type": "Group",
            "area": "United States",
            "begin_year": 2005,
            "end_year": None,
        }

        result = await artist_import_module.import_artist_by_mbid(
            session, connector, "mbid-new"
        )

        connector.get_artist_by_mbid.assert_awaited_once_with("mbid-new")
        session.add.assert_called_once()
        added = session.add.call_args.args[0]
        assert added is result
        assert isinstance(result, music_models.Artist)
        assert result.name == "Elder"
        assert result.disambiguation == "stoner rock"
        assert result.artist_type == "Group"
        assert result.area == "United States"
        assert result.begin_year == 2005
        assert result.end_year is None
        assert result.service_links == {"musicbrainz": {"id": "mbid-new"}}
        session.flush.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_blank_optional_fields_become_none(self) -> None:
        session = _session_returning(None)
        connector = AsyncMock()
        connector.get_artist_by_mbid.return_value = {
            "mbid": "mbid-min",
            "name": "Minimal",
            "disambiguation": "",
            "artist_type": "",
            "area": "",
            "begin_year": None,
            "end_year": None,
        }

        result = await artist_import_module.import_artist_by_mbid(
            session, connector, "mbid-min"
        )

        assert isinstance(result, music_models.Artist)
        assert result.disambiguation is None
        assert result.artist_type is None
        assert result.area is None

    @pytest.mark.anyio()
    async def test_returns_none_when_mb_unresolved(self) -> None:
        session = _session_returning(None)
        connector = AsyncMock()
        connector.get_artist_by_mbid.return_value = None

        result = await artist_import_module.import_artist_by_mbid(
            session, connector, "mbid-ghost"
        )

        assert result is None
        session.add.assert_not_called()
        session.flush.assert_not_awaited()
