"""Tests for the Last.fm sync strategy."""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.connectors.lastfm as lastfm_module
import resonance.crypto as crypto_module
import resonance.sync.lastfm as lastfm_sync_module
import resonance.types as types_module

_TEST_ENCRYPTION_KEY = "pVnp8Zq06TgKRtq3xvI5zouN1E84mlTsX1V9UhMwJuI="


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_connection(
    *,
    session_key: str = "test-session-key",
    external_user_id: str = "testuser",
    sync_watermark: dict[str, dict[str, object]] | None = None,
) -> MagicMock:
    """Create a mock ServiceConnection with an encrypted session key."""
    conn = MagicMock()
    conn.id = uuid.uuid4()
    conn.external_user_id = external_user_id
    conn.encrypted_access_token = crypto_module.encrypt_token(
        session_key, _TEST_ENCRYPTION_KEY
    )
    conn.service_type = types_module.ServiceType.LASTFM
    conn.sync_watermark = sync_watermark or {}
    return conn


def _make_task(
    *,
    params: dict[str, object] | None = None,
    user_id: uuid.UUID | None = None,
    connection_id: uuid.UUID | None = None,
) -> MagicMock:
    """Create a mock SyncTask."""
    task = MagicMock()
    task.id = uuid.uuid4()
    task.user_id = user_id or uuid.uuid4()
    task.service_connection_id = connection_id or uuid.uuid4()
    task.params = params or {}
    task.progress_current = 0
    task.progress_total = None
    return task


def _make_lastfm_connector() -> MagicMock:
    """Create a mock LastFmConnector."""
    connector = MagicMock(spec=lastfm_module.LastFmConnector)
    connector.get_recent_tracks = AsyncMock(return_value={})
    connector.get_loved_tracks = AsyncMock(return_value={})
    return connector


def _recent_tracks_response(
    tracks: list[dict[str, Any]],
    page: str = "1",
    total_pages: str = "1",
    total: str | None = None,
) -> dict[str, Any]:
    """Build a Last.fm user.getRecentTracks response."""
    if total is None:
        total = str(len(tracks))
    return {
        "recenttracks": {
            "track": tracks,
            "@attr": {"page": page, "totalPages": total_pages, "total": total},
        }
    }


def _loved_tracks_response(
    tracks: list[dict[str, Any]],
    page: str = "1",
    total_pages: str = "1",
    total: str | None = None,
) -> dict[str, Any]:
    """Build a Last.fm user.getLovedTracks response."""
    if total is None:
        total = str(len(tracks))
    return {
        "lovedtracks": {
            "track": tracks,
            "@attr": {"page": page, "totalPages": total_pages, "total": total},
        }
    }


def _make_recent_track(
    *,
    artist_name: str = "Test Artist",
    artist_mbid: str = "",
    track_name: str = "Test Song",
    track_mbid: str = "",
    uts: str = "1700000000",
    nowplaying: bool = False,
) -> dict[str, Any]:
    """Build a single track dict matching Last.fm getRecentTracks format."""
    track: dict[str, Any] = {
        "artist": {"mbid": artist_mbid, "#text": artist_name},
        "name": track_name,
        "mbid": track_mbid,
    }
    if nowplaying:
        track["@attr"] = {"nowplaying": "true"}
    else:
        track["date"] = {"uts": uts, "#text": "14 Nov 2023, 22:13"}
    return track


def _make_loved_track(
    *,
    artist_name: str = "Loved Artist",
    artist_mbid: str = "",
    track_name: str = "Loved Song",
    track_mbid: str = "",
    uts: str = "1700000000",
) -> dict[str, Any]:
    """Build a single track dict matching Last.fm getLovedTracks format."""
    return {
        "artist": {"mbid": artist_mbid, "name": artist_name},
        "name": track_name,
        "mbid": track_mbid,
        "date": {"uts": uts, "#text": "14 Nov 2023, 22:13"},
    }


# ---------------------------------------------------------------------------
# Tests: concurrency
# ---------------------------------------------------------------------------


class TestLastFmConcurrency:
    """Tests for concurrency setting."""

    def test_concurrency_is_sequential(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        assert strategy.concurrency == "sequential"


# ---------------------------------------------------------------------------
# Tests: plan()
# ---------------------------------------------------------------------------


class TestLastFmPlan:
    """Tests for LastFmSyncStrategy.plan()."""

    @pytest.mark.asyncio
    async def test_returns_two_descriptors(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = _make_lastfm_connector()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 2

    @pytest.mark.asyncio
    async def test_descriptor_data_types(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = _make_lastfm_connector()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        data_types = [d.params["data_type"] for d in descriptors]
        assert data_types == ["recent_tracks", "loved_tracks"]

    @pytest.mark.asyncio
    async def test_recent_tracks_includes_from_ts_with_watermark(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = _make_lastfm_connector()
        connection = _make_connection(
            sync_watermark={"recent_tracks": {"last_scrobbled_at": 1700000000}}
        )

        descriptors = await strategy.plan(session, connection, connector)

        recent = descriptors[0]
        assert recent.params["from_ts"] == 1700000000

    @pytest.mark.asyncio
    async def test_recent_tracks_no_from_ts_without_watermark(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = _make_lastfm_connector()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        recent = descriptors[0]
        assert recent.params.get("from_ts") is None

    @pytest.mark.asyncio
    async def test_descriptors_have_descriptions(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = _make_lastfm_connector()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert desc.description

    @pytest.mark.asyncio
    async def test_task_type_is_time_range(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = _make_lastfm_connector()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert desc.task_type == types_module.SyncTaskType.TIME_RANGE


# ---------------------------------------------------------------------------
# Tests: track parsing helpers
# ---------------------------------------------------------------------------


class TestParseRecentTrack:
    """Tests for _parse_recent_track helper."""

    def test_parses_normal_track(self) -> None:
        raw = _make_recent_track(
            artist_name="Radiohead",
            track_name="Everything In Its Right Place",
            track_mbid="abc123",
            artist_mbid="def456",
            uts="1700000000",
        )
        track_data, timestamp = lastfm_sync_module._parse_recent_track(raw)
        assert track_data.title == "Everything In Its Right Place"
        assert track_data.artist_name == "Radiohead"
        assert track_data.external_id == "abc123"
        assert track_data.artist_external_id == "def456"
        assert track_data.service == types_module.ServiceType.LASTFM
        assert timestamp == 1700000000

    def test_skips_nowplaying_track(self) -> None:
        raw = _make_recent_track(nowplaying=True)
        result = lastfm_sync_module._parse_recent_track(raw)
        assert result is None

    def test_uses_track_name_as_fallback_external_id(self) -> None:
        raw = _make_recent_track(track_mbid="", artist_mbid="")
        track_data, _ = lastfm_sync_module._parse_recent_track(raw)  # type: ignore[misc]
        # When no mbid, uses a generated key
        assert track_data.external_id != ""


class TestParseLovedTrack:
    """Tests for _parse_loved_track helper."""

    def test_parses_loved_track(self) -> None:
        raw = _make_loved_track(
            artist_name="Bjork",
            track_name="Hyperballad",
            track_mbid="xyz789",
            artist_mbid="uvw321",
        )
        track_data = lastfm_sync_module._parse_loved_track(raw)
        assert track_data.title == "Hyperballad"
        assert track_data.artist_name == "Bjork"
        assert track_data.external_id == "xyz789"
        assert track_data.artist_external_id == "uvw321"
        assert track_data.service == types_module.ServiceType.LASTFM

    def test_uses_track_name_as_fallback_external_id(self) -> None:
        raw = _make_loved_track(track_mbid="", artist_mbid="")
        track_data = lastfm_sync_module._parse_loved_track(raw)
        assert track_data.external_id != ""


# ---------------------------------------------------------------------------
# Tests: execute() — recent_tracks
# ---------------------------------------------------------------------------


class TestSyncRecentTracks:
    """Tests for recent_tracks sync execution."""

    @pytest.mark.asyncio
    async def test_sync_recent_tracks_basic(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        connection = _make_connection()
        connector = _make_lastfm_connector()
        task = _make_task(
            params={"data_type": "recent_tracks"},
            connection_id=connection.id,
        )

        tracks = [
            _make_recent_track(uts="1700000002", track_name="Song A"),
            _make_recent_track(uts="1700000001", track_name="Song B"),
        ]
        connector.get_recent_tracks = AsyncMock(
            side_effect=[
                _recent_tracks_response(tracks, page="1", total_pages="1"),
            ]
        )

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "resonance.sync.lastfm.runner_module.bulk_fetch_artists",
                AsyncMock(return_value={}),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module.bulk_fetch_tracks",
                AsyncMock(return_value={}),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_artist_from_track",
                AsyncMock(),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_track",
                AsyncMock(return_value=True),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_listening_event",
                AsyncMock(),
            )
            mp.setattr(
                "resonance.sync.lastfm.crypto_module.decrypt_token",
                MagicMock(return_value="session-key"),
            )

            result = await strategy.execute(session, task, connector, connection)

        assert result["items_created"] == 2
        assert result["watermark"]["last_scrobbled_at"] == 1700000002

    @pytest.mark.asyncio
    async def test_sync_recent_tracks_skips_nowplaying(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        connection = _make_connection()
        connector = _make_lastfm_connector()
        task = _make_task(
            params={"data_type": "recent_tracks"},
            connection_id=connection.id,
        )

        tracks = [
            _make_recent_track(nowplaying=True, track_name="Now Playing"),
            _make_recent_track(uts="1700000001", track_name="Past Song"),
        ]
        connector.get_recent_tracks = AsyncMock(
            return_value=_recent_tracks_response(tracks),
        )

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "resonance.sync.lastfm.runner_module.bulk_fetch_artists",
                AsyncMock(return_value={}),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module.bulk_fetch_tracks",
                AsyncMock(return_value={}),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_artist_from_track",
                AsyncMock(),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_track",
                AsyncMock(return_value=True),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_listening_event",
                AsyncMock(),
            )
            mp.setattr(
                "resonance.sync.lastfm.crypto_module.decrypt_token",
                MagicMock(return_value="session-key"),
            )

            result = await strategy.execute(session, task, connector, connection)

        # Only the non-nowplaying track should be counted
        assert result["items_created"] == 1


# ---------------------------------------------------------------------------
# Tests: execute() — loved_tracks
# ---------------------------------------------------------------------------


class TestSyncLovedTracks:
    """Tests for loved_tracks sync execution."""

    @pytest.mark.asyncio
    async def test_sync_loved_tracks_basic(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        connection = _make_connection()
        connector = _make_lastfm_connector()
        task = _make_task(
            params={"data_type": "loved_tracks"},
            connection_id=connection.id,
        )

        tracks = [
            _make_loved_track(track_name="Loved A"),
            _make_loved_track(track_name="Loved B"),
        ]
        connector.get_loved_tracks = AsyncMock(
            side_effect=[
                _loved_tracks_response(tracks, page="1", total_pages="1"),
            ]
        )

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "resonance.sync.lastfm.runner_module.bulk_fetch_artists",
                AsyncMock(return_value={}),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module.bulk_fetch_tracks",
                AsyncMock(return_value={}),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_artist_from_track",
                AsyncMock(),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_track",
                AsyncMock(return_value=True),
            )
            mp.setattr(
                "resonance.sync.lastfm.runner_module._upsert_user_track_relation",
                AsyncMock(),
            )
            mp.setattr(
                "resonance.sync.lastfm.crypto_module.decrypt_token",
                MagicMock(return_value="session-key"),
            )

            result = await strategy.execute(session, task, connector, connection)

        assert result["items_created"] == 2
        assert result["items_updated"] == 0


# ---------------------------------------------------------------------------
# Tests: execute() dispatch
# ---------------------------------------------------------------------------


class TestExecuteDispatch:
    """Tests for execute() routing by data_type."""

    @pytest.mark.asyncio
    async def test_unknown_data_type_returns_empty(self) -> None:
        strategy = lastfm_sync_module.LastFmSyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connection = _make_connection()
        connector = _make_lastfm_connector()
        task = _make_task(params={"data_type": "unknown"})

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(
                "resonance.sync.lastfm.crypto_module.decrypt_token",
                MagicMock(return_value="session-key"),
            )
            result = await strategy.execute(session, task, connector, connection)

        assert result["items_created"] == 0
        assert result["items_updated"] == 0
