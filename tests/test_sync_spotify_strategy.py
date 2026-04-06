"""Tests for the Spotify sync strategy."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.connectors.base as connector_base
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
import resonance.sync.base as sync_base
import resonance.sync.spotify as sync_spotify_module
import resonance.types as types_module

_TEST_ENCRYPTION_KEY = "pVnp8Zq06TgKRtq3xvI5zouN1E84mlTsX1V9UhMwJuI="


def _make_connection(
    access_token: str = "test-token",
    sync_watermark: dict[str, dict[str, object]] | None = None,
) -> MagicMock:
    """Create a mock ServiceConnection with an encrypted access token."""
    conn = MagicMock()
    conn.id = uuid.uuid4()
    conn.encrypted_access_token = crypto_module.encrypt_token(
        access_token, _TEST_ENCRYPTION_KEY
    )
    conn.service_type = types_module.ServiceType.SPOTIFY
    conn.sync_watermark = sync_watermark or {}
    return conn


def _make_task(
    params: dict[str, object] | None = None,
) -> MagicMock:
    """Create a mock SyncTask."""
    task = MagicMock()
    task.id = uuid.uuid4()
    task.user_id = uuid.uuid4()
    task.service_connection_id = uuid.uuid4()
    task.params = params or {}
    return task


class TestSpotifyPlan:
    """Tests for SpotifySyncStrategy.plan()."""

    @pytest.mark.asyncio
    async def test_returns_three_descriptors(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 3

    @pytest.mark.asyncio
    async def test_descriptor_data_types(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        data_types = [d.params["data_type"] for d in descriptors]
        assert data_types == ["followed_artists", "saved_tracks", "recently_played"]

    @pytest.mark.asyncio
    async def test_descriptors_have_descriptions(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert desc.description != ""

    @pytest.mark.asyncio
    async def test_descriptors_do_not_contain_access_token(self) -> None:
        """Access tokens must not be persisted in task params."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection(access_token="my-secret-token")

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert "access_token" not in desc.params

    @pytest.mark.asyncio
    async def test_passes_watermarks_to_params(self) -> None:
        """Watermarks from connection are passed into descriptor params."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "recently_played": {"last_played_at": "2026-04-05T12:00:00Z"},
                "saved_tracks": {"last_saved_at": "2026-04-05T12:00:00Z"},
                "followed_artists": {"after_cursor": "abc123"},
            }
        )
        descriptors = await strategy.plan(session, connection, connector)
        by_type = {d.params["data_type"]: d for d in descriptors}
        assert (
            by_type["recently_played"].params["last_played_at"]
            == "2026-04-05T12:00:00Z"
        )
        assert by_type["saved_tracks"].params["last_saved_at"] == "2026-04-05T12:00:00Z"
        assert by_type["followed_artists"].params["after_cursor"] == "abc123"

    @pytest.mark.asyncio
    async def test_no_watermarks_passes_none(self) -> None:
        """Without watermarks, params contain None for watermark fields."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()
        descriptors = await strategy.plan(session, connection, connector)
        by_type = {d.params["data_type"]: d for d in descriptors}
        assert by_type["recently_played"].params.get("last_played_at") is None
        assert by_type["saved_tracks"].params.get("last_saved_at") is None
        assert by_type["followed_artists"].params.get("after_cursor") is None

    @pytest.mark.asyncio
    async def test_incremental_description(self) -> None:
        """Descriptions mention 'new' for incremental sync."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "recently_played": {"last_played_at": "2026-04-05T12:00:00Z"}
            }
        )
        descriptors = await strategy.plan(session, connection, connector)
        by_type = {d.params["data_type"]: d for d in descriptors}
        assert "new" in by_type["recently_played"].description.lower()
        # followed_artists and saved_tracks have no watermark,
        # so keep original description
        assert (
            by_type["followed_artists"].description == "Fetching your followed artists"
        )
        assert by_type["saved_tracks"].description == "Fetching your saved tracks"

    def test_concurrency_is_sequential(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        assert strategy.concurrency == "sequential"


class TestSpotifyExecute:
    """Tests for SpotifySyncStrategy.execute()."""

    @pytest.mark.asyncio
    async def test_followed_artists_dispatches_to_connector(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "followed_artists"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_followed_artists",
                new_callable=AsyncMock,
                return_value=(2, 1),
            ) as mock_sync,
        ):
            result = await strategy.execute(session, task, connector)

        mock_sync.assert_awaited_once()
        assert result["items_created"] == 2
        assert result["items_updated"] == 1

    @pytest.mark.asyncio
    async def test_unknown_data_type_returns_zero_counts(self) -> None:
        """Unknown data_type logs a warning and returns zero counts."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "nonexistent_type"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with patch.object(
            strategy,
            "_get_access_token",
            new_callable=AsyncMock,
            return_value="tok",
        ):
            result = await strategy.execute(session, task, connector)

        assert result["items_created"] == 0
        assert result["items_updated"] == 0

    @pytest.mark.asyncio
    async def test_rate_limit_raises_defer_request(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "followed_artists"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_followed_artists",
                new_callable=AsyncMock,
                side_effect=connector_base.RateLimitExceededError(
                    retry_after=300.0, max_wait=120.0
                ),
            ),
            pytest.raises(sync_base.DeferRequest) as exc_info,
        ):
            await strategy.execute(session, task, connector)

        assert exc_info.value.retry_after == 300.0
        assert exc_info.value.resume_params["data_type"] == "followed_artists"


class TestCastConnector:
    """Tests for _cast_connector helper."""

    def test_returns_spotify_connector(self) -> None:
        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        result = sync_spotify_module._cast_connector(connector)
        assert result is connector

    def test_raises_type_error_for_wrong_type(self) -> None:
        connector = MagicMock(spec=connector_base.BaseConnector)
        with pytest.raises(TypeError, match="Expected SpotifyConnector"):
            sync_spotify_module._cast_connector(connector)
