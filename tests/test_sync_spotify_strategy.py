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


def _make_connection(access_token: str = "test-token") -> MagicMock:
    """Create a mock ServiceConnection with an encrypted access token."""
    conn = MagicMock()
    conn.id = uuid.uuid4()
    conn.encrypted_access_token = crypto_module.encrypt_token(
        access_token, _TEST_ENCRYPTION_KEY
    )
    conn.service_type = types_module.ServiceType.SPOTIFY
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
    async def test_descriptors_include_decrypted_token(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection(access_token="my-secret-token")

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert desc.params["access_token"] == "my-secret-token"

    def test_concurrency_is_sequential(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        assert strategy.concurrency == "sequential"


class TestSpotifyExecute:
    """Tests for SpotifySyncStrategy.execute()."""

    @pytest.mark.asyncio
    async def test_followed_artists_dispatches_to_connector(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(
            params={"data_type": "followed_artists", "access_token": "tok"}
        )
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with patch.object(
            sync_spotify_module,
            "_sync_followed_artists",
            new_callable=AsyncMock,
            return_value=(2, 1),
        ) as mock_sync:
            result = await strategy.execute(session, task, connector)

        mock_sync.assert_awaited_once()
        assert result["items_created"] == 2
        assert result["items_updated"] == 1

    @pytest.mark.asyncio
    async def test_rate_limit_raises_defer_request(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(
            params={"data_type": "followed_artists", "access_token": "tok"}
        )
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
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
