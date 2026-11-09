"""Tests for the connector framework."""

from unittest.mock import AsyncMock

import httpx
import pytest

import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.connectors.registry as registry_module
import resonance.types as types_module


class FakeConnector(base_module.BaseConnector):
    """Concrete test implementation of BaseConnector."""

    service_type = types_module.ServiceType.SPOTIFY
    capabilities = frozenset({base_module.ConnectorCapability.FOLLOWS})

    def __init__(self) -> None:
        self._http_client = None
        self._budget = ratelimit_module.RateLimitBudget()

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        return base_module.ConnectionConfig(
            auth_type="oauth",
            sync_function="plan_sync",
            sync_style="incremental",
        )

    def get_auth_url(self, state: str) -> str:
        return f"https://fake.example.com/auth?state={state}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        return base_module.TokenResponse(access_token="fake-token")

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        return {"id": "fake-user", "display_name": "Fake User"}


class TestConnectorCapability:
    """Tests for ConnectorCapability enum."""

    def test_listening_history_value(self) -> None:
        assert base_module.ConnectorCapability.LISTENING_HISTORY == "listening_history"

    def test_authentication_value(self) -> None:
        assert base_module.ConnectorCapability.AUTHN == "authn"

    def test_all_capabilities_are_strings(self) -> None:
        for cap in base_module.ConnectorCapability:
            assert isinstance(cap, str)


class TestBaseConnector:
    """Tests for BaseConnector ABC."""

    def test_has_capability_true(self) -> None:
        connector = FakeConnector()
        assert connector.has_capability(base_module.ConnectorCapability.FOLLOWS) is True

    def test_has_capability_false(self) -> None:
        connector = FakeConnector()
        assert (
            connector.has_capability(base_module.ConnectorCapability.LISTENING_HISTORY)
            is False
        )


class TestTokenResponse:
    """Tests for TokenResponse model."""

    def test_minimal(self) -> None:
        token = base_module.TokenResponse(access_token="abc123")
        assert token.access_token == "abc123"
        assert token.refresh_token is None
        assert token.expires_in is None
        assert token.scope is None

    def test_full(self) -> None:
        token = base_module.TokenResponse(
            access_token="abc",
            refresh_token="def",
            expires_in=3600,
            scope="read",
        )
        assert token.refresh_token == "def"
        assert token.expires_in == 3600
        assert token.scope == "read"


class TestArtistData:
    """Tests for ArtistData model."""

    def test_creation(self) -> None:
        artist = base_module.ArtistData(
            external_id="abc",
            name="Test Artist",
            service=types_module.ServiceType.SPOTIFY,
        )
        assert artist.external_id == "abc"
        assert artist.name == "Test Artist"
        assert artist.service == types_module.ServiceType.SPOTIFY


class TestTrackData:
    """Tests for TrackData model."""

    def test_creation(self) -> None:
        track = base_module.TrackData(
            external_id="track1",
            title="Test Track",
            artist_external_id="artist1",
            artist_name="Test Artist",
            service=types_module.ServiceType.SPOTIFY,
        )
        assert track.external_id == "track1"
        assert track.title == "Test Track"
        assert track.artist_external_id == "artist1"
        assert track.artist_name == "Test Artist"


class TestConnectorRegistry:
    """Tests for ConnectorRegistry."""

    def test_register_and_retrieve(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        result = registry.get(types_module.ServiceType.SPOTIFY)
        assert result is connector

    def test_get_unknown_service_returns_none(self) -> None:
        registry = registry_module.ConnectorRegistry()
        result = registry.get(types_module.ServiceType.LASTFM)
        assert result is None

    def test_get_by_capability(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        results = registry.get_by_capability(base_module.ConnectorCapability.FOLLOWS)
        assert len(results) == 1
        assert results[0] is connector

    def test_get_by_capability_no_matches(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        results = registry.get_by_capability(
            base_module.ConnectorCapability.LISTENING_HISTORY
        )
        assert results == []

    def test_all(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        assert registry.all() == [connector]

    def test_all_empty(self) -> None:
        registry = registry_module.ConnectorRegistry()
        assert registry.all() == []


class TestRequestRateLimitCap:
    """Tests for _request rate limit handling."""

    @pytest.mark.asyncio
    async def test_raises_when_retry_after_exceeds_cap(self) -> None:
        """A 429 with Retry-After above _MAX_RATE_LIMIT_WAIT raises."""
        connector = FakeConnector()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request.return_value = httpx.Response(
            429, headers={"Retry-After": "9999"}
        )
        connector._http_client = mock_client

        with pytest.raises(base_module.RateLimitExceededError) as exc_info:
            await connector._request("GET", "https://api.example.com/test")

        assert exc_info.value.retry_after == 9999.0
        assert exc_info.value.max_wait == 120.0

    @pytest.mark.asyncio
    async def test_retries_when_retry_after_within_cap(self) -> None:
        """A 429 with Retry-After within cap retries and succeeds."""
        connector = FakeConnector()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        request = httpx.Request("GET", "https://api.example.com/test")
        mock_client.request.side_effect = [
            httpx.Response(429, headers={"Retry-After": "0"}, request=request),
            httpx.Response(200, json={"ok": True}, request=request),
        ]
        connector._http_client = mock_client

        response = await connector._request("GET", "https://api.example.com/test")
        assert response.status_code == 200
        assert mock_client.request.call_count == 2


class _FakeClock:
    """Monotonic clock that only moves when the code under test sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _responses(*status_retry_after: tuple[int, str | None]) -> list[httpx.Response]:
    request = httpx.Request("GET", "https://api.example.com/test")
    return [
        httpx.Response(
            status,
            headers={"Retry-After": retry} if retry is not None else {},
            request=request,
        )
        for status, retry in status_retry_after
    ]


class TestRequest429Cap:
    """_request bounds 429 retries by count and by wall time (#200)."""

    @pytest.fixture
    def clock(self, monkeypatch: pytest.MonkeyPatch) -> _FakeClock:
        fake = _FakeClock()
        monkeypatch.setattr(base_module.time, "monotonic", fake.monotonic)
        monkeypatch.setattr(base_module.asyncio, "sleep", fake.sleep)
        return fake

    def _connector(self, responses: list[httpx.Response]) -> FakeConnector:
        connector = FakeConnector()
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request.side_effect = responses
        connector._http_client = mock_client
        return connector

    def test_defaults(self) -> None:
        assert base_module.BaseConnector._MAX_RATE_LIMIT_RETRIES == 5
        assert base_module.BaseConnector._RATE_LIMIT_BUDGET == 600.0
        assert (
            base_module.BaseConnector._MAX_RATE_LIMIT_WAIT
            < base_module.BaseConnector._RATE_LIMIT_BUDGET
        )

    async def test_raises_after_max_retries(self, clock: _FakeClock) -> None:
        connector = self._connector(_responses(*[(429, "1")] * 10))

        with pytest.raises(base_module.RateLimitExceededError) as exc_info:
            await connector._request("GET", "https://api.example.com/test")

        assert connector.http_client.request.call_count == 6  # type: ignore[attr-defined]
        assert clock.sleeps == [1.0] * 5
        assert exc_info.value.retry_after == 60.0  # floored; the server said 1s
        assert "budget exhausted" in str(exc_info.value)

    async def test_exhausted_defer_honors_larger_retry_after(
        self, clock: _FakeClock
    ) -> None:
        connector = self._connector(_responses(*[(429, "90")] * 10))

        with pytest.raises(base_module.RateLimitExceededError) as exc_info:
            await connector._request("GET", "https://api.example.com/test")

        assert exc_info.value.retry_after == 90.0

    async def test_transient_backoff_after_429_counts_against_budget(
        self, clock: _FakeClock
    ) -> None:
        connector = self._connector(
            _responses((429, "100"), *[(503, None)] * 50, (200, None))
        )
        connector._RATE_LIMIT_BUDGET = 107.0

        with pytest.raises(base_module.RateLimitExceededError) as exc_info:
            await connector._request("GET", "https://api.example.com/test")

        # Each 5xx response resets the transient counter, so its backoff stays
        # 2s; the budget is what ends the loop.
        assert clock.sleeps == [100.0, 2.0, 2.0, 2.0]
        assert exc_info.value.retry_after == 100.0

    async def test_transient_backoff_without_429_stops_at_budget(
        self, clock: _FakeClock
    ) -> None:
        connector = self._connector(_responses(*[(503, None)] * 10))
        connector._RATE_LIMIT_BUDGET = 5.0

        with pytest.raises(httpx.HTTPStatusError):
            await connector._request("GET", "https://api.example.com/test")

        assert clock.sleeps == [2.0, 2.0]

    async def test_transient_exception_backoff_stops_at_budget(
        self, clock: _FakeClock
    ) -> None:
        connector = self._connector([])
        connector.http_client.request.side_effect = httpx.ConnectError("down")  # type: ignore[attr-defined]
        connector._RATE_LIMIT_BUDGET = 10.0

        with pytest.raises(httpx.ConnectError):
            await connector._request("GET", "https://api.example.com/test")

        assert clock.sleeps == [2.0, 4.0]

    async def test_succeeds_within_retry_cap(self, clock: _FakeClock) -> None:
        connector = self._connector(_responses(*[(429, "1")] * 5, (200, None)))

        response = await connector._request("GET", "https://api.example.com/test")

        assert response.status_code == 200
        assert clock.sleeps == [1.0] * 5

    async def test_raises_when_wait_would_exceed_wall_budget(
        self, clock: _FakeClock
    ) -> None:
        connector = self._connector(_responses(*[(429, "100")] * 10))
        connector._MAX_RATE_LIMIT_RETRIES = 100

        with pytest.raises(base_module.RateLimitExceededError) as exc_info:
            await connector._request("GET", "https://api.example.com/test")

        # Six 100s waits fit in 600s; a seventh would end past the budget.
        assert clock.sleeps == [100.0] * 6
        assert exc_info.value.retry_after == 100.0

    async def test_budget_counts_time_outside_429_waits(
        self, clock: _FakeClock
    ) -> None:
        connector = self._connector(_responses((503, None), (429, "50"), (200, None)))
        connector._RATE_LIMIT_BUDGET = 50.0

        with pytest.raises(base_module.RateLimitExceededError):
            await connector._request("GET", "https://api.example.com/test")

        assert clock.sleeps == [2.0]  # the 5xx backoff; the 429 wait never ran

    async def test_retry_after_above_max_wait_raises_immediately(
        self, clock: _FakeClock
    ) -> None:
        connector = self._connector(_responses((429, "121")))

        with pytest.raises(base_module.RateLimitExceededError) as exc_info:
            await connector._request("GET", "https://api.example.com/test")

        assert clock.sleeps == []
        assert exc_info.value.retry_after == 121.0
        assert exc_info.value.max_wait == 120.0

    async def test_per_connector_override(self, clock: _FakeClock) -> None:
        connector = self._connector(_responses(*[(429, "1")] * 3))
        connector._MAX_RATE_LIMIT_RETRIES = 1

        with pytest.raises(base_module.RateLimitExceededError):
            await connector._request("GET", "https://api.example.com/test")

        assert clock.sleeps == [1.0]

    async def test_429_count_is_per_request(self, clock: _FakeClock) -> None:
        connector = self._connector(
            _responses(*[(429, "1")] * 5, (200, None), *[(429, "1")] * 5, (200, None))
        )

        await connector._request("GET", "https://api.example.com/a")
        response = await connector._request("GET", "https://api.example.com/b")

        assert response.status_code == 200
