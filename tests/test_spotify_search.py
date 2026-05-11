"""Tests for SpotifyConnector.search_artists."""

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.connectors.spotify as spotify_module


def _make_settings() -> config_module.Settings:
    """Create test settings with Spotify credentials."""
    return config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        spotify_redirect_path="/callback",
    )


def _make_connector(
    handler: httpx.MockTransport | None = None,
) -> spotify_module.SpotifyConnector:
    """Create a SpotifyConnector wired to a mock transport."""
    settings = _make_settings()
    connector = spotify_module.SpotifyConnector(settings=settings)
    if handler is not None:
        connector._http_client = httpx.AsyncClient(transport=handler)
    connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    return connector


class TestSearchArtists:
    """Tests for search_artists."""

    @pytest.mark.anyio()
    async def test_returns_list_of_artist_dicts(self) -> None:
        api_response = {
            "artists": {
                "items": [
                    {
                        "id": "4gzpq5DPGxSnKTe4SA8HAU",
                        "name": "Coldplay",
                        "genres": ["pop", "rock"],
                        "popularity": 89,
                        "images": [
                            {
                                "url": "https://img.example.com/1.jpg",
                                "height": 640,
                                "width": 640,
                            }
                        ],
                    },
                    {
                        "id": "1dfeR4HaWDbWqFHLkxsg1d",
                        "name": "Queen",
                        "genres": ["classic rock"],
                        "popularity": 85,
                        "images": [],
                    },
                ],
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.search_artists(access_token="token", query="Coldplay")

        assert len(result) == 2
        assert result[0] == {
            "spotify_id": "4gzpq5DPGxSnKTe4SA8HAU",
            "name": "Coldplay",
        }
        assert result[1] == {
            "spotify_id": "1dfeR4HaWDbWqFHLkxsg1d",
            "name": "Queen",
        }

    @pytest.mark.anyio()
    async def test_sends_correct_request_parameters(self) -> None:
        api_response = {"artists": {"items": []}}

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert request.url.path == "/v1/search"
            assert request.headers["Authorization"] == "Bearer my-token"
            params = dict(request.url.params)
            assert params["q"] == "Radiohead"
            assert params["type"] == "artist"
            assert params["limit"] == "10"
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        await connector.search_artists(access_token="my-token", query="Radiohead")

    @pytest.mark.anyio()
    async def test_empty_results_return_empty_list(self) -> None:
        api_response = {"artists": {"items": []}}

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.search_artists(
            access_token="token", query="nonexistent-artist-xyz"
        )

        assert result == []

    @pytest.mark.anyio()
    async def test_respects_limit_parameter(self) -> None:
        api_response = {"artists": {"items": []}}

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            assert params["limit"] == "5"
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        await connector.search_artists(access_token="token", query="Beatles", limit=5)

    @pytest.mark.anyio()
    async def test_only_extracts_id_and_name(self) -> None:
        """Extra fields in the Spotify response are not included in the result."""
        api_response = {
            "artists": {
                "items": [
                    {
                        "id": "abc123",
                        "name": "Test Artist",
                        "genres": ["indie"],
                        "popularity": 42,
                        "images": [{"url": "https://img.example.com/pic.jpg"}],
                        "followers": {"total": 1000},
                        "external_urls": {
                            "spotify": "https://open.spotify.com/artist/abc123"
                        },
                    },
                ],
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.search_artists(
            access_token="token", query="Test Artist"
        )

        assert len(result) == 1
        assert set(result[0].keys()) == {"spotify_id", "name"}
