"""Tests for MusicBrainz URL relations lookup on ListenBrainzConnector."""

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.ratelimit as ratelimit_module


def _make_connector() -> listenbrainz_module.ListenBrainzConnector:
    settings = config_module.Settings(
        musicbrainz_client_id="test-mb-client-id",
        musicbrainz_client_secret="test-mb-client-secret",
        musicbrainz_redirect_path="/callback",
    )
    connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
    connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    return connector


class TestGetRecordingSpotifyId:
    """Tests for get_recording_spotify_id MusicBrainz URL relations lookup."""

    @pytest.mark.anyio()
    async def test_returns_spotify_id_from_url_relation(self) -> None:
        mb_response = {
            "id": "rec-aaa",
            "title": "Lateralus",
            "relations": [
                {
                    "type": "free streaming",
                    "url": {
                        "resource": "https://open.spotify.com/track/4uLU6hMCjMI75M1A2tKUQC"
                    },
                },
            ],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert "/recording/rec-aaa" in str(request.url)
            assert "inc=url-rels" in str(request.url)
            assert "fmt=json" in str(request.url)
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("rec-aaa")

        assert result == "4uLU6hMCjMI75M1A2tKUQC"

    @pytest.mark.anyio()
    async def test_returns_none_when_no_relations(self) -> None:
        mb_response = {
            "id": "rec-bbb",
            "title": "No Links",
            "relations": [],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("rec-bbb")

        assert result is None

    @pytest.mark.anyio()
    async def test_returns_none_when_no_spotify_relation(self) -> None:
        mb_response = {
            "id": "rec-ccc",
            "title": "YouTube Only",
            "relations": [
                {
                    "type": "free streaming",
                    "url": {"resource": "https://www.youtube.com/watch?v=abc123"},
                },
            ],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("rec-ccc")

        assert result is None

    @pytest.mark.anyio()
    async def test_returns_none_on_404(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={"error": "Not Found"})

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("nonexistent")

        assert result is None

    @pytest.mark.anyio()
    async def test_returns_none_on_400(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"error": "Bad Request"})

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("bad-format")

        assert result is None

    @pytest.mark.anyio()
    async def test_strips_query_params_from_spotify_url(self) -> None:
        mb_response = {
            "id": "rec-ddd",
            "title": "With Params",
            "relations": [
                {
                    "type": "free streaming",
                    "url": {
                        "resource": "https://open.spotify.com/track/ABC123?si=xyz&nd=1"
                    },
                },
            ],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("rec-ddd")

        assert result == "ABC123"

    @pytest.mark.anyio()
    async def test_picks_first_spotify_url(self) -> None:
        mb_response = {
            "id": "rec-eee",
            "title": "Multiple Links",
            "relations": [
                {
                    "type": "free streaming",
                    "url": {"resource": "https://www.youtube.com/watch?v=yt1"},
                },
                {
                    "type": "free streaming",
                    "url": {"resource": "https://open.spotify.com/track/FIRST"},
                },
                {
                    "type": "streaming",
                    "url": {"resource": "https://open.spotify.com/track/SECOND"},
                },
            ],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("rec-eee")

        assert result == "FIRST"

    @pytest.mark.anyio()
    async def test_handles_missing_relations_key(self) -> None:
        mb_response = {"id": "rec-fff", "title": "No Relations Key"}

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_recording_spotify_id("rec-fff")

        assert result is None
