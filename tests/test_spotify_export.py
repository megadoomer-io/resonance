"""Tests for Spotify playlist export methods."""

import json

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.connectors.spotify as spotify_module


def _make_connector() -> spotify_module.SpotifyConnector:
    """Create a SpotifyConnector with test settings and no rate limiting."""
    settings = config_module.Settings(
        spotify_client_id="test_id",
        spotify_client_secret="test_secret",
        spotify_redirect_path="/callback",
    )
    connector = spotify_module.SpotifyConnector(settings=settings)
    connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    return connector


class TestSpotifyPlaylistWriteCapability:
    """Verify PLAYLIST_WRITE is declared in capabilities."""

    def test_playlist_write_in_capabilities(self) -> None:
        connector = _make_connector()
        assert base_module.ConnectorCapability.PLAYLIST_WRITE in connector.capabilities

    def test_has_capability_playlist_write(self) -> None:
        connector = _make_connector()
        assert connector.has_capability(base_module.ConnectorCapability.PLAYLIST_WRITE)


class TestCreatePlaylist:
    """Tests for create_playlist."""

    @pytest.mark.anyio()
    async def test_posts_to_me_playlists(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "POST"
            assert request.url.path == "/v1/me/playlists"
            assert request.headers["Authorization"] == "Bearer test-token"
            body = json.loads(request.content)
            assert body["name"] == "My Playlist"
            assert body["description"] == "A test playlist"
            # Private: the grant has playlist-modify-private, not -public.
            assert body["public"] is False
            return httpx.Response(
                201, json={"id": "playlist-abc123", "name": "My Playlist"}
            )

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.create_playlist(
            access_token="test-token",
            name="My Playlist",
            description="A test playlist",
        )

        assert result == "playlist-abc123"

    @pytest.mark.anyio()
    async def test_default_empty_description(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            assert body["description"] == ""
            return httpx.Response(201, json={"id": "pl-456"})

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.create_playlist(
            access_token="test-token",
            name="Untitled",
        )

        assert result == "pl-456"


class TestAddTracksToPlaylist:
    """Tests for add_tracks_to_playlist."""

    @pytest.mark.anyio()
    async def test_posts_uris_to_playlist_items(self) -> None:
        uris = ["spotify:track:aaa", "spotify:track:bbb"]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "POST"
            assert request.url.path == "/v1/playlists/pl-123/items"
            assert request.headers["Authorization"] == "Bearer tok"
            body = json.loads(request.content)
            assert body["uris"] == uris
            return httpx.Response(200, json={"snapshot_id": "snap1"})

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        await connector.add_tracks_to_playlist(
            access_token="tok",
            playlist_id="pl-123",
            uris=uris,
        )

    @pytest.mark.anyio()
    async def test_batches_over_100_tracks(self) -> None:
        uris = [f"spotify:track:{i:04d}" for i in range(150)]
        batches_received: list[list[str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            batches_received.append(body["uris"])
            return httpx.Response(200, json={"snapshot_id": "snap"})

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        await connector.add_tracks_to_playlist(
            access_token="tok",
            playlist_id="pl-123",
            uris=uris,
        )

        assert len(batches_received) == 2
        assert len(batches_received[0]) == 100
        assert len(batches_received[1]) == 50
        assert batches_received[0] == uris[:100]
        assert batches_received[1] == uris[100:]

    @pytest.mark.anyio()
    async def test_exactly_100_tracks_single_batch(self) -> None:
        uris = [f"spotify:track:{i:04d}" for i in range(100)]
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            return httpx.Response(200, json={"snapshot_id": "snap"})

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        await connector.add_tracks_to_playlist(
            access_token="tok",
            playlist_id="pl-123",
            uris=uris,
        )

        assert call_count == 1


class TestReplacePlaylistTracks:
    """Tests for replace_playlist_tracks."""

    @pytest.mark.anyio()
    async def test_puts_uris_to_playlist_items(self) -> None:
        uris = ["spotify:track:aaa", "spotify:track:bbb"]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "PUT"
            assert request.url.path == "/v1/playlists/pl-999/items"
            assert request.headers["Authorization"] == "Bearer tok"
            body = json.loads(request.content)
            assert body["uris"] == uris
            return httpx.Response(200, json={"snapshot_id": "snap2"})

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        await connector.replace_playlist_tracks(
            access_token="tok",
            playlist_id="pl-999",
            uris=uris,
        )


class TestSearchTrack:
    """Tests for search_track."""

    @pytest.mark.anyio()
    async def test_returns_result_with_id_and_popularity_when_found(self) -> None:
        search_response = {
            "tracks": {
                "items": [
                    {
                        "id": "track-xyz",
                        "name": "Bohemian Rhapsody",
                        "popularity": 87,
                        "artists": [{"id": "art1", "name": "Queen"}],
                    }
                ]
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert "/v1/search" in str(request.url)
            assert "type=track" in str(request.url)
            assert "limit=1" in str(request.url)
            assert request.headers["Authorization"] == "Bearer tok"
            return httpx.Response(200, json=search_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.search_track(
            access_token="tok",
            title="Bohemian Rhapsody",
            artist_name="Queen",
        )

        assert result is not None
        assert result.track_id == "track-xyz"
        assert result.popularity == 87

    @pytest.mark.anyio()
    async def test_popularity_none_when_field_absent(self) -> None:
        """A track object without a ``popularity`` field yields popularity=None."""
        search_response = {
            "tracks": {
                "items": [
                    {
                        "id": "track-no-pop",
                        "name": "Obscure Track",
                        "artists": [{"id": "art1", "name": "Nobody"}],
                    }
                ]
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=search_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.search_track(
            access_token="tok",
            title="Obscure Track",
            artist_name="Nobody",
        )

        assert result is not None
        assert result.track_id == "track-no-pop"
        assert result.popularity is None

    @pytest.mark.anyio()
    async def test_returns_none_when_not_found(self) -> None:
        search_response: dict[str, dict[str, list[object]]] = {"tracks": {"items": []}}

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=search_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.search_track(
            access_token="tok",
            title="Nonexistent Song",
            artist_name="Unknown Artist",
        )

        assert result is None

    @pytest.mark.anyio()
    async def test_query_format(self) -> None:
        """Verify the search query format includes track: and artist: prefixes."""
        captured_url: str = ""

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal captured_url
            captured_url = str(request.url)
            return httpx.Response(200, json={"tracks": {"items": []}})

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        await connector.search_track(
            access_token="tok",
            title="Test Song",
            artist_name="Test Artist",
        )

        # URL-encoded query should contain track: and artist: prefixes
        assert "track%3ATest" in captured_url or "track:Test" in captured_url
        assert "artist%3ATest" in captured_url or "artist:Test" in captured_url


class TestGetTracks:
    """Tests for get_tracks (batch popularity lookup by Spotify track ID)."""

    @pytest.mark.anyio()
    async def test_returns_popularity_keyed_by_id(self) -> None:
        api_response = {
            "tracks": [
                {"id": "id-a", "name": "A", "popularity": 50},
                {"id": "id-b", "name": "B", "popularity": 10},
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert request.url.path == "/v1/tracks"
            params = dict(request.url.params)
            assert params["ids"] == "id-a,id-b"
            assert request.headers["Authorization"] == "Bearer tok"
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.get_tracks(
            access_token="tok", track_ids=["id-a", "id-b"]
        )

        assert result == {"id-a": 50, "id-b": 10}

    @pytest.mark.anyio()
    async def test_skips_null_tracks_and_missing_popularity(self) -> None:
        """Null track entries (unknown IDs) and absent popularity are skipped."""
        api_response = {
            "tracks": [
                {"id": "id-a", "name": "A", "popularity": 50},
                None,
                {"id": "id-c", "name": "C"},
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.get_tracks(
            access_token="tok", track_ids=["id-a", "missing", "id-c"]
        )

        assert result == {"id-a": 50}

    @pytest.mark.anyio()
    async def test_empty_input_returns_empty_dict_without_request(self) -> None:
        called = False

        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            nonlocal called
            called = True
            return httpx.Response(200, json={"tracks": []})

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.get_tracks(access_token="tok", track_ids=[])

        assert result == {}
        assert called is False

    @pytest.mark.anyio()
    async def test_batches_over_50_ids(self) -> None:
        ids = [f"id-{i:03d}" for i in range(120)]
        batches: list[list[str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            ids_param = dict(request.url.params)["ids"]
            batches.append(ids_param.split(","))
            return httpx.Response(
                200,
                json={
                    "tracks": [
                        {"id": tid, "popularity": 1} for tid in ids_param.split(",")
                    ]
                },
            )

        transport = httpx.MockTransport(handler)
        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(transport=transport)

        result = await connector.get_tracks(access_token="tok", track_ids=ids)

        assert len(batches) == 3
        assert len(batches[0]) == 50
        assert len(batches[1]) == 50
        assert len(batches[2]) == 20
        assert len(result) == 120
