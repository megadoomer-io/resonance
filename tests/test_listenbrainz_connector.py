"""Tests for the ListenBrainz connector."""

import json
import urllib.parse

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module


def _make_settings() -> config_module.Settings:
    """Create test settings with MusicBrainz credentials."""
    return config_module.Settings(
        musicbrainz_client_id="test-mb-client-id",
        musicbrainz_client_secret="test-mb-client-secret",
        musicbrainz_redirect_path="/callback",
    )


class TestListenBrainzConnectorProperties:
    """Tests for ListenBrainzConnector service_type and capabilities."""

    def test_service_type(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        assert connector.service_type == types_module.ServiceType.LISTENBRAINZ

    def test_capabilities(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        expected = frozenset(
            {
                base_module.ConnectorCapability.AUTHN,
                base_module.ConnectorCapability.LISTENING_HISTORY,
                base_module.ConnectorCapability.TRACK_DISCOVERY,
                base_module.ConnectorCapability.SIMILAR_ARTISTS,
            }
        )
        assert connector.capabilities == expected

    def test_is_base_connector(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        assert isinstance(connector, base_module.BaseConnector)


class TestGetAuthUrlMusicBrainz:
    """Tests for get_auth_url with MusicBrainz OAuth."""

    def test_contains_musicbrainz_domain(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="test-state")
        assert "musicbrainz.org" in url

    def test_contains_client_id(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="test-state")
        assert "client_id=test-mb-client-id" in url

    def test_contains_state(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="my-state")
        assert "state=my-state" in url

    def test_contains_response_type_code(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert "response_type=code" in url

    def test_contains_profile_scope(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert params["scope"] == ["profile"]

    def test_contains_offline_access_type(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert "access_type=offline" in url

    def test_starts_with_musicbrainz_auth_url(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert url.startswith(listenbrainz_module.MUSICBRAINZ_AUTH_URL)


class TestExchangeCode:
    """Tests for exchange_code."""

    @pytest.mark.anyio()
    async def test_returns_token_response(self) -> None:
        token_data = {
            "access_token": "access-abc",
            "refresh_token": "refresh-xyz",
            "expires_in": 3600,
            "scope": "profile",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/oauth2/token"
            body = request.content.decode()
            assert "grant_type=authorization_code" in body
            assert "code=auth-code-123" in body
            return httpx.Response(200, json=token_data)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.exchange_code(code="auth-code-123")

        assert isinstance(result, base_module.TokenResponse)
        assert result.access_token == "access-abc"
        assert result.refresh_token == "refresh-xyz"
        assert result.expires_in == 3600


class TestGetCurrentUser:
    """Tests for get_current_user."""

    @pytest.mark.anyio()
    async def test_returns_user_dict(self) -> None:
        userinfo_data = {
            "sub": "cooluser42",
            "metabrainz_user_id": 12345,
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/oauth2/userinfo"
            assert request.headers["Authorization"] == "Bearer my-token"
            return httpx.Response(200, json=userinfo_data)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_current_user(access_token="my-token")

        assert result == {"id": "cooluser42", "display_name": "cooluser42"}


class TestGetListens:
    """Tests for get_listens."""

    @pytest.mark.anyio()
    async def test_parses_listens_response(self) -> None:
        api_response = {
            "payload": {
                "listens": [
                    {
                        "listened_at": 1700000000,
                        "track_metadata": {
                            "track_name": "Song One",
                            "artist_name": "Artist One",
                            "additional_info": {
                                "recording_mbid": "rec-mbid-1",
                                "artist_mbids": ["art-mbid-1"],
                            },
                        },
                    },
                    {
                        "listened_at": 1700001000,
                        "track_metadata": {
                            "track_name": "Song Two",
                            "artist_name": "Artist Two",
                            "additional_info": {
                                "recording_mbid": "rec-mbid-2",
                                "artist_mbids": ["art-mbid-2a", "art-mbid-2b"],
                            },
                        },
                    },
                ]
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert "/user/cooluser42/listens" in str(request.url)
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_listens(username="cooluser42")

        assert len(result) == 2
        assert isinstance(result[0], listenbrainz_module.ListenBrainzListenItem)

        assert result[0].listened_at == 1700000000
        assert result[0].track.title == "Song One"
        assert result[0].track.artist_name == "Artist One"
        assert result[0].track.external_id == "rec-mbid-1"
        assert result[0].track.artist_external_id == "art-mbid-1"
        assert result[0].track.service == types_module.ServiceType.LISTENBRAINZ

        assert result[1].listened_at == 1700001000
        assert result[1].track.title == "Song Two"
        assert result[1].track.artist_name == "Artist Two"
        assert result[1].track.external_id == "rec-mbid-2"
        # Uses first artist MBID
        assert result[1].track.artist_external_id == "art-mbid-2a"

    @pytest.mark.anyio()
    async def test_handles_missing_mbids(self) -> None:
        """Listens without recording_mbid or artist_mbids use empty strings."""
        api_response = {
            "payload": {
                "listens": [
                    {
                        "listened_at": 1700002000,
                        "track_metadata": {
                            "track_name": "Unknown Track",
                            "artist_name": "Unknown Artist",
                            "additional_info": {},
                        },
                    },
                ]
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_listens(username="user1")

        assert len(result) == 1
        assert result[0].track.external_id == ""
        assert result[0].track.artist_external_id == ""

    @pytest.mark.anyio()
    async def test_passes_query_params(self) -> None:
        """Verify max_ts, min_ts, and count are forwarded as query params."""
        api_response = {"payload": {"listens": []}}

        def handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            assert "max_ts=1700000000" in url_str
            assert "min_ts=1699000000" in url_str
            assert "count=50" in url_str
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_listens(
            username="user1", max_ts=1700000000, min_ts=1699000000, count=50
        )

        assert result == []


class TestDiscoverTracks:
    """Tests for discover_tracks via MusicBrainz recordings API."""

    @pytest.mark.anyio()
    async def test_discover_by_mbid(self) -> None:
        """When service_links has a listenbrainz MBID, uses it directly."""
        recordings_response = {
            "recordings": [
                {"id": "rec-1", "title": "Track One", "length": 240000},
                {"id": "rec-2", "title": "Track Two", "length": 180000},
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert "/recording/" in str(request.url)
            assert "artist=artist-mbid-123" in str(request.url)
            return httpx.Response(200, json=recordings_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.discover_tracks(
            artist_name="Test Artist",
            service_links={"listenbrainz": "artist-mbid-123"},
        )

        assert len(result) == 2
        assert isinstance(result[0], base_module.DiscoveredTrack)
        assert result[0].external_id == "rec-1"
        assert result[0].title == "Track One"
        assert result[0].artist_name == "Test Artist"
        assert result[0].artist_external_id == "artist-mbid-123"
        assert result[0].service == types_module.ServiceType.LISTENBRAINZ
        assert result[0].duration_ms == 240000
        assert result[1].external_id == "rec-2"
        assert result[1].title == "Track Two"
        assert result[1].duration_ms == 180000

    @pytest.mark.anyio()
    async def test_discover_by_name_search(self) -> None:
        """Falls back to MusicBrainz name search when no MBID."""
        artist_search_response = {
            "artists": [{"id": "found-mbid-456", "name": "Searched Artist"}]
        }
        recordings_response = {
            "recordings": [
                {"id": "rec-a", "title": "Found Track"},
            ]
        }
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            url_str = str(request.url)
            if "/artist/" in url_str:
                assert (
                    "query=Searched+Artist" in url_str
                    or "query=Searched%20Artist" in url_str
                )
                return httpx.Response(200, json=artist_search_response)
            assert "/recording/" in url_str
            assert "artist=found-mbid-456" in url_str
            return httpx.Response(200, json=recordings_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.discover_tracks(
            artist_name="Searched Artist",
            service_links=None,
        )

        assert call_count == 2
        assert len(result) == 1
        assert result[0].external_id == "rec-a"
        assert result[0].title == "Found Track"
        assert result[0].artist_external_id == "found-mbid-456"

    @pytest.mark.anyio()
    async def test_discover_returns_empty_on_no_match(self) -> None:
        """Returns empty list when artist not found in MusicBrainz."""
        artist_search_response: dict[str, list[object]] = {"artists": []}

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=artist_search_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.discover_tracks(
            artist_name="Nonexistent Artist",
            service_links=None,
        )

        assert result == []

    @pytest.mark.anyio()
    async def test_popularity_score_decreases_by_position(self) -> None:
        """First recording gets score 100, decreasing by 5 per position."""
        recordings_response = {
            "recordings": [
                {"id": "rec-1", "title": "First"},
                {"id": "rec-2", "title": "Second"},
                {"id": "rec-3", "title": "Third"},
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=recordings_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.discover_tracks(
            artist_name="Artist",
            service_links={"listenbrainz": "mbid-xyz"},
        )

        assert len(result) == 3
        assert result[0].popularity_score == 100
        assert result[1].popularity_score == 95
        assert result[2].popularity_score == 90


class TestGetRecordingPopularity:
    """Tests for get_recording_popularity (POST /1/popularity/recording)."""

    def _connector_with_handler(
        self, handler: object
    ) -> listenbrainz_module.ListenBrainzConnector:
        transport = httpx.MockTransport(handler)  # type: ignore[arg-type]
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
        return connector

    @pytest.mark.anyio()
    async def test_posts_mbids_and_returns_listen_counts(self) -> None:
        api_response = [
            {
                "recording_mbid": "rec-a",
                "total_listen_count": 1000,
                "total_user_count": 10,
            },
            {
                "recording_mbid": "rec-b",
                "total_listen_count": 25,
                "total_user_count": 3,
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.method == "POST"
            assert "api.listenbrainz.org/1/popularity/recording" in str(request.url)
            # Public endpoint — no Authorization header threaded through.
            assert "Authorization" not in request.headers
            body = json.loads(request.content)
            assert body["recording_mbids"] == ["rec-a", "rec-b"]
            return httpx.Response(200, json=api_response)

        connector = self._connector_with_handler(handler)
        result = await connector.get_recording_popularity(["rec-a", "rec-b"])

        assert result == {"rec-a": 1000, "rec-b": 25}

    @pytest.mark.anyio()
    async def test_skips_null_listen_counts(self) -> None:
        api_response = [
            {
                "recording_mbid": "rec-a",
                "total_listen_count": 500,
                "total_user_count": 8,
            },
            {
                "recording_mbid": "rec-b",
                "total_listen_count": None,
                "total_user_count": None,
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        connector = self._connector_with_handler(handler)
        result = await connector.get_recording_popularity(["rec-a", "rec-b"])

        assert result == {"rec-a": 500}

    @pytest.mark.anyio()
    async def test_empty_input_returns_empty_without_request(self) -> None:
        called = False

        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            nonlocal called
            called = True
            return httpx.Response(200, json=[])

        connector = self._connector_with_handler(handler)
        result = await connector.get_recording_popularity([])

        assert result == {}
        assert called is False

    @pytest.mark.anyio()
    async def test_batches_over_limit(self) -> None:
        mbids = [f"rec-{i:03d}" for i in range(120)]
        batches: list[list[str]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            posted = body["recording_mbids"]
            batches.append(posted)
            return httpx.Response(
                200,
                json=[
                    {
                        "recording_mbid": m,
                        "total_listen_count": 1,
                        "total_user_count": 1,
                    }
                    for m in posted
                ],
            )

        connector = self._connector_with_handler(handler)
        result = await connector.get_recording_popularity(mbids)

        # Batched at MAX_RECORDING_POPULARITY_BATCH (50) -> 50 + 50 + 20.
        assert [len(b) for b in batches] == [50, 50, 20]
        assert len(result) == 120


class TestGetSimilarArtists:
    """Tests for get_similar_artists via the ListenBrainz labs endpoint."""

    def _connector_with_handler(
        self, handler: object
    ) -> listenbrainz_module.ListenBrainzConnector:
        transport = httpx.MockTransport(handler)  # type: ignore[arg-type]
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
        return connector

    @pytest.mark.anyio()
    async def test_parses_and_normalizes_scores(self) -> None:
        labs_response = [
            {"artist_mbid": "mbid-nirvana", "name": "Nirvana", "score": 10000},
            {"artist_mbid": "mbid-muse", "name": "Muse", "score": 5000},
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            assert "labs.api.listenbrainz.org/similar-artists/json" in str(request.url)
            assert "artist_mbids=mbid-radiohead" in str(request.url)
            return httpx.Response(200, json=labs_response)

        connector = self._connector_with_handler(handler)

        result = await connector.get_similar_artists("Radiohead", mbid="mbid-radiohead")

        assert result == [
            {"name": "Nirvana", "mbid": "mbid-nirvana", "match": 1.0},
            {"name": "Muse", "mbid": "mbid-muse", "match": 0.5},
        ]

    @pytest.mark.anyio()
    async def test_requires_mbid(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            raise AssertionError("endpoint should not be called without an mbid")

        connector = self._connector_with_handler(handler)

        result = await connector.get_similar_artists("Radiohead")

        assert result == []

    @pytest.mark.anyio()
    async def test_truncates_to_limit(self) -> None:
        labs_response = [
            {"artist_mbid": f"mbid-{i}", "name": f"Artist {i}", "score": 100 - i}
            for i in range(10)
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=labs_response)

        connector = self._connector_with_handler(handler)

        result = await connector.get_similar_artists(
            "Radiohead", mbid="mbid-radiohead", limit=3
        )

        assert len(result) == 3
        assert [r["name"] for r in result] == ["Artist 0", "Artist 1", "Artist 2"]

    @pytest.mark.anyio()
    async def test_returns_empty_on_http_error(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        connector = self._connector_with_handler(handler)

        result = await connector.get_similar_artists("Radiohead", mbid="mbid-radiohead")

        assert result == []
