"""Tests for MusicBrainz artist search methods on ListenBrainzConnector."""

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.ratelimit as ratelimit_module


def _make_connector() -> listenbrainz_module.ListenBrainzConnector:
    """Create a ListenBrainzConnector with test settings and no rate limiting."""
    settings = config_module.Settings(
        musicbrainz_client_id="test-mb-client-id",
        musicbrainz_client_secret="test-mb-client-secret",
        musicbrainz_redirect_path="/callback",
    )
    connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
    connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    return connector


class TestSearchArtists:
    """Tests for search_artists MusicBrainz name search."""

    @pytest.mark.anyio()
    async def test_returns_parsed_artist_list(self) -> None:
        """Returns list of dicts with expected keys from MB response."""
        mb_response = {
            "artists": [
                {
                    "id": "mbid-aaa",
                    "name": "Radiohead",
                    "disambiguation": "UK rock band",
                    "type": "Group",
                    "area": {"name": "Oxford"},
                    "life-span": {"begin": "1985", "end": ""},
                },
                {
                    "id": "mbid-bbb",
                    "name": "Radiohead Tribute",
                    "disambiguation": "",
                    "type": "Group",
                    "area": {"name": "London"},
                    "life-span": {"begin": "2010-03-15", "end": "2015"},
                },
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            assert "/artist/" in url_str
            assert "query=Radiohead" in url_str
            assert "fmt=json" in url_str
            assert "limit=10" in url_str
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.search_artists("Radiohead")

        assert len(result) == 2
        assert result[0] == {
            "mbid": "mbid-aaa",
            "name": "Radiohead",
            "disambiguation": "UK rock band",
            "artist_type": "Group",
            "area": "Oxford",
            "begin_year": 1985,
            "end_year": None,
        }
        assert result[1] == {
            "mbid": "mbid-bbb",
            "name": "Radiohead Tribute",
            "disambiguation": "",
            "artist_type": "Group",
            "area": "London",
            "begin_year": 2010,
            "end_year": 2015,
        }

    @pytest.mark.anyio()
    async def test_handles_partial_date_strings(self) -> None:
        """Extracts year from partial date strings like '1996-06'."""
        mb_response = {
            "artists": [
                {
                    "id": "mbid-ccc",
                    "name": "Partial Date Band",
                    "life-span": {"begin": "1996-06", "end": "2020-11-30"},
                },
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.search_artists("Partial")

        assert result[0]["begin_year"] == 1996
        assert result[0]["end_year"] == 2020

    @pytest.mark.anyio()
    async def test_handles_missing_optional_fields(self) -> None:
        """Missing disambiguation, type, area return empty strings."""
        mb_response = {
            "artists": [
                {
                    "id": "mbid-ddd",
                    "name": "Minimal Artist",
                    "life-span": {},
                },
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.search_artists("Minimal")

        assert len(result) == 1
        assert result[0]["disambiguation"] == ""
        assert result[0]["artist_type"] == ""
        assert result[0]["area"] == ""
        assert result[0]["begin_year"] is None
        assert result[0]["end_year"] is None

    @pytest.mark.anyio()
    async def test_empty_results_return_empty_list(self) -> None:
        """Empty artist list from MusicBrainz returns empty list."""
        mb_response: dict[str, list[object]] = {"artists": []}

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.search_artists("Nonexistent")

        assert result == []

    @pytest.mark.anyio()
    async def test_custom_limit_passed_to_api(self) -> None:
        """Custom limit parameter is forwarded to the MusicBrainz API."""
        mb_response: dict[str, list[object]] = {"artists": []}

        def handler(request: httpx.Request) -> httpx.Response:
            assert "limit=5" in str(request.url)
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        await connector.search_artists("Test", limit=5)

    @pytest.mark.anyio()
    async def test_no_life_span_key(self) -> None:
        """Artist without a life-span key at all still parses correctly."""
        mb_response = {
            "artists": [
                {
                    "id": "mbid-eee",
                    "name": "No Lifespan",
                },
            ]
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.search_artists("No Lifespan")

        assert result[0]["begin_year"] is None
        assert result[0]["end_year"] is None


class TestGetArtistByMbid:
    """Tests for get_artist_by_mbid single-artist lookup."""

    @pytest.mark.anyio()
    async def test_returns_single_artist_dict(self) -> None:
        """Fetches and parses a single artist by MBID."""
        mb_response = {
            "id": "mbid-fff",
            "name": "Tool",
            "disambiguation": "US rock band",
            "type": "Group",
            "area": {"name": "Los Angeles"},
            "life-span": {"begin": "1990", "end": ""},
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert "/artist/mbid-fff" in str(request.url)
            assert "fmt=json" in str(request.url)
            return httpx.Response(200, json=mb_response)

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_artist_by_mbid("mbid-fff")

        assert result is not None
        assert result == {
            "mbid": "mbid-fff",
            "name": "Tool",
            "disambiguation": "US rock band",
            "artist_type": "Group",
            "area": "Los Angeles",
            "begin_year": 1990,
            "end_year": None,
        }

    @pytest.mark.anyio()
    async def test_returns_none_on_404(self) -> None:
        """Returns None when MusicBrainz returns 404 for unknown MBID."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={"error": "Not Found"})

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_artist_by_mbid("nonexistent-mbid")

        assert result is None

    @pytest.mark.anyio()
    async def test_returns_none_on_400(self) -> None:
        """Returns None for other client errors like 400 Bad Request."""

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(400, json={"error": "Bad Request"})

        connector = _make_connector()
        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler)
        )

        result = await connector.get_artist_by_mbid("invalid-mbid-format")

        assert result is None


class TestParseMbArtist:
    """Tests for the _parse_mb_artist static helper."""

    def test_full_data(self) -> None:
        data = {
            "id": "abc-123",
            "name": "The Beatles",
            "disambiguation": "Liverpool band",
            "type": "Group",
            "area": {"name": "Liverpool"},
            "life-span": {"begin": "1960", "end": "1970"},
        }
        result = listenbrainz_module.ListenBrainzConnector._parse_mb_artist(data)
        assert result == {
            "mbid": "abc-123",
            "name": "The Beatles",
            "disambiguation": "Liverpool band",
            "artist_type": "Group",
            "area": "Liverpool",
            "begin_year": 1960,
            "end_year": 1970,
        }

    def test_minimal_data(self) -> None:
        data = {"id": "xyz-789", "name": "Solo"}
        result = listenbrainz_module.ListenBrainzConnector._parse_mb_artist(data)
        assert result == {
            "mbid": "xyz-789",
            "name": "Solo",
            "disambiguation": "",
            "artist_type": "",
            "area": "",
            "begin_year": None,
            "end_year": None,
        }
