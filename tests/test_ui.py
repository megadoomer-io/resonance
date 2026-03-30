from typing import TYPE_CHECKING

import httpx
import pytest

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

import resonance.app as app_module


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", follow_redirects=False
    ) as c:
        yield c


async def test_login_page_returns_html(client: httpx.AsyncClient) -> None:
    response = await client.get("/login")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


async def test_login_page_contains_spotify_link(client: httpx.AsyncClient) -> None:
    response = await client.get("/login")
    assert "/api/v1/auth/spotify" in response.text


async def test_unauthenticated_root_redirects_to_login(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/")
    assert response.status_code == 307
    assert response.headers["location"] == "/login"


class TestDashboard:
    """Tests for the dashboard page."""

    async def test_dashboard_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"


class TestArtistsPage:
    """Tests for the artists page."""

    async def test_artists_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/artists", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
