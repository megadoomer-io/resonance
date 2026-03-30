from collections.abc import AsyncIterator

import httpx
import pytest

import resonance.app as app_module


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_healthz_returns_ok(client: httpx.AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
