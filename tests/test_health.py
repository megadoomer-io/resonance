import httpx
import pytest

import resonance.app as app_module


@pytest.fixture
def client() -> httpx.AsyncClient:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_healthz_returns_ok(client: httpx.AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
