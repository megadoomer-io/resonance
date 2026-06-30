"""Tests for event candidate resolution undo (unreject/unaccept) UI routes."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import httpx
import pytest

import resonance.app as app_module
import resonance.config as config_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app(config_module.Settings(debug=True))
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test", follow_redirects=False
    ) as c:
        yield c


class TestUnrejectCandidate:
    """Tests for the unreject candidate UI endpoint."""

    async def test_requires_auth(self, client: httpx.AsyncClient) -> None:
        """Unauthenticated unreject requests should redirect to login."""
        event_id = uuid.uuid4()
        candidate_id = uuid.uuid4()
        response = await client.post(
            f"/events/{event_id}/candidates/{candidate_id}/unreject",
            follow_redirects=False,
        )
        assert response.status_code == 307
        assert response.headers["location"] == "/login"


class TestUnacceptCandidate:
    """Tests for the unaccept candidate UI endpoint."""

    async def test_requires_auth(self, client: httpx.AsyncClient) -> None:
        """Unauthenticated unaccept requests should redirect to login."""
        event_id = uuid.uuid4()
        candidate_id = uuid.uuid4()
        response = await client.post(
            f"/events/{event_id}/candidates/{candidate_id}/unaccept",
            follow_redirects=False,
        )
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
