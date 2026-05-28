"""Tests for the GitHub/Dex OIDC connector."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.github as github_module
import resonance.types as types_module


def _make_settings() -> config_module.Settings:
    return config_module.Settings(
        dex_client_id="test-dex-client",
        dex_client_secret="test-dex-secret",
        dex_issuer_url="https://auth.megadoomer.io",
        base_url="https://resonance.megadoomer.io",
    )


def _make_connector() -> github_module.GitHubConnector:
    return github_module.GitHubConnector(settings=_make_settings())


class TestGitHubConnectorMetadata:
    def test_service_type(self) -> None:
        c = _make_connector()
        assert c.service_type == types_module.ServiceType.GITHUB

    def test_display_name(self) -> None:
        c = _make_connector()
        assert c.display_name == "GitHub"

    def test_capabilities(self) -> None:
        c = _make_connector()
        assert c.has_capability(base_module.ConnectorCapability.AUTHN)
        assert c.has_capability(base_module.ConnectorCapability.AUTHZ)
        assert not c.has_capability(base_module.ConnectorCapability.LISTENING_HISTORY)

    def test_connection_config_is_oauth_no_sync(self) -> None:
        config = github_module.GitHubConnector.connection_config()
        assert config.auth_type == "oauth"
        assert config.sync_function is None
        assert config.sync_style is None


class TestGetAuthUrl:
    def test_builds_dex_authorize_url(self) -> None:
        c = _make_connector()
        url = c.get_auth_url(state="test-state-123")
        assert url.startswith("https://auth.megadoomer.io/auth?")
        assert "client_id=test-dex-client" in url
        assert "state=test-state-123" in url
        assert "scope=openid+profile+email+groups" in url
        assert "response_type=code" in url

    def test_redirect_uri_in_auth_url(self) -> None:
        c = _make_connector()
        url = c.get_auth_url(state="s")
        assert "redirect_uri=" in url
        assert "resonance.megadoomer.io" in url


class TestExchangeCode:
    @pytest.mark.anyio
    async def test_exchange_posts_to_dex_token_endpoint(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "dex-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        with patch.object(c, "_request", return_value=mock_response) as req:
            result = await c.exchange_code("auth-code-123")
            req.assert_called_once()
            call_args = req.call_args
            assert call_args[0][0] == "POST"
            assert "/token" in call_args[0][1]

        assert result.access_token == "dex-access-token"
        assert result.expires_in == 3600


class TestGetCurrentUser:
    @pytest.mark.anyio
    async def test_returns_sub_and_name(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "sub": "dex-user-abc",
            "name": "Mike D",
            "preferred_username": "miked",
            "email": "mike@example.com",
        }

        with patch.object(c, "_request", return_value=mock_response):
            result = await c.get_current_user("access-token")

        assert result["id"] == "dex-user-abc"
        assert result["display_name"] == "Mike D"

    @pytest.mark.anyio
    async def test_falls_back_to_preferred_username(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "sub": "dex-user-abc",
            "preferred_username": "miked",
        }

        with patch.object(c, "_request", return_value=mock_response):
            result = await c.get_current_user("access-token")

        assert result["display_name"] == "miked"


class TestGetRole:
    @pytest.mark.anyio
    async def test_admins_team_returns_admin(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "groups": ["megadoomer-io:admins", "megadoomer-io:other-team"],
        }

        with patch.object(c, "_request", return_value=mock_response):
            role = await c.get_role("access-token")

        assert role == types_module.UserRole.ADMIN

    @pytest.mark.anyio
    async def test_resonance_admins_team_returns_admin(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "groups": ["megadoomer-io:resonance-admins"],
        }

        with patch.object(c, "_request", return_value=mock_response):
            role = await c.get_role("access-token")

        assert role == types_module.UserRole.ADMIN

    @pytest.mark.anyio
    async def test_no_admin_team_returns_user(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "groups": ["megadoomer-io:some-other-team"],
        }

        with patch.object(c, "_request", return_value=mock_response):
            role = await c.get_role("access-token")

        assert role == types_module.UserRole.USER

    @pytest.mark.anyio
    async def test_empty_groups_returns_user(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {"groups": []}

        with patch.object(c, "_request", return_value=mock_response):
            role = await c.get_role("access-token")

        assert role == types_module.UserRole.USER

    @pytest.mark.anyio
    async def test_missing_groups_returns_user(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {}

        with patch.object(c, "_request", return_value=mock_response):
            role = await c.get_role("access-token")

        assert role == types_module.UserRole.USER

    @pytest.mark.anyio
    async def test_non_list_groups_returns_user(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {"groups": "not-a-list"}

        with patch.object(c, "_request", return_value=mock_response):
            role = await c.get_role("access-token")

        assert role == types_module.UserRole.USER

    @pytest.mark.anyio
    async def test_request_failure_returns_none(self) -> None:
        c = _make_connector()
        with patch.object(c, "_request", side_effect=httpx.ConnectError("down")):
            role = await c.get_role("access-token")

        assert role is None

    @pytest.mark.anyio
    async def test_both_admin_teams_still_returns_admin(self) -> None:
        c = _make_connector()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "groups": [
                "megadoomer-io:admins",
                "megadoomer-io:resonance-admins",
            ],
        }

        with patch.object(c, "_request", return_value=mock_response):
            role = await c.get_role("access-token")

        assert role == types_module.UserRole.ADMIN
