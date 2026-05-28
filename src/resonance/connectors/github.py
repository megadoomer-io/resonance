"""GitHub/Dex OIDC connector for identity and role assignment."""

import urllib.parse

import structlog

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module

logger = structlog.get_logger()

_SCOPES = "openid profile email groups"

_ADMIN_TEAMS = frozenset(
    {
        "megadoomer-io:admins",
        "megadoomer-io:resonance-admins",
    }
)


class GitHubConnector(base_module.BaseConnector):
    """Identity connector using Dex as OIDC broker with GitHub as upstream IdP."""

    service_type = types_module.ServiceType.GITHUB
    display_name = "GitHub"
    icon = "github"
    color = ""
    capabilities = frozenset(
        {
            base_module.ConnectorCapability.AUTHN,
            base_module.ConnectorCapability.AUTHZ,
        }
    )

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        return base_module.ConnectionConfig(auth_type="oauth")

    def __init__(self, settings: config_module.Settings) -> None:
        self._client_id = settings.dex_client_id
        self._client_secret = settings.dex_client_secret
        self._issuer_url = settings.dex_issuer_url.rstrip("/")
        self._redirect_uri = settings.dex_redirect_uri
        self._last_userinfo: dict[str, object] | None = None
        self._http_client = None
        self._budget = ratelimit_module.RateLimitBudget(
            default_interval=1.0,
            window_seconds=30,
            window_ceiling=20,
        )

    def get_auth_url(self, state: str) -> str:
        params = urllib.parse.urlencode(
            {
                "client_id": self._client_id,
                "response_type": "code",
                "redirect_uri": self._redirect_uri,
                "scope": _SCOPES,
                "state": state,
            }
        )
        return f"{self._issuer_url}/auth?{params}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        logger.info("Exchanging Dex authorization code for tokens")
        response = await self._request(
            "POST",
            f"{self._issuer_url}/token",
            high_priority=True,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._redirect_uri,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        logger.info("Dex token exchange successful")
        return base_module.TokenResponse.model_validate(response.json())

    async def _fetch_userinfo(self, access_token: str) -> dict[str, object]:
        response = await self._request(
            "GET",
            f"{self._issuer_url}/userinfo",
            high_priority=True,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        return response.json()  # type: ignore[no-any-return]

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        logger.info("Fetching user profile from Dex userinfo")
        data = await self._fetch_userinfo(access_token)
        self._last_userinfo = data
        return {
            "id": str(data.get("preferred_username") or data.get("sub", "")),
            "display_name": str(data.get("name") or data.get("preferred_username", "")),
        }

    async def get_role(self, access_token: str) -> types_module.UserRole | None:
        """Derive user role from Dex groups claim (GitHub team membership).

        Uses cached userinfo from get_current_user() if available,
        otherwise fetches fresh.
        """
        try:
            data = getattr(self, "_last_userinfo", None)
            if data is None:
                data = await self._fetch_userinfo(access_token)
            groups = data.get("groups", [])

            if not isinstance(groups, list):
                logger.warning(
                    "Unexpected groups claim format",
                    groups_type=type(groups).__name__,
                )
                return types_module.UserRole.USER

            if _ADMIN_TEAMS & set(groups):
                return types_module.UserRole.ADMIN

            return types_module.UserRole.USER
        except Exception:
            logger.exception("Failed to parse groups claim from Dex")
            return None
        finally:
            self._last_userinfo = None
