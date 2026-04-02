"""Base connector framework with capability declarations."""

import abc
import asyncio
import enum
from typing import Any

import httpx
import pydantic
import structlog

import resonance.connectors.ratelimit as ratelimit_module  # noqa: TC001 — used at runtime in _request
import resonance.types as types_module  # noqa: TC001 — Pydantic models need this at runtime

logger = structlog.get_logger()


class ConnectorCapability(enum.StrEnum):
    """Capabilities that a connector can declare support for."""

    AUTHENTICATION = "authentication"
    LISTENING_HISTORY = "listening_history"
    RECOMMENDATIONS = "recommendations"
    PLAYLIST_WRITE = "playlist_write"
    ARTIST_DATA = "artist_data"
    EVENTS = "events"
    FOLLOWS = "follows"
    TRACK_RATINGS = "track_ratings"
    NEW_RELEASES = "new_releases"


class TokenResponse(pydantic.BaseModel):
    """OAuth token response from an external service."""

    access_token: str
    refresh_token: str | None = None
    expires_in: int | None = None
    scope: str | None = None


class ArtistData(pydantic.BaseModel):
    """Artist data returned from a connector."""

    external_id: str
    name: str
    service: types_module.ServiceType


class TrackData(pydantic.BaseModel):
    """Track data returned from a connector."""

    external_id: str
    title: str
    artist_external_id: str
    artist_name: str
    service: types_module.ServiceType


class BaseConnector(abc.ABC):
    """Abstract base class for all service connectors."""

    service_type: types_module.ServiceType
    capabilities: frozenset[ConnectorCapability]

    # Subclasses must set these
    _http_client: httpx.AsyncClient | None
    _budget: ratelimit_module.RateLimitBudget

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazily create and return the HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=60.0)
        return self._http_client

    def has_capability(self, capability: ConnectorCapability) -> bool:
        """Check whether this connector supports a given capability."""
        return capability in self.capabilities

    async def _request(
        self,
        method: str,
        url: str,
        *,
        high_priority: bool = False,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make an HTTP request with rate limit pacing and 429 backoff.

        Respects Retry-After headers and backs off accordingly. Will keep
        retrying 429 responses indefinitely — the worker context has no
        user-facing timeout, so we always wait and resume rather than fail.

        Non-429 errors are raised immediately.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: Request URL.
            high_priority: If True, skip pacing when budget is available.
            **kwargs: Additional arguments passed to httpx request.

        Returns:
            The HTTP response.

        Raises:
            httpx.HTTPStatusError: On non-429 HTTP errors.
        """
        while True:
            interval = self._budget.paced_interval(high_priority=high_priority)
            if interval > 0:
                if interval > 5:
                    logger.info(
                        "rate_limit_backoff",
                        wait_seconds=round(interval, 1),
                        method=method,
                        url=url,
                    )
                else:
                    logger.debug(
                        "Pacing: waiting %.1fs before %s %s", interval, method, url
                    )
                await asyncio.sleep(interval)

            response = await self.http_client.request(method, url, **kwargs)
            self._budget.update_from_headers(dict(response.headers))

            if response.status_code != 429:
                response.raise_for_status()
                return response

            # On 429, the budget has been updated with Retry-After.
            # Log and loop — paced_interval will return the wait time.
            retry_after = response.headers.get("Retry-After", "unknown")
            logger.warning(
                "rate_limited",
                method=method,
                url=url,
                retry_after=retry_after,
            )
