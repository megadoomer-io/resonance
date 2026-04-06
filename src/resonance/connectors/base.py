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


class RateLimitExceededError(Exception):
    """Raised when a server's Retry-After exceeds the maximum acceptable wait."""

    def __init__(self, retry_after: float, max_wait: float) -> None:
        self.retry_after = retry_after
        self.max_wait = max_wait
        super().__init__(
            f"Rate limit Retry-After ({retry_after:.0f}s) exceeds "
            f"maximum wait ({max_wait:.0f}s)"
        )


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

    # Transient errors that should be retried with exponential backoff.
    _TRANSIENT_ERRORS: tuple[type[Exception], ...] = (
        httpx.ReadTimeout,
        httpx.RemoteProtocolError,
        httpx.ConnectError,
    )
    _MAX_TRANSIENT_RETRIES = 5
    _TRANSIENT_BACKOFF_BASE = 2.0  # seconds — doubles each retry
    _MAX_RATE_LIMIT_WAIT = 120.0  # seconds — fail instead of sleeping longer

    async def _request(
        self,
        method: str,
        url: str,
        *,
        high_priority: bool = False,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make an HTTP request with rate limit pacing and automatic retry.

        Handles two retry scenarios:
        - **429 rate limits**: Respects Retry-After headers, retries
          indefinitely until the request succeeds.
        - **Transient errors** (timeouts, disconnects, connection errors):
          Retries with exponential backoff up to _MAX_TRANSIENT_RETRIES.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: Request URL.
            high_priority: If True, skip pacing when budget is available.
            **kwargs: Additional arguments passed to httpx request.

        Returns:
            The HTTP response.

        Raises:
            httpx.HTTPStatusError: On non-429/non-transient HTTP errors.
            httpx.ReadTimeout: After exhausting transient retries.
            httpx.RemoteProtocolError: After exhausting transient retries.
            httpx.ConnectError: After exhausting transient retries.
        """
        transient_attempt = 0

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

            # Check rolling window budget (safety net)
            budget_wait = self._budget.check_window_budget()
            if budget_wait > 0:
                logger.warning(
                    "request_budget_throttled",
                    window_used=self._budget.window_used,
                    window_ceiling=self._budget.window_ceiling,
                    wait_seconds=round(budget_wait, 1),
                )
                await asyncio.sleep(budget_wait)

            try:
                response = await self.http_client.request(method, url, **kwargs)
            except self._TRANSIENT_ERRORS as exc:
                transient_attempt += 1
                if transient_attempt > self._MAX_TRANSIENT_RETRIES:
                    logger.error(
                        "transient_retries_exhausted",
                        method=method,
                        url=url,
                        attempts=transient_attempt,
                        error=str(exc),
                    )
                    raise
                backoff = self._TRANSIENT_BACKOFF_BASE**transient_attempt
                logger.warning(
                    "transient_error_retry",
                    method=method,
                    url=url,
                    attempt=transient_attempt,
                    max_attempts=self._MAX_TRANSIENT_RETRIES,
                    backoff_seconds=round(backoff, 1),
                    error=type(exc).__name__,
                )
                await asyncio.sleep(backoff)
                continue

            # Reset transient counter on successful response (even if 429)
            transient_attempt = 0
            self._budget.update_from_headers(dict(response.headers))
            self._budget.record_request()
            window_used = self._budget.window_used
            if window_used is not None:
                logger.info(
                    "request_budget_status",
                    window_used=window_used,
                    window_ceiling=self._budget.window_ceiling,
                    window_seconds=self._budget.window_seconds,
                )

            if response.status_code != 429:
                response.raise_for_status()
                return response

            # On 429, check Retry-After and either wait or fail fast.
            retry_after_raw = response.headers.get("Retry-After")
            retry_after = float(retry_after_raw) if retry_after_raw else 30.0
            logger.warning(
                "rate_limited",
                method=method,
                url=url,
                retry_after_seconds=round(retry_after, 1),
            )
            if retry_after > self._MAX_RATE_LIMIT_WAIT:
                raise RateLimitExceededError(retry_after, self._MAX_RATE_LIMIT_WAIT)
            await asyncio.sleep(retry_after)
