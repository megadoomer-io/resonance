"""Per-IP rate limiting on auth + admin paths (security review #141, finding #10).

Blunts brute-force against the login/callback flow and the admin bearer token
(compounds #7's constant-time compare). Fixed-window counters in Redis, keyed by
client IP and path scope.

Two deliberate choices:

- **Client IP comes from ``X-Forwarded-For``** when present. Behind the gateway
  (nginx-gateway-fabric) every request's socket peer is the gateway, so
  ``request.client.host`` would lump all clients together. We take the first hop
  in XFF, falling back to the socket peer for direct/local requests.
- **Fail open.** If Redis is unavailable the request is allowed through rather
  than blocked — a Redis blip must not lock everyone out of authentication.
  Availability wins over the rate-limit hardening here.
"""

from __future__ import annotations

import dataclasses
import typing

import starlette.middleware.base as base_middleware
import starlette.responses as starlette_responses
import structlog

if typing.TYPE_CHECKING:
    import starlette.requests as starlette_requests
    import starlette.types as starlette_types

    from resonance.middleware.session import RedisClient

logger = structlog.get_logger()


@dataclasses.dataclass(frozen=True)
class RateLimitRule:
    """A path prefix and its per-IP budget."""

    prefix: str
    max_requests: int
    window_seconds: int


# Generous enough for real use (a person logging in, an agent driving the admin
# API), tight enough to make credential brute-force impractical.
DEFAULT_RULES: tuple[RateLimitRule, ...] = (
    RateLimitRule("/api/v1/auth/", max_requests=20, window_seconds=60),
    RateLimitRule("/api/v1/admin/", max_requests=60, window_seconds=60),
)


def client_ip(request: starlette_requests.Request) -> str:
    """The originating client IP, honoring X-Forwarded-For behind the gateway."""
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        first = forwarded.split(",", 1)[0].strip()
        if first:
            return first
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(base_middleware.BaseHTTPMiddleware):
    """Fixed-window per-IP rate limiting on the configured path prefixes."""

    def __init__(
        self,
        app: starlette_types.ASGIApp,
        redis: RedisClient,
        rules: tuple[RateLimitRule, ...] = DEFAULT_RULES,
    ) -> None:
        super().__init__(app)
        self.redis = redis
        self.rules = rules

    async def dispatch(
        self,
        request: starlette_requests.Request,
        call_next: base_middleware.RequestResponseEndpoint,
    ) -> starlette_responses.Response:
        rule = next(
            (r for r in self.rules if request.url.path.startswith(r.prefix)),
            None,
        )
        if rule is None:
            return await call_next(request)

        ip = client_ip(request)
        key = f"ratelimit:{rule.prefix}:{ip}"
        try:
            count = await self.redis.incr(key)
            if count == 1:
                await self.redis.expire(key, rule.window_seconds)
        except Exception:
            # Fail open: don't let a Redis problem block authentication.
            logger.warning("rate_limit_redis_error", key=key, exc_info=True)
            return await call_next(request)

        if count > rule.max_requests:
            ttl = await self.redis.ttl(key)
            retry_after = (
                ttl if isinstance(ttl, int) and ttl > 0 else rule.window_seconds
            )
            return starlette_responses.JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Please slow down."},
                headers={"Retry-After": str(retry_after)},
            )

        return await call_next(request)
