"""Tests for per-IP rate limiting (security review #141, finding #10)."""

from __future__ import annotations

from typing import Any

import fastapi
import httpx

import resonance.middleware.rate_limit as rate_limit_module


class FakeRedis:
    """Minimal in-memory Redis stand-in for incr/expire/ttl."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.ttls: dict[str, int] = {}
        self.fail = False

    async def incr(self, key: str) -> int:
        if self.fail:
            raise RuntimeError("redis down")
        self.counts[key] = self.counts.get(key, 0) + 1
        return self.counts[key]

    async def expire(self, key: str, seconds: int) -> None:
        self.ttls[key] = seconds

    async def ttl(self, key: str) -> int:
        return self.ttls.get(key, -1)


def _app(redis: FakeRedis, rules: tuple[rate_limit_module.RateLimitRule, ...]) -> Any:
    app = fastapi.FastAPI()
    app.add_middleware(rate_limit_module.RateLimitMiddleware, redis=redis, rules=rules)

    @app.get("/api/v1/auth/ping")
    async def auth_ping() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    return app


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


_RULES = (rate_limit_module.RateLimitRule("/api/v1/auth/", 3, 60),)


class TestRateLimitMiddleware:
    async def test_allows_under_limit_then_429s_over(self) -> None:
        redis = FakeRedis()
        async with _client(_app(redis, _RULES)) as c:
            for _ in range(3):
                assert (await c.get("/api/v1/auth/ping")).status_code == 200
            blocked = await c.get("/api/v1/auth/ping")
        assert blocked.status_code == 429
        assert blocked.headers["Retry-After"] == "60"

    async def test_non_matching_path_never_limited(self) -> None:
        redis = FakeRedis()
        async with _client(_app(redis, _RULES)) as c:
            for _ in range(10):
                assert (await c.get("/health")).status_code == 200
        # No counter was created for the unmatched path.
        assert redis.counts == {}

    async def test_per_ip_isolation_via_xff(self) -> None:
        """Different X-Forwarded-For clients get independent budgets."""
        redis = FakeRedis()
        async with _client(_app(redis, _RULES)) as c:
            for _ in range(3):
                await c.get("/api/v1/auth/ping", headers={"X-Forwarded-For": "1.1.1.1"})
            # A different client is unaffected by the first's exhausted budget.
            other = await c.get(
                "/api/v1/auth/ping", headers={"X-Forwarded-For": "2.2.2.2"}
            )
            same = await c.get(
                "/api/v1/auth/ping", headers={"X-Forwarded-For": "1.1.1.1"}
            )
        assert other.status_code == 200
        assert same.status_code == 429

    async def test_fails_open_when_redis_errors(self) -> None:
        """A Redis failure must not block auth — requests pass through."""
        redis = FakeRedis()
        redis.fail = True
        async with _client(_app(redis, _RULES)) as c:
            for _ in range(5):
                assert (await c.get("/api/v1/auth/ping")).status_code == 200


class TestClientIp:
    def _request(self, xff: str | None, peer: str | None) -> Any:
        scope: dict[str, Any] = {
            "type": "http",
            "headers": [(b"x-forwarded-for", xff.encode())] if xff else [],
            "client": (peer, 12345) if peer else None,
        }
        import starlette.requests as starlette_requests

        return starlette_requests.Request(scope)

    def test_prefers_first_xff_hop(self) -> None:
        req = self._request("9.9.9.9, 10.0.0.1", "172.16.0.1")
        assert rate_limit_module.client_ip(req) == "9.9.9.9"

    def test_falls_back_to_peer(self) -> None:
        req = self._request(None, "203.0.113.5")
        assert rate_limit_module.client_ip(req) == "203.0.113.5"
