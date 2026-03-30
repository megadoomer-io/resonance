"""Redis-backed session middleware with signed cookies."""

from __future__ import annotations

import json
import uuid
from typing import Any

import itsdangerous
import redis.asyncio as aioredis
import starlette.middleware.base as base_middleware
import starlette.requests as starlette_requests
import starlette.responses as starlette_responses
import starlette.types as starlette_types

# redis.asyncio.Redis is not generic in the type stubs shipped with redis 7.x,
# so we use the unparameterised form.
RedisClient = aioredis.Redis


class SessionData:
    """Dict-like container for session data with modification tracking."""

    def __init__(
        self,
        session_id: str,
        data: dict[str, Any],
        is_new: bool = False,
    ) -> None:
        self.session_id = session_id
        self.data = data
        self.is_new = is_new
        self.modified = False

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.modified = True

    def __contains__(self, key: object) -> bool:
        return key in self.data

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def clear(self) -> None:
        self.data.clear()
        self.modified = True


class SessionMiddleware(base_middleware.BaseHTTPMiddleware):
    """Middleware that loads/saves session data via Redis and signed cookies."""

    def __init__(
        self,
        app: starlette_types.ASGIApp,
        redis: RedisClient,
        secret_key: str,
        cookie_name: str = "session_id",
        max_age: int = 86400 * 30,
    ) -> None:
        super().__init__(app)
        self.redis = redis
        self.signer = itsdangerous.TimestampSigner(secret_key)
        self.cookie_name = cookie_name
        self.max_age = max_age

    async def dispatch(
        self,
        request: starlette_requests.Request,
        call_next: base_middleware.RequestResponseEndpoint,
    ) -> starlette_responses.Response:
        session = await self._load_session(request)
        request.state.session = session

        response = await call_next(request)

        if session.modified or (session.is_new and session.data):
            await self._save_session(session, response)

        return response

    async def _load_session(
        self,
        request: starlette_requests.Request,
    ) -> SessionData:
        """Load session from signed cookie + Redis, or create a new one."""
        cookie_value = request.cookies.get(self.cookie_name)
        if cookie_value:
            try:
                session_id = self.signer.unsign(
                    cookie_value,
                    max_age=self.max_age,
                ).decode("utf-8")
                raw = await self.redis.get(f"session:{session_id}")
                if raw is not None:
                    data: dict[str, Any] = json.loads(raw)
                    return SessionData(session_id=session_id, data=data)
            except (itsdangerous.BadSignature, itsdangerous.SignatureExpired):
                pass

        # No valid session found — create a new one.
        return SessionData(
            session_id=str(uuid.uuid4()),
            data={},
            is_new=True,
        )

    async def _save_session(
        self,
        session: SessionData,
        response: starlette_responses.Response,
    ) -> None:
        """Persist session data to Redis and set the signed cookie."""
        key = f"session:{session.session_id}"
        await self.redis.setex(key, self.max_age, json.dumps(session.data))

        signed = self.signer.sign(session.session_id).decode("utf-8")
        response.set_cookie(
            key=self.cookie_name,
            value=signed,
            max_age=self.max_age,
            httponly=True,
            samesite="lax",
        )


async def destroy_session(
    request: starlette_requests.Request,
    response: starlette_responses.Response,
    redis: RedisClient,
    cookie_name: str = "session_id",
) -> None:
    """Delete a session from Redis and clear the cookie."""
    session: SessionData = request.state.session
    await redis.delete(f"session:{session.session_id}")
    response.delete_cookie(key=cookie_name)
