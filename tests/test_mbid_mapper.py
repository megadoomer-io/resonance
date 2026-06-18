"""Tests for the MusicBrainz MBID mapper client (#71)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import resonance.config as config_module
import resonance.services.mbid_mapper as mapper_module

_MATCH = {
    "recording_mbid": "8f3471b5-7e6a-48da-86a9-c1c07a0f47ae",
    "artist_mbids": ["db92a151-1ac2-438b-bc43-b82e149ddd50"],
    "artist_credit_name": "Rick Astley",
    "recording_name": "Never Gonna Give You Up",
    "release_mbid": "18550a84-4ede-451c-a91a-dd9b3af9f71d",
    "release_name": "Red Hot",
}
_MISS = {
    "recording_mbid": None,
    "artist_credit_name": None,
    "recording_name": None,
}


def _make_settings(batch_size: int = 50) -> config_module.Settings:
    return config_module.Settings(
        lb_user_token="test-token", mbid_mapper_batch_size=batch_size
    )


def _client_with_handler(
    handler: Any, *, batch_size: int = 50
) -> mapper_module.MbidMapperClient:
    client = mapper_module.MbidMapperClient(_make_settings(batch_size))
    client._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


def _q(
    artist: str, recording: str, release: str | None = None
) -> mapper_module.RecordingQuery:
    return mapper_module.RecordingQuery(
        artist_name=artist, recording_name=recording, release_name=release
    )


class TestLookupRecordings:
    @pytest.mark.anyio()
    async def test_empty_queries_returns_empty(self) -> None:
        client = _client_with_handler(lambda req: httpx.Response(200, json=[]))
        assert await client.lookup_recordings([]) == []

    @pytest.mark.anyio()
    async def test_match_and_miss_aligned_by_index(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[_MATCH, _MISS])

        client = _client_with_handler(handler)
        results = await client.lookup_recordings(
            [_q("Rick Astley", "Never Gonna Give You Up"), _q("Zzqq", "Fake")]
        )
        assert results[0] is not None
        assert results[0].recording_mbid == _MATCH["recording_mbid"]
        assert results[0].artist_mbids == _MATCH["artist_mbids"]
        assert results[0].artist_credit_name == "Rick Astley"
        assert results[1] is None

    @pytest.mark.anyio()
    async def test_auth_header_and_payload(self) -> None:
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["auth"] = request.headers.get("Authorization")
            import json

            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json=[_MATCH])

        client = _client_with_handler(handler)
        await client.lookup_recordings(
            [_q("Rick Astley", "Never Gonna Give You Up", release="Red Hot")]
        )
        assert captured["auth"] == "Token test-token"
        assert captured["body"] == {
            "recordings": [
                {
                    "artist_name": "Rick Astley",
                    "recording_name": "Never Gonna Give You Up",
                    "release_name": "Red Hot",
                }
            ]
        }

    @pytest.mark.anyio()
    async def test_release_name_omitted_when_absent(self) -> None:
        captured: dict[str, Any] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            import json

            captured["body"] = json.loads(request.content)
            return httpx.Response(200, json=[_MISS])

        client = _client_with_handler(handler)
        await client.lookup_recordings([_q("A", "B")])
        assert "release_name" not in captured["body"]["recordings"][0]

    @pytest.mark.anyio()
    async def test_chunking_splits_requests_and_preserves_order(self) -> None:
        calls: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:
            import json

            body = json.loads(request.content)
            n = len(body["recordings"])
            calls.append(n)
            # Echo a distinct match per item so order can be verified.
            resp = [
                {**_MATCH, "recording_name": rec["recording_name"]}
                for rec in body["recordings"]
            ]
            return httpx.Response(200, json=resp)

        client = _client_with_handler(handler, batch_size=2)
        queries = [_q("Artist", f"T{i}") for i in range(5)]
        results = await client.lookup_recordings(queries)
        assert calls == [2, 2, 1]  # 5 items in chunks of 2
        assert [r.recording_name for r in results if r] == [f"T{i}" for i in range(5)]


class TestRetryAndErrors:
    @pytest.mark.anyio()
    async def test_429_then_success(self) -> None:
        state = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            state["n"] += 1
            if state["n"] == 1:
                return httpx.Response(429)
            return httpx.Response(200, json=[_MATCH])

        client = _client_with_handler(handler)
        with patch.object(mapper_module.asyncio, "sleep", new_callable=AsyncMock):
            results = await client.lookup_recordings([_q("Rick Astley", "NGGYU")])
        assert state["n"] == 2
        assert results[0] is not None

    @pytest.mark.anyio()
    async def test_429_exhausted_raises_unavailable(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(429)

        client = _client_with_handler(handler)
        with (
            patch.object(mapper_module.asyncio, "sleep", new_callable=AsyncMock),
            pytest.raises(mapper_module.MapperUnavailableError),
        ):
            await client.lookup_recordings([_q("A", "B")])

    @pytest.mark.anyio()
    async def test_5xx_then_success(self) -> None:
        state = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            state["n"] += 1
            if state["n"] == 1:
                return httpx.Response(503)
            return httpx.Response(200, json=[_MATCH])

        client = _client_with_handler(handler)
        with patch.object(mapper_module.asyncio, "sleep", new_callable=AsyncMock):
            results = await client.lookup_recordings([_q("A", "B")])
        assert results[0] is not None

    @pytest.mark.anyio()
    async def test_transient_error_raises_unavailable_after_retries(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("boom")

        client = _client_with_handler(handler)
        with (
            patch.object(mapper_module.asyncio, "sleep", new_callable=AsyncMock),
            pytest.raises(mapper_module.MapperUnavailableError),
        ):
            await client.lookup_recordings([_q("A", "B")])

    @pytest.mark.anyio()
    async def test_length_mismatch_raises_unavailable(self) -> None:
        # Two queries, but the mapper returns only one entry -> misalignment.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[_MATCH])

        client = _client_with_handler(handler)
        with pytest.raises(mapper_module.MapperUnavailableError):
            await client.lookup_recordings([_q("A", "B"), _q("C", "D")])

    @pytest.mark.anyio()
    async def test_401_propagates_as_http_error_not_unavailable(self) -> None:
        # A bad/expired token is a hard failure, not a per-entity transient one.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, json={"error": "invalid token"})

        client = _client_with_handler(handler)
        with pytest.raises(httpx.HTTPStatusError):
            await client.lookup_recordings([_q("A", "B")])
