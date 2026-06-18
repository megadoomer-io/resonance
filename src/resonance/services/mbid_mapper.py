"""Client for the ListenBrainz metadata lookup (MusicBrainz MBID mapper).

Resolves ``(artist_name, recording_name)`` pairs to MusicBrainz recording and
artist MBIDs via the ListenBrainz ``/1/metadata/lookup`` batch endpoint (#71).

Design notes:

- This is deliberately separate from ``connectors.listenbrainz`` (the OAuth
  user-connection connector). The mapper is an anonymous bulk-lookup tool, not a
  per-user connection — see /plan-eng-review issue 7 (7A).
- The endpoint requires a ListenBrainz user token (anti-scraper) and returns
  **no confidence score**, so callers gate matches with ``normalize.name_similarity``
  rather than trusting the mapper blindly (T1-A).
- The batch POST returns a JSON array aligned to the input order; a miss is an
  entry whose ``recording_mbid`` is null. Alignment is therefore positional.
- It reuses ``RateLimitBudget`` for pacing/backoff but keeps a small, focused
  request loop (no OAuth, no high-priority lane, no Spotify 403 path). The full
  ``BaseConnector._request`` is richer but lives in the connector ABC; extracting
  it into a shared mixin is a possible future DRY refactor.

Transient failures (timeouts, 429, 5xx) raise ``MapperUnavailableError`` so the
backfill core can leave the entity unattempted (retried later) instead of
recording a false ``no_match``. Hard failures (e.g. 401 bad token, 400 bad
request) propagate as ``httpx.HTTPStatusError``.

       queries (in order)              response array (aligned)
   ┌──────────────────────────┐    ┌──────────────────────────────┐
   │ 0: Rick Astley / NGGYU   │ →  │ 0: {recording_mbid: "8f34…"} │ → RecordingMatch
   │ 1: Zzqq / Fake Track     │ →  │ 1: {recording_mbid: null}    │ → None (no match)
   └──────────────────────────┘    └──────────────────────────────┘
"""

import asyncio
from typing import Any

import httpx
import pydantic
import structlog

import resonance.config as config_module
import resonance.connectors.ratelimit as ratelimit_module

logger = structlog.get_logger()

_DEFAULT_BASE_URL = "https://api.listenbrainz.org/1"
_LOOKUP_PATH = "/metadata/lookup/"


class RecordingQuery(pydantic.BaseModel):
    """A single (artist, recording) lookup request."""

    artist_name: str
    recording_name: str
    release_name: str | None = None

    def to_payload(self) -> dict[str, str]:
        """Render the query as the endpoint's per-recording payload dict."""
        payload = {
            "artist_name": self.artist_name,
            "recording_name": self.recording_name,
        }
        if self.release_name:
            payload["release_name"] = self.release_name
        return payload


class RecordingMatch(pydantic.BaseModel):
    """A resolved MusicBrainz match for a recording query.

    ``artist_credit_name`` / ``recording_name`` are the mapper's view of the
    matched entity and are what callers feed to the similarity gate.
    """

    model_config = pydantic.ConfigDict(extra="ignore")

    recording_mbid: str
    artist_mbids: list[str] = pydantic.Field(default_factory=list)
    artist_credit_name: str | None = None
    recording_name: str | None = None
    release_mbid: str | None = None
    release_name: str | None = None


class MapperUnavailableError(Exception):
    """Raised when the mapper cannot produce a usable answer for a batch.

    Covers transient conditions (timeouts, connection errors, 429, 5xx) and
    malformed/misaligned responses. The backfill core treats this as "not
    attempted" (leaves ``mb_attempted_at`` NULL) so the entities are retried,
    rather than recording a false ``no_match``.
    """


class MbidMapperClient:
    """Batch client for the ListenBrainz MusicBrainz mapper endpoint."""

    _TRANSIENT_ERRORS: tuple[type[Exception], ...] = (
        httpx.ReadTimeout,
        httpx.ConnectError,
        httpx.RemoteProtocolError,
    )
    _MAX_RETRIES = 5
    _BACKOFF_BASE = 2.0  # seconds — doubles each retry

    def __init__(
        self,
        settings: config_module.Settings,
        *,
        base_url: str | None = None,
    ) -> None:
        self._token = settings.lb_user_token
        self._base_url = (base_url or _DEFAULT_BASE_URL).rstrip("/")
        self._batch_size = max(1, settings.mbid_mapper_batch_size)
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazily create and return the HTTP client.

        Tests inject a transport by assigning ``_http_client`` directly, matching
        the connector test convention.
        """
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=60.0)
        return self._http_client

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def lookup_recordings(
        self, queries: list[RecordingQuery]
    ) -> list[RecordingMatch | None]:
        """Resolve queries to matches, aligned by index (None = no match).

        Splits the input into chunks of ``mbid_mapper_batch_size`` and POSTs each
        chunk to the batch endpoint.

        Args:
            queries: Lookup queries.

        Returns:
            A list the same length as ``queries``; element ``i`` is the match for
            ``queries[i]`` or None if the mapper found no match.

        Raises:
            MapperUnavailableError: On transient failure for any chunk.
            httpx.HTTPStatusError: On hard HTTP errors (e.g. 401, 400).
        """
        if not queries:
            return []
        results: list[RecordingMatch | None] = []
        for start in range(0, len(queries), self._batch_size):
            chunk = queries[start : start + self._batch_size]
            results.extend(await self._lookup_chunk(chunk))
        return results

    async def _lookup_chunk(
        self, chunk: list[RecordingQuery]
    ) -> list[RecordingMatch | None]:
        url = f"{self._base_url}{_LOOKUP_PATH}"
        body = {"recordings": [q.to_payload() for q in chunk]}
        headers = {
            "Authorization": f"Token {self._token}",
            "Content-Type": "application/json",
        }
        attempt = 0
        while True:
            interval = self._budget.paced_interval()
            if interval > 0:
                await asyncio.sleep(interval)

            try:
                response = await self.http_client.post(url, json=body, headers=headers)
            except self._TRANSIENT_ERRORS as exc:
                attempt += 1
                if attempt > self._MAX_RETRIES:
                    raise MapperUnavailableError(
                        f"transient error after {attempt} attempts: {exc}"
                    ) from exc
                await asyncio.sleep(self._BACKOFF_BASE**attempt)
                continue

            self._budget.update_from_headers(dict(response.headers))
            self._budget.record_request()

            if response.status_code == 429 or response.status_code >= 500:
                attempt += 1
                if attempt > self._MAX_RETRIES:
                    raise MapperUnavailableError(
                        f"mapper {response.status_code} after {attempt} attempts"
                    )
                await asyncio.sleep(self._BACKOFF_BASE**attempt)
                continue

            # Hard errors (401 bad token, 400 bad request) propagate — not transient.
            response.raise_for_status()
            return self._parse_results(response.json(), len(chunk))

    @staticmethod
    def _parse_results(payload: Any, expected_len: int) -> list[RecordingMatch | None]:
        """Parse the batch response array into index-aligned matches.

        A length mismatch is treated as transient (raises
        MapperUnavailableError) rather than risking misaligned writes.
        """
        if not isinstance(payload, list) or len(payload) != expected_len:
            raise MapperUnavailableError(
                f"unexpected mapper response shape (expected list of {expected_len})"
            )
        out: list[RecordingMatch | None] = []
        for entry in payload:
            if isinstance(entry, dict) and entry.get("recording_mbid"):
                out.append(RecordingMatch.model_validate(entry))
            else:
                out.append(None)
        return out
