"""Client for the ListenBrainz artist metadata endpoint (genre/folksonomy tags).

Fetches MusicBrainz tags for artists by MBID via the public
``GET /1/metadata/artist/?artist_mbids=<csv>&inc=tag`` endpoint (#136 genre model).

Design notes:

- Deliberately separate from ``connectors.listenbrainz`` (the OAuth user
  connection) and from ``mbid_mapper`` (which carries a user token): this endpoint
  is **token-free** today. A focused GET loop reuses ``RateLimitBudget`` for
  pacing/backoff, mirroring the mapper without inheriting its token/POST path.
- The response is a JSON array; each entry carries an ``artist_mbid`` field and a
  ``tag.artist`` list of ``{tag, count, genre_mbid?}``. We key results BY
  ``artist_mbid`` (not by position) so a reordered or partial response maps
  correctly, and a requested MBID absent from the response simply yields no tags
  (a valid "attempted, no tags" outcome). Because we key by field rather than
  align by position, there is no positional length-check to share with the
  mapper's ``_parse_results`` -- the DRY concern dissolves by construction.
- ``genre_mbid`` is present only on canonical MusicBrainz genres ("rock",
  "art pop"); it is absent on free folksonomy tags ("alternative", "seen live").

Transient failures (timeouts, 429, 5xx) raise ``ArtistTagsUnavailableError`` so
the backfill leaves the artist unattempted (retried later) rather than recording a
false "no tags". Hard failures (4xx other than 429) propagate as
``httpx.HTTPStatusError``.
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
_ARTIST_PATH = "/metadata/artist/"
# Matches the ArtistTag.tag column width (models.music).
_MAX_TAG_LEN = 256


class ArtistTagResult(pydantic.BaseModel):
    """One genre/folksonomy tag reported for an artist.

    ``genre_mbid`` is non-None only for canonical MusicBrainz genres; None marks a
    free folksonomy tag. ``count`` is the folksonomy vote count.
    """

    model_config = pydantic.ConfigDict(extra="ignore")

    tag: str
    count: int = 0
    genre_mbid: str | None = None


class ArtistTagsUnavailableError(Exception):
    """Raised when the artist-tags endpoint cannot produce a usable batch answer.

    Covers transient conditions (timeouts, connection errors, 429, 5xx) and
    malformed responses (payload not a list). The backfill treats this as "not
    attempted" (leaves ``genre_attempted_at`` NULL) so artists are retried.
    """


class ArtistTagsClient:
    """Batch client for the ListenBrainz artist-tags endpoint (token-free)."""

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
        self._base_url = (base_url or _DEFAULT_BASE_URL).rstrip("/")
        # Mirror the recording batch size (default 50); the endpoint takes a
        # comma-separated artist_mbids list.
        self._batch_size = max(1, settings.mbid_mapper_batch_size)
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazily create the HTTP client (tests inject via ``_http_client``)."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=60.0)
        return self._http_client

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        if self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def fetch_tags(
        self, artist_mbids: list[str]
    ) -> dict[str, list[ArtistTagResult]]:
        """Fetch tags for artist MBIDs, keyed by MBID.

        Splits into chunks of ``mbid_mapper_batch_size`` and GETs each. A MBID
        absent from the merged result had no tags (or was unknown to LB) -- the
        caller treats that as "attempted, no tags".

        Args:
            artist_mbids: MusicBrainz artist MBIDs.

        Returns:
            ``{artist_mbid: [ArtistTagResult, ...]}`` for MBIDs that returned tags.

        Raises:
            ArtistTagsUnavailableError: On transient failure for any chunk.
            httpx.HTTPStatusError: On hard HTTP errors.
        """
        if not artist_mbids:
            return {}
        out: dict[str, list[ArtistTagResult]] = {}
        for start in range(0, len(artist_mbids), self._batch_size):
            chunk = artist_mbids[start : start + self._batch_size]
            out.update(await self._fetch_chunk(chunk))
        return out

    async def _fetch_chunk(self, chunk: list[str]) -> dict[str, list[ArtistTagResult]]:
        url = f"{self._base_url}{_ARTIST_PATH}"
        params = {"artist_mbids": ",".join(chunk), "inc": "tag"}
        attempt = 0
        while True:
            interval = self._budget.paced_interval()
            if interval > 0:
                await asyncio.sleep(interval)

            try:
                response = await self.http_client.get(url, params=params)
            except self._TRANSIENT_ERRORS as exc:
                attempt += 1
                if attempt > self._MAX_RETRIES:
                    raise ArtistTagsUnavailableError(
                        f"transient error after {attempt} attempts: {exc}"
                    ) from exc
                await asyncio.sleep(self._BACKOFF_BASE**attempt)
                continue

            self._budget.update_from_headers(dict(response.headers))
            self._budget.record_request()

            if response.status_code == 429 or response.status_code >= 500:
                attempt += 1
                if attempt > self._MAX_RETRIES:
                    raise ArtistTagsUnavailableError(
                        f"artist-tags {response.status_code} after {attempt} attempts"
                    )
                await asyncio.sleep(self._BACKOFF_BASE**attempt)
                continue

            response.raise_for_status()
            return self._parse(response.json())

    @staticmethod
    def _parse(payload: Any) -> dict[str, list[ArtistTagResult]]:
        """Parse the artist array into ``{artist_mbid: [tags]}``.

        The payload is a list of artist objects, each with an ``artist_mbid`` and a
        ``tag.artist`` list. A payload that is not a list is treated as transient
        (raises ``ArtistTagsUnavailableError``). Per-entry shape problems yield no
        tags for that artist rather than failing the whole batch.
        """
        if not isinstance(payload, list):
            raise ArtistTagsUnavailableError(
                "unexpected artist-tags response shape (expected a list)"
            )
        out: dict[str, list[ArtistTagResult]] = {}
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            mbid = entry.get("artist_mbid") or entry.get("mbid")
            if not mbid:
                continue
            tag_block = entry.get("tag")
            raw_tags = tag_block.get("artist") if isinstance(tag_block, dict) else None
            # Dedup by (truncated) tag name -- last wins -- and clip to the
            # ArtistTag.tag column width, so a duplicate or overlong tag from the
            # source can never trip the (artist_id, tag) unique constraint or the
            # length limit on insert.
            deduped: dict[str, ArtistTagResult] = {}
            if isinstance(raw_tags, list):
                for raw in raw_tags:
                    if isinstance(raw, dict) and raw.get("tag"):
                        res = ArtistTagResult.model_validate(raw)
                        res.tag = res.tag[:_MAX_TAG_LEN]
                        deduped[res.tag] = res
            out[mbid] = list(deduped.values())
        return out
