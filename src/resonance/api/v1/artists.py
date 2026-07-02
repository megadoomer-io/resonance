"""Artist API routes — list, detail, external search, and import endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.connectors.base as base_module
import resonance.crypto as crypto_module
import resonance.dependencies as deps_module
import resonance.generators.genre as genre_module
import resonance.models.music as music_models
import resonance.models.user as user_models
import resonance.services.artist_import as artist_import_module
import resonance.types as types_module

logger = structlog.get_logger()

_PAGE_SIZE = 50
# Widen the name-match candidate pool before ranking so the right artist can't
# fall outside the returned window just for being alphabetically later (#136).
_SEARCH_CANDIDATE_CAP = 50
# How many genre names to surface in a picker result for disambiguation.
_DISPLAY_GENRES = 3

router = fastapi.APIRouter(prefix="/artists", tags=["artists"])


def _escape_ilike(q: str) -> str:
    return q.replace("%", r"\%").replace("_", r"\_")


async def artists_in_library(
    db: sa_async.AsyncSession, artist_ids: list[uuid.UUID]
) -> set[uuid.UUID]:
    """Return the subset of ``artist_ids`` we have catalog tracks for.

    Used as a lightweight "in library" signal in pickers: an artist we have
    tracks for is far more likely to be the one the user means than a cold
    external match of the same name. This is the disambiguation aid behind the
    lineup builder's artist picker (#136 — ambiguous short names like "nite"
    resolving to the wrong artist).
    """
    if not artist_ids:
        return set()
    result = await db.execute(
        sa.select(music_models.Track.artist_id)
        .where(music_models.Track.artist_id.in_(artist_ids))
        .distinct()
    )
    return set(result.scalars().all())


def _format_artist_summary(artist: music_models.Artist | Any) -> dict[str, Any]:
    return {
        "id": str(artist.id),
        "name": artist.name,
        "origin": artist.origin,
        "disambiguation": getattr(artist, "disambiguation", None) or "",
        "artist_type": getattr(artist, "artist_type", None) or "",
        "area": getattr(artist, "area", None) or "",
        "begin_year": getattr(artist, "begin_year", None),
        "end_year": getattr(artist, "end_year", None),
        "service_links": artist.service_links,
    }


async def _load_artist_tags(
    db: sa_async.AsyncSession, artist_ids: list[uuid.UUID]
) -> dict[uuid.UUID, list[music_models.ArtistTag]]:
    """Batch-load ArtistTag rows for the given artists (one query, no N+1)."""
    if not artist_ids:
        return {}
    result = await db.execute(
        sa.select(music_models.ArtistTag).where(
            music_models.ArtistTag.artist_id.in_(artist_ids)
        )
    )
    out: dict[uuid.UUID, list[music_models.ArtistTag]] = {}
    for tag in result.scalars().all():
        out.setdefault(tag.artist_id, []).append(tag)
    return out


def _genre_pairs(
    tags: list[music_models.ArtistTag],
) -> list[tuple[str | None, float]]:
    """Adapt ArtistTag rows to the affinity primitive's (genre_mbid, count) pairs."""
    return [(t.genre_mbid, float(t.count)) for t in tags]


def _display_genres(tags: list[music_models.ArtistTag]) -> list[str]:
    """Top canonical-genre tag names by count, for picker disambiguation."""
    genres = sorted(
        (t for t in tags if t.genre_mbid),
        key=lambda t: t.count,
        reverse=True,
    )
    return [t.tag for t in genres[:_DISPLAY_GENRES]]


def _genre_sort_value(affinity: float | None) -> float:
    """Rank key from an affinity score, keeping unknown distinct from mismatch.

    Positive overlap ranks highest (its value), an unknown-genre candidate
    (``None``) is neutral at 0.0, and a confirmed 0.0 genre mismatch sinks below
    neutral -- so an untagged possible-match is never tied below a known off-genre
    artist (#136 / affinity primitive finding).
    """
    if affinity is None:
        return 0.0
    if affinity == 0.0:
        return -1.0
    return affinity


def rank_search_candidates(
    candidates: list[music_models.Artist | Any],
    in_library: set[uuid.UUID],
    tags_by_artist: dict[uuid.UUID, list[music_models.ArtistTag]],
    seed_tag_lists: list[list[tuple[str | None, float]]],
) -> list[music_models.Artist | Any]:
    """Order name-match candidates for disambiguation (#136). Pure, no I/O.

    In-library artists first, then higher genre affinity to the seed set, then
    name A-Z. Affinity is computed only when there is a seed set; otherwise every
    candidate is genre-neutral and the order is in-library-then-name.
    """
    affinity_sort: dict[uuid.UUID, float] = {}
    for a in candidates:
        score = (
            genre_module.affinity_score(
                _genre_pairs(tags_by_artist.get(a.id, [])), seed_tag_lists
            )
            if seed_tag_lists
            else None
        )
        affinity_sort[a.id] = _genre_sort_value(score)

    return sorted(
        candidates,
        key=lambda a: (
            a.id not in in_library,
            -affinity_sort[a.id],
            a.name.lower(),
        ),
    )


@router.get(
    "",
    summary="List artists",
    description=(
        "Paginated list of artists, alphabetical by name. Optionally filtered by "
        "genre (``genre_mbid``, repeatable -- an artist matches if it carries ANY "
        "of the given genres). Each result carries its top canonical genres."
    ),
)
async def list_artists(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
    q: str | None = None,
    genre_mbid: Annotated[list[str] | None, fastapi.Query()] = None,
) -> dict[str, Any]:
    offset = (page - 1) * _PAGE_SIZE

    stmt = sa.select(music_models.Artist).order_by(music_models.Artist.name)

    if q:
        stmt = stmt.where(music_models.Artist.name.ilike(f"%{_escape_ilike(q)}%"))

    if genre_mbid:
        # OR-match: the artist has at least one tag in the selected genres. A
        # correlated EXISTS (not a JOIN) keeps the row set one-per-artist without a
        # DISTINCT, and hits ix_artist_tags_genre_mbid.
        stmt = stmt.where(
            sa.exists().where(
                music_models.ArtistTag.artist_id == music_models.Artist.id,
                music_models.ArtistTag.genre_mbid.in_(genre_mbid),
            )
        )

    stmt = stmt.offset(offset).limit(_PAGE_SIZE + 1)

    result = await db.execute(stmt)
    artists = list(result.scalars().all())

    has_next = len(artists) > _PAGE_SIZE
    artists = artists[:_PAGE_SIZE]

    tags_by_artist = await _load_artist_tags(db, [a.id for a in artists])
    items = []
    for a in artists:
        summary = _format_artist_summary(a)
        summary["genres"] = _display_genres(tags_by_artist.get(a.id, []))
        items.append(summary)

    return {
        "items": items,
        "page": page,
        "page_size": _PAGE_SIZE,
        "has_next": has_next,
    }


@router.get(
    "/search",
    summary="Search artists by name",
    description="Search for artists matching a query string.",
)
async def search_artists(
    q: str,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    limit: Annotated[int, fastapi.Query(ge=1, le=_SEARCH_CANDIDATE_CAP)] = 10,
    seed_artist_ids: Annotated[list[uuid.UUID] | None, fastapi.Query()] = None,
) -> dict[str, Any]:
    """Search artists by name, ranked for disambiguation (#136).

    Candidates are all exact-name matches (never truncated -- the literal
    same-name collision like two artists named "Nite" is the core #136 case) plus
    a capped alphabetical pool of substring matches. They are then ranked so the
    artist the user most likely means surfaces first:

    1. In-library artists (we have their tracks) beat cold same-name matches.
    2. Then genre affinity to ``seed_artist_ids`` (the artists already in the
       builder) -- e.g. metal seeds prefer the metal "Nite" over the electronic
       one. Unknown-genre stays neutral; a confirmed off-genre match sinks.
    3. Then name, for a stable order.

    Each result carries ``genres`` (top canonical genres) so two same-name artists
    are visually distinguishable in the picker.
    """
    q_clean = q.strip()
    # Exact-name matches are never dropped by the cap (the same-name collision is
    # exactly what #136 is about); the substring pool is capped alphabetically and
    # ranked around them.
    exact_stmt = sa.select(music_models.Artist).where(
        sa.func.lower(music_models.Artist.name) == q_clean.lower()
    )
    sub_stmt = (
        sa.select(music_models.Artist)
        .where(music_models.Artist.name.ilike(f"%{_escape_ilike(q_clean)}%"))
        .order_by(music_models.Artist.name)
        .limit(_SEARCH_CANDIDATE_CAP)
    )
    exact = list((await db.execute(exact_stmt)).scalars().all())
    substring = list((await db.execute(sub_stmt)).scalars().all())
    # Merge exact-first, dedup by id (exact rows are a subset of substring but may
    # sit past the alphabetical cap).
    seen: set[uuid.UUID] = set()
    candidates: list[music_models.Artist] = []
    for a in (*exact, *substring):
        if a.id not in seen:
            seen.add(a.id)
            candidates.append(a)

    in_library = await artists_in_library(db, [a.id for a in candidates])

    # Batch-load genre tags for candidates + seeds in one query each (no N+1).
    seed_ids = list(seed_artist_ids or [])
    tags_by_artist = await _load_artist_tags(db, [a.id for a in candidates] + seed_ids)
    seed_tag_lists = [
        _genre_pairs(tags_by_artist.get(sid, []))
        for sid in seed_ids
        if tags_by_artist.get(sid)
    ]

    ranked = rank_search_candidates(
        candidates, in_library, tags_by_artist, seed_tag_lists
    )

    items = []
    for a in ranked[:limit]:  # limit is bounded to [1, cap] by the Query gate
        summary = _format_artist_summary(a)
        summary["in_library"] = a.id in in_library
        summary["genres"] = _display_genres(tags_by_artist.get(a.id, []))
        items.append(summary)
    return {"items": items}


@router.get(
    "/search-external",
    summary="Search external services for artists",
    description="Search MusicBrainz or Spotify for artists by name or URL.",
)
async def search_external(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str | None = None,
    url: str | None = None,
    services: str = "musicbrainz",
) -> list[dict[str, Any]]:
    """Search external services for artists.

    Provide either ``q`` (text search) or ``url`` (direct lookup).
    When ``url`` is given, connectors are checked to identify the service.

    Args:
        request: The incoming HTTP request (for connector registry access).
        user_id: The authenticated user's ID.
        db: The async database session.
        q: Text search query.
        url: External service URL for direct artist lookup.
        services: Comma-separated service names to search (default: musicbrainz).

    Returns:
        List of external artist result dicts.

    Raises:
        HTTPException: 400 if neither q nor url is provided.
    """
    if not q and not url:
        raise fastapi.HTTPException(
            status_code=400, detail="Provide either 'q' or 'url' parameter"
        )

    registry = request.app.state.connector_registry
    results: list[dict[str, Any]] = []
    log = logger.bind(user_id=str(user_id))

    if url:
        log.info("artist_search_external_by_url", url=url)
        results = await _search_by_url(url, registry, db, user_id, request)
    else:
        assert q is not None
        service_list = [s.strip() for s in services.split(",")]
        log.info(
            "artist_search_external_by_query",
            query=q,
            services=service_list,
        )
        results = await _search_by_query(
            q, service_list, registry, db, user_id, request
        )

    log.info("artist_search_external_complete", result_count=len(results))
    return results


async def _search_by_url(
    url: str,
    registry: Any,
    db: sa_async.AsyncSession,
    user_id: uuid.UUID,
    request: fastapi.Request,
) -> list[dict[str, Any]]:
    """Resolve an artist from a service URL.

    Iterates registered connectors to find one that recognizes the URL,
    then looks up the artist on that service.

    Args:
        url: The external service URL.
        registry: The connector registry.
        db: The async database session.
        user_id: The authenticated user's ID.
        request: The incoming HTTP request (for settings access).

    Returns:
        List with zero or one result dicts.
    """
    # Try each registered connector's parse_url
    for connector in registry.all():
        if not isinstance(connector, base_module.BaseConnector):
            continue
        artist_id_str = connector.parse_url(url)
        if artist_id_str is None:
            continue

        logger.info(
            "artist_url_detected",
            service=connector.service_type.value,
            parsed_id=artist_id_str,
        )

        if connector.service_type == types_module.ServiceType.LISTENBRAINZ:
            # artist_id_str is an MBID
            lb_connector = registry.get_base_connector(
                types_module.ServiceType.LISTENBRAINZ
            )
            if lb_connector is None:
                return []
            artist_data = await lb_connector.get_artist_by_mbid(artist_id_str)
            if artist_data is None:
                return []
            return await _annotate_mb_results([artist_data], db)

        if connector.service_type == types_module.ServiceType.SPOTIFY:
            access_token = await _get_spotify_token(db, user_id, request)
            if access_token is None:
                return []
            sp_connector = registry.get_base_connector(types_module.ServiceType.SPOTIFY)
            if sp_connector is None:
                return []
            sp_results = await sp_connector.search_artists(
                access_token, artist_id_str, limit=1
            )
            return [
                {
                    "service": "spotify",
                    "spotify_id": r["spotify_id"],
                    "name": r["name"],
                    "already_imported": False,
                    "local_artist_id": None,
                }
                for r in sp_results
            ]

    return []


async def _search_by_query(
    q: str,
    service_list: list[str],
    registry: Any,
    db: sa_async.AsyncSession,
    user_id: uuid.UUID,
    request: fastapi.Request,
) -> list[dict[str, Any]]:
    """Search external services by text query.

    Args:
        q: The search query.
        service_list: Service names to search.
        registry: The connector registry.
        db: The async database session.
        user_id: The authenticated user's ID.
        request: The incoming HTTP request.

    Returns:
        List of result dicts from all requested services.
    """
    results: list[dict[str, Any]] = []

    if "musicbrainz" in service_list:
        lb_connector = registry.get_base_connector(
            types_module.ServiceType.LISTENBRAINZ
        )
        if lb_connector is not None:
            mb_results = await lb_connector.search_artists(q)
            results.extend(await _annotate_mb_results(mb_results, db))

    if "spotify" in service_list:
        access_token = await _get_spotify_token(db, user_id, request)
        if access_token is not None:
            sp_connector = registry.get_base_connector(types_module.ServiceType.SPOTIFY)
            if sp_connector is not None:
                sp_results = await sp_connector.search_artists(access_token, q)
                results.extend(
                    {
                        "service": "spotify",
                        "spotify_id": r["spotify_id"],
                        "name": r["name"],
                        "already_imported": False,
                        "local_artist_id": None,
                    }
                    for r in sp_results
                )

    return results


async def _annotate_mb_results(
    mb_results: list[dict[str, Any]],
    db: sa_async.AsyncSession,
) -> list[dict[str, Any]]:
    """Annotate MusicBrainz results with local import status.

    For each result, checks if an artist with the same MBID exists
    locally and sets ``already_imported`` and ``local_artist_id``.

    Args:
        mb_results: List of MusicBrainz artist dicts.
        db: The async database session.

    Returns:
        Annotated result dicts with import status.
    """
    annotated: list[dict[str, Any]] = []
    for result in mb_results:
        mbid = result["mbid"]
        local_artist = await _find_local_artist_by_mbid(db, mbid)
        annotated.append(
            {
                **result,
                "service": "musicbrainz",
                "already_imported": local_artist is not None,
                "local_artist_id": (
                    str(local_artist.id) if local_artist is not None else None
                ),
            }
        )
    return annotated


async def _find_local_artist_by_mbid(
    db: sa_async.AsyncSession, mbid: str
) -> Any | None:
    """Find a local artist by MBID, checking both storage locations.

    Thin wrapper over the shared service helper so the HTTP import path and the
    worker's adjacent-artist import (issue #115) dedup identically.

    Args:
        db: The async database session.
        mbid: The MusicBrainz artist ID.

    Returns:
        The Artist if found, or None.
    """
    return await artist_import_module.find_local_artist_by_mbid(db, mbid)


async def _get_spotify_token(
    db: sa_async.AsyncSession,
    user_id: uuid.UUID,
    request: fastapi.Request,
) -> str | None:
    """Get a decrypted Spotify access token for the user.

    Args:
        db: The async database session.
        user_id: The user's ID.
        request: The incoming HTTP request (for settings access).

    Returns:
        The decrypted access token, or None if no connection exists.
    """
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type == types_module.ServiceType.SPOTIFY,
    )
    result = await db.execute(stmt)
    connection = result.scalar_one_or_none()
    if connection is None or connection.encrypted_access_token is None:
        return None
    settings = request.app.state.settings
    return crypto_module.decrypt_token(
        connection.encrypted_access_token, settings.token_encryption_key
    )


class ArtistImportRequest(pydantic.BaseModel):
    """Request body for importing an artist from an external service."""

    mbid: str
    name: str
    disambiguation: str = ""
    artist_type: str = ""
    area: str = ""
    begin_year: int | None = None
    end_year: int | None = None
    service_ids: dict[str, str] = {}


@router.post(
    "/import",
    summary="Import an artist from an external service",
    description="Create a local artist record from external service data.",
)
async def import_artist(
    request: fastapi.Request,
    body: ArtistImportRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    """Import an artist by MBID, or return existing if already present.

    Args:
        request: The incoming HTTP request.
        body: The import request with artist metadata.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        The created or existing artist as a summary dict.
    """
    log = logger.bind(
        user_id=str(user_id),
        artist_name=body.name,
        mbid=body.mbid,
    )
    # Check for existing artist by MBID
    existing = await _find_local_artist_by_mbid(db, body.mbid)
    if existing is not None:
        log.info("artist_import_existing", artist_id=str(existing.id))
        return _format_artist_summary(existing)

    # Build service_links
    service_links: dict[str, Any] = {
        "musicbrainz": {"id": body.mbid},
    }
    for svc_name, svc_id in body.service_ids.items():
        service_links[svc_name] = {"id": svc_id}

    artist = music_models.Artist(
        name=body.name,
        disambiguation=body.disambiguation or None,
        artist_type=body.artist_type or None,
        area=body.area or None,
        begin_year=body.begin_year,
        end_year=body.end_year,
        service_links=service_links,
    )
    db.add(artist)
    await db.flush()
    await db.commit()

    log.info("artist_imported", artist_id=str(artist.id), created=True)
    return _format_artist_summary(artist)


@router.get(
    "/{artist_id}",
    summary="Get artist detail",
    description="Get an artist with service links.",
)
async def get_artist(
    artist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    stmt = sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
    result = await db.execute(stmt)
    artist = result.scalar_one_or_none()

    if artist is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    return {
        "id": str(artist.id),
        "name": artist.name,
        "origin": artist.origin,
        "service_links": artist.service_links,
        "created_at": artist.created_at.isoformat(),
        "updated_at": artist.updated_at.isoformat(),
    }
