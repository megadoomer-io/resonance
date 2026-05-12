# Entity Matching UI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add entity detail pages, side-by-side comparison/merge views, and candidate accept/reject UI to enable users to manage entity relationships that affect playlist generation quality.

**Architecture:** Layered approach — API endpoints first (testable without UI), then UI routes and templates. Each layer builds on the previous. Existing `dedup.py` merge functions and `concerts/sync.py` candidate logic are reused; no new business logic modules needed.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 (async), Jinja2/HTMX, Alembic, pytest

**Design doc:** `docs/plans/2026-04-28-entity-matching-ui-design.md`

---

## Task 1: Add `origin` field to Artist model + Alembic migration

**Files:**
- Modify: `src/resonance/models/music.py` — add `origin` column to `Artist`
- Create: `alembic/versions/u8p9q0r1s2t3_add_artist_origin_field.py`
- Modify: `src/resonance/api/v1/artists.py` — include `origin` in API responses

**Step 1: Add `origin` column to Artist model**

In `src/resonance/models/music.py`, add after the `service_links` field on `Artist`:

```python
origin: orm.Mapped[str | None] = orm.mapped_column(
    sa.String(256), nullable=True, default=None
)
```

**Step 2: Create Alembic migration**

Create `alembic/versions/u8p9q0r1s2t3_add_artist_origin_field.py`:

```python
"""add origin field to artists table

Revision ID: u8p9q0r1s2t3
Revises: t7o8p9q0r1s2
Create Date: 2026-04-28

"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "u8p9q0r1s2t3"
down_revision: str = "t7o8p9q0r1s2"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column("artists", sa.Column("origin", sa.String(256), nullable=True))


def downgrade() -> None:
    op.drop_column("artists", "origin")
```

**Step 3: Update API responses to include `origin`**

In `src/resonance/api/v1/artists.py`, add `"origin": artist.origin` to both `_format_artist_summary` return dict and `get_artist` return dict.

**Step 4: Run lint and type checks**

```bash
uv run ruff check src/resonance/models/music.py src/resonance/api/v1/artists.py
uv run mypy src/resonance/models/music.py src/resonance/api/v1/artists.py
```

**Step 5: Run existing tests to confirm no regressions**

```bash
uv run pytest tests/ -x -q
```

**Step 6: Commit**

```bash
git add src/resonance/models/music.py alembic/versions/u8p9q0r1s2t3_add_artist_origin_field.py src/resonance/api/v1/artists.py
git commit -m "feat: add origin field to Artist model with migration"
```

---

## Task 2: Candidate accept/reject and artist search API endpoints

These are the new API endpoints from the design doc. They must exist before the UI can call them.

**Files:**
- Modify: `src/resonance/api/v1/events.py` — add accept, reject, create-candidate endpoints
- Modify: `src/resonance/api/v1/artists.py` — add search endpoint
- Create: `src/resonance/api/v1/matching.py` — merge preview and confirm endpoints
- Modify: `src/resonance/api/v1/__init__.py` — register matching router
- Create: `tests/test_api_matching.py` — tests for all new endpoints

**Step 1: Write failing tests for candidate accept/reject**

Create `tests/test_api_matching.py` with tests using the same fake DB pattern from `tests/test_api_data.py`:

```python
"""Tests for entity matching API endpoints."""
from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import httpx
import pytest

import resonance.config as config_module
import resonance.middleware.session as session_middleware

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _make_settings() -> config_module.Settings:
    return config_module.Settings(
        spotify_client_id="test-id",
        spotify_client_secret="test-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
    )


class FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._sets: dict[str, set[str]] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> int:
        return sum(1 for k in keys if self._store.pop(k, None) is not None)

    async def sadd(self, key: str, *values: str) -> int:
        self._sets.setdefault(key, set()).update(values)
        return len(values)

    async def smembers(self, key: str) -> set[bytes]:
        return {v.encode() for v in self._sets.get(key, set())}

    async def expire(self, key: str, ttl: int) -> bool:
        return key in self._store or key in self._sets

    async def aclose(self) -> None:
        pass

    def inject_session(self, session_id: str, data: dict[str, Any]) -> None:
        import json
        self._store[f"session:{session_id}"] = json.dumps(data)


class FakeResult:
    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []

    def unique(self) -> FakeResult:
        return self

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


class FakeSession:
    def __init__(self, results: list[Any] | None = None) -> None:
        self._results = results or []
        self._call_idx = 0
        self.added: list[Any] = []
        self.committed = False

    async def execute(self, stmt: Any) -> FakeResult:
        if self._call_idx < len(self._results):
            result = self._results[self._call_idx]
            self._call_idx += 1
            return FakeResult(result if isinstance(result, list) else [result])
        return FakeResult([])

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def commit(self) -> None:
        self.committed = True

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


@pytest.fixture
def user_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def session_id() -> str:
    return "test-session-accept-reject"


@pytest.fixture
async def client(
    user_id: uuid.UUID,
    session_id: str,
) -> AsyncIterator[httpx.AsyncClient]:
    """Create test client with auth session."""
    from resonance.app import create_app

    settings = _make_settings()
    app = create_app(settings)
    redis = FakeRedis()
    redis.inject_session(session_id, {"user_id": str(user_id)})
    app.state.redis = redis
    app.state.settings = settings

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        cookies={session_middleware.SESSION_COOKIE_NAME: session_id},
    ) as c:
        yield c


@pytest.mark.anyio
async def test_artist_search_returns_matching_artists(
    client: httpx.AsyncClient,
) -> None:
    resp = await client.get("/api/v1/artists/search", params={"q": "test"})
    assert resp.status_code == 200
    data = resp.json()
    assert "items" in data


@pytest.mark.anyio
async def test_artist_search_requires_query(
    client: httpx.AsyncClient,
) -> None:
    resp = await client.get("/api/v1/artists/search")
    assert resp.status_code == 422


@pytest.mark.anyio
async def test_candidate_accept_404_missing_event(
    client: httpx.AsyncClient,
) -> None:
    event_id = uuid.uuid4()
    candidate_id = uuid.uuid4()
    resp = await client.post(
        f"/api/v1/events/{event_id}/candidates/{candidate_id}/accept"
    )
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_candidate_reject_404_missing_event(
    client: httpx.AsyncClient,
) -> None:
    event_id = uuid.uuid4()
    candidate_id = uuid.uuid4()
    resp = await client.post(
        f"/api/v1/events/{event_id}/candidates/{candidate_id}/reject"
    )
    assert resp.status_code == 404
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_api_matching.py -v
```

Expected: FAIL — endpoints don't exist yet.

**Step 3: Implement artist search endpoint**

In `src/resonance/api/v1/artists.py`, add a new route before `get_artist`:

```python
@router.get(
    "/search",
    summary="Search artists by name",
    description="Search for artists matching a query string.",
)
async def search_artists(
    q: str,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    limit: int = 10,
) -> dict[str, Any]:
    stmt = (
        sa.select(music_models.Artist)
        .where(music_models.Artist.name.ilike(f"%{q}%"))
        .order_by(music_models.Artist.name)
        .limit(min(limit, 50))
    )
    result = await db.execute(stmt)
    artists = list(result.scalars().all())
    return {"items": [_format_artist_summary(a) for a in artists]}
```

**Step 4: Implement candidate accept/reject endpoints**

In `src/resonance/api/v1/events.py`, add these routes:

```python
import resonance.models.music as music_models
import resonance.types as types_module

@router.post(
    "/{event_id}/candidates/{candidate_id}/accept",
    summary="Accept a candidate match",
)
async def accept_candidate(
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    candidate = await _get_candidate(db, event_id, candidate_id)

    if candidate.matched_artist_id is None:
        raise fastapi.HTTPException(
            status_code=400, detail="Candidate has no matched artist"
        )

    # Create EventArtist
    event_artist = concert_models.EventArtist(
        event_id=event_id,
        artist_id=candidate.matched_artist_id,
        position=candidate.position,
        raw_name=candidate.raw_name,
    )
    db.add(event_artist)

    candidate.status = types_module.CandidateStatus.ACCEPTED
    await db.commit()

    return {"status": "accepted", "candidate_id": str(candidate_id)}


@router.post(
    "/{event_id}/candidates/{candidate_id}/reject",
    summary="Reject a candidate match",
)
async def reject_candidate(
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    candidate = await _get_candidate(db, event_id, candidate_id)
    candidate.status = types_module.CandidateStatus.REJECTED
    await db.commit()
    return {"status": "rejected", "candidate_id": str(candidate_id)}


@router.post(
    "/{event_id}/candidates",
    summary="Create candidate from artist search",
    status_code=201,
)
async def create_candidate(
    event_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    artist_id: uuid.UUID = fastapi.Body(..., embed=True),
) -> dict[str, Any]:
    # Verify event exists
    event = (
        await db.execute(
            sa.select(concert_models.Event).where(concert_models.Event.id == event_id)
        )
    ).scalar_one_or_none()
    if event is None:
        raise fastapi.HTTPException(status_code=404, detail="Event not found")

    # Verify artist exists
    artist = (
        await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
    ).scalar_one_or_none()
    if artist is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    # Check for existing candidate with same raw_name
    existing = (
        await db.execute(
            sa.select(concert_models.EventArtistCandidate).where(
                concert_models.EventArtistCandidate.event_id == event_id,
                concert_models.EventArtistCandidate.raw_name == artist.name,
            )
        )
    ).scalar_one_or_none()
    if existing:
        raise fastapi.HTTPException(
            status_code=409, detail="Candidate already exists for this artist name"
        )

    candidate = concert_models.EventArtistCandidate(
        event_id=event_id,
        raw_name=artist.name,
        matched_artist_id=artist_id,
        position=0,
        confidence_score=100,
        status=types_module.CandidateStatus.PENDING,
    )
    db.add(candidate)
    await db.commit()

    return {
        "id": str(candidate.id),
        "raw_name": candidate.raw_name,
        "matched_artist_id": str(artist_id),
        "status": "PENDING",
    }


async def _get_candidate(
    db: sa_async.AsyncSession,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> concert_models.EventArtistCandidate:
    stmt = sa.select(concert_models.EventArtistCandidate).where(
        concert_models.EventArtistCandidate.id == candidate_id,
        concert_models.EventArtistCandidate.event_id == event_id,
    )
    candidate = (await db.execute(stmt)).scalar_one_or_none()
    if candidate is None:
        raise fastapi.HTTPException(status_code=404, detail="Candidate not found")
    return candidate
```

**Step 5: Create merge preview/confirm API module**

Create `src/resonance/api/v1/matching.py`:

```python
"""Entity merge API routes — preview and confirm merge operations."""
from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.dedup as dedup_module
import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.music as music_models

router = fastapi.APIRouter(prefix="/matching", tags=["matching"])


@router.post(
    "/artists/{artist_id}/merge/{other_id}",
    summary="Preview artist merge",
)
async def preview_artist_merge(
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    canonical = (
        await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
    ).scalar_one_or_none()
    duplicate = (
        await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == other_id)
        )
    ).scalar_one_or_none()

    if canonical is None or duplicate is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    # Count affected records
    track_count = (
        await db.execute(
            sa.select(sa.func.count())
            .select_from(music_models.Track)
            .where(music_models.Track.artist_id == other_id)
        )
    ).scalar_one()

    event_count = (
        await db.execute(
            sa.select(sa.func.count())
            .select_from(concert_models.EventArtist)
            .where(concert_models.EventArtist.artist_id == other_id)
        )
    ).scalar_one()

    listening_count = (
        await db.execute(
            sa.select(sa.func.count())
            .select_from(music_models.ListeningEvent)
            .join(music_models.Track)
            .where(music_models.Track.artist_id == other_id)
        )
    ).scalar_one()

    # Merge service_links preview
    merged_links = dict(canonical.service_links or {})
    for k, v in (duplicate.service_links or {}).items():
        if v and k not in merged_links:
            merged_links[k] = v

    return {
        "canonical": {"id": str(canonical.id), "name": canonical.name},
        "duplicate": {"id": str(duplicate.id), "name": duplicate.name},
        "tracks_to_repoint": track_count,
        "events_to_repoint": event_count,
        "listening_events_affected": listening_count,
        "merged_service_links": merged_links,
    }


@router.post(
    "/artists/{artist_id}/merge/{other_id}/confirm",
    summary="Execute artist merge",
)
async def confirm_artist_merge(
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    canonical = (
        await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
    ).scalar_one_or_none()
    duplicate = (
        await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == other_id)
        )
    ).scalar_one_or_none()

    if canonical is None or duplicate is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    stats = await dedup_module.merge_artists(db, canonical, duplicate)
    await db.commit()

    return {
        "merged": True,
        "canonical_id": str(artist_id),
        "tracks_repointed": stats.tracks_repointed,
        "events_repointed": stats.events_repointed,
    }


@router.post(
    "/tracks/{track_id}/merge/{other_id}/confirm",
    summary="Execute track merge",
)
async def confirm_track_merge(
    track_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    canonical = (
        await db.execute(
            sa.select(music_models.Track)
            .where(music_models.Track.id == track_id)
            .options(sa_orm.joinedload(music_models.Track.artist))
        )
    ).scalar_one_or_none()
    duplicate = (
        await db.execute(
            sa.select(music_models.Track)
            .where(music_models.Track.id == other_id)
            .options(sa_orm.joinedload(music_models.Track.artist))
        )
    ).scalar_one_or_none()

    if canonical is None or duplicate is None:
        raise fastapi.HTTPException(status_code=404, detail="Track not found")

    stats = await dedup_module.merge_tracks(db, canonical, duplicate)
    await db.commit()

    return {
        "merged": True,
        "canonical_id": str(track_id),
        "events_repointed": stats.events_repointed,
    }
```

**Step 6: Register matching router**

In `src/resonance/api/v1/__init__.py`, add:

```python
import resonance.api.v1.matching as matching_module
router.include_router(matching_module.router)
```

**Step 7: Run tests**

```bash
uv run pytest tests/test_api_matching.py -v
uv run ruff check src/resonance/api/v1/
uv run mypy src/resonance/api/v1/
```

**Step 8: Commit**

```bash
git add src/resonance/api/v1/events.py src/resonance/api/v1/artists.py src/resonance/api/v1/matching.py src/resonance/api/v1/__init__.py tests/test_api_matching.py
git commit -m "feat: add entity matching API endpoints for candidates and merge"
```

---

## Task 3: Make list page names clickable links to detail pages

Update existing list page partials to link entity names to their new detail pages.

**Files:**
- Modify: `src/resonance/templates/partials/artist_list.html`
- Modify: `src/resonance/templates/partials/track_list.html`
- Modify: `src/resonance/templates/partials/event_list.html`

**Step 1: Update artist_list.html**

Change the name cell from plain text to a link:

```html
<!-- old -->
<td>{{ artist.name }}</td>
<!-- new -->
<td><a href="/artists/{{ artist.id }}">{{ artist.name }}</a></td>
```

**Step 2: Update track_list.html**

Link track title and artist name:

```html
<!-- old -->
<td>{{ track.title }}</td>
<td>{{ track.artist.name }}</td>
<!-- new -->
<td><a href="/tracks/{{ track.id }}">{{ track.title }}</a></td>
<td><a href="/artists/{{ track.artist_id }}">{{ track.artist.name }}</a></td>
```

**Step 3: Update event_list.html**

Link event title to detail page (replace external link pattern):

```html
<!-- old: event title links to external_url -->
<!-- new: event title links to detail page, external link as separate icon -->
<td>
    <a href="/events/{{ event.id }}">{{ event.title }}</a>
    {% if event.external_url %}
    <a href="{{ event.external_url }}" target="_blank" rel="noopener" style="opacity: 0.5; margin-left: 0.25em;">&#x2197;</a>
    {% endif %}
</td>
```

Also link confirmed artist names to their detail pages. In the Artists column:

```html
<td>
    {% for ea in event.artists %}{{ ", " if not loop.first }}<a href="/artists/{{ ea.artist_id }}">{{ ea.raw_name }}</a>{% endfor %}
    {% if event.artists and event.artist_candidates %}, {% endif %}
    {% for candidate in event.artist_candidates %}<small style="opacity: 0.6">{{ ", " if not loop.first }}{{ candidate.raw_name }}</small>{% endfor %}
</td>
```

Note: `EventArtist` has `artist_id` as a direct column. Confirm this is eagerly loaded in the UI route query (it is — `joinedload(Event.artists)` loads full `EventArtist` objects).

**Step 4: Verify lint passes on templates**

Templates don't need lint, but verify the app starts:

```bash
uv run pytest tests/ -x -q
```

**Step 5: Commit**

```bash
git add src/resonance/templates/partials/artist_list.html src/resonance/templates/partials/track_list.html src/resonance/templates/partials/event_list.html
git commit -m "feat: make entity names clickable links to detail pages"
```

---

## Task 4: Entity detail pages — UI routes and templates

Three new detail pages following the `playlist_detail` pattern.

**Files:**
- Modify: `src/resonance/ui/routes.py` — add artist_detail, track_detail, event_detail routes
- Create: `src/resonance/templates/artist_detail.html`
- Create: `src/resonance/templates/track_detail.html`
- Create: `src/resonance/templates/event_detail.html`
- Create: `src/resonance/templates/partials/artist_tracks.html`
- Create: `src/resonance/templates/partials/artist_events.html`
- Create: `src/resonance/templates/partials/artist_candidates.html`
- Create: `src/resonance/templates/partials/event_candidates.html`
- Create: `src/resonance/templates/partials/event_confirmed_artists.html`
- Create: `src/resonance/templates/partials/track_history.html`

**Step 1: Add artist detail UI route**

In `src/resonance/ui/routes.py`, add after the `artists_page` route:

```python
@router.get("/artists/{artist_id}", response_model=None)
async def artist_detail_page(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    page: int = 1,
    section: str = "tracks",
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        # Load artist
        artist = (
            await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id == artist_id
                )
            )
        ).scalar_one_or_none()

        if artist is None:
            raise fastapi.HTTPException(status_code=404, detail="Artist not found")

        offset = (page - 1) * _PAGE_SIZE

        # Tracks by this artist (always load for header count)
        track_count_result = await db.execute(
            sa.select(sa.func.count())
            .select_from(music_models.Track)
            .where(music_models.Track.artist_id == artist_id)
        )
        track_count = track_count_result.scalar_one()

        tracks: list[music_models.Track] = []
        tracks_has_next = False
        if section == "tracks":
            result = await db.execute(
                sa.select(music_models.Track)
                .where(music_models.Track.artist_id == artist_id)
                .order_by(music_models.Track.title)
                .offset(offset)
                .limit(_PAGE_SIZE + 1)
            )
            tracks = list(result.scalars().all())
            tracks_has_next = len(tracks) > _PAGE_SIZE
            tracks = tracks[:_PAGE_SIZE]

        # Events linked via EventArtist
        events_result = await db.execute(
            sa.select(concert_models.Event)
            .join(concert_models.EventArtist)
            .where(concert_models.EventArtist.artist_id == artist_id)
            .options(sa_orm.joinedload(concert_models.Event.venue))
            .order_by(concert_models.Event.event_date.desc())
        )
        events = list(events_result.unique().scalars().all())

        # Pending candidates matched to this artist
        candidates_result = await db.execute(
            sa.select(concert_models.EventArtistCandidate)
            .where(
                concert_models.EventArtistCandidate.matched_artist_id == artist_id,
                concert_models.EventArtistCandidate.status
                == types_module.CandidateStatus.PENDING,
            )
            .options(
                sa_orm.joinedload(concert_models.EventArtistCandidate.event)
            )
        )
        candidates = list(candidates_result.unique().scalars().all())

        # Potential duplicates (case-insensitive name match, excluding self)
        dupes_result = await db.execute(
            sa.select(music_models.Artist)
            .where(
                sa.func.lower(music_models.Artist.name)
                == sa.func.lower(artist.name),
                music_models.Artist.id != artist_id,
            )
            .limit(10)
        )
        duplicates = list(dupes_result.scalars().all())

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "artist": artist,
        "tracks": tracks,
        "track_count": track_count,
        "tracks_has_next": tracks_has_next,
        "tracks_has_prev": page > 1,
        "events": events,
        "candidates": candidates,
        "duplicates": duplicates,
        "page": page,
    }

    if request.headers.get("HX-Request") and section == "tracks":
        return templates.TemplateResponse(
            request, "partials/artist_tracks.html", context
        )
    return templates.TemplateResponse(request, "artist_detail.html", context)
```

**Step 2: Add track detail UI route**

```python
@router.get("/tracks/{track_id}", response_model=None)
async def track_detail_page(
    request: fastapi.Request,
    track_id: uuid.UUID,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        track = (
            await db.execute(
                sa.select(music_models.Track)
                .where(music_models.Track.id == track_id)
                .options(sa_orm.joinedload(music_models.Track.artist))
            )
        ).unique().scalar_one_or_none()

        if track is None:
            raise fastapi.HTTPException(status_code=404, detail="Track not found")

        # Listening history for this track
        offset = (page - 1) * _PAGE_SIZE
        history_result = await db.execute(
            sa.select(music_models.ListeningEvent)
            .where(
                music_models.ListeningEvent.track_id == track_id,
                music_models.ListeningEvent.user_id == uuid.UUID(user_id),
            )
            .order_by(music_models.ListeningEvent.listened_at.desc())
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        history = list(history_result.scalars().all())
        history_has_next = len(history) > _PAGE_SIZE
        history = history[:_PAGE_SIZE]

        # Potential duplicates (same title, same artist)
        dupes_result = await db.execute(
            sa.select(music_models.Track)
            .where(
                sa.func.lower(music_models.Track.title)
                == sa.func.lower(track.title),
                music_models.Track.artist_id == track.artist_id,
                music_models.Track.id != track_id,
            )
            .options(sa_orm.joinedload(music_models.Track.artist))
            .limit(10)
        )
        duplicates = list(dupes_result.unique().scalars().all())

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "track": track,
        "history": history,
        "history_has_next": history_has_next,
        "history_has_prev": page > 1,
        "duplicates": duplicates,
        "page": page,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/track_history.html", context
        )
    return templates.TemplateResponse(request, "track_detail.html", context)
```

**Step 3: Add event detail UI route**

```python
@router.get("/events/{event_id}", response_model=None)
async def event_detail_page(
    request: fastapi.Request,
    event_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        event = (
            await db.execute(
                sa.select(concert_models.Event)
                .where(concert_models.Event.id == event_id)
                .options(
                    sa_orm.joinedload(concert_models.Event.venue),
                    sa_orm.joinedload(concert_models.Event.artists),
                    sa_orm.joinedload(concert_models.Event.artist_candidates),
                )
            )
        ).unique().scalar_one_or_none()

        if event is None:
            raise fastapi.HTTPException(status_code=404, detail="Event not found")

        # Load matched artist names for candidates that have a match
        matched_artist_ids = [
            c.matched_artist_id
            for c in event.artist_candidates
            if c.matched_artist_id is not None
        ]
        matched_artists: dict[uuid.UUID, music_models.Artist] = {}
        if matched_artist_ids:
            result = await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id.in_(matched_artist_ids)
                )
            )
            for a in result.scalars().all():
                matched_artists[a.id] = a

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "event": event,
        "matched_artists": matched_artists,
    }

    return templates.TemplateResponse(request, "event_detail.html", context)
```

**Step 4: Create artist_detail.html template**

Create `src/resonance/templates/artist_detail.html`:

```html
{% extends "base.html" %}
{% from "partials/service_badges.html" import service_badge_headers, service_badge_cells %}
{% block title %}{{ artist.name }} — resonance{% endblock %}
{% block content %}
<p><a href="/artists">&larr; Back to Artists</a></p>
<h2>{{ artist.name }}</h2>
{% if artist.origin %}<p><small>{{ artist.origin }}</small></p>{% endif %}

<table>
    <tbody>
        <tr>
            <th>Tracks</th>
            <td>{{ track_count }}</td>
        </tr>
        <tr>
            <th>Events</th>
            <td>{{ events | length }}</td>
        </tr>
        <tr>
            <th>Service Links</th>
            <td>
                {% if artist.service_links %}
                {% for svc, val in artist.service_links.items() %}
                {{ ", " if not loop.first }}{{ svc }}
                {% endfor %}
                {% else %}
                None
                {% endif %}
            </td>
        </tr>
        <tr>
            <th>Created</th>
            <td>{{ artist.created_at | localtime(user_tz) }}</td>
        </tr>
    </tbody>
</table>

{% if candidates %}
<h3>Pending Matches</h3>
<div id="artist-candidates">
    {% include "partials/artist_candidates.html" %}
</div>
{% endif %}

<h3>Tracks</h3>
<div id="artist-tracks">
    {% include "partials/artist_tracks.html" %}
</div>

{% if events %}
<h3>Events</h3>
{% include "partials/artist_events.html" %}
{% endif %}

{% if duplicates %}
<h3>Potential Duplicates</h3>
<table>
    <thead>
        <tr>
            <th>Name</th>
            {{ service_badge_headers() }}
            <th></th>
        </tr>
    </thead>
    <tbody>
        {% for dupe in duplicates %}
        <tr>
            <td><a href="/artists/{{ dupe.id }}">{{ dupe.name }}</a></td>
            {{ service_badge_cells(dupe.service_links, entity_type="artist", entity_name=dupe.name) }}
            <td><a href="/artists/{{ artist.id }}/compare/{{ dupe.id }}" role="button" class="outline secondary" style="padding: 0.25em 0.5em; font-size: 0.85em;">Compare</a></td>
        </tr>
        {% endfor %}
    </tbody>
</table>
{% endif %}
{% endblock %}
```

**Step 5: Create artist_tracks.html partial**

Create `src/resonance/templates/partials/artist_tracks.html`:

```html
{% from "partials/service_badges.html" import service_badge_headers, service_badge_cells %}
{% if tracks %}
<figure>
    <table>
        <thead>
            <tr>
                <th>Title</th>
                {{ service_badge_headers() }}
            </tr>
        </thead>
        <tbody>
            {% for track in tracks %}
            <tr>
                <td><a href="/tracks/{{ track.id }}">{{ track.title }}</a></td>
                {{ service_badge_cells(track.service_links, entity_type="track", entity_name=artist.name, track_title=track.title) }}
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
<nav>
    {% if tracks_has_prev %}
    <a href="/artists/{{ artist.id }}?page={{ page - 1 }}"
       hx-get="/artists/{{ artist.id }}?page={{ page - 1 }}"
       hx-target="#artist-tracks"
       hx-swap="innerHTML"
       role="button"
       class="secondary">Previous</a>
    {% endif %}
    {% if tracks_has_next %}
    <a href="/artists/{{ artist.id }}?page={{ page + 1 }}"
       hx-get="/artists/{{ artist.id }}?page={{ page + 1 }}"
       hx-target="#artist-tracks"
       hx-swap="innerHTML"
       role="button">Next</a>
    {% endif %}
</nav>
{% else %}
<p>No tracks for this artist.</p>
{% endif %}
```

**Step 6: Create artist_events.html partial**

Create `src/resonance/templates/partials/artist_events.html`:

```html
<figure>
    <table>
        <thead>
            <tr>
                <th>Date</th>
                <th>Event</th>
                <th>Venue</th>
            </tr>
        </thead>
        <tbody>
            {% for event in events %}
            <tr>
                <td>{{ event.event_date }}</td>
                <td><a href="/events/{{ event.id }}">{{ event.title }}</a></td>
                <td>{{ event.venue.name if event.venue else "" }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
```

**Step 7: Create artist_candidates.html partial**

Create `src/resonance/templates/partials/artist_candidates.html`:

```html
<figure>
    <table>
        <thead>
            <tr>
                <th>Event</th>
                <th>Raw Name</th>
                <th>Confidence</th>
                <th></th>
            </tr>
        </thead>
        <tbody>
            {% for candidate in candidates %}
            <tr id="candidate-{{ candidate.id }}">
                <td><a href="/events/{{ candidate.event_id }}">{{ candidate.event.title }}</a></td>
                <td>{{ candidate.raw_name }}</td>
                <td>{{ candidate.confidence_score }}%</td>
                <td>
                    <div class="grid" style="gap: 0.25em;">
                        <button
                            hx-post="/api/v1/events/{{ candidate.event_id }}/candidates/{{ candidate.id }}/accept"
                            hx-target="#candidate-{{ candidate.id }}"
                            hx-swap="outerHTML"
                            class="outline"
                            style="padding: 0.25em 0.5em; font-size: 0.85em;">Accept</button>
                        <button
                            hx-post="/api/v1/events/{{ candidate.event_id }}/candidates/{{ candidate.id }}/reject"
                            hx-target="#candidate-{{ candidate.id }}"
                            hx-swap="outerHTML"
                            class="outline secondary"
                            style="padding: 0.25em 0.5em; font-size: 0.85em;">Reject</button>
                    </div>
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
```

**Step 8: Create track_detail.html template**

Create `src/resonance/templates/track_detail.html`:

```html
{% extends "base.html" %}
{% from "partials/service_badges.html" import service_badge_headers, service_badge_cells %}
{% block title %}{{ track.title }} — resonance{% endblock %}
{% block content %}
<p><a href="/tracks">&larr; Back to Tracks</a></p>
<h2>{{ track.title }}</h2>
<p>by <a href="/artists/{{ track.artist.id }}">{{ track.artist.name }}</a></p>

<table>
    <tbody>
        {% if track.duration_ms %}
        <tr>
            <th>Duration</th>
            <td>{{ "%d:%02d" | format(track.duration_ms // 60000, (track.duration_ms % 60000) // 1000) }}</td>
        </tr>
        {% endif %}
        <tr>
            <th>Service Links</th>
            <td>
                {% if track.service_links %}
                {% for svc, val in track.service_links.items() %}
                {{ ", " if not loop.first }}{{ svc }}
                {% endfor %}
                {% else %}
                None
                {% endif %}
            </td>
        </tr>
        <tr>
            <th>Created</th>
            <td>{{ track.created_at | localtime(user_tz) }}</td>
        </tr>
    </tbody>
</table>

<h3>Listening History</h3>
<div id="track-history">
    {% include "partials/track_history.html" %}
</div>

{% if duplicates %}
<h3>Potential Duplicates</h3>
<table>
    <thead>
        <tr>
            <th>Title</th>
            <th>Artist</th>
            {{ service_badge_headers() }}
            <th></th>
        </tr>
    </thead>
    <tbody>
        {% for dupe in duplicates %}
        <tr>
            <td><a href="/tracks/{{ dupe.id }}">{{ dupe.title }}</a></td>
            <td>{{ dupe.artist.name }}</td>
            {{ service_badge_cells(dupe.service_links, entity_type="track", entity_name=dupe.artist.name, track_title=dupe.title) }}
            <td><a href="/tracks/{{ track.id }}/compare/{{ dupe.id }}" role="button" class="outline secondary" style="padding: 0.25em 0.5em; font-size: 0.85em;">Compare</a></td>
        </tr>
        {% endfor %}
    </tbody>
</table>
{% endif %}
{% endblock %}
```

**Step 9: Create track_history.html partial**

Create `src/resonance/templates/partials/track_history.html`:

```html
{% if history %}
<figure>
    <table>
        <thead>
            <tr>
                <th>Listened At</th>
                <th>Source</th>
            </tr>
        </thead>
        <tbody>
            {% for le in history %}
            <tr>
                <td>{{ le.listened_at | localtime(user_tz) }}</td>
                <td>{{ le.source_service.value }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
<nav>
    {% if history_has_prev %}
    <a href="/tracks/{{ track.id }}?page={{ page - 1 }}"
       hx-get="/tracks/{{ track.id }}?page={{ page - 1 }}"
       hx-target="#track-history"
       hx-swap="innerHTML"
       role="button"
       class="secondary">Previous</a>
    {% endif %}
    {% if history_has_next %}
    <a href="/tracks/{{ track.id }}?page={{ page + 1 }}"
       hx-get="/tracks/{{ track.id }}?page={{ page + 1 }}"
       hx-target="#track-history"
       hx-swap="innerHTML"
       role="button">Next</a>
    {% endif %}
</nav>
{% else %}
<p>No listening history for this track.</p>
{% endif %}
```

**Step 10: Create event_detail.html template**

Create `src/resonance/templates/event_detail.html`:

```html
{% extends "base.html" %}
{% block title %}{{ event.title }} — resonance{% endblock %}
{% block content %}
<p><a href="/events">&larr; Back to Events</a></p>
<h2>{{ event.title }}</h2>

<table>
    <tbody>
        <tr>
            <th>Date</th>
            <td>{{ event.event_date }}</td>
        </tr>
        {% if event.venue %}
        <tr>
            <th>Venue</th>
            <td>{{ event.venue.name }}{% if event.venue.city %}, {{ event.venue.city }}{% endif %}{% if event.venue.state %}, {{ event.venue.state }}{% endif %}</td>
        </tr>
        {% endif %}
        {% if event.external_url %}
        <tr>
            <th>Link</th>
            <td><a href="{{ event.external_url }}" target="_blank" rel="noopener">{{ event.source_service.value }} &#x2197;</a></td>
        </tr>
        {% endif %}
    </tbody>
</table>

{% if event.artists %}
<h3>Confirmed Artists</h3>
{% include "partials/event_confirmed_artists.html" %}
{% endif %}

<h3>Pending Candidates</h3>
<div id="event-candidates">
    {% include "partials/event_candidates.html" %}
</div>

<h3>Add Artist</h3>
<div>
    <input type="search"
           name="q"
           placeholder="Search artists..."
           hx-get="/api/v1/artists/search"
           hx-trigger="input changed delay:300ms"
           hx-target="#artist-search-results"
           hx-swap="innerHTML"
           hx-include="this">
    <div id="artist-search-results"></div>
</div>
{% endblock %}
```

**Step 11: Create event_confirmed_artists.html partial**

Create `src/resonance/templates/partials/event_confirmed_artists.html`:

```html
<figure>
    <table>
        <thead>
            <tr>
                <th>#</th>
                <th>Artist</th>
            </tr>
        </thead>
        <tbody>
            {% for ea in event.artists | sort(attribute="position") %}
            <tr>
                <td>{{ ea.position }}</td>
                <td><a href="/artists/{{ ea.artist_id }}">{{ ea.raw_name }}</a></td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
```

**Step 12: Create event_candidates.html partial**

Create `src/resonance/templates/partials/event_candidates.html`:

```html
{% set pending = event.artist_candidates | selectattr("status.value", "equalto", "pending") | list %}
{% set resolved = event.artist_candidates | rejectattr("status.value", "equalto", "pending") | list %}

{% if pending %}
<figure>
    <table>
        <thead>
            <tr>
                <th>Raw Name</th>
                <th>Matched Artist</th>
                <th>Confidence</th>
                <th></th>
            </tr>
        </thead>
        <tbody>
            {% for c in pending | sort(attribute="position") %}
            <tr id="event-candidate-{{ c.id }}">
                <td>{{ c.raw_name }}</td>
                <td>
                    {% if c.matched_artist_id and matched_artists.get(c.matched_artist_id) %}
                    <a href="/artists/{{ c.matched_artist_id }}">{{ matched_artists[c.matched_artist_id].name }}</a>
                    {% else %}
                    <small style="opacity: 0.6">No match</small>
                    {% endif %}
                </td>
                <td>{{ c.confidence_score }}%</td>
                <td>
                    {% if c.matched_artist_id %}
                    <div class="grid" style="gap: 0.25em;">
                        <button
                            hx-post="/api/v1/events/{{ event.id }}/candidates/{{ c.id }}/accept"
                            hx-target="#event-candidate-{{ c.id }}"
                            hx-swap="outerHTML"
                            class="outline"
                            style="padding: 0.25em 0.5em; font-size: 0.85em;">Accept</button>
                        <button
                            hx-post="/api/v1/events/{{ event.id }}/candidates/{{ c.id }}/reject"
                            hx-target="#event-candidate-{{ c.id }}"
                            hx-swap="outerHTML"
                            class="outline secondary"
                            style="padding: 0.25em 0.5em; font-size: 0.85em;">Reject</button>
                    </div>
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
{% else %}
<p>No pending candidates.</p>
{% endif %}

{% if resolved %}
<details>
    <summary>Resolved candidates ({{ resolved | length }})</summary>
    <table>
        <thead>
            <tr>
                <th>Raw Name</th>
                <th>Status</th>
            </tr>
        </thead>
        <tbody>
            {% for c in resolved %}
            <tr>
                <td>{{ c.raw_name }}</td>
                <td>{{ c.status.value }}</td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</details>
{% endif %}
```

**Step 13: Handle artist search results for "Add Artist" on event detail**

The artist search API returns JSON. We need an HTMX-compatible partial that renders search results as clickable items. The search endpoint currently returns JSON — we need a UI partial endpoint.

Add to `src/resonance/ui/routes.py`:

```python
@router.get("/partials/artist-search", response_model=None)
async def artist_search_partial(
    request: fastapi.Request,
    q: str = "",
    event_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    if len(q) < 2:
        return fastapi.responses.HTMLResponse("")

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(music_models.Artist)
            .where(music_models.Artist.name.ilike(f"%{q}%"))
            .order_by(music_models.Artist.name)
            .limit(10)
        )
        artists = list(result.scalars().all())

    return templates.TemplateResponse(
        request,
        "partials/artist_search_results.html",
        {"artists": artists, "event_id": event_id},
    )
```

Create `src/resonance/templates/partials/artist_search_results.html`:

```html
{% if artists %}
<ul style="list-style: none; padding: 0; margin: 0;">
    {% for artist in artists %}
    <li style="padding: 0.25em 0;">
        {% if event_id %}
        <button
            hx-post="/api/v1/events/{{ event_id }}/candidates"
            hx-vals='{"artist_id": "{{ artist.id }}"}'
            hx-target="#event-candidates"
            hx-swap="innerHTML"
            class="outline"
            style="padding: 0.25em 0.5em; font-size: 0.85em; width: auto;">
            + {{ artist.name }}
        </button>
        {% else %}
        <a href="/artists/{{ artist.id }}">{{ artist.name }}</a>
        {% endif %}
    </li>
    {% endfor %}
</ul>
{% elif q %}
<p><small>No artists found.</small></p>
{% endif %}
```

Update the event_detail.html search input to use the UI partial instead of the API directly:

```html
<input type="search"
       name="q"
       placeholder="Search artists..."
       hx-get="/partials/artist-search"
       hx-trigger="input changed delay:300ms"
       hx-target="#artist-search-results"
       hx-swap="innerHTML"
       hx-vals='{"event_id": "{{ event.id }}"}'>
```

**Step 14: Run lint, type checks, and tests**

```bash
uv run ruff check src/resonance/ui/routes.py
uv run mypy src/resonance/ui/routes.py
uv run pytest tests/ -x -q
```

**Step 15: Commit**

```bash
git add src/resonance/ui/routes.py \
    src/resonance/templates/artist_detail.html \
    src/resonance/templates/track_detail.html \
    src/resonance/templates/event_detail.html \
    src/resonance/templates/partials/artist_tracks.html \
    src/resonance/templates/partials/artist_events.html \
    src/resonance/templates/partials/artist_candidates.html \
    src/resonance/templates/partials/event_candidates.html \
    src/resonance/templates/partials/event_confirmed_artists.html \
    src/resonance/templates/partials/track_history.html \
    src/resonance/templates/partials/artist_search_results.html
git commit -m "feat: add entity detail pages for artists, tracks, and events"
```

---

## Task 5: Comparison and merge views

Side-by-side comparison pages for artists and tracks with merge preview and confirm.

**Files:**
- Modify: `src/resonance/ui/routes.py` — add compare routes
- Create: `src/resonance/templates/artist_compare.html`
- Create: `src/resonance/templates/track_compare.html`
- Create: `src/resonance/templates/partials/merge_preview.html`

**Step 1: Add artist comparison UI route**

In `src/resonance/ui/routes.py`:

```python
@router.get("/artists/{artist_id}/compare/{other_id}", response_model=None)
async def artist_compare_page(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    import resonance.dedup as dedup_module

    async with _get_db(request) as db:
        artist_a = (
            await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id == artist_id
                )
            )
        ).scalar_one_or_none()
        artist_b = (
            await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id == other_id
                )
            )
        ).scalar_one_or_none()

        if artist_a is None or artist_b is None:
            raise fastapi.HTTPException(status_code=404, detail="Artist not found")

        # Use _pick_canonical to pre-select
        canonical, duplicate = dedup_module._pick_canonical(artist_a, artist_b)

        # Get counts for each
        a_track_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(music_models.Track)
                .where(music_models.Track.artist_id == artist_a.id)
            )
        ).scalar_one()
        b_track_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(music_models.Track)
                .where(music_models.Track.artist_id == artist_b.id)
            )
        ).scalar_one()

        a_event_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(concert_models.EventArtist)
                .where(concert_models.EventArtist.artist_id == artist_a.id)
            )
        ).scalar_one()
        b_event_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(concert_models.EventArtist)
                .where(concert_models.EventArtist.artist_id == artist_b.id)
            )
        ).scalar_one()

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "artist_a": artist_a,
        "artist_b": artist_b,
        "canonical": canonical,
        "duplicate": duplicate,
        "a_track_count": a_track_count,
        "b_track_count": b_track_count,
        "a_event_count": a_event_count,
        "b_event_count": b_event_count,
    }

    return templates.TemplateResponse(request, "artist_compare.html", context)
```

**Step 2: Add track comparison UI route**

```python
@router.get("/tracks/{track_id}/compare/{other_id}", response_model=None)
async def track_compare_page(
    request: fastapi.Request,
    track_id: uuid.UUID,
    other_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    import resonance.dedup as dedup_module

    async with _get_db(request) as db:
        track_a = (
            await db.execute(
                sa.select(music_models.Track)
                .where(music_models.Track.id == track_id)
                .options(sa_orm.joinedload(music_models.Track.artist))
            )
        ).unique().scalar_one_or_none()
        track_b = (
            await db.execute(
                sa.select(music_models.Track)
                .where(music_models.Track.id == other_id)
                .options(sa_orm.joinedload(music_models.Track.artist))
            )
        ).unique().scalar_one_or_none()

        if track_a is None or track_b is None:
            raise fastapi.HTTPException(status_code=404, detail="Track not found")

        canonical, duplicate = dedup_module._pick_canonical_track(track_a, track_b)

        # Listening event counts
        a_listen_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(music_models.ListeningEvent)
                .where(music_models.ListeningEvent.track_id == track_a.id)
            )
        ).scalar_one()
        b_listen_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(music_models.ListeningEvent)
                .where(music_models.ListeningEvent.track_id == track_b.id)
            )
        ).scalar_one()

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "track_a": track_a,
        "track_b": track_b,
        "canonical": canonical,
        "duplicate": duplicate,
        "a_listen_count": a_listen_count,
        "b_listen_count": b_listen_count,
    }

    return templates.TemplateResponse(request, "track_compare.html", context)
```

**Step 3: Create artist_compare.html template**

Create `src/resonance/templates/artist_compare.html`:

```html
{% extends "base.html" %}
{% block title %}Compare Artists — resonance{% endblock %}
{% block content %}
<p><a href="/artists/{{ artist_a.id }}">&larr; Back to {{ artist_a.name }}</a></p>
<h2>Compare Artists</h2>

<div class="grid">
    <article{% if canonical.id == artist_a.id %} style="border-color: var(--pico-primary);"{% endif %}>
        <header>
            {% if canonical.id == artist_a.id %}<strong>Keep</strong>{% else %}<strong>Merge into other</strong>{% endif %}
        </header>
        <h3>{{ artist_a.name }}</h3>
        {% if artist_a.origin %}<p><small>{{ artist_a.origin }}</small></p>{% endif %}
        <table>
            <tbody>
                <tr><th>Tracks</th><td>{{ a_track_count }}</td></tr>
                <tr><th>Events</th><td>{{ a_event_count }}</td></tr>
                <tr><th>Services</th><td>{{ (artist_a.service_links or {}).keys() | join(", ") or "None" }}</td></tr>
                <tr><th>Created</th><td>{{ artist_a.created_at | localtime(user_tz) }}</td></tr>
            </tbody>
        </table>
    </article>

    <article{% if canonical.id == artist_b.id %} style="border-color: var(--pico-primary);"{% endif %}>
        <header>
            {% if canonical.id == artist_b.id %}<strong>Keep</strong>{% else %}<strong>Merge into other</strong>{% endif %}
        </header>
        <h3>{{ artist_b.name }}</h3>
        {% if artist_b.origin %}<p><small>{{ artist_b.origin }}</small></p>{% endif %}
        <table>
            <tbody>
                <tr><th>Tracks</th><td>{{ b_track_count }}</td></tr>
                <tr><th>Events</th><td>{{ b_event_count }}</td></tr>
                <tr><th>Services</th><td>{{ (artist_b.service_links or {}).keys() | join(", ") or "None" }}</td></tr>
                <tr><th>Created</th><td>{{ artist_b.created_at | localtime(user_tz) }}</td></tr>
            </tbody>
        </table>
    </article>
</div>

<p><small>
    <a href="/artists/{{ artist_b.id }}/compare/{{ artist_a.id }}">Swap canonical selection</a>
</small></p>

<div id="merge-preview">
    <button
        hx-post="/api/v1/matching/artists/{{ canonical.id }}/merge/{{ duplicate.id }}"
        hx-target="#merge-preview"
        hx-swap="innerHTML"
        class="outline">Preview Merge</button>
</div>
{% endblock %}
```

**Step 4: Create merge_preview.html partial**

Create `src/resonance/templates/partials/merge_preview.html`:

This partial is returned as JSON from the API, so HTMX needs to render it. Instead, we'll create a UI route that returns HTML.

Add to `src/resonance/ui/routes.py`:

```python
@router.post("/artists/{artist_id}/merge-preview/{other_id}", response_model=None)
async def artist_merge_preview(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        canonical = (
            await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id == artist_id
                )
            )
        ).scalar_one_or_none()
        duplicate = (
            await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id == other_id
                )
            )
        ).scalar_one_or_none()

        if canonical is None or duplicate is None:
            raise fastapi.HTTPException(status_code=404, detail="Artist not found")

        track_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(music_models.Track)
                .where(music_models.Track.artist_id == other_id)
            )
        ).scalar_one()

        event_count = (
            await db.execute(
                sa.select(sa.func.count())
                .select_from(concert_models.EventArtist)
                .where(concert_models.EventArtist.artist_id == other_id)
            )
        ).scalar_one()

    context = {
        "canonical": canonical,
        "duplicate": duplicate,
        "tracks_to_repoint": track_count,
        "events_to_repoint": event_count,
    }

    return templates.TemplateResponse(
        request, "partials/merge_preview.html", context
    )
```

Create `src/resonance/templates/partials/merge_preview.html`:

```html
<article>
    <header>Merge Preview</header>
    <p>Merging <strong>{{ duplicate.name }}</strong> into <strong>{{ canonical.name }}</strong>:</p>
    <ul>
        <li>{{ tracks_to_repoint }} track(s) will be repointed</li>
        <li>{{ events_to_repoint }} event link(s) will be repointed</li>
        <li>Service links will be combined</li>
        <li><strong>{{ duplicate.name }}</strong> will be deleted</li>
    </ul>
    <div class="grid">
        <button
            hx-post="/api/v1/matching/artists/{{ canonical.id }}/merge/{{ duplicate.id }}/confirm"
            hx-target="body"
            hx-swap="none"
            hx-on::after-request="if(event.detail.successful) window.location='/artists/{{ canonical.id }}'">Confirm Merge</button>
        <button class="secondary outline" onclick="document.getElementById('merge-preview').innerHTML='<button hx-post=\'/artists/{{ canonical.id }}/merge-preview/{{ duplicate.id }}\' hx-target=\'#merge-preview\' hx-swap=\'innerHTML\' class=\'outline\'>Preview Merge</button>'">Cancel</button>
    </div>
</article>
```

Update `artist_compare.html` to call the UI preview route instead of the API:

```html
<button
    hx-post="/artists/{{ canonical.id }}/merge-preview/{{ duplicate.id }}"
    hx-target="#merge-preview"
    hx-swap="innerHTML"
    class="outline">Preview Merge</button>
```

**Step 5: Create track_compare.html template**

Create `src/resonance/templates/track_compare.html`:

```html
{% extends "base.html" %}
{% block title %}Compare Tracks — resonance{% endblock %}
{% block content %}
<p><a href="/tracks/{{ track_a.id }}">&larr; Back to {{ track_a.title }}</a></p>
<h2>Compare Tracks</h2>

<div class="grid">
    <article{% if canonical.id == track_a.id %} style="border-color: var(--pico-primary);"{% endif %}>
        <header>
            {% if canonical.id == track_a.id %}<strong>Keep</strong>{% else %}<strong>Merge into other</strong>{% endif %}
        </header>
        <h3>{{ track_a.title }}</h3>
        <p>by {{ track_a.artist.name }}</p>
        <table>
            <tbody>
                <tr><th>Duration</th><td>{% if track_a.duration_ms %}{{ "%d:%02d" | format(track_a.duration_ms // 60000, (track_a.duration_ms % 60000) // 1000) }}{% else %}—{% endif %}</td></tr>
                <tr><th>Listens</th><td>{{ a_listen_count }}</td></tr>
                <tr><th>Services</th><td>{{ (track_a.service_links or {}).keys() | join(", ") or "None" }}</td></tr>
                <tr><th>Created</th><td>{{ track_a.created_at | localtime(user_tz) }}</td></tr>
            </tbody>
        </table>
    </article>

    <article{% if canonical.id == track_b.id %} style="border-color: var(--pico-primary);"{% endif %}>
        <header>
            {% if canonical.id == track_b.id %}<strong>Keep</strong>{% else %}<strong>Merge into other</strong>{% endif %}
        </header>
        <h3>{{ track_b.title }}</h3>
        <p>by {{ track_b.artist.name }}</p>
        <table>
            <tbody>
                <tr><th>Duration</th><td>{% if track_b.duration_ms %}{{ "%d:%02d" | format(track_b.duration_ms // 60000, (track_b.duration_ms % 60000) // 1000) }}{% else %}—{% endif %}</td></tr>
                <tr><th>Listens</th><td>{{ b_listen_count }}</td></tr>
                <tr><th>Services</th><td>{{ (track_b.service_links or {}).keys() | join(", ") or "None" }}</td></tr>
                <tr><th>Created</th><td>{{ track_b.created_at | localtime(user_tz) }}</td></tr>
            </tbody>
        </table>
    </article>
</div>

<p><small>
    <a href="/tracks/{{ track_b.id }}/compare/{{ track_a.id }}">Swap canonical selection</a>
</small></p>

<div class="grid">
    <button
        hx-post="/api/v1/matching/tracks/{{ canonical.id }}/merge/{{ duplicate.id }}/confirm"
        hx-target="body"
        hx-swap="none"
        hx-on::after-request="if(event.detail.successful) window.location='/tracks/{{ canonical.id }}'">Confirm Merge</button>
    <a href="/tracks/{{ track_a.id }}" role="button" class="secondary outline">Cancel</a>
</div>
{% endblock %}
```

Note: Track comparison does not need a preview step (tracks are simpler entities) — the comparison page itself shows the counts, and merge is a direct confirm. If you want preview parity with artists, use the same pattern. For v1, direct confirm with the side-by-side view is sufficient.

**Step 6: Run lint, type checks, and tests**

```bash
uv run ruff check src/resonance/ui/routes.py
uv run mypy src/resonance/ui/routes.py
uv run pytest tests/ -x -q
```

**Step 7: Commit**

```bash
git add src/resonance/ui/routes.py \
    src/resonance/templates/artist_compare.html \
    src/resonance/templates/track_compare.html \
    src/resonance/templates/partials/merge_preview.html
git commit -m "feat: add comparison and merge views for artists and tracks"
```

---

## Task 6: HTMX response handling for accept/reject actions

The accept/reject API endpoints currently return JSON, but the HTMX buttons expect HTML fragments to replace the candidate row. Update the API endpoints to return HTML when called via HTMX, or add UI routes that wrap the API logic and return HTML partials.

**Files:**
- Modify: `src/resonance/ui/routes.py` — add accept/reject UI action routes
- Create: `src/resonance/templates/partials/candidate_accepted.html`
- Create: `src/resonance/templates/partials/candidate_rejected.html`

**Step 1: Add UI action routes for accept/reject**

In `src/resonance/ui/routes.py`:

```python
@router.post("/events/{event_id}/candidates/{candidate_id}/accept", response_model=None)
async def accept_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        candidate = (
            await db.execute(
                sa.select(concert_models.EventArtistCandidate).where(
                    concert_models.EventArtistCandidate.id == candidate_id,
                    concert_models.EventArtistCandidate.event_id == event_id,
                )
            )
        ).scalar_one_or_none()

        if candidate is None:
            raise fastapi.HTTPException(status_code=404, detail="Candidate not found")

        if candidate.matched_artist_id is None:
            raise fastapi.HTTPException(
                status_code=400, detail="Candidate has no matched artist"
            )

        event_artist = concert_models.EventArtist(
            event_id=event_id,
            artist_id=candidate.matched_artist_id,
            position=candidate.position,
            raw_name=candidate.raw_name,
        )
        db.add(event_artist)
        candidate.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()

    return templates.TemplateResponse(
        request,
        "partials/candidate_accepted.html",
        {"candidate": candidate},
    )


@router.post("/events/{event_id}/candidates/{candidate_id}/reject", response_model=None)
async def reject_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        candidate = (
            await db.execute(
                sa.select(concert_models.EventArtistCandidate).where(
                    concert_models.EventArtistCandidate.id == candidate_id,
                    concert_models.EventArtistCandidate.event_id == event_id,
                )
            )
        ).scalar_one_or_none()

        if candidate is None:
            raise fastapi.HTTPException(status_code=404, detail="Candidate not found")

        candidate.status = types_module.CandidateStatus.REJECTED
        await db.commit()

    return templates.TemplateResponse(
        request,
        "partials/candidate_rejected.html",
        {"candidate": candidate},
    )
```

**Step 2: Create response partials**

Create `src/resonance/templates/partials/candidate_accepted.html`:

```html
<tr style="opacity: 0.5;">
    <td>{{ candidate.raw_name }}</td>
    <td colspan="3"><small>Accepted &#x2714;</small></td>
</tr>
```

Create `src/resonance/templates/partials/candidate_rejected.html`:

```html
<tr style="opacity: 0.3; text-decoration: line-through;">
    <td>{{ candidate.raw_name }}</td>
    <td colspan="3"><small>Rejected</small></td>
</tr>
```

**Step 3: Update HTMX targets in templates to use UI routes instead of API**

In `partials/event_candidates.html` and `partials/artist_candidates.html`, change the `hx-post` URLs:
- From: `/api/v1/events/...`
- To: `/events/...` (UI routes)

**Step 4: Run tests and lint**

```bash
uv run ruff check src/resonance/ui/routes.py
uv run mypy src/resonance/ui/routes.py
uv run pytest tests/ -x -q
```

**Step 5: Commit**

```bash
git add src/resonance/ui/routes.py \
    src/resonance/templates/partials/candidate_accepted.html \
    src/resonance/templates/partials/candidate_rejected.html \
    src/resonance/templates/partials/event_candidates.html \
    src/resonance/templates/partials/artist_candidates.html
git commit -m "feat: add HTMX accept/reject UI actions for candidates"
```

---

## Task 7: Integration testing and final verification

**Files:**
- Modify: `tests/test_api_matching.py` — expand tests
- Run: full test suite, lint, mypy

**Step 1: Add integration-style tests for artist search**

Extend `tests/test_api_matching.py` to test the search endpoint against a real (test) DB if available, or verify the response shape with the fake DB approach.

**Step 2: Run full test suite**

```bash
uv run pytest tests/ -v
```

**Step 3: Run full lint and type check**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
```

**Step 4: Verify app starts locally**

```bash
uv run uvicorn resonance.app:create_app --factory --host 0.0.0.0 --port 8000
```

Visit detail pages, test accept/reject buttons, test comparison views. Verify all navigation links work.

**Step 5: Final commit (if any fixes needed)**

```bash
git add -p  # stage specific fixes
git commit -m "fix: address integration test and lint findings"
```

---

## Commit Organization Summary

| Commit | Scope |
|--------|-------|
| 1 | `origin` field + migration |
| 2 | API endpoints (search, accept/reject, merge preview/confirm) |
| 3 | Clickable links in list pages |
| 4 | Detail page routes + templates |
| 5 | Comparison/merge views |
| 6 | HTMX accept/reject UI actions |
| 7 | Integration tests + fixes |
