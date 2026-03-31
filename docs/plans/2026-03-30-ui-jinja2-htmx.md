# UI Layer with Jinja2 + HTMX

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a server-rendered UI so users can log in via Spotify, trigger data syncs, and browse their followed artists, saved tracks, and listening history — all from a browser.

**Architecture:** Jinja2 templates served by FastAPI UI routes, styled with Pico CSS (system theme detection, no custom CSS), interactive via HTMX (partial page updates, sync polling). The UI routes are a thin layer over the same database/session infrastructure the API uses. No build step, no custom JavaScript.

**Tech Stack:** Jinja2, Pico CSS (CDN), HTMX (CDN), FastAPI TemplateResponse

---

### Task 1: Jinja2 Setup + Base Template

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/resonance/app.py`
- Create: `src/resonance/templates/base.html`
- Create: `src/resonance/templates/login.html`
- Test: `tests/test_ui.py`

**Step 1: Add Jinja2 dependency**

Add `"jinja2>=3.1"` to `dependencies` in `pyproject.toml`.

Run: `uv sync --all-extras`

**Step 2: Write the failing test**

Create `tests/test_ui.py`:

```python
from collections.abc import AsyncIterator

import httpx
import pytest

import resonance.app as app_module


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestLoginPage:
    async def test_login_page_returns_html(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/login")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    async def test_login_page_contains_spotify_link(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/login")
        assert "/api/v1/auth/spotify" in response.text

    async def test_unauthenticated_root_redirects_to_login(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
```

**Step 3: Run tests to verify they fail**

Run: `uv run pytest tests/test_ui.py -v`
Expected: FAIL — 404 (routes not registered).

**Step 4: Create base template**

Create `src/resonance/templates/base.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{% block title %}Resonance{% endblock %}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css">
    <script src="https://unpkg.com/htmx.org@2.0.4"></script>
    {% block head %}{% endblock %}
</head>
<body>
    {% if user_id %}
    <nav class="container">
        <ul>
            <li><strong>Resonance</strong></li>
        </ul>
        <ul>
            <li><a href="/">Dashboard</a></li>
            <li><a href="/artists">Artists</a></li>
            <li><a href="/tracks">Tracks</a></li>
            <li><a href="/history">History</a></li>
            <li><a href="/account">Account</a></li>
        </ul>
    </nav>
    {% endif %}
    <main class="container">
        {% block content %}{% endblock %}
    </main>
</body>
</html>
```

**Step 5: Create login template**

Create `src/resonance/templates/login.html`:

```html
{% extends "base.html" %}

{% block title %}Login — Resonance{% endblock %}

{% block content %}
<article>
    <header>
        <h1>Resonance</h1>
        <p>Personal media discovery and playlist generation</p>
    </header>
    <a href="/api/v1/auth/spotify" role="button">Connect with Spotify</a>
</article>
{% endblock %}
```

**Step 6: Create UI routes module**

Create `src/resonance/ui/__init__.py` (empty file).

Create `src/resonance/ui/routes.py`:

```python
"""UI routes — server-rendered pages via Jinja2 templates."""

from __future__ import annotations

import pathlib

import fastapi
import fastapi.responses as fastapi_responses
import fastapi.templating as fastapi_templating

import resonance.dependencies as deps_module
import resonance.middleware.session as session_module

router = fastapi.APIRouter(tags=["ui"])

_templates_dir = pathlib.Path(__file__).parent.parent / "templates"
templates = fastapi_templating.Jinja2Templates(directory=str(_templates_dir))


@router.get("/login")
async def login_page(request: fastapi.Request) -> fastapi_responses.HTMLResponse:
    """Render the login page."""
    return templates.TemplateResponse(request, "login.html", {"user_id": None})


@router.get("/")
async def dashboard(
    request: fastapi.Request,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
) -> fastapi_responses.HTMLResponse | fastapi_responses.RedirectResponse:
    """Render the dashboard, or redirect to login if not authenticated."""
    user_id = session.get("user_id")
    if user_id is None:
        return fastapi_responses.RedirectResponse(url="/login", status_code=307)
    return templates.TemplateResponse(
        request, "dashboard.html", {"user_id": user_id}
    )
```

Note: The dashboard template doesn't exist yet — this will cause a 500 if an authenticated user hits `/`. That's fine; Task 5 creates the dashboard template. For now, the tests only check login and unauthenticated redirect.

Actually, create a minimal placeholder `src/resonance/templates/dashboard.html` so tests don't error:

```html
{% extends "base.html" %}
{% block title %}Dashboard — Resonance{% endblock %}
{% block content %}
<h1>Dashboard</h1>
<p>Coming soon.</p>
{% endblock %}
```

**Step 7: Register UI routes in app factory**

Add to `src/resonance/app.py` after the API router registration:

```python
import resonance.ui.routes as ui_routes_module
application.include_router(ui_routes_module.router)
```

**Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/test_ui.py -v`
Expected: PASS

**Step 9: Run full suite + lint + type check**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 10: Commit**

```bash
git add pyproject.toml uv.lock src/resonance/templates/ src/resonance/ui/ src/resonance/app.py tests/test_ui.py
git commit -m "feat: add Jinja2 templating with base layout, login page, and UI routes"
```

---

### Task 2: Auth Flow Redirects

**Files:**
- Modify: `src/resonance/api/v1/auth.py`
- Modify: `tests/test_api_auth.py`

**Step 1: Update auth callback to redirect**

Change the `auth_callback` return from `dict[str, str]` to `RedirectResponse`. After successful OAuth:

```python
return fastapi_responses.RedirectResponse(url="/", status_code=307)
```

**Step 2: Update logout to redirect**

Change the `logout` return:

```python
return fastapi_responses.RedirectResponse(url="/login", status_code=307)
```

**Step 3: Update existing tests**

The auth tests that check for JSON responses need to be updated:
- `test_logout_returns_ok`: change to check for 307 redirect to `/login`
- Any callback tests checking JSON: change to check for 307 redirect to `/`

**Step 4: Run full test suite**

Run: `uv run pytest -q`
Expected: All pass.

**Step 5: Run lint + type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 6: Commit**

```bash
git add src/resonance/api/v1/auth.py tests/test_api_auth.py
git commit -m "feat: redirect auth callback to dashboard and logout to login page"
```

---

### Task 3: Dashboard Page

**Files:**
- Modify: `src/resonance/ui/routes.py`
- Modify: `src/resonance/templates/dashboard.html`
- Test: `tests/test_ui.py` (add tests)

**Step 1: Write the failing tests**

Add to `tests/test_ui.py`:

```python
class TestDashboard:
    async def test_dashboard_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
```

Note: Testing the authenticated dashboard with real data requires a database session. For now, test the unauthenticated redirect. Authenticated dashboard tests can use mocked sessions if needed, but the unauthenticated case is the critical guard.

**Step 2: Update dashboard route with stats queries**

Update the dashboard route in `src/resonance/ui/routes.py` to query counts:

```python
@router.get("/")
async def dashboard(
    request: fastapi.Request,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> fastapi_responses.HTMLResponse | fastapi_responses.RedirectResponse:
    """Render the dashboard with stats and sync controls."""
    user_id = session.get("user_id")
    if user_id is None:
        return fastapi_responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)

    # Stats queries
    artist_count = await _count(db, models_module.Artist)
    track_count = await _count(db, models_module.Track)
    event_count = await _count(
        db, models_module.ListeningEvent,
        models_module.ListeningEvent.user_id == user_uuid,
    )

    # Connected services
    connections_result = await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_uuid
        )
    )
    connections = connections_result.scalars().all()

    # Latest sync job
    sync_result = await db.execute(
        sa.select(sync_models.SyncJob)
        .where(sync_models.SyncJob.user_id == user_uuid)
        .order_by(sync_models.SyncJob.created_at.desc())
        .limit(1)
    )
    latest_sync = sync_result.scalar_one_or_none()

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "user_id": user_id,
            "artist_count": artist_count,
            "track_count": track_count,
            "event_count": event_count,
            "connections": connections,
            "latest_sync": latest_sync,
        },
    )


async def _count(
    db: sa_async.AsyncSession,
    model: type,
    *filters: Any,
) -> int:
    """Count rows in a table, optionally filtered."""
    stmt = sa.select(sa.func.count()).select_from(model)
    for f in filters:
        stmt = stmt.where(f)
    result = await db.execute(stmt)
    return result.scalar_one()
```

**Step 3: Create dashboard template**

Replace `src/resonance/templates/dashboard.html`:

```html
{% extends "base.html" %}

{% block title %}Dashboard — Resonance{% endblock %}

{% block content %}
<h1>Dashboard</h1>

<div class="grid">
    <article>
        <header>Artists</header>
        <p><strong>{{ artist_count }}</strong></p>
    </article>
    <article>
        <header>Tracks</header>
        <p><strong>{{ track_count }}</strong></p>
    </article>
    <article>
        <header>Listening Events</header>
        <p><strong>{{ event_count }}</strong></p>
    </article>
</div>

<h2>Connected Services</h2>
{% if connections %}
<table>
    <thead>
        <tr>
            <th>Service</th>
            <th>Account</th>
            <th>Sync</th>
        </tr>
    </thead>
    <tbody>
        {% for conn in connections %}
        <tr>
            <td>{{ conn.service_type.value | capitalize }}</td>
            <td>{{ conn.external_user_id }}</td>
            <td>
                <button
                    hx-post="/api/v1/sync/{{ conn.service_type.value }}"
                    hx-target="#sync-status"
                    hx-swap="innerHTML"
                >
                    Sync Now
                </button>
            </td>
        </tr>
        {% endfor %}
    </tbody>
</table>
{% else %}
<p>No services connected. <a href="/api/v1/auth/spotify">Connect Spotify</a></p>
{% endif %}

<div id="sync-status">
    {% if latest_sync %}
    <p>
        Last sync: {{ latest_sync.status.value }}
        {% if latest_sync.completed_at %}
            ({{ latest_sync.completed_at.strftime('%Y-%m-%d %H:%M') }})
        {% endif %}
        — {{ latest_sync.items_created }} created, {{ latest_sync.items_updated }} updated
    </p>
    {% endif %}
</div>
{% endblock %}
```

**Step 4: Run tests + lint + type check**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 5: Commit**

```bash
git add src/resonance/ui/routes.py src/resonance/templates/dashboard.html tests/test_ui.py
git commit -m "feat: add dashboard page with stats, connected services, and sync controls"
```

---

### Task 4: Sync Status Partial (HTMX Polling)

**Files:**
- Create: `src/resonance/templates/partials/sync_status.html`
- Modify: `src/resonance/ui/routes.py`
- Modify: `src/resonance/templates/dashboard.html`

**Step 1: Create sync status partial**

Create `src/resonance/templates/partials/sync_status.html`:

```html
{% if sync_jobs %}
{% for job in sync_jobs %}
<article>
    <p>
        <strong>{{ job.status.value | capitalize }}</strong>
        {% if job.status.value in ("pending", "running") %}
            <span aria-busy="true"></span>
        {% endif %}
    </p>
    {% if job.progress_total %}
    <progress value="{{ job.progress_current }}" max="{{ job.progress_total }}"></progress>
    {% endif %}
    <small>
        {% if job.completed_at %}
            Completed {{ job.completed_at.strftime('%Y-%m-%d %H:%M') }}
        {% elif job.started_at %}
            Started {{ job.started_at.strftime('%Y-%m-%d %H:%M') }}
        {% endif %}
        — {{ job.items_created }} created, {{ job.items_updated }} updated
        {% if job.error_message %}
            — Error: {{ job.error_message }}
        {% endif %}
    </small>
</article>
{% endfor %}
{% else %}
<p>No syncs yet.</p>
{% endif %}

{% if has_active_sync %}
<div hx-get="/partials/sync-status" hx-trigger="every 3s" hx-swap="outerHTML"></div>
{% endif %}
```

The key pattern: if there's an active sync (PENDING or RUNNING), the partial includes a self-polling div that refreshes every 3 seconds. Once syncs are all COMPLETED/FAILED, no more polling.

**Step 2: Add partial route**

Add to `src/resonance/ui/routes.py`:

```python
@router.get("/partials/sync-status")
async def sync_status_partial(
    request: fastapi.Request,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> fastapi_responses.HTMLResponse:
    """Return sync status HTML fragment for HTMX polling."""
    user_id = session.get("user_id")
    if user_id is None:
        return fastapi_responses.HTMLResponse("")

    user_uuid = uuid.UUID(user_id)
    result = await db.execute(
        sa.select(sync_models.SyncJob)
        .where(sync_models.SyncJob.user_id == user_uuid)
        .order_by(sync_models.SyncJob.created_at.desc())
        .limit(5)
    )
    sync_jobs = result.scalars().all()

    has_active_sync = any(
        j.status in (types_module.SyncStatus.PENDING, types_module.SyncStatus.RUNNING)
        for j in sync_jobs
    )

    return templates.TemplateResponse(
        request,
        "partials/sync_status.html",
        {"sync_jobs": sync_jobs, "has_active_sync": has_active_sync},
    )
```

**Step 3: Update dashboard to use sync partial**

In `dashboard.html`, replace the sync-status div with an include of the partial and HTMX attributes:

```html
<h2>Sync Status</h2>
<div id="sync-status"
     hx-get="/partials/sync-status"
     hx-trigger="load"
     hx-swap="innerHTML">
    Loading...
</div>
```

Also update the Sync Now button to trigger a refresh of the sync-status div after posting:

```html
<button
    hx-post="/api/v1/sync/{{ conn.service_type.value }}"
    hx-target="#sync-status"
    hx-swap="innerHTML"
    hx-trigger="click"
>
    Sync Now
</button>
```

Note: The sync API returns JSON, but HTMX will swap whatever comes back into the target. We need the sync API POST to return HTML when called via HTMX. The simplest approach: after the sync trigger, HTMX immediately polls the partial endpoint. Change the button to:

```html
<button
    hx-post="/api/v1/sync/{{ conn.service_type.value }}"
    hx-on::after-request="htmx.trigger('#sync-status', 'load')"
>
    Sync Now
</button>
```

This triggers the sync via the API, then re-triggers the sync-status partial load.

**Step 4: Run tests + lint**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 5: Commit**

```bash
git add src/resonance/templates/partials/ src/resonance/ui/routes.py src/resonance/templates/dashboard.html
git commit -m "feat: add HTMX sync status polling with auto-refresh partial"
```

---

### Task 5: Artists Page

**Files:**
- Create: `src/resonance/templates/artists.html`
- Create: `src/resonance/templates/partials/artist_list.html`
- Modify: `src/resonance/ui/routes.py`
- Test: `tests/test_ui.py` (add tests)

**Step 1: Write the failing test**

Add to `tests/test_ui.py`:

```python
class TestArtistsPage:
    async def test_artists_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/artists", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
```

**Step 2: Add artists route**

Add to `src/resonance/ui/routes.py`:

```python
PAGE_SIZE = 50


@router.get("/artists")
async def artists_page(
    request: fastapi.Request,
    page: int = 1,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> fastapi_responses.HTMLResponse | fastapi_responses.RedirectResponse:
    """Render the artists page with paginated list."""
    user_id = session.get("user_id")
    if user_id is None:
        return fastapi_responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * PAGE_SIZE
    result = await db.execute(
        sa.select(models_module.Artist)
        .order_by(models_module.Artist.name)
        .offset(offset)
        .limit(PAGE_SIZE + 1)  # fetch one extra to detect next page
    )
    artists = result.scalars().all()
    has_next = len(artists) > PAGE_SIZE
    artists = artists[:PAGE_SIZE]

    context = {
        "user_id": user_id,
        "artists": artists,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    # HTMX partial response
    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/artist_list.html", context
        )

    return templates.TemplateResponse(request, "artists.html", context)
```

**Step 3: Create artists template**

Create `src/resonance/templates/artists.html`:

```html
{% extends "base.html" %}

{% block title %}Artists — Resonance{% endblock %}

{% block content %}
<h1>Artists</h1>
<div id="artist-list">
    {% include "partials/artist_list.html" %}
</div>
{% endblock %}
```

**Step 4: Create artists list partial**

Create `src/resonance/templates/partials/artist_list.html`:

```html
<table>
    <thead>
        <tr>
            <th>Name</th>
            <th>Services</th>
        </tr>
    </thead>
    <tbody>
        {% for artist in artists %}
        <tr>
            <td>{{ artist.name }}</td>
            <td>
                {% for service in artist.service_links %}
                <small>{{ service | capitalize }}</small>
                {% endfor %}
            </td>
        </tr>
        {% else %}
        <tr>
            <td colspan="2">No artists synced yet.</td>
        </tr>
        {% endfor %}
    </tbody>
</table>

<nav>
    {% if has_prev %}
    <a href="/artists?page={{ page - 1 }}"
       hx-get="/artists?page={{ page - 1 }}"
       hx-target="#artist-list"
       hx-swap="innerHTML"
       role="button" class="outline">&laquo; Previous</a>
    {% endif %}
    <span>Page {{ page }}</span>
    {% if has_next %}
    <a href="/artists?page={{ page + 1 }}"
       hx-get="/artists?page={{ page + 1 }}"
       hx-target="#artist-list"
       hx-swap="innerHTML"
       role="button" class="outline">Next &raquo;</a>
    {% endif %}
</nav>
```

**Step 5: Run tests + lint**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 6: Commit**

```bash
git add src/resonance/templates/artists.html src/resonance/templates/partials/artist_list.html src/resonance/ui/routes.py tests/test_ui.py
git commit -m "feat: add paginated artists page with HTMX partial navigation"
```

---

### Task 6: Tracks Page

**Files:**
- Create: `src/resonance/templates/tracks.html`
- Create: `src/resonance/templates/partials/track_list.html`
- Modify: `src/resonance/ui/routes.py`
- Test: `tests/test_ui.py` (add tests)

**Step 1: Write the failing test**

Add to `tests/test_ui.py`:

```python
class TestTracksPage:
    async def test_tracks_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/tracks", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
```

**Step 2: Add tracks route**

Add to `src/resonance/ui/routes.py`. Same pagination pattern as artists:

```python
@router.get("/tracks")
async def tracks_page(
    request: fastapi.Request,
    page: int = 1,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> fastapi_responses.HTMLResponse | fastapi_responses.RedirectResponse:
    """Render the tracks page with paginated list."""
    user_id = session.get("user_id")
    if user_id is None:
        return fastapi_responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * PAGE_SIZE
    result = await db.execute(
        sa.select(models_module.Track)
        .join(models_module.Artist)
        .order_by(models_module.Track.title)
        .options(sa_orm.joinedload(models_module.Track.artist))
        .offset(offset)
        .limit(PAGE_SIZE + 1)
    )
    tracks = result.scalars().unique().all()
    has_next = len(tracks) > PAGE_SIZE
    tracks = tracks[:PAGE_SIZE]

    context = {
        "user_id": user_id,
        "tracks": tracks,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/track_list.html", context
        )

    return templates.TemplateResponse(request, "tracks.html", context)
```

Note: Uses `joinedload` to eager-load the artist relationship so we can display `track.artist.name` without N+1 queries. Requires `import sqlalchemy.orm as sa_orm`.

**Step 3: Create tracks template**

Create `src/resonance/templates/tracks.html`:

```html
{% extends "base.html" %}

{% block title %}Tracks — Resonance{% endblock %}

{% block content %}
<h1>Tracks</h1>
<div id="track-list">
    {% include "partials/track_list.html" %}
</div>
{% endblock %}
```

**Step 4: Create tracks list partial**

Create `src/resonance/templates/partials/track_list.html`:

```html
<table>
    <thead>
        <tr>
            <th>Title</th>
            <th>Artist</th>
            <th>Services</th>
        </tr>
    </thead>
    <tbody>
        {% for track in tracks %}
        <tr>
            <td>{{ track.title }}</td>
            <td>{{ track.artist.name }}</td>
            <td>
                {% for service in track.service_links %}
                <small>{{ service | capitalize }}</small>
                {% endfor %}
            </td>
        </tr>
        {% else %}
        <tr>
            <td colspan="3">No tracks synced yet.</td>
        </tr>
        {% endfor %}
    </tbody>
</table>

<nav>
    {% if has_prev %}
    <a href="/tracks?page={{ page - 1 }}"
       hx-get="/tracks?page={{ page - 1 }}"
       hx-target="#track-list"
       hx-swap="innerHTML"
       role="button" class="outline">&laquo; Previous</a>
    {% endif %}
    <span>Page {{ page }}</span>
    {% if has_next %}
    <a href="/tracks?page={{ page + 1 }}"
       hx-get="/tracks?page={{ page + 1 }}"
       hx-target="#track-list"
       hx-swap="innerHTML"
       role="button" class="outline">Next &raquo;</a>
    {% endif %}
</nav>
```

**Step 5: Run tests + lint**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 6: Commit**

```bash
git add src/resonance/templates/tracks.html src/resonance/templates/partials/track_list.html src/resonance/ui/routes.py tests/test_ui.py
git commit -m "feat: add paginated tracks page with artist names and HTMX navigation"
```

---

### Task 7: Listening History Page

**Files:**
- Create: `src/resonance/templates/history.html`
- Create: `src/resonance/templates/partials/history_list.html`
- Modify: `src/resonance/ui/routes.py`
- Test: `tests/test_ui.py` (add tests)

**Step 1: Write the failing test**

Add to `tests/test_ui.py`:

```python
class TestHistoryPage:
    async def test_history_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/history", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
```

**Step 2: Add history route**

Add to `src/resonance/ui/routes.py`:

```python
@router.get("/history")
async def history_page(
    request: fastapi.Request,
    page: int = 1,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> fastapi_responses.HTMLResponse | fastapi_responses.RedirectResponse:
    """Render listening history with paginated events."""
    user_id = session.get("user_id")
    if user_id is None:
        return fastapi_responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * PAGE_SIZE
    result = await db.execute(
        sa.select(models_module.ListeningEvent)
        .where(models_module.ListeningEvent.user_id == user_uuid)
        .order_by(models_module.ListeningEvent.listened_at.desc())
        .options(
            sa_orm.joinedload(models_module.ListeningEvent.track).joinedload(
                models_module.Track.artist
            )
        )
        .offset(offset)
        .limit(PAGE_SIZE + 1)
    )
    events = result.scalars().unique().all()
    has_next = len(events) > PAGE_SIZE
    events = events[:PAGE_SIZE]

    context = {
        "user_id": user_id,
        "events": events,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/history_list.html", context
        )

    return templates.TemplateResponse(request, "history.html", context)
```

Note: This requires a `track` relationship on `ListeningEvent` and `artist` on `Track`. Check that these relationships exist in the models. The `Track.artist` relationship exists. The `ListeningEvent` model may need a `track` relationship added — check the model and add it if missing:

```python
# In models/music.py, add to ListeningEvent:
track: orm.Mapped[Track] = orm.relationship()
```

**Step 3: Create history template**

Create `src/resonance/templates/history.html`:

```html
{% extends "base.html" %}

{% block title %}Listening History — Resonance{% endblock %}

{% block content %}
<h1>Listening History</h1>
<div id="history-list">
    {% include "partials/history_list.html" %}
</div>
{% endblock %}
```

**Step 4: Create history list partial**

Create `src/resonance/templates/partials/history_list.html`:

```html
<table>
    <thead>
        <tr>
            <th>Track</th>
            <th>Artist</th>
            <th>Played</th>
            <th>Source</th>
        </tr>
    </thead>
    <tbody>
        {% for event in events %}
        <tr>
            <td>{{ event.track.title }}</td>
            <td>{{ event.track.artist.name }}</td>
            <td>{{ event.listened_at.strftime('%Y-%m-%d %H:%M') }}</td>
            <td><small>{{ event.source_service.value | capitalize }}</small></td>
        </tr>
        {% else %}
        <tr>
            <td colspan="4">No listening history yet. Sync a service to get started.</td>
        </tr>
        {% endfor %}
    </tbody>
</table>

<nav>
    {% if has_prev %}
    <a href="/history?page={{ page - 1 }}"
       hx-get="/history?page={{ page - 1 }}"
       hx-target="#history-list"
       hx-swap="innerHTML"
       role="button" class="outline">&laquo; Previous</a>
    {% endif %}
    <span>Page {{ page }}</span>
    {% if has_next %}
    <a href="/history?page={{ page + 1 }}"
       hx-get="/history?page={{ page + 1 }}"
       hx-target="#history-list"
       hx-swap="innerHTML"
       role="button" class="outline">Next &raquo;</a>
    {% endif %}
</nav>
```

**Step 5: Run tests + lint**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 6: Commit**

```bash
git add src/resonance/templates/history.html src/resonance/templates/partials/history_list.html src/resonance/ui/routes.py src/resonance/models/music.py tests/test_ui.py
git commit -m "feat: add paginated listening history page with track and artist details"
```

---

### Task 8: Account Page

**Files:**
- Create: `src/resonance/templates/account.html`
- Modify: `src/resonance/ui/routes.py`
- Test: `tests/test_ui.py` (add tests)

**Step 1: Write the failing test**

Add to `tests/test_ui.py`:

```python
class TestAccountPage:
    async def test_account_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/account", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
```

**Step 2: Add account route**

Add to `src/resonance/ui/routes.py`:

```python
@router.get("/account")
async def account_page(
    request: fastapi.Request,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> fastapi_responses.HTMLResponse | fastapi_responses.RedirectResponse:
    """Render account management page."""
    user_id = session.get("user_id")
    if user_id is None:
        return fastapi_responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)

    # Get user profile
    user_result = await db.execute(
        sa.select(user_models.User).where(user_models.User.id == user_uuid)
    )
    user = user_result.scalar_one_or_none()

    # Get connections
    conn_result = await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_uuid
        )
    )
    connections = conn_result.scalars().all()

    return templates.TemplateResponse(
        request,
        "account.html",
        {
            "user_id": user_id,
            "user": user,
            "connections": connections,
        },
    )
```

**Step 3: Create account template**

Create `src/resonance/templates/account.html`:

```html
{% extends "base.html" %}

{% block title %}Account — Resonance{% endblock %}

{% block content %}
<h1>Account</h1>

{% if user %}
<article>
    <header>Profile</header>
    <p><strong>Display Name:</strong> {{ user.display_name }}</p>
    {% if user.email %}
    <p><strong>Email:</strong> {{ user.email }}</p>
    {% endif %}
    <p><small>Member since {{ user.created_at.strftime('%Y-%m-%d') }}</small></p>
</article>
{% endif %}

<h2>Connected Services</h2>
<table>
    <thead>
        <tr>
            <th>Service</th>
            <th>Account</th>
            <th>Connected</th>
            <th></th>
        </tr>
    </thead>
    <tbody>
        {% for conn in connections %}
        <tr>
            <td>{{ conn.service_type.value | capitalize }}</td>
            <td>{{ conn.external_user_id }}</td>
            <td>{{ conn.connected_at.strftime('%Y-%m-%d') }}</td>
            <td>
                {% if connections | length > 1 %}
                <button
                    hx-delete="/api/v1/account/connections/{{ conn.id }}"
                    hx-confirm="Disconnect {{ conn.service_type.value | capitalize }}?"
                    hx-target="closest tr"
                    hx-swap="outerHTML"
                    class="outline secondary"
                >
                    Disconnect
                </button>
                {% else %}
                <small>Last connection</small>
                {% endif %}
            </td>
        </tr>
        {% endfor %}
    </tbody>
</table>

<h2>Connect Another Service</h2>
<a href="/api/v1/auth/spotify" role="button" class="outline">Connect Spotify</a>

<h2>Session</h2>
<form method="post" action="/api/v1/auth/logout">
    <button type="submit" class="secondary">Log Out</button>
</form>
{% endblock %}
```

Note: The disconnect button uses `hx-delete` to call the existing API endpoint. The API returns JSON `{"status": "unlinked"}` — HTMX will replace the table row with that text. For a cleaner UX, the implementer can make the API return an empty 200 response on HTMX requests (check `HX-Request` header), which would just remove the row. Alternatively, use `hx-on::after-request="location.reload()"` to refresh the page.

**Step 4: Run tests + lint**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 5: Commit**

```bash
git add src/resonance/templates/account.html src/resonance/ui/routes.py tests/test_ui.py
git commit -m "feat: add account page with connection management and logout"
```

---

### Task 9: Include Templates in Docker Image

**Files:**
- Modify: `Dockerfile`

The templates directory needs to be in the Docker image. Currently the Dockerfile copies `src/` which includes `src/resonance/templates/`. Verify this works — if templates are inside the `src/resonance/` package directory, they're already copied. But `Jinja2Templates` needs to find them at runtime.

**Step 1: Verify templates are included**

Run: `docker build -t resonance:test . && docker run --rm resonance:test python -c "import pathlib; p = pathlib.Path('/app/src/resonance/templates'); print(p.exists(), list(p.iterdir()) if p.exists() else 'missing')"`

If templates are missing, add a COPY line to the Dockerfile. If they're present (which they should be since they're under `src/`), no change needed.

**Step 2: Commit if changes needed**

Only commit if the Dockerfile needed modification.

---

## Summary

After all 9 tasks, the UI will have:

| Page | URL | Features |
|------|-----|----------|
| Login | `/login` | "Connect with Spotify" button |
| Dashboard | `/` | Stats cards, connected services, sync controls with HTMX polling |
| Artists | `/artists` | Paginated artist list with HTMX navigation |
| Tracks | `/tracks` | Paginated track list with artist names |
| History | `/history` | Listening events with timestamps |
| Account | `/account` | Profile, connection management, disconnect, logout |

**Styling:** Pico CSS from CDN, system light/dark theme auto-detection.
**Interactivity:** HTMX from CDN — sync polling, pagination, disconnect actions.
**No JavaScript written.** No build step. No custom CSS.

**Not in scope (future phases):**
- Search and filtering
- Playlist generation UI
- Mobile-specific optimization
- Custom light/dark theme toggle
- Real-time notifications (WebSocket/SSE)
