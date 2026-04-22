# Events List Page Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a paginated Events page showing discovered concert events with date, title, venue, and artist candidates.

**Architecture:** Single new route + two templates following the exact Tracks/Artists page pattern. Paginated table with HTMX partial swap. Event titles link to external Songkick page. Artist candidates shown inline (display only, no review actions).

**Tech Stack:** FastAPI, Jinja2, HTMX, SQLAlchemy async, Pico CSS

---

### Task 1: Events List Page

**Files:**
- Create: `src/resonance/templates/events.html`
- Create: `src/resonance/templates/partials/event_list.html`
- Modify: `src/resonance/templates/base.html` (nav — add Events link after Tracks, before History)
- Modify: `src/resonance/ui/routes.py` (add events_page route)
- Modify: `tests/test_ui.py` (add test for events route)

**Step 1: Create the full page template**

Create `src/resonance/templates/events.html`:

```html
{% extends "base.html" %}
{% block title %}Events — resonance{% endblock %}
{% block content %}
<h1>Events</h1>
<div id="event-list">
    {% include "partials/event_list.html" %}
</div>
{% endblock %}
```

**Step 2: Create the partial template**

Create `src/resonance/templates/partials/event_list.html`:

```html
{% if events %}
<figure>
    <table>
        <thead>
            <tr>
                <th>Date</th>
                <th>Event</th>
                <th>Venue</th>
                <th>Artists</th>
            </tr>
        </thead>
        <tbody>
            {% for event in events %}
            <tr>
                <td>{{ event.event_date.strftime('%Y-%m-%d') }}</td>
                <td>
                    {% if event.external_url %}
                    <a href="{{ event.external_url }}" target="_blank" rel="noopener">{{ event.title }}</a>
                    {% else %}
                    {{ event.title }}
                    {% endif %}
                </td>
                <td>{{ event.venue.name if event.venue else '' }}</td>
                <td>
                    {% if event.artists %}
                        {{ event.artists | map(attribute='raw_name') | join(', ') }}
                    {% elif event.artist_candidates %}
                        <small style="opacity: 0.6">{{ event.artist_candidates | map(attribute='raw_name') | join(', ') }}</small>
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
<nav>
    {% if has_prev %}
    <a href="/events?page={{ page - 1 }}"
       hx-get="/events?page={{ page - 1 }}"
       hx-target="#event-list"
       hx-swap="innerHTML"
       role="button"
       class="secondary">Previous</a>
    {% endif %}
    {% if has_next %}
    <a href="/events?page={{ page + 1 }}"
       hx-get="/events?page={{ page + 1 }}"
       hx-target="#event-list"
       hx-swap="innerHTML"
       role="button">Next</a>
    {% endif %}
</nav>
{% else %}
<p>No events synced yet. <a href="/account">Connect Songkick</a> to import concert data.</p>
{% endif %}
```

Confirmed artists (`event.artists`) show at full opacity. Unreviewed candidates
(`event.artist_candidates`) show at reduced opacity to visually distinguish them.

**Step 3: Add nav link**

In `src/resonance/templates/base.html`, add Events link after Tracks (line 34), before History:

```html
<li><a href="/events">Events</a></li>
```

**Step 4: Add the route**

In `src/resonance/ui/routes.py`, add the events_page route. Follow the exact tracks_page pattern:

```python
@router.get("/events", response_model=None)
async def events_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated events list, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(concert_models.Event)
            .options(
                sa_orm.joinedload(concert_models.Event.venue),
                sa_orm.joinedload(concert_models.Event.artists),
                sa_orm.joinedload(concert_models.Event.artist_candidates),
            )
            .order_by(concert_models.Event.event_date.desc())
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        events = list(result.unique().scalars().all())

    has_next = len(events) > _PAGE_SIZE
    events = events[:_PAGE_SIZE]

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "events": events,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/event_list.html", context
        )
    return templates.TemplateResponse(request, "events.html", context)
```

Key differences from tracks_page:
- Uses `concert_models.Event` (already imported in routes.py from Task 3)
- Eager-loads `venue`, `artists`, and `artist_candidates` relationships via `joinedload` to avoid N+1 queries
- Uses `.unique()` on results because `joinedload` with collections produces duplicate parent rows
- Orders by `event_date DESC` (most recent first)

**Step 5: Add test**

Add a basic route test to `tests/test_ui.py`:

```python
class TestEventsPage:
    async def test_redirects_to_login_when_unauthenticated(self) -> None:
        # GET /events without session → 307 redirect to /login
        ...
```

Follow the pattern of existing page tests in that file.

**Step 6: Run tests, lint, type check**

```bash
uv run pytest -v
uv run ruff check src/resonance/ui/routes.py src/resonance/templates/
uv run mypy src/resonance/ui/routes.py
```

**Step 7: Commit**

```bash
git add src/resonance/templates/events.html src/resonance/templates/partials/event_list.html src/resonance/templates/base.html src/resonance/ui/routes.py tests/test_ui.py
git commit -m "feat: add paginated Events list page with venue and artist display"
```
