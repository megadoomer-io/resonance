# Feed Management UI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let users connect/disconnect Songkick accounts and trigger syncs from the browser, integrated into existing Account and Dashboard pages.

**Architecture:** New API lookup endpoint for username validation, new delete-by-username endpoint, Songkick entries in the connections list on Account and Dashboard pages, HTMX-driven connect flow with inline validation. No new full page templates — just a new partial and modifications to existing templates/routes.

**Tech Stack:** Python 3.14, FastAPI, Jinja2, HTMX, Pico CSS, httpx, SQLAlchemy async

**Design Doc:** [docs/plans/2026-04-21-feed-management-ui-design.md](2026-04-21-feed-management-ui-design.md)

---

### Task 1: Songkick Lookup API Endpoint

Add a validation endpoint that fetches both iCal feeds to verify the username exists and returns event counts.

**Files:**
- Modify: `src/resonance/api/v1/calendar_feeds.py`
- Create: `tests/test_api_songkick_lookup.py`

**Step 1: Write failing tests**

Test cases:
- Valid username returns 200 with username, plans_count, tracked_artist_count
- Invalid username (Songkick 404) returns 404
- HTTP error from Songkick returns 502

Mock `httpx.AsyncClient` to return sample iCal responses. Count `BEGIN:VEVENT` occurrences.

```python
"""Tests for Songkick username lookup endpoint."""

class TestSongkickLookup:

    async def test_valid_username(self, ...):
        # Mock httpx to return iCal with 2 VEVENTs for attendance,
        # 5 for tracked_artist
        # POST /api/v1/calendar-feeds/songkick/lookup {"username": "mike"}
        # Expect 200, {"username": "mike", "plans_count": 2, "tracked_artist_count": 5}

    async def test_invalid_username(self, ...):
        # Mock httpx to return 404
        # Expect 404

    async def test_songkick_server_error(self, ...):
        # Mock httpx to raise httpx.HTTPStatusError with 500
        # Expect 502
```

**Step 2: Implement endpoint**

Add to `calendar_feeds.py`:

```python
class SongkickLookupResponse(pydantic.BaseModel):
    username: str
    plans_count: int
    tracked_artist_count: int

@router.post("/songkick/lookup")
async def lookup_songkick_user(
    body: SongkickFeedRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
) -> SongkickLookupResponse:
    base = f"https://www.songkick.com/users/{body.username}/calendars.ics"
    async with httpx.AsyncClient() as client:
        try:
            att_resp = await client.get(f"{base}?filter=attendance")
            att_resp.raise_for_status()
            trk_resp = await client.get(f"{base}?filter=tracked_artist")
            trk_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise fastapi.HTTPException(404, "Songkick user not found")
            raise fastapi.HTTPException(502, "Songkick unavailable")

    plans_count = att_resp.text.count("BEGIN:VEVENT")
    tracked_count = trk_resp.text.count("BEGIN:VEVENT")
    return SongkickLookupResponse(
        username=body.username,
        plans_count=plans_count,
        tracked_artist_count=tracked_count,
    )
```

Add `import httpx` to the file imports.

**Step 3: Run tests, lint, type check**

```bash
uv run pytest tests/test_api_songkick_lookup.py -v
uv run ruff check src/resonance/api/v1/calendar_feeds.py && uv run mypy src/resonance/api/v1/calendar_feeds.py
```

**Step 4: Commit**

```bash
git add src/resonance/api/v1/calendar_feeds.py tests/test_api_songkick_lookup.py
git commit -m "feat: add Songkick username lookup endpoint with validation"
```

---

### Task 2: Delete Songkick by Username Endpoint

Add an endpoint that deletes both feeds for a Songkick username.

**Files:**
- Modify: `src/resonance/api/v1/calendar_feeds.py`
- Modify: `tests/test_api_calendar_feeds.py`

**Step 1: Write failing tests**

```python
class TestDeleteSongkickByUsername:

    async def test_deletes_both_feeds(self, ...):
        # Create 2 feeds for username "mike", DELETE /api/v1/calendar-feeds/songkick/mike
        # Expect 200, both feeds removed

    async def test_unknown_username_404(self, ...):
        # DELETE /api/v1/calendar-feeds/songkick/nobody
        # Expect 404

    async def test_only_deletes_matching_username(self, ...):
        # Create feeds for "mike" and "alice"
        # DELETE /api/v1/calendar-feeds/songkick/mike
        # alice's feeds remain
```

**Step 2: Implement endpoint**

Add to `calendar_feeds.py`:

```python
@router.delete("/songkick/{username}")
async def delete_songkick_feeds(
    username: str,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    base = f"https://www.songkick.com/users/{username}/calendars.ics"
    stmt = sa.select(concert_models.UserCalendarFeed).where(
        concert_models.UserCalendarFeed.user_id == user_id,
        concert_models.UserCalendarFeed.url.like(f"{base}%"),
    )
    result = await db.execute(stmt)
    feeds = result.scalars().all()
    if not feeds:
        raise fastapi.HTTPException(404, "No Songkick feeds for this username")
    for feed in feeds:
        await db.delete(feed)
    await db.commit()
    return {"status": "deleted", "count": str(len(feeds))}
```

**Important:** This endpoint must be registered BEFORE the `/{feed_id}` route in the router, otherwise FastAPI will try to parse "songkick" as a UUID. Move the `delete_songkick_feeds` route definition above `delete_feed`.

**Step 3: Run tests, lint, type check, commit**

```bash
uv run pytest tests/test_api_calendar_feeds.py -v
git add src/resonance/api/v1/calendar_feeds.py tests/test_api_calendar_feeds.py
git commit -m "feat: add delete Songkick feeds by username endpoint"
```

---

### Task 3: Account Page — Songkick Connect Flow

Add the Songkick connect UI to the Account page: button → lookup form → confirmation card → connected.

**Files:**
- Modify: `src/resonance/ui/routes.py` (account_page route)
- Modify: `src/resonance/templates/account.html`
- Create: `src/resonance/templates/partials/songkick_connect.html`

**Step 1: Create the partial template**

`partials/songkick_connect.html` — three states via HTMX swaps:

**Initial state (button):**
```html
<div id="songkick-connect">
    <button
        hx-get="/partials/songkick-lookup"
        hx-target="#songkick-connect"
        hx-swap="innerHTML"
        class="outline"
    >Connect Songkick</button>
</div>
```

**Lookup form (swapped in on button click):**
```html
<form
    hx-post="/partials/songkick-lookup"
    hx-target="#songkick-connect"
    hx-swap="innerHTML"
>
    <fieldset role="group">
        <input name="username" placeholder="Songkick username" required autofocus>
        <button type="submit">Look up</button>
    </fieldset>
</form>
```

**Confirmation card (swapped in on successful lookup):**
```html
<article>
    <header>Songkick account found</header>
    <p><strong>{{ username }}</strong> — {{ plans_count }} upcoming plans,
       {{ tracked_artist_count }} tracked artist shows</p>
    <footer>
        <div class="grid">
            <button
                hx-post="/partials/songkick-confirm"
                hx-vals='{"username": "{{ username }}"}'
                hx-target="#songkick-connect"
                hx-swap="innerHTML"
            >Connect</button>
            <button
                hx-get="/partials/songkick-connect"
                hx-target="#songkick-connect"
                hx-swap="innerHTML"
                class="secondary outline"
            >Cancel</button>
        </div>
    </footer>
</article>
```

**Error state (swapped in on failed lookup):**
```html
<p><mark>No Songkick user found with that username.</mark></p>
<button
    hx-get="/partials/songkick-lookup"
    hx-target="#songkick-connect"
    hx-swap="innerHTML"
    class="outline"
>Try Again</button>
```

**Step 2: Add UI routes for the HTMX partials**

Add to `routes.py`:

```python
@router.get("/partials/songkick-connect")
async def songkick_connect_initial(request: fastapi.Request):
    """Return the initial Connect Songkick button."""
    return templates.TemplateResponse(
        request, "partials/songkick_connect.html",
        {"state": "button"},
    )

@router.get("/partials/songkick-lookup")
async def songkick_lookup_form(request: fastapi.Request):
    """Return the username input form."""
    return templates.TemplateResponse(
        request, "partials/songkick_connect.html",
        {"state": "form"},
    )

@router.post("/partials/songkick-lookup")
async def songkick_lookup_submit(request: fastapi.Request):
    """Validate username against Songkick, return confirmation or error."""
    form = await request.form()
    username = str(form.get("username", "")).strip()
    if not username:
        return templates.TemplateResponse(
            request, "partials/songkick_connect.html",
            {"state": "error"},
        )

    base = f"https://www.songkick.com/users/{username}/calendars.ics"
    async with httpx.AsyncClient() as client:
        try:
            att = await client.get(f"{base}?filter=attendance")
            att.raise_for_status()
            trk = await client.get(f"{base}?filter=tracked_artist")
            trk.raise_for_status()
        except httpx.HTTPStatusError:
            return templates.TemplateResponse(
                request, "partials/songkick_connect.html",
                {"state": "error"},
            )

    return templates.TemplateResponse(
        request, "partials/songkick_connect.html",
        {
            "state": "confirm",
            "username": username,
            "plans_count": att.text.count("BEGIN:VEVENT"),
            "tracked_artist_count": trk.text.count("BEGIN:VEVENT"),
        },
    )

@router.post("/partials/songkick-confirm")
async def songkick_confirm(request: fastapi.Request):
    """Create the feeds and return updated connection state."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        raise fastapi.HTTPException(401)

    form = await request.form()
    username = str(form.get("username", "")).strip()

    async with _get_db(request) as db:
        base = f"https://www.songkick.com/users/{username}/calendars.ics"
        for feed_type, filt in [
            (types_module.FeedType.SONGKICK_ATTENDANCE, "attendance"),
            (types_module.FeedType.SONGKICK_TRACKED_ARTIST, "tracked_artist"),
        ]:
            feed = concert_models.UserCalendarFeed(
                user_id=uuid.UUID(user_id),
                feed_type=feed_type,
                url=f"{base}?filter={filt}",
            )
            db.add(feed)
        await db.commit()

    # Return the button again + trigger page reload to show new connection
    return fastapi.responses.HTMLResponse(
        '<script>location.reload()</script>'
    )
```

**Step 3: Update account.html**

In the "Connect Another Service" section (line 88-100), add after the existing buttons:

```html
<div id="songkick-connect">
    {% include "partials/songkick_connect.html" %}
</div>
```

Pass `state: "button"` in the account_page context so the partial renders the initial button.

**Step 4: Update account_page route**

Add `import resonance.models.concert as concert_models` and `import httpx` to routes.py.

Query `UserCalendarFeed` for the user and group by Songkick username (extract from URL). Pass `songkick_accounts` list to the template context so connected Songkick usernames appear in the connections table.

**Step 5: Add Songkick entries to the connections table in account.html**

After the existing `{% for conn in connections %}` loop, add a second loop for `songkick_accounts`:

```html
{% for sk in songkick_accounts %}
<tr>
    <td>Songkick</td>
    <td>{{ sk.username }}</td>
    <td>{{ (sk.created_at | localtime(user_tz)).strftime('%Y-%m-%d') }}</td>
    <td>
        <button
            hx-delete="/api/v1/calendar-feeds/songkick/{{ sk.username }}"
            hx-confirm="Disconnect Songkick account {{ sk.username }}?"
            hx-on::after-request="location.reload()"
        >Disconnect</button>
    </td>
</tr>
{% endfor %}
```

**Step 6: Run tests, lint, type check, commit**

```bash
uv run pytest -v
uv run ruff check src/resonance/ui/routes.py src/resonance/templates/ && uv run mypy src/resonance/ui/routes.py
git add src/resonance/ui/routes.py src/resonance/templates/account.html src/resonance/templates/partials/songkick_connect.html
git commit -m "feat: add Songkick connect flow on Account page"
```

---

### Task 4: Dashboard — Songkick Sync Buttons

Add per-username Songkick sync buttons to the Dashboard connected services table.

**Files:**
- Modify: `src/resonance/ui/routes.py` (dashboard route)
- Modify: `src/resonance/templates/dashboard.html`

**Step 1: Update dashboard route**

In the `dashboard()` function, after querying `ServiceConnection`, also query `UserCalendarFeed` grouped by Songkick username. Build a list of Songkick accounts with their feed IDs:

```python
# Query Songkick calendar feeds
sk_feeds_result = await db.execute(
    sa.select(concert_models.UserCalendarFeed).where(
        concert_models.UserCalendarFeed.user_id == user_uuid,
        concert_models.UserCalendarFeed.feed_type.in_([
            types_module.FeedType.SONGKICK_ATTENDANCE,
            types_module.FeedType.SONGKICK_TRACKED_ARTIST,
        ]),
    )
)
sk_feeds = sk_feeds_result.scalars().all()

# Group feeds by username (extracted from URL)
songkick_accounts: dict[str, list] = {}
for feed in sk_feeds:
    # URL: https://www.songkick.com/users/{username}/calendars.ics?...
    parts = feed.url.split("/users/")
    if len(parts) > 1:
        username = parts[1].split("/")[0]
        songkick_accounts.setdefault(username, []).append(feed)
```

Pass `songkick_accounts` to template context.

Also query active CALENDAR_SYNC tasks (by feed_id in params) so we can show "Syncing..." state.

**Step 2: Update dashboard.html**

After the `{% for conn in connections %}` loop in the Connected Services table, add Songkick rows:

```html
{% for username, sk_feeds in songkick_accounts.items() %}
<tr>
    <td>Songkick</td>
    <td>{{ username }}</td>
    <td>
        {% set last_sync = sk_feeds | map(attribute='last_synced_at') | select | sort | last %}
        {{ (last_sync | localtime(user_tz)).strftime('%Y-%m-%d %H:%M') if last_sync else 'Never' }}
    </td>
    <td>
        {% set is_syncing = active_feed_syncs.get(username) %}
        {% if is_syncing %}
        <span aria-busy="true">Syncing...</span>
        {% else %}
        <button
            hx-post="/partials/songkick-sync/{{ username }}"
            hx-swap="none"
            hx-on::after-request="htmx.trigger('#sync-status', 'load'); this.textContent='Syncing...'; this.disabled=true"
        >Sync Now</button>
        {% endif %}
    </td>
</tr>
{% endfor %}
```

**Step 3: Add UI route for triggering Songkick sync**

```python
@router.post("/partials/songkick-sync/{username}")
async def songkick_sync_trigger(username: str, request: fastapi.Request):
    """Trigger sync for all feeds belonging to a Songkick username."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        raise fastapi.HTTPException(401)

    user_uuid = uuid.UUID(user_id)
    base = f"https://www.songkick.com/users/{username}/calendars.ics"

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(concert_models.UserCalendarFeed).where(
                concert_models.UserCalendarFeed.user_id == user_uuid,
                concert_models.UserCalendarFeed.url.like(f"{base}%"),
            )
        )
        feeds = result.scalars().all()
        if not feeds:
            raise fastapi.HTTPException(404)

        arq_redis = request.app.state.arq_redis
        for feed in feeds:
            task = task_models.Task(
                user_id=user_uuid,
                task_type=types_module.TaskType.CALENDAR_SYNC,
                status=types_module.SyncStatus.PENDING,
                params={"feed_id": str(feed.id)},
            )
            db.add(task)
            await db.flush()
            await arq_redis.enqueue_job(
                "sync_calendar_feed",
                str(feed.id),
                str(task.id),
                _job_id=f"sync_calendar_feed:{feed.id}",
            )
        await db.commit()

    return fastapi.responses.HTMLResponse("")
```

**Step 4: Run tests, lint, type check, commit**

```bash
uv run pytest -v
uv run ruff check src/resonance/ui/routes.py && uv run mypy src/resonance/ui/routes.py
git add src/resonance/ui/routes.py src/resonance/templates/dashboard.html
git commit -m "feat: add Songkick sync buttons on Dashboard"
```

---

### Task 5: Sync Status Partial — Show Calendar Sync Tasks

The existing sync status partial polls `/partials/sync-status` and shows SYNC_JOB tasks. It needs to also show CALENDAR_SYNC tasks so Songkick sync progress is visible.

**Files:**
- Modify: `src/resonance/ui/routes.py` (sync_status partial route)
- Modify: `src/resonance/templates/partials/sync_status.html`

**Step 1: Update the sync status route**

Find the route that renders `partials/sync_status.html`. Modify the query to also include `CALENDAR_SYNC` tasks:

```python
task_models.Task.task_type.in_([
    types_module.TaskType.SYNC_JOB,
    types_module.TaskType.CALENDAR_SYNC,
]),
```

**Step 2: Update the sync status template**

CALENDAR_SYNC tasks don't have `service_connection`, so the template needs a fallback for the "Service" column. Use `task.params.get("feed_id")` or the task description to show "Songkick" instead:

```html
{% if job.task_type.value == 'calendar_sync' %}
<td>Songkick</td>
{% else %}
<td>{{ job.service_connection.service_type.value | capitalize }}</td>
{% endif %}
```

**Step 3: Run tests, lint, commit**

```bash
uv run pytest -v
git add src/resonance/ui/routes.py src/resonance/templates/partials/sync_status.html
git commit -m "feat: show calendar sync tasks in sync status partial"
```

---

### Task 6: Manual Testing & Polish

Verify the full flow end-to-end in the browser.

**Step 1: Deploy and test**

```bash
git push origin main
# Wait for deploy
uv run resonance-api healthz
```

**Step 2: Manual test checklist**

- [ ] Account page shows "Connect Songkick" button
- [ ] Clicking it reveals username input
- [ ] Entering valid username shows confirmation with event counts
- [ ] Entering invalid username shows error message
- [ ] Clicking Connect adds Songkick to connections list
- [ ] Adding a second Songkick username works (multiple accounts)
- [ ] Disconnect removes only that username's feeds
- [ ] Dashboard shows Songkick sync buttons per username
- [ ] Clicking Sync triggers both feeds, sync status shows progress
- [ ] Sync status polls and shows completion

**Step 3: Fix any issues found, commit**

---

## Task Dependency Graph

```
Task 1 (lookup endpoint)
  └→ Task 3 (account page connect flow) — uses lookup
Task 2 (delete by username endpoint)
  └→ Task 3 (account page disconnect)
Task 3 (account page)
  └→ Task 4 (dashboard sync buttons) — same patterns
Task 4 (dashboard)
  └→ Task 5 (sync status partial)
Task 6 (manual testing) — depends on all above
```

Tasks 1 and 2 are independent and can run in parallel.
