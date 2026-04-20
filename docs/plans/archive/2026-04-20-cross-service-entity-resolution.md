# Cross-Service Entity Resolution Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Auto-run artist/track/event dedup after every sync and surface clickable service links on artist and track list pages. Fixes #26.

**Architecture:** Add a `dedup_all()` function that runs artist → track → event dedup in sequence. Change the post-sync auto-trigger to use it. Replace the plain service name text in artist/track list templates with colored, clickable badges linking to each entity on Spotify, Last.fm, and ListenBrainz.

**Tech Stack:** Python (dedup module, worker), Jinja2 templates, Pico CSS, HTML/CSS badges

---

### Task 1: Add `dedup_all()` to dedup module

**Files:**
- Modify: `src/resonance/dedup.py`
- Test: `tests/test_dedup.py`

**Step 1: Write the failing test**

Create `tests/test_dedup.py`:

```python
"""Tests for the dedup module."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import resonance.dedup as dedup_module


class TestDedupAll:
    """Tests for dedup_all() orchestration."""

    @pytest.mark.asyncio
    async def test_calls_all_three_in_order(self) -> None:
        """Runs artist dedup, then track dedup, then event dedup."""
        session = AsyncMock()
        call_order: list[str] = []

        async def mock_artists(s: object) -> dedup_module.MergeStats:
            call_order.append("artists")
            return dedup_module.MergeStats(artists_merged=2, tracks_repointed=3)

        async def mock_tracks(s: object) -> dedup_module.MergeStats:
            call_order.append("tracks")
            return dedup_module.MergeStats(tracks_merged=5, events_repointed=10)

        async def mock_events(s: object) -> int:
            call_order.append("events")
            return 42

        with (
            patch.object(
                dedup_module,
                "find_and_merge_duplicate_artists",
                side_effect=mock_artists,
            ),
            patch.object(
                dedup_module,
                "find_and_merge_duplicate_tracks",
                side_effect=mock_tracks,
            ),
            patch.object(
                dedup_module,
                "delete_cross_service_duplicate_events",
                side_effect=mock_events,
            ),
        ):
            result = await dedup_module.dedup_all(session)

        assert call_order == ["artists", "tracks", "events"]
        assert result["artists_merged"] == 2
        assert result["tracks_merged"] == 5
        assert result["events_deleted"] == 42

    @pytest.mark.asyncio
    async def test_returns_combined_stats(self) -> None:
        """Result includes stats from all three operations."""
        session = AsyncMock()

        with (
            patch.object(
                dedup_module,
                "find_and_merge_duplicate_artists",
                new_callable=AsyncMock,
                return_value=dedup_module.MergeStats(),
            ),
            patch.object(
                dedup_module,
                "find_and_merge_duplicate_tracks",
                new_callable=AsyncMock,
                return_value=dedup_module.MergeStats(),
            ),
            patch.object(
                dedup_module,
                "delete_cross_service_duplicate_events",
                new_callable=AsyncMock,
                return_value=0,
            ),
        ):
            result = await dedup_module.dedup_all(session)

        assert "artists_merged" in result
        assert "tracks_merged" in result
        assert "events_deleted" in result
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_dedup.py -v`
Expected: FAIL with `AttributeError: module has no attribute 'dedup_all'`

**Step 3: Write the implementation**

Add to `src/resonance/dedup.py` at the end of the file:

```python
async def dedup_all(session: AsyncSession) -> dict[str, int]:
    """Run artist, track, and event dedup in sequence.

    Order matters: artist merges affect track grouping (title + artist_id),
    and track merges affect event dedup (same track + same user).

    Args:
        session: Active database session.

    Returns:
        Combined stats from all three operations.
    """
    artist_stats = await find_and_merge_duplicate_artists(session)
    track_stats = await find_and_merge_duplicate_tracks(session)
    events_deleted = await delete_cross_service_duplicate_events(session)

    result: dict[str, int] = {
        "artists_merged": artist_stats.artists_merged,
        "tracks_repointed": artist_stats.tracks_repointed,
        "artist_relations_repointed": artist_stats.artist_relations_repointed,
        "artist_relations_deleted": artist_stats.artist_relations_deleted,
        "tracks_merged": track_stats.tracks_merged,
        "events_repointed": track_stats.events_repointed,
        "track_relations_repointed": track_stats.track_relations_repointed,
        "track_relations_deleted": track_stats.track_relations_deleted,
        "events_deleted": events_deleted,
    }

    logger.info("dedup_all_complete", **result)
    return result
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_dedup.py -v`
Expected: PASS

**Step 5: Commit**

```bash
git add src/resonance/dedup.py tests/test_dedup.py
git commit -m "feat: add dedup_all() for sequential artist/track/event dedup

Runs artist dedup first (affects track grouping), then track dedup
(affects event dedup), then event dedup. Returns combined stats.

Part of #26"
```

---

### Task 2: Wire `dedup_all` into worker and auto-trigger

**Files:**
- Modify: `src/resonance/worker.py`
- Modify: `tests/test_worker.py`

**Step 1: Add `dedup_all` operation to `run_bulk_job`**

In `src/resonance/worker.py`, in the `run_bulk_job` function (around line 363), add a new `elif` branch after the `dedup_events` branch:

```python
            elif operation == "dedup_all":
                result_dict = await dedup_module.dedup_all(session)
                task.result = result_dict
```

**Step 2: Change auto-trigger from `dedup_events` to `dedup_all`**

In `_check_parent_completion` (around line 517-532), change:

```python
        params={"operation": "dedup_events"},
        description="Post-sync event dedup",
```

to:

```python
        params={"operation": "dedup_all"},
        description="Post-sync entity resolution",
```

**Step 3: Write tests**

Add to `tests/test_worker.py` or a new test section:

```python
class TestBulkJobDedupAll:
    """Tests for the dedup_all bulk operation."""

    @pytest.mark.asyncio
    async def test_dedup_all_operation(self) -> None:
        """run_bulk_job dispatches dedup_all and stores combined result."""
        from unittest.mock import patch

        import resonance.dedup as dedup_module

        mock_result = {
            "artists_merged": 1,
            "tracks_merged": 2,
            "events_deleted": 3,
            "tracks_repointed": 0,
            "events_repointed": 0,
            "artist_relations_repointed": 0,
            "artist_relations_deleted": 0,
            "track_relations_repointed": 0,
            "track_relations_deleted": 0,
        }

        task = task_module.Task(
            id=uuid.uuid4(),
            task_type=types_module.TaskType.BULK_JOB,
            status=types_module.SyncStatus.PENDING,
            params={"operation": "dedup_all"},
        )

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        mock_session_factory = MagicMock(return_value=mock_session)

        with patch.object(
            worker_module,
            "_load_task",
            new_callable=AsyncMock,
            return_value=task,
        ), patch.object(
            dedup_module,
            "dedup_all",
            new_callable=AsyncMock,
            return_value=mock_result,
        ) as mock_dedup_all:
            ctx: dict[str, Any] = {
                "session_factory": mock_session_factory,
                "redis": AsyncMock(),
                "job_id": f"bulk:{task.id}",
            }
            await worker_module.run_bulk_job(ctx, str(task.id))

        mock_dedup_all.assert_called_once()
        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result == mock_result
```

**Step 4: Run tests**

Run: `uv run pytest tests/test_worker.py tests/test_dedup.py -v`
Expected: PASS

**Step 5: Run full quality checks**

Run: `uv run ruff check . && uv run mypy src/ && uv run pytest`

**Step 6: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: auto-run full entity resolution after sync completion

Changes post-sync auto-trigger from dedup_events to dedup_all, which
runs artists -> tracks -> events in sequence. Adds dedup_all as a
recognized bulk job operation.

Part of #26"
```

---

### Task 3: Service link badge template macro

**Files:**
- Create: `src/resonance/templates/partials/service_badges.html`

**Step 1: Create the badge macro**

This is a Jinja2 macro that generates service link badges for an entity's `service_links` dict.

```html
{#
  Service link badges — colored abbreviation badges linking to external services.
  
  Usage:
    {% from "partials/service_badges.html" import service_badge_headers, service_badge_cells %}
    
    In <thead>: {{ service_badge_headers() }}
    In <tbody>: {{ service_badge_cells(entity.service_links, entity_type="artist", entity_name=entity.name) }}
#}

{% macro service_badge_headers() %}
<th style="width: 3em; text-align: center;">SP</th>
<th style="width: 3em; text-align: center;">LF</th>
<th style="width: 3em; text-align: center;">LB</th>
{% endmacro %}

{% macro service_badge_cells(service_links, entity_type="artist", entity_name="", track_title="") %}
{# Spotify #}
<td style="text-align: center;">
    {% if service_links and service_links.get("spotify") %}
    <a href="https://open.spotify.com/{{ entity_type }}/{{ service_links['spotify'] }}"
       target="_blank" rel="noopener"
       style="background: #1DB954; color: white; padding: 2px 6px; border-radius: 4px; text-decoration: none; font-size: 0.75em; font-weight: bold;">SP</a>
    {% endif %}
</td>
{# Last.fm #}
<td style="text-align: center;">
    {% if service_links and service_links.get("lastfm") is not none %}
    {% if entity_type == "track" and track_title %}
    <a href="https://www.last.fm/music/{{ entity_name | urlencode }}/_/{{ track_title | urlencode }}"
       target="_blank" rel="noopener"
       style="background: #D51007; color: white; padding: 2px 6px; border-radius: 4px; text-decoration: none; font-size: 0.75em; font-weight: bold;">LF</a>
    {% else %}
    <a href="https://www.last.fm/music/{{ entity_name | urlencode }}"
       target="_blank" rel="noopener"
       style="background: #D51007; color: white; padding: 2px 6px; border-radius: 4px; text-decoration: none; font-size: 0.75em; font-weight: bold;">LF</a>
    {% endif %}
    {% endif %}
</td>
{# ListenBrainz #}
<td style="text-align: center;">
    {% if service_links and service_links.get("listenbrainz") %}
    {% if entity_type == "track" %}
    <a href="https://musicbrainz.org/recording/{{ service_links['listenbrainz'] }}"
       target="_blank" rel="noopener"
       style="background: #E66000; color: white; padding: 2px 6px; border-radius: 4px; text-decoration: none; font-size: 0.75em; font-weight: bold;">LB</a>
    {% else %}
    <a href="https://listenbrainz.org/artist/{{ service_links['listenbrainz'] }}"
       target="_blank" rel="noopener"
       style="background: #E66000; color: white; padding: 2px 6px; border-radius: 4px; text-decoration: none; font-size: 0.75em; font-weight: bold;">LB</a>
    {% endif %}
    {% endif %}
</td>
{% endmacro %}
```

**Step 2: Commit**

```bash
git add src/resonance/templates/partials/service_badges.html
git commit -m "feat: add service badge Jinja2 macro for artist/track list pages

Colored clickable badges (SP green, LF red, LB orange) linking to
entities on Spotify, Last.fm, and ListenBrainz. Fixed-width columns
for consistent alignment.

Part of #26"
```

---

### Task 4: Update artist list template

**Files:**
- Modify: `src/resonance/templates/partials/artist_list.html`

**Step 1: Replace the Services column with badge columns**

Replace the current content of `artist_list.html` with:

```html
{% from "partials/service_badges.html" import service_badge_headers, service_badge_cells %}
{% if artists %}
<figure>
    <table>
        <thead>
            <tr>
                <th>Name</th>
                {{ service_badge_headers() }}
            </tr>
        </thead>
        <tbody>
            {% for artist in artists %}
            <tr>
                <td>{{ artist.name }}</td>
                {{ service_badge_cells(artist.service_links, entity_type="artist", entity_name=artist.name) }}
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
<nav>
    {% if has_prev %}
    <a href="/artists?page={{ page - 1 }}"
       hx-get="/artists?page={{ page - 1 }}"
       hx-target="#artist-list"
       hx-swap="innerHTML"
       role="button"
       class="secondary">Previous</a>
    {% endif %}
    {% if has_next %}
    <a href="/artists?page={{ page + 1 }}"
       hx-get="/artists?page={{ page + 1 }}"
       hx-target="#artist-list"
       hx-swap="innerHTML"
       role="button">Next</a>
    {% endif %}
</nav>
{% else %}
<p>No artists synced yet.</p>
{% endif %}
```

**Step 2: Commit**

```bash
git add src/resonance/templates/partials/artist_list.html
git commit -m "feat: replace artist Services column with clickable service badges

Fixed-width SP/LF/LB columns with colored badges linking to the
artist on each external service.

Part of #26"
```

---

### Task 5: Update track list template

**Files:**
- Modify: `src/resonance/templates/partials/track_list.html`

**Step 1: Replace the Services column with badge columns**

Replace the current content of `track_list.html` with:

```html
{% from "partials/service_badges.html" import service_badge_headers, service_badge_cells %}
{% if tracks %}
<figure>
    <table>
        <thead>
            <tr>
                <th>Title</th>
                <th>Artist</th>
                {{ service_badge_headers() }}
            </tr>
        </thead>
        <tbody>
            {% for track in tracks %}
            <tr>
                <td>{{ track.title }}</td>
                <td>{{ track.artist.name }}</td>
                {{ service_badge_cells(track.service_links, entity_type="track", entity_name=track.artist.name, track_title=track.title) }}
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
<nav>
    {% if has_prev %}
    <a href="/tracks?page={{ page - 1 }}"
       hx-get="/tracks?page={{ page - 1 }}"
       hx-target="#track-list"
       hx-swap="innerHTML"
       role="button"
       class="secondary">Previous</a>
    {% endif %}
    {% if has_next %}
    <a href="/tracks?page={{ page + 1 }}"
       hx-get="/tracks?page={{ page + 1 }}"
       hx-target="#track-list"
       hx-swap="innerHTML"
       role="button">Next</a>
    {% endif %}
</nav>
{% else %}
<p>No tracks synced yet.</p>
{% endif %}
```

**Step 2: Commit**

```bash
git add src/resonance/templates/partials/track_list.html
git commit -m "feat: replace track Services column with clickable service badges

Fixed-width SP/LF/LB columns with colored badges linking to the
track on each external service. Uses artist name for Last.fm URLs.

Part of #26"
```

---

### Task 6: Final verification

**Step 1: Run full quality checks**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/ && uv run pytest`

**Step 2: Verify templates render (manual check)**

Deploy and visually check:
- Artist list page shows SP/LF/LB columns with badges
- Track list page shows SP/LF/LB columns with badges
- Badge links open correct external URLs in new tabs
- Empty cells where no service link exists

**Step 3: Trigger a sync and verify auto dedup_all runs**

```bash
uv run resonance-api sync listenbrainz
# Wait for completion, then check status — should show "Post-sync entity resolution" task
uv run resonance-api status
```

**Step 4: Create PR or merge**

Reference: Fixes #26
