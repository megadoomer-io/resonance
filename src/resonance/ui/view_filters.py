"""Per-view filter registries.

Each view defines its filter fields, presets, and template metadata so the
route handler can parse query params, build SQL clauses, and render the
filter bar.
"""

from __future__ import annotations

import datetime
from typing import Any

import sqlalchemy as sa

import resonance.models.concert as concert_models
import resonance.models.music as music_models
import resonance.ui.filters as filters_module

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _today_iso() -> str:
    """Return today's date as an ISO string."""
    return datetime.date.today().isoformat()


# ---------------------------------------------------------------------------
# Event filter fields
# ---------------------------------------------------------------------------

# ExistsField for "has pending candidates" — correlates against the outer
# Event.id via a scalar subquery.  The `correlate(concert_models.Event)`
# call ensures SQLAlchemy treats Event as the outer reference.
_pending_candidates_subquery: sa.Select[Any] = (
    sa.select(concert_models.EventArtistCandidate.id)
    .where(
        concert_models.EventArtistCandidate.event_id == concert_models.Event.id,
        concert_models.EventArtistCandidate.status == "PENDING",
    )
    .correlate(concert_models.Event)
)

EVENT_FILTERS: list[filters_module.AnyFilterField] = [
    filters_module.TextField("title", concert_models.Event.title),
    filters_module.TextField("venue", concert_models.Venue.name),
    filters_module.TextField("artist", music_models.Artist.name),
    filters_module.DateRangeField("date", concert_models.Event.event_date),
    filters_module.ExistsField("has_pending", _pending_candidates_subquery),
]


def build_event_presets() -> list[dict[str, str]]:
    """Return event presets with today's date resolved dynamically."""
    return [
        {
            "name": "upcoming",
            "label": "Upcoming",
            "params": f"date_from={_today_iso()}",
        },
        {
            "name": "going",
            "label": "Going",
            "params": "attendance=GOING",
        },
        {
            "name": "needs_review",
            "label": "Needs Review",
            "params": "has_pending=true",
        },
    ]


EVENT_TEMPLATE_FILTERS: list[dict[str, Any]] = [
    {"name": "title", "label": "Title", "type": "text"},
    {"name": "date", "label": "Date", "type": "daterange"},
    {"name": "venue", "label": "Venue", "type": "text"},
    {"name": "artist", "label": "Artists", "type": "text"},
    {
        "name": "attendance",
        "label": "Attendance",
        "type": "multiselect",
        "options": [
            {"value": "GOING", "label": "Going"},
            {"value": "INTERESTED", "label": "Interested"},
            {"value": "NONE", "label": "None"},
        ],
    },
]


def detect_active_preset(
    params: dict[str, str],
    presets: list[dict[str, str]],
) -> str | None:
    """Return the name of the preset whose params match *params*, or None.

    The "upcoming" preset is the default when no filters are specified at all.
    """
    # If there are no filter-related params at all, default to "upcoming"
    filter_keys = {
        "q",
        "title",
        "venue",
        "artist",
        "date_from",
        "date_to",
        "attendance",
        "has_pending",
    }
    has_any = any(params.get(k, "").strip() for k in filter_keys)
    if not has_any:
        return "upcoming"

    # Check each preset's params against the current params
    for preset in presets:
        preset_params: dict[str, str] = {}
        for pair in preset["params"].split("&"):
            if "=" in pair:
                k, v = pair.split("=", 1)
                preset_params[k] = v

        # All preset params must match, and no other filter keys should be set
        match = True
        for k, v in preset_params.items():
            if params.get(k, "") != v:
                match = False
                break

        if match:
            # Ensure no extra filter params are set beyond the preset's
            extra_keys = filter_keys - set(preset_params.keys())
            if not any(params.get(k, "").strip() for k in extra_keys):
                return preset["name"]

    return None


def build_filter_query_string(
    active_filters: dict[str, Any],
    fields: list[filters_module.AnyFilterField],
) -> str:
    """Build a query string from active filter values for pagination links."""
    parts: list[str] = []
    for field in fields:
        value = active_filters.get(field.name)
        if value is None:
            continue
        if isinstance(field, filters_module.TextField):
            parts.append(f"{field.name}={value}")
        elif isinstance(field, filters_module.DateRangeField):
            from_val = value.get(f"{field.name}_from")
            to_val = value.get(f"{field.name}_to")
            if from_val is not None:
                parts.append(f"{field.name}_from={from_val}")
            if to_val is not None:
                parts.append(f"{field.name}_to={to_val}")
        elif isinstance(field, filters_module.MultiSelectField):
            for v in value:
                parts.append(f"{field.name}={v}")
        elif isinstance(field, filters_module.ExistsField):
            parts.append(f"{field.name}={'true' if value else 'false'}")

    # Include quick search if present
    # (handled separately since it's not a registered field)
    return "&".join(parts)
