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
import resonance.models.playlist as playlist_models
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


# ---------------------------------------------------------------------------
# Artist filter fields
# ---------------------------------------------------------------------------

_has_events_subquery: sa.Select[Any] = (
    sa.select(concert_models.EventArtist.id)
    .where(concert_models.EventArtist.artist_id == music_models.Artist.id)
    .correlate(music_models.Artist)
)

_has_tracks_subquery: sa.Select[Any] = (
    sa.select(music_models.Track.id)
    .where(music_models.Track.artist_id == music_models.Artist.id)
    .correlate(music_models.Artist)
)

ARTIST_FILTERS: list[filters_module.AnyFilterField] = [
    filters_module.TextField("name", music_models.Artist.name),
    filters_module.TextField("origin", music_models.Artist.origin),
    filters_module.ExistsField("has_events", _has_events_subquery),
    filters_module.ExistsField("has_tracks", _has_tracks_subquery),
]

ARTIST_PRESETS: list[dict[str, str]] = [
    {"name": "has_events", "label": "Has Events", "params": "has_events=true"},
    {"name": "no_tracks", "label": "No Tracks", "params": "has_tracks=false"},
]

ARTIST_TEMPLATE_FILTERS: list[dict[str, Any]] = [
    {"name": "name", "label": "Name", "type": "text"},
    {"name": "origin", "label": "Origin", "type": "text"},
]


# ---------------------------------------------------------------------------
# Track filter fields
# ---------------------------------------------------------------------------

_recently_played_subquery: sa.Select[Any] = (
    sa.select(music_models.ListeningEvent.id)
    .where(
        music_models.ListeningEvent.track_id == music_models.Track.id,
        music_models.ListeningEvent.listened_at
        >= sa.func.now() - datetime.timedelta(days=30),
    )
    .correlate(music_models.Track)
)

TRACK_FILTERS: list[filters_module.AnyFilterField] = [
    filters_module.TextField("title", music_models.Track.title),
    filters_module.TextField("artist", music_models.Artist.name),
    filters_module.ExistsField("recently_played", _recently_played_subquery),
]

TRACK_PRESETS: list[dict[str, str]] = [
    {
        "name": "recently_played",
        "label": "Recently Played",
        "params": "recently_played=true",
    },
]

TRACK_TEMPLATE_FILTERS: list[dict[str, Any]] = [
    {"name": "title", "label": "Title", "type": "text"},
    {"name": "artist", "label": "Artist", "type": "text"},
]


# ---------------------------------------------------------------------------
# Listening history filter fields
# ---------------------------------------------------------------------------

HISTORY_FILTERS: list[filters_module.AnyFilterField] = [
    filters_module.TextField("track", music_models.Track.title),
    filters_module.TextField("artist", music_models.Artist.name),
    filters_module.DateRangeField("date", music_models.ListeningEvent.listened_at),
]

HISTORY_PRESETS: list[dict[str, str]] = [
    {"name": "spotify", "label": "Spotify", "params": "source=SPOTIFY"},
    {"name": "listenbrainz", "label": "ListenBrainz", "params": "source=LISTENBRAINZ"},
    {"name": "lastfm", "label": "Last.fm", "params": "source=LASTFM"},
]

HISTORY_TEMPLATE_FILTERS: list[dict[str, Any]] = [
    {"name": "track", "label": "Track", "type": "text"},
    {"name": "artist", "label": "Artist", "type": "text"},
    {
        "name": "source",
        "label": "Source",
        "type": "multiselect",
        "options": [
            {"value": "SPOTIFY", "label": "Spotify"},
            {"value": "LISTENBRAINZ", "label": "ListenBrainz"},
            {"value": "LASTFM", "label": "Last.fm"},
        ],
    },
    {"name": "date", "label": "Date", "type": "daterange"},
]


# ---------------------------------------------------------------------------
# Playlist filter fields
# ---------------------------------------------------------------------------

PLAYLIST_FILTERS: list[filters_module.AnyFilterField] = [
    filters_module.TextField("name", playlist_models.Playlist.name),
    filters_module.DateRangeField("created", playlist_models.Playlist.created_at),
    filters_module.NumericRangeField("tracks", playlist_models.Playlist.track_count),
]

PLAYLIST_PRESETS: list[dict[str, str]] = []

PLAYLIST_TEMPLATE_FILTERS: list[dict[str, Any]] = [
    {"name": "name", "label": "Name", "type": "text"},
    {"name": "created", "label": "Created", "type": "daterange"},
    {"name": "tracks", "label": "Tracks", "type": "numericrange"},
]


# ---------------------------------------------------------------------------
# Generic preset / query-string helpers
# ---------------------------------------------------------------------------


def detect_active_preset(
    params: dict[str, str],
    presets: list[dict[str, str]],
    *,
    filter_keys: set[str] | None = None,
    default_preset: str | None = None,
) -> str | None:
    """Return the name of the preset whose params match *params*, or None.

    Args:
        params: Current query parameters.
        presets: List of preset definitions.
        filter_keys: All query-parameter keys considered "filter" params for
            this view.  When omitted, keys are derived from presets plus ``q``.
        default_preset: Preset name to return when no filters are active.
            Pass ``None`` (the default) to return ``None`` when there are no
            matching filters.
    """
    # Derive filter keys from presets if not provided
    if filter_keys is None:
        filter_keys = {"q"}
        for preset in presets:
            for pair in preset["params"].split("&"):
                if "=" in pair:
                    k, _v = pair.split("=", 1)
                    filter_keys.add(k)

    has_any = any(params.get(k, "").strip() for k in filter_keys)
    if not has_any:
        return default_preset

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
        elif isinstance(field, filters_module.NumericRangeField):
            min_val = value.get(f"{field.name}_min")
            max_val = value.get(f"{field.name}_max")
            if min_val is not None:
                parts.append(f"{field.name}_min={min_val}")
            if max_val is not None:
                parts.append(f"{field.name}_max={max_val}")
        elif isinstance(field, filters_module.MultiSelectField):
            for v in value:
                parts.append(f"{field.name}={v}")
        elif isinstance(field, filters_module.ExistsField):
            parts.append(f"{field.name}={'true' if value else 'false'}")

    # Include quick search if present
    # (handled separately since it's not a registered field)
    return "&".join(parts)
