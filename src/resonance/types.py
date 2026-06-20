"""Shared enumeration types used across the Resonance application."""

import enum


class ServiceType(enum.StrEnum):
    """Data sources that Resonance can connect to or receive data from."""

    SPOTIFY = "spotify"
    LASTFM = "lastfm"
    LISTENBRAINZ = "listenbrainz"
    SONGKICK = "songkick"
    BANDSINTOWN = "bandsintown"
    BANDCAMP = "bandcamp"
    SOUNDCLOUD = "soundcloud"
    ICAL = "ical"
    CONCERT_ARCHIVES = "concert_archives"
    GITHUB = "github"
    MANUAL = "manual"
    TEST = "test"


class ArtistRelationType(enum.StrEnum):
    """Types of relationships a user can have with an artist."""

    FOLLOW = "follow"
    FAVORITE = "favorite"


class TrackRelationType(enum.StrEnum):
    """Types of relationships a user can have with a track."""

    LIKE = "like"
    LOVE = "love"


class UserRole(enum.StrEnum):
    """User authorization roles."""

    USER = "user"
    ADMIN = "admin"
    OWNER = "owner"


class SyncType(enum.StrEnum):
    """Types of data synchronization jobs."""

    FULL = "full"
    INCREMENTAL = "incremental"


class TaskType(enum.StrEnum):
    """Types of tasks in the task queue."""

    SYNC_JOB = "sync_job"
    TIME_RANGE = "time_range"
    PAGE_FETCH = "page_fetch"
    BULK_JOB = "bulk_job"
    CALENDAR_SYNC = "calendar_sync"
    PLAYLIST_GENERATION = "playlist_generation"
    TRACK_DISCOVERY = "track_discovery"
    TRACK_SCORING = "track_scoring"
    CONCERT_ARCHIVES_IMPORT = "concert_archives_import"
    CONCERT_ARCHIVES_CHUNK = "concert_archives_chunk"
    PLAYLIST_EXPORT = "playlist_export"
    MBID_BACKFILL = "mbid_backfill"
    POPULARITY_BACKFILL = "popularity_backfill"


class SyncStatus(enum.StrEnum):
    """Status of a synchronization job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEFERRED = "deferred"


class FeedType(enum.StrEnum):
    """Types of calendar feeds."""

    SONGKICK_ATTENDANCE = "songkick_attendance"
    SONGKICK_TRACKED_ARTIST = "songkick_tracked_artist"
    ICAL_GENERIC = "ical_generic"


class AttendanceStatus(enum.StrEnum):
    """User attendance status for an event."""

    GOING = "going"
    INTERESTED = "interested"
    NOT_GOING = "not_going"


class CandidateStatus(enum.StrEnum):
    """Resolution status for source candidates matched to canonical entities."""

    PENDING = "pending"
    AUTO_ACCEPTED = "auto_accepted"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class GeneratorType(enum.StrEnum):
    """Types of playlist generators."""

    CONCERT_PREP = "concert_prep"
    ARTIST_DEEP_DIVE = "artist_deep_dive"
    REDISCOVERY = "rediscovery"
    DISCOGRAPHY = "discography"
    PLAYLIST_REFRESH = "playlist_refresh"
    CURATED_MIX = "curated_mix"


class ParameterScaleType(enum.StrEnum):
    """Scale types for generator parameters."""

    BIPOLAR = "bipolar"
    UNIPOLAR = "unipolar"


class TrackSource(enum.StrEnum):
    """How a track was sourced for a playlist."""

    LIBRARY = "library"
    DISCOVERY = "discovery"
    MANUAL = "manual"


class MatchStatus(enum.StrEnum):
    """Outcome of a MusicBrainz MBID backfill attempt for an artist or track.

    Stored in Artist/Track.mb_match_status. A NULL column (no member) means the
    entity has not been attempted yet (resume key). MATCHED means an MBID was
    written; NO_MATCH means the mapper returned nothing; BELOW_SIMILARITY means a
    candidate was returned but rejected by the name-similarity gate. Transient
    errors leave mb_attempted_at / mb_match_status NULL so they are retried.

    Note: SQLAlchemy stores the enum .name (UPPERCASE) in the column, so DB-level
    CHECK constraints and raw SQL must use 'MATCHED' / 'NO_MATCH' /
    'BELOW_SIMILARITY', not the lowercase values.
    """

    MATCHED = "matched"
    NO_MATCH = "no_match"
    BELOW_SIMILARITY = "below_similarity"
