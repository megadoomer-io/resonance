"""Shared enumeration types used across the Resonance application."""

import enum


class ServiceType(enum.StrEnum):
    """External music services that Resonance can connect to."""

    SPOTIFY = "spotify"
    LASTFM = "lastfm"
    LISTENBRAINZ = "listenbrainz"
    SONGKICK = "songkick"
    BANDSINTOWN = "bandsintown"
    BANDCAMP = "bandcamp"
    SOUNDCLOUD = "soundcloud"
    ICAL = "ical"
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
    NONE = "none"


class CandidateStatus(enum.StrEnum):
    """Status of an artist-to-event candidate."""

    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
