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


class SyncStatus(enum.StrEnum):
    """Status of a synchronization job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEFERRED = "deferred"
