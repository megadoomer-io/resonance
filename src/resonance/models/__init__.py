"""SQLAlchemy models for the Resonance application."""

from resonance.models.base import Base
from resonance.models.concert import (
    EntityExclusion,
    Event,
    EventArtist,
    EventArtistCandidate,
    EventCandidate,
    UserEventAttendance,
    Venue,
    VenueCandidate,
)
from resonance.models.generator import GenerationRecord, GeneratorProfile
from resonance.models.music import Artist, ListeningEvent, Track
from resonance.models.playlist import Playlist, PlaylistTrack
from resonance.models.task import Task
from resonance.models.taste import UserArtistRelation, UserTrackRelation
from resonance.models.user import ServiceConnection, User

__all__ = [
    "Artist",
    "Base",
    "EntityExclusion",
    "Event",
    "EventArtist",
    "EventArtistCandidate",
    "EventCandidate",
    "GenerationRecord",
    "GeneratorProfile",
    "ListeningEvent",
    "Playlist",
    "PlaylistTrack",
    "ServiceConnection",
    "Task",
    "Track",
    "User",
    "UserArtistRelation",
    "UserEventAttendance",
    "UserTrackRelation",
    "Venue",
    "VenueCandidate",
]
