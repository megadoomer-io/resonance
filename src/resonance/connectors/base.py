"""Base connector framework with capability declarations."""

import abc
import enum

import pydantic

import resonance.types as types_module


class ConnectorCapability(enum.StrEnum):
    """Capabilities that a connector can declare support for."""

    AUTHENTICATION = "authentication"
    LISTENING_HISTORY = "listening_history"
    RECOMMENDATIONS = "recommendations"
    PLAYLIST_WRITE = "playlist_write"
    ARTIST_DATA = "artist_data"
    EVENTS = "events"
    FOLLOWS = "follows"
    TRACK_RATINGS = "track_ratings"
    NEW_RELEASES = "new_releases"


class TokenResponse(pydantic.BaseModel):
    """OAuth token response from an external service."""

    access_token: str
    refresh_token: str | None = None
    expires_in: int | None = None
    scope: str | None = None


class SpotifyArtistData(pydantic.BaseModel):
    """Artist data returned from a connector."""

    external_id: str
    name: str
    service: types_module.ServiceType


class SpotifyTrackData(pydantic.BaseModel):
    """Track data returned from a connector."""

    external_id: str
    title: str
    artist_external_id: str
    artist_name: str
    service: types_module.ServiceType


class BaseConnector(abc.ABC):
    """Abstract base class for all service connectors."""

    service_type: types_module.ServiceType
    capabilities: frozenset[ConnectorCapability]

    def has_capability(self, capability: ConnectorCapability) -> bool:
        """Check whether this connector supports a given capability."""
        return capability in self.capabilities
