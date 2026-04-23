"""Songkick connector — username-based calendar feed sync."""

from __future__ import annotations

import resonance.connectors.base as base_module
import resonance.types as types_module


def derive_songkick_urls(username: str) -> list[str]:
    """Generate Songkick iCal feed URLs from a username."""
    base = f"https://www.songkick.com/users/{username}/calendars.ics"
    return [
        f"{base}?filter=attendance",
        f"{base}?filter=tracked_artist",
    ]


class SongkickConnector:
    """Minimal connector for Songkick calendar feed connections.

    Not a full BaseConnector — Songkick doesn't use OAuth or HTTP client.
    Only provides ConnectionConfig for the unified sync dispatch.
    """

    service_type = types_module.ServiceType.SONGKICK

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        """Return the connection configuration for Songkick."""
        return base_module.ConnectionConfig(
            auth_type="username",
            sync_function="sync_calendar_feed",
            sync_style="full",
            derive_urls=derive_songkick_urls,
        )
