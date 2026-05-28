"""Concert Archives connector — CSV file upload sync."""

from __future__ import annotations

import resonance.connectors.base as base_module
import resonance.types as types_module


class ConcertArchivesConnector:
    """Minimal connector for Concert Archives CSV import connections."""

    service_type = types_module.ServiceType.CONCERT_ARCHIVES
    display_name = "Concert Archives"
    icon = "archive"
    color = ""

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        """Return the connection configuration for Concert Archives."""
        return base_module.ConnectionConfig(
            auth_type="file_upload",
            sync_function="sync_concert_archives",
            sync_style="full",
        )
