"""Generic iCal connector — URL-based calendar feed sync."""

from __future__ import annotations

import resonance.connectors.base as base_module
import resonance.types as types_module


class ICalConnector:
    """Minimal connector for generic iCal feed connections."""

    service_type = types_module.ServiceType.ICAL

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        """Return the connection configuration for generic iCal feeds."""
        return base_module.ConnectionConfig(
            auth_type="url",
            sync_function="sync_calendar_feed",
            sync_style="full",
        )
