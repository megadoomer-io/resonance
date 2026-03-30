"""Registry for managing connector instances by service type."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import resonance.connectors.base as base_module
    import resonance.types as types_module


class ConnectorRegistry:
    """Stores and retrieves connectors by service type or capability."""

    def __init__(self) -> None:
        self._connectors: dict[types_module.ServiceType, base_module.BaseConnector] = {}

    def register(self, connector: base_module.BaseConnector) -> None:
        """Register a connector instance, keyed by its service type."""
        self._connectors[connector.service_type] = connector

    def get(
        self, service_type: types_module.ServiceType
    ) -> base_module.BaseConnector | None:
        """Retrieve a connector by service type, or None if not registered."""
        return self._connectors.get(service_type)

    def get_by_capability(
        self, capability: base_module.ConnectorCapability
    ) -> list[base_module.BaseConnector]:
        """Return all connectors that declare the given capability."""
        return [c for c in self._connectors.values() if c.has_capability(capability)]

    def all(self) -> list[base_module.BaseConnector]:
        """Return all registered connectors."""
        return list(self._connectors.values())
