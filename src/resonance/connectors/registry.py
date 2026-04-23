"""Registry for managing connector instances by service type."""

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    import resonance.connectors.base as base_module
    import resonance.types as types_module


@runtime_checkable
class Connectable(Protocol):
    """Minimal interface for anything registerable in the connector registry.

    Both full BaseConnector subclasses and lightweight connectors
    (Songkick, iCal) satisfy this protocol.
    """

    service_type: types_module.ServiceType

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig: ...


class ConnectorRegistry:
    """Stores and retrieves connectors by service type or capability."""

    def __init__(self) -> None:
        self._connectors: dict[types_module.ServiceType, Connectable] = {}

    def register(self, connector: Connectable) -> None:
        """Register a connector instance, keyed by its service type."""
        self._connectors[connector.service_type] = connector

    def get(self, service_type: types_module.ServiceType) -> Connectable | None:
        """Retrieve a connector by service type, or None if not registered."""
        return self._connectors.get(service_type)

    def get_base_connector(
        self, service_type: types_module.ServiceType
    ) -> base_module.BaseConnector | None:
        """Retrieve a full BaseConnector by service type.

        Returns None if the service type is not registered or if
        the registered connector is a lightweight (non-BaseConnector)
        implementation.
        """
        import resonance.connectors.base as base_module_rt

        connector = self._connectors.get(service_type)
        if isinstance(connector, base_module_rt.BaseConnector):
            return connector
        return None

    def get_by_capability(
        self, capability: base_module.ConnectorCapability
    ) -> list[base_module.BaseConnector]:
        """Return all connectors that declare the given capability.

        Only full BaseConnector instances support capabilities;
        lightweight connectors are silently excluded.
        """
        import resonance.connectors.base as base_module_rt

        return [
            c
            for c in self._connectors.values()
            if isinstance(c, base_module_rt.BaseConnector)
            and c.has_capability(capability)
        ]

    def get_config(
        self, service_type: types_module.ServiceType
    ) -> base_module.ConnectionConfig | None:
        """Retrieve a connector's config by service type."""
        connector = self._connectors.get(service_type)
        if connector is None:
            return None
        return connector.connection_config()

    def all(self) -> list[Connectable]:
        """Return all registered connectors."""
        return list(self._connectors.values())
