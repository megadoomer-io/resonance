"""Test service connector for admin testing and development."""

import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module


class TestConnector(base_module.BaseConnector):
    """Fake connector for testing the sync pipeline."""

    service_type = types_module.ServiceType.TEST
    capabilities = frozenset(
        {
            base_module.ConnectorCapability.LISTENING_HISTORY,
        }
    )

    def __init__(self) -> None:
        self._http_client = None
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
