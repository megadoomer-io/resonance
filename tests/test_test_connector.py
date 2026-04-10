"""Tests for the test service connector."""

import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.connectors.test as test_module
import resonance.types as types_module


class TestTestConnector:
    """Tests for TestConnector."""

    def test_service_type(self) -> None:
        connector = test_module.TestConnector()
        assert connector.service_type == types_module.ServiceType.TEST

    def test_capabilities(self) -> None:
        connector = test_module.TestConnector()
        assert (
            base_module.ConnectorCapability.LISTENING_HISTORY in connector.capabilities
        )

    def test_http_client_is_none(self) -> None:
        connector = test_module.TestConnector()
        assert connector._http_client is None

    def test_budget_has_zero_interval(self) -> None:
        connector = test_module.TestConnector()
        assert isinstance(connector._budget, ratelimit_module.RateLimitBudget)

    def test_has_capability_listening_history(self) -> None:
        connector = test_module.TestConnector()
        assert connector.has_capability(
            base_module.ConnectorCapability.LISTENING_HISTORY
        )

    def test_has_capability_authentication_false(self) -> None:
        connector = test_module.TestConnector()
        assert not connector.has_capability(
            base_module.ConnectorCapability.AUTHENTICATION
        )
