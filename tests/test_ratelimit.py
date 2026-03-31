"""Tests for the rate limit budget manager."""

from unittest import mock

import resonance.connectors.ratelimit as ratelimit_module


class TestRateLimitBudget:
    """Tests for RateLimitBudget."""

    def test_initial_state_allows_requests(self) -> None:
        """A fresh budget with no data should allow requests."""
        budget = ratelimit_module.RateLimitBudget()
        assert budget.can_proceed() is True
        assert budget.remaining is None
        assert budget.reset_in is None

    def test_update_from_headers(self) -> None:
        """Update from X-RateLimit-Remaining and X-RateLimit-Reset-In headers."""
        budget = ratelimit_module.RateLimitBudget()
        budget.update_from_headers(
            {
                "X-RateLimit-Remaining": "42",
                "X-RateLimit-Reset-In": "60",
            }
        )
        assert budget.remaining == 42
        assert budget.reset_in is not None
        assert budget.reset_in <= 60.0

    def test_paced_interval_spreads_requests(self) -> None:
        """With 10 remaining and 30s reset, interval should be ~3.0s."""
        budget = ratelimit_module.RateLimitBudget()
        with mock.patch("time.monotonic", return_value=100.0):
            budget.update(remaining=10, reset_in=30.0)
        with mock.patch("time.monotonic", return_value=100.0):
            interval = budget.paced_interval()
        assert interval == 3.0

    def test_paced_interval_with_no_remaining(self) -> None:
        """With 0 remaining, interval should equal the full reset time."""
        budget = ratelimit_module.RateLimitBudget()
        with mock.patch("time.monotonic", return_value=100.0):
            budget.update(remaining=0, reset_in=15.0)
        with mock.patch("time.monotonic", return_value=100.0):
            interval = budget.paced_interval()
        assert interval == 15.0

    def test_paced_interval_uses_default_when_no_data(self) -> None:
        """With no rate limit data, return default_interval."""
        budget = ratelimit_module.RateLimitBudget(default_interval=1.0)
        interval = budget.paced_interval()
        assert interval == 1.0

    def test_can_proceed_false_when_exhausted(self) -> None:
        """can_proceed should be False when remaining is 0."""
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=0, reset_in=10.0)
        assert budget.can_proceed() is False

    def test_high_priority_bypasses_pacing(self) -> None:
        """High priority returns 0.0 when remaining > 0."""
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=10, reset_in=30.0)
        interval = budget.paced_interval(high_priority=True)
        assert interval == 0.0

    def test_high_priority_blocked_when_exhausted(self) -> None:
        """High priority still waits when remaining is 0."""
        budget = ratelimit_module.RateLimitBudget()
        with mock.patch("time.monotonic", return_value=100.0):
            budget.update(remaining=0, reset_in=10.0)
        with mock.patch("time.monotonic", return_value=100.0):
            interval = budget.paced_interval(high_priority=True)
        assert interval == 10.0

    def test_update_from_response_headers(self) -> None:
        """ListenBrainz-style headers should update the budget."""
        budget = ratelimit_module.RateLimitBudget()
        with mock.patch("time.monotonic", return_value=100.0):
            budget.update_from_headers(
                {
                    "X-RateLimit-Remaining": "5",
                    "X-RateLimit-Reset-In": "20",
                }
            )
        assert budget.remaining == 5
        with mock.patch("time.monotonic", return_value=100.0):
            interval = budget.paced_interval()
        assert interval == 4.0  # 20 / 5

    def test_update_from_spotify_headers(self) -> None:
        """Spotify Retry-After header implies remaining=0."""
        budget = ratelimit_module.RateLimitBudget()
        budget.update_from_headers({"Retry-After": "7"})
        assert budget.remaining == 0
        assert budget.reset_in is not None
        assert budget.reset_in <= 7.0
        assert budget.can_proceed() is False

    def test_update_from_empty_headers(self) -> None:
        """Empty headers should be a no-op."""
        budget = ratelimit_module.RateLimitBudget()
        budget.update_from_headers({})
        assert budget.remaining is None
        assert budget.reset_in is None

    def test_reset_in_adjusts_for_elapsed_time(self) -> None:
        """reset_in should decrease as time passes after update."""
        budget = ratelimit_module.RateLimitBudget()
        with mock.patch("time.monotonic", return_value=100.0):
            budget.update(remaining=10, reset_in=30.0)
        with mock.patch("time.monotonic", return_value=110.0):
            assert budget.reset_in is not None
            assert abs(budget.reset_in - 20.0) < 0.1

    def test_reset_in_floors_at_zero(self) -> None:
        """reset_in should not go negative after the window expires."""
        budget = ratelimit_module.RateLimitBudget()
        with mock.patch("time.monotonic", return_value=100.0):
            budget.update(remaining=0, reset_in=5.0)
        with mock.patch("time.monotonic", return_value=110.0):
            assert budget.reset_in is not None
            assert budget.reset_in == 0.0
