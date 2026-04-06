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


class TestRollingWindowBudget:
    """Tests for rolling window budget tracking."""

    def test_window_disabled_by_default(self) -> None:
        """Without window params, window tracking is off."""
        budget = ratelimit_module.RateLimitBudget()
        assert budget.window_used is None
        assert budget.window_ceiling is None
        assert budget.window_seconds is None
        assert budget.check_window_budget() == 0.0

    def test_record_request_appends_timestamp(self) -> None:
        """record_request should add timestamps when window is configured."""
        budget = ratelimit_module.RateLimitBudget(window_seconds=30, window_ceiling=10)
        with mock.patch("time.monotonic", return_value=100.0):
            budget.record_request()
            assert budget.window_used == 1

    def test_record_request_noop_when_disabled(self) -> None:
        """record_request is a no-op when window tracking is disabled."""
        budget = ratelimit_module.RateLimitBudget()
        budget.record_request()
        assert budget.window_used is None

    def test_check_window_budget_under_ceiling(self) -> None:
        """Returns 0 when request count is below the ceiling."""
        budget = ratelimit_module.RateLimitBudget(window_seconds=30, window_ceiling=10)
        with mock.patch("time.monotonic", return_value=100.0):
            for _ in range(5):
                budget.record_request()
            assert budget.check_window_budget() == 0.0

    def test_check_window_budget_at_ceiling(self) -> None:
        """Returns wait time when at ceiling."""
        budget = ratelimit_module.RateLimitBudget(window_seconds=30, window_ceiling=3)
        # Record 3 requests at t=100
        with mock.patch("time.monotonic", return_value=100.0):
            for _ in range(3):
                budget.record_request()

        # At t=110, oldest is 10s old; need to wait 20s more for it to age out
        with mock.patch("time.monotonic", return_value=110.0):
            wait = budget.check_window_budget()
        assert wait == 20.0

    def test_window_used_returns_count(self) -> None:
        """window_used returns the number of requests in the window."""
        budget = ratelimit_module.RateLimitBudget(window_seconds=30, window_ceiling=10)
        with mock.patch("time.monotonic", return_value=100.0):
            budget.record_request()
            budget.record_request()
            budget.record_request()
        with mock.patch("time.monotonic", return_value=100.0):
            assert budget.window_used == 3

    def test_window_used_returns_none_when_disabled(self) -> None:
        """window_used returns None when window tracking is disabled."""
        budget = ratelimit_module.RateLimitBudget()
        assert budget.window_used is None

    def test_old_timestamps_pruned(self) -> None:
        """Timestamps older than window_seconds are pruned."""
        budget = ratelimit_module.RateLimitBudget(window_seconds=10, window_ceiling=100)
        # Record at t=100
        with mock.patch("time.monotonic", return_value=100.0):
            budget.record_request()
            budget.record_request()

        # Record at t=115 — the t=100 entries should be pruned (older than 10s)
        with mock.patch("time.monotonic", return_value=115.0):
            budget.record_request()
            assert budget.window_used == 1

    def test_window_ceiling_property(self) -> None:
        """window_ceiling property returns the configured ceiling."""
        budget = ratelimit_module.RateLimitBudget(window_seconds=30, window_ceiling=10)
        assert budget.window_ceiling == 10

    def test_window_seconds_property(self) -> None:
        """window_seconds property returns the configured window."""
        budget = ratelimit_module.RateLimitBudget(window_seconds=30, window_ceiling=10)
        assert budget.window_seconds == 30
