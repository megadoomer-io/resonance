"""Rate limit budget manager with priority lanes.

Tracks remaining API request budget from response headers and paces
requests to spread them evenly across the rate limit window.
"""

import time


class RateLimitBudget:
    """Tracks rate limit budget and computes paced request intervals.

    Args:
        default_interval: Fallback delay between requests when no rate
            limit data is available.
        window_seconds: Rolling window duration for budget tracking.
            None disables window tracking.
        window_ceiling: Maximum requests allowed within the rolling window.
            None disables window tracking.
    """

    def __init__(
        self,
        default_interval: float = 0.2,
        window_seconds: float | None = None,
        window_ceiling: int | None = None,
    ) -> None:
        self._default_interval = default_interval
        self._remaining: int | None = None
        self._reset_in: float | None = None
        self._last_update: float | None = None
        self._window_seconds = window_seconds
        self._window_ceiling = window_ceiling
        self._request_timestamps: list[float] = []

    @property
    def remaining(self) -> int | None:
        """Current remaining requests, or None if no data."""
        return self._remaining

    @property
    def reset_in(self) -> float | None:
        """Seconds until reset, adjusted for elapsed time since last update.

        Returns None if no rate limit data is available. Never goes below 0.
        """
        if self._reset_in is None or self._last_update is None:
            return None
        elapsed = time.monotonic() - self._last_update
        return max(0.0, self._reset_in - elapsed)

    @property
    def window_ceiling(self) -> int | None:
        """Configured window ceiling, or None if tracking disabled."""
        return self._window_ceiling

    @property
    def window_seconds(self) -> float | None:
        """Configured window duration in seconds, or None if tracking disabled."""
        return self._window_seconds

    @property
    def window_used(self) -> int | None:
        """Current requests in window, or None if tracking disabled."""
        if self._window_seconds is None:
            return None
        self._prune_window(time.monotonic())
        return len(self._request_timestamps)

    def record_request(self) -> None:
        """Record that a request was made. Prunes old timestamps."""
        if self._window_seconds is None:
            return
        now = time.monotonic()
        self._request_timestamps.append(now)
        self._prune_window(now)

    def check_window_budget(self) -> float:
        """Return seconds to wait if at window ceiling, else 0."""
        if self._window_seconds is None or self._window_ceiling is None:
            return 0.0
        now = time.monotonic()
        self._prune_window(now)
        if len(self._request_timestamps) < self._window_ceiling:
            return 0.0
        # At ceiling — wait until oldest request ages out
        oldest = self._request_timestamps[0]
        return max(0.0, self._window_seconds - (now - oldest))

    def _prune_window(self, now: float) -> None:
        """Remove timestamps older than window_seconds."""
        if self._window_seconds is None:
            return
        cutoff = now - self._window_seconds
        self._request_timestamps = [
            ts for ts in self._request_timestamps if ts >= cutoff
        ]

    def update(self, remaining: int, reset_in: float) -> None:
        """Update budget from known values.

        Args:
            remaining: Number of requests remaining in the current window.
            reset_in: Seconds until the rate limit window resets.
        """
        self._remaining = remaining
        self._reset_in = reset_in
        self._last_update = time.monotonic()

    def update_from_headers(self, headers: dict[str, str]) -> None:
        """Parse rate limit info from HTTP response headers.

        Supports two header styles:

        - ListenBrainz: ``X-RateLimit-Remaining`` + ``X-RateLimit-Reset-In``
        - Spotify: ``Retry-After`` (implies remaining=0)

        If no recognised headers are present this is a no-op.

        Args:
            headers: Response headers as a string-keyed dict.
        """
        remaining_val = headers.get("X-RateLimit-Remaining")
        reset_in_val = headers.get("X-RateLimit-Reset-In")

        if remaining_val is not None and reset_in_val is not None:
            self.update(
                remaining=int(remaining_val),
                reset_in=float(reset_in_val),
            )
            return

        retry_after_val = headers.get("Retry-After")
        if retry_after_val is not None:
            self.update(remaining=0, reset_in=float(retry_after_val))
            return

    def can_proceed(self) -> bool:
        """Return True if a request can be made without waiting.

        When no rate limit data is available, defaults to True.
        """
        if self._remaining is None:
            return True
        return self._remaining > 0

    def paced_interval(self, high_priority: bool = False) -> float:
        """Compute seconds to wait before the next request.

        Args:
            high_priority: If True and budget remains, skip pacing (return 0).

        Returns:
            Seconds to wait. Zero means proceed immediately.
        """
        current_reset_in = self.reset_in

        # No rate limit data available
        if self._remaining is None or current_reset_in is None:
            return 0.0 if high_priority else self._default_interval

        # Budget exhausted — must wait regardless of priority
        if self._remaining == 0:
            return current_reset_in

        # Budget available + high priority — go immediately
        if high_priority:
            return 0.0

        # Budget available + normal priority — spread evenly
        return current_reset_in / self._remaining
