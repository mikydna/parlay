"""Simple rate limiter for ingestion requests."""

from __future__ import annotations

import random
import time


class RateLimiter:
    """Token interval limiter using requests-per-minute target."""

    def __init__(self, *, rpm: int, jitter_seconds: float = 0.0) -> None:
        self.rpm = max(1, int(rpm))
        self.jitter_seconds = max(0.0, float(jitter_seconds))
        self._interval = 60.0 / float(self.rpm)
        self._last_ts = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        delta = now - self._last_ts
        remaining = self._interval - delta
        if remaining > 0:
            time.sleep(remaining)
        if self.jitter_seconds > 0:
            time.sleep(random.uniform(0.0, self.jitter_seconds))
        self._last_ts = time.monotonic()
