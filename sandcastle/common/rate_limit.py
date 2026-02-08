from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class RateLimiter:
    min_interval: float
    last_time: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_time = time.monotonic()


def backoff_sleep(attempt: int, base: float = 1.0, cap: float = 30.0) -> None:
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    time.sleep(delay)
