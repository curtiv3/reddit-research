from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from sandcastle.common.time import now_timestamp


@dataclass(frozen=True)
class TimeWindow:
    label: str
    seconds: int


def build_windows(labels: Iterable[str]) -> list[TimeWindow]:
    mapping = {
        "14d": 14 * 24 * 3600,
        "60d": 60 * 24 * 3600,
        "180d": 180 * 24 * 3600,
        "365d": 365 * 24 * 3600,
    }
    return [TimeWindow(label=label, seconds=mapping[label]) for label in labels if label in mapping]


def window_bounds(window: TimeWindow) -> tuple[int, int]:
    now = now_timestamp()
    return now - window.seconds, now
