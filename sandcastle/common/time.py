from __future__ import annotations

from datetime import datetime, timezone


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_from_timestamp(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def now_timestamp() -> int:
    return int(datetime.now(timezone.utc).timestamp())
