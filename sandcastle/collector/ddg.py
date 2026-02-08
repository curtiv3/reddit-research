from __future__ import annotations

from typing import Iterable

from sandcastle.collector.base import Collector, SearchResult


class DuckDuckGoCollector(Collector):
    def __init__(self) -> None:
        self.name = "ddg"

    def search(self, query: str, limit: int) -> Iterable[SearchResult]:
        raise RuntimeError(
            "DuckDuckGo collector is disabled by default. "
            "Use SearxNG or implement a stable endpoint in config."
        )
