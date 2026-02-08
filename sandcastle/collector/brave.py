from __future__ import annotations

from typing import Iterable

from sandcastle.collector.base import Collector, SearchResult


class BraveCollector(Collector):
    def __init__(self, api_key: str | None = None) -> None:
        self.name = "brave"
        self.api_key = api_key

    def search(self, query: str, limit: int) -> Iterable[SearchResult]:
        if not self.api_key:
            raise RuntimeError("Brave Search requires an API key. Configure brave.api_key.")
        raise RuntimeError("Brave collector stub only. Provide implementation if key is set.")
