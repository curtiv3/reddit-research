from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol


@dataclass
class SearchResult:
    query: str
    engine: str
    rank: int
    url: str
    title: str
    snippet: str
    meta: dict


class Collector(Protocol):
    name: str

    def search(self, query: str, limit: int) -> Iterable[SearchResult]:
        ...
