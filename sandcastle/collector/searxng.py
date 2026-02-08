from __future__ import annotations

import logging
from typing import Iterable

import requests

from sandcastle.collector.base import Collector, SearchResult
from sandcastle.common.rate_limit import RateLimiter, backoff_sleep

logger = logging.getLogger(__name__)


class SearxNGCollector(Collector):
    def __init__(self, endpoint: str, rate_limit_s: float = 1.0, timeout_s: float = 10.0, pages: int = 1):
        self.name = "searxng"
        self.endpoint = endpoint.rstrip("/")
        self.rate_limiter = RateLimiter(rate_limit_s)
        self.timeout_s = timeout_s
        self.pages = pages

    def search_page(self, query: str, limit: int, page: int) -> list[SearchResult]:
        self.rate_limiter.wait()
        params = {
            "q": query,
            "format": "json",
            "language": "en",
            "safesearch": 1,
            "pageno": page,
        }
        attempt = 0
        while attempt < 3:
            try:
                response = requests.get(self.endpoint, params=params, timeout=self.timeout_s)
                if response.status_code == 429:
                    raise RuntimeError("Rate limit reached")
                response.raise_for_status()
                data = response.json()
                results = data.get("results", [])[:limit]
                return [
                    SearchResult(
                        query=query,
                        engine=self.name,
                        rank=idx,
                        url=item.get("url") or "",
                        title=item.get("title") or "",
                        snippet=item.get("content") or "",
                        meta={"engine": item.get("engine")},
                    )
                    for idx, item in enumerate(results, start=1)
                ]
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                logger.warning("SearxNG request failed", exc_info=exc)
                if attempt >= 3:
                    raise
                backoff_sleep(attempt)
        return []

    def search(self, query: str, limit: int) -> Iterable[SearchResult]:
        yielded = 0
        for page in range(1, self.pages + 1):
            results = self.search_page(query, limit, page)
            if not results:
                break
            for result in results:
                yield result
                yielded += 1
                if yielded >= limit:
                    return
