from __future__ import annotations

import logging
from typing import Iterable

import requests

from sandcastle.common.rate_limit import RateLimiter, backoff_sleep

logger = logging.getLogger(__name__)


class RedditSearchClient:
    def __init__(self, rate_limit_s: float = 1.0, timeout_s: float = 10.0):
        self.rate_limiter = RateLimiter(rate_limit_s)
        self.timeout_s = timeout_s
        self.base_url = "https://www.reddit.com/search.json"

    def search(self, query: str, limit: int, after: str | None = None) -> dict:
        self.rate_limiter.wait()
        params = {
            "q": query,
            "sort": "new",
            "limit": limit,
            "type": "link",
        }
        if after:
            params["after"] = after
        headers = {"User-Agent": "sandcastle/0.1 (research pipeline)"}
        attempt = 0
        while attempt < 3:
            try:
                resp = requests.get(self.base_url, params=params, headers=headers, timeout=self.timeout_s)
                if resp.status_code == 429:
                    raise RuntimeError("Rate limit reached")
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                logger.warning("Reddit request failed", exc_info=exc)
                if attempt >= 3:
                    raise
                backoff_sleep(attempt)
        return {}


def iter_posts(query: str, max_pages: int, min_score: int, min_comments: int, allow_nsfw: bool, only_posts: bool) -> Iterable[dict]:
    client = RedditSearchClient()
    after = None
    page = 0
    while page < max_pages:
        data = client.search(query, limit=100, after=after)
        payload = data.get("data", {})
        children = payload.get("children", [])
        if not children:
            break
        for child in children:
            post = child.get("data", {})
            if only_posts and post.get("is_self") is False:
                continue
            if not allow_nsfw and post.get("over_18"):
                continue
            if post.get("score", 0) < min_score:
                continue
            if post.get("num_comments", 0) < min_comments:
                continue
            yield post
        after = payload.get("after")
        page += 1
        if not after:
            break
