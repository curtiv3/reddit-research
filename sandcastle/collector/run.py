from __future__ import annotations

import logging
import time
from pathlib import Path
from urllib.parse import urlparse

from sandcastle.collector.base import SearchResult
from sandcastle.collector.searxng import SearxNGCollector
from sandcastle.collector.ddg import DuckDuckGoCollector
from sandcastle.collector.brave import BraveCollector
from sandcastle.common.hash import sha256_text
from sandcastle.common.io import append_jsonl, read_jsonl
from sandcastle.common.text import normalize
from sandcastle.common.time import iso_now
from sandcastle.common.url import canonicalize_url
from sandcastle.config import Config, resolve_path

logger = logging.getLogger(__name__)

COLLECTOR_MAP = {
    "searxng": SearxNGCollector,
    "ddg": DuckDuckGoCollector,
    "brave": BraveCollector,
}


def build_collectors(config: Config):
    collectors = []
    for engine in config.engines:
        name = engine.get("name")
        if name not in COLLECTOR_MAP:
            raise ValueError(f"Unknown engine: {name}")
        if name == "searxng":
            collectors.append(
                SearxNGCollector(
                    endpoint=engine.get("endpoint", ""),
                    rate_limit_s=engine.get("rate_limit_s", 1.0),
                    timeout_s=engine.get("timeout_s", 10.0),
                    pages=int(engine.get("pages", 1)),
                )
            )
        elif name == "ddg":
            collectors.append(DuckDuckGoCollector())
        elif name == "brave":
            collectors.append(BraveCollector(api_key=engine.get("api_key")))
    return collectors


def result_to_row(result: SearchResult) -> dict:
    canonical_url = canonicalize_url(result.url)
    title_norm = normalize(result.title)
    snippet_norm = normalize(result.snippet)
    identity = sha256_text(f"{canonical_url.lower()}|{title_norm}|{snippet_norm}")
    return {
        "id": identity,
        "query": result.query,
        "engine": result.engine,
        "rank": result.rank,
        "source_url": result.url,
        "title": result.title,
        "snippet": result.snippet,
        "collected_at": iso_now(),
        "meta": result.meta,
    }


def is_blocked(url: str, blocked_domains: list[str]) -> bool:
    domain = urlparse(url).netloc.lower()
    return any(blocked in domain for blocked in blocked_domains)


def run_collect(config: Config) -> None:
    output_path = resolve_path(config.path.parent, config.outputs.get("raw_results", "data/raw_results.jsonl"))
    limits = config.limits
    per_query = int(limits.get("per_query", 50))
    per_engine = int(limits.get("per_engine", 50))
    global_max = int(limits.get("global_max", 1000))
    max_minutes = int(limits.get("max_minutes", 10))
    no_new_pages_limit = int(limits.get("no_new_pages", 3))

    blocked_domains = config.raw.get("filters", {}).get("blocked_domains", [])

    collectors = build_collectors(config)
    existing_ids = {row.get("id") for row in read_jsonl(output_path)}

    start_time = time.monotonic()
    total_added = 0

    for query in config.queries:
        for collector in collectors:
            added_for_combo = 0
            no_new_pages = 0
            try:
                if isinstance(collector, SearxNGCollector):
                    for page in range(1, collector.pages + 1):
                        page_results = collector.search_page(query, per_engine, page)
                        if not page_results:
                            break
                        added_in_page = 0
                        for result in page_results:
                            if is_blocked(result.url, blocked_domains):
                                continue
                            row = result_to_row(result)
                            if row["id"] in existing_ids:
                                continue
                            append_jsonl(output_path, [row])
                            existing_ids.add(row["id"])
                            added_for_combo += 1
                            added_in_page += 1
                            total_added += 1
                            if added_for_combo >= per_query or total_added >= global_max:
                                break
                            if time.monotonic() - start_time > max_minutes * 60:
                                logger.info("Max minutes reached")
                                return
                        if added_in_page == 0:
                            no_new_pages += 1
                            if no_new_pages >= no_new_pages_limit:
                                logger.info("No new unique items, stopping for %s", collector.name)
                                break
                        if added_for_combo >= per_query or total_added >= global_max:
                            break
                else:
                    for result in collector.search(query, limit=per_engine):
                        if is_blocked(result.url, blocked_domains):
                            continue
                        row = result_to_row(result)
                        if row["id"] in existing_ids:
                            continue
                        append_jsonl(output_path, [row])
                        existing_ids.add(row["id"])
                        added_for_combo += 1
                        total_added += 1
                        if added_for_combo >= per_query or total_added >= global_max:
                            break
                        if time.monotonic() - start_time > max_minutes * 60:
                            logger.info("Max minutes reached")
                            return
            except RuntimeError as exc:
                logger.warning("Collector error: %s", exc)
                if "Rate limit" in str(exc):
                    logger.info("Rate limit reached, stopping collector %s", collector.name)
                    break
            if total_added >= global_max:
                logger.info("Global max reached")
                return
