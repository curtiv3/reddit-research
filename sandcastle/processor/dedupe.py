from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable

from sandcastle.common.text import normalize
from sandcastle.common.url import canonicalize_url
from sandcastle.processor.minhash import jaccard_similarity


@dataclass
class RawItem:
    id: str
    canonical_url: str
    source_url: str
    title: str
    snippet: str
    query: str
    engine: str
    collected_at: str


class UnionFind:
    def __init__(self, items: list[str]):
        self.parent = {item: item for item in items}

    def find(self, item: str) -> str:
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]

    def union(self, left: str, right: str) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        chosen = min(root_left, root_right)
        other = max(root_left, root_right)
        self.parent[other] = chosen


def union_find_groups(items: list[str], similarity_threshold: float = 0.85) -> list[list[str]]:
    sorted_items = sorted(items)
    uf = UnionFind(sorted_items)
    for idx, item in enumerate(sorted_items):
        for other in sorted_items[idx + 1:]:
            if item == other:
                continue
            score = jaccard_similarity(item, other, shingle_size=2)
            if score >= similarity_threshold:
                uf.union(item, other)
    groups: dict[str, list[str]] = defaultdict(list)
    for item in sorted_items:
        groups[uf.find(item)].append(item)
    return [sorted(group) for group in groups.values()]


def dedupe_items(raw_items: Iterable[dict]) -> list[dict]:
    items: list[RawItem] = []
    for raw in raw_items:
        items.append(
            RawItem(
                id=raw.get("id"),
                canonical_url=canonicalize_url(raw.get("source_url", "")),
                source_url=raw.get("source_url", ""),
                title=raw.get("title", ""),
                snippet=raw.get("snippet", ""),
                query=raw.get("query", ""),
                engine=raw.get("engine", ""),
                collected_at=raw.get("collected_at", ""),
            )
        )

    items.sort(key=lambda item: item.id)
    uf = UnionFind([item.id for item in items])

    url_map: dict[str, str] = {}
    for item in items:
        if item.canonical_url in url_map:
            uf.union(item.id, url_map[item.canonical_url])
        else:
            url_map[item.canonical_url] = item.id

    for idx, item in enumerate(items):
        text_a = normalize(f"{item.title} {item.snippet}")
        for other in items[idx + 1:]:
            text_b = normalize(f"{other.title} {other.snippet}")
            score = jaccard_similarity(text_a, text_b)
            if score >= 0.85:
                uf.union(item.id, other.id)

    grouped: dict[str, list[RawItem]] = defaultdict(list)
    for item in items:
        grouped[uf.find(item.id)].append(item)

    results = []
    for group_id, group_items in grouped.items():
        group_items.sort(key=lambda item: item.collected_at)
        results.append(
            {
                "id": group_id,
                "canonical_url": group_items[0].canonical_url,
                "original_urls": sorted({item.source_url for item in group_items if item.source_url}),
                "titles": sorted({item.title for item in group_items if item.title}),
                "snippets": sorted({item.snippet for item in group_items if item.snippet}),
                "queries": sorted({item.query for item in group_items if item.query}),
                "engines": sorted({item.engine for item in group_items if item.engine}),
                "first_seen": group_items[0].collected_at,
                "last_seen": group_items[-1].collected_at,
                "flags": {"blocked": False, "suspicious": False},
            }
        )

    return sorted(results, key=lambda item: item["id"])
