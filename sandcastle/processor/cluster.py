from __future__ import annotations

import re
from collections import defaultdict

from sandcastle.common.text import normalize, tokenize, limit_tag_length

DEFAULT_TAGS = {
    "pdf": re.compile(r"\bpdf\b"),
    "printable": re.compile(r"\bprintable\b"),
    "prompt": re.compile(r"\bprompt(s)?\b"),
    "bundle": re.compile(r"\bbundle\b"),
    "undated": re.compile(r"\bundated\b"),
    "planner": re.compile(r"\bplanner\b"),
    "workbook": re.compile(r"\bworkbook\b"),
    "cards": re.compile(r"\bcard(s)?\b"),
    "guided": re.compile(r"\bguided\b"),
}


def tag_intents(text: str, extra_tags: dict[str, str] | None = None) -> list[str]:
    tags = dict(DEFAULT_TAGS)
    if extra_tags:
        tags.update({name: re.compile(pattern) for name, pattern in extra_tags.items()})
    found = [limit_tag_length(name) for name, pattern in tags.items() if pattern.search(text)]
    return sorted(set(found))


def assign_clusters(items: list[dict], clusters: list[dict], extra_tags: dict[str, str] | None = None) -> dict:
    cluster_map = {cluster["cluster_id"]: cluster for cluster in clusters}
    assignments: dict[str, dict] = {cluster["cluster_id"]: {"items": [], "intent_tags": []} for cluster in clusters}

    for item in items:
        text = normalize(" ".join(item.get("titles", []) + item.get("snippets", [])))
        best_cluster = None
        best_hits = 0
        for cluster in clusters:
            keywords = cluster.get("keywords", [])
            hits = sum(1 for keyword in keywords if normalize(keyword) in text)
            if hits > best_hits:
                best_hits = hits
                best_cluster = cluster
            elif hits == best_hits and hits > 0 and best_cluster:
                if cluster["cluster_id"] < best_cluster["cluster_id"]:
                    best_cluster = cluster
        if best_cluster and best_hits > 0:
            cluster_id = best_cluster["cluster_id"]
            assignments[cluster_id]["items"].append(item["id"])
            tag_text = " ".join(item.get("snippets", []) + item.get("titles", []))
            assignments[cluster_id]["intent_tags"].extend(tag_intents(tag_text, extra_tags))

    clusters_out = []
    for cluster_id, payload in assignments.items():
        cluster = cluster_map[cluster_id]
        tags = sorted(set(payload["intent_tags"]))
        clusters_out.append(
            {
                "cluster_id": cluster_id,
                "label": cluster.get("label"),
                "items": sorted(payload["items"]),
                "intent_tags": tags,
                "top_terms": [],
                "top_bigrams": [],
            }
        )

    clusters_out = sorted(clusters_out, key=lambda item: item["cluster_id"])
    return {"clusters": clusters_out}


def flatten_cluster_text(items: list[dict], cluster_items: list[str]) -> list[str]:
    item_map = {item["id"]: item for item in items}
    tokens: list[str] = []
    for item_id in cluster_items:
        item = item_map.get(item_id)
        if not item:
            continue
        combined = " ".join(item.get("titles", []) + item.get("snippets", []))
        tokens.extend(tokenize(combined))
    return tokens
