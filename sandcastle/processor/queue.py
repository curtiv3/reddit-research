from __future__ import annotations


def build_queue(items: list[dict], max_items: int = 50) -> list[dict]:
    queue = []
    for item in items[:max_items]:
        queue.append({"id": item["id"], "canonical_url": item["canonical_url"]})
    return queue
