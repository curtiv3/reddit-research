from __future__ import annotations

from collections import Counter
from urllib.parse import urlparse


def compute_quality(raw_count: int, deduped: list[dict], clusters: dict) -> dict:
    deduped_count = len(deduped)
    dedupe_ratio = 1.0 - (deduped_count / raw_count) if raw_count else 0.0

    domains = Counter()
    for item in deduped:
        domain = urlparse(item.get("canonical_url", "")).netloc
        if domain:
            domains[domain] += 1

    top_domains = [{"domain": domain, "count": count} for domain, count in domains.most_common(10)]

    intent_total = 0
    intent_clusters = 0
    for cluster in clusters.get("clusters", []):
        intent_total += len(cluster.get("intent_tags", []))
        if cluster.get("intent_tags"):
            intent_clusters += 1
    intent_density = intent_total / max(1, intent_clusters)

    issues = []
    if top_domains:
        top_ratio = top_domains[0]["count"] / max(1, deduped_count)
        if top_ratio > 0.5:
            issues.append({"code": "DOMAIN_MONOCULTURE", "severity": "warn", "details": {"top_ratio": top_ratio}})
    if intent_density < 1:
        issues.append({"code": "LOW_INTENT_DENSITY", "severity": "warn", "details": {"intent_density": intent_density}})

    return {
        "summary": {
            "raw_count": raw_count,
            "deduped_count": deduped_count,
            "dedupe_ratio": round(dedupe_ratio, 4),
            "top_domains": top_domains,
        },
        "issues": issues,
    }
