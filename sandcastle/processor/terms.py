from __future__ import annotations

from sandcastle.common.text import top_bigrams, top_terms
from sandcastle.processor.cluster import flatten_cluster_text


def build_terms(items: list[dict], clusters: dict) -> dict:
    cluster_terms = {}
    for cluster in clusters.get("clusters", []):
        tokens = flatten_cluster_text(items, cluster.get("items", []))
        top_terms_list = top_terms(tokens, limit=20)
        top_bigrams_list = top_bigrams(tokens, limit=20)
        cluster_terms[cluster["cluster_id"]] = {
            "top_terms": [{"term": term, "count": count} for term, count in top_terms_list],
            "top_bigrams": [{"bigram": bigram, "count": count} for bigram, count in top_bigrams_list],
        }
    return {"cluster_terms": cluster_terms}
