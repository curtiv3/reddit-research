from __future__ import annotations

import logging

from sandcastle.common.io import read_jsonl, write_json
from sandcastle.config import Config, resolve_path
from sandcastle.processor.cluster import assign_clusters
from sandcastle.processor.dedupe import dedupe_items
from sandcastle.processor.terms import build_terms
from sandcastle.processor.quality import compute_quality

logger = logging.getLogger(__name__)


def run_process(config: Config) -> None:
    outputs = config.outputs
    raw_path = resolve_path(config.path.parent, outputs.get("raw_results", "data/raw_results.jsonl"))
    deduped_path = resolve_path(config.path.parent, outputs.get("deduped", "data/deduped.json"))
    clusters_path = resolve_path(config.path.parent, outputs.get("clusters", "data/clusters.json"))
    terms_path = resolve_path(config.path.parent, outputs.get("terms", "data/terms.json"))
    quality_path = resolve_path(config.path.parent, outputs.get("quality", "data/quality.json"))

    raw_items = list(read_jsonl(raw_path))
    deduped_items = dedupe_items(raw_items)
    clusters_cfg = config.clustering.get("clusters", [])
    extra_tags = config.clustering.get("intent_tags", {})
    clusters = assign_clusters(deduped_items, clusters_cfg, extra_tags)
    terms = build_terms(deduped_items, clusters)
    for cluster in clusters.get("clusters", []):
        term_payload = terms.get("cluster_terms", {}).get(cluster["cluster_id"], {})
        cluster["top_terms"] = [item["term"] for item in term_payload.get("top_terms", [])]
        cluster["top_bigrams"] = [item["bigram"] for item in term_payload.get("top_bigrams", [])]
    quality = compute_quality(len(raw_items), deduped_items, clusters)

    write_json(deduped_path, deduped_items)
    write_json(clusters_path, clusters)
    write_json(terms_path, terms)
    write_json(quality_path, quality)

    logger.info("Processing complete", extra={"deduped": len(deduped_items)})
