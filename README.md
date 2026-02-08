# Sandcastle Research Pipeline

Sandcastle is a deterministic, production-grade research pipeline with three phases:

1. **Collector (no LLM)**: gathers raw search results from multiple sources into append-only JSONL.
2. **Processor (no LLM)**: canonicalizes URLs, deduplicates, clusters, extracts terms/signals, and builds quality metrics.
3. **Reality Anchor (no LLM)**: collects Reddit "problem-language" signals across multiple time windows, then dedupes/clusters phrases and labels intent stability.
4. **Reasoner Stub (LLM)**: a stub schema and prompt template (no LLM calls).

## Requirements

- Python 3.11+
- Network access for search endpoints (SearxNG and Reddit JSON)

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

Update `config.yaml` with your SearxNG endpoint, then run:

```bash
python -m sandcastle collect --config config.yaml
python -m sandcastle reddit --config config.yaml
python -m sandcastle process --config config.yaml
```

Count valid JSONL objects:

```bash
python -m sandcastle count --file data/raw_results.jsonl
```

The count command also reports unique IDs, per-engine, per-query, and per-cluster counts when applicable.

Validate config and endpoints:

```bash
python -m sandcastle doctor --config config.yaml
```

## Determinism & Safety

- Deterministic hashing (SHA-256), stable sorting, and union-find clustering.
- Config-driven limits prevent infinite loops and uncontrolled scraping.
- Rate limiting, timeouts, retries with backoff, and stop conditions are built-in.

## Data Outputs

### Raw web results (append-only)
`data/raw_results.jsonl`

```json
{
  "id": "<sha256>",
  "query": "<string>",
  "engine": "<string>",
  "rank": 1,
  "source_url": "<string>",
  "title": "<string>",
  "snippet": "<string>",
  "collected_at": "<ISO8601>",
  "meta": {"engine": "<engine_id>"}
}
```

ID definition:

```
sha256(lower(canonical_url) + '|' + normalize(title) + '|' + normalize(snippet))
```

### Reddit posts (append-only)
`data/reddit_posts.jsonl`

```json
{
  "id": "<sha256>",
  "query": "<string>",
  "window": "14d|60d|180d|365d",
  "source_url": "<permalink>",
  "title": "<string>",
  "selftext": "<string>",
  "subreddit": "<string>",
  "score": 123,
  "num_comments": 12,
  "created_utc": 1700000000,
  "collected_at": "<ISO8601>",
  "meta": {"id": "<reddit_id>", "author": "<author>"}
}
```

### Processor outputs

- `data/deduped.json` (list of deduped items)
- `data/clusters.json` (deterministic clusters)
- `data/terms.json` (top terms/bigrams per cluster)
- `data/quality.json` (quality gates + diagnostics)

### Reddit intent stability output
`data/reddit_intents.json`

```json
{
  "intents": [
    {
      "intent_id": "cant_focus_overload",
      "label": "can't focus overload",
      "evidence_counts": {"14d": 12, "60d": 40, "180d": 110, "365d": 260},
      "classification": "structural",
      "common_phrases": ["can't focus", "too much to do"],
      "top_subreddits": [{"subreddit": "productivity", "count": 10}],
      "examples": [{"source_url": "...", "title": "...", "snippet": "..."}]
    }
  ],
  "rules": {
    "structural_if": "evidence in >=2 windows including one of {180d,365d} and recent {14d or 60d} > 0",
    "temporal_if": "evidence mostly in recent windows and near-zero in {180d,365d}"
  }
}
```

## Configuration

See `config.yaml` for an end-to-end example. Key sections:

- `queries`: search queries for collectors
- `engines`: list of engines (SearxNG supported by default)
- `limits`: stop conditions and caps
- `filters`: blocked domains (e.g., etsy/pinterest/reddit)
- `reddit`: time windows, queries, and filters
- `clustering`: seed keywords and intent tags

## Collector Notes

- **SearxNG**: supported via JSON output. Configure `endpoint`.
- **DuckDuckGo** and **Brave**: stubbed and fail with clear errors unless implemented.

## Reasoner Stub

The reasoner stub lives in `sandcastle/reasoner_stub` and includes:

- `prompt_template.txt`
- `strategy.schema.json`

No LLM calls are implemented.

## Tests

```bash
pytest
```
