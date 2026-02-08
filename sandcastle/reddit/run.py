from __future__ import annotations

import logging
from collections import Counter, defaultdict
from pathlib import Path

from sandcastle.common.hash import sha256_text
from sandcastle.common.io import append_jsonl, read_jsonl, write_json
from sandcastle.common.text import normalize, tokenize
from sandcastle.common.time import iso_now
from sandcastle.config import Config, resolve_path
from sandcastle.reddit.search import iter_posts
from sandcastle.reddit.windows import build_windows, window_bounds
from sandcastle.processor.dedupe import union_find_groups

logger = logging.getLogger(__name__)


def post_to_row(post: dict, query: str, window_label: str) -> dict:
    permalink = f"https://www.reddit.com{post.get('permalink', '')}"
    return {
        "id": sha256_text(permalink),
        "query": query,
        "window": window_label,
        "source_url": permalink,
        "title": post.get("title") or "",
        "selftext": post.get("selftext") or "",
        "subreddit": post.get("subreddit") or "",
        "score": int(post.get("score") or 0),
        "num_comments": int(post.get("num_comments") or 0),
        "created_utc": int(post.get("created_utc") or 0),
        "collected_at": iso_now(),
        "meta": {"id": post.get("id"), "author": post.get("author")},
    }


def extract_phrases(text: str, limit: int = 5) -> list[str]:
    tokens = tokenize(text)
    if not tokens:
        return []
    bigrams = [" ".join(tokens[i:i+2]) for i in range(len(tokens) - 1)]
    trigrams = [" ".join(tokens[i:i+3]) for i in range(len(tokens) - 2)]
    counts = Counter(bigrams + trigrams)
    ranked = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [phrase for phrase, _ in ranked[:limit]]


def build_intents(posts: list[dict]) -> dict:
    phrase_windows: dict[str, Counter] = defaultdict(Counter)
    phrase_examples: dict[str, list[dict]] = defaultdict(list)
    phrase_subreddits: dict[str, Counter] = defaultdict(Counter)

    for post in posts:
        text = f"{post.get('title', '')} {post.get('selftext', '')}"
        phrases = extract_phrases(text)
        for phrase in phrases:
            phrase_windows[phrase][post["window"]] += 1
            phrase_subreddits[phrase][post.get("subreddit", "")] += 1
            if len(phrase_examples[phrase]) < 3:
                phrase_examples[phrase].append(
                    {
                        "source_url": post.get("source_url"),
                        "title": post.get("title"),
                        "snippet": (post.get("selftext") or "")[:200],
                    }
                )

    phrases = sorted(phrase_windows.keys())
    groups = union_find_groups(phrases, similarity_threshold=0.6)

    intents = []
    for group in groups:
        sorted_group = sorted(group)
        label = sorted_group[0]
        intent_id = normalize(label).replace(" ", "_")[:40] or "intent"
        evidence_counts = Counter()
        combined_subreddits = Counter()
        combined_examples = []
        common_phrases = []
        for phrase in sorted_group:
            evidence_counts.update(phrase_windows[phrase])
            combined_subreddits.update(phrase_subreddits[phrase])
            common_phrases.append(phrase)
            combined_examples.extend(phrase_examples[phrase])
        classification = classify_intent(evidence_counts)
        intents.append(
            {
                "intent_id": intent_id,
                "label": label,
                "evidence_counts": dict(evidence_counts),
                "classification": classification,
                "common_phrases": common_phrases[:10],
                "top_subreddits": [
                    {"subreddit": subreddit, "count": count}
                    for subreddit, count in combined_subreddits.most_common(5)
                ],
                "examples": combined_examples[:5],
            }
        )

    intents = sorted(intents, key=lambda item: item["intent_id"])
    return {
        "intents": intents,
        "rules": {
            "structural_if": "evidence in >=2 windows including one of {180d,365d} and recent {14d or 60d} > 0",
            "temporal_if": "evidence mostly in recent windows and near-zero in {180d,365d}",
        },
    }


def classify_intent(counts: Counter) -> str:
    recent = counts.get("14d", 0) + counts.get("60d", 0)
    long_term = counts.get("180d", 0) + counts.get("365d", 0)
    windows_with_evidence = sum(1 for value in counts.values() if value > 0)
    if long_term > 0 and recent > 0 and windows_with_evidence >= 2:
        return "structural"
    if recent > 0 and long_term == 0:
        return "temporal"
    return "structural" if long_term >= recent else "temporal"


def run_reddit(config: Config) -> None:
    outputs = config.outputs
    posts_path = resolve_path(config.path.parent, outputs.get("reddit_posts", "data/reddit_posts.jsonl"))
    intents_path = resolve_path(config.path.parent, outputs.get("reddit_intents", "data/reddit_intents.json"))
    reddit_cfg = config.reddit
    queries = reddit_cfg.get("queries", [])
    windows = build_windows(reddit_cfg.get("windows", ["14d", "60d", "180d", "365d"]))
    filters = reddit_cfg.get("filters", {})
    min_score = int(filters.get("min_score", 0))
    min_comments = int(filters.get("min_comments", 0))
    allow_nsfw = bool(filters.get("allow_nsfw", False))
    only_posts = bool(filters.get("only_posts", True))
    max_pages = int(filters.get("max_pages", 3))

    existing_ids = {row.get("id") for row in read_jsonl(posts_path)}

    for query in queries:
        for window in windows:
            start_ts, end_ts = window_bounds(window)
            for post in iter_posts(query, max_pages, min_score, min_comments, allow_nsfw, only_posts):
                created = int(post.get("created_utc") or 0)
                if created < start_ts or created > end_ts:
                    continue
                row = post_to_row(post, query=query, window_label=window.label)
                if row["id"] in existing_ids:
                    continue
                append_jsonl(posts_path, [row])
                existing_ids.add(row["id"])

    posts = list(read_jsonl(posts_path))
    intents = build_intents(posts)
    write_json(intents_path, intents)
    logger.info("Reddit intents written", extra={"count": len(intents.get("intents", []))})
