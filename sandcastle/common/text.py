from __future__ import annotations

import re
from collections import Counter
from typing import Iterable

NON_WORD = re.compile(r"[^a-z0-9]+")
WHITESPACE = re.compile(r"\s+")

STOPWORDS = {
    "the", "a", "an", "and", "or", "for", "to", "in", "of", "on", "with", "by", "from",
    "is", "are", "be", "as", "at", "it", "this", "that", "these", "those", "your", "you",
}


def normalize(text: str) -> str:
    lowered = text.lower()
    cleaned = NON_WORD.sub(" ", lowered)
    cleaned = WHITESPACE.sub(" ", cleaned).strip()
    return cleaned


def tokenize(text: str) -> list[str]:
    normalized = normalize(text)
    tokens = [token for token in normalized.split(" ") if token and token not in STOPWORDS]
    return tokens


def shingles(text: str, size: int = 3) -> set[str]:
    tokens = tokenize(text)
    if len(tokens) < size:
        return set(tokens)
    return {" ".join(tokens[idx: idx + size]) for idx in range(len(tokens) - size + 1)}


def top_terms(tokens: Iterable[str], limit: int = 20) -> list[tuple[str, int]]:
    counts = Counter(tokens)
    return counts.most_common(limit)


def top_bigrams(tokens: list[str], limit: int = 20) -> list[tuple[str, int]]:
    bigrams = [" ".join(tokens[i:i+2]) for i in range(len(tokens) - 1)]
    counts = Counter(bigrams)
    return counts.most_common(limit)


def limit_tag_length(tag: str, max_len: int = 32) -> str:
    if len(tag) <= max_len:
        return tag
    return tag[:max_len]
