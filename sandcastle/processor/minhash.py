from __future__ import annotations

from sandcastle.common.text import shingles


def jaccard_similarity(text_a: str, text_b: str, shingle_size: int = 3) -> float:
    set_a = shingles(text_a, size=shingle_size)
    set_b = shingles(text_b, size=shingle_size)
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union
