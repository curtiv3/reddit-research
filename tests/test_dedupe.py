from sandcastle.processor.dedupe import dedupe_items


def test_dedupe_by_canonical_url():
    raw_items = [
        {
            "id": "a",
            "source_url": "https://example.com/page?utm_source=x",
            "title": "Guide",
            "snippet": "Great guide",
            "query": "focus",
            "engine": "searxng",
            "collected_at": "2024-01-01T00:00:00Z",
        },
        {
            "id": "b",
            "source_url": "https://example.com/page",
            "title": "Guide",
            "snippet": "Great guide",
            "query": "focus",
            "engine": "searxng",
            "collected_at": "2024-01-02T00:00:00Z",
        },
    ]
    deduped = dedupe_items(raw_items)
    assert len(deduped) == 1
    assert deduped[0]["canonical_url"] == "https://example.com/page"


def test_dedupe_by_similarity():
    raw_items = [
        {
            "id": "a",
            "source_url": "https://example.com/a",
            "title": "Focus journal tips",
            "snippet": "Deep focus journal tips",
            "query": "focus",
            "engine": "searxng",
            "collected_at": "2024-01-01T00:00:00Z",
        },
        {
            "id": "b",
            "source_url": "https://example.com/b",
            "title": "Focus journal tips",
            "snippet": "Deep focus journal tips",
            "query": "focus",
            "engine": "searxng",
            "collected_at": "2024-01-02T00:00:00Z",
        },
    ]
    deduped = dedupe_items(raw_items)
    assert len(deduped) == 1
