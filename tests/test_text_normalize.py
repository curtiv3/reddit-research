from sandcastle.common.text import normalize, limit_tag_length


def test_normalize_removes_punct_and_lowercases():
    assert normalize("Hello, WORLD!!") == "hello world"


def test_limit_tag_length():
    assert limit_tag_length("short", max_len=3) == "sho"
