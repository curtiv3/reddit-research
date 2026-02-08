from sandcastle.common.url import canonicalize_url


def test_canonicalize_strips_tracking_and_sorts():
    url = "HTTP://Example.com/Path/?b=2&utm_source=x&a=1#frag"
    assert canonicalize_url(url) == "http://example.com/Path?a=1&b=2"


def test_canonicalize_trailing_slash():
    url = "https://example.com/path/"
    assert canonicalize_url(url) == "https://example.com/path"
