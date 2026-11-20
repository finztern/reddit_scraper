import asyncio

from reddit.seen_cache import SeenCache
from reddit.url_check import UrlChecker, extract_known


def test_extract_known():
    assert extract_known(["a", "b"]) == {"a", "b"}
    assert extract_known({"known": [{"url": "a"}]}) == {"a"}
    assert extract_known({}) == set()


def test_policy_when_no_proxy():
    items = [{"url": "u1", "external_id": "t1_a"}]
    closed = UrlChecker("http://x", lambda: None, "closed")
    openp = UrlChecker("http://x", lambda: None, "open")
    assert asyncio.run(closed.unknown(items)) is None
    assert asyncio.run(openp.unknown(items)) == items


def test_seen_cache():
    c = SeenCache(max_size=2)
    assert c.claim("a") and not c.claim("a")
    c.release("a")
    assert c.claim("a")
    c.confirm("a")
    c.release("a")  # confirmed claims are not released
    assert not c.claim("a")
    c.claim("b"); c.claim("c")  # LRU evicts the oldest
    assert len(c) == 2
