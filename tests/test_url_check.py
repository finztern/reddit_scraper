import asyncio

from reddit.seen_cache import SeenCache
from reddit.url_check import UrlChecker, extract_known

ITEMS = [{"url": f"u{i}", "external_id": f"t1_{i}"} for i in range(4)]


def test_extract_known():
    assert extract_known(["a", "b"]) == {"a", "b"}
    assert extract_known({"known": [{"url": "a"}]}) == {"a"}
    assert extract_known({}) == set()


def test_policy_when_no_proxy():
    items = [{"url": "u1", "external_id": "t1_a"}]
    closed = UrlChecker("http://x", lambda: None, "closed", retry_delay=0)
    openp = UrlChecker("http://x", lambda: None, "open", retry_delay=0)
    assert asyncio.run(closed.unknown(items)) is None
    assert asyncio.run(openp.unknown(items)) == items


def _checker(answers, policy="closed", retries=1):
    """answers: proxy -> set of known urls, or Exception."""
    fails: list = []
    c = UrlChecker("http://x", lambda: None, policy, on_failure=fails.append,
                   proxies_getter=lambda: list(answers), retries=retries, retry_delay=0)

    async def q(proxy, urls):
        a = answers[proxy]
        if isinstance(a, Exception):
            raise a
        return a
    c._query = q
    return c, fails


def test_two_proxies_union_of_known():
    c, fails = _checker({"p1": {"u0"}, "p2": {"u1"}})
    out = asyncio.run(c.unknown(ITEMS))
    assert [i["url"] for i in out] == ["u2", "u3"] and not fails


def test_one_proxy_down_uses_other():
    c, fails = _checker({"p1": RuntimeError("x"), "p2": {"u0"}})
    out = asyncio.run(c.unknown(ITEMS))
    assert [i["url"] for i in out] == ["u1", "u2", "u3"] and fails == ["p1"]


def test_all_down_policy():
    down = {"p1": RuntimeError("x"), "p2": RuntimeError("y")}
    c, fails = _checker(dict(down), "closed")
    assert asyncio.run(c.unknown(ITEMS)) is None and len(fails) == 4   # 2 proxies x (1 try + 1 retry)
    c, _ = _checker(dict(down), "open")
    assert asyncio.run(c.unknown(ITEMS)) == ITEMS


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
