from reddit.filters import reject_reason as r


def test_ok():
    assert r("bob", "This is fine text") is None


def test_reasons():
    assert r("[deleted]", "hello world") == "deleted_author"
    assert r("AutoModerator", "hello world") == "bot"
    assert r("bob", "[removed]") == "removed_body"
    assert r("bob", "   ") == "empty"
    assert r("bob", "‹‹media›› https://i.redd.it/a.jpg ‹‹/media››") == "empty"
    assert r("bob", "😀😀 🔥") == "emoji_only"
    assert r("bob", "short") == "too_short"
    assert r("bob", "ok 😀😀😀😀") == "too_short"
    assert r("bob", "https://example.com/very/long/url/path") == "empty"
    assert r("bob", "hi --https://example.com/very/long/url/path") == "too_short"


def test_min_len_boundary():
    assert r("bob", "1234567") is None
    assert r("bob", "123456") == "too_short"
    assert r("bob", "1234", min_len=4) is None
