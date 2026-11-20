from pathlib import Path

from reddit.parser import parse

S = Path(__file__).parent / "samples"


def _load(n):
    return parse((S / n).read_text(encoding="utf-8"))


def test_popular_counts_and_fields():
    cs = _load("popular_0.html")
    assert len(cs) == 100
    c = cs[0]
    assert c.fullname == "t1_19swuc0g6vcqp"
    assert c.subreddit == "SideProject" and c.author == "Young_Cheesy"
    assert c.permalink.startswith("/r/SideProject/comments/1y0vrk8/")
    assert c.post_id == "1y0vrk8"
    assert c.created_at == "2026-11-20T08:12:57Z"
    assert c.score == 1
    assert "new ask immediately" in c.body_html
    assert len({x.fullname for x in cs}) == 100


def test_all_have_utc_time_and_t1():
    for n in ("popular_0.html", "askreddit_0.html"):
        for c in _load(n):
            assert c.fullname.startswith("t1_")
            assert c.created_at.endswith("Z") and len(c.created_at) == 20


def test_hidden_score_is_none():
    cs = _load("askreddit_0.html")
    assert len(cs) == 100
    assert all(c.score is None for c in cs)


def test_empty_page():
    assert parse("<html><body>nothing</body></html>") == []
