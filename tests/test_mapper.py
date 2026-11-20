import hashlib
import json
import sys
from pathlib import Path

import pytest

from reddit import filters, mapper
from reddit.models import RawComment
from reddit.parser import parse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import check_item  # noqa: E402

S = Path(__file__).parent / "samples"
NOW = "2026-11-20T09:00:00Z"


def _raw(body="<p>hello world</p>", author="Bob_1", score=3):
    return RawComment("t1_abc123", "AskReddit", author, "/r/AskReddit/comments/1y0x7e1/slug/abc123/",
                      "2026-11-20T08:00:00Z", score, body)


def test_samples_all_valid():
    n = 0
    for f in ("popular_0.html", "askreddit_0.html"):
        for r in parse((S / f).read_text(encoding="utf-8")):
            c = mapper.build_content(r.body_html)
            if filters.reject_reason(r.author, c):
                continue
            it = mapper.to_item(r, c, NOW)
            assert check_item.validate(it) == [], it
            n += 1
    assert n > 100


def test_item_fields():
    it = mapper.to_item(_raw(), "hello world", NOW)
    assert it["author"] == hashlib.sha1(b"Bob_1").hexdigest()
    assert it["username"] == "Bob_1" and it["title"] == "" and it["domain"] == "reddit.com"
    assert it["url"] == "https://www.reddit.com/r/AskReddit/comments/1y0x7e1/slug/abc123/"
    assert isinstance(it["summary"], str)
    assert json.loads(it["summary"]) == {"platform": "reddit", "communityName": "AskReddit",
                                         "data_type": "comment", "score": 3}
    assert it["external_parent_id"] is None


def test_hidden_score_null():
    it = mapper.to_item(_raw(score=None), "hello world", NOW)
    assert json.loads(it["summary"])["score"] is None


def test_parent_modes():
    assert mapper.to_item(_raw(), "x", NOW, "null")["external_parent_id"] is None
    assert mapper.to_item(_raw(), "x", NOW, "post")["external_parent_id"] == "t3_1y0x7e1"
    assert mapper.validate_parent_mode("post") == "post"
    for bad in ("resolve", "foo"):
        with pytest.raises(ValueError):
            mapper.validate_parent_mode(bad)


def test_text_and_links():
    h = ('<p>See <a href="https://example.com/a">this</a> and <a href="https://x.org/b">https://x.org/b</a></p>'
         '\n\n<p>Hi <a href="/u/foo">u/foo</a><br>line2 &amp; more</p>')
    assert mapper.build_content(h) == "See this and https://x.org/b\n\nHi u/foo\nline2 & more --https://example.com/a"


def test_media_block():
    h = ('<p>pic <a href="https://giphy.com/gifs/z"><img src="https://external-preview.redd.it/a.gif?w=1&amp;s=2"></a></p>')
    c = mapper.build_content(h)
    assert c == "pic --https://giphy.com/gifs/z\n‹‹media›› https://external-preview.redd.it/a.gif?w=1&s=2 ‹‹/media››"
    assert mapper.build_content('<p><a href="https://i.redd.it/x.jpeg">look</a></p>').endswith(
        "‹‹media›› https://i.redd.it/x.jpeg ‹‹/media››")


def test_check_item_rejects_bad():
    it = mapper.to_item(_raw(), "hello world", NOW)
    it["summary"] = json.loads(it["summary"])
    it["external_parent_id"] = it["external_id"]
    errs = check_item.validate(it)
    assert any("summary" in e for e in errs) and any("parent" in e for e in errs)
