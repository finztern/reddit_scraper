"""Validate items against TZ section 3.

Usage:
  python scripts/check_item.py items.json|items.jsonl   # {"items":[..]}, [..] or one item per line
  python scripts/check_item.py page.html                # parse -> filter -> map -> validate
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

REQUIRED = {"content", "external_id", "created_at", "collected_at", "url", "title", "author",
            "username", "external_parent_id", "domain", "summary"}
OPTIONAL = {"userprofile_url"}
TS = re.compile(r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$")
SUMMARY_KEYS = {"platform", "communityName", "data_type", "score"}


def validate(it: dict) -> list[str]:
    e: list[str] = []
    keys = set(it)
    if keys - REQUIRED - OPTIONAL:
        e.append(f"unknown keys: {sorted(keys - REQUIRED - OPTIONAL)}")
    if REQUIRED - keys:
        e.append(f"missing keys: {sorted(REQUIRED - keys)}")
        return e
    c = it["content"]
    if not isinstance(c, str) or not c.strip() or c != c.strip():
        e.append("content must be non-empty, stripped str")
    eid = it["external_id"]
    if not isinstance(eid, str) or not re.fullmatch(r"t1_[0-9a-z]+", eid):
        e.append("external_id must match t1_<base36>")
    for k in ("created_at", "collected_at"):
        if not isinstance(it[k], str) or not TS.match(it[k]):
            e.append(f"{k} must be YYYY-MM-DDTHH:MM:SSZ")
    if all(isinstance(it[k], str) and TS.match(it[k]) for k in ("created_at", "collected_at")) \
            and it["created_at"] > it["collected_at"]:
        e.append("created_at is after collected_at")
    url = it["url"]
    if not isinstance(url, str) or not re.fullmatch(r"https://www\.reddit\.com/(?:r|user)/[^/]+/comments/[0-9a-z]+/[^/]*/[0-9a-z]+/?", url):
        e.append("url must be a reddit comment permalink")
    elif isinstance(eid, str) and url.rstrip("/").rsplit("/", 1)[-1] != eid[3:]:
        e.append("url does not end with external_id")
    if it["title"] != "":
        e.append('title must be ""')
    u = it["username"]
    if not isinstance(u, str) or not u or u == "[deleted]":
        e.append("bad username")
    elif it["author"] != hashlib.sha1(u.encode("utf-8")).hexdigest():
        e.append("author != sha1(username)")
    if not isinstance(it["author"], str) or not re.fullmatch(r"[0-9a-f]{40}", it["author"]):
        e.append("author must be 40 hex")
    p = it["external_parent_id"]
    if p is not None and (not isinstance(p, str) or not re.fullmatch(r"t[13]_[0-9a-z]+", p) or p == eid):
        e.append("external_parent_id must be null or t1_/t3_ id (not self)")
    if it["domain"] != "reddit.com":
        e.append("domain must be reddit.com")
    s = it["summary"]
    if not isinstance(s, str):
        e.append("summary must be a JSON string")
    else:
        try:
            d = json.loads(s)
        except ValueError:
            d = None
            e.append("summary is not valid JSON")
        if isinstance(d, dict):
            if set(d) != SUMMARY_KEYS:
                e.append(f"summary keys must be {sorted(SUMMARY_KEYS)}")
            if d.get("platform") != "reddit" or d.get("data_type") != "comment":
                e.append("summary platform/data_type wrong")
            if not isinstance(d.get("communityName"), str) or not d.get("communityName"):
                e.append("summary communityName missing")
            sc = d.get("score")
            if sc is not None and (isinstance(sc, bool) or not isinstance(sc, int)):
                e.append("summary score must be int or null")
        elif d is not None:
            e.append("summary JSON must be an object")
    if "userprofile_url" in it and it["userprofile_url"] != f"https://www.reddit.com/user/{u}":
        e.append("userprofile_url mismatch")
    return e


def _load(path: Path) -> list[dict]:
    if path.suffix == ".html":
        from reddit import filters, mapper, parser
        now = mapper.utc_now_str()
        items = []
        for r in parser.parse(path.read_text(encoding="utf-8")):
            c = mapper.build_content(r.body_html)
            if not filters.reject_reason(r.author, c):
                items.append(mapper.to_item(r, c, now))
        return items
    txt = path.read_text(encoding="utf-8").strip()
    try:
        d = json.loads(txt)
        return d["items"] if isinstance(d, dict) else d
    except ValueError:
        return [json.loads(x) for x in txt.splitlines() if x.strip()]


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    bad = total = 0
    for a in argv:
        for it in _load(Path(a)):
            total += 1
            errs = validate(it)
            if errs:
                bad += 1
                print(f"{it.get('external_id')}: {'; '.join(errs)}")
    print(f"checked={total} bad={bad}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
