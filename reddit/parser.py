"""Old-style Reddit comment listing HTML -> list[RawComment]."""
from __future__ import annotations

import html as _html
import re
from datetime import datetime, timezone

from .models import RawComment

_THING = re.compile(r'<div class="[^"]*\bthing\b[^"]*"[^>]*>')
_DATA = re.compile(r'\sdata-([a-z-]+)="([^"]*)"')
_TIME = re.compile(r'<time[^>]*\bdatetime="([^"]+)"')
_SCORE = re.compile(r'<span class="score unvoted"[^>]*\btitle="(-?\d+)"')
_BODY = re.compile(r'<div class="usertext-body[^"]*"[^>]*>\s*<div class="md">(.*?)</div>', re.S)


def _iso_utc(s: str) -> str | None:
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None  # no tz info -> do not guess
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(page: str) -> list[RawComment]:
    """Extract comments from a /comments/ listing page. Malformed entries are skipped."""
    starts = [m for m in _THING.finditer(page)]
    out: list[RawComment] = []
    for i, m in enumerate(starts):
        tag = m.group(0)
        if 'data-type="comment"' not in tag:
            continue
        end = starts[i + 1].start() if i + 1 < len(starts) else len(page)
        chunk = page[m.end():end]
        d = {k: _html.unescape(v) for k, v in _DATA.findall(tag)}
        fullname = d.get("fullname", "")
        permalink = d.get("permalink", "")
        tm = _TIME.search(chunk)
        created = _iso_utc(_html.unescape(tm.group(1))) if tm else None
        if not fullname.startswith("t1_") or not permalink or not created:
            continue
        sc = _SCORE.search(chunk)
        body = _BODY.search(chunk)
        out.append(RawComment(
            fullname=fullname,
            subreddit=d.get("subreddit", ""),
            author=d.get("author", ""),
            permalink=permalink,
            created_at=created,
            score=int(sc.group(1)) if sc else None,
            body_html=body.group(1) if body else "",
        ))
    return out
