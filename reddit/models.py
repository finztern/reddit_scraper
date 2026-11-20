"""Data models."""
from __future__ import annotations

import re
from dataclasses import dataclass

_POST_ID = re.compile(r"/comments/([0-9a-z]+)/")


@dataclass(slots=True)
class RawComment:
    fullname: str          # t1_<base36>
    subreddit: str
    author: str            # may be "[deleted]"
    permalink: str         # "/r/<sub>/comments/<post>/<slug>/<id>/"
    created_at: str        # UTC, YYYY-MM-DDTHH:MM:SSZ
    score: int | None      # None when hidden by the subreddit
    body_html: str         # inner HTML of div.md

    @property
    def post_id(self) -> str | None:
        m = _POST_ID.search(self.permalink)
        return m.group(1) if m else None
