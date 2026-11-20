"""RawComment -> Exorde item dict (format: TZ section 3)."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.parse import urlparse

from .models import RawComment

PARENT_MODES = ("null", "post")
_BLOCK = {"div", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "ul", "ol", "pre", "table", "tr", "hr"}
_MEDIA_EXT = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4")


def validate_parent_mode(mode: str) -> str:
    if mode == "resolve":
        raise ValueError("PARENT_MODE=resolve is not implemented in v1")
    if mode not in PARENT_MODES:
        raise ValueError(f"PARENT_MODE must be one of {PARENT_MODES}, got {mode!r}")
    return mode


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _plain(s: str) -> str:
    return s.lower().replace("https://", "").replace("http://", "").replace("www.", "")


def _is_media_href(u: str) -> bool:
    p = urlparse(u)
    return (p.hostname or "").endswith("redd.it") or p.path.lower().endswith(_MEDIA_EXT)


class _Html2Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.out: list[str] = []
        self.links: list[str] = []   # all http(s) hrefs, in order
        self.imgs: list[str] = []    # <img src>
        self._pre = 0

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "p" or tag in _BLOCK:
            self.out.append("\n")
        elif tag == "br":
            self.out.append("\n")
        elif tag == "li":
            self.out.append("\n- ")
        if tag == "pre":
            self._pre += 1
        elif tag == "a":
            self.links.append(a.get("href") or "")
        elif tag == "img" and a.get("src"):
            self.imgs.append(a["src"])

    def handle_endtag(self, tag):
        if tag == "p":
            self.out.append("\n\n")
        elif tag in _BLOCK:
            self.out.append("\n")
        if tag == "pre" and self._pre:
            self._pre -= 1

    def handle_data(self, data):
        self.out.append(data if self._pre else re.sub(r"\s+", " ", data))


def build_content(body_html: str) -> str:
    """HTML body -> plain text + ' --<url>' for hidden external links + media block."""
    p = _Html2Text()
    p.feed(body_html)
    p.close()
    lines = [ln.strip() for ln in "".join(p.out).split("\n")]
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
    visible = _plain(text)

    extra: list[str] = []
    media: list[str] = []
    seen: set[str] = set()
    for href in p.links:
        if not href.startswith(("http://", "https://")):
            continue
        key = _plain(href).rstrip("/")
        if key in seen or key in visible:
            continue
        seen.add(key)
        (media if _is_media_href(href) else extra).append(href)
    for src in p.imgs:
        if src.startswith(("http://", "https://")) and src not in media:
            media.append(src)

    content = text
    for u in extra:
        content += f" --{u}"
    if media:
        content += ("\n" if content else "") + "‹‹media›› " + " ".join(media) + " ‹‹/media››"
    return content.strip()


def parent_id(raw: RawComment, mode: str) -> str | None:
    if mode == "post":
        pid = raw.post_id
        return f"t3_{pid}" if pid else None
    return None  # "null": do not invent a parent


def to_item(raw: RawComment, content: str, collected_at: str, parent_mode: str = "null") -> dict:
    return {
        "content": content,
        "external_id": raw.fullname,
        "created_at": raw.created_at,
        "collected_at": collected_at,
        "url": "https://www.reddit.com" + raw.permalink,
        "title": "",
        "author": hashlib.sha1(raw.author.encode("utf-8")).hexdigest(),
        "username": raw.author,
        "external_parent_id": parent_id(raw, parent_mode),
        "domain": "reddit.com",
        "summary": json.dumps({
            "platform": "reddit",
            "communityName": raw.subreddit,
            "data_type": "comment",
            "score": raw.score,
        }),
        "userprofile_url": f"https://www.reddit.com/user/{raw.author}",
    }
