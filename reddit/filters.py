"""Content filters (TZ section 5). reject_reason() returns a reason string or None."""
from __future__ import annotations

import re

BOT_AUTHORS = frozenset({"automoderator"})

_MEDIA = re.compile(r"‹‹media››.*?‹‹/media››", re.S)
_URL = re.compile(r"https?://\S+")
_EMOJI = re.compile(
    "[\U0001F000-\U0001FAFF\U0001F1E6-\U0001F1FF\u2600-\u27BF\u2300-\u23FF\u2B00-\u2BFF"
    "\u2190-\u21FF\u2900-\u297F\u25AA-\u25FE\uFE0F\u200D\u20E3\u203C\u2049\u2122\u2139\u24C2"
    "\u00A9\u00AE\u3030\u303D\u3297\u3299\U000E0020-\U000E007F]"
)


def reject_reason(author: str, content: str, min_len: int = 7) -> str | None:
    a = author.strip().lower()
    if not a or a in ("[deleted]", "[removed]"):
        return "deleted_author"
    if a in BOT_AUTHORS:
        return "bot"
    body = content.strip()
    if body.lower() in ("[deleted]", "[removed]"):
        return "removed_body"
    no_media = _MEDIA.sub(" ", body)
    no_url = _URL.sub(" ", no_media).replace(" --", " ")
    if not no_url.strip(" -\n\t"):
        return "empty"
    no_emoji = _EMOJI.sub("", no_url)
    if _EMOJI.search(no_url) and not re.sub(r"[\s\-]+", "", no_emoji):
        return "emoji_only"
    if len(" ".join(no_emoji.split())) < min_len:
        return "too_short"
    return None
