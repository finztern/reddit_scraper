"""Page handler: parse -> filter -> rate cap -> SeenCache claim -> sender."""
from __future__ import annotations

from . import filters, mapper, parser
from .scheduler import TargetPicker


def make_on_page(s, m, seen, sender, parent_mode: str, bucket=None, demand=None):
    async def on_page(text: str, url: str = "") -> int:
        raws = parser.parse(text)
        n = len(raws)
        m.inc("comments", n)
        if not n:
            return 0
        m.inc("pages_cmt")
        collected = mapper.utc_now_str()
        dups = new = 0
        for raw in raws:
            if seen.has(raw.fullname):  # cheap early exit: most comments repeat between nodes
                dups += 1
                m.inc("dup")
                m.drop("dup")
                continue
            content = mapper.build_content(raw.body_html)
            reason = filters.reject_reason(raw.author, content, s.min_text_len)
            if reason:
                m.drop(reason)
                continue
            if bucket and not bucket.take():  # before claim: a dropped item stays collectable
                m.drop("rate_cap")
                continue
            if not seen.claim(raw.fullname):
                dups += 1
                m.inc("dup")
                m.drop("dup")
                continue
            m.inc("unique")
            new += 1
            await sender.submit(mapper.to_item(raw, content, collected, parent_mode))
        if demand:
            demand.note_new(new)
        sat = n >= 90 and dups == 0
        if sat:
            m.inc("pages_sat")
        if TargetPicker.is_popular(url):
            m.inc("pop_pages")
            m.inc("pop_cmt", n)
            m.inc("pop_dup", dups)
            if sat:
                m.inc("pop_sat")
        return n

    return on_page
