"""Save raw listing HTML to data/samples and print a short report.

Env: URLS (comma list), PAGES=3, PAUSE=2.0, OUT=data/samples
Run:  docker compose run --rm parser python scripts/dump_samples.py
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from reddit.http_client import HttpClient  # noqa: E402

DEFAULT_URLS = (
    "https://www.reddit.com/r/popular/comments/?limit=100,"
    "https://www.reddit.com/r/AskReddit/comments/?limit=100"
)
URLS = [u for u in os.environ.get("URLS", DEFAULT_URLS).split(",") if u]
PAGES = int(os.environ.get("PAGES", "3"))
PAUSE = float(os.environ.get("PAUSE", "2.0"))
OUT = Path(os.environ.get("OUT", "data/samples"))
THING_RE = re.compile(r'data-fullname="t1_[a-z0-9]+"', re.I)


async def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    c = HttpClient()
    stamp = time.strftime("%Y%m%d_%H%M%S")
    try:
        for url in URLS:
            for p in range(PAGES):
                try:
                    code, html = await c.fetch(url)
                except Exception as exc:  # noqa: BLE001
                    print(f"ERR {url}: {type(exc).__name__}")
                    await asyncio.sleep(PAUSE)
                    continue
                name = re.sub(r"\W+", "_", url.split("reddit.com/")[1])[:40] + f"_{stamp}_{p}.html"
                (OUT / name).write_text(html, encoding="utf-8")
                print(f"{code} {len(html):>7}B {len(THING_RE.findall(html)):>3} comments -> {OUT / name}")
                await asyncio.sleep(PAUSE)
    finally:
        await c.close()


if __name__ == "__main__":
    asyncio.run(main())
