"""Print the raw url-check response for a few urls (use to verify the answer format and the proxy)."""
import asyncio
import os
import sys

import httpx

URL = os.environ.get("URLCHECK_URL", "https://url-check.exorde.network/v1/check")


async def main() -> None:
    proxy = os.environ.get("URLCHECK_PROXY") or None
    urls = sys.argv[1:] or ["https://www.reddit.com/r/AskReddit/comments/1/x/2/"]
    async with httpx.AsyncClient(proxy=proxy, timeout=20, trust_env=False) as c:
        r = await c.post(URL, json={"urls": urls})
        print(r.status_code, r.text[:500])


asyncio.run(main())
