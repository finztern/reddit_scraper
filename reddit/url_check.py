"""url-check client (TZ section 6): returns which items are already known to Exorde.

Each batch is sent as N independent parallel requests (via the N fastest VPN nodes). A URL is known
if ANY successful answer lists it. If every request fails: retry, then URLCHECK_POLICY (open = pass
the batch unchecked, closed = None -> caller retries).
"""
from __future__ import annotations

import asyncio
import logging
import statistics
import time
from collections import deque
from typing import Callable

import httpx

log = logging.getLogger("urlcheck")


def extract_known(data) -> set[str]:
    """Tolerant parser: list of urls, list of {"url":..}, or dict wrapping such a list."""
    if isinstance(data, dict):
        for k in ("known", "existing", "urls", "results", "data"):
            if isinstance(data.get(k), list):
                return extract_known(data[k])
        return {u for u in data if isinstance(u, str) and u.startswith("http")} if data else set()
    out: set[str] = set()
    if isinstance(data, list):
        for x in data:
            if isinstance(x, str):
                out.add(x)
            elif isinstance(x, dict) and isinstance(x.get("url"), str):
                out.add(x["url"])
    return out


class UrlChecker:
    def __init__(self, url: str, proxy_getter: Callable[[], str | None], policy: str = "closed",
                 on_failure: Callable[[str | None], None] | None = None, direct: bool = False,
                 proxies_getter: Callable[[], list[str]] | None = None, retries: int = 1,
                 retry_delay: float = 0.5) -> None:
        self._url = url
        self._proxy = proxy_getter
        self._proxies_fn = proxies_getter   # N fastest proxies; overrides proxy_getter when set
        self._policy = policy
        self._on_failure = on_failure
        self._direct = direct
        self._retries = max(0, retries)
        self._retry_delay = retry_delay
        self._clients: dict[str | None, httpx.AsyncClient] = {}
        self._lat: deque[float] = deque(maxlen=50)

    def median_latency(self) -> float:
        return statistics.median(self._lat) if self._lat else 0.0

    def _targets(self) -> list[str | None]:
        if self._proxies_fn is not None:
            return list(self._proxies_fn())
        p = self._proxy()
        if p is not None:
            return [p]
        return [None] if self._direct else []

    async def _client(self, proxy: str | None) -> httpx.AsyncClient:
        c = self._clients.get(proxy)
        if c is None:
            c = self._clients[proxy] = httpx.AsyncClient(proxy=proxy, timeout=20, trust_env=False)
        return c

    async def _prune(self, keep: list[str | None]) -> None:
        for p in [p for p in self._clients if p not in keep]:
            try:
                await self._clients.pop(p).aclose()
            except Exception:  # noqa: BLE001
                pass

    async def _query(self, proxy: str | None, urls: list[str]) -> set[str]:
        c = await self._client(proxy)
        t0 = time.monotonic()
        r = await c.post(self._url, json={"urls": urls})
        r.raise_for_status()
        known = extract_known(r.json())
        self._lat.append(time.monotonic() - t0)
        return known

    async def unknown(self, items: list[dict]) -> list[dict] | None:
        """Items NOT known to Exorde. None = service unavailable and policy is `closed`."""
        urls = [i["url"] for i in items]
        for attempt in range(self._retries + 1):
            targets = self._targets()
            await self._prune(targets)
            if targets:
                res = await asyncio.gather(*(self._query(p, urls) for p in targets), return_exceptions=True)
                known: set[str] | None = None
                for p, r in zip(targets, res):
                    if isinstance(r, BaseException):
                        log.warning("url-check failed: %s", type(r).__name__)
                        if self._on_failure:
                            self._on_failure(p)
                    else:
                        known = r if known is None else known | r   # any independent answer counts
                if known is not None:
                    return [i for i in items if i["url"] not in known]
            else:
                log.warning("url-check: no proxy available")
            if attempt < self._retries:
                await asyncio.sleep(self._retry_delay)
        return list(items) if self._policy == "open" else None

    async def close(self) -> None:
        for c in self._clients.values():
            await c.aclose()
        self._clients.clear()
