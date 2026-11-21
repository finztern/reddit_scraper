"""url-check client (TZ section 6): returns which items are already known to Exorde."""
from __future__ import annotations

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
                 on_failure: Callable[[], None] | None = None, direct: bool = False) -> None:
        self._url = url
        self._proxy = proxy_getter
        self._policy = policy
        self._on_failure = on_failure
        self._direct = direct
        self._client: httpx.AsyncClient | None = None
        self._client_proxy: str | None = None
        self._lat: deque[float] = deque(maxlen=50)

    def median_latency(self) -> float:
        return statistics.median(self._lat) if self._lat else 0.0

    async def _get_client(self) -> httpx.AsyncClient | None:
        proxy = self._proxy()
        if proxy is None and not self._direct:
            return None
        if self._client is None or proxy != self._client_proxy:
            if self._client:
                await self._client.aclose()
            self._client = httpx.AsyncClient(proxy=proxy, timeout=20, trust_env=False)
            self._client_proxy = proxy
        return self._client

    async def unknown(self, items: list[dict]) -> list[dict] | None:
        """Items NOT known to Exorde. None = service unavailable and policy is `closed`."""
        try:
            c = await self._get_client()
            if c is None:
                raise RuntimeError("no proxy available")
            t0 = time.monotonic()
            r = await c.post(self._url, json={"urls": [i["url"] for i in items]})
            r.raise_for_status()
            self._lat.append(time.monotonic() - t0)
            known = extract_known(r.json())
        except Exception as e:  # noqa: BLE001
            log.warning("url-check failed: %s", type(e).__name__)
            if self._on_failure:
                self._on_failure()
            return list(items) if self._policy == "open" else None
        return [i for i in items if i["url"] not in known]

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
