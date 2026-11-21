"""Request scheduler (TZ section 7): one request at a time, backoff, empty-page handling."""
from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Awaitable, Callable

from .http_client import HttpClient
from .demand import NO_TICKET, Ticket
from .metrics import Metrics

log = logging.getLogger("scheduler")


POPULAR_URL = "https://www.reddit.com/r/popular/comments/?limit=100"


def sub_url(sub: str) -> str:
    return f"https://www.reddit.com/r/{sub}/comments/?limit=100"


class TargetPicker:
    """Hands out request targets: popular + SUBS. Sub choice = least recently polled (round-robin by age)."""

    def __init__(self, subs: list[str], popular_weight: int = 3) -> None:
        self.subs = list(subs)
        self.weight = max(1, popular_weight)
        self._last: dict[str, float] = {s: 0.0 for s in self.subs}
        self._n = 0
        self._acc = 0.0

    def _sub(self) -> str:
        s = min(self.subs, key=lambda x: self._last[x])
        self._last[s] = time.monotonic()
        self._n += 1  # tie-break for equal timestamps on coarse clocks
        self._last[s] += self._n * 1e-9
        return sub_url(s)

    def pick(self) -> str:
        """Direct mode: `popular_weight` popular requests per one sub request."""
        if not self.subs:
            return POPULAR_URL
        self._n += 1
        return self._sub() if self._n % (self.weight + 1) == 0 else POPULAR_URL

    def pick_share(self, share: float) -> str:
        """Nodes mode: fraction `share` of requests goes to popular (deterministic accumulator)."""
        if not self.subs:
            return POPULAR_URL
        self._acc += max(0.0, min(1.0, share))
        if self._acc >= 1.0:
            self._acc -= 1.0
            return POPULAR_URL
        return self._sub()

    @staticmethod
    def is_popular(url: str) -> bool:
        return url == POPULAR_URL


class Scheduler:
    def __init__(self, s, http: HttpClient, metrics: Metrics,
                 on_page: Callable[..., Awaitable[int]], picker: TargetPicker | None = None, demand=None) -> None:
        self.demand = demand
        self.s = s
        self.http = http
        self.m = metrics
        self.on_page = on_page  # returns number of comments parsed from the page
        self.interval = s.req_interval
        self.picker = picker or TargetPicker(s.subs, s.popular_weight)
        self._ban = 60.0
        self._empty_streak = 0
        self._win = [0, 0]  # [pages, bad] for the adaptive mode
        self._win_t = time.monotonic()
        self.stop = asyncio.Event()

    async def _sleep(self, sec: float) -> None:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=sec)
        except asyncio.TimeoutError:
            pass

    def _adapt(self) -> None:
        if not self.s.adaptive or time.monotonic() - self._win_t < 60 or self._win[0] < 10:
            return
        ratio = self._win[1] / self._win[0]
        if ratio > 0.3:
            self.interval = min(self.interval * 1.15, self.s.interval_max)
        elif ratio < 0.1:
            self.interval = max(self.interval * 0.95, self.s.interval_min)
        log.info("adaptive: bad=%.0f%% interval=%.2fs", ratio * 100, self.interval)
        self._win = [0, 0]
        self._win_t = time.monotonic()

    async def run(self) -> None:
        while not self.stop.is_set():
            url = self.picker.pick()
            tk = await self.demand.acquire(self.stop) if self.demand else NO_TICKET
            t0 = time.monotonic()
            try:
                n = await self._one(url, tk)
            finally:
                tk.done()
            self._adapt()
            if n == -1:  # 403/429 or network error: backoff already slept
                continue
            if n == 0:
                self._empty_streak += 1
                if self._empty_streak % 5 == 0:
                    self.m.warn("profile", f"empty pages x{self._empty_streak}: impersonate -> {self.http.rotate_profile()}", 30)
                await self._sleep(random.uniform(2, 5))
                continue
            self._empty_streak = 0
            gap = self.interval * random.uniform(1 - self.s.jitter, 1 + self.s.jitter)
            await self._sleep(max(0.0, gap - (time.monotonic() - t0)))

    async def _one(self, url: str, tk: Ticket = NO_TICKET) -> int:
        self._win[0] += 1
        try:
            status, text = await self.http.fetch(url)
        except Exception as e:  # noqa: BLE001
            tk.done()
            self.m.inc("fetch_err")
            self._win[1] += 1
            self.m.warn("fetch_exc", f"fetch error: {type(e).__name__}", 30)
            await self._sleep(5)
            return -1
        self.m.inc("pages")
        if status != 200:
            tk.done()
        if status in (403, 429):
            self.m.inc(f"http_{status}")
            self._win[1] += 1
            self.m.warn("ban", f"HTTP {status}: backoff {self._ban:.0f}s", 30)
            await self._sleep(self._ban)
            self._ban = min(self._ban * 2, 300.0)
            return -1
        if status != 200:
            self.m.inc("fetch_err")
            self._win[1] += 1
            await self._sleep(5)
            return -1
        self._ban = 60.0
        n = await self.on_page(text, url)
        tk.done()
        if n == 0:
            self.m.inc("pages_empty")
            self._win[1] += 1
        return n
