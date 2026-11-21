"""Optional limiters (all off when value is 0): item token bucket, page pacing, rolling traffic budget."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Callable

log = logging.getLogger("limits")


class TokenBucket:
    """Non-blocking token bucket. rate<=0 -> unlimited."""

    def __init__(self, rate: float, burst: float | None = None, now: Callable[[], float] = time.monotonic) -> None:
        self.rate = rate
        self.burst = burst if burst is not None else max(rate, 1.0)
        self._now = now
        self._tokens = self.burst
        self._t = now()

    def take(self, n: float = 1.0) -> bool:
        if self.rate <= 0:
            return True
        t = self._now()
        self._tokens = min(self.burst, self._tokens + (t - self._t) * self.rate)
        self._t = t
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False


class PageGate:
    """Global request pacing (slot reservation): at most `rate` page requests/s across all workers."""

    def __init__(self, rate: float, now: Callable[[], float] = time.monotonic) -> None:
        self.rate = rate
        self._now = now
        self._next = 0.0

    async def wait(self) -> None:
        if self.rate <= 0:
            return
        gap = 1.0 / self.rate
        t = self._now()
        slot = max(t, self._next)
        self._next = slot + gap
        if slot > t:
            await asyncio.sleep(slot - t)


class TrafficBudget:
    """Rolling 24h byte counter, persisted to data/traffic.json (per-minute buckets, wall clock)."""

    WINDOW = 86400

    def __init__(self, max_gb: float, path: str | Path | None = None, now: Callable[[], float] = time.time) -> None:
        self.max_bytes = max_gb * 1e9
        self._path = Path(path) if path else None
        self._now = now
        self._b: dict[int, int] = {}
        self._dirty = False
        self._load()

    def _prune(self) -> None:
        lim = int(self._now() // 60) - self.WINDOW // 60
        for k in [k for k in self._b if k <= lim]:
            del self._b[k]

    def _load(self) -> None:
        if not self._path or not self._path.exists():
            return
        try:
            self._b = {int(k): int(v) for k, v in json.loads(self._path.read_text()).items()}
        except Exception:  # noqa: BLE001
            self._b = {}
        self._prune()

    def save(self) -> None:
        if not self._path or not self._dirty:
            return
        self._prune()
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._b))
        os.replace(tmp, self._path)
        self._dirty = False

    def add(self, nbytes: int) -> None:
        k = int(self._now() // 60)
        self._b[k] = self._b.get(k, 0) + nbytes
        self._dirty = True

    def used_bytes(self) -> int:
        self._prune()
        return sum(self._b.values())

    def used_gb(self) -> float:
        return self.used_bytes() / 1e9

    def ratio(self) -> float:
        return self.used_bytes() / self.max_bytes if self.max_bytes > 0 else 0.0

    def interval_factor(self) -> float:
        """x2 slowdown from 90% of the daily budget; 1 when disabled."""
        return 2.0 if self.max_bytes > 0 and self.ratio() >= 0.9 else 1.0

    def blocked(self) -> bool:
        return self.max_bytes > 0 and self.ratio() >= 1.0
