"""Counters + one-line periodic log + throttled warnings."""
from __future__ import annotations

import logging
import time
from collections import Counter
from typing import Callable

log = logging.getLogger("metrics")


class Metrics:
    def __init__(self) -> None:
        self.c: Counter[str] = Counter()
        self.filtered: Counter[str] = Counter()
        self.gauges: dict[str, Callable[[], int]] = {}
        self._last = time.monotonic()
        self._snap: Counter[str] = Counter()
        self._snap_f: Counter[str] = Counter()
        self._warn_at: dict[str, float] = {}

    def inc(self, key: str, n: int = 1) -> None:
        self.c[key] += n

    def drop(self, reason: str) -> None:
        self.filtered[reason] += 1

    def warn(self, key: str, msg: str, every: float = 60.0) -> None:
        now = time.monotonic()
        if now - self._warn_at.get(key, -1e9) >= every:
            self._warn_at[key] = now
            logging.getLogger("app").warning(msg)

    def line(self) -> str:
        now = time.monotonic()
        dt = max(now - self._last, 1e-6)
        d = {k: self.c[k] - self._snap[k] for k in self.c}
        g = lambda k: d.get(k, 0)  # noqa: E731
        pages = g("pages")
        empty_pct = 100.0 * g("pages_empty") / pages if pages else 0.0
        uc_in = g("uc_in")
        known_pct = 100.0 * g("uc_known") / uc_in if uc_in else 0.0
        fl = {k: self.filtered[k] - self._snap_f[k] for k in self.filtered if self.filtered[k] - self._snap_f[k]}
        gauges = " ".join(f"{k}={fn()}" for k, fn in self.gauges.items())
        out = (
            f"pages/s={pages/dt:.2f} comments/s={g('comments')/dt:.1f} new/s={g('unique')/dt:.1f} "
            f"empty={empty_pct:.0f}% 403={g('http_403')} 429={g('http_429')} err={g('fetch_err')} "
            f"filtered={fl or 0} uc_known={known_pct:.0f}% uc_err={g('uc_err')} "
            f"sent/s={g('sent')/dt:.1f} store_err={g('store_err')} spooled={g('spooled')} {gauges}"
        )
        self._last = now
        self._snap = Counter(self.c)
        self._snap_f = Counter(self.filtered)
        return out
