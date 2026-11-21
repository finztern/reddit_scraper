"""Adaptive tuning for nodes mode: per-window node interval (hill-climb with rollback) and popular share."""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

log = logging.getLogger("autotune")


@dataclass
class Window:
    dur: float = 60.0
    pages: int = 0        # node-pool page requests
    c429: int = 0
    warm403: int = 0      # 403 after the session was warmed up
    empty: int = 0        # soft-empty pages
    new: int = 0          # unique new comments (whole pipeline)
    pop_pages: int = 0    # popular pages with comments
    pop_cmt: int = 0
    pop_dup: int = 0
    pop_sat: int = 0      # popular pages where every comment was new

    @property
    def new_s(self) -> float:
        return self.new / max(self.dur, 1e-6)


@dataclass
class _Pending:
    prev: float
    base: float
    left: int = 2
    acc: list[float] = field(default_factory=list)


class AutoTuner:
    DOWN, UP = 0.9, 1.25
    FREEZE = 600.0

    def __init__(self, interval: float, imin: float, imax: float, share: float, enabled: bool = True,
                 path: str | Path | None = None, now: Callable[[], float] = time.time) -> None:
        self.enabled = enabled
        self.imin, self.imax = imin, imax
        self.interval = min(max(interval, imin), imax)
        self.base = self.interval  # recover towards this after a back-off
        self.share = min(max(share, 0.2), 0.7) if enabled else share
        self.fixed_until = 0.0
        self.probe = True  # False when output is demand-limited (target_rate): no hill-climb downwards
        self._pending: _Pending | None = None
        self._path = Path(path) if path else None
        self._now = now
        if enabled:
            self.load()

    # ---- persistence ----
    def load(self) -> None:
        if not self._path or not self._path.exists():
            return
        try:
            d = json.loads(self._path.read_text())
            self.interval = min(max(float(d.get("interval", self.interval)), self.imin), self.imax)
            self.share = min(max(float(d.get("popular_share", self.share)), 0.2), 0.7)
            self.fixed_until = float(d.get("fixed_until", 0.0))
        except Exception:  # noqa: BLE001
            log.warning("nodepool.json unreadable, using defaults")

    def save(self) -> None:
        if not self._path:
            return
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"interval": round(self.interval, 3), "popular_share": round(self.share, 3),
                                   "fixed_until": self.fixed_until}))
        os.replace(tmp, self._path)

    # ---- interval ----
    def step(self, w: Window) -> str:
        if not self.enabled or w.pages < 10:
            return "skip"
        # throttling signal: 429 plus 403 after warm-up
        thr = (w.c429 + w.warm403) / w.pages
        emp = w.empty / w.pages
        now = self._now()
        if thr > 0.08 or emp > 0.40:
            self._pending = None
            self.interval = min(self.interval * self.UP, self.imax)
            return "up"
        if not self.probe:  # demand-limited: never speed up, only return to the base after a back-off
            if thr < 0.03 and emp < 0.25 and self.interval > self.base:
                self.interval = max(self.base, self.interval * self.DOWN)
                return "recover"
            return "hold"
        if self._pending:
            p = self._pending
            p.acc.append(w.new_s)
            p.left -= 1
            if p.left > 0:
                return "wait"
            self._pending = None
            if sum(p.acc) / len(p.acc) <= p.base * 1.03:  # no >3% gain: roll back and freeze
                self.interval = p.prev
                self.fixed_until = now + self.FREEZE
                return "revert"
            return "keep"
        if self.probe and thr < 0.03 and emp < 0.25 and now >= self.fixed_until and self.interval > self.imin:
            self._pending = _Pending(prev=self.interval, base=w.new_s)
            self.interval = max(self.interval * self.DOWN, self.imin)
            return "down"
        return "hold"

    # ---- popular share ----
    def step_share(self, w: Window) -> float:
        if not self.enabled or w.pop_cmt < 200 or w.pop_pages < 3:
            return self.share
        rep = w.pop_dup / w.pop_cmt
        sat = w.pop_sat / w.pop_pages
        if rep > 0.6 and sat < 0.3:        # everyone reads the same feed: spread nodes over SUBS
            self.share = max(0.2, self.share - 0.05)
        elif sat >= 0.5 or rep < 0.4:      # feed not keeping up / few repeats: more popular
            self.share = min(0.7, self.share + 0.05)
        return self.share
