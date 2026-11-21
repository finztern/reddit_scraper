"""Demand gate: fetch pages only as needed to keep the pre-sender buffer around target_rate*buffer_seconds."""
from __future__ import annotations

import asyncio
import random
from typing import Callable


class Ticket:
    """Marks one in-flight page request; done() is idempotent."""

    def __init__(self, gate: "DemandGate | None") -> None:
        self._gate = gate

    def done(self) -> None:
        g, self._gate = self._gate, None
        if g is not None:
            g.inflight = max(0, g.inflight - 1)


NO_TICKET = Ticket(None)


class DemandGate:
    FLOOR = 100  # watermark never below one page worth of comments

    def __init__(self, sender, rate_fn: Callable[[], float], buf_fn: Callable[[], float]) -> None:
        self.sender, self.rate_fn, self.buf_fn = sender, rate_fn, buf_fn
        self.inflight = 0
        self._turn = asyncio.Lock()
        self.avg_new = 30.0  # EMA of unique comments per page (before url-check)

    def enabled(self) -> bool:
        return self.rate_fn() > 0

    def high(self) -> float:
        return max(self.rate_fn() * self.buf_fn(), self.FLOOR)

    def note_new(self, k: int) -> None:
        self.avg_new = max(1.0, 0.8 * self.avg_new + 0.2 * k)

    def _ok(self) -> bool:
        expected = self.inflight * self.avg_new * self.sender.keep  # pages already requested but not parsed yet
        return self.sender.pending() + expected < self.high()

    async def acquire(self, stop: asyncio.Event | None = None) -> Ticket:
        """Admit workers strictly in arrival order (asyncio.Lock is FIFO) = the longest-idle node/IP goes first,
        so a limited demand is spread evenly and every node is polled as rarely as possible."""
        while self.enabled():
            if stop is not None and stop.is_set():
                break
            async with self._turn:
                while self.enabled() and not (stop is not None and stop.is_set()):
                    if self._ok():
                        self.inflight += 1
                        return Ticket(self)
                    await asyncio.sleep(random.uniform(0.05, 0.15))
        return NO_TICKET
