"""SeenCache: claim/confirm/release with TTL + LRU (TZ section 5)."""
from __future__ import annotations

import time
from collections import OrderedDict


class SeenCache:
    def __init__(self, max_size: int = 500000, ttl: float = 86400.0, claim_ttl: float = 900.0) -> None:
        self._d: OrderedDict[str, tuple[float, bool]] = OrderedDict()
        self._max = max_size
        self._ttl = ttl
        self._claim_ttl = claim_ttl

    def __len__(self) -> int:
        return len(self._d)

    def has(self, key: str) -> bool:
        """True if key is seen/claimed (does not modify the cache)."""
        cur = self._d.get(key)
        return cur is not None and cur[0] > time.monotonic()

    def claim(self, key: str) -> bool:
        """True if key was not seen (and is now claimed); False if already seen/claimed."""
        now = time.monotonic()
        cur = self._d.get(key)
        if cur is not None and cur[0] > now:
            self._d.move_to_end(key)
            return False
        self._d[key] = (now + self._claim_ttl, False)
        self._d.move_to_end(key)
        while len(self._d) > self._max:
            self._d.popitem(last=False)
        return True

    def confirm(self, key: str) -> None:
        """Mark as delivered/known: stays in cache for the full TTL."""
        self._d[key] = (time.monotonic() + self._ttl, True)
        self._d.move_to_end(key)

    def release(self, key: str) -> None:
        """Failed delivery: forget an unconfirmed claim so the item can be re-collected."""
        cur = self._d.get(key)
        if cur is not None and not cur[1]:
            del self._d[key]
