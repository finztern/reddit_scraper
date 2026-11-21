"""Hot-reloadable runtime settings: config/runtime.yaml is polled, changes apply without restart."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import yaml

log = logging.getLogger("runtime")


class RuntimeConfig:
    def __init__(self, path: str | Path, target_rate: float = 0.0, buffer_seconds: float = 5.0) -> None:
        self.path = Path(path)
        self.target_rate = target_rate        # unique comments/s sent to the collector; 0 = unlimited
        self.buffer_seconds = buffer_seconds  # how many seconds of output are kept pre-fetched
        self._mtime = -1
        self.reload(first=True)

    def reload(self, first: bool = False) -> bool:
        try:
            st = self.path.stat()
        except OSError:
            return False
        if st.st_mtime_ns == self._mtime:
            return False
        self._mtime = st.st_mtime_ns
        try:
            d = yaml.safe_load(self.path.read_text()) or {}
            rate = float(d.get("target_rate", self.target_rate))
            buf = float(d.get("buffer_seconds", self.buffer_seconds))
            if not (0 <= rate <= 100000 and 1 <= buf <= 300):
                raise ValueError("target_rate must be 0..100000, buffer_seconds 1..300")
        except Exception as e:  # noqa: BLE001
            log.warning("runtime.yaml ignored (%s), keeping target_rate=%g", e, self.target_rate)
            return False
        if first or (rate, buf) != (self.target_rate, self.buffer_seconds):
            log.info("runtime: target_rate=%g/s buffer_seconds=%g%s", rate, buf, "" if first else " (reloaded)")
        changed = (rate, buf) != (self.target_rate, self.buffer_seconds)
        self.target_rate, self.buffer_seconds = rate, buf
        return changed

    async def watch(self, every: float = 2.0) -> None:
        while True:
            await asyncio.sleep(every)
            self.reload()
