"""ExternalVpn (VPN_MODE=external): read nodes published by xray-hub (hub/data/nodes.json). No own xray."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Callable

log = logging.getLogger("vpn")


class ExternalVpn:
    def __init__(self, path: str, poll: float = 30.0, max_age: float = 3600.0) -> None:
        self._path = Path(path)
        self._poll = poll
        self._max_age = max_age
        self._nodes: list[dict] = []
        self._mtime = -1
        self._stop = asyncio.Event()
        self.on_nodes_changed: Callable[[], None] | None = None

    def _alive(self) -> list[dict]:
        return [n for n in self._nodes if n.get("alive")]

    @property
    def ready(self) -> bool:
        return bool(self._alive())

    def load(self) -> bool:
        """Re-read the file. True when the set of alive nodes changed."""
        try:
            st = self._path.stat()
            if st.st_mtime_ns == self._mtime:
                return False
            self._mtime = st.st_mtime_ns
            d = json.loads(self._path.read_text())
            if time.time() - float(d.get("updated", 0)) > self._max_age:
                log.warning("hub nodes.json is stale (hub down?)")
            nodes = [n for n in d["nodes"] if isinstance(n.get("port"), int) and n.get("key")]
        except Exception as e:  # noqa: BLE001
            log.warning("hub nodes.json unreadable: %s", type(e).__name__)
            return False
        before = {(n["key"], n["port"]) for n in self._alive()}
        self._nodes = nodes
        after = {(n["key"], n["port"]) for n in self._alive()}
        if before != after:
            log.info("hub: %d/%d nodes alive", len(after), len(nodes))
        return before != after

    def node_ports(self) -> list[tuple[str, str, int]]:
        return [(n["key"], n.get("name") or n["key"], n["port"]) for n in self._alive()]

    def _by_speed(self) -> list[dict]:
        return sorted(self._alive(), key=lambda n: n.get("latency_ms") or 1e9)

    def proxy(self) -> str | None:
        f = self._by_speed()
        return f"socks5h://127.0.0.1:{f[0]['port']}" if f else None

    def proxies(self, n: int = 2) -> list[str]:
        return [f"socks5h://127.0.0.1:{x['port']}" for x in self._by_speed()[:n]]

    def report_failure(self, proxy: str | None = None) -> None:
        pass  # the hub tests nodes itself

    async def start(self) -> None:
        self.load()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll)
            except asyncio.TimeoutError:
                pass
            if not self._stop.is_set() and self.load() and self.on_nodes_changed:
                self.on_nodes_changed()

    async def stop(self) -> None:
        self._stop.set()
