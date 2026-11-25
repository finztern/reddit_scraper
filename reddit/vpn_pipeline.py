"""PipelineVpn (VPN_MODE=pipeline): live node list from the local pipeline service (GET NODES_URL, JSON).

Every alive node = SOCKS5 127.0.0.1:<port>. The list is re-polled every NODES_POLL seconds;
a changed alive set triggers a NodePool rescan. No own xray.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable

import httpx

log = logging.getLogger("vpn")


def parse_nodes(data) -> list[dict]:
    """-> [{key, name, port, latency_ms}] for nodes with ok=true and an int port."""
    out: list[dict] = []
    for n in (data or {}).get("nodes", []):
        if not (n.get("ok") and isinstance(n.get("port"), int)):
            continue
        key = str(n.get("tag") or n["port"])
        out.append({"key": key, "name": n.get("name") or key, "port": n["port"],
                    "latency_ms": n.get("latency_ms") or 1e9})
    return sorted(out, key=lambda x: x["port"])


class PipelineVpn:
    def __init__(self, url: str, poll: float = 30.0) -> None:
        self._url = url
        self._poll = poll
        self._nodes: list[dict] = []
        self._stop = asyncio.Event()
        self._warned = False
        self.on_nodes_changed: Callable[[], None] | None = None

    @property
    def ready(self) -> bool:
        return bool(self._nodes)

    async def load(self) -> bool:
        """Fetch the list. True when the set of (key, port) changed. On error keep the previous set."""
        try:
            async with httpx.AsyncClient(timeout=10, trust_env=False) as c:
                r = await c.get(self._url)
                r.raise_for_status()
                nodes = parse_nodes(r.json())
        except Exception as e:  # noqa: BLE001
            if not self._warned:
                log.warning("nodes list unavailable (%s): keeping %d nodes", type(e).__name__, len(self._nodes))
                self._warned = True
            return False
        self._warned = False
        before = {(n["key"], n["port"]) for n in self._nodes}
        self._nodes = nodes
        after = {(n["key"], n["port"]) for n in nodes}
        if before != after:
            log.info("pipeline nodes: %d alive (+%d/-%d)", len(after), len(after - before), len(before - after))
        return before != after

    def node_ports(self) -> list[tuple[str, str, int]]:
        return [(n["key"], n["name"], n["port"]) for n in self._nodes]

    def _by_speed(self) -> list[dict]:
        return sorted(self._nodes, key=lambda n: n["latency_ms"])

    def proxy(self) -> str | None:
        f = self._by_speed()
        return f"socks5h://127.0.0.1:{f[0]['port']}" if f else None

    def proxies(self, n: int = 2) -> list[str]:
        return [f"socks5h://127.0.0.1:{x['port']}" for x in self._by_speed()[:n]]

    def report_failure(self, proxy: str | None = None) -> None:
        pass  # the pipeline tests nodes itself

    async def start(self) -> None:
        await self.load()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll)
            except asyncio.TimeoutError:
                pass
            if not self._stop.is_set() and await self.load() and self.on_nodes_changed:
                self.on_nodes_changed()

    async def stop(self) -> None:
        self._stop.set()
