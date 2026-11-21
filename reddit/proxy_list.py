"""ProxyList (SOURCE_MODE=proxy): proxies from a text file, one per line, exposed like VpnManager nodes.

Line formats: user:pass@host:port | host:port:user:pass | host:port | scheme://user:pass@host:port
(default scheme http; socks5/socks5h also work). Secrets stay in the file (git-ignored), never in logs.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from pathlib import Path
from typing import Callable
from urllib.parse import urlsplit

log = logging.getLogger("proxylist")


def parse_proxy(line: str) -> tuple[str, str, str] | None:
    """-> (key, name, proxy_url) or None. `name` is host:port only (no credentials)."""
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "://" not in line:
        parts = line.split(":")
        if "@" not in line and len(parts) == 4:
            host, port, user, pw = parts
            line = f"{user}:{pw}@{host}:{port}"
        line = "http://" + line
    try:
        u = urlsplit(line)
        if not (u.hostname and u.port) or u.scheme not in ("http", "https", "socks5", "socks5h", "socks4"):
            return None
    except ValueError:
        return None
    return hashlib.sha1(line.encode()).hexdigest()[:10], f"{u.hostname}:{u.port}", line


def parse_proxies(text: str) -> list[tuple[str, str, str]]:
    seen: dict[str, tuple[str, str, str]] = {}
    for ln in text.splitlines():
        p = parse_proxy(ln)
        if p:
            seen[p[0]] = p
    return sorted(seen.values(), key=lambda x: x[0])


class ProxyList:
    def __init__(self, path: str | Path, reload_every: float = 60.0) -> None:
        self._path = Path(path)
        self._every = reload_every
        self._items: list[tuple[str, str, str]] = []
        self._mtime = -1
        self._stop = asyncio.Event()
        self.on_nodes_changed: Callable[[], None] | None = None

    def load(self) -> bool:
        """Re-read the file if it changed. True when the proxy set changed."""
        try:
            mt = self._path.stat().st_mtime_ns
            if mt == self._mtime:
                return False
            self._mtime = mt
            items = parse_proxies(self._path.read_text())
        except OSError as e:
            log.warning("proxy file unreadable: %s", type(e).__name__)
            return False
        if [i[0] for i in items] == [i[0] for i in self._items]:
            return False
        self._items = items
        log.info("proxylist: %d proxies loaded", len(items))
        return True

    def node_ports(self) -> list[tuple[str, str, str]]:
        """(key, name, proxy_url): same shape as VpnManager.node_ports (endpoint is a URL instead of a port)."""
        return list(self._items)

    async def start(self) -> None:
        self.load()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._every)
            except asyncio.TimeoutError:
                pass
            if not self._stop.is_set() and self.load() and self.on_nodes_changed:
                self.on_nodes_changed()

    async def stop(self) -> None:
        self._stop.set()
