"""VPN manager: vless subscription -> one xray process (a SOCKS inbound per node) -> fastest node.

All nodes live in a single xray instance (inbound i -> outbound i), so switching the
active node is just choosing another local port: no xray restart. xray is restarted
only when the node set changes after a subscription refresh.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import os
import statistics
import time
from dataclasses import dataclass
from typing import Callable
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

import httpx

log = logging.getLogger("vpn")


@dataclass(frozen=True)
class Node:
    key: str
    name: str
    host: str
    port: int
    uuid: str
    params: dict[str, str]


def parse_vless(uri: str) -> Node | None:
    uri = uri.strip()
    if not uri.startswith("vless://"):
        return None
    try:
        u = urlsplit(uri)
        if not (u.username and u.hostname and u.port):
            return None
        q = {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
        key = hashlib.sha1(uri.split("#", 1)[0].encode()).hexdigest()[:10]
        return Node(key, unquote(u.fragment) or key, u.hostname, u.port, u.username, q)
    except ValueError:
        return None


def parse_subscription(text: str) -> list[Node]:
    text = text.strip()
    if "://" not in text:  # base64-encoded subscription
        try:
            raw = text + "=" * (-len(text) % 4)
            text = base64.urlsafe_b64decode(raw).decode("utf-8", "ignore")
        except Exception:  # noqa: BLE001
            return []
    seen: dict[str, Node] = {}
    for line in text.splitlines():
        n = parse_vless(line)
        if n:
            seen[n.key] = n
    return sorted(seen.values(), key=lambda n: n.key)


def _stream(n: Node) -> dict:
    p = n.params
    net = p.get("type", "tcp")
    sec = p.get("security", "none")
    st: dict = {"network": net, "security": sec, "sockopt": {"domainStrategy": "UseIPv4"}}
    path, host = unquote(p.get("path", "/")), p.get("host", "")
    if net == "ws":
        st["wsSettings"] = {"path": path, "headers": {"Host": host} if host else {}}
    elif net == "httpupgrade":
        st["httpupgradeSettings"] = {"path": path, "host": host}
    elif net in ("xhttp", "splithttp"):
        st["network"] = "xhttp"
        st["xhttpSettings"] = {"path": path, "host": host, "mode": p.get("mode", "auto")}
    elif net == "grpc":
        st["grpcSettings"] = {"serviceName": unquote(p.get("serviceName", ""))}
    elif net == "tcp" and p.get("headerType") == "http":
        st["tcpSettings"] = {"header": {"type": "http", "request": {"path": [path], "headers": {"Host": [host]}}}}
    if sec == "tls":
        tls = {"serverName": p.get("sni") or host or n.host, "fingerprint": p.get("fp") or "chrome"}
        if p.get("alpn"):
            tls["alpn"] = p["alpn"].split(",")
        st["tlsSettings"] = tls
    elif sec == "reality":
        st["realitySettings"] = {
            "serverName": p.get("sni", ""), "fingerprint": p.get("fp") or "chrome",
            "publicKey": p.get("pbk", ""), "password": p.get("pbk", ""),
            "shortId": p.get("sid", ""), "spiderX": unquote(p.get("spx", "")),
        }
    return st


def build_xray_config(nodes: list[Node], port_base: int) -> dict:
    inbounds, outbounds, rules = [], [], []
    for i, n in enumerate(nodes):
        inbounds.append({"tag": f"in{i}", "listen": "127.0.0.1", "port": port_base + i, "protocol": "socks",
                         "settings": {"auth": "noauth", "udp": False}})
        user = {"id": n.uuid, "encryption": n.params.get("encryption", "none") or "none"}
        if n.params.get("flow"):
            user["flow"] = n.params["flow"]
        outbounds.append({"tag": f"out{i}", "protocol": "vless",
                          "settings": {"vnext": [{"address": n.host, "port": n.port, "users": [user]}]},
                          "streamSettings": _stream(n)})
        rules.append({"type": "field", "inboundTag": [f"in{i}"], "outboundTag": f"out{i}"})
    return {"log": {"loglevel": "warning"}, "inbounds": inbounds, "outbounds": outbounds,
            "routing": {"domainStrategy": "AsIs", "rules": rules}}


class VpnManager:
    def __init__(self, sub_url: str, data_dir: str, test_url: str, port_base: int = 21000,
                 test_interval: float = 600.0, sub_refresh: float = 3600.0, switch_gain: float = 0.8,
                 xray_bin: str = "xray", nodes_file: str = "") -> None:
        self._sub_url = sub_url
        self._file = nodes_file
        self._dir = Path(data_dir) / "xray"
        self._test_url = test_url
        self._base = port_base
        self._test_iv = test_interval
        self._refresh_iv = min(sub_refresh, test_interval) if nodes_file else sub_refresh  # local file: cheap re-read
        self._gain = switch_gain
        self._bin = xray_bin
        self._nodes: list[Node] = []
        self._lat: dict[str, float] = {}      # node key -> median latency (s); absent = dead
        self._cur: str | None = None
        self._proc: asyncio.subprocess.Process | None = None
        self._wake = asyncio.Event()
        self._last_test = 0.0
        self._stop = False
        self.on_nodes_changed: Callable[[], None] | None = None

    # ---- public API ----
    @property
    def ready(self) -> bool:
        return bool(self._nodes) and self._proc is not None and self._proc.returncode is None

    def node_ports(self) -> list[tuple[str, str, int]]:
        """(key, name, socks port) for every node; ports are base+index of the current node set."""
        return [(n.key, n.name, self._base + i) for i, n in enumerate(self._nodes)]

    def proxy(self) -> str | None:
        for i, n in enumerate(self._nodes):
            if n.key == self._cur:
                return f"socks5h://127.0.0.1:{self._base + i}"
        return None

    def report_failure(self) -> None:
        """url-check failed through the current node: drop it and re-test soon."""
        if self._cur:
            self._lat.pop(self._cur, None)
            self._pick(force=True)
        if time.monotonic() - self._last_test > 30:
            self._wake.set()

    async def start(self) -> None:
        self._dir.mkdir(parents=True, exist_ok=True)
        await self._refresh_subscription()
        await self._test_all()

    async def run(self) -> None:
        last_refresh = time.monotonic()
        while not self._stop:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self._test_iv)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()
            if self._stop:
                break
            try:
                if time.monotonic() - last_refresh >= self._refresh_iv:
                    last_refresh = time.monotonic()
                    await self._refresh_subscription()
                await self._test_all()
                if self._proc and self._proc.returncode is not None:
                    log.warning("xray exited (code %s), restarting", self._proc.returncode)
                    await self._restart_xray()
                    await self._test_all()
            except Exception as e:  # noqa: BLE001
                log.warning("vpn loop error: %s", type(e).__name__)

    async def stop(self) -> None:
        self._stop = True
        self._wake.set()
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), 5)
            except asyncio.TimeoutError:
                self._proc.kill()

    # ---- internals ----
    async def _refresh_subscription(self) -> None:
        cache = self._dir / "nodes.cache"
        text = ""
        try:
            if self._file:
                text = Path(self._file).read_text()
            else:
                async with httpx.AsyncClient(timeout=20, trust_env=False) as c:
                    r = await c.get(self._sub_url)
                    r.raise_for_status()
                    text = r.text
        except Exception as e:  # noqa: BLE001
            log.warning("nodes %s failed: %s", "file read" if self._file else "subscription download", type(e).__name__)
        nodes = parse_subscription(text) if text else []
        if nodes:
            cache.write_text(text)
            os.chmod(cache, 0o600)
        elif not self._nodes and cache.exists():
            nodes = parse_subscription(cache.read_text())
            log.warning("using cached subscription (%d nodes)", len(nodes))
        if not nodes:
            log.warning("no vless nodes available; keeping previous set (%d)", len(self._nodes))
            return
        if [n.key for n in nodes] != [n.key for n in self._nodes]:
            self._nodes = nodes
            self._lat = {k: v for k, v in self._lat.items() if k in {n.key for n in nodes}}
            log.info("subscription: %d nodes, restarting xray", len(nodes))
            await self._restart_xray()
            if self.on_nodes_changed:
                try:
                    self.on_nodes_changed()
                except Exception:  # noqa: BLE001
                    log.warning("on_nodes_changed callback failed")
        else:
            log.info("subscription refreshed: %d nodes, unchanged", len(nodes))

    async def _restart_xray(self) -> None:
        if self._proc and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), 5)
            except asyncio.TimeoutError:
                self._proc.kill()
        cfg = self._dir / "config.json"
        cfg.write_text(json.dumps(build_xray_config(self._nodes, self._base)))
        os.chmod(cfg, 0o600)
        logf = open(self._dir / "xray.log", "w")  # noqa: SIM115
        self._proc = await asyncio.create_subprocess_exec(
            self._bin, "run", "-c", str(cfg), stdout=logf, stderr=logf)
        await asyncio.sleep(1.5)
        if self._proc.returncode is not None:
            log.error("xray failed to start, see data/xray/xray.log")

    async def _probe(self, i: int, sem: asyncio.Semaphore) -> float | None:
        async with sem:
            times: list[float] = []
            try:
                async with httpx.AsyncClient(proxy=f"socks5h://127.0.0.1:{self._base + i}",
                                             timeout=8, trust_env=False) as c:
                    for _ in range(3):
                        t = time.monotonic()
                        try:
                            r = await c.post(self._test_url, json={"urls": ["https://www.reddit.com/"]})
                            if r.status_code < 500:
                                times.append(time.monotonic() - t)
                        except Exception:  # noqa: BLE001
                            pass
            except Exception:  # noqa: BLE001
                pass
            return statistics.median(times) if len(times) >= 2 else None

    async def _test_all(self) -> None:
        if not self._nodes:
            return
        self._last_test = time.monotonic()
        sem = asyncio.Semaphore(8)
        res = await asyncio.gather(*(self._probe(i, sem) for i in range(len(self._nodes))))
        self._lat = {n.key: r for n, r in zip(self._nodes, res) if r is not None}
        log.info("vpn test: %d/%d alive, best=%s", len(self._lat), len(self._nodes),
                 ", ".join(f"{n.name}:{int(self._lat[n.key]*1000)}ms" for n in
                           sorted((n for n in self._nodes if n.key in self._lat), key=lambda n: self._lat[n.key])[:3]) or "none")
        self._pick()

    def _pick(self, force: bool = False) -> None:
        if not self._lat:
            if self._cur:
                log.warning("vpn: no alive nodes")
            self._cur = None
            return
        best = min(self._lat, key=self._lat.get)
        cur_ok = self._cur in self._lat
        if self._cur is None or not cur_ok or self._lat[best] < self._lat[self._cur] * self._gain:
            if best != self._cur:
                name = next(n.name for n in self._nodes if n.key == best)
                log.info("vpn: using %s (%dms)", name, int(self._lat[best] * 1000))
            self._cur = best
