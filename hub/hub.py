"""hub: one sing-box (vless + hysteria2) for the whole host, shared by all parsers.

- every node of the subscription gets a SOCKS inbound on a STABLE port (key -> port, persisted in ports.json)
- 127.0.0.1:HUB_BEST_PORT (10809) always goes through the fastest alive node (sing-box urltest)
- every HUB_TEST_MINUTES each node is probed through its own port; result -> data/nodes.json
- subscription is re-downloaded every HUB_SUB_HOURS; sing-box restarts only when the node set changes
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import statistics
import time
from pathlib import Path

import httpx

from urllib.parse import unquote

from reddit.vpn import Node, parse_subscription

log = logging.getLogger("hub")


def assign_ports(keys: list[str], old: dict[str, int], base: int, skip: set[int]) -> dict[str, int]:
    """Keep the old port of every surviving node; new nodes take the lowest free port."""
    ports = {k: old[k] for k in keys if k in old}
    used = set(ports.values()) | skip
    nxt = base
    for k in keys:
        if k in ports:
            continue
        while nxt in used:
            nxt += 1
        ports[k] = nxt
        used.add(nxt)
    return ports


def _tls(n: Node) -> dict:
    p = n.params
    tls: dict = {"enabled": True, "server_name": p.get("sni") or p.get("host") or n.host}
    if p.get("alpn"):
        tls["alpn"] = p["alpn"].split(",")
    if p.get("insecure") in ("1", "true") or p.get("allowInsecure") in ("1", "true"):
        tls["insecure"] = True
    if n.scheme == "vless":
        fp = p.get("fp") or "chrome"
        tls["utls"] = {"enabled": True, "fingerprint": fp}
        if p.get("security") == "reality":
            tls["reality"] = {"enabled": True, "public_key": p.get("pbk", ""), "short_id": p.get("sid", "")}
    return tls


def supported(n: Node) -> bool:
    """sing-box has no xhttp/splithttp transport; everything else from the subscription is usable."""
    if n.scheme == "hysteria2":
        return True
    return n.params.get("type", "tcp") not in ("xhttp", "splithttp") and n.params.get("security", "none") in ("tls", "reality", "none")


def build_outbound(n: Node, tag: str) -> dict:
    o: dict = {"tag": tag, "server": n.host, "server_port": n.port, "domain_strategy": "ipv4_only"}
    p = n.params
    if n.scheme == "hysteria2":
        o.update(type="hysteria2", password=n.uuid, tls=_tls(n))
        if p.get("obfs"):
            o["obfs"] = {"type": p["obfs"], "password": p.get("obfs-password", "")}
        return o
    o.update(type="vless", uuid=n.uuid)
    if p.get("flow"):
        o["flow"] = p["flow"]
    if p.get("security") in ("tls", "reality"):
        o["tls"] = _tls(n)
    net, path, host = p.get("type", "tcp"), unquote(p.get("path", "/")), p.get("host", "")
    if net == "ws":
        o["transport"] = {"type": "ws", "path": path, "headers": {"Host": host} if host else {}}
    elif net == "httpupgrade":
        o["transport"] = {"type": "httpupgrade", "path": path, "host": host}
    elif net == "grpc":
        o["transport"] = {"type": "grpc", "service_name": unquote(p.get("serviceName", ""))}
    return o


def build_config(nodes: list[Node], ports: dict[str, int], best_port: int, probe_url: str, bind: str = "127.0.0.1",
                 best_interval: str = "10m") -> dict:
    inbounds, outbounds, rules = [], [], []
    for n in nodes:
        inbounds.append({"type": "socks", "tag": f"in-{n.key}", "listen": bind, "listen_port": ports[n.key]})
        outbounds.append(build_outbound(n, f"out-{n.key}"))
        rules.append({"inbound": [f"in-{n.key}"], "outbound": f"out-{n.key}"})
    inbounds.append({"type": "socks", "tag": "in-best", "listen": bind, "listen_port": best_port})
    outbounds.append({"type": "urltest", "tag": "best", "outbounds": [f"out-{n.key}" for n in nodes],
                      "url": probe_url, "interval": best_interval, "tolerance": 100, "idle_timeout": "30m"})
    outbounds.append({"type": "direct", "tag": "direct"})
    rules.append({"inbound": ["in-best"], "outbound": "best"})
    return {"log": {"level": "warn"}, "inbounds": inbounds, "outbounds": outbounds,
            "route": {"rules": rules, "final": "direct"}}


class Hub:
    def __init__(self) -> None:
        e = os.environ.get
        self.sub_url = e("VPN_SUB_URL", "")
        self.nodes_file = e("VPN_NODES_FILE", "")
        self.base = int(e("HUB_PORT_BASE", "21000"))
        self.best = int(e("HUB_BEST_PORT", "10809"))
        self.test_iv = float(e("HUB_TEST_MINUTES", "10")) * 60
        self.sub_iv = float(e("HUB_SUB_HOURS", "6")) * 3600
        self.probe_url = e("HUB_PROBE_URL", "https://www.gstatic.com/generate_204")
        self.bind = e("HUB_BIND", "127.0.0.1")
        self.dir = Path(e("HUB_DATA", "/hub/data"))
        self.dir.mkdir(parents=True, exist_ok=True)
        self.nodes: list[Node] = []
        self.ports: dict[str, int] = {}
        self.proc: asyncio.subprocess.Process | None = None
        self.stop = asyncio.Event()

    # ---- subscription ----
    async def _download(self) -> str:
        if self.nodes_file:
            return Path(self.nodes_file).read_text()
        async with httpx.AsyncClient(timeout=20, trust_env=False) as c:
            r = await c.get(self.sub_url)
            r.raise_for_status()
            return r.text

    async def refresh(self) -> bool:
        """Re-read the subscription. True when the node set changed (sing-box was restarted)."""
        cache = self.dir / "nodes.cache"
        text = ""
        try:
            text = await self._download()
        except Exception as e:  # noqa: BLE001
            log.warning("subscription failed: %s", type(e).__name__)
        nodes = [n for n in parse_subscription(text) if supported(n)] if text else []
        if nodes:
            cache.write_text(text)
            os.chmod(cache, 0o600)
        elif not self.nodes and cache.exists():
            nodes = [n for n in parse_subscription(cache.read_text()) if supported(n)]
            log.warning("using cached subscription (%d nodes)", len(nodes))
        if not nodes:
            log.warning("no nodes; keeping previous set (%d)", len(self.nodes))
            return False
        if [n.key for n in nodes] == [n.key for n in self.nodes]:
            log.info("subscription refreshed: %d nodes, unchanged", len(nodes))
            return False
        old = self.ports or self._load_ports()
        self.ports = assign_ports([n.key for n in nodes], old, self.base, {self.best})
        self._save_ports()
        self.nodes = nodes
        log.info("subscription: %d nodes, restarting sing-box", len(nodes))
        await self._restart_singbox()
        return True

    def _load_ports(self) -> dict[str, int]:
        try:
            return {str(k): int(v) for k, v in json.loads((self.dir / "ports.json").read_text()).items()}
        except Exception:  # noqa: BLE001
            return {}

    def _save_ports(self) -> None:
        (self.dir / "ports.json").write_text(json.dumps(self.ports))

    # ---- sing-box ----
    async def _kill(self) -> None:
        if self.proc and self.proc.returncode is None:
            self.proc.terminate()
            try:
                await asyncio.wait_for(self.proc.wait(), 5)
            except asyncio.TimeoutError:
                self.proc.kill()

    async def _restart_singbox(self) -> None:
        await self._kill()
        cfg = self.dir / "singbox.json"
        cfg.write_text(json.dumps(build_config(self.nodes, self.ports, self.best, self.probe_url, self.bind)))
        os.chmod(cfg, 0o600)
        logf = open(self.dir / "singbox.log", "w")  # noqa: SIM115
        self.proc = await asyncio.create_subprocess_exec("sing-box", "run", "-c", str(cfg), stdout=logf, stderr=logf)
        await asyncio.sleep(1.5)
        if self.proc.returncode is not None:
            log.error("sing-box failed to start, see data/singbox.log")

    # ---- health ----
    async def _probe(self, n: Node, sem: asyncio.Semaphore) -> tuple[str, float | None]:
        async with sem:
            times: list[float] = []
            try:
                async with httpx.AsyncClient(proxy=f"socks5h://127.0.0.1:{self.ports[n.key]}", timeout=8,
                                             trust_env=False) as c:
                    for _ in range(3):
                        t = time.monotonic()
                        try:
                            r = await c.get(self.probe_url)
                            if r.status_code < 500:
                                times.append(time.monotonic() - t)
                        except Exception:  # noqa: BLE001
                            if not times:
                                break  # dead node: fail fast
            except Exception:  # noqa: BLE001
                pass
            return n.key, (statistics.median(times) if len(times) >= 2 else None)

    async def test(self) -> None:
        if not self.nodes:
            return
        sem = asyncio.Semaphore(64)
        res = dict(await asyncio.gather(*(self._probe(n, sem) for n in self.nodes)))
        now = int(time.time())
        out = {"updated": now, "best_port": self.best, "nodes": [
            {"key": n.key, "name": n.name, "port": self.ports[n.key], "alive": res[n.key] is not None,
             "latency_ms": int(res[n.key] * 1000) if res[n.key] is not None else None, "checked": now}
            for n in self.nodes]}
        tmp = self.dir / "nodes.json.tmp"
        tmp.write_text(json.dumps(out))
        os.replace(tmp, self.dir / "nodes.json")
        alive = sum(1 for v in res.values() if v is not None)
        log.info("test: %d/%d alive", alive, len(self.nodes))

    async def _sleep(self, sec: float) -> None:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=sec)
        except asyncio.TimeoutError:
            pass

    async def _beat(self) -> None:
        while not self.stop.is_set():
            (self.dir / "heartbeat").touch()
            await self._sleep(30)

    async def run(self) -> None:
        if not (self.sub_url or self.nodes_file):
            raise SystemExit("VPN_SUB_URL or VPN_NODES_FILE is required")
        beat = asyncio.create_task(self._beat())
        await self.refresh()
        await self.test()
        last_sub = last_test = time.monotonic()
        while not self.stop.is_set():
            await self._sleep(min(30.0, self.test_iv))
            if self.stop.is_set():
                break
            now = time.monotonic()
            try:
                if self.proc and self.proc.returncode is not None:
                    log.warning("sing-box exited (code %s), restarting", self.proc.returncode)
                    await self._restart_singbox()
                    last_test = 0.0
                if now - last_sub >= self.sub_iv:
                    last_sub = now
                    if await self.refresh():
                        last_test = 0.0
                if now - last_test >= self.test_iv:
                    last_test = now
                    await self.test()
            except Exception as e:  # noqa: BLE001
                log.warning("hub loop error: %s", type(e).__name__)
        beat.cancel()
        await self._kill()


async def amain() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    hub = Hub()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, hub.stop.set)
    asyncio.get_running_loop().add_signal_handler(signal.SIGINT, hub.stop.set)
    await hub.run()


if __name__ == "__main__":
    asyncio.run(amain())
