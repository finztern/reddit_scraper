"""NodePool (nodes mode): scan subscription nodes, one worker per good node, quarantine, rescan, autotune."""
from __future__ import annotations

import asyncio
import logging
import random
import re
import statistics
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from .autotune import AutoTuner, Window
from .http_client import NodeClient
from .limits import PageGate, TrafficBudget
from .scheduler import POPULAR_URL, TargetPicker

log = logging.getLogger("nodepool")

COMMENT_MARK = 'data-type="comment"'
_REGION = re.compile(r"\bp=([A-Za-z0-9_-]+)")


@dataclass
class ScanResult:
    ok: dict[str, bool] = field(default_factory=dict)
    codes: list[int] = field(default_factory=list)
    region: str | None = None
    profile: str | None = None

    @property
    def good(self) -> bool:
        return self.profile is not None


async def scan_node(client, url: str, profiles: list[str], *, rounds: int = 2, pause: float = 1.0,
                    round_pause: float = 4.0, stop_on_ok: bool = True, min_comments: int = 20,
                    hook: Callable[[int, str, int], Awaitable[None]] | None = None,
                    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> ScanResult:
    """One session, one request at a time; good = at least one 200 with >=min_comments comments."""
    r = ScanResult(ok={p: False for p in profiles})
    for rd in range(rounds):
        for i, prof in enumerate(profiles):
            client.set_profile(prof)
            try:
                status, text, nb = await client.fetch(url)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                status, text, nb = 0, "", 0
            r.codes.append(status)
            hdr = getattr(client, "last_headers", {}) or {}
            if r.region is None and hdr.get("server-timing"):
                m = _REGION.search(hdr["server-timing"])
                r.region = m.group(1) if m else None
            if hook:
                await hook(status, text, nb)
            if status == 200 and text.count(COMMENT_MARK) >= min_comments:
                r.ok[prof] = True
                if r.profile is None:
                    r.profile = prof
                if stop_on_ok:
                    return r
            if i < len(profiles) - 1:
                await sleep(pause)
        if rd < rounds - 1:
            await sleep(round_pause)
    return r


class NodeState:
    def __init__(self, key: str, name: str, port: int) -> None:
        self.key, self.name, self.port = key, name, port
        self.client = None
        self.profile: str | None = None
        self.task: asyncio.Task | None = None
        self.mult = 1.0          # per-node slowdown after 429, decays on success
        self.bad_streak = 0
        self.errs = 0
        self.warm = False        # session got at least one good page
        self.quarantined = False

    @property
    def active(self) -> bool:
        return self.task is not None and not self.task.done()


class NodePool:
    def __init__(self, s, vpn, metrics, on_page: Callable[..., Awaitable[int]], picker: TargetPicker,
                 tuner: AutoTuner, budget: TrafficBudget | None = None, gate: PageGate | None = None,
                 client_factory: Callable[[int, list[str]], object] | None = None) -> None:
        self.s, self.vpn, self.m, self.on_page, self.picker = s, vpn, metrics, on_page, picker
        self.tuner, self.budget, self.gate = tuner, budget, gate
        self._factory = client_factory or (lambda port, prof: NodeClient(port, prof, s.connect_timeout, s.read_timeout))
        self.nodes: dict[str, NodeState] = {}
        self.stop = asyncio.Event()
        self._rescan = asyncio.Event()
        self._sem = asyncio.Semaphore(max(1, s.node_scan_concurrency))
        self._last_scan = 0.0
        self._bg: list[asyncio.Task] = []
        metrics.gauges["nodes"] = lambda: f"{self.good_count()}/{len(self.nodes)}"
        metrics.gauges["gb_day"] = lambda: f"{self.budget.used_gb():.1f}" if self.budget else "0"
        metrics.gauges["iv"] = lambda: f"{self.median_interval():.1f}"
        metrics.gauges["pop"] = lambda: f"{self.tuner.share:.2f}"

    # ---- helpers (overridable in tests) ----
    async def _sleep(self, sec: float) -> None:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=sec)
        except asyncio.TimeoutError:
            pass

    def good_count(self) -> int:
        return sum(1 for n in self.nodes.values() if n.active and not n.quarantined)

    def _interval(self, n: NodeState) -> float:
        base = min(self.tuner.interval * n.mult, self.s.node_interval_max)
        return base * (self.budget.interval_factor() if self.budget else 1.0)

    def median_interval(self) -> float:
        iv = [self._interval(n) for n in self.nodes.values() if n.active]
        return statistics.median(iv) if iv else self.tuner.interval

    def nodes_changed(self) -> None:  # VpnManager.on_nodes_changed
        self._rescan.set()

    # ---- node set ----
    def sync_nodes(self) -> None:
        ports = {k: (name, port) for k, name, port in self.vpn.node_ports()}
        for k in list(self.nodes):
            n = self.nodes[k]
            if k not in ports or n.port != ports[k][1]:
                self._drop(self.nodes.pop(k))
        for k, (name, port) in ports.items():
            if k not in self.nodes:
                self.nodes[k] = NodeState(k, name, port)

    def _drop(self, n: NodeState) -> None:
        if n.task:
            n.task.cancel()
            n.task = None
        if n.client is not None:
            asyncio.get_running_loop().create_task(n.client.close())
            n.client = None

    # ---- scan ----
    async def _scan_hook(self, status: int, text: str, nb: int) -> None:
        self.m.inc("pages")
        self.m.inc("bytes", nb)
        if self.budget:
            self.budget.add(nb)
        if status == 403:
            self.m.inc("pages_cold403")
        elif status == 200 and COMMENT_MARK in text:
            await self.on_page(text, POPULAR_URL)

    async def _scan_one(self, n: NodeState) -> None:
        async with self._sem:
            if self.stop.is_set():
                return
            client = self._factory(n.port, self.s.node_profiles)
            res = await scan_node(client, POPULAR_URL, self.s.node_profiles, hook=self._scan_hook, sleep=self._sleep)
            if res.good:
                client.set_profile(res.profile)
                n.client, n.profile, n.warm = client, res.profile, True
                n.bad_streak = n.errs = 0
                n.task = asyncio.create_task(self._worker(n))
            else:
                await client.close()

    async def scan(self, nodes: list[NodeState]) -> None:
        self._last_scan = time.monotonic()
        await asyncio.gather(*(self._scan_one(n) for n in nodes), return_exceptions=True)
        log.info("nodepool: scan good=%d/%d", self.good_count(), len(self.nodes))

    # ---- worker ----
    async def _quarantine(self, n: NodeState) -> None:
        n.quarantined = True
        self.m.inc("node_quarantine")
        self.m.warn("quarantine", f"nodepool: node {n.name} quarantined {self.s.node_quarantine:.0f}s", 30)
        await self._sleep(self.s.node_quarantine)
        if self.stop.is_set():
            return
        await n.client.reset()
        n.bad_streak = n.errs = 0
        n.warm = False
        n.mult = 1.0
        n.quarantined = False

    async def _worker(self, n: NodeState) -> None:
        s, m = self.s, self.m
        await self._sleep(random.uniform(0, self._interval(n)))  # spread the start
        while not self.stop.is_set():
            if self.budget and self.budget.blocked():
                m.warn("gb_cap", "nodepool: MAX_GB_PER_DAY reached, paused", 60)
                await self._sleep(30)
                continue
            if self.gate:
                await self.gate.wait()
            t0 = time.monotonic()
            url = self.picker.pick_share(self.tuner.share)
            try:
                status, text, nb = await n.client.fetch(url)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                n.errs += 1
                m.inc("fetch_err")
                m.warn("node_exc", f"nodepool: fetch error {type(e).__name__}", 30)
                if n.errs >= 6:
                    n.errs = 0
                    await self._quarantine(n)
                    continue
                if n.errs == 3:
                    await n.client.reset()
                    n.warm = False
                await self._sleep(5)
                continue
            n.errs = 0
            m.inc("pages")
            m.inc("np_pages")
            m.inc("bytes", nb)
            if self.budget:
                self.budget.add(nb)
            if status == 200:
                try:
                    cnt = await self.on_page(text, url)
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # noqa: BLE001
                    m.warn("on_page", f"nodepool: on_page error {type(e).__name__}", 30)
                    cnt = 1
                if cnt == 0:  # soft-empty page: not an error
                    m.inc("pages_empty")
                    m.inc("np_empty")
                    await self._sleep(random.uniform(2, 5))
                    continue
                n.bad_streak = 0
                n.warm = True
                n.mult = max(1.0, n.mult * 0.97)
            elif status == 403:
                n.bad_streak += 1
                if n.warm:
                    m.inc("http_403")
                    m.inc("np_403w")
                else:
                    m.inc("pages_cold403")
                if n.bad_streak >= 4:
                    await self._quarantine(n)
                    continue
                if n.bad_streak == 2:
                    n.client.rotate_profile()  # same session, other TLS profile
                await self._sleep(2 + 4 * min(n.bad_streak - 1, 5))  # retry in the SAME session
                continue
            elif status == 429:
                m.inc("http_429")
                m.inc("np_429")
                m.warn("n429", f"nodepool: 429 on {n.name}, slowing node", 30)
                n.mult = min(n.mult * 1.5, s.node_interval_max / max(self.tuner.interval, 1e-6))
                await self._sleep(random.uniform(30, 60))
                continue
            else:
                m.inc("fetch_err")
                await self._sleep(5)
                continue
            gap = self._interval(n) * random.uniform(1 - s.jitter, 1 + s.jitter)
            await self._sleep(max(0.0, gap - (time.monotonic() - t0)))

    # ---- background loops ----
    async def _rescan_loop(self) -> None:
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self._rescan.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
            if self.stop.is_set():
                return
            age = time.monotonic() - self._last_scan
            good = self.good_count()
            if good == 0:
                self.m.warn("no_good", "nodepool: no good nodes, rescanning", 60)
            due = (self._rescan.is_set() or age >= self.s.node_rescan
                   or (good < self.s.node_min_good and age >= (60 if good == 0 else 120)))
            self._rescan.clear()
            if not due:
                continue
            self.sync_nodes()
            cand = [n for n in self.nodes.values() if not n.active]  # working workers are not touched
            if cand:
                await self.scan(cand)

    async def _tune_loop(self) -> None:
        keys = ("np_pages", "np_429", "np_403w", "np_empty", "unique", "pop_pages", "pop_cmt", "pop_dup", "pop_sat")
        snap = {k: self.m.c[k] for k in keys}
        t_prev = time.monotonic()
        while not self.stop.is_set():
            await self._sleep(self.s.node_tune_window)
            if self.stop.is_set():
                break
            cur = {k: self.m.c[k] for k in keys}
            now = time.monotonic()
            d = {k: cur[k] - snap[k] for k in keys}
            w = Window(dur=now - t_prev, pages=d["np_pages"], c429=d["np_429"], warm403=d["np_403w"],
                       empty=d["np_empty"], new=d["unique"], pop_pages=d["pop_pages"], pop_cmt=d["pop_cmt"],
                       pop_dup=d["pop_dup"], pop_sat=d["pop_sat"])
            snap, t_prev = cur, now
            act = self.tuner.step(w)
            self.tuner.step_share(w)
            if self.tuner.enabled:
                log.info("nodepool: tune %s iv=%.2fs pop=%.2f new/s=%.1f", act, self.tuner.interval, self.tuner.share, w.new_s)
            self.tuner.save()
            if self.budget:
                self.budget.save()
                if self.budget.max_bytes > 0 and self.budget.ratio() >= 0.9:
                    self.m.warn("gb90", f"nodepool: traffic {self.budget.used_gb():.1f} GB/day >= 90% of limit", 300)

    async def run(self) -> None:
        self.sync_nodes()
        await self.scan(list(self.nodes.values()))
        self._bg = [asyncio.create_task(self._tune_loop()), asyncio.create_task(self._rescan_loop())]
        await self.stop.wait()
        for t in self._bg:
            t.cancel()
        for n in self.nodes.values():
            if n.task:
                n.task.cancel()
        await asyncio.gather(*self._bg, *(n.task for n in self.nodes.values() if n.task), return_exceptions=True)
        for n in self.nodes.values():
            if n.client is not None:
                await n.client.close()
        self.tuner.save()
        if self.budget:
            self.budget.save()
