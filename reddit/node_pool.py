"""NodePool (nodes mode): scan subscription nodes, one worker per good node, quarantine, rescan, autotune."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import statistics
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

from .autotune import AutoTuner, Window
from .demand import NO_TICKET, Ticket
from .http_client import NodeClient
from .limits import PageGate, TrafficBudget
from .scheduler import POPULAR_URL, TargetPicker

log = logging.getLogger("nodepool")

Q_FACTORS = (1, 3, 12)  # quarantine escalation: 10 min, 30 min, 2 h (x NODE_QUARANTINE/600)
Q_RESET = 3600.0        # a node that stayed healthy this long starts again from level 0

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
        self.hist: deque[float] = deque()  # start times of the last requests (per-minute ceiling)

    @property
    def active(self) -> bool:
        return self.task is not None and not self.task.done()


class NodePool:
    def __init__(self, s, vpn, metrics, on_page: Callable[..., Awaitable[int]], picker: TargetPicker,
                 tuner: AutoTuner, budget: TrafficBudget | None = None, gate: PageGate | None = None, demand=None,
                 client_factory: Callable[[int, list[str]], object] | None = None,
                 state_path: str | Path | None = None) -> None:
        self.s, self.vpn, self.m, self.on_page, self.picker = s, vpn, metrics, on_page, picker
        self.tuner, self.budget, self.gate, self.demand = tuner, budget, gate, demand
        self._factory = client_factory or (lambda port, prof: NodeClient(port, prof, s.connect_timeout, s.read_timeout))
        self.nodes: dict[str, NodeState] = {}
        self._wall = time.time
        self._mono = time.monotonic
        self._state_path = Path(state_path) if state_path else None
        self._state: dict[str, dict] = self._load_state()
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

    def _cap_wait(self, n: NodeState) -> float:
        """Seconds to wait so that the node never exceeds NODE_MAX_PER_MIN requests in any 60 s window."""
        cap = self.s.node_max_per_min
        if cap <= 0:
            return 0.0
        now = self._mono()
        while n.hist and now - n.hist[0] >= 60.0:
            n.hist.popleft()
        return 60.0 - (now - n.hist[0]) if len(n.hist) >= cap else 0.0

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

    # ---- persistent node state (scan results + quarantine escalation survive restarts) ----
    def _load_state(self) -> dict[str, dict]:
        if not self._state_path or not self._state_path.exists():
            return {}
        try:
            d = json.loads(self._state_path.read_text())
            return {str(k): dict(v) for k, v in d.items()}
        except Exception:  # noqa: BLE001
            log.warning("nodes_state.json unreadable, ignoring")
            return {}

    def _save_state(self) -> None:
        if not self._state_path:
            return
        keep = {k: v for k, v in self._state.items() if k in self.nodes}
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(keep))
        os.replace(tmp, self._state_path)

    def _st(self, n: NodeState) -> dict:
        return self._state.setdefault(n.key, {})

    def _start(self, n: NodeState, profile: str | None, wait: float = 0.0, client=None) -> None:
        """Start a worker. Without `client` the session is cold (no scan was done): the first 403 is retried."""
        warm = client is not None
        if client is None:
            client = self._factory(n.port, self.s.node_profiles)
        if profile:
            client.set_profile(profile)
        n.client, n.profile, n.warm = client, profile, warm
        n.bad_streak = n.errs = 0
        n.task = asyncio.create_task(self._worker(n, wait))

    def _plan(self, cands: list[NodeState], bad_ttl: float) -> list[NodeState]:
        """Use remembered results: start known-good nodes (or resting ones) without a scan, skip recently
        known-bad ones. Returns nodes that really need a scan."""
        now, scan = self._wall(), []
        for n in cands:
            st = self._state.get(n.key)
            if not st or "checked" not in st:
                scan.append(n)
                continue
            age, q_left = now - st["checked"], st.get("q_until", 0.0) - now
            if q_left > 0:
                self._start(n, st.get("profile"), q_left)
            elif st.get("good") and age < self.s.node_state_ttl:
                self._start(n, st.get("profile"))
            elif not st.get("good") and age < bad_ttl:
                continue
            else:
                scan.append(n)
        return scan

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
            self._st(n).update(good=res.good, profile=res.profile, checked=self._wall())
            if res.good:
                self._start(n, res.profile, client=client)
            else:
                await client.close()

    async def scan(self, nodes: list[NodeState]) -> None:
        self._last_scan = time.monotonic()
        await asyncio.gather(*(self._scan_one(n) for n in nodes), return_exceptions=True)
        self._save_state()
        log.info("nodepool: scan good=%d/%d", self.good_count(), len(self.nodes))

    # ---- worker ----
    async def _quarantine(self, n: NodeState) -> None:
        st, now = self._st(n), self._wall()
        level = st.get("q_level", 0)
        if now - st.get("q_end", 0.0) > Q_RESET:
            level = 0
        dur = self.s.node_quarantine * Q_FACTORS[min(level, len(Q_FACTORS) - 1)]
        st.update(q_level=level + 1, q_until=now + dur)
        self._save_state()
        n.quarantined = True
        self.m.inc("node_quarantine")
        self.m.warn("quarantine", f"nodepool: node {n.name} quarantined {dur:.0f}s (level {level + 1})", 30)
        await self._sleep(dur)
        if self.stop.is_set():
            return
        await n.client.reset()
        n.bad_streak = n.errs = 0
        n.warm = False
        n.mult = 1.0
        n.quarantined = False
        st.update(q_until=0.0, q_end=self._wall())
        self._save_state()

    async def _worker(self, n: NodeState, wait: float = 0.0) -> None:
        if wait > 0:  # restored from state: still resting after a restart
            n.quarantined = True
            await self._sleep(wait)
            n.quarantined = False
            n.warm = False
            self._st(n).update(q_until=0.0, q_end=self._wall())
        await self._sleep(random.uniform(0, self._interval(n)))  # spread the start
        while not self.stop.is_set():
            if self.budget and self.budget.blocked():
                self.m.warn("gb_cap", "nodepool: MAX_GB_PER_DAY reached, paused", 60)
                await self._sleep(30)
                continue
            wait = self._cap_wait(n)  # per-node ceiling first: do not hold a demand ticket while waiting
            if wait > 0:
                await self._sleep(wait)
                continue
            if self.gate:
                await self.gate.wait()
            tk = await self.demand.acquire(self.stop) if self.demand else NO_TICKET  # fetch only when the buffer needs it
            if self.stop.is_set():
                tk.done()
                break
            try:
                await self._tick(n, tk)
            finally:
                tk.done()

    async def _tick(self, n: NodeState, tk: Ticket) -> None:
        s, m = self.s, self.m
        t0 = time.monotonic()
        url = self.picker.pick_share(self.tuner.share)
        n.hist.append(self._mono())
        try:
            status, text, nb = await n.client.fetch(url)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            tk.done()
            n.errs += 1
            m.inc("fetch_err")
            m.warn("node_exc", f"nodepool: fetch error {type(e).__name__}", 30)
            if n.errs >= 6:
                n.errs = 0
                await self._quarantine(n)
                return
            if n.errs == 3:
                await n.client.reset()
                n.warm = False
            await self._sleep(5)
            return
        n.errs = 0
        m.inc("pages")
        m.inc("np_pages")
        m.inc("bytes", nb)
        if self.budget:
            self.budget.add(nb)
        if status != 200:
            tk.done()
        if status == 200:
            try:
                cnt = await self.on_page(text, url)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                m.warn("on_page", f"nodepool: on_page error {type(e).__name__}", 30)
                cnt = 1
            tk.done()
            if cnt == 0:  # soft-empty page: not an error
                m.inc("pages_empty")
                m.inc("np_empty")
                await self._sleep(random.uniform(2, 5))
                return
            n.bad_streak = 0
            n.warm = True
            n.mult = max(1.0, n.mult * 0.97)
            self._st(n)["checked"] = self._wall()  # keeps a healthy node's record fresh
        elif status == 403:
            n.bad_streak += 1
            if n.warm:
                m.inc("http_403")
                m.inc("np_403w")
            else:
                m.inc("pages_cold403")
            if n.bad_streak >= 4:
                await self._quarantine(n)
                return
            # TLS profile stays constant per node (a fingerprint that flips mid-session looks unnatural)
            await self._sleep(2 + 4 * min(n.bad_streak - 1, 5))  # retry in the SAME session
            return
        elif status == 429:
            m.inc("http_429")
            m.inc("np_429")
            m.warn("n429", f"nodepool: 429 on {n.name}, slowing node", 30)
            n.mult = min(n.mult * 1.5, s.node_interval_max / max(self.tuner.interval, 1e-6))
            await self._sleep(random.uniform(30, 60))
            return
        else:
            m.inc("fetch_err")
            await self._sleep(5)
            return
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
            ttl = self.s.node_state_ttl if good >= self.s.node_min_good else 600.0  # emergency: recheck bad nodes sooner
            cand = self._plan([n for n in self.nodes.values() if not n.active], ttl)  # working workers are not touched
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
            self.tuner.probe = not (self.demand and self.demand.enabled())  # rate-limited: do not probe faster
            act = self.tuner.step(w)
            self.tuner.step_share(w)
            if self.tuner.enabled:
                log.info("nodepool: tune %s iv=%.2fs pop=%.2f new/s=%.1f", act, self.tuner.interval, self.tuner.share, w.new_s)
            self.tuner.save()
            self._save_state()
            if self.budget:
                self.budget.save()
                if self.budget.max_bytes > 0 and self.budget.ratio() >= 0.9:
                    self.m.warn("gb90", f"nodepool: traffic {self.budget.used_gb():.1f} GB/day >= 90% of limit", 300)

    async def run(self) -> None:
        self.sync_nodes()
        self._rescan.clear()  # startup xray restart already fired nodes_changed
        todo = self._plan(list(self.nodes.values()), self.s.node_state_ttl)
        log.info("nodepool: restored %d nodes from state, scanning %d", self.good_count() + sum(1 for n in self.nodes.values() if n.active and n.quarantined), len(todo))
        await self.scan(todo)
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
        self._save_state()
        if self.budget:
            self.budget.save()
