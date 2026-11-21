"""Sender: url-check stage -> batching (BATCH_MIN..MAX, FLUSH_SECONDS) -> POST to collector, retry, spool."""
from __future__ import annotations

import asyncio
import itertools
import json
import logging
import time
from pathlib import Path

import httpx

from .metrics import Metrics
from .seen_cache import SeenCache
from .url_check import UrlChecker

log = logging.getLogger("sender")


class Sender:
    def __init__(self, s, seen: SeenCache, metrics: Metrics, checker: UrlChecker | None) -> None:
        self.s = s
        self.seen = seen
        self.m = metrics
        self.checker = checker
        self._in: asyncio.Queue[dict] = asyncio.Queue(maxsize=s.queue_max)
        self._ready: asyncio.Queue[dict] = asyncio.Queue(maxsize=s.queue_max)
        self._cbatches: asyncio.Queue[list[dict]] = asyncio.Queue(maxsize=max(1, s.urlcheck_concurrency))
        self._batches: asyncio.Queue[list[dict]] = asyncio.Queue(maxsize=max(2, s.store_concurrency * 2))
        self._rr = itertools.cycle(range(len(s.store_endpoints)))
        self._client = httpx.AsyncClient(timeout=20, trust_env=False)
        self._spool = Path(s.data_dir) / "spool"
        self._spool.mkdir(parents=True, exist_ok=True)
        self._seq = 0
        self._fails = 0
        self._open_until = 0.0
        self._tasks: list[asyncio.Task] = []
        self._stopping = False
        metrics.gauges["queue"] = lambda: self._in.qsize() + self._ready.qsize() + (self._batches.qsize() + self._cbatches.qsize()) * s.batch_max

    async def submit(self, item: dict) -> None:
        await self._in.put(item)  # blocks when full -> natural backpressure on the scheduler

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._check_batcher()), asyncio.create_task(self._batch_loop()),
                       asyncio.create_task(self._replay_loop())]
        self._tasks += [asyncio.create_task(self._check_worker()) for _ in range(max(1, self.s.urlcheck_concurrency))]
        self._tasks += [asyncio.create_task(self._send_worker()) for _ in range(self.s.store_concurrency)]

    # ---- helpers ----
    async def _take(self, q: asyncio.Queue, maxn: int, idle: float) -> list[dict]:
        out: list[dict] = []
        try:
            out.append(await asyncio.wait_for(q.get(), timeout=idle))
        except asyncio.TimeoutError:
            return out
        while len(out) < maxn:
            try:
                out.append(await asyncio.wait_for(q.get(), timeout=idle))
            except asyncio.TimeoutError:
                break
        return out

    def _release(self, items: list[dict]) -> None:
        for i in items:
            self.seen.release(i["external_id"])

    # ---- stages ----
    async def _check_batcher(self) -> None:
        while True:
            items = await self._take(self._in, self.s.urlcheck_batch, self.s.urlcheck_flush)
            if items:
                await self._cbatches.put(items)

    async def _check_worker(self) -> None:
        while True:
            items = await self._cbatches.get()
            try:
                await self._check(items)
            except asyncio.CancelledError:
                self._release(items)  # unchecked: forget claims, items can be re-collected later
                raise

    async def _check(self, items: list[dict]) -> None:
        if self.checker is None:
            fresh = items
        else:
            self.m.inc("uc_in", len(items))
            delay = 2.0
            while True:
                fresh = await self.checker.unknown(items)
                if fresh is not None:
                    break
                self.m.inc("uc_err")
                if self._stopping:
                    self._release(items)
                    return
                await asyncio.sleep(delay)  # policy=closed: never send unchecked, retry
                delay = min(delay * 2, 30.0)
            fresh_ids = {i["external_id"] for i in fresh}
            known = [i for i in items if i["external_id"] not in fresh_ids]
            self.m.inc("uc_known", len(known))
            for i in known:
                self.seen.confirm(i["external_id"])
        for i in fresh:
            await self._ready.put(i)

    async def _batch_loop(self) -> None:
        while True:
            batch = await self._take(self._ready, self.s.batch_max, self.s.flush_seconds)
            if batch:
                await self._batches.put(batch)

    async def _send_worker(self) -> None:
        while True:
            batch = await self._batches.get()
            try:
                await self._deliver(batch)
            except asyncio.CancelledError:
                self._to_spool(batch)  # shutdown mid-delivery: keep the batch
                raise

    async def _post(self, batch: list[dict]) -> bool:
        for attempt in range(3):
            ep = self.s.store_endpoints[next(self._rr)]
            try:
                r = await self._client.post(ep, json={"items": batch})
                if 200 <= r.status_code < 300:
                    try:
                        j = r.json()
                        res = j.get("results") or {}
                        log.debug("collector: count=%s to_gpu=%s dropped=%s", j.get("count"),
                                  res.get("to_gpu"), res.get("dropped"))
                    except Exception:  # noqa: BLE001
                        pass
                    return True
                self.m.warn("store_http", f"collector HTTP {r.status_code}")
            except Exception as e:  # noqa: BLE001
                self.m.warn("store_exc", f"collector error: {type(e).__name__}")
            self.m.inc("store_err")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
        return False

    async def _deliver(self, batch: list[dict]) -> None:
        if time.monotonic() < self._open_until:  # circuit open: don't hammer a dead collector
            self._to_spool(batch)
            return
        if await self._post(batch):
            self._fails = 0
            self.m.inc("sent", len(batch))
            for i in batch:
                self.seen.confirm(i["external_id"])
            return
        self._fails += 1
        if self._fails >= 3:
            self._open_until = time.monotonic() + 30
            self.m.warn("breaker", "collector down: circuit open 30s, spooling to disk", 30)
        self._to_spool(batch)

    # ---- spool ----
    def _to_spool(self, batch: list[dict]) -> None:
        files = sorted(self._spool.glob("*.jsonl"))
        if len(files) >= self.s.spool_max_files:
            self.m.warn("spool_full", "spool full: dropping batch", 60)
            self._release(batch)
            return
        self._seq += 1
        p = self._spool / f"{int(time.time()*1000)}-{self._seq}.jsonl"
        tmp = p.with_suffix(".tmp")
        tmp.write_text("\n".join(json.dumps(i, ensure_ascii=False) for i in batch))
        tmp.rename(p)
        self.m.inc("spooled", len(batch))

    async def _replay_loop(self) -> None:
        while True:
            await asyncio.sleep(15)
            if time.monotonic() < self._open_until:
                continue
            for p in sorted(self._spool.glob("*.jsonl"))[:20]:
                try:
                    batch = [json.loads(x) for x in p.read_text().splitlines() if x.strip()]
                except Exception:  # noqa: BLE001
                    p.unlink(missing_ok=True)
                    continue
                if not await self._post(batch):
                    self._fails += 1
                    break
                self._fails = 0
                self.m.inc("sent", len(batch))
                for i in batch:
                    self.seen.confirm(i["external_id"])
                p.unlink(missing_ok=True)

    async def close(self) -> None:
        """Flush what is still queued to the spool (nothing is lost on shutdown)."""
        self._stopping = True
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        left: list[dict] = []
        while not self._cbatches.empty():
            self._release(self._cbatches.get_nowait())  # not url-checked yet: drop claims, never spool unchecked
        for q in (self._in,):
            while not q.empty():
                self._release([q.get_nowait()])
        while not self._ready.empty():
            left.append(self._ready.get_nowait())
        while not self._batches.empty():
            left += self._batches.get_nowait()
        for k in range(0, len(left), self.s.batch_max):
            self._to_spool(left[k:k + self.s.batch_max])
        await self._client.aclose()
