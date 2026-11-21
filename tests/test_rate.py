import asyncio
import os
import time

from reddit.config import Settings
from reddit.demand import DemandGate
from reddit.metrics import Metrics
from reddit.runtime import RuntimeConfig
from reddit.seen_cache import SeenCache
from reddit.sender import Sender


def test_runtime_hot_reload(tmp_path):
    f = tmp_path / "runtime.yaml"
    f.write_text("target_rate: 50\nbuffer_seconds: 5\n")
    rt = RuntimeConfig(f, 0, 5)
    assert rt.target_rate == 50
    f.write_text("target_rate: 80\n")
    os.utime(f, ns=(time.time_ns() + 10**9, time.time_ns() + 10**9))
    assert rt.reload() and rt.target_rate == 80
    f.write_text("target_rate: oops\n")
    os.utime(f, ns=(time.time_ns() + 2 * 10**9, time.time_ns() + 2 * 10**9))
    assert not rt.reload() and rt.target_rate == 80     # invalid file: keep the old value
    assert RuntimeConfig(tmp_path / "none.yaml", 7, 5).target_rate == 7


class FakeSender:
    keep = 1.0

    def __init__(self):
        self.p = 0

    def pending(self):
        return self.p


def test_demand_gate_blocks_when_buffer_full():
    async def go():
        sd = FakeSender()
        g = DemandGate(sd, lambda: 50.0, lambda: 5.0)       # high = 250
        g.avg_new = 50.0
        tks = [await g.acquire() for _ in range(5)]          # admitted while 4*50=200 < 250
        assert g.inflight == 5
        try:
            await asyncio.wait_for(g.acquire(), 0.5)        # 5*50=250 >= 250 -> blocked
            raise AssertionError("must block")
        except asyncio.TimeoutError:
            pass
        tks[0].done(); tks[0].done()                         # idempotent
        assert g.inflight == 4
        assert (await asyncio.wait_for(g.acquire(), 1)) is not None
        sd.p = 10_000                                        # buffer full: blocked even with no inflight
        g.inflight = 0
        try:
            await asyncio.wait_for(g.acquire(), 0.5)
            raise AssertionError("must block")
        except asyncio.TimeoutError:
            pass
        off = DemandGate(sd, lambda: 0.0, lambda: 5.0)       # rate 0: gate disabled
        await asyncio.wait_for(off.acquire(), 0.5)
    asyncio.run(go())


def test_sender_paces_output_evenly(tmp_path):
    async def go():
        s = Settings()
        s.data_dir = str(tmp_path)
        rate = [200.0]
        snd = Sender(s, SeenCache(1000), Metrics(), None, rate_fn=lambda: rate[0])
        snd._tick = 0.05
        for i in range(400):
            snd._ready.put_nowait({"external_id": f"t1_{i}"})
        task = asyncio.create_task(snd._batch_loop())
        t0 = time.monotonic()
        sizes = []
        while time.monotonic() - t0 < 0.6:
            try:
                sizes.append(len(await asyncio.wait_for(snd._batches.get(), 0.1)))
            except asyncio.TimeoutError:
                pass
        total = sum(sizes)
        assert 0.8 * 120 <= total <= 1.2 * 120 + 20           # ~200/s * 0.6 s
        assert max(sizes) <= 30                                # even slices, no burst of everything
        rate[0] = 20.0                                         # live change down
        await asyncio.sleep(0.15)
        while not snd._batches.empty():
            snd._batches.get_nowait()
        before = snd._ready.qsize()
        await asyncio.sleep(0.5)
        assert before - snd._ready.qsize() <= 20 * 0.5 + 6
        task.cancel()
        await snd._client.aclose()
    asyncio.run(go())


def test_demand_gate_admits_in_arrival_order():
    async def go():
        sd = FakeSender()
        sd.p = 10_000                                        # buffer full: everybody waits
        g = DemandGate(sd, lambda: 50.0, lambda: 5.0)
        g.avg_new = 250.0                                    # one in-flight request fills the demand
        order, tickets = [], []

        async def worker(name):
            tk = await g.acquire()
            order.append(name)
            tickets.append(tk)

        tasks = []
        for name in "abcd":
            tasks.append(asyncio.create_task(worker(name)))
            await asyncio.sleep(0.01)                        # arrival order a, b, c, d
        sd.p = 0
        for _ in range(4):
            await asyncio.sleep(0.3)
            tickets[-1].done()
        await asyncio.gather(*tasks)
        assert order == list("abcd")
    asyncio.run(go())
