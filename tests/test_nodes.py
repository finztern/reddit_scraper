import asyncio
from pathlib import Path

import pytest

from reddit import config
from reddit.autotune import AutoTuner, Window
from reddit.config import Settings
from reddit.limits import TokenBucket, TrafficBudget
from reddit.metrics import Metrics
from reddit.node_pool import COMMENT_MARK, NodePool, scan_node
from reddit.pipeline import make_on_page
from reddit.scheduler import POPULAR_URL, TargetPicker
from reddit.seen_cache import SeenCache

FULL = COMMENT_MARK * 100
EMPTY = "x" * 100


class FakeClient:
    """Scripted responses: list of (status, text); the last one repeats."""

    def __init__(self, script):
        self.script, self.calls, self.resets, self.profile, self.rotations = list(script), 0, 0, "safari", 0
        self.last_headers = {}

    def set_profile(self, p):
        self.profile = p

    def rotate_profile(self):
        self.rotations += 1
        self.profile = "chrome" if self.profile == "safari" else "safari"

    async def fetch(self, url):
        r = self.script[min(self.calls, len(self.script) - 1)]
        self.calls += 1
        await asyncio.sleep(0)
        return r[0], r[1], len(r[1])

    async def reset(self):
        self.resets += 1

    async def close(self):
        pass


class FakeVpn:
    def __init__(self, ports):
        self.ports = ports

    def node_ports(self):
        return self.ports


def mk_pool(clients: dict[int, FakeClient]):
    s = Settings()
    s.node_profiles = ["safari", "chrome"]
    s.node_max_per_min = 0  # per-node ceiling has its own test
    m = Metrics()

    async def on_page(t, u=""):
        return t.count(COMMENT_MARK)

    tuner = AutoTuner(5, 1.5, 15, 0.5, enabled=False)
    pool = NodePool(s, FakeVpn([(f"k{p}", f"n{p}", p) for p in clients]), m, on_page, TargetPicker([], 3), tuner,
                    client_factory=lambda port, prof: clients[port])
    sleeps: list[float] = []

    async def fsleep(sec):
        sleeps.append(sec)
        await asyncio.sleep(0)

    pool._sleep = fsleep
    return pool, m, sleeps


async def run_pool(pool, cond, max_iter=5000):
    pool.sync_nodes()
    await pool.scan(list(pool.nodes.values()))
    for _ in range(max_iter):
        if cond():
            break
        await asyncio.sleep(0)
    pool.stop.set()
    await asyncio.gather(*(n.task for n in pool.nodes.values() if n.task), return_exceptions=True)


def test_scan_good_nodes_and_profile():
    async def go():
        a = FakeClient([(200, FULL)])
        b = FakeClient([(403, "blocked")])
        c = FakeClient([(403, "blocked"), (200, FULL)])  # cold 403, then ok
        pool, m, _ = mk_pool({1: a, 2: b, 3: c})
        await run_pool(pool, lambda: True)
        good = {n.name: n.profile for n in pool.nodes.values() if n.task}
        assert good == {"n1": "safari", "n3": "chrome"}
        assert m.c["pages_cold403"] >= 3
    asyncio.run(go())


def test_403_keeps_session_then_quarantine_and_return():
    async def go():
        script = [(200, FULL), (200, FULL)] + [(403, "x")] * 4 + [(200, FULL)]
        c = FakeClient(script)
        pool, m, sleeps = mk_pool({1: c})
        await run_pool(pool, lambda: c.calls >= 10)
        assert m.c["http_403"] == 4
        assert 600.0 in sleeps               # quarantine NODE_QUARANTINE
        assert c.resets == 1                 # only after quarantine, never on a plain 403
        assert {2, 6, 10} <= set(sleeps)     # same-session retry delays
        assert m.c["node_quarantine"] == 1
        assert pool.nodes["k1"].bad_streak == 0
        assert c.rotations == 0              # TLS profile never flips
    asyncio.run(go())


def test_soft_empty_page_is_not_an_error():
    async def go():
        c = FakeClient([(200, FULL), (200, FULL)] + [(200, EMPTY)] * 6 + [(200, FULL)])
        pool, m, _ = mk_pool({1: c})
        await run_pool(pool, lambda: c.calls >= 12)
        assert m.c["pages_empty"] == 6 and m.c["node_quarantine"] == 0 and c.resets == 0
    asyncio.run(go())


def test_429_slows_only_that_node():
    async def go():
        a = FakeClient([(200, FULL), (200, FULL), (429, "x"), (200, FULL)])
        b = FakeClient([(200, FULL)])
        pool, m, _ = mk_pool({1: a, 2: b})
        await run_pool(pool, lambda: a.calls >= 4)
        assert m.c["http_429"] == 1
        assert pool.nodes["k1"].mult > 1.0 >= pool.nodes["k2"].mult
    asyncio.run(go())


def test_target_picker():
    p = TargetPicker(["a", "b", "c"], 3)
    seq = [p.pick() for _ in range(8)]
    assert seq.count(POPULAR_URL) == 6 and len({u for u in seq if u != POPULAR_URL}) == 2  # round-robin by age
    q = TargetPicker(["a", "b"], 3)
    urls = [q.pick_share(0.5) for _ in range(100)]
    assert urls.count(POPULAR_URL) == 50
    assert TargetPicker([], 3).pick_share(0.1) == POPULAR_URL


def test_token_bucket_and_rate_cap_without_claim():
    t = [0.0]
    b = TokenBucket(2, burst=2, now=lambda: t[0])
    assert [b.take() for _ in range(3)] == [True, True, False]
    t[0] += 1.0
    assert b.take() and b.take() and not b.take()
    assert TokenBucket(0).take()

    sample = Path(__file__).parent / "samples" / "popular_0.html"
    s, m, seen = Settings(), Metrics(), SeenCache(1000)
    got = []

    class Snd:
        async def submit(self, item):
            got.append(item)

    on_page = make_on_page(s, m, seen, Snd(), "null", TokenBucket(5, burst=5, now=lambda: 0.0))
    asyncio.run(on_page(sample.read_text(encoding="utf-8"), POPULAR_URL))
    assert len(got) == 5 and m.filtered["rate_cap"] > 0
    assert sum(seen.has(i["external_id"]) for i in got) == 5
    assert len(seen) == 5  # rate-capped comments were NOT claimed


def test_traffic_budget_rolling_and_restart(tmp_path):
    t = [1_000_000.0]
    f = tmp_path / "traffic.json"
    b = TrafficBudget(1.0, f, now=lambda: t[0])
    b.add(500_000_000)
    assert b.interval_factor() == 1.0 and not b.blocked()
    b.add(450_000_000)
    assert b.interval_factor() == 2.0 and not b.blocked()
    b.add(100_000_000)
    assert b.blocked()
    b.save()
    b2 = TrafficBudget(1.0, f, now=lambda: t[0])           # restart: restored
    assert b2.blocked()
    t[0] += 86400 + 120                                      # window slides
    assert not b2.blocked() and b2.used_bytes() == 0
    assert not TrafficBudget(0, None).blocked()


def test_autotune_down_up_revert_and_persist(tmp_path):
    t = [1000.0]
    f = tmp_path / "nodepool.json"
    a = AutoTuner(5.0, 1.5, 15, 0.5, True, f, now=lambda: t[0])
    ok = Window(pages=100, new=600)                          # 10 new/s, no throttling
    assert a.step(ok) == "down" and a.interval == pytest.approx(4.5)
    assert a.step(Window(pages=100, new=720)) == "wait"
    assert a.step(Window(pages=100, new=720)) == "keep"      # +20% -> keep
    assert a.step(ok) == "down" and a.interval == pytest.approx(4.05)
    a.step(ok)
    assert a.step(ok) == "revert" and a.interval == pytest.approx(4.5) and a.fixed_until > t[0]
    assert a.step(ok) == "hold"                              # frozen 10 min
    assert a.step(Window(pages=100, c429=20, new=600)) == "up" and a.interval == pytest.approx(4.5 * 1.25)
    a.save()
    b = AutoTuner(5.0, 1.5, 15, 0.5, True, f, now=lambda: t[0])
    assert b.interval == pytest.approx(a.interval) and b.fixed_until == a.fixed_until
    assert AutoTuner(5.0, 1.5, 15, 0.5, False, f).interval == 5.0   # autotune off ignores the file


def test_autotune_share():
    a = AutoTuner(5, 1.5, 15, 0.5)
    assert a.step_share(Window(pop_pages=10, pop_cmt=1000, pop_dup=700, pop_sat=0)) == pytest.approx(0.45)
    assert a.step_share(Window(pop_pages=10, pop_cmt=1000, pop_dup=700, pop_sat=4)) == pytest.approx(0.45)  # saturated: hold
    assert a.step_share(Window(pop_pages=10, pop_cmt=1000, pop_dup=100, pop_sat=6)) == pytest.approx(0.5)


def test_nodes_mode_requires_sub_url():
    s = Settings()
    s.source_mode = "nodes"
    with pytest.raises(config.ConfigError, match="VPN_NODES_FILE"):
        config.check(s)
    s.vpn_sub_url = "https://example.com/sub"
    config.check(s)


def test_nodes_file(tmp_path):
    s = Settings()
    s.source_mode = "nodes"
    s.vpn_nodes_file = str(tmp_path / "nodes.txt")
    with pytest.raises(config.ConfigError, match="not found"):
        config.check(s)
    (tmp_path / "nodes.txt").write_text("vless://11111111-2222-3333-4444-555555555555@example.org:443?security=tls#n1\n")
    config.check(s)
    from reddit.vpn import VpnManager

    v = VpnManager("", str(tmp_path), "http://x", nodes_file=s.vpn_nodes_file)

    async def go():
        v._dir.mkdir(parents=True, exist_ok=True)
        async def noop():
            pass
        v._restart_xray = noop
        await v._refresh_subscription()
    asyncio.run(go())
    assert [n[1] for n in v.node_ports()] == ["n1"]


def test_quarantine_escalates():
    async def go():
        c = FakeClient([(200, FULL), (200, FULL)] + [(403, "x")])    # then 403 forever
        pool, m, sleeps = mk_pool({1: c})
        await run_pool(pool, lambda: len([x for x in sleeps if x >= 600]) >= 4)
        long = [x for x in sleeps if x >= 600]
        assert long[:4] == [600, 1800, 7200, 7200]                  # level caps at x12
        assert pool._state["k1"]["q_level"] >= 3
    asyncio.run(go())


def test_state_survives_restart(tmp_path):
    async def go():
        sp = tmp_path / "nodes_state.json"
        a1, b1 = FakeClient([(200, FULL)]), FakeClient([(403, "x")])
        pool, _, _ = mk_pool({1: a1, 2: b1})
        pool._state_path = sp
        await run_pool(pool, lambda: True)
        pool._save_state()
        assert sp.exists()

        a2, b2 = FakeClient([(200, FULL)]), FakeClient([(403, "x")])
        pool2, _, _ = mk_pool({1: a2, 2: b2})
        pool2._state_path = sp
        pool2._state = pool2._load_state()
        pool2.sync_nodes()
        todo = pool2._plan(list(pool2.nodes.values()), 21600)
        assert todo == []                                            # nothing to scan: A restored, B known bad
        await asyncio.sleep(0.05)
        pool2.stop.set()
        await asyncio.gather(*(n.task for n in pool2.nodes.values() if n.task), return_exceptions=True)
        assert a2.calls > 0 and b2.calls == 0                        # known-bad node was not touched
        assert pool2.nodes["k1"].task is not None and pool2.nodes["k2"].task is None
    asyncio.run(go())


def test_restored_quarantine_is_respected(tmp_path):
    async def go():
        c = FakeClient([(200, FULL)])
        pool, _, sleeps = mk_pool({1: c})
        pool.sync_nodes()
        now = pool._wall()
        pool._state = {"k1": {"good": True, "profile": "safari", "checked": now, "q_until": now + 300, "q_level": 1}}
        assert pool._plan(list(pool.nodes.values()), 21600) == []
        await asyncio.sleep(0.05)
        pool.stop.set()
        await asyncio.gather(*(n.task for n in pool.nodes.values() if n.task), return_exceptions=True)
        assert any(290 <= x <= 300 for x in sleeps)                  # rests the remaining time first
    asyncio.run(go())


def test_per_node_ceiling_per_minute():
    async def go():
        clock = [1000.0]
        times: list[float] = []

        class C(FakeClient):
            async def fetch(self, url):
                times.append(clock[0])
                return await super().fetch(url)

        c = C([(200, FULL)])
        pool, m, _ = mk_pool({1: c})
        pool.s.node_max_per_min = 3
        pool._mono = lambda: clock[0]

        async def fs(sec):
            clock[0] += sec
            await asyncio.sleep(0)

        pool._sleep = fs
        await run_pool(pool, lambda: len(times) >= 25)
        worker = times[1:]                                       # first call is the scan
        for i, t0 in enumerate(worker):
            assert sum(1 for t in worker[i:] if t - t0 < 60) <= 3  # never >3 requests in 60 s
    asyncio.run(go())


def test_interval_recovers_when_demand_limited():
    a = AutoTuner(5.0, 5.0, 15, 0.5)
    a.probe = False
    assert a.step(Window(pages=100, c429=20)) == "up" and a.interval == pytest.approx(6.25)
    assert a.step(Window(pages=100)) == "recover" and a.interval == pytest.approx(5.625)
    assert a.step(Window(pages=100)) == "recover" and a.interval == pytest.approx(5.0625)
    assert a.step(Window(pages=100)) == "recover" and a.interval == pytest.approx(5.0)
    assert a.step(Window(pages=100)) == "hold"                   # never below the base


def test_picker_prefers_active_subs():
    import reddit.scheduler as sch
    t = [1000.0]
    orig = sch.time.monotonic
    sch.time.monotonic = lambda: t[0]
    try:
        p = TargetPicker(["hot", "mid", "dead"], 3)
        for _ in range(3):                       # probe all once
            p.pick_share(0.0)
        counts = {"hot": 0, "mid": 0, "dead": 0}
        for _ in range(300):
            t[0] += 2.0
            url = p.pick_share(0.0)
            s = url.split("/r/")[1].split("/")[0]
            counts[s] += 1
            p.note(url, {"hot": 60, "mid": 10, "dead": 0}[s])
        assert counts["hot"] > counts["mid"] > counts["dead"]
    finally:
        sch.time.monotonic = orig
