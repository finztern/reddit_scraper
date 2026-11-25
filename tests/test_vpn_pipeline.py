import asyncio

from reddit import config
from reddit.config import Settings
from reddit.vpn_pipeline import PipelineVpn, parse_nodes

D = {"nodes": [
    {"tag": "vpn-0", "name": "A", "port": 10800, "ok": True, "latency_ms": 300.0},
    {"tag": "vpn-1", "name": "B", "port": 10801, "ok": False, "latency_ms": None},
    {"tag": "vpn-2", "name": "C", "port": 10802, "ok": True, "latency_ms": 100.0},
    {"tag": "vpn-3", "name": "D", "port": None, "ok": True}]}


def test_parse_and_fastest():
    assert [n["key"] for n in parse_nodes(D)] == ["vpn-0", "vpn-2"]
    assert parse_nodes({}) == []
    v = PipelineVpn("http://x")
    v._nodes = parse_nodes(D)
    assert v.proxy() == "socks5h://127.0.0.1:10802" and len(v.proxies(2)) == 2
    assert [p[2] for p in v.node_ports()] == [10800, 10802]


def test_load_change_detect_and_keep_on_error(monkeypatch):
    import httpx
    state = {"d": D, "fail": False}

    class R:
        def raise_for_status(self): pass
        def json(self): return state["d"]

    class C:
        def __init__(self, **k): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *a): pass
        async def get(self, url):
            if state["fail"]:
                raise httpx.ConnectError("x")
            return R()

    monkeypatch.setattr(httpx, "AsyncClient", C)

    async def go():
        v = PipelineVpn("http://x")
        assert await v.load() and not await v.load()
        state["fail"] = True
        assert not await v.load() and len(v.node_ports()) == 2     # error: previous set kept
        state["fail"] = False
        state["d"] = {"nodes": [{"tag": "vpn-9", "port": 10809, "ok": True, "latency_ms": 1}]}
        assert await v.load() and [p[0] for p in v.node_ports()] == ["vpn-9"]
    asyncio.run(go())


def test_config_pipeline_mode():
    s = Settings()
    s.source_mode, s.vpn_mode = "nodes", "pipeline"
    config.check(s)
