import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hub"))
from hub import assign_ports, build_config  # noqa: E402

from reddit.vpn import parse_subscription
from reddit.vpn_external import ExternalVpn

U = ("vless://11111111-2222-3333-4444-555555555555@example.org:443?security=tls&type=ws&path=%2Fa&host=h.example.org"
     "&sni=h.example.org#{n}")


def test_ports_are_stable():
    p1 = assign_ports(["a", "b", "c"], {}, 21000, {21001})
    assert p1 == {"a": 21000, "b": 21002, "c": 21003}
    p2 = assign_ports(["a", "c", "d"], p1, 21000, {21001})   # b removed, d added
    assert p2["a"] == 21000 and p2["c"] == 21003 and p2["d"] == 21002


def test_config_has_best_balancer():
    nodes = parse_subscription("\n".join(U.format(n=f"n{i}").replace("11111111", f"{i}" * 8) for i in range(2)))
    cfg = build_config(nodes, {n.key: 21000 + i for i, n in enumerate(nodes)}, 10809, "https://x/204")
    assert {i["port"] for i in cfg["inbounds"]} == {21000, 21001, 10809}
    assert cfg["routing"]["balancers"][0]["strategy"]["type"] == "leastPing"
    assert any(r.get("balancerTag") == "best" for r in cfg["routing"]["rules"])
    json.dumps(cfg)


def test_external_only_alive_and_fastest(tmp_path):
    f = tmp_path / "nodes.json"
    f.write_text(json.dumps({"updated": time.time(), "nodes": [
        {"key": "a", "name": "A", "port": 21000, "alive": True, "latency_ms": 300},
        {"key": "b", "name": "B", "port": 21001, "alive": False, "latency_ms": None},
        {"key": "c", "name": "C", "port": 21002, "alive": True, "latency_ms": 100}]}))
    v = ExternalVpn(str(f))
    assert v.load()
    assert [n[0] for n in v.node_ports()] == ["a", "c"]
    assert v.proxy() == "socks5h://127.0.0.1:21002" and len(v.proxies(2)) == 2
    assert not v.load()                                   # unchanged file
    d = json.loads(f.read_text())
    d["nodes"][1].update(alive=True, latency_ms=50)
    f.write_text(json.dumps(d))
    import os
    os.utime(f, ns=(time.time_ns() + 10**9,) * 2)
    assert v.load() and v.proxy() == "socks5h://127.0.0.1:21001"
