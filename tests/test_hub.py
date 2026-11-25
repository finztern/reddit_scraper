import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hub"))
from hub import assign_ports, build_config, supported  # noqa: E402

from reddit.vpn import parse_subscription
from reddit.vpn_external import ExternalVpn

U = ("vless://11111111-2222-3333-4444-555555555555@example.org:443?security=tls&type=ws&path=%2Fa&host=h.example.org"
     "&sni=h.example.org#{n}")


def test_ports_are_stable():
    p1 = assign_ports(["a", "b", "c"], {}, 21000, {21001})
    assert p1 == {"a": 21000, "b": 21002, "c": 21003}
    p2 = assign_ports(["a", "c", "d"], p1, 21000, {21001})   # b removed, d added
    assert p2["a"] == 21000 and p2["c"] == 21003 and p2["d"] == 21002


HY = "hysteria2://pass%40word@hy.example.org:8443?sni=hy.example.org&insecure=1&obfs=salamander&obfs-password=ob#hy-1"


def test_config_singbox_best_urltest_and_hy2():
    nodes = parse_subscription("\n".join([U.format(n="n0"), HY]))
    assert {n.scheme for n in nodes} == {"vless", "hysteria2"}
    cfg = build_config(nodes, {n.key: 21000 + i for i, n in enumerate(nodes)}, 10809, "https://x/204")
    assert {i["listen_port"] for i in cfg["inbounds"]} == {21000, 21001, 10809}
    by = {o["type"]: o for o in cfg["outbounds"]}
    assert by["urltest"]["tag"] == "best" and len(by["urltest"]["outbounds"]) == 2
    hy = by["hysteria2"]
    assert hy["password"] == "pass@word" and hy["tls"]["insecure"] is True and hy["obfs"]["password"] == "ob"
    assert by["vless"]["transport"]["type"] == "ws" and by["vless"]["tls"]["server_name"] == "h.example.org"
    assert any(r.get("outbound") == "best" for r in cfg["route"]["rules"])
    json.dumps(cfg)


def test_xhttp_is_skipped():
    n = parse_subscription(U.format(n="x").replace("type=ws", "type=xhttp"))[0]
    assert not supported(n)


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
