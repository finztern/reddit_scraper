import base64
import json

from reddit.vpn import build_xray_config, parse_subscription

URI = ("vless://11111111-2222-3333-4444-555555555555@example.org:443?security=tls&type=httpupgrade&headerType="
       "&path=%2Fde2%2F&host=h.example.org&sni=h.example.org&fp=chrome#de2.trust.zone")
URI2 = URI.replace("de2", "nl1").replace("11111111", "99999999")


def test_parse_plain_and_base64():
    for text in (URI + "\n" + URI2 + "\n", base64.b64encode((URI + "\n" + URI2).encode()).decode()):
        nodes = parse_subscription(text)
        assert len(nodes) == 2
        assert {n.name for n in nodes} == {"de2.trust.zone", "nl1.trust.zone"}
    assert parse_subscription("vmess://xxx\ngarbage") == []


def test_config():
    nodes = parse_subscription(URI + "\n" + URI2)
    cfg = build_xray_config(nodes, 21000)
    assert [i["port"] for i in cfg["inbounds"]] == [21000, 21001]
    st = cfg["outbounds"][0]["streamSettings"]
    assert st["network"] == "httpupgrade" and st["httpupgradeSettings"]["path"] == "/de2/"
    assert st["tlsSettings"]["serverName"] == "h.example.org"
    json.dumps(cfg)
