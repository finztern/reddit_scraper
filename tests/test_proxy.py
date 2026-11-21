import asyncio

import pytest

from reddit import config
from reddit.config import Settings
from reddit.http_client import NodeClient
from reddit.proxy_list import ProxyList, parse_proxies, parse_proxy


def test_parse_formats():
    a = parse_proxy("u:p@1.2.3.4:3129")
    assert a[1] == "1.2.3.4:3129" and a[2] == "http://u:p@1.2.3.4:3129" and "u:p" not in a[1]
    assert parse_proxy("1.2.3.4:3129:u:p")[2] == "http://u:p@1.2.3.4:3129"
    assert parse_proxy("socks5://1.2.3.4:1080")[2] == "socks5://1.2.3.4:1080"
    for bad in ("", "# c", "garbage", "host:notaport", "ftp://1.2.3.4:21"):
        assert parse_proxy(bad) is None
    assert len(parse_proxies("u:p@1.1.1.1:1\nu:p@1.1.1.1:1\n\n2.2.2.2:2\n")) == 2


def test_list_reload(tmp_path):
    f = tmp_path / "proxys.txt"
    f.write_text("u:p@1.1.1.1:1\n")
    pl = ProxyList(f)
    assert pl.load() and len(pl.node_ports()) == 1 and not pl.load()
    f.write_text("u:p@1.1.1.1:1\nu:p@2.2.2.2:2\n")
    assert pl.load() and len(pl.node_ports()) == 2


def test_client_accepts_url():
    async def go():
        c = NodeClient("http://u:p@1.1.1.1:1", ["chrome"])
        await c.close()
    asyncio.run(go())


def test_proxy_mode_config(tmp_path):
    s = Settings()
    s.source_mode = "proxy"
    s.proxy_file = str(tmp_path / "proxys.txt")
    with pytest.raises(config.ConfigError, match="PROXY_FILE"):
        config.check(s)
    (tmp_path / "proxys.txt").write_text("1.1.1.1:1\n")
    config.check(s)
