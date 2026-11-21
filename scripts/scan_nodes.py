"""Scan subscription nodes against Reddit: python scripts/scan_nodes.py  (needs VPN_SUB_URL, xray in PATH)."""
from __future__ import annotations

import asyncio

from reddit import config
from reddit.http_client import NodeClient
from reddit.node_pool import scan_node
from reddit.scheduler import POPULAR_URL
from reddit.vpn import VpnManager


async def main() -> None:
    s = config.load()
    if not s.vpn_sub_url:
        raise SystemExit("VPN_SUB_URL is not set")
    vpn = VpnManager(s.vpn_sub_url, s.data_dir, s.urlcheck_url, s.vpn_port_base)
    await vpn.start()
    nodes = vpn.node_ports()
    sem = asyncio.Semaphore(s.node_scan_concurrency)
    profiles = ["safari", "chrome"]

    async def one(i: int, name: str, port: int):
        async with sem:
            c = NodeClient(port, profiles, s.connect_timeout, s.read_timeout)
            try:
                return i, name, await scan_node(c, POPULAR_URL, profiles, stop_on_ok=False)
            finally:
                await c.close()

    res = await asyncio.gather(*(one(i, n, p) for i, (_, n, p) in enumerate(nodes)))
    print(f"{'#':>3} {'node':<28} {'region':<8} {'safari':<7} {'chrome':<7} codes")
    for i, name, r in res:
        print(f"{i:>3} {name[:28]:<28} {(r.region or '-'):<8} {'ok' if r.ok['safari'] else '-':<7} "
              f"{'ok' if r.ok['chrome'] else '-':<7} {','.join(map(str, r.codes))}")
    print(f"good={sum(r.good for _, _, r in res)}/{len(res)} "
          f"safari={sum(r.ok['safari'] for _, _, r in res)} chrome={sum(r.ok['chrome'] for _, _, r in res)}")
    await vpn.stop()


if __name__ == "__main__":
    asyncio.run(main())
