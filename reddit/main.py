"""Pipeline assembly: scheduler -> parse/filter/map -> SeenCache claim -> Sender (url-check -> collector)."""
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from . import config, filters, mapper, parser
from .http_client import HttpClient
from .metrics import Metrics
from .scheduler import Scheduler
from .seen_cache import SeenCache
from .sender import Sender
from .url_check import UrlChecker
from .vpn import VpnManager

log = logging.getLogger("app")


async def amain() -> None:
    s = config.load()
    logging.basicConfig(level=s.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    mode = mapper.validate_parent_mode(s.parent_mode)
    log.info("PARENT_MODE=%s urlcheck_policy=%s endpoints=%d", mode, s.urlcheck_policy, len(s.store_endpoints))
    Path(s.data_dir).mkdir(parents=True, exist_ok=True)

    m = Metrics()
    seen = SeenCache(s.seen_max)
    vpn: VpnManager | None = None
    if s.urlcheck_proxy:
        proxy_getter = lambda: s.urlcheck_proxy  # noqa: E731
        on_fail = None
        log.info("url-check via fixed proxy")
    elif s.vpn_sub_url:
        vpn = VpnManager(s.vpn_sub_url, s.data_dir, s.urlcheck_url, s.vpn_port_base,
                         s.vpn_test_interval, s.vpn_sub_refresh, s.vpn_switch_gain)
        proxy_getter, on_fail = vpn.proxy, vpn.report_failure
    else:
        proxy_getter, on_fail = (lambda: None), None
        log.warning("no URLCHECK_PROXY / VPN_SUB_URL: url-check goes direct")
    checker = UrlChecker(s.urlcheck_url, proxy_getter, s.urlcheck_policy, on_fail,
                         direct=not (s.urlcheck_proxy or s.vpn_sub_url))
    sender = Sender(s, seen, m, checker)

    async def on_page(text: str) -> int:
        raws = parser.parse(text)
        m.inc("comments", len(raws))
        collected = mapper.utc_now_str()
        for raw in raws:
            content = mapper.build_content(raw.body_html)
            reason = filters.reject_reason(raw.author, content, s.min_text_len)
            if reason:
                m.drop(reason)
                continue
            if not seen.claim(raw.fullname):
                m.drop("dup")
                continue
            m.inc("unique")
            await sender.submit(mapper.to_item(raw, content, collected, mode))
        return len(raws)

    http = HttpClient(s.connect_timeout, s.read_timeout)
    sched = Scheduler(s, http, m, on_page)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, sched.stop.set)

    if vpn:
        await vpn.start()
    sender.start()
    hb = Path(s.data_dir) / "heartbeat"

    async def ticker() -> None:
        while True:
            hb.touch()
            log.info(m.line())
            await asyncio.sleep(s.metrics_interval)

    tasks = [asyncio.create_task(ticker())]
    if vpn:
        tasks.append(asyncio.create_task(vpn.run()))
    try:
        await sched.run()
    finally:
        for t in tasks:
            t.cancel()
        await sender.close()
        await checker.close()
        await http.close()
        if vpn:
            await vpn.stop()
        log.info("stopped")


if __name__ == "__main__":
    asyncio.run(amain())
