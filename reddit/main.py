"""Pipeline assembly: scheduler -> parse/filter/map -> SeenCache claim -> Sender (url-check -> collector)."""
from __future__ import annotations

import asyncio
import logging
import signal
from pathlib import Path

from . import config, mapper
from .autotune import AutoTuner
from .http_client import HttpClient
from .limits import PageGate, TokenBucket, TrafficBudget
from .metrics import Metrics
from .dashboard import Sampler, serve as serve_dashboard
from .demand import DemandGate
from .node_pool import NodePool
from .pipeline import make_on_page
from .proxy_list import ProxyList
from .runtime import RuntimeConfig
from .scheduler import Scheduler, TargetPicker
from .seen_cache import SeenCache
from .sender import Sender
from .url_check import UrlChecker
from .vpn import VpnManager
from .vpn_external import ExternalVpn
from .vpn_pipeline import PipelineVpn

log = logging.getLogger("app")


async def amain() -> None:
    s = config.load()
    logging.basicConfig(level=s.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    try:
        config.check(s)
    except config.ConfigError as e:
        log.error("config error: %s", e)
        raise SystemExit(2) from e
    mode = mapper.validate_parent_mode(s.parent_mode)
    log.info("SOURCE_MODE=%s PARENT_MODE=%s urlcheck_policy=%s endpoints=%d", s.source_mode, mode,
             s.urlcheck_policy, len(s.store_endpoints))
    data = Path(s.data_dir)
    data.mkdir(parents=True, exist_ok=True)
    nodes_mode = s.source_mode == "nodes"
    pool_mode = s.source_mode in ("nodes", "proxy")
    plist = ProxyList(s.proxy_file) if s.source_mode == "proxy" else None

    m = Metrics()
    seen = SeenCache(s.seen_max)
    vpn: VpnManager | ExternalVpn | PipelineVpn | None = None
    vpn_src = s.vpn_sub_url or s.vpn_nodes_file
    if s.vpn_mode == "pipeline":
        vpn = PipelineVpn(s.nodes_url, s.nodes_poll)
        vpn_src = s.nodes_url
        log.info("vpn: pipeline, nodes from %s every %.0fs", s.nodes_url, s.nodes_poll)
    elif s.vpn_mode == "external":
        vpn = ExternalVpn(s.hub_nodes_file)
        vpn_src = s.hub_nodes_file
        log.info("vpn: external (xray-hub), nodes from %s", s.hub_nodes_file)
    elif vpn_src and (nodes_mode or not s.urlcheck_proxy):
        vpn = VpnManager(s.vpn_sub_url, s.data_dir, s.urlcheck_url, s.vpn_port_base,
                         s.vpn_test_interval, s.vpn_sub_refresh, s.vpn_switch_gain, nodes_file=s.vpn_nodes_file)
    if s.urlcheck_proxy:
        proxy_getter = lambda: s.urlcheck_proxy  # noqa: E731
        on_fail = None
        log.info("url-check via fixed proxy")
    elif vpn:
        proxy_getter, on_fail = vpn.proxy, vpn.report_failure
    else:
        proxy_getter, on_fail = (lambda: None), None
        log.warning("no URLCHECK_PROXY / VPN_SUB_URL / VPN_NODES_FILE: url-check goes direct")
    checker = UrlChecker(s.urlcheck_url, proxy_getter, s.urlcheck_policy, on_fail,
                         direct=not (s.urlcheck_proxy or vpn_src),
                         proxies_getter=(lambda: vpn.proxies(s.urlcheck_parallel)) if (vpn and not s.urlcheck_proxy) else None,
                         retries=s.urlcheck_retries)
    m.gauges["uc_lat"] = lambda: f"{checker.median_latency():.2f}"
    rt = RuntimeConfig(Path(config.CONFIG_PATH).parent / "runtime.yaml", s.target_rate, s.buffer_seconds)
    sender = Sender(s, seen, m, checker, rate_fn=lambda: rt.target_rate)
    demand = DemandGate(sender, lambda: rt.target_rate, lambda: rt.buffer_seconds)
    m.gauges["rate"] = lambda: f"{rt.target_rate:g}"
    m.gauges["buf"] = sender.pending
    bucket = TokenBucket(s.max_items_per_sec) if s.max_items_per_sec > 0 else None

    picker = TargetPicker(s.subs, s.popular_weight)
    on_page = make_on_page(s, m, seen, sender, mode, bucket, demand, picker)

    http = HttpClient(s.connect_timeout, s.read_timeout)
    scheds: list[Scheduler] = []
    pool: NodePool | None = None
    if pool_mode:
        src = plist or vpn
        tuner = AutoTuner(s.node_interval, s.node_interval_min, s.node_interval_max, s.node_popular_share,
                          s.node_autotune, data / "nodepool.json")
        budget = TrafficBudget(s.max_gb_per_day, data / "traffic.json")
        gate = PageGate(s.max_pages_per_sec) if s.max_pages_per_sec > 0 else None
        pool = NodePool(s, src, m, on_page, picker, tuner, budget, gate, demand,
                        state_path=data / "nodes_state.json")
        src.on_nodes_changed = pool.nodes_changed
        log.info("nodes mode: interval=%.1fs autotune=%s popular_share=%.2f include_direct=%s limits: items=%s pages=%s gb=%s",
                 tuner.interval, s.node_autotune, tuner.share, s.node_include_direct,
                 s.max_items_per_sec or "off", s.max_pages_per_sec or "off", s.max_gb_per_day or "off")

    def shutdown() -> None:
        if pool:
            pool.stop.set()
        for sc in scheds:
            sc.stop.set()

    def new_direct() -> Scheduler:
        sc = Scheduler(s, http, m, on_page, demand=demand)
        scheds.append(sc)
        return sc

    async def fallback_loop() -> None:  # NODE_FALLBACK_DIRECT: direct scheduler only while the pool has no good nodes
        cur: Scheduler | None = None
        task: asyncio.Task | None = None
        while pool is not None and not pool.stop.is_set():
            need = pool.good_count() == 0
            if need and cur is None:
                log.warning("nodepool: no good nodes, fallback to direct")
                cur = new_direct()
                task = asyncio.create_task(cur.run())
            elif not need and cur is not None and task is not None:
                cur.stop.set()
                await task
                cur = task = None
            await asyncio.sleep(10)
        if cur is not None and task is not None:
            cur.stop.set()
            await task

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    if vpn:
        await vpn.start()
    if plist:
        await plist.start()
    sender.start()
    hb = data / "heartbeat"

    async def ticker() -> None:
        while True:
            hb.touch()
            log.info(m.line())
            await asyncio.sleep(s.metrics_interval)

    tasks = [asyncio.create_task(ticker()), asyncio.create_task(rt.watch())]
    if s.dash_port > 0:
        sampler = Sampler(m)
        tasks += [asyncio.create_task(sampler.run()), asyncio.create_task(serve_dashboard(sampler, s.dash_host, s.dash_port))]
    if vpn:
        tasks.append(asyncio.create_task(vpn.run()))
    if plist:
        tasks.append(asyncio.create_task(plist.run()))
    try:
        if pool is None:
            await new_direct().run()
        else:
            runs = [pool.run()]
            if s.node_include_direct:
                runs.append(new_direct().run())
            elif s.node_fallback_direct:
                runs.append(fallback_loop())
            await asyncio.gather(*runs)
    finally:
        for t in tasks:
            t.cancel()
        await sender.close()
        await checker.close()
        await http.close()
        if vpn:
            await vpn.stop()
        if plist:
            await plist.stop()
        log.info("stopped")


if __name__ == "__main__":
    asyncio.run(amain())
