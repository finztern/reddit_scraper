"""Settings: config/config.yaml defaults overridden by env vars."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

CONFIG_PATH = Path(os.environ.get("CONFIG_PATH", "config/config.yaml"))


def _split(v: str) -> list[str]:
    return [x.strip() for x in v.split(",") if x.strip()]


@dataclass
class Settings:
    store_endpoints: list[str] = field(default_factory=lambda: ["http://127.0.0.1:9000/store_items"])
    urlcheck_proxy: str = ""
    urlcheck_policy: str = "closed"
    subs: list[str] = field(default_factory=list)
    req_interval: float = 1.0
    jitter: float = 0.2
    batch_min: int = 30
    batch_max: int = 100
    flush_seconds: float = 5.0
    min_text_len: int = 7
    parent_mode: str = "null"
    log_level: str = "INFO"
    seen_max: int = 500000
    connect_timeout: float = 10.0
    read_timeout: float = 15.0
    store_concurrency: int = 4
    urlcheck_url: str = "https://url-check.exorde.network/v1/check"
    urlcheck_batch: int = 100
    urlcheck_flush: float = 1.0
    popular_weight: int = 3
    adaptive: bool = False
    interval_min: float = 0.6
    interval_max: float = 3.0
    queue_max: int = 5000
    spool_max_files: int = 2000
    spool_max_age: float = 1800.0       # spooled batches older than this (sec) are deleted; 0 = keep forever
    metrics_interval: float = 30.0
    data_dir: str = "data"
    # VPN manager (active when vpn_sub_url is set and urlcheck_proxy is empty)
    vpn_sub_url: str = ""
    vpn_mode: str = "managed"            # managed (own xray) | external (xray-hub) | pipeline (local nodes API)
    nodes_url: str = "http://127.0.0.1:8099/nodes"
    nodes_poll: float = 30.0
    hub_nodes_file: str = "/hub/nodes.json"
    vpn_nodes_file: str = ""             # file with vless:// lines (alternative to the subscription link)
    vpn_port_base: int = 21000
    vpn_test_interval: float = 600.0
    vpn_sub_refresh: float = 3600.0
    vpn_switch_gain: float = 0.8
    urlcheck_concurrency: int = 4
    urlcheck_parallel: int = 2          # independent url-check requests per batch (fastest nodes)
    urlcheck_retries: int = 1           # extra rounds before URLCHECK_POLICY applies
    # nodes mode
    source_mode: str = "direct"          # direct | nodes
    proxy_file: str = "config/proxys.txt"  # SOURCE_MODE=proxy: one proxy per line (git-ignored)
    node_interval: float = 5.0
    node_autotune: bool = True
    node_interval_min: float = 5.0
    node_interval_max: float = 15.0
    node_tune_window: float = 60.0
    node_include_direct: bool = True
    node_scan_concurrency: int = 100
    node_quarantine: float = 600.0
    node_rescan: float = 1800.0
    node_min_good: int = 10
    node_popular_share: float = 0.5
    node_fallback_direct: bool = False
    node_max_per_min: int = 6            # hard per-node ceiling, requests per 60 s (0 = off)
    node_state_ttl: float = 21600.0      # remembered scan result is trusted this long (sec)
    node_profiles: list[str] = field(default_factory=lambda: ["safari", "chrome"])
    max_items_per_sec: float = 0.0       # 0 = unlimited
    max_pages_per_sec: float = 0.0
    max_gb_per_day: float = 0.0
    dash_host: str = "0.0.0.0"          # LAN-visible; DASH_HOST=127.0.0.1 to keep it local only
    dash_port: int = 8080                # 0 = dashboard off
    target_rate: float = 0.0             # initial value; config/runtime.yaml overrides and hot-reloads
    buffer_seconds: float = 5.0


class ConfigError(Exception):
    pass


_NODE_KEYS = {  # attr -> type
    "vpn_mode": str, "hub_nodes_file": str, "nodes_url": str, "nodes_poll": float,
    "source_mode": str, "proxy_file": str, "node_interval": float, "node_autotune": bool, "node_interval_min": float,
    "node_interval_max": float, "node_tune_window": float, "node_include_direct": bool,
    "node_scan_concurrency": int, "node_quarantine": float, "node_rescan": float, "node_min_good": int,
    "node_popular_share": float, "node_fallback_direct": bool, "node_state_ttl": float, "node_max_per_min": int, "node_profiles": list,
    "max_items_per_sec": float, "max_pages_per_sec": float, "max_gb_per_day": float,
    "urlcheck_concurrency": int, "urlcheck_parallel": int, "urlcheck_retries": int, "target_rate": float, "buffer_seconds": float,
    "spool_max_age": float,
}


def _conv(typ, v):
    if typ is bool:
        return v if isinstance(v, bool) else str(v).lower() in ("1", "true", "yes")
    if typ is list:
        return _split(v) if isinstance(v, str) else [str(x) for x in v]
    return typ(v)


def check(s: "Settings") -> None:
    """Startup validation; raises ConfigError with a readable message."""
    if s.vpn_mode not in ("managed", "external", "pipeline"):
        raise ConfigError(f"VPN_MODE must be managed|external|pipeline, got {s.vpn_mode!r}")
    if s.source_mode not in ("direct", "nodes", "proxy"):
        raise ConfigError(f"SOURCE_MODE must be direct|nodes|proxy, got {s.source_mode!r}")
    if s.source_mode == "proxy" and not Path(s.proxy_file).is_file():
        raise ConfigError(f"SOURCE_MODE=proxy: PROXY_FILE not found: {s.proxy_file}")
    if s.source_mode == "nodes":
        if s.vpn_mode == "external":
            if not Path(s.hub_nodes_file).is_file():
                raise ConfigError(f"VPN_MODE=external: HUB_NODES_FILE not found: {s.hub_nodes_file} (start xray-hub first)")
        elif s.vpn_mode == "managed" and not (s.vpn_sub_url or s.vpn_nodes_file):
            raise ConfigError("SOURCE_MODE=nodes requires VPN_SUB_URL or VPN_NODES_FILE (set it in .env)")
        if s.vpn_nodes_file and not Path(s.vpn_nodes_file).is_file():
            raise ConfigError(f"VPN_NODES_FILE not found: {s.vpn_nodes_file}")


def load() -> Settings:
    y = yaml.safe_load(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    y = y or {}
    s = Settings()
    if "store_endpoint" in y:
        s.store_endpoints = _split(str(y["store_endpoint"]))
    for k in ("urlcheck_policy", "parent_mode", "log_level", "urlcheck_url", "data_dir"):
        if k in y:
            setattr(s, k, str(y[k]))
    for k in ("req_interval", "jitter", "flush_seconds", "connect_timeout", "read_timeout",
              "urlcheck_flush", "interval_min", "interval_max", "metrics_interval",
              "vpn_test_interval", "vpn_sub_refresh", "vpn_switch_gain"):
        if k in y:
            setattr(s, k, float(y[k]))
    for k in ("batch_min", "batch_max", "min_text_len", "seen_max", "store_concurrency", "urlcheck_batch",
              "popular_weight", "queue_max", "spool_max_files", "vpn_port_base"):
        if k in y:
            setattr(s, k, int(y[k]))
    if "adaptive" in y:
        s.adaptive = bool(y["adaptive"])
    if "subs" in y:
        s.subs = [str(x) for x in y["subs"]]
    e = os.environ.get
    if e("STORE_ENDPOINT"):
        s.store_endpoints = _split(e("STORE_ENDPOINT"))
    s.urlcheck_proxy = e("URLCHECK_PROXY", s.urlcheck_proxy)
    s.urlcheck_policy = e("URLCHECK_POLICY", s.urlcheck_policy)
    if e("SUBS"):
        s.subs = _split(e("SUBS"))
    s.req_interval = float(e("REQ_INTERVAL", s.req_interval))
    s.batch_min = int(e("BATCH_MIN", s.batch_min))
    s.batch_max = int(e("BATCH_MAX", s.batch_max))
    s.flush_seconds = float(e("FLUSH_SECONDS", s.flush_seconds))
    s.min_text_len = int(e("MIN_TEXT_LEN", s.min_text_len))
    s.parent_mode = e("PARENT_MODE", s.parent_mode)
    s.log_level = e("LOG_LEVEL", s.log_level)
    s.seen_max = int(e("SEEN_MAX", s.seen_max))
    s.store_concurrency = int(e("STORE_CONCURRENCY", s.store_concurrency))
    s.adaptive = e("ADAPTIVE", str(s.adaptive)).lower() in ("1", "true", "yes")
    s.vpn_sub_url = e("VPN_SUB_URL", s.vpn_sub_url)  # secret: env only
    s.vpn_nodes_file = e("VPN_NODES_FILE", s.vpn_nodes_file)
    s.vpn_test_interval = float(e("VPN_TEST_INTERVAL", s.vpn_test_interval))
    s.vpn_sub_refresh = float(e("VPN_SUB_REFRESH", s.vpn_sub_refresh))
    s.data_dir = e("DATA_DIR", s.data_dir)
    s.dash_host = e("DASH_HOST", s.dash_host)
    s.dash_port = int(e("DASH_PORT", s.dash_port))
    for attr, typ in _NODE_KEYS.items():
        if attr in y:
            setattr(s, attr, _conv(typ, y[attr]))
        if e(attr.upper()) not in (None, ""):
            setattr(s, attr, _conv(typ, e(attr.upper())))
    return s
