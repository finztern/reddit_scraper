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


def load() -> Settings:
    y = yaml.safe_load(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    y = y or {}
    s = Settings()
    if "store_endpoint" in y:
        s.store_endpoints = _split(str(y["store_endpoint"]))
    for k in ("urlcheck_policy", "parent_mode", "log_level"):
        if k in y:
            setattr(s, k, str(y[k]))
    for k in ("req_interval", "jitter", "flush_seconds", "connect_timeout", "read_timeout"):
        if k in y:
            setattr(s, k, float(y[k]))
    for k in ("batch_min", "batch_max", "min_text_len", "seen_max"):
        if k in y:
            setattr(s, k, int(y[k]))
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
    return s
