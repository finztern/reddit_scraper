# reddit_scraper

Reddit comments -> local collector (Exorde). Runs only on a home machine (residential IPv4). Spec: `TZ_reddit_parser.md`.

## Setup
```
cp .env.example .env        # edit values, never commit .env
docker compose build
```
Code is baked into the image: after any change run `docker compose up -d --build`.
Config dir `./config` and `./data` are mounted as directories. `./data` must be writable by uid 1000.

## Step 0: collect HTML samples
```
mkdir -p data && sudo chown 1000:1000 data
docker compose run --rm parser python scripts/dump_samples.py
ls -la data/samples
```

## Run
```
cp .env.example .env     # set VPN_SUB_URL (secret, env only)
docker compose up -d --build
docker compose logs -f --tail=50
```
Metrics line every 30 s. Heartbeat: `data/heartbeat`. Spool (collector down): `data/spool/`.

## url-check VPN
`VPN_SUB_URL` -> vless subscription (re-downloaded every `VPN_SUB_REFRESH`=3600 s). All nodes run in one xray
(SOCKS 127.0.0.1:21000+N); every `VPN_TEST_INTERVAL`=600 s each node is probed against url-check, the lowest
median latency wins (switch only if >20% faster or current is dead). Fixed `URLCHECK_PROXY` disables the manager.
Check url-check answer format: `docker compose run --rm parser python scripts/check_urlcheck.py`
(with `URLCHECK_PROXY` set), or look for `vpn:` lines in the log.

## Checklist
- `docker compose logs parser | grep vpn` -> "vpn test: N/M alive", "vpn: using <node>"
- log line: `new/s` >= 10, `uc_known` low, `store_err=0`, `queue` stable
- stop collector 1 min -> `spooled` grows, process alive; start it -> spool drains

## Nodes mode (`SOURCE_MODE=nodes`)
Reddit is polled through the subscription nodes (one worker + one long-lived session per good node), home IP optionally
as one more worker (`NODE_INCLUDE_DIRECT`). Needs `VPN_SUB_URL`; `direct` stays the default.
```
# .env: SOURCE_MODE=nodes, VPN_SUB_URL=...
docker compose up -d --build
docker compose logs -f --tail=50 | grep -E "nodepool|new/s"
docker compose run --rm parser python scripts/scan_nodes.py     # per-node table (safari/chrome/codes/region)
```
- Start: all nodes are scanned (`NODE_SCAN_CONCURRENCY`), good = 200 with >=20 comments; log `nodepool: scan good=N/M`.
- A 403 never recreates the session (retry in the same one); 4 in a row -> quarantine `NODE_QUARANTINE`s, then session reset.
- Rescan every `NODE_RESCAN`s, on subscription change, or when good < `NODE_MIN_GOOD` (only nodes without a worker).
- `NODE_AUTOTUNE`: every `NODE_TUNE_WINDOW`s interval x0.9 (429<3%, empty<25%) / x1.25 (429>8% or empty>40%), rollback + 10 min
  freeze if new/s does not grow >3% in 2 windows; popular share auto 0.2-0.7. State: `data/nodepool.json`.
- Limiters (0 = off): `MAX_ITEMS_PER_SEC`, `MAX_PAGES_PER_SEC`, `MAX_GB_PER_DAY` (rolling 24h, `data/traffic.json`; 90% -> interval x2, 100% -> pause).
- `URLCHECK_CONCURRENCY` = parallel url-check batches (watch `uc_lat` and `queue`).
- Log fields: `nodes=good/total mb/s gb_day iv pop rep sat cold403 uc_lat queue rate_cap(in filtered)`.

### Nodes from a file
Put vless:// links (one per line, or base64) into `config/nodes.txt` (git-ignored) and set `VPN_NODES_FILE=/app/config/nodes.txt`
instead of `VPN_SUB_URL`. The file is re-read every `VPN_TEST_INTERVAL`s; node set changes restart xray automatically.

## Output rate (hot-reload, no restart)
`config/runtime.yaml`: `target_rate: 50` = unique (after url-check) comments/s sent to the collector, `0` = unlimited.
Edit the file and save: picked up within ~2 s (`docker compose logs | grep runtime`). Works because `./config` is mounted as a directory.
- Output is released in even 0.5 s slices; `buffer_seconds` of output are kept pre-fetched, so the flow does not stall during a request.
- Page requests are demand-driven (nodes and direct): fetch only while buffer + in-flight < `target_rate*buffer_seconds`.
- Check: `sent/s` ~= `target_rate`, `buf` ~= `target_rate*buffer_seconds`, `rate=` in the log line. Interval autotune does not probe faster while limited.

## Node state & escalating quarantine
- `data/nodes_state.json` remembers per node (by hash, no links): good/bad scan result, profile, quarantine level. After a restart
  known-good nodes start without a scan, known-bad ones (checked < `NODE_STATE_TTL`=6 h ago) are skipped, resting nodes finish their rest.
- Quarantine escalates per node: `NODE_QUARANTINE` x1, x3, x12 (600 s / 30 min / 2 h), resets after 1 h of healthy work.
- Periodic rescan only rechecks bad nodes older than the TTL (10 min if good < `NODE_MIN_GOOD`). Delete the file to force a full rescan.

## Longevity settings
- `NODE_MAX_PER_MIN=6`: hard per-node ceiling (any 60 s window), independent of demand/autotune. Capacity ~ nodes*cap/60 pages/s.
- TLS profile is fixed per node (the one that passed the scan); it is never rotated mid-session.
- `NODE_INTERVAL_MIN` = `NODE_INTERVAL` (5 s): autotune never polls faster than the base, it only backs off on 429/403 and returns to the base.

## Dashboard
Live web page with all metrics: `http://<pc-ip>:8080` from any LAN device (`DASH_HOST`, default 0.0.0.0, no auth, `DASH_PORT`; 0 = off). JSON: `/api`. Host network, no port mapping needed.
