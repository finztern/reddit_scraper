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
