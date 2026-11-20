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

## Run (after TZ step 3)
`docker compose up -d --build`, logs: `docker compose logs -f --tail=50`.
