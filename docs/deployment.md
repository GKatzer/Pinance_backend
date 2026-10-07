# Deployment

How the backend is packaged and started, from `Dockerfile`, `docker-compose.yml`, `entrypoint.sh` and `app/config.py`. Real hosts, addresses and the reverse-proxy configuration are not part of this repository and are not described here.

What was and was not verified for this document:

| Item | Status |
|---|---|
| `docker compose config` with `.env.example` and the override file renders a valid configuration (3 services, local ports `127.0.0.1:8002` and `127.0.0.1:5433`) | run, 2026-10-03 |
| `docker compose up`, image build, health checks, `entrypoint.sh` | **not run**: the Docker daemon was not accessible in the environment where this documentation was written (the user was not in the `docker` group) |
| Migrations against TimescaleDB (`alembic upgrade head`) | **not run** for the same reason; `alembic heads` returns the single head `d9e4b6f8c3a5` |
| Application code, admin queries, backfill/resolve scripts | run against plain PostgreSQL 16 without TimescaleDB, see [`examples/admin_offline_demo.py`](examples/admin_offline_demo.py) |

## Services

`docker-compose.yml` (project name `pinance`, one internal bridge network):

| Service | Image | Limits and settings |
|---|---|---|
| `postgres` | `timescale/timescaledb:latest-pg16` | 512 MB memory limit, 256 MB shared memory; named volume `postgres_data`; health check `pg_isready` every 5 s |
| `redis` | `redis:7-alpine` | append-only file on, `--maxmemory 256mb --maxmemory-policy allkeys-lru`; named volume `redis_data`; health check `redis-cli ping` |
| `api` | built from `Dockerfile` | 256 MB memory limit; reads `.env`; compose sets `DATABASE_URL` (from the `POSTGRES_*` variables) and `REDIS_URL`; starts only after both dependencies are healthy; health check `GET /health` every 10 s |

The container listens on port 8000. In `docker-compose.yml` the published ports of `postgres` and `api` are bound to one specific address of a private network, so that the other machines of the system (the training host reads the database; the promotion script calls `/admin/...`) can reach them and nothing else can. Such an interface does not exist on a development machine, so Docker refuses to start. The file `docker-compose.override.yml.example` replaces the bindings with `127.0.0.1:5433` (PostgreSQL) and `127.0.0.1:8002` (API); copy it to `docker-compose.override.yml`, which is git-ignored. Compose picks the override up automatically, which is why it must never be committed: on the server it would silently replace the private binding.

The `api` service also bind-mounts `./app` over `/app/app`, so a code change on the host is visible in the container after a restart without rebuilding the image.

## Image

`Dockerfile`: `python:3.12-slim`, `libpq5` and `curl` installed, dependencies from `requirements.txt` in their own layer, an unprivileged `app` user, `ENTRYPOINT ["./entrypoint.sh"]`. No `--reload` (the comment says: it adds a watcher process in production).

## Startup sequence (`entrypoint.sh`)

1. `alembic upgrade head` (the first migration needs the TimescaleDB extension; `CREATE EXTENSION IF NOT EXISTS timescaledb`).
2. `python -m app.backfill.run --days 2`: two days of 5-minute candles from Binance REST for the four symbols. If Binance is unreachable the script's failure is ignored (`|| echo ...`) and startup continues.
3. `uvicorn app.main:app --host 0.0.0.0 --port 8000 --proxy-headers --forwarded-allow-ips="*"`.

Then, inside the process (`lifespan`): Redis pool, scheduler, WebSocket consumer. Missing forecasts for the last 48 hours are filled by the first scheduler ticks after the inference service becomes available.

## What is needed besides the code

| Need | Why | Where to get it |
|---|---|---|
| PostgreSQL with the TimescaleDB extension | hypertables, `time_bucket`, `first`/`last` in the daily candle aggregate and in `history` | the compose image |
| Redis | live ticks, SSE, caches, watermark | the compose image |
| Outbound access to Binance (`wss://stream.binance.com:9443`, `https://api.binance.com`) | candles | public endpoints, no API key |
| The inference service (`Pinance_ml_inference`) reachable at `ML_INFERENCE_URL`, with a loaded model | forecasts | the related repository. Without it candles, the live tick stream and the candle endpoints still work; no forecasts are stored |
| A reverse proxy for the public site | TLS, forwarding of the public prefixes only | not in this repository |

## Configuration

Settings are read by `app/config.py` (pydantic-settings, from the environment, then from `.env`); the compose file additionally uses the `POSTGRES_*` variables.

| Variable | Meaning | Default | Required |
|---|---|---|---|
| `DATABASE_URL` | async SQLAlchemy URL (`postgresql+asyncpg://...`) | none | yes (compose builds it) |
| `REDIS_URL` | Redis URL | `redis://redis:6379/0` | no (compose sets it) |
| `ML_INFERENCE_URL` | base URL of the inference service | none | yes |
| `ML_INFERENCE_TIMEOUT_S` | HTTP timeout of one inference call, seconds | `5.0` | no |
| `ML_POLL_INTERVAL_MINUTES` | period of the forecast job (cron `*/N`) | `5` | no |
| `ML_CANDLE_WINDOW_SIZE` | candles sent per request; must be at least the inference service's minimum | `150` | no |
| `ML_GAP_LOOKBACK_HOURS` | how far back the gap scan looks | `48` | no |
| `METRICS_REFRESH_INTERVAL_MINUTES` | period of the cache-refresh job | `5` | no |
| `ML_SHADOW_ENABLED` | also poll the candidate model (`/predict/{symbol}/shadow`) | `false` | no |
| `APP_ENV` | `dev` (console log renderer) or `prod` (JSON log lines) | `dev` | no |
| `LOG_LEVEL` | log level | `INFO` | no |
| `DB_POOL_SIZE`, `DB_MAX_OVERFLOW` | SQLAlchemy pool; not listed in `.env.example` | `5`, `5` | no |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | used by compose for the database container and the URL; `POSTGRES_PASSWORD` has no default and compose refuses to start without it | `pinance`, `pinance`, none | password: yes |

`.env.example` ships `POSTGRES_PASSWORD=changeme`: replace it before exposing anything. The same file's `ML_INFERENCE_URL` default points at `http://localhost:8001`, which inside the `api` container is the container itself, so set it to an address the container can reach.

## Operating notes

- **Logs.** `structlog`; event names such as `scheduler.predictions_missing`, `scheduler.tick_skipped`, `ml_inference.rejected`, `ml_inference.stored`, `actualizer.due`, `binance_ws.error`.
- **Binance WebSocket.** Reconnects with backoff from 1 s to 60 s, ping every 20 s, timeout 10 s.
- **Health.** `/health` is a liveness check only; `/ready` also checks PostgreSQL and Redis. Neither checks the inference service or the freshness of the data.
- **Schema changes** go through Alembic (`app/db/migrations`); the container applies them on start. Resolving forecasts that matured before migration `b6e2c4a8f1d3` is a separate manual command, `python -m app.backfill.resolve_predictions`.
