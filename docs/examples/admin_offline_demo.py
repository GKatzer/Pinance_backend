"""Offline demo of the /admin/* endpoints (no Docker, no TimescaleDB, no Redis, no Binance).

What it does
  1. starts a throw-away PostgreSQL 16 from the `pgserver` pip package (plain Postgres,
     without the TimescaleDB extension);
  2. creates the tables from app/db/models.py (not from the Alembic migrations, which need
     the TimescaleDB extension);
  3. inserts SYNTHETIC data: a random-walk of 5-minute BTCUSDT candles, and forecasts for
     two slots ("production" and "candidate") with two model versions and two quantile
     versions, then resolves them with the repository's own `app.backfill.resolve_predictions`;
  4. serves the admin and metrics routers of the real application code on 127.0.0.1:8099
     (the /history and /coverage_history metrics need TimescaleDB's time_bucket and fail here).

The numbers it produces say nothing about model quality: the forecasts are random noise
around the realised path. The point is to exercise the real query code in
app/api/admin.py (GET compare, POST retrain-events) with realistic response shapes.

Run from the repository root (a clean checkout, without a real .env):

    uv venv .venv && uv pip install -p .venv -r requirements.txt pgserver
    .venv/bin/python docs/examples/admin_offline_demo.py        # keeps serving until Ctrl-C

then, in another shell:

    curl -s '127.0.0.1:8099/admin/metrics/BTC%2FUSDT/compare?window=24h'
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pgserver

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

PORT = 8099
SYMBOL = "BTCUSDT"
N_CANDLES = 700          # about 2.4 days of 5-minute candles
SEED = 42


def _async_url(pg_uri: str) -> str:
    # pgserver gives postgresql://postgres:@/postgres?host=<unix socket dir>
    return pg_uri.replace("postgresql://", "postgresql+asyncpg://", 1)


async def _prepare() -> None:
    from sqlalchemy import text

    from app.backfill.resolve_predictions import main as resolve_main
    from app.db.models import Base
    from app.db.session import SessionLocal, engine

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    rnd = random.Random(SEED)
    now = datetime.now(UTC).replace(tzinfo=None, second=0, microsecond=0)
    last_ts = now - timedelta(minutes=now.minute % 5 + 5)
    ts0 = last_ts - timedelta(minutes=5 * (N_CANDLES - 1))

    price, candles = 65000.0, []
    for i in range(N_CANDLES):
        o = price
        price = o * (1 + rnd.gauss(0, 0.0007))
        candles.append({
            "symbol": SYMBOL, "ts": ts0 + timedelta(minutes=5 * i), "open": o,
            "high": max(o, price) * 1.0002, "low": min(o, price) * 0.9998,
            "close": price, "volume": 10 + rnd.random(),
        })

    # (slot, point version, quantile version, share of the window that uses this pair)
    plan = [
        ("production", "202610010300-demoprod", "q-202609270430", 0.0, 0.5),
        ("production", "202610020300-demoprod", "q-202609270430", 0.5, 1.0),
        ("candidate",  "202610030300-democand", "q-202610030430", 0.5, 1.0),
    ]
    rows = []
    for slot, ver, qver, lo, hi in plan:
        for i in range(int(N_CANDLES * lo), min(int(N_CANDLES * hi), N_CANDLES - 12)):
            as_of = candles[i]
            for h in range(1, 13):
                target = candles[i + h]
                true_r = (target["close"] / as_of["close"]) - 1
                r_pred = true_r * 0.1 + rnd.gauss(0, 0.0006)
                width = 0.0004 * (h ** 0.5) * (1.6 if slot == "candidate" else 1.0)
                rows.append({
                    "symbol": SYMBOL, "as_of_ts": as_of["ts"], "horizon": h, "slot": slot,
                    "model_version": ver, "quantile_model_version": qver,
                    "target_ts": target["ts"], "r_pred": r_pred,
                    "price_pred": as_of["close"] * (1 + r_pred),
                    "r_q10": r_pred - width, "r_q90": r_pred + width,
                    "price_q10": as_of["close"] * (1 + r_pred - width),
                    "price_q90": as_of["close"] * (1 + r_pred + width),
                    "inference_ms": 40 + rnd.random() * 30,
                })

    async with SessionLocal() as session:
        await session.execute(text(
            "INSERT INTO candles (symbol, ts, open, high, low, close, volume) "
            "VALUES (:symbol, :ts, :open, :high, :low, :close, :volume)"), candles)
        await session.execute(text(
            "INSERT INTO predictions (symbol, as_of_ts, horizon, slot, model_version, "
            "quantile_model_version, target_ts, r_pred, price_pred, r_q10, r_q90, "
            "price_q10, price_q90, inference_ms) VALUES (:symbol, :as_of_ts, :horizon, :slot, "
            ":model_version, :quantile_model_version, :target_ts, :r_pred, :price_pred, "
            ":r_q10, :r_q90, :price_q10, :price_q90, :inference_ms)"), rows)
        await session.commit()
    print(f"seeded {len(candles)} candles and {len(rows)} prediction rows", flush=True)

    await resolve_main(batch_hours=24, sleep_s=0.0)   # the repository's own resolver script


def main() -> None:
    import uvicorn
    from fastapi import FastAPI

    tmp = tempfile.mkdtemp(prefix="pinance-demo-pg-")
    server = pgserver.get_server(tmp, cleanup_mode="delete")
    os.environ["DATABASE_URL"] = _async_url(server.get_uri())
    os.environ.setdefault("ML_INFERENCE_URL", "http://127.0.0.1:1")   # never contacted here
    os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:1/0")       # not running: see note below

    asyncio.run(_prepare())

    from app.api.admin import routers
    from app.api.metrics import router as metrics_router

    app = FastAPI(title="Pinance admin (offline demo)")
    for r in routers:
        app.include_router(r)
    app.include_router(metrics_router)

    print(f"serving admin + metrics routers on http://127.0.0.1:{PORT}", flush=True)
    # Redis is not running here: POST /admin/retrain-events then logs a warning for the
    # cache invalidation and still returns 201 (the row is already committed).
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
