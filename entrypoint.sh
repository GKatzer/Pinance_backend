#!/bin/sh
set -e

echo "Running database migrations..."
alembic upgrade head

echo "Running candle backfill (last 2 days)..."
python -m app.backfill.run --days 2 || echo "Backfill failed (Binance unreachable?), continuing..."


echo "Starting API server..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --proxy-headers --forwarded-allow-ips="*"
