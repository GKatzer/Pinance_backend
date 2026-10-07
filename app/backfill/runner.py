import asyncio
from datetime import UTC, datetime

import aiohttp

from app.backfill.binance_client import fetch_batch
from app.backfill.align import align_to_interval, now_ms
from app.db.session import SessionLocal
from sqlalchemy import text


INTERVAL_MAP = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "1d": 60 * 24,
}


async def backfill(symbol: str, interval: str, days: int) -> list[dict]:
    interval_min = INTERVAL_MAP[interval]

    end_ms = now_ms()
    start_ms = end_ms - days * 24 * 60 * 60 * 1000

    start_ms = align_to_interval(start_ms, interval_min)

    candles = {}

    async with aiohttp.ClientSession() as session:
        while start_ms < end_ms:
            batch = await fetch_batch(session, symbol, interval, start_ms, end_ms)

            if not batch:
                break

            for c in batch:
                open_time = align_to_interval(c[0], interval_min)

                candles[open_time] = {
                    "symbol": symbol,
                    "ts": open_time,
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                }

            last = batch[-1][0]
            step = interval_min * 60 * 1000

            start_ms = align_to_interval(last + step, interval_min)

            await asyncio.sleep(0.2)

    return [candles[k] for k in sorted(candles.keys())]


async def save_candles(candles: list[dict]) -> int:
    """Bulk-upsert candles into the DB. Returns the count of rows written."""
    if not candles:
        return 0

    params = [
        {
            "symbol": c["symbol"],
            "ts":     datetime.fromtimestamp(c["ts"] / 1000, tz=UTC).replace(tzinfo=None),
            "open":   c["open"],
            "high":   c["high"],
            "low":    c["low"],
            "close":  c["close"],
            "volume": c["volume"],
        }
        for c in candles
    ]

    async with SessionLocal() as session:
        await session.execute(
            text("""
                INSERT INTO candles (symbol, ts, open, high, low, close, volume)
                VALUES (:symbol, :ts, :open, :high, :low, :close, :volume)
                ON CONFLICT (symbol, ts) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """),
            params,
        )
        await session.commit()

    return len(params)