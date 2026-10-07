"""Backfill CLI — seeds the DB with historical 5m candles from Binance.

Usage:
    python -m app.backfill.run [--days N] [--symbols SYM ...]

Examples:
    python -m app.backfill.run                          # 2 days, all symbols
    python -m app.backfill.run --days 7                 # 7 days, all symbols
    python -m app.backfill.run --days 7 --symbols BTCUSDT ETHUSDT
"""

import argparse
import asyncio

from app.backfill.runner import backfill, save_candles
from app.db.session import engine

SYMBOLS  = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"]
INTERVAL = "5m"


async def main(days: int, symbols: list[str]) -> None:
    for symbol in symbols:
        print(f"[{symbol}] fetching {days}d of {INTERVAL} candles from Binance...", flush=True)
        candles = await backfill(symbol, INTERVAL, days)
        n = await save_candles(candles)
        print(f"[{symbol}] {n} candles upserted into DB", flush=True)

    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill 5m candles into the DB")
    parser.add_argument("--days",    type=int,  default=2,       help="Days of history (default: 2)")
    parser.add_argument("--symbols", nargs="+", default=SYMBOLS, help="Symbols to backfill")
    args = parser.parse_args()

    asyncio.run(main(args.days, args.symbols))
