"""Разовый бэкафилл actual_price/actual_r/hit/sim_return для строк predictions,
дозревших ДО миграции b6e2c4a8f1d3 (актуализатор пишет их сам для всего, что
дозревает после деплоя — см. app/ingest/actualizer.py).

Батчами по as_of_ts (по умолчанию 24ч), от старых к новым, каждый батч —
отдельная короткая транзакция + пауза между батчами, чтобы не держать одно
соединение пула долго (это и есть то, что мы убираем) и не забивать пул,
которым в это время пользуется живой трафик. Оба slot (production и
candidate) — WHERE actual_price IS NULL сам решает, что ещё не резолвлено,
без явного фильтра по slot. Безопасно перезапускать/прерывать: уже
обработанные строки просто не попадут под условие повторно.

Usage:
    python -m app.backfill.resolve_predictions [--batch-hours N] [--sleep S]
"""

import argparse
import asyncio
from datetime import datetime, timedelta

from sqlalchemy import text

from app.db.session import SessionLocal, engine


async def _bounds() -> tuple[datetime, datetime] | None:
    async with SessionLocal() as session:
        result = await session.execute(
            text("SELECT MIN(as_of_ts), MAX(as_of_ts) FROM predictions WHERE actual_price IS NULL")
        )
        lo, hi = result.first()
    return (lo, hi) if lo is not None else None


async def _run_batch(batch_start: datetime, batch_end: datetime) -> int:
    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                UPDATE predictions p
                SET actual_price = c1.close,
                    actual_r     = ln(c1.close / c0.close),
                    hit          = (sign(p.r_pred) = sign(ln(c1.close / c0.close))),
                    sim_return   = sign(p.r_pred) * ln(c1.close / c0.close)
                FROM candles c1, candles c0
                WHERE c1.symbol   = p.symbol AND c1.ts = p.target_ts
                  AND c0.symbol   = p.symbol AND c0.ts = p.as_of_ts
                  AND p.actual_price IS NULL
                  AND p.as_of_ts >= :batch_start
                  AND p.as_of_ts  < :batch_end
            """),
            {"batch_start": batch_start, "batch_end": batch_end},
        )
        await session.commit()
        return result.rowcount


async def main(batch_hours: int, sleep_s: float) -> None:
    bounds = await _bounds()
    if bounds is None:
        print("Nothing to backfill — no unresolved rows.")
        await engine.dispose()
        return

    lo, hi = bounds
    print(f"Backfilling predictions {lo} .. {hi} in {batch_hours}h batches, {sleep_s}s pause between batches")

    step = timedelta(hours=batch_hours)
    batch_start = lo
    total = 0
    while batch_start <= hi:
        batch_end = batch_start + step
        n = await _run_batch(batch_start, batch_end)
        total += n
        print(f"  [{batch_start} .. {batch_end}) -> {n} rows resolved (total {total})", flush=True)
        batch_start = batch_end
        if sleep_s:
            await asyncio.sleep(sleep_s)

    print(f"Done. {total} rows resolved.")
    await engine.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill actual_price/actual_r/hit/sim_return on predictions (one-time, batched)"
    )
    parser.add_argument("--batch-hours", type=int, default=24, help="Batch window in hours (default: 24)")
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to sleep between batches (default: 1.0)")
    args = parser.parse_args()

    asyncio.run(main(args.batch_hours, args.sleep))
