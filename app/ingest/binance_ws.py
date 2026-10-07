"""Binance WebSocket consumer — 5-minute candles.

На каждом тике (каждую секунду внутри 5-минутной свечи):
  → Redis candle:{symbol}          — текущая цена для SSE-стрима фронта
  → Redis pub/sub candles:{symbol} — для SSE endpoint

На каждой ЗАКРЫТОЙ 5m свече:
  → PostgreSQL INSERT/UPDATE (candles)

Прогнозы больше не считаются здесь: ML-инференс переехал на VDS2, а опрос
`GET /predict/{symbol}` и запись в predictions — в app.scheduler.jobs,
независимая APScheduler-джоба раз в 5 минут (см. app/inference/predictor.py).
Этот модуль отвечает только за свечи и live-тики.

Reconnect: exponential backoff 1s → 60s.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import TypedDict

import structlog
import websockets

from app.core.redis import get_redis
from app.db.session import SessionLocal
from sqlalchemy import text

log = structlog.get_logger()

SYMBOLS   = ["btcusdt", "ethusdt", "solusdt", "bnbusdt"]
INTERVAL  = "5m"   # ← 5-минутные свечи

_BACKOFF_INITIAL = 1.0
_BACKOFF_MAX     = 60.0
_BACKOFF_FACTOR  = 2.0

_REDIS_LAST_KEY = "candle:{symbol}"
_REDIS_CHANNEL  = "candles:{symbol}"

_WS_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@kline_{INTERVAL}" for s in SYMBOLS)
)


class CandleDict(TypedDict):
    symbol: str
    ts: int          # unix ms
    open: float
    high: float
    low: float
    close: float
    volume: float


def _display(symbol: str) -> str:
    """btcusdt → BTC/USDT"""
    s = symbol.upper()
    return (s[:-4] + "/USDT") if s.endswith("USDT") else s


# ──────────────────────────────────────────────────────────────────────────────
# Тик → Redis
# ──────────────────────────────────────────────────────────────────────────────

async def _publish_tick(raw_symbol: str, kline: dict, closed: bool) -> None:
    sym     = _display(raw_symbol)
    payload = json.dumps({
        "symbol": sym,
        "ts":     kline["t"],
        "closed": closed,
        "open":   float(kline["o"]),
        "high":   float(kline["h"]),
        "low":    float(kline["l"]),
        "close":  float(kline["c"]),
        "volume": float(kline["v"]),
    })
    await get_redis().set(_REDIS_LAST_KEY.format(symbol=sym), payload)
    await get_redis().publish(_REDIS_CHANNEL.format(symbol=sym), payload)


# ──────────────────────────────────────────────────────────────────────────────
# Свеча → PostgreSQL
# ──────────────────────────────────────────────────────────────────────────────

async def _save_candle(symbol: str, candle: CandleDict) -> None:
    async with SessionLocal() as session:
        await session.execute(
            text("""
                INSERT INTO candles (symbol, ts, open, high, low, close, volume)
                VALUES (:symbol, :ts, :open, :high, :low, :close, :volume)
                ON CONFLICT (symbol, ts) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                    close=EXCLUDED.close, volume=EXCLUDED.volume
            """),
            {
                "symbol": symbol.upper(),
                "ts":     datetime.fromtimestamp(candle["ts"]/1000, tz=UTC).replace(tzinfo=None),
                "open":   candle["open"],  "high": candle["high"],
                "low":    candle["low"],   "close": candle["close"],
                "volume": candle["volume"],
            },
        )
        await session.commit()


# ──────────────────────────────────────────────────────────────────────────────
# Обработка сообщения
# ──────────────────────────────────────────────────────────────────────────────

async def _handle_message(raw: str) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError:
        return

    data = msg.get("data", msg)
    if data.get("e") != "kline":
        return

    kline      = data["k"]
    raw_symbol = data["s"].lower()
    closed     = bool(kline["x"])

    try:
        await _publish_tick(raw_symbol, kline, closed)
    except Exception as exc:
        log.warning("tick.publish_failed", symbol=raw_symbol, error=str(exc))

    if closed:
        candle: CandleDict = {
            "symbol": _display(raw_symbol), "ts": int(kline["t"]),
            "open":   float(kline["o"]), "high": float(kline["h"]),
            "low":    float(kline["l"]), "close": float(kline["c"]),
            "volume": float(kline["v"]),
        }
        try:
            await _save_candle(raw_symbol, candle)
        except Exception as exc:
            log.exception("candle.save_failed", symbol=raw_symbol, error=str(exc))


# ──────────────────────────────────────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────────────────────────────────────

async def run_consumer() -> None:
    backoff = _BACKOFF_INITIAL
    log.info("binance_ws.starting", symbols=SYMBOLS, interval=INTERVAL)

    while True:
        try:
            async with websockets.connect(
                _WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5,
            ) as ws:
                log.info("binance_ws.connected")
                backoff = _BACKOFF_INITIAL
                async for message in ws:
                    await _handle_message(message)

        except asyncio.CancelledError:
            raise

        except websockets.exceptions.ConnectionClosedOK:
            continue

        except (
            websockets.exceptions.ConnectionClosedError,
            websockets.exceptions.WebSocketException,
            OSError, TimeoutError,
        ) as exc:
            log.warning("binance_ws.error", error=str(exc), backoff=backoff)

        except Exception as exc:
            log.exception("binance_ws.unexpected", error=str(exc))

        await asyncio.sleep(backoff)
        backoff = min(backoff * _BACKOFF_FACTOR, _BACKOFF_MAX)
