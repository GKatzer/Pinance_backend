import aiohttp

BINANCE_URL = "https://api.binance.com/api/v3/klines"

async def fetch_batch(session, symbol, interval, start_ms, end_ms):
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000
    }

    async with session.get(BINANCE_URL, params=params) as r:
        return await r.json()