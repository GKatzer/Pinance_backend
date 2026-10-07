"""Candles REST endpoint.

GET /candles/{symbol}?timeframe=1D|1W|1M|1Y|ALL

Источник данных по таймфрейму:
  1D  → PostgreSQL (5m свечи, live-данные от WS)         →  288 точек
  1W  → Binance REST kline 15m                           →  672 точек
  1M  → Binance REST kline 1h                            →  720 точек
  1Y  → PostgreSQL (дневная агрегация 5m через time_bucket) → 365 точек
  ALL → PostgreSQL (дневная агрегация, вся история)      →  вся история

GET /candles/{symbol}/latest
  → последний тик из Redis (polling fallback)

GET /candles/{symbol}/pred_history?horizon=1&hours=24
  → актуализированные прогнозы одного горизонта за окно — трейл для графика

GET /candles/{symbol}/prediction_log?limit=240&before=<ISO ts>
  → все горизонты подряд (Prediction Log на фронте) — "load more" за пределами
    Redis-хвоста (app.ingest.actualizer.RESULTS_RECENT_MAXLEN/TTL), напрямую
    из Postgres
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from app.core.redis import get_redis
from app.db.session import SessionLocal
from app.ingest.actualizer import RESULTS_RECENT_MAXLEN
from app.inference.registry import normalize as _normalize
from app.inference.registry import to_binance as _binance_symbol
from sqlalchemy import text

router = APIRouter(prefix="/candles", tags=["candles"])

# pred_history: как и /metrics/*, читает JOIN predictions x candles x candles —
# без кэша это пересчитывалось бы на каждый заход на страницу (это её дефолтный
# запрос для 1D). Тот же принцип: scheduler (app.scheduler.jobs) прогревает
# фиксированные комбинации — ровно то, что шлёт фронт (см. PRED_HISTORY_PARAMS
# в useLiveCandles.js: 1D → horizon=1/hours=24, 1W → horizon=3/hours=168, там
# горизонт модели кратно ложится на границы 15м-свечи) — с длинным TTL; кэш
# здесь ниже — только fallback для нетиповых комбинаций.
_PRED_HISTORY_CACHE_TTL = 120
PRED_HISTORY_SCHEDULED_TTL = 900
PRED_HISTORY_SCHEDULED_TARGETS = [
    (1, 24),   # 1D
    (3, 168),  # 1W
]

# ──────────────────────────────────────────────────────────────────────────────
# Конфиг таймфреймов
# ──────────────────────────────────────────────────────────────────────────────

_TIMEFRAME_CONFIG = {
    "1D":  {"interval": "5m",  "source": "db",      "limit": 288},
    "1W":  {"interval": "15m", "source": "binance",  "limit": 672},
    "1M":  {"interval": "1h",  "source": "binance",  "limit": 720},
    "1Y":  {"interval": "1d",  "source": "db_daily", "limit": 365, "days": 370},
    "ALL": {"interval": "1d",  "source": "db_daily", "limit": 5000, "days": None},
}

# TTL кэша ответов в Redis (ключ klines:{symbol}:{timeframe})
_CACHE_TTL = {
    "1D":  300,   # 5 минут — совпадает с интервалом закрытия свечи (см. _from_db)
    "1W":  300,   # 5 минут
    "1M":  600,   # 10 минут
    "1Y":  3600,  # 1 час
    "ALL": 3600,
}

_BINANCE_REST  = "https://api.binance.com/api/v3/klines"


# ──────────────────────────────────────────────────────────────────────────────
# Источники данных
# ──────────────────────────────────────────────────────────────────────────────

async def _query_db_candles(symbol: str, limit: int) -> list[dict]:
    """Чистый запрос, без кэша — переиспользуется _from_db (кэш-чтение) и
    refresh_symbol_candles (прогрев, всегда пересчитывает напрямую)."""
    async with SessionLocal() as session:
        rows = await session.execute(
            text("""
                SELECT
                    extract(epoch from ts) * 1000 AS ts,
                    open, high, low, close, volume
                FROM (
                    SELECT ts, open, high, low, close, volume
                    FROM candles
                    WHERE symbol = :symbol
                    ORDER BY ts DESC
                    LIMIT :limit
                ) sub
                ORDER BY ts DESC
            """),
            {"symbol": _binance_symbol(symbol), "limit": limit},
        )
        rows = rows.mappings().all()

    return [
        {
            "ts":     int(r["ts"]),
            "open":   float(r["open"]),
            "high":   float(r["high"]),
            "low":    float(r["low"]),
            "close":  float(r["close"]),
            "volume": float(r["volume"]),
            "closed": True,
        }
        for r in reversed(rows)   # БД вернула DESC, нам нужен ASC
    ]


async def _from_db(symbol: str, limit: int) -> list[dict]:
    """PostgreSQL — 5m свечи для дефолтного таймфрейма 1D. Кэш в Redis, тот
    же принцип, что у _from_db_daily/_from_binance рядом (ключ
    klines:{symbol}:1D) — но здесь это не просто fallback: 1D открывается
    при каждом заходе на график, без кэша это была бы БД-нагрузка на каждый
    HTTP-запрос. candles пишется только по закрытию свечи (см.
    app.ingest.binance_ws._save_candle) — результат стабилен между
    закрытиями. Основной путь свежести — прогрев из scheduler
    (refresh_symbol_candles) сразу после каждого закрытия; TTL здесь —
    fallback на случай пропущенного цикла опроса, не основной механизм."""
    cache_key = f"klines:{symbol}:1D"

    try:
        cached = await get_redis().get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    candles = await _query_db_candles(symbol, limit)

    try:
        await get_redis().set(cache_key, json.dumps(candles), ex=_CACHE_TTL["1D"])
    except Exception:
        pass

    return candles


async def refresh_symbol_candles(symbol_display: str) -> None:
    """Вызывается из app.scheduler.jobs сразу после закрытия свечи —
    пересчитывает 1D-кэш напрямую (минуя кэш-чтение в _from_db) и форсит
    запись, иначе при TTL, близком к периоду опроса, можно было бы отдать
    предыдущее закрытие ещё не истёкшим по TTL кэшем. Тот же принцип, что
    refresh_symbol_pred_history/app.api.metrics.refresh_symbol_metrics."""
    limit = _TIMEFRAME_CONFIG["1D"]["limit"]
    candles = await _query_db_candles(symbol_display, limit)
    cache_key = f"klines:{symbol_display}:1D"
    try:
        await get_redis().set(cache_key, json.dumps(candles), ex=_CACHE_TTL["1D"])
    except Exception:
        pass


async def _from_db_daily(symbol: str, limit: int, timeframe: str, days: int | None) -> list[dict]:
    """PostgreSQL — дневные свечи, агрегированные из 5m через time_bucket. Кэш в Redis."""
    cache_key = f"klines:{symbol}:{timeframe}"
    ttl = _CACHE_TTL.get(timeframe, 300)

    try:
        cached = await get_redis().get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    where_days = "AND ts > now() - (:days * INTERVAL '1 day')" if days is not None else ""
    params: dict = {"symbol": _binance_symbol(symbol), "limit": limit}
    if days is not None:
        params["days"] = days

    async with SessionLocal() as session:
        rows = await session.execute(
            text(f"""
                SELECT
                    extract(epoch from bucket) * 1000 AS ts,
                    open, high, low, close, volume
                FROM (
                    SELECT
                        time_bucket('1 day', ts) AS bucket,
                        first(open, ts) AS open,
                        max(high) AS high,
                        min(low) AS low,
                        last(close, ts) AS close,
                        sum(volume) AS volume
                    FROM candles
                    WHERE symbol = :symbol {where_days}
                    GROUP BY bucket
                    ORDER BY bucket DESC
                    LIMIT :limit
                ) sub
                ORDER BY bucket DESC
            """),
            params,
        )
        rows = rows.mappings().all()

    candles = [
        {
            "ts":     int(r["ts"]),
            "open":   float(r["open"]),
            "high":   float(r["high"]),
            "low":    float(r["low"]),
            "close":  float(r["close"]),
            "volume": float(r["volume"]),
            "closed": True,
        }
        for r in reversed(rows)   # БД вернула DESC, нам нужен ASC
    ]

    try:
        await get_redis().set(cache_key, json.dumps(candles), ex=ttl)
    except Exception:
        pass

    return candles


async def _from_binance(symbol: str, interval: str, limit: int, timeframe: str) -> list[dict]:
    """Binance REST /klines с кэшом в Redis."""
    cache_key = f"klines:{symbol}:{timeframe}"
    ttl = _CACHE_TTL.get(timeframe, 300)

    # Пробуем кэш
    try:
        cached = await get_redis().get(cache_key)
        if cached:
            return json.loads(cached)
    except Exception:
        pass

    # Запрос к Binance
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(
            _BINANCE_REST,
            params={"symbol": _binance_symbol(symbol), "interval": interval, "limit": limit},
        )
        resp.raise_for_status()
        raw = resp.json()

    # Binance: [open_time, open, high, low, close, volume, close_time, ...]
    candles = [
        {
            "ts":     int(k[0]),
            "open":   float(k[1]),
            "high":   float(k[2]),
            "low":    float(k[3]),
            "close":  float(k[4]),
            "volume": float(k[5]),
            "closed": True,
        }
        for k in raw
    ]

    # Кэшируем результат
    try:
        await get_redis().set(cache_key, json.dumps(candles), ex=ttl)
    except Exception:
        pass

    return candles


# ──────────────────────────────────────────────────────────────────────────────
# Endpoints
#
# ВАЖНО: маршруты с суффиксом (/latest, /pred_history) регистрируются РАНЬШЕ
# catch-all /{symbol:path} — FastAPI матчит роуты в порядке регистрации, а
# :path жадно захватывает слэши, так что при обратном порядке catch-all
# перехватывает "BTC/USDT/latest" целиком как symbol и /latest никогда не
# срабатывает (не гипотетически — так и было, проверено).
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/{symbol:path}/latest")
async def get_latest(symbol: str) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    raw = await get_redis().get(f"candle:{sym}")
    if not raw:
        raise HTTPException(status_code=404, detail="No live data yet")

    return JSONResponse(json.loads(raw))


async def _compute_pred_history(binance_symbol: str, horizon: int, hours: int) -> list[dict]:
    """История прогнозов одного горизонта, для трейла на графике (LAYER 1
    в PriceChart). "Актуализированная" строка = actual_price/hit уже
    проставлены actualizer'ом при дозревании (не JOIN на чтении, как было
    до b6e2c4a8f1d3 — см. app/db/models.py Prediction). Строки, которые ещё
    не дозрели (actual_price IS NULL), просто не попадают.

    q10/q90 — источник для прошлой (актуализированной) части band на графике,
    nullable как и predicted (см. app/db/models.py Prediction) — точечные
    строки до промоушена квантилей отдают только predicted, фронт фоллбэчит.

    Только slot = 'production' — фронт не должен видеть shadow-кандидата,
    если сейчас идёт его сравнение с прод-моделью.
    """
    async with SessionLocal() as session:
        rows = await session.execute(
            text("""
                SELECT target_ts, price_pred, price_q10, price_q90, hit
                FROM predictions
                WHERE symbol       = :symbol
                  AND horizon      = :horizon
                  AND slot         = 'production'
                  AND actual_price IS NOT NULL
                  AND target_ts   >= now() - (:hours * INTERVAL '1 hour')
                  AND target_ts   <= now()
                ORDER BY target_ts ASC
            """),
            {"symbol": binance_symbol, "horizon": horizon, "hours": hours},
        )
        rows = rows.mappings().all()

    return [
        {
            "ts":        int(r["target_ts"].replace(tzinfo=UTC).timestamp() * 1000),
            "predicted": float(r["price_pred"]),
            "q10":       float(r["price_q10"]) if r["price_q10"] is not None else None,
            "q90":       float(r["price_q90"]) if r["price_q90"] is not None else None,
            "hit":       bool(r["hit"]),
        }
        for r in rows
    ]


def _pred_history_cache_key(sym: str, horizon: int, hours: int) -> str:
    return f"pred_history:{sym}:{horizon}:{hours}"


@router.get("/{symbol:path}/pred_history")
async def get_pred_history(
    symbol: str,
    horizon: int = Query(default=1, ge=1, le=12),
    hours: int = Query(default=24, ge=1, le=168),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = _pred_history_cache_key(sym, horizon, hours)
    try:
        cached = await get_redis().get(cache_key)
        if cached:
            return JSONResponse(json.loads(cached))
    except Exception:
        pass

    history = await _compute_pred_history(_binance_symbol(sym), horizon, hours)
    payload = {"symbol": sym, "horizon": horizon, "history": history}

    try:
        await get_redis().set(cache_key, json.dumps(payload), ex=_PRED_HISTORY_CACHE_TTL)
    except Exception:
        pass

    return JSONResponse(payload)


async def refresh_symbol_pred_history(symbol_display: str) -> None:
    """Вызывается из app.scheduler.jobs после актуализации — прогревает кэш
    для всех комбинаций (horizon, hours), которые реально шлёт фронт
    (см. PRED_HISTORY_SCHEDULED_TARGETS выше), с длинным TTL. Тот же принцип,
    что и app.api.metrics.refresh_symbol_metrics."""
    binance_symbol = _binance_symbol(symbol_display)
    for horizon, hours in PRED_HISTORY_SCHEDULED_TARGETS:
        history = await _compute_pred_history(binance_symbol, horizon, hours)
        payload = {"symbol": symbol_display, "horizon": horizon, "history": history}
        cache_key = _pred_history_cache_key(symbol_display, horizon, hours)
        try:
            await get_redis().set(cache_key, json.dumps(payload), ex=PRED_HISTORY_SCHEDULED_TTL)
        except Exception:
            pass


# ──────────────────────────────────────────────────────────────────────────────
# prediction_log — "load more" для Prediction Log за пределами Redis-хвоста
# ──────────────────────────────────────────────────────────────────────────────
#
# /snapshot отдаёт последние RESULTS_RECENT_MAXLEN (=240, 20 свечей × 12
# горизонтов) записей из Redis-листа — этого достаточно для первого экрана,
# но список живёт с TTL 2 часа и не бесконечен. Дальше вглубь (90 дней
# retention predictions) — читаем напрямую из Postgres, actual_price/actual_r/
# hit уже проставлены actualizer'ом при дозревании (не JOIN на чтении, как
# было до b6e2c4a8f1d3 — см. app/db/models.py Prediction), без watermark:
# просто окно назад по курсору `before`.
#
# Все 12 горизонтов подряд, без фильтра — фронт сам выбирает/сортирует
# (см. обсуждение с фронтом: это чистая клиентская фильтрация готовых полей,
# агрегации на бэке не требует). ORDER BY идёт (target_ts DESC, horizon ASC),
# поэтому курсор `before` корректно постранично работает, только если limit —
# кратен 12 (иначе страница обрывается посреди горизонтов одной свечи, и
# следующий `before` пропустит остаток той же свечи). 240 по умолчанию это
# уже соблюдает; если фронт передаёт свой limit — должен держать это сам.
#
# Кэшируется только первая страница (before=None, limit=RESULTS_RECENT_MAXLEN
# — литеральный дефолт, "открыл панель, ещё не скроллил") — это единственная
# комбинация, у которой есть один общий ключ на всех клиентов. Курсорная
# пагинация вглубь (before=<что угодно>) — длинный хвост уникальных курсоров,
# общий кэш ей не поможет, остаётся посчитанной на лету (тот же принцип,
# что у admin.py::_compare_window — не каждый read стоит кэшировать, только
# тот, что реально бьёт много клиентов одним и тем же ключом).
_PREDICTION_LOG_CACHE_TTL = 120
PREDICTION_LOG_SCHEDULED_TTL = 900


async def _compute_prediction_log(
    binance_symbol: str, limit: int, before: datetime | None,
) -> list[dict]:
    where_before = "AND target_ts < :before" if before is not None else ""
    params: dict = {"symbol": binance_symbol, "limit": limit}
    if before is not None:
        params["before"] = before

    async with SessionLocal() as session:
        rows = await session.execute(
            text(f"""
                SELECT
                    as_of_ts, horizon, target_ts, r_pred, price_pred,
                    price_q10, price_q90, model_version,
                    actual_price, actual_r, hit
                FROM predictions
                WHERE symbol       = :symbol
                  AND slot         = 'production'
                  AND actual_price IS NOT NULL
                  AND target_ts   <= now()
                  {where_before}
                ORDER BY target_ts DESC, horizon ASC
                LIMIT :limit
            """),
            params,
        )
        rows = rows.mappings().all()

    return [
        {
            # naive isoformat (без 'Z'/offset) — тот же формат, что и живые
            # записи из actualizer._publish_results; фронт (toMs в
            # useLiveCandles.js) уже умеет достраивать 'Z' сам.
            "as_of_ts":      r["as_of_ts"].isoformat(),
            "horizon":       r["horizon"],
            "target_ts":     r["target_ts"].isoformat(),
            "r_pred":        float(r["r_pred"]),
            "price_pred":    float(r["price_pred"]),
            "price_q10":     float(r["price_q10"]) if r["price_q10"] is not None else None,
            "price_q90":     float(r["price_q90"]) if r["price_q90"] is not None else None,
            "actual_r":      float(r["actual_r"]),
            "actual_price":  float(r["actual_price"]),
            "hit":           bool(r["hit"]),
            "model_version": r["model_version"],
        }
        for r in rows
    ]


def _prediction_log_cache_key(sym: str, limit: int) -> str:
    return f"prediction_log:{sym}:{limit}"


@router.get("/{symbol:path}/prediction_log")
async def get_prediction_log(
    symbol: str,
    limit: int = Query(default=RESULTS_RECENT_MAXLEN, ge=1, le=1000),
    before: str | None = Query(default=None),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    before_ts: datetime | None = None
    if before:
        try:
            before_ts = datetime.fromisoformat(before)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid 'before': {before!r}")
        if before_ts.tzinfo is not None:
            before_ts = before_ts.astimezone(UTC).replace(tzinfo=None)

    # Кэш — только для первой страницы (см. docstring секции выше).
    cache_key = _prediction_log_cache_key(sym, limit) if before_ts is None else None
    if cache_key is not None:
        try:
            cached = await get_redis().get(cache_key)
            if cached:
                return JSONResponse(json.loads(cached))
        except Exception:
            pass

    log = await _compute_prediction_log(_binance_symbol(sym), limit, before_ts)
    payload = {
        "symbol":   sym,
        "log":      log,
        "has_more": len(log) == limit,
    }

    if cache_key is not None:
        try:
            await get_redis().set(cache_key, json.dumps(payload), ex=_PREDICTION_LOG_CACHE_TTL)
        except Exception:
            pass

    return JSONResponse(payload)


async def refresh_symbol_prediction_log(symbol_display: str) -> None:
    """Вызывается из app.scheduler.jobs после актуализации — прогревает кэш
    первой страницы Prediction Log (limit=RESULTS_RECENT_MAXLEN, before=None)
    — единственная комбинация с общим ключом (см. docstring секции выше).
    Тот же принцип, что refresh_symbol_pred_history."""
    binance_symbol = _binance_symbol(symbol_display)
    log = await _compute_prediction_log(binance_symbol, RESULTS_RECENT_MAXLEN, None)
    payload = {
        "symbol":   symbol_display,
        "log":      log,
        "has_more": len(log) == RESULTS_RECENT_MAXLEN,
    }
    cache_key = _prediction_log_cache_key(symbol_display, RESULTS_RECENT_MAXLEN)
    try:
        await get_redis().set(cache_key, json.dumps(payload), ex=PREDICTION_LOG_SCHEDULED_TTL)
    except Exception:
        pass


@router.get("/{symbol:path}")
async def get_candles(
    symbol: str,
    timeframe: str = Query(default="1D", pattern="^(1D|1W|1M|1Y|ALL)$"),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cfg = _TIMEFRAME_CONFIG[timeframe]

    try:
        if cfg["source"] == "db":
            candles = await _from_db(sym, cfg["limit"])
        elif cfg["source"] == "db_daily":
            candles = await _from_db_daily(sym, cfg["limit"], timeframe, cfg["days"])
        else:
            candles = await _from_binance(sym, cfg["interval"], cfg["limit"], timeframe)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Binance unavailable: {exc}")

    return JSONResponse({
        "symbol":    sym,
        "timeframe": timeframe,
        "interval":  cfg["interval"],
        "candles":   candles,
    })