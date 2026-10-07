"""Метрики качества модели — для сайдбара LivePrediction (page1) и
ModelPerformance (page2). См. BACKEND_REQUIREMENTS.md во фронтенд-репо.

Принцип: фронт не копит и не агрегирует ничего сам — все окна (1h/24h/7d),
тренды и распределения считает бэкенд, по всей доступной истории для окна,
не по последним N в памяти вкладки (это и было проблемой старого HIT RATE
на page1 — 20 последних SSE-результатов, разное на разных вкладках).

Everything here читает готовые actual_price/actual_r/hit/sim_return —
проставляет их app.ingest.actualizer в момент, когда прогноз дозревает (см.
её docstring и app/db/models.py Prediction, почему не JOIN на чтении, как
было до b6e2c4a8f1d3). Только slot = 'production' — кандидат из shadow-
деплоя сюда не должен попадать (сравнение с ним — app/api/admin.py).

Агрегируется по всем 12 горизонтам сразу (headline "как модель сейчас
работает в целом"), не по одному горизонту — в отличие от pred_history,
которая специально про один горизонт для трейла на графике. Исключение —
sharpe в _window_metrics: симулированная торговля требует фиксированного
шага одной стратегии, поэтому считается только по horizon=1 (см. её
docstring).

Кэш пишется в двух местах:
  1. Здесь, лениво — если кто-то запросил комбинацию параметров, которую
     scheduler не считает по расписанию (см. ниже), считаем один раз и
     кэшируем на короткий TTL.
  2. app.scheduler.jobs.refresh_metrics_cache — на каждом цикле опроса VDS2
     пересчитывает фиксированный набор комбинаций (ровно те, что реально
     использует фронт) и пишет с длинным TTL. Это основной путь: при 1000
     одновременных пользователей они читают Redis, а не считают JOIN сами —
     вычисление одно, не одно на запрос.
"""

from __future__ import annotations

import asyncio
import json
import re
import statistics
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.core.redis import get_redis
from app.db.session import SessionLocal
from app.inference.registry import normalize as _normalize
from app.inference.registry import to_binance as _binance_symbol

router = APIRouter(prefix="/metrics", tags=["metrics"])

# Ничего здесь не претендует на realtime — окна меняются раз в ~5 минут
# (см. BACKEND_REQUIREMENTS.md). TTL здесь — только fallback для комбинаций
# параметров, которые scheduler не считает (см. _HISTORY_TARGETS и т.д.);
# для "родных" комбинаций scheduler переписывает кэш каждый цикл раньше,
# чем истечёт даже этот короткий TTL.
_SUMMARY_CACHE_TTL = 45   # page1 — рядом с ценой, чуть свежее
_CHART_CACHE_TTL   = 120  # page2 — карточки, обновление раз в ~2 минуты незаметно

# TTL для записи scheduler'ом (app.scheduler.jobs.refresh_metrics_cache) —
# намного длиннее цикла опроса, чтобы пара пропущенных циклов (VDS2 недоступен
# и т.п.) не обнулила дашборд всем пользователям одновременно.
SCHEDULED_CACHE_TTL = 900  # 15 минут

# Комбинации параметров, которые реально дёргает фронт (см. useMetrics.js) —
# именно их пересчитывает scheduler каждый цикл. Любая другая комбинация
# по-прежнему работает через обычный запрос ниже, просто без прогрева.
_HISTORY_TARGETS: list[tuple[str, str, str]] = [
    ("accuracy", "24h", "1h"),
    ("accuracy", "7d", "1d"),
    ("mae", "24h", "1h"),
]
_ERRORS_TARGETS: list[tuple[str, int]] = [("24h", 24)]
_WIDTH_HISTOGRAM_TARGETS: list[tuple[str, int]] = [("24h", 24)]
_SCATTER_TARGETS: list[tuple[str, int]] = [("24h", 200)]
_COVERAGE_HISTORY_TARGETS: list[tuple[str, str]] = [("24h", "1h")]
_LATENCY_TARGETS: list[str] = ["24h"]
_RETRAIN_TIMELINE_TARGETS: list[int] = [8]

_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)([mhd])$")
_DURATION_UNIT_HOURS = {"m": 1 / 60, "h": 1.0, "d": 24.0}

# Аннуализация Sharpe — периодов в году при шаге в 5 минут (horizon=1,
# см. _window_metrics). 365*24*60/5 = 105120.
_SHARPE_PERIODS_PER_YEAR = 365 * 24 * 60 // 5


def _parse_hours(raw: str) -> float:
    """'24h' -> 24.0, '7d' -> 168.0, '15m' -> 0.25."""
    m = _DURATION_RE.match(raw.strip().lower())
    if not m:
        raise HTTPException(status_code=400, detail=f"Invalid duration: {raw!r}")
    value, unit = m.groups()
    return float(value) * _DURATION_UNIT_HOURS[unit]


async def _cache_get(key: str) -> dict | None:
    try:
        raw = await get_redis().get(key)
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def _cache_set(key: str, payload: dict, ttl: int) -> None:
    try:
        await get_redis().set(key, json.dumps(payload), ex=ttl)
    except Exception:
        pass


# ──────────────────────────────────────────────────────────────────────────────
# /summary — чистое вычисление, без кэша (кэш — в вызывающих)
# ──────────────────────────────────────────────────────────────────────────────

async def _window_metrics(symbol: str, hours: float) -> dict:
    """accuracy/mae/rmse/n за последние `hours`, плюс accuracy предыдущего
    такого же по длине окна (для delta_accuracy) — одним запросом через
    FILTER, без второго round-trip'а.

    sharpe — Sharpe симулированной торговли "лонг, если r_pred>0, шорт если
    <0" (sim_return = sign(r_pred) * actual_r), СЧИТАЕТСЯ ТОЛЬКО ПО
    horizon=1 (5-минутный шаг), а не пулом по всем 12 горизонтам, как
    accuracy/mae/rmse выше — одна стратегия должна иметь фиксированный шаг,
    иначе аннуализация теряет смысл (нельзя мешать в одной серии
    доходностей 5-минутные и часовые позиции). horizon=1 вдобавок даёт
    неперекрывающиеся окна (as_of_ts=T → target_ts=T+5м, следующая
    as_of_ts=T+5м) — чистая серия для Sharpe, без наложения периодов.

    Раньше (до b6e2c4a8f1d3) resolved здесь был JOIN predictions×candles —
    для окна "all" (10 лет, т.е. вся история) это был двойной джойн ~250к+
    строк к 3М+ строкам candles, живьём выполнявшийся по 20+ минут и державший
    соединение пула всё это время. actual_price/actual_r/hit/sim_return теперь
    проставлены заранее (app.ingest.actualizer) — здесь просто range-скан по
    ix_predictions_resolved."""
    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                WITH resolved AS (
                    SELECT target_ts, horizon, price_pred, actual_price, actual_r, hit, sim_return
                    FROM predictions
                    WHERE symbol        = :symbol
                      AND slot          = 'production'
                      AND actual_price  IS NOT NULL
                      AND target_ts     > now() - (:hours2 * INTERVAL '1 hour')
                      AND target_ts    <= now()
                )
                SELECT
                    COUNT(*) FILTER (
                        WHERE target_ts > now() - (:hours * INTERVAL '1 hour')
                    ) AS n,
                    (AVG(hit::int) FILTER (
                        WHERE target_ts > now() - (:hours * INTERVAL '1 hour')
                    ))::float8 * 100 AS accuracy,
                    (AVG(hit::int) FILTER (
                        WHERE target_ts <= now() - (:hours * INTERVAL '1 hour')
                    ))::float8 * 100 AS accuracy_prev,
                    AVG(ABS(price_pred - actual_price)) FILTER (
                        WHERE target_ts > now() - (:hours * INTERVAL '1 hour')
                    ) AS mae,
                    sqrt(AVG(POWER(price_pred - actual_price, 2)) FILTER (
                        WHERE target_ts > now() - (:hours * INTERVAL '1 hour')
                    )) AS rmse,
                    (AVG(sim_return) FILTER (
                        WHERE horizon = 1 AND target_ts > now() - (:hours * INTERVAL '1 hour')
                    ))::float8 AS sharpe_mean,
                    (STDDEV_SAMP(sim_return) FILTER (
                        WHERE horizon = 1 AND target_ts > now() - (:hours * INTERVAL '1 hour')
                    ))::float8 AS sharpe_std,
                    COUNT(*) FILTER (
                        WHERE horizon = 1 AND target_ts > now() - (:hours * INTERVAL '1 hour')
                    ) AS sharpe_n
                FROM resolved
            """),
            {"symbol": symbol, "hours": hours, "hours2": hours * 2},
        )
        row = result.mappings().first()

    n = row["n"] or 0
    accuracy = row["accuracy"]
    accuracy_prev = row["accuracy_prev"]
    delta_accuracy = (
        round(accuracy - accuracy_prev, 1)
        if accuracy is not None and accuracy_prev is not None
        else None
    )

    sharpe_n = row["sharpe_n"] or 0
    sharpe = (
        round((row["sharpe_mean"] / row["sharpe_std"]) * (_SHARPE_PERIODS_PER_YEAR ** 0.5), 3)
        if row["sharpe_std"] not in (None, 0) and sharpe_n >= 2
        else None
    )

    return {
        "directional_accuracy": round(accuracy, 1) if accuracy is not None else None,
        "mae":                  round(row["mae"], 2) if row["mae"] is not None else None,
        "rmse":                 round(row["rmse"], 2) if row["rmse"] is not None else None,
        "delta_accuracy":       delta_accuracy,
        "n":                    n,
        "sharpe":               sharpe,
        "sharpe_n":             sharpe_n,
    }


# "all" — не буквально бесконечность, а "всё, что есть" в пределах retention
# predictions (90 дней, см. миграцию d4e8f1a72b9c). 10 лет — просто заведомо
# больше любого возможного retention, без спецкейса в SQL/_window_metrics.
_ALL_TIME_HOURS = 24 * 365 * 10

# Windows для /summary — те же ключи, что фронт мапит на вкладки таймфрейма
# (1D→1h, 1W→7d, 1M→30d, 1Y/ALL→all, см. HIT_RATE_WINDOW_BY_TIMEFRAME
# в screens.jsx). Горизонт модели (5-60 мин) от этого не меняется — меняется
# только глубина, за которую усредняем точность. Без leading underscore —
# переиспользуется в app.api.admin для production-vs-candidate сравнения.
SUMMARY_WINDOWS: dict[str, float] = {
    "1h":  1.0,
    "24h": 24.0,
    "7d":  168.0,
    "30d": 720.0,
    "all": _ALL_TIME_HOURS,
}


async def _compute_summary(binance_symbol: str) -> dict:
    labels = tuple(SUMMARY_WINDOWS.keys())
    results = await asyncio.gather(
        *(_window_metrics(binance_symbol, SUMMARY_WINDOWS[label]) for label in labels)
    )
    return {
        "windows":    dict(zip(labels, results)),
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


@router.get("/{symbol:path}/summary")
async def get_summary(symbol: str) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:summary:{sym}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_summary(_binance_symbol(sym))
    await _cache_set(cache_key, payload, _SUMMARY_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /coverage — живое покрытие квантильной корзины, для Accuracy на публичном UI
# ──────────────────────────────────────────────────────────────────────────────

async def _window_quantile_coverage(symbol: str, hours: float) -> dict:
    """q10_coverage/q90_coverage — та же живая метрика, что admin.py::
    _compare_window считает для production/candidate сравнения (см. её
    quantile_stats CTE), здесь — публичный срез: только slot='production'
    (публичный UI не сравнивает с кандидатом) и без группировки по
    quantile_model_version (публичный UI не должен знать про версии моделей,
    только "насколько корзина сейчас честная"). c0/as_of_ts не нужен — в
    отличие от _window_metrics, coverage не требует знака факт. return, только
    actual_price при target_ts."""
    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT
                    COUNT(*)                                        AS n,
                    (AVG((actual_price < price_q10)::int))::float8  AS q10_coverage,
                    (AVG((actual_price < price_q90)::int))::float8  AS q90_coverage
                FROM predictions
                WHERE symbol        = :symbol
                  AND slot          = 'production'
                  AND actual_price  IS NOT NULL
                  AND r_q10        IS NOT NULL
                  AND r_q90        IS NOT NULL
                  AND target_ts     > now() - (:hours * INTERVAL '1 hour')
                  AND target_ts    <= now()
            """),
            {"symbol": symbol, "hours": hours},
        )
        row = result.mappings().first()

    n = row["n"] or 0
    return {
        "n":            n,
        # None = в окне вообще нет квантильных строк (point-only модель на
        # всём окне) — не путать с 0.0 (квантили есть, но корзина промахивается
        # на каждой строке). См. app/api/admin.py._compare_window.
        "q10_coverage": round(row["q10_coverage"], 4) if row["q10_coverage"] is not None else None,
        "q90_coverage": round(row["q90_coverage"], 4) if row["q90_coverage"] is not None else None,
    }


async def _compute_quantile_coverage(binance_symbol: str) -> dict:
    labels = tuple(SUMMARY_WINDOWS.keys())
    results = await asyncio.gather(
        *(_window_quantile_coverage(binance_symbol, SUMMARY_WINDOWS[label]) for label in labels)
    )
    return {
        "windows":    dict(zip(labels, results)),
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


@router.get("/{symbol:path}/coverage")
async def get_coverage(symbol: str) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:coverage:{sym}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_quantile_coverage(_binance_symbol(sym))
    await _cache_set(cache_key, payload, _SUMMARY_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /history — точки для спарклайнов
# ──────────────────────────────────────────────────────────────────────────────

async def _compute_history(binance_symbol: str, metric: str, window: str, bucket: str) -> dict:
    window_hours = _parse_hours(window)
    bucket_hours = _parse_hours(bucket)
    if bucket_hours > window_hours:
        raise HTTPException(status_code=400, detail="bucket must not be larger than window")

    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT
                    time_bucket(:bucket_interval, target_ts)      AS bucket,
                    (AVG(hit::int))::float8 * 100                 AS accuracy,
                    AVG(ABS(price_pred - actual_price))           AS mae
                FROM predictions
                WHERE symbol        = :symbol
                  AND slot          = 'production'
                  AND actual_price  IS NOT NULL
                  AND target_ts     > now() - (:window_hours * INTERVAL '1 hour')
                  AND target_ts    <= now()
                GROUP BY bucket
                ORDER BY bucket ASC
            """),
            {
                "symbol":          binance_symbol,
                "window_hours":    window_hours,
                "bucket_interval": timedelta(hours=bucket_hours),
            },
        )
        rows = result.mappings().all()

    points = [
        {
            "ts":    r["bucket"].replace(tzinfo=UTC).isoformat().replace("+00:00", "Z"),
            "value": round(r["accuracy"], 1) if metric == "accuracy" else round(r["mae"], 2),
        }
        for r in rows
    ]

    return {"metric": metric, "window": window, "bucket": bucket, "points": points}


@router.get("/{symbol:path}/history")
async def get_history(
    symbol: str,
    metric: str = Query(pattern="^(accuracy|mae)$"),
    window: str = Query(default="24h"),
    bucket: str = Query(default="1h"),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:history:{sym}:{metric}:{window}:{bucket}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_history(_binance_symbol(sym), metric, window, bucket)
    await _cache_set(cache_key, payload, _CHART_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /coverage_history — то же самое для квантильной корзины: q10/q90 coverage,
# ширина корзины и pinball loss по бакетам. Отдельный роут, не третье
# значение metric= в /history — там на бакет одно число, здесь четыре сразу,
# тащить их round-trip'ами через /history?metric=... смысла нет.
# ──────────────────────────────────────────────────────────────────────────────

async def _compute_coverage_history(binance_symbol: str, window: str, bucket: str) -> dict:
    """Тот же паттерн, что _compute_history (window/bucket → time_bucket
    GROUP BY). avg_width (price_q90 - price_q10) — бесплатно в той же
    агрегации, для графика "сужается/расширяется корзина со временем".

    pinball_loss — та же формула, что pinance_ml.metrics.pinball_loss в
    Pinance_ML (max(q*(y-ŷ), (q-1)*(y-ŷ)), пулинг q10+q90 в одно среднее —
    см. auto_retrain_quantiles.py::_avg_pinball_and_coverage_ok), здесь — на
    реально актуализированных live-исходах, а не на офлайн holdout. Обе части
    — в r-пространстве (log-return, actual_r/r_q10/r_q90), не в ценах — чтобы
    число было сравнимо с тем, что тренировка репортит по тому же символу, а
    не считалось в другой шкале. Loss'у нужен actual_r — в отличие от
    q10_coverage/q90_coverage/avg_width, которым достаточно actual_price
    (ценовое неравенство actual_price < price_q10 эквивалентно r-неравенству
    actual_r < r_q10 — монотонное ln/exp сохраняет знак, а вот разность
    (y-ŷ) в pinball loss — нет)."""
    window_hours = _parse_hours(window)
    bucket_hours = _parse_hours(bucket)
    if bucket_hours > window_hours:
        raise HTTPException(status_code=400, detail="bucket must not be larger than window")

    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT
                    time_bucket(:bucket_interval, target_ts)         AS bucket,
                    (AVG((actual_price < price_q10)::int))::float8   AS q10_coverage,
                    (AVG((actual_price < price_q90)::int))::float8   AS q90_coverage,
                    AVG(price_q90 - price_q10)                       AS avg_width,
                    AVG(
                        (GREATEST(0.1 * (actual_r - r_q10), -0.9 * (actual_r - r_q10))
                       + GREATEST(0.9 * (actual_r - r_q90), -0.1 * (actual_r - r_q90))) / 2.0
                    )                                                 AS pinball_loss
                FROM predictions
                WHERE symbol        = :symbol
                  AND slot          = 'production'
                  AND actual_price  IS NOT NULL
                  AND r_q10        IS NOT NULL
                  AND r_q90        IS NOT NULL
                  AND target_ts     > now() - (:window_hours * INTERVAL '1 hour')
                  AND target_ts    <= now()
                GROUP BY bucket
                ORDER BY bucket ASC
            """),
            {
                "symbol":          binance_symbol,
                "window_hours":    window_hours,
                "bucket_interval": timedelta(hours=bucket_hours),
            },
        )
        rows = result.mappings().all()

    points = [
        {
            "ts":            r["bucket"].replace(tzinfo=UTC).isoformat().replace("+00:00", "Z"),
            "q10_coverage":  round(r["q10_coverage"], 4) if r["q10_coverage"] is not None else None,
            "q90_coverage":  round(r["q90_coverage"], 4) if r["q90_coverage"] is not None else None,
            "avg_width":     round(r["avg_width"], 2) if r["avg_width"] is not None else None,
            "pinball_loss":  round(r["pinball_loss"], 6) if r["pinball_loss"] is not None else None,
        }
        for r in rows
    ]

    return {"window": window, "bucket": bucket, "points": points}


@router.get("/{symbol:path}/coverage_history")
async def get_coverage_history(
    symbol: str,
    window: str = Query(default="24h"),
    bucket: str = Query(default="1h"),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:coverage_history:{sym}:{window}:{bucket}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_coverage_history(_binance_symbol(sym), window, bucket)
    await _cache_set(cache_key, payload, _CHART_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /errors — гистограмма ошибок
# ──────────────────────────────────────────────────────────────────────────────

async def _compute_errors(binance_symbol: str, window: str, bins: int) -> dict:
    window_hours = _parse_hours(window)

    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT price_pred - actual_price AS error
                FROM predictions
                WHERE symbol        = :symbol
                  AND slot          = 'production'
                  AND actual_price  IS NOT NULL
                  AND target_ts     > now() - (:window_hours * INTERVAL '1 hour')
                  AND target_ts    <= now()
            """),
            {"symbol": binance_symbol, "window_hours": window_hours},
        )
        errors = [float(r[0]) for r in result.all()]

    if not errors:
        return {"bin_edges": [], "counts": [], "mean_error": None, "std_error": None}

    lo, hi = min(errors), max(errors)
    width = (hi - lo) / bins if hi > lo else 1.0
    counts = [0] * bins
    for e in errors:
        idx = min(int((e - lo) / width), bins - 1) if width else 0
        counts[idx] += 1
    bin_edges = [round(lo + i * width, 2) for i in range(bins + 1)]

    mean_error = statistics.mean(errors)
    std_error = statistics.pstdev(errors) if len(errors) > 1 else 0.0

    return {
        "bin_edges":  bin_edges,
        "counts":     counts,
        "mean_error": round(mean_error, 2),
        "std_error":  round(std_error, 2),
    }


@router.get("/{symbol:path}/errors")
async def get_errors(
    symbol: str,
    window: str = Query(default="24h"),
    bins: int = Query(default=24, ge=1, le=100),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:errors:{sym}:{window}:{bins}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_errors(_binance_symbol(sym), window, bins)
    await _cache_set(cache_key, payload, _CHART_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /width_histogram — распределение ширины квантильной корзины. Зеркало
# /errors выше — то же самое (гистограмма одномерной величины по фикс. числу
# бинов), только величина — price_q90 - price_q10 вместо price_pred - close.
# ──────────────────────────────────────────────────────────────────────────────

async def _compute_width_histogram(binance_symbol: str, window: str, bins: int) -> dict:
    window_hours = _parse_hours(window)

    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT price_q90 - price_q10 AS width
                FROM predictions
                WHERE symbol        = :symbol
                  AND slot          = 'production'
                  AND actual_price  IS NOT NULL
                  AND r_q10        IS NOT NULL
                  AND r_q90        IS NOT NULL
                  AND target_ts     > now() - (:window_hours * INTERVAL '1 hour')
                  AND target_ts    <= now()
            """),
            {"symbol": binance_symbol, "window_hours": window_hours},
        )
        widths = [float(r[0]) for r in result.all()]

    if not widths:
        return {"bin_edges": [], "counts": [], "mean_width": None, "std_width": None}

    lo, hi = min(widths), max(widths)
    step = (hi - lo) / bins if hi > lo else 1.0
    counts = [0] * bins
    for w in widths:
        idx = min(int((w - lo) / step), bins - 1) if step else 0
        counts[idx] += 1
    bin_edges = [round(lo + i * step, 2) for i in range(bins + 1)]

    mean_width = statistics.mean(widths)
    std_width = statistics.pstdev(widths) if len(widths) > 1 else 0.0

    return {
        "bin_edges":  bin_edges,
        "counts":     counts,
        "mean_width": round(mean_width, 2),
        "std_width":  round(std_width, 2),
    }


@router.get("/{symbol:path}/width_histogram")
async def get_width_histogram(
    symbol: str,
    window: str = Query(default="24h"),
    bins: int = Query(default=24, ge=1, le=100),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:width_histogram:{sym}:{window}:{bins}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_width_histogram(_binance_symbol(sym), window, bins)
    await _cache_set(cache_key, payload, _CHART_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /scatter — predicted vs actual
# ──────────────────────────────────────────────────────────────────────────────

async def _compute_scatter(binance_symbol: str, window: str, limit: int) -> dict:
    window_hours = _parse_hours(window)

    # Один round-trip: total/ss_res/var_actual — оконные агрегаты по всей
    # выборке (одно и то же значение в каждой строке, берём из первой),
    # семпл — систематический (каждая step-я строка), чтобы точки были
    # распределены по всему окну, а не были просто "последними limit по времени".
    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                WITH numbered AS (
                    SELECT
                        price_pred, price_q10, price_q90, actual_price AS actual,
                        ROW_NUMBER() OVER (ORDER BY target_ts)                AS rn,
                        COUNT(*)     OVER ()                                  AS total,
                        SUM(POWER(actual_price - price_pred, 2)) OVER ()      AS ss_res,
                        VAR_POP(actual_price) OVER ()                         AS var_actual
                    FROM predictions
                    WHERE symbol        = :symbol
                      AND slot          = 'production'
                      AND actual_price  IS NOT NULL
                      AND target_ts     > now() - (:window_hours * INTERVAL '1 hour')
                      AND target_ts    <= now()
                )
                SELECT price_pred, price_q10, price_q90, actual, total, ss_res, var_actual
                FROM numbered
                WHERE rn % GREATEST(total / :limit, 1) = 0
                ORDER BY rn
                LIMIT :limit
            """),
            {"symbol": binance_symbol, "window_hours": window_hours, "limit": limit},
        )
        rows = result.mappings().all()

    # q10/q90 — nullable, как везде: point-only строки не имеют квантилей
    # (см. app/db/models.py Prediction). Фронт красит in/out-of-band поверх
    # уже существующего scatter — без них тут нечем.
    points = [
        {
            "actual":    float(r["actual"]),
            "predicted": float(r["price_pred"]),
            "q10":       float(r["price_q10"]) if r["price_q10"] is not None else None,
            "q90":       float(r["price_q90"]) if r["price_q90"] is not None else None,
        }
        for r in rows
    ]

    r2 = None
    if rows:
        total, ss_res, var_actual = rows[0]["total"], rows[0]["ss_res"], rows[0]["var_actual"]
        ss_tot = (var_actual or 0) * total
        if ss_tot:
            r2 = round(1 - ss_res / ss_tot, 3)

    return {"points": points, "r2": r2}


@router.get("/{symbol:path}/scatter")
async def get_scatter(
    symbol: str,
    window: str = Query(default="24h"),
    limit: int = Query(default=200, ge=1, le=2000),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:scatter:{sym}:{window}:{limit}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_scatter(_binance_symbol(sym), window, limit)
    await _cache_set(cache_key, payload, _CHART_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /latency — перцентили inference_ms VDS2
# ──────────────────────────────────────────────────────────────────────────────

async def _compute_latency(binance_symbol: str, window: str) -> dict:
    """p50/p95/p99 inference_ms за window. В отличие от остальных окон в
    этом файле — БЕЗ JOIN на candles: inference_ms известен сразу в момент
    записи (predictor.py), не требует дождаться закрытия свечи ("резолва"
    прогноза), так что фильтруем по as_of_ts (когда опросили VDS2), не по
    target_ts (когда прогноз сбудется) — тут нечему сбываться, это факт
    про сам вызов инференса, не про его результат.

    horizon = 1 — inference_ms одно значение на весь /predict-вызов (все
    12 горизонтов разом, см. predictor.py._store_db), дублируется во все
    12 строк одного as_of_ts; без фильтра percentile считался бы по 12
    копиям каждого значения — на сам percentile равномерное 12-кратное
    дублирование не влияет, но n был бы обманчивым (тот же приём, что у
    sharpe в _window_metrics — см. её докстринг)."""
    window_hours = _parse_hours(window)

    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT
                    COUNT(inference_ms)                                       AS n,
                    percentile_cont(0.5)  WITHIN GROUP (ORDER BY inference_ms) AS p50,
                    percentile_cont(0.95) WITHIN GROUP (ORDER BY inference_ms) AS p95,
                    percentile_cont(0.99) WITHIN GROUP (ORDER BY inference_ms) AS p99
                FROM predictions
                WHERE symbol    = :symbol
                  AND slot      = 'production'
                  AND horizon   = 1
                  AND as_of_ts  > now() - (:window_hours * INTERVAL '1 hour')
                  AND as_of_ts <= now()
            """),
            {"symbol": binance_symbol, "window_hours": window_hours},
        )
        row = result.mappings().first()

    return {
        "window": window,
        "n":      row["n"] or 0,
        "p50":    round(row["p50"], 1) if row["p50"] is not None else None,
        "p95":    round(row["p95"], 1) if row["p95"] is not None else None,
        "p99":    round(row["p99"], 1) if row["p99"] is not None else None,
    }


@router.get("/{symbol:path}/latency")
async def get_latency(
    symbol: str,
    window: str = Query(default="24h"),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:latency:{sym}:{window}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_latency(_binance_symbol(sym), window)
    await _cache_set(cache_key, payload, _CHART_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# /retrain-timeline — лог решений ретрейна (app.db.models.RetrainEvent),
# записывается через POST /admin/retrain-events. Отдельная, самостоятельная
# таблица — без JOIN на predictions/candles вообще.
# ──────────────────────────────────────────────────────────────────────────────

async def _compute_retrain_timeline(binance_symbol: str, limit: int) -> dict:
    """point/quantile — раздельные массивы, не одна общая лента: это две
    независимо ретрейнящиеся сущности с разным естественным ритмом (point —
    ежедневно, quantile — реже, см. app/db/models.py RetrainEvent) — общая
    лента вперемешку создавала бы ложное впечатление, что они идут в ногу.
    limit — на КАЖДУЮ из двух отдельно, не на сумму: иначе редкие
    quantile-события вымывались бы частыми point при общем лимите."""
    async with SessionLocal() as session:
        rows_by_kind = {}
        for kind in ("point", "quantile"):
            result = await session.execute(
                text("""
                    SELECT candidate_version, production_version, decision,
                           metric_name, candidate_value, production_value, threshold,
                           n_samples, train_wall_seconds, decided_at
                    FROM retrain_events
                    WHERE symbol = :symbol AND kind = :kind
                    ORDER BY decided_at DESC
                    LIMIT :limit
                """),
                {"symbol": binance_symbol, "kind": kind, "limit": limit},
            )
            rows_by_kind[kind] = result.mappings().all()

    def _serialize(row) -> dict:
        return {
            "candidate_version":  row["candidate_version"],
            "production_version": row["production_version"],
            "decision":           row["decision"],
            "metric_name":        row["metric_name"],
            "candidate_value":    row["candidate_value"],
            "production_value":   row["production_value"],
            "threshold":          row["threshold"],
            "n_samples":          row["n_samples"],
            "train_wall_seconds": row["train_wall_seconds"],
            "decided_at":         row["decided_at"].isoformat(),
        }

    return {
        "point":    [_serialize(r) for r in rows_by_kind["point"]],
        "quantile": [_serialize(r) for r in rows_by_kind["quantile"]],
    }


@router.get("/{symbol:path}/retrain-timeline")
async def get_retrain_timeline(
    symbol: str,
    limit: int = Query(default=8, ge=1, le=100),
) -> JSONResponse:
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    cache_key = f"metrics:retrain_timeline:{sym}:{limit}"
    cached = await _cache_get(cache_key)
    if cached is not None:
        return JSONResponse(cached)

    payload = await _compute_retrain_timeline(_binance_symbol(sym), limit)
    await _cache_set(cache_key, payload, _CHART_CACHE_TTL)
    return JSONResponse(payload)


# ──────────────────────────────────────────────────────────────────────────────
# Прогрев кэша по расписанию (app.scheduler.jobs) — основной путь записи.
# Пишет ровно те ключи, которые читают роуты выше, тем же _cache_set, но с
# длинным TTL. При 1000 одновременных пользователей каждый читает Redis;
# вычисление одно на цикл опроса, а не одно на HTTP-запрос.
# ──────────────────────────────────────────────────────────────────────────────

async def refresh_symbol_metrics(symbol_display: str) -> None:
    """symbol_display — отображаемая форма ('BTC/USDT'), как в DISPLAY_SYMBOLS
    и как её кладут в cache-ключи роуты выше."""
    binance_symbol = _binance_symbol(symbol_display)

    summary = await _compute_summary(binance_symbol)
    await _cache_set(f"metrics:summary:{symbol_display}", summary, SCHEDULED_CACHE_TTL)

    coverage = await _compute_quantile_coverage(binance_symbol)
    await _cache_set(f"metrics:coverage:{symbol_display}", coverage, SCHEDULED_CACHE_TTL)

    for metric, window, bucket in _HISTORY_TARGETS:
        payload = await _compute_history(binance_symbol, metric, window, bucket)
        key = f"metrics:history:{symbol_display}:{metric}:{window}:{bucket}"
        await _cache_set(key, payload, SCHEDULED_CACHE_TTL)

    for window, bucket in _COVERAGE_HISTORY_TARGETS:
        payload = await _compute_coverage_history(binance_symbol, window, bucket)
        key = f"metrics:coverage_history:{symbol_display}:{window}:{bucket}"
        await _cache_set(key, payload, SCHEDULED_CACHE_TTL)

    for window, bins in _ERRORS_TARGETS:
        payload = await _compute_errors(binance_symbol, window, bins)
        await _cache_set(f"metrics:errors:{symbol_display}:{window}:{bins}", payload, SCHEDULED_CACHE_TTL)

    for window, bins in _WIDTH_HISTOGRAM_TARGETS:
        payload = await _compute_width_histogram(binance_symbol, window, bins)
        await _cache_set(f"metrics:width_histogram:{symbol_display}:{window}:{bins}", payload, SCHEDULED_CACHE_TTL)

    for window, limit in _SCATTER_TARGETS:
        payload = await _compute_scatter(binance_symbol, window, limit)
        await _cache_set(f"metrics:scatter:{symbol_display}:{window}:{limit}", payload, SCHEDULED_CACHE_TTL)

    for window in _LATENCY_TARGETS:
        payload = await _compute_latency(binance_symbol, window)
        await _cache_set(f"metrics:latency:{symbol_display}:{window}", payload, SCHEDULED_CACHE_TTL)

    for limit in _RETRAIN_TIMELINE_TARGETS:
        payload = await _compute_retrain_timeline(binance_symbol, limit)
        await _cache_set(f"metrics:retrain_timeline:{symbol_display}:{limit}", payload, SCHEDULED_CACHE_TTL)
