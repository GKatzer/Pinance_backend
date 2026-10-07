"""Внутренние эндпоинты — НЕ для публичного UI.

Специально НЕ под /metrics и НЕ проксируются Caddy'ем на pinance.katzer.ru:
@api-матчер там — только /candles*, /stream*, /snapshot*, /health*,
/ready*, /metrics*. /admin/* под него не попадает, значит через публичный
домен недостижим вообще — только напрямую на 127.0.0.1:8002 (с самого
VDS1, по SSH/Tailscale). Если понадобится доступ снаружи VDS1 — это explicit
Caddy-конфиг с чем-то посильнее security-through-obscurity, не "просто
не добавили в матчер".

/admin/metrics/{symbol}/compare — production vs shadow-кандидат на одних
и тех же реализовавшихся исходах, GROUP BY (slot, model_version) вместо
жёсткого WHERE slot = 'production' в app.api.metrics (см. app/db/models.py
Prediction про разделение slot/model_version). Группировка именно по паре,
не по одному slot — point-ретрейн на стороне Pinance_ML ежедневный, так что
широкое окно (7d и шире) почти гарантированно застанет несколько разных
model_version подряд в одном и том же slot; тому, кто принимает решение о
промоушене, нужна метрика ИМЕННО текущей версии candidate, а не средняя
по всем версиям, что успели тут перебывать за окно.

Квантильная корзина версионируется на VDS2 независимо от point-модели
(quantile_model_version, отдельная колонка — см. app/db/models.py
Prediction) — поэтому q10_coverage/q90_coverage под каждым окном едут
отдельным ключом "quantiles", сгруппированным по (slot,
quantile_model_version), а не внутри point-версионированных
production/candidate — та же логика "почти гарантированно расходятся по
версии внутри широкого окна", что и у point-модели, только для своего
собственного, независимого цикла ретрейна.

Потребитель — Pinance_ML/scripts/promote_if_better.py: тянет отсюда метрику
конкретной (slot='candidate', model_version=<то, что сейчас реально лежит
в MinIO>), сверяет с production, и если проходит гейт — сам, локально (тут
MinIO-креды никогда не хранились и не будут), зовёт
pinance_ml.model_storage.promote_candidate_point и/или
promote_candidate_quantiles (независимые решения — см. этого скрипта
docstring). Здесь — только цифры, никакого решения.

?window=<label> (опционально, один из app.api.metrics.SUMMARY_WINDOWS) —
посчитать и отдать только это окно, не все пять через asyncio.gather разом.
promote_if_better.py читает ровно одно окно (PROMOTION_WINDOW, обычно '7d'),
но без параметра эндпоинт всё равно исторически считал все пять, включая
'all' (SUMMARY_WINDOWS['all'] = 10 лет — предел ставит по факту retention
predictions, не сам запрос) — и promote_if_better.py платил временем именно
за него, хотя он ему не нужен. Без параметра — прежнее поведение (все окна),
для ручной проверки в браузере/curl это по-прежнему удобнее.

Без кэша, в отличие от app.api.metrics: трафик сюда — человек (или
promotion-джоба, нечасто — раз в 6 часов) изредка, не 1000 параллельных
пользователей. Здесь свежесть важнее экономии на вычислении.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text

from app.api.metrics import SUMMARY_WINDOWS
from app.core.redis import get_redis
from app.db.session import SessionLocal
from app.inference.registry import normalize as _normalize
from app.inference.registry import to_binance as _binance_symbol

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/metrics", tags=["admin"])

# Отдельный роутер под голым /admin (не /admin/metrics) — POST /retrain-events
# это не метрика-на-чтение, а запись лога решений, ей нечего делать под
# /metrics-неймспейсом compare/summary/history и т.п. Тот же непубличный
# периметр, что у router выше (см. docstring модуля) — просто другой
# префикс. main.py подключает оба (см. app.api.stream.routers — тот же
# приём для файла с >1 роутером).
_RETRAIN_ROUTER = APIRouter(prefix="/admin", tags=["admin"])

routers = [router, _RETRAIN_ROUTER]


async def _compare_window(binance_symbol: str, hours: float) -> dict:
    """Те же метрики, что _window_metrics в app.api.metrics, но GROUP BY
    (slot, model_version) вместо фильтра на 'production' — production и
    candidate с уже дозревшими строками появляются оба, и каждая версия
    внутри слота — отдельной строкой (см. docstring модуля, почему по
    версии, не только по слоту).

    q10_coverage/q90_coverage — то же самое живое покрытие квантильной
    корзины, что _avg_pinball_and_coverage_ok в
    Pinance_ML/scripts/auto_retrain_quantiles.py считает офлайн на holdout,
    здесь — на реально проверившихся продовых/shadow исходах; без этого
    promote_if_better.py не может отличить честную квантильную корзину от
    завышенно-широкой. VDS2 версионирует point-модель и квантильную корзину
    НЕЗАВИСИМО (см. app/db/models.py Prediction.quantile_model_version) —
    point-ретрейн ежедневный, квантильный реже, так что оба в одном и том
    же slot почти гарантированно расходятся по версии в любом окне шире
    суток. Раньше coverage считался в той же строке, что GROUP BY (slot,
    model_version) — то есть неявно и неверно привязан к point-версии, а не
    к версии корзины, которая эту квантильную ширину и определяет. Здесь —
    отдельная агрегация той же resolved-выборки, GROUP BY (slot,
    quantile_model_version), под ключом "quantiles" в результате; её n —
    именно количество строк с непустой корзиной под этой версией, не то же
    самое n, что у point-строки того же slot.

    resolved читает готовые actual_price/hit (не JOIN, как было до
    b6e2c4a8f1d3) — actualizer дозаписывает их для ОБОИХ slot именно ради
    этого запроса: он единственный без фильтра slot='production' (сравнивает
    production и candidate), поэтому был одним из самых дорогих потребителей
    JOIN'а здесь наравне с /summary "all" (см. app/db/models.py Prediction)."""
    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                WITH resolved AS (
                    SELECT
                        slot, model_version, quantile_model_version,
                        price_pred, actual_price,
                        r_q10, r_q90, price_q10, price_q90, hit
                    FROM predictions
                    WHERE symbol       = :symbol
                      AND actual_price IS NOT NULL
                      AND target_ts    > now() - (:hours * INTERVAL '1 hour')
                      AND target_ts   <= now()
                ),
                point_stats AS (
                    SELECT
                        slot, model_version,
                        COUNT(*)                                        AS n,
                        (AVG(hit::int))::float8 * 100                   AS directional_accuracy,
                        AVG(ABS(price_pred - actual_price))             AS mae,
                        sqrt(AVG(POWER(price_pred - actual_price, 2)))  AS rmse
                    FROM resolved
                    GROUP BY slot, model_version
                ),
                quantile_stats AS (
                    SELECT
                        slot, quantile_model_version,
                        COUNT(*)                                         AS n,
                        (AVG((actual_price < price_q10)::int))::float8   AS q10_coverage,
                        (AVG((actual_price < price_q90)::int))::float8   AS q90_coverage
                    FROM resolved
                    WHERE r_q10 IS NOT NULL AND r_q90 IS NOT NULL
                    GROUP BY slot, quantile_model_version
                )
                SELECT 'point' AS kind, slot, model_version AS version,
                       n, directional_accuracy, mae, rmse,
                       NULL::float8 AS q10_coverage, NULL::float8 AS q90_coverage
                FROM point_stats
                UNION ALL
                SELECT 'quantile' AS kind, slot, quantile_model_version AS version,
                       n, NULL::float8, NULL::float8, NULL::float8,
                       q10_coverage, q90_coverage
                FROM quantile_stats
            """),
            {"symbol": binance_symbol, "hours": hours},
        )
        rows = result.mappings().all()

    out: dict[str, dict] = {}
    for row in rows:
        version = row["version"] or "unknown"
        if row["kind"] == "point":
            out.setdefault(row["slot"], {})[version] = {
                "n":                    row["n"],
                "directional_accuracy": round(row["directional_accuracy"], 1)
                                         if row["directional_accuracy"] is not None else None,
                "mae":                  round(row["mae"], 2) if row["mae"] is not None else None,
                "rmse":                 round(row["rmse"], 2) if row["rmse"] is not None else None,
            }
        else:
            out.setdefault("quantiles", {}).setdefault(row["slot"], {})[version] = {
                "n":            row["n"],
                "q10_coverage": round(row["q10_coverage"], 4) if row["q10_coverage"] is not None else None,
                "q90_coverage": round(row["q90_coverage"], 4) if row["q90_coverage"] is not None else None,
            }
    return out


@router.get("/{symbol:path}/compare")
async def get_compare(
    symbol: str,
    window: str | None = Query(default=None, description="Один label из SUMMARY_WINDOWS — посчитать только его, не все пять"),
) -> JSONResponse:
    """production vs shadow-кандидат(ы) — по умолчанию все стандартные окна
    сразу (см. app.api.metrics.SUMMARY_WINDOWS), для ручной оценки перед
    pinance_ml.model_storage.promote_candidate_point/promote_candidate_quantiles;
    с ?window=<label> — только оно, дешевле (см. docstring модуля). Каждое
    окно — {"production": {...}, "candidate": {...}} с point-метриками по
    model_version, плюс, если для окна есть проверившиеся квантильные
    строки, "quantiles": {"production": {...}, "candidate": {...}} с
    q10_coverage/q90_coverage по quantile_model_version — отдельная,
    независимая от point, группировка (см. docstring модуля). Отсутствие
    ключа (слота, версии или всего "quantiles") под окном = ни одна версия
    ещё не имеет дозревших строк в этом окне, не ошибка."""
    sym = _normalize(symbol)
    if not sym:
        raise HTTPException(status_code=400, detail="Invalid symbol")
    binance_symbol = _binance_symbol(sym)

    if window is not None:
        if window not in SUMMARY_WINDOWS:
            raise HTTPException(status_code=400, detail=f"Invalid window: {window!r}, expected one of {tuple(SUMMARY_WINDOWS)}")
        labels = (window,)
    else:
        labels = tuple(SUMMARY_WINDOWS.keys())

    results = await asyncio.gather(
        *(_compare_window(binance_symbol, SUMMARY_WINDOWS[label]) for label in labels)
    )

    return JSONResponse({
        "symbol":     sym,
        "windows":    dict(zip(labels, results)),
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    })


# ──────────────────────────────────────────────────────────────────────────────
# POST /retrain-events — лог решений ретрейна (см. app/db/models.py RetrainEvent)
# ──────────────────────────────────────────────────────────────────────────────

class RetrainEventIn(BaseModel):
    """Тело POST /admin/retrain-events — один вызов на одно решение
    (promoted/rejected), от Pinance_ML/scripts/promote_if_better.py и
    auto_retrain.py/auto_retrain_quantiles.py (decide_point_promotion/
    decide_quantile_promotion уже возвращают ровно эти составляющие
    решения — decision_label, метрика, кандидат/прод значения, порог).
    Поля 1:1 с app/db/models.py RetrainEvent — см. её докстринг."""

    kind: Literal["point", "quantile"]
    symbol: str
    candidate_version: str | None = Field(default=None, max_length=64)
    production_version: str | None = Field(default=None, max_length=64)
    decision: Literal["promoted", "rejected"]
    metric_name: str | None = Field(default=None, max_length=128)
    candidate_value: float | None = None
    production_value: float | None = None
    threshold: float | None = None
    n_samples: int | None = None
    train_wall_seconds: float | None = None
    decided_at: datetime

    @field_validator("decided_at")
    @classmethod
    def _naive_utc(cls, v: datetime) -> datetime:
        """БД хранит наивные UTC-datetime, как и везде в проекте (см.
        predictor.py._store_db) — decided_at:'...Z' от вызывающей стороны
        парсится Pydantic'ом в aware datetime, asyncpg такой в naive
        TIMESTAMP-колонку не запишет ('can't subtract offset-naive and
        offset-aware datetimes'). Приводим к UTC и снимаем tzinfo здесь,
        а не заставляем каждого вызывающего думать об этом самому."""
        return v.astimezone(UTC).replace(tzinfo=None) if v.tzinfo is not None else v


@_RETRAIN_ROUTER.post("/retrain-events", status_code=201)
async def create_retrain_event(event: RetrainEventIn) -> JSONResponse:
    """Пишет одну строку в retrain_events — чисто append-only лог, без
    upsert'а: каждое решение (в т.ч. повторные rejected на одном и том же
    кандидате следующим циклом) — отдельная, неизменяемая запись, это и
    есть история для RETRAIN TIMELINE (GET /metrics/{symbol}/retrain-timeline).

    symbol пишем как прислали, без нормализации через app.inference.registry
    (форма Binance, как в predictions.symbol/candles.symbol) — вызывающая
    сторона доверенная (тот же периметр, что у остального /admin/*, см.
    docstring модуля), не публичный HTTP-ввод."""
    async with SessionLocal() as session:
        await session.execute(
            text("""
                INSERT INTO retrain_events
                    (kind, symbol, candidate_version, production_version, decision,
                     metric_name, candidate_value, production_value, threshold,
                     n_samples, train_wall_seconds, decided_at)
                VALUES
                    (:kind, :symbol, :candidate_version, :production_version, :decision,
                     :metric_name, :candidate_value, :production_value, :threshold,
                     :n_samples, :train_wall_seconds, :decided_at)
            """),
            event.model_dump(),
        )
        await session.commit()

    # GET /metrics/{symbol}/retrain-timeline кэширует на SCHEDULED_CACHE_TTL
    # (15 минут) — без явной инвалидации здесь только что записанное решение
    # было бы не видно на фронте до следующего цикла refresh_metrics_cache.
    # scan_iter, не KEYS — не блокирует Redis, событие пишется не на
    # hot-path, но привычка та же. sym_display может быть '' на неизвестном
    # символе (registry.normalize не знает form'ы за пределами
    # DISPLAY_SYMBOLS) — тогда просто нечего инвалидировать, не ошибка.
    sym_display = _normalize(event.symbol)
    # Строка уже закоммичена — недоступный Redis не должен превращать
    # успешную запись в 500 (вызывающая сторона ретраила бы и плодила
    # дубли в append-only логе). Кэш и так истечёт по TTL.
    if sym_display:
        try:
            redis = get_redis()
            async for key in redis.scan_iter(match=f"metrics:retrain_timeline:{sym_display}:*"):
                await redis.delete(key)
        except Exception:
            logger.warning(
                "retrain-events: не удалось инвалидировать кэш retrain_timeline для %s",
                sym_display, exc_info=True,
            )

    return JSONResponse({"status": "created"}, status_code=201)
