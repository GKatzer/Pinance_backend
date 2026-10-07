"""Actualizer — публикует в Redis прогнозы, которые только что дозрели, и
заодно дописывает в БД их резолв (actual_price/actual_r/hit/sim_return).

До b6e2c4a8f1d3 здесь ничего в БД не писалось — "предсказано vs факт"
читалось JOIN'ом на каждый запрос (candles.ts = target_ts / as_of_ts). На
продовом объёме это стало главной причиной деградации (см. docstring
app.db.models.Prediction) — эта функция и так уже вычисляет actual_price/
actual_r/hit тем же JOIN'ом для live-ленты фронта, разница только в том,
что теперь результат ещё и пишется обратно в predictions по PK (point-
UPDATE, не скан), чтобы 10 read-сайтов дальше читали готовую колонку, а не
пересчитывали JOIN сами.

Дозревание считается для ОБОИХ slot (production и candidate) — админский
/admin/metrics/.../compare сравнивает их и тоже раньше JOIN'ил сам. В Redis
и на live-ленту фронта по-прежнему публикуется только slot='production' —
кандидат не должен туда попадать (см. ниже), это ограничение касается
только публикации, не UPDATE'а.

Вызывается из app.scheduler.jobs после каждого опроса VDS2. Публикует/пишет
только строки, дозревшие с прошлого запуска (watermark в Redis, per-символ) —
иначе на каждом цикле пришлось бы republish'ить всю историю заново, ведь
target_ts <= now() остаётся истинным навсегда после созревания строки.
Историю до этой миграции разово бэкафиллит app/backfill/resolve_predictions.py.

Верхняя граница окна — не настенные часы, а MAX(ts) из candles для символа.
Настенное время (datetime.now()) течёт независимо от того, дозаписалась ли
уже свеча в БД — WS-консьюмер пишет её только после закрытия, обычно с
небольшой задержкой. Раньше здесь стоял datetime.now(UTC): если actualizer
успевал пробежать цикл ПОСЛЕ того, как по часам T+5 уже наступило, но ДО того
как свеча ts=T физически попала в candles, watermark всё равно продвигался
до этого "now" — и target_ts=T терялся НАВСЕГДА (condition target_ts > since
после этого никогда больше не станет true для этого T), даже когда свеча
секундами позже появлялась. Использование MAX(ts) вместо часов гарантирует,
что watermark никогда не обгонит то, что реально есть в БД.

Только slot = 'production': если сейчас идёт shadow-сравнение кандидата
(см. app.inference.predictor.predict_candle(shadow=True)), в predictions на те же
(symbol, as_of_ts, horizon) есть вторая строка кандидата (slot='candidate')
— она не должна попасть в live-ленту фронта.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta

import structlog
from sqlalchemy import text

from app.core.redis import get_redis
from app.db.session import SessionLocal
from app.inference.registry import to_binance

log = structlog.get_logger()

# Первый запуск для символа — нет watermark'а в Redis, не льём всю историю.
_BOOTSTRAP_LOOKBACK_MINUTES = 15
_WATERMARK_KEY = "actualizer:watermark:{symbol}"
_WATERMARK_TTL = 7 * 24 * 3600  # неделя — переживает рестарты, не висит вечно

# Prediction Log на фронте: не фильтруется по горизонту (в отличие от
# pred_history) — на каждую дозревшую свечу пишется до 12 строк (по одной на
# горизонт). 240 = 12 горизонтов × 20 свечей, чтобы фронту хватало полных 20
# свечей истории, а не обрубок на середине горизонтов последней свечи.
# Используется и здесь (LTRIM), и в app.api.stream (LRANGE) — держим один
# источник правды на размер, а не два независимых числа, которые могут разъехаться.
RESULTS_RECENT_MAXLEN = 240


async def actualize_due_predictions(symbol: str) -> int:
    """Находит прогнозы, дозревшие с прошлого запуска, дописывает им резолв
    в БД (оба slot) и публикует production-часть в Redis.

    "Дозревший" = у target_ts уже есть закрытая свеча в candles. Окно —
    (watermark, latest_candle_ts], watermark хранится в Redis per-символ (один
    на оба slot — дозревание зависит только от наличия свечи, не от slot) и
    продвигается после каждого успешного прогона: так строка обрабатывается
    ровно один раз, а пропущенный цикл (рестарт, сбой) само-заживает на
    следующем прогоне более широким окном, а не теряет строки и не
    дублирует публикацию. Верхняя граница — MAX(ts) из candles, не часы
    (см. docstring модуля — почему это принципиально).

    Returns:
        Число production-строк, дозревших и опубликованных в этом прогоне
        (как и раньше — candidate-строки резолвятся молча, без публикации).
    """
    redis = get_redis()
    binance_symbol = to_binance(symbol)

    async with SessionLocal() as session:
        latest = await session.execute(
            text("SELECT MAX(ts) AS latest_ts FROM candles WHERE symbol = :symbol"),
            {"symbol": binance_symbol},
        )
        now = latest.scalar()
        if now is None:
            return 0  # ни одной свечи для символа ещё нет — нечего актуализировать

        watermark_key = _WATERMARK_KEY.format(symbol=symbol)
        raw_watermark = await redis.get(watermark_key)
        since = (
            datetime.fromisoformat(raw_watermark) if raw_watermark
            else now - timedelta(minutes=_BOOTSTRAP_LOOKBACK_MINUTES)
        )

        # Без фильтра по slot — резолвим и production, и candidate одним
        # проходом (см. docstring модуля). Публикация в Redis ниже всё равно
        # берёт только production-подмножество.
        result = await session.execute(
            text("""
                SELECT
                    p.slot, p.as_of_ts, p.horizon, p.target_ts, p.r_pred, p.price_pred,
                    p.price_q10, p.price_q90,
                    p.model_version,
                    c1.close AS actual_price,
                    ln(c1.close / c0.close) AS actual_r,
                    (sign(p.r_pred) = sign(ln(c1.close / c0.close))) AS hit,
                    sign(p.r_pred) * ln(c1.close / c0.close) AS sim_return
                FROM predictions p
                JOIN candles c1 ON c1.symbol = p.symbol AND c1.ts = p.target_ts
                JOIN candles c0 ON c0.symbol = p.symbol AND c0.ts = p.as_of_ts
                WHERE p.symbol     = :symbol
                  AND p.target_ts  > :since
                  AND p.target_ts <= :now
            """),
            {"symbol": binance_symbol, "since": since, "now": now},
        )
        due = result.mappings().all()

        if due:
            # point-UPDATE по PK (symbol, as_of_ts, horizon, slot) — не скан,
            # окно и так маленькое (дельта с прошлого watermark).
            await session.execute(
                text("""
                    UPDATE predictions
                    SET actual_price = :actual_price,
                        actual_r     = :actual_r,
                        hit          = :hit,
                        sim_return   = :sim_return
                    WHERE symbol   = :symbol
                      AND as_of_ts = :as_of_ts
                      AND horizon  = :horizon
                      AND slot     = :slot
                """),
                [
                    {
                        "symbol":       binance_symbol,
                        "as_of_ts":     row["as_of_ts"],
                        "horizon":      row["horizon"],
                        "slot":         row["slot"],
                        "actual_price": row["actual_price"],
                        "actual_r":     row["actual_r"],
                        "hit":          row["hit"],
                        "sim_return":   row["sim_return"],
                    }
                    for row in due
                ],
            )
            await session.commit()

    await redis.set(watermark_key, now.isoformat(), ex=_WATERMARK_TTL)

    production_due = [row for row in due if row["slot"] == "production"]
    if not production_due:
        return 0

    log.info("actualizer.due", symbol=symbol, count=len(production_due))
    await _publish_results(symbol, production_due)
    return len(production_due)


async def resolve_filled_predictions(filled: list[tuple[str, datetime]]) -> int:
    """Дописывает резолв (actual_price/actual_r/hit/sim_return) строкам
    predictions для свечей, прогноз по которым был дозаписан с опозданием
    (gap-backfill планировщика: смена модели, сбой VDS2 и т.п.).

    actualize_due_predictions такие строки не видит: он берёт только
    target_ts > watermark, а у запоздавшей строки target_ts уже позади
    watermark'а — без этой функции она оставалась бы без результата
    навсегда. Здесь, наоборот, ограничение по as_of_ts конкретных свечей
    (point-lookup по индексу), без публикации в Redis — старые результаты
    не должны попадать в live-ленту фронта. Оба slot, как и в актуализаторе;
    строки, чья свеча-цель ещё не закрылась, JOIN просто не вернёт — их
    подберёт обычный актуализатор по watermark.

    filled — [(symbol в форме Binance, as_of_ts)]. Returns число обновлённых строк."""
    if not filled:
        return 0

    by_symbol: dict[str, list[datetime]] = {}
    for symbol_binance, as_of_ts in filled:
        by_symbol.setdefault(symbol_binance, []).append(as_of_ts)

    updated = 0
    async with SessionLocal() as session:
        for symbol_binance, as_of_list in by_symbol.items():
            result = await session.execute(
                text("""
                    UPDATE predictions p
                    SET actual_price = c1.close,
                        actual_r     = ln(c1.close / c0.close),
                        hit          = (sign(p.r_pred) = sign(ln(c1.close / c0.close))),
                        sim_return   = sign(p.r_pred) * ln(c1.close / c0.close)
                    FROM candles c1, candles c0
                    WHERE p.symbol      = :symbol
                      AND p.as_of_ts    = ANY(:as_of_list)
                      AND p.actual_price IS NULL
                      AND c1.symbol = p.symbol AND c1.ts = p.target_ts
                      AND c0.symbol = p.symbol AND c0.ts = p.as_of_ts
                """),
                {"symbol": symbol_binance, "as_of_list": as_of_list},
            )
            updated += result.rowcount
        await session.commit()
    return updated


async def _publish_results(symbol: str, due: list) -> None:
    """Публикует дозревшие результаты для live-метрик на фронте.

    price_q10/price_q90 — пробрасываются как есть, NULL для point-only строк
    (см. app/db/models.py Prediction) — фронт уже фоллбэчит на price_pred."""
    try:
        redis = get_redis()
        list_key = f"results:{symbol}:recent"
        pipe = redis.pipeline()
        for row in due:
            payload = json.dumps({
                "as_of_ts":      row["as_of_ts"].isoformat(),
                "horizon":       row["horizon"],
                "target_ts":     row["target_ts"].isoformat(),
                "r_pred":        row["r_pred"],
                "price_pred":    row["price_pred"],
                "price_q10":     row["price_q10"],
                "price_q90":     row["price_q90"],
                "actual_r":      row["actual_r"],
                "actual_price":  row["actual_price"],
                "hit":           row["hit"],
                "model_version": row["model_version"],
            })
            pipe.lpush(list_key, payload)
            pipe.publish(f"results:{symbol}", payload)
        pipe.ltrim(list_key, 0, RESULTS_RECENT_MAXLEN - 1)
        pipe.expire(list_key, 7200)    # TTL 2 часа
        await pipe.execute()
    except Exception as exc:
        log.warning("actualizer.redis_publish_failed", symbol=symbol, error=str(exc))
