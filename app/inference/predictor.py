"""VDS2 ML-инференс — push-клиент.

VDS1 (этот процесс) находит свечи без прогноза (app.scheduler.jobs.
sync_predictions — LEFT JOIN candles/predictions) и досылает их на VDS2 по
одной: `POST {ml_inference_url}/predict/{symbol}` с телом
`{"as_of_ts": ..., "candles": {...}}` — окном OHLCV, из которого VDS2 сам
считает признаки. Раньше (до 2026-08-25) было наоборот: GET без тела, VDS2
сам решал, что такое "текущая" свеча, опираясь на собственный независимый
фид. Проблема той схемы — VDS2 не хранит историю своих ответов, поэтому
пропущенный (сетевой сбой, рестарт) as_of_ts нельзя было спросить повторно:
следующий опрос возвращал прогноз уже для новой свечи, потерянная терялась
навсегда (см. историю incident 2026-08-13 в app/scheduler/jobs.py).

Push-контракт снимает это ограничение: `/predict` — чистая функция от
`(as_of_ts, candles)`, не от "текущего" состояния VDS2, поэтому повторный
запрос для одной и той же (возможно, старой) свечи в любой момент безопасен
и корректен. "Очередь" при этом не отдельная инфраструктура (Redis Stream и
т.п.), а просто разница между `candles` и `predictions` — см. jobs.py
sync_predictions.

Формат ответа VDS2 не изменился — точечный прогноз + 10%/90% квантили на 12
горизонтов:
    {
      "symbol": "BTCUSDT",
      "as_of_ts": "2026-07-23T13:30:00+00:00",
      "close": 65187.16,
      "model_version": "202608031714-7883e760",
      "quantile_model_version": "q-202607290002",
      "schema_version": "a1b2c3d4e5f6",
      "inference_ms": 42.7,
      "predictions": [
        {"horizon": 1, "target_ts": "...", "r_pred": 1.52e-05, "price_pred": 65188.15,
         "r_q10": -9e-04, "r_q90": 1.5e-03, "price_q10": 65129.7, "price_q90": 65285.3},
        ...  // 12 штук
      ]
    }

Гарантия со стороны VDS2: если квантили есть, то price_q10 <= price_pred <=
price_q90 (сортировка там, не перепроверяется здесь). Квантили опциональны
per-модель, не фиксированный контракт: если у текущего слота (production
или candidate) пуст/отсутствует quantile_levels, VDS2 отдаёт точечный
прогноз без ключей r_q10/r_q90/price_q10/price_q90 вообще — это валидное
состояние ("point-only"), не ошибка схемы (см. Pinance_ml_inference
models.quantiles_from_metadata). Парсинг ниже (_optional_float) читает их
через .get(), пишет NULL, если их нет.

model_version и quantile_model_version версионируются на VDS2 независимо
друг от друга (раздельные MinIO-артефакты, раздельный promote_candidate_point/
promote_candidate_quantiles на стороне Pinance_ML) — quantile_model_version
может обновиться (ретрейн корзины), пока model_version остаётся прежним, и
наоборот. VDS2 отдаёт data["quantile_model_version"] = null, когда у слота
нет квантильной корзины (point-only, тот же случай, что отсутствие
r_q10/r_q90/price_q10/price_q90 в predictions) — читается через .get(),
как и остальные опциональные поля здесь.

inference_ms — время всего /predict-вызова на VDS2 (весь набор из 12
горизонтов сразу, не один запрос на горизонт), едет в БД (predictions.
inference_ms — аддитивная колонка, см. app/db/models.py) для
GET /metrics/{symbol}/latency (percentile_cont по окну). feature_count
(сколько фич видит модель) VDS2 пока не присылает вообще — Redis-снимок
(_store_redis) уже читает data.get("feature_count") заранее, начнёт
заполняться сам, как только поле появится в ответе, без правок здесь.

Shadow deployment (см. README): /predict/{symbol}/shadow — тот же контракт
запроса и ответа, от кандидата, который VDS2 держит параллельно с
production-моделью. Пишется в ту же таблицу под slot='candidate'
(production — под slot='production'), но никогда не публикуется в Redis —
пользователь кандидата не видит. Включается ML_SHADOW_ENABLED.
data["model_version"]/data["quantile_model_version"] (реальные версии
point-модели и квантильной корзины) едут в БД отдельными от slot колонками —
см. app/db/models.py Prediction. data["schema_version"] (хэш списка фич —
см. Pinance_ML export_models.py _schema_version) в БД не идёт вообще, только
в Redis-снимок (_store_redis) — это факт про "что сейчас грузится", не
история отдельных прогнозов.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.core.redis import get_redis
from app.db.session import SessionLocal
from app.inference.registry import to_binance

log = structlog.get_logger()

REDIS_KEY_PREDICTION = "prediction:{symbol}"   # актуальный снимок — для SSE/снапшота фронта
REDIS_PRED_TTL = 600   # 10 минут: переживает один пропущенный опрос (цикл — 5 минут)

BTC_BINANCE_SYMBOL = "BTCUSDT"   # cross-asset фича для альткоинов (см. README "Признаки")

# Один быстрый повтор при сетевой ошибке (таймаут/обрыв/5xx) — чисто на
# случай транзиентного сетевого сбоя внутри одной попытки. Не единственная
# защита от потери свечи, как раньше: если и повтор не помог, jobs.
# sync_predictions найдёт эту же свечу на следующем тике снова (не выполнена
# запись в predictions => LEFT JOIN снова её вернёт) и попробует заново —
# сколько угодно раз, пока не получится, потому что запрос теперь
# самодостаточен (as_of_ts + окно), а не завязан на "текущее" состояние VDS2.
_FETCH_RETRY_ATTEMPTS = 2
_FETCH_RETRY_DELAY_S = 2.0

# Статусы, на которые повторять запрос бессмысленно — они детерминированы
# относительно уже отправленного тела: InsufficientHistoryError (у символа
# физически ещё нет ml_candle_window_size истории на этот as_of_ts — до
# следующего ретрая ничего не изменится, since as_of_ts фиксирован в
# прошлом) и AsOfMismatchError (последняя свеча в окне не совпала с
# as_of_ts — сигнал бага на нашей стороне сборки окна, не транзиентный сбой)
# оба маппятся VDS2 на 422. ModelNotLoadedError -> 404: слот пуст на VDS2
# прямо сейчас, но это МОЖЕТ измениться (модель ещё грузится/деплоится) —
# тоже не ретраим внутри одной попытки, но sync_predictions подхватит на
# следующем тике.
_NO_RETRY_STATUSES = frozenset({404, 422})


def _optional_float(raw: Any) -> float | None:
    """Квантили опциональны per-модель на стороне VDS2 (quantile_levels
    пуст/отсутствует у слота => point-only, это валидное состояние, не
    ошибка схемы — см. Pinance_ml_inference/models.quantiles_from_metadata)."""
    return float(raw) if raw is not None else None


# ──────────────────────────────────────────────────────────────────────────────
# Сборка окна свечей для тела запроса
# ──────────────────────────────────────────────────────────────────────────────

async def _fetch_candle_window(
    session: AsyncSession, symbol_binance: str, as_of_ts: datetime, limit: int,
) -> list[dict[str, Any]] | None:
    """Последние `limit` свечей symbol_binance с ts <= as_of_ts, по возрастанию.

    None, если строк меньше limit — либо у символа физически ещё нет такой
    истории (первые часы после появления в DISPLAY_SYMBOLS — см.
    app/config.py ml_gap_lookback_hours, почему это не зависает навечно),
    либо as_of_ts не нашёлся в candles (не должно случаться: вызывающая
    сторона всегда берёт as_of_ts из уже существующей строки candles, см.
    jobs.sync_predictions)."""
    result = await session.execute(
        text("""
            SELECT ts, open, high, low, close, volume
            FROM candles
            WHERE symbol = :symbol AND ts <= :as_of_ts
            ORDER BY ts DESC
            LIMIT :limit
        """),
        {"symbol": symbol_binance, "as_of_ts": as_of_ts, "limit": limit},
    )
    rows = result.mappings().all()
    if len(rows) < limit:
        return None

    return [
        {
            "ts":     row["ts"].replace(tzinfo=UTC).isoformat(),
            "open":   row["open"], "high": row["high"],
            "low":    row["low"],  "close": row["close"], "volume": row["volume"],
        }
        for row in reversed(rows)   # DESC -> ASC, старая -> as_of_ts последней
    ]


async def _build_candles_payload(
    symbol_binance: str, as_of_ts: datetime,
) -> dict[str, list[dict[str, Any]]] | None:
    """{binance_symbol: окно} (+ BTCUSDT отдельным ключом для альткоинов —
    cross-asset признак, см. README "BTC-return как признак для альткоинов").
    None, если своего окна или окна BTC не хватает (см. _fetch_candle_window)."""
    settings = get_settings()

    async with SessionLocal() as session:
        window = await _fetch_candle_window(session, symbol_binance, as_of_ts, settings.ml_candle_window_size)
        if window is None:
            log.warning("ml_inference.insufficient_history", symbol=symbol_binance, as_of_ts=as_of_ts.isoformat())
            return None

        candles = {symbol_binance: window}
        if symbol_binance != BTC_BINANCE_SYMBOL:
            btc_window = await _fetch_candle_window(session, BTC_BINANCE_SYMBOL, as_of_ts, settings.ml_candle_window_size)
            if btc_window is None:
                log.warning("ml_inference.insufficient_btc_history", as_of_ts=as_of_ts.isoformat())
                return None
            candles[BTC_BINANCE_SYMBOL] = btc_window

    return candles


# ──────────────────────────────────────────────────────────────────────────────
# HTTP-клиент к VDS2
# ──────────────────────────────────────────────────────────────────────────────

async def fetch_prediction(symbol: str, as_of_ts: datetime, *, shadow: bool = False) -> dict[str, Any] | None:
    """POST /predict/{symbol}[/shadow] на VDS2. symbol — отображаемая форма 'BTC/USDT'.

    Тело — {"as_of_ts": ..., "candles": {...}}, см. _build_candles_payload.
    shadow=True — тот же контракт, но от кандидата, который VDS2 держит
    параллельно с production-моделью (см. README: shadow deployment).

    Returns:
        Сырой JSON-ответ VDS2, либо None (нет данных для окна, детерминированный
        отказ VDS2 — 404/422, — или сетевая ошибка после ретрая).
    """
    settings = get_settings()
    binance_symbol = to_binance(symbol)

    candles = await _build_candles_payload(binance_symbol, as_of_ts)
    if candles is None:
        return None

    suffix = "/shadow" if shadow else ""
    url = f"{settings.ml_inference_url.rstrip('/')}/predict/{binance_symbol}{suffix}"
    body = {"as_of_ts": as_of_ts.replace(tzinfo=UTC).isoformat(), "candles": candles}

    data: dict[str, Any] | None = None
    for attempt in range(1, _FETCH_RETRY_ATTEMPTS + 1):
        try:
            async with httpx.AsyncClient(timeout=settings.ml_inference_timeout_s) as client:
                resp = await client.post(url, json=body)

            if resp.status_code in _NO_RETRY_STATUSES:
                log.warning(
                    "ml_inference.rejected", symbol=symbol, url=url,
                    status=resp.status_code, body=resp.text[:500],
                )
                return None

            resp.raise_for_status()
            data = resp.json()
            break
        except httpx.HTTPError as exc:
            if attempt < _FETCH_RETRY_ATTEMPTS:
                log.warning("ml_inference.fetch_failed_retrying", symbol=symbol, url=url, attempt=attempt, error=str(exc))
                await asyncio.sleep(_FETCH_RETRY_DELAY_S)
                continue
            log.warning("ml_inference.fetch_failed", symbol=symbol, url=url, attempt=attempt, error=str(exc))
            return None
        except ValueError as exc:  # невалидный JSON
            log.warning("ml_inference.bad_json", symbol=symbol, url=url, error=str(exc))
            return None

    if not data.get("predictions"):
        log.warning("ml_inference.empty_response", symbol=symbol, url=url)
        return None

    return data


# ──────────────────────────────────────────────────────────────────────────────
# Персистентность
# ──────────────────────────────────────────────────────────────────────────────

async def store_prediction(
    symbol: str, data: dict[str, Any], slot: str, model_version: str | None,
    quantile_model_version: str | None, *, publish: bool = True,
) -> None:
    """Пишет прогноз в Postgres (история, N строк) и, если publish=True, в Redis
    (снимок для фронта). publish=False — путь shadow-кандидата: он не должен
    быть виден пользователю, поэтому в Redis не попадает вообще, только в БД
    для последующего сравнения с production по одним и тем же исходам.

    slot — 'production' | 'candidate', ключевой дискриминатор (PK, все
    read-фильтры). model_version/quantile_model_version — реальные версии
    point-модели и квантильной корзины от VDS2, версионируются независимо
    друг от друга, чисто информационные (см. app/db/models.py Prediction).
    inference_ms/feature_count берутся из data напрямую внутри _store_db/
    _store_redis — не параметры этой функции, в отличие от версий (см. их
    докстринги, почему): версии влияют на то, КУДА публиковать (например,
    решение промоушена смотрит именно на них), latency и feature_count —
    чисто описательные, читаются одинаково для production и candidate.
    """
    if publish:
        await _store_redis(symbol, data)
    await _store_db(symbol, data, slot, model_version, quantile_model_version)


async def _store_redis(symbol: str, data: dict[str, Any]) -> None:
    """Актуальный прогноз одним JSON — для /snapshot и SSE-событий 'prediction'.

    model_version/quantile_model_version/schema_version едут сюда же (не
    только в БД) — это единственный публичный путь наружу для "какая модель
    сейчас обслуживает" (nav-плашка/футер/MLOps на фронте): /admin/metrics/.../
    compare не проксируется наружу (см. app/api/admin.py), а этот payload и
    так уже публичный через /snapshot и SSE. quantile_model_version — тот же
    принцип, что и у schema_version рядом: nullable, для point-only слотов
    (см. app/db/models.py Prediction — версионируется независимо от
    model_version). Живёт только тут, в Redis с TTL — это состояние "сейчас",
    не история, БД (predictions.quantile_model_version) для этого не трогаем.

    feature_count — сколько фич видит модель прямо сейчас (BACKEND_REQUIREMENTS.md
    фронта: захардкожено 124 в трёх местах UI, просили отдать тем же полем,
    что версию). VDS2 пока это поле не шлёт (данных нет ни у кого) — читаем
    через .get(), как и всё опциональное здесь: как только Pinance_ml_inference
    его добавит в /predict, начнёт приходить само, без правок здесь."""
    payload = {
        "symbol":                  symbol,
        "as_of_ts":                data["as_of_ts"],
        "close":                   data["close"],
        "model_version":           data.get("model_version"),
        "quantile_model_version":  data.get("quantile_model_version"),
        "schema_version":          data.get("schema_version"),
        "feature_count":           data.get("feature_count"),
        "predictions":             data["predictions"],
    }
    raw = json.dumps(payload)
    redis = get_redis()
    await redis.set(REDIS_KEY_PREDICTION.format(symbol=symbol), raw, ex=REDIS_PRED_TTL)
    await redis.publish(f"predictions:{symbol}", raw)


async def _store_db(
    symbol: str, data: dict[str, Any], slot: str,
    model_version: str | None, quantile_model_version: str | None,
) -> None:
    """Upsert по (symbol, as_of_ts, horizon, slot) — одна строка на горизонт
    на слот. slot в ключе — то, что позволяет production и shadow-кандидату
    писать в одну таблицу под одним и тем же (symbol, as_of_ts, horizon), не
    затирая друг друга. model_version/quantile_model_version едут вместе со
    строкой, но не в ключе — только для трейсабилити (admin.compare),
    версионируются независимо друг от друга (см. app/db/models.py Prediction).

    symbol пишем в форме Binance ('BTCUSDT'), как и candles.symbol — иначе
    JOIN predictions.symbol = candles.symbol в actualizer/pred_history
    ничего не находит. Redis-снимок (_store_redis) — отдельно, в display-форме,
    её ждёт SSE-подписчик во app/api/stream.py.

    as_of_ts берём из ответа VDS2 (data["as_of_ts"]), не из аргумента вызова
    fetch_prediction — VDS2 теперь сам проверяет совпадение (AsOfMismatchError,
    422, не долетает сюда), так что они гарантированно совпадают, но именно
    ответ — источник истины для того, что реально было посчитано.

    Никакой цены "на момент прогноза" тут не храним — она уже есть в
    candles.close при ts = as_of_ts, читается JOIN'ом при необходимости.

    inference_ms — время всего /predict-вызова на VDS2 (все 12 горизонтов
    разом, см. Pinance_ml_inference predict.py), не время одного горизонта —
    как и model_version, одно значение на весь ответ, дублируется во все
    12 строк (GET /metrics/{symbol}/latency потом фильтрует horizon=1,
    чтобы не считать его 12 раз). NULL для старых билдов VDS2 без этого
    поля — читаем через .get(), тот же принцип, что везде здесь.
    """
    as_of_ts = datetime.fromisoformat(data["as_of_ts"]).replace(tzinfo=None)
    inference_ms = _optional_float(data.get("inference_ms"))

    rows = [
        {
            "symbol":        to_binance(symbol),
            "as_of_ts":      as_of_ts,
            "horizon":       int(p["horizon"]),
            "target_ts":     datetime.fromisoformat(p["target_ts"]).replace(tzinfo=None),
            "r_pred":        float(p["r_pred"]),
            "price_pred":    float(p["price_pred"]),
            "r_q10":         _optional_float(p.get("r_q10")),
            "r_q90":         _optional_float(p.get("r_q90")),
            "price_q10":     _optional_float(p.get("price_q10")),
            "price_q90":     _optional_float(p.get("price_q90")),
            "slot":          slot,
            "model_version": model_version,
            "quantile_model_version": quantile_model_version,
            "inference_ms":  inference_ms,
        }
        for p in data["predictions"]
    ]

    async with SessionLocal() as session:
        await session.execute(
            text("""
                INSERT INTO predictions
                    (symbol, as_of_ts, horizon, target_ts,
                     r_pred, price_pred, r_q10, r_q90, price_q10, price_q90,
                     slot, model_version, quantile_model_version, inference_ms)
                VALUES
                    (:symbol, :as_of_ts, :horizon, :target_ts,
                     :r_pred, :price_pred, :r_q10, :r_q90, :price_q10, :price_q90,
                     :slot, :model_version, :quantile_model_version, :inference_ms)
                ON CONFLICT (symbol, as_of_ts, horizon, slot) DO UPDATE SET
                    target_ts               = EXCLUDED.target_ts,
                    r_pred                  = EXCLUDED.r_pred,
                    price_pred              = EXCLUDED.price_pred,
                    r_q10                   = EXCLUDED.r_q10,
                    r_q90                   = EXCLUDED.r_q90,
                    price_q10               = EXCLUDED.price_q10,
                    price_q90               = EXCLUDED.price_q90,
                    model_version           = EXCLUDED.model_version,
                    quantile_model_version  = EXCLUDED.quantile_model_version,
                    inference_ms            = EXCLUDED.inference_ms
            """),
            rows,
        )
        await session.commit()

    log.info(
        "ml_inference.stored",
        symbol=symbol, as_of_ts=data["as_of_ts"], horizons=len(rows), slot=slot,
        model_version=model_version, quantile_model_version=quantile_model_version,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Точка входа для scheduler-джобы
# ──────────────────────────────────────────────────────────────────────────────

async def predict_candle(symbol: str, as_of_ts: datetime, *, shadow: bool = False) -> bool:
    """Запрашивает и сохраняет прогноз для одной конкретной свечи (as_of_ts).

    Вызывается из app.scheduler.jobs.sync_predictions на каждую найденную
    дыру (candles без соответствующей строки в predictions) — не "текущую"
    свечу, любую, сколько бы времени ни прошло с её закрытия: запрос теперь
    самодостаточен (as_of_ts + окно), результат от него не зависит от того,
    когда именно он ушёл.

    slot — 'production' (публикуется в Redis, видно фронту) или 'candidate'
    (shadow, только БД — см. README: shadow deployment). Дискриминатор PK,
    не зависит от того, что VDS2 прислал в data["model_version"]: версию
    доверяем VDS2 как есть и просто прокидываем, она едет в БД отдельной,
    не-ключевой колонкой, для admin.compare.

    Returns:
        True если прогноз получен и сохранён, False при отказе VDS2/нехватке
        истории/сетевой ошибке — дыра просто останется дырой до следующего
        тика sync_predictions.
    """
    data = await fetch_prediction(symbol, as_of_ts, shadow=shadow)
    if data is None:
        return False
    slot = "candidate" if shadow else "production"
    await store_prediction(
        symbol, data, slot, data.get("model_version"), data.get("quantile_model_version"), publish=not shadow,
    )
    return True
