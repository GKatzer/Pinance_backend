"""APScheduler — backfill прогнозов + отдельная джоба прогрева кэша.

Раз в ML_POLL_INTERVAL_MINUTES минут (по умолчанию 5): ищем свечи в `candles`,
для которых ещё нет строки в `predictions` (LEFT JOIN, см. _find_missing), и
досылаем их на VDS2 по одной (predictor.predict_candle), затем актуализируем
прогнозы, чей target_ts уже наступил (actualizer). sync_predictions —
единственный писатель в predictions, должен быть дешёвым и надёжным.

До 2026-08-25 здесь был другой принцип: раз в тик спрашивали у VDS2 "что
сейчас актуально" (GET без тела) — если тик срывался (VDS2 недоступен, сеть),
пропущенная свеча терялась навсегда, потому что VDS2 не хранил историю своих
ответов и на следующем тике отвечал уже за новую свечу (см. incident ниже).
Теперь /predict — чистая функция от (as_of_ts, окно свечей), которую
собирает и шлёт сам VDS1 (app.inference.predictor) — поэтому "очередь" не
нужно городить отдельно: это просто разница между candles и predictions,
и повторный запрос для той же дыры в любой момент безопасен. Пропущенный
тик здесь больше не значит потерянные строки — на следующем тике та же дыра
снова найдётся тем же LEFT JOIN'ом и будет обработана.

VDS2 — read-only и stateless (ограничение "5 ГБ диска — не хранилище"),
поэтому вся запись идёт отсюда, единственного владельца TimescaleDB/Redis.

Shadow deployment (ML_SHADOW_ENABLED): отдельная джоба ищет дыры в slot=
'candidate' и досылает их на /predict/{symbol}/shadow — для сравнения с
production (slot='production') на одних и тех же исходах. Выключена по
умолчанию: включать только пока реально идёт сравнение кандидата.

Пересчёт кэша /metrics/* (app.api.metrics), /candles/{symbol}/pred_history,
дефолтного таймфрейма 1D и первой страницы /prediction_log — refresh_metrics_
cache, ОТДЕЛЬНАЯ джоба на своём интервале (METRICS_REFRESH_INTERVAL_MINUTES,
см. app/config.py), НЕ часть sync_predictions. Это единственное место, где
эти агрегаты вообще считаются проактивно; сами роуты только читают Redis
(с лениво-fallback пересчётом на промах, см. докстринги соответствующих
_from_*/_compute_* функций).

Incident 2026-08-13: раньше refresh был частью основной джобы приёма
прогнозов, под тем же max_instances=1/coalesce=True. Пока predictions был
вдвое меньше (только slot='production'), дорогой JOIN predictions×candles по
5 окнам × 4 символа укладывался в ML_POLL_INTERVAL_MINUTES с запасом. После
включения ML_SHADOW_ENABLED объём строк удвоился (candidate пишет наравне с
production) — цикл стал занимать 6-9 минут при бюджете в 1 минуту.
APScheduler с coalesce=True не ретраит пропущенные тики, а просто их
скипает — значит именно на этих тиках приём прогнозов для slot='production'
не выполнялся вообще, но соседняя shadow-джоба (лёгкая, без refresh) ни разу
не пропускалась — отсюда систематическая асимметрия production/candidate,
~20-25% production-строк молча пропадало на всех символах с 2026-08-13.
Разведение на две джобы — не оптимизация, а фикс: тяжёлый пересчёт кэша
теперь может skip'нуться сам на себя сколько угодно (стухший кэш на лишний
интервал — не потеря), не трогая приём прогнозов вообще. Сам переход на
backfill (2026-08-25, см. выше) устраняет и другую половину того же
инцидента — то, что пропущенный тик вообще что-то теряет.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog
from apscheduler.events import EVENT_JOB_MAX_INSTANCES, EVENT_JOB_MISSED, JobExecutionEvent, JobSubmissionEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from app.api.candles import refresh_symbol_candles, refresh_symbol_pred_history, refresh_symbol_prediction_log
from app.api.metrics import refresh_symbol_metrics
from app.config import get_settings
from app.db.session import SessionLocal
from app.inference import predictor
from app.inference.registry import DISPLAY_SYMBOLS, normalize, to_binance
from app.ingest.actualizer import actualize_due_predictions, resolve_filled_predictions

log = structlog.get_logger()

_JOB_ID = "sync_ml_predictions"
_REFRESH_JOB_ID = "refresh_metrics_cache"
_SHADOW_JOB_ID = "sync_ml_predictions_shadow"
_POLL_OFFSET_SECONDS = 10          # даём WS-консьюмеру время дозаписать свежую свечу перед сканом
_SHADOW_POLL_OFFSET_SECONDS = 15   # чуть позже прод-скана — не бьём VDS2 одним и тем же тиком
_REFRESH_MISFIRE_GRACE_SECONDS = 300  # тяжёлая джоба, отставание на минуты — норма, не повод скипать совсем

_scheduler: AsyncIOScheduler | None = None


async def _find_missing(slot: str) -> list[tuple[str, datetime]]:
    """Свечи из candles, для которых ещё нет строки в predictions под данным
    slot — простой LEFT JOIN, без отдельной очереди: разница между двумя
    таблицами и есть бэклог. Bounded по ml_gap_lookback_hours (см.
    app/config.py, почему без этой границы дыра у самого начала истории
    символа стучалась бы в VDS2 вечно). symbol в возврате — форма Binance
    (как в candles.symbol), ORDER BY ts ASC — старые дыры обрабатываются
    первыми."""
    settings = get_settings()
    since = datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=settings.ml_gap_lookback_hours)
    symbols = [to_binance(s) for s in DISPLAY_SYMBOLS]

    async with SessionLocal() as session:
        result = await session.execute(
            text("""
                SELECT c.symbol, c.ts
                FROM candles c
                LEFT JOIN predictions p
                    ON p.symbol = c.symbol AND p.as_of_ts = c.ts AND p.slot = :slot
                WHERE p.as_of_ts IS NULL
                  AND c.symbol = ANY(:symbols)
                  AND c.ts >= :since
                ORDER BY c.ts ASC
            """),
            {"slot": slot, "symbols": symbols, "since": since},
        )
        return [(row["symbol"], row["ts"]) for row in result.mappings()]


async def _resolve_filled(filled: list[tuple[str, datetime]]) -> None:
    """Резолв дозаписанных с опозданием прогнозов (см. actualizer.
    resolve_filled_predictions — почему их не подберёт watermark). Ошибка
    здесь не должна ронять тик: строки останутся без резолва, но прогнозы
    уже сохранены."""
    try:
        n = await resolve_filled_predictions(filled)
        if n:
            log.info("scheduler.late_predictions_resolved", rows=n)
    except Exception as exc:
        log.error("scheduler.resolve_filled_failed", error=str(exc))


async def sync_predictions() -> None:
    """Находит production-дыры и досылает их на VDS2 по одной, затем
    актуализирует прогнозы каждого символа. Ошибка одной свечи не должна
    ронять обработку остальных.

    Намеренно НЕ делает здесь refresh кэша метрик (см. docstring модуля,
    incident 2026-08-13) — это refresh_metrics_cache, отдельная джоба."""
    missing = await _find_missing("production")
    if missing:
        log.info("scheduler.predictions_missing", count=len(missing))
        filled: list[tuple[str, datetime]] = []
        for symbol_binance, as_of_ts in missing:
            symbol = normalize(symbol_binance)
            try:
                ok = await predictor.predict_candle(symbol, as_of_ts)
                if ok:
                    filled.append((symbol_binance, as_of_ts))
                else:
                    log.warning("scheduler.predict_no_data", symbol=symbol, as_of_ts=as_of_ts.isoformat())
            except Exception as exc:
                log.error("scheduler.predict_failed", symbol=symbol, as_of_ts=as_of_ts.isoformat(), error=str(exc))
        await _resolve_filled(filled)

    for symbol in DISPLAY_SYMBOLS:
        try:
            await actualize_due_predictions(symbol)
        except Exception as exc:
            log.error("scheduler.actualize_failed", symbol=symbol, error=str(exc))


async def refresh_metrics_cache() -> None:
    """Пересчитывает кэш /metrics/*, /candles/{symbol}/pred_history,
    дефолтный таймфрейм 1D и первую страницу /prediction_log — для всех
    символов. Отдельная джоба от sync_predictions (см. docstring модуля):
    дорогой JOIN predictions×candles по нескольким окнам на символ, здесь
    ей можно skip'нуться сама на себя (max_instances=1) сколько угодно —
    это стухший кэш на лишний интервал, не потерянные строки в БД."""
    for symbol in DISPLAY_SYMBOLS:
        try:
            await refresh_symbol_metrics(symbol)
        except Exception as exc:
            log.error("scheduler.metrics_refresh_failed", symbol=symbol, error=str(exc))

    for symbol in DISPLAY_SYMBOLS:
        try:
            await refresh_symbol_pred_history(symbol)
        except Exception as exc:
            log.error("scheduler.pred_history_refresh_failed", symbol=symbol, error=str(exc))

    for symbol in DISPLAY_SYMBOLS:
        try:
            await refresh_symbol_candles(symbol)
        except Exception as exc:
            log.error("scheduler.candles_refresh_failed", symbol=symbol, error=str(exc))

    for symbol in DISPLAY_SYMBOLS:
        try:
            await refresh_symbol_prediction_log(symbol)
        except Exception as exc:
            log.error("scheduler.prediction_log_refresh_failed", symbol=symbol, error=str(exc))


async def sync_predictions_shadow() -> None:
    """Shadow-скан кандидата — находит дыры в slot='candidate', досылает их
    на /predict/{symbol}/shadow, пишет в БД, никогда не публикуется во фронт
    (см. predictor.predict_candle(shadow=True)). Отдельная от
    actualize_due_predictions джоба: сравнение production vs candidate —
    обычный JOIN с GROUP BY slot (app.api.admin), ему не нужен отдельный
    watermark или live-push, только накопленные строки в predictions."""
    missing = await _find_missing("candidate")
    if not missing:
        return

    log.info("scheduler.shadow_predictions_missing", count=len(missing))
    filled: list[tuple[str, datetime]] = []
    for symbol_binance, as_of_ts in missing:
        symbol = normalize(symbol_binance)
        try:
            ok = await predictor.predict_candle(symbol, as_of_ts, shadow=True)
            if ok:
                filled.append((symbol_binance, as_of_ts))
            else:
                log.warning("scheduler.shadow_predict_no_data", symbol=symbol, as_of_ts=as_of_ts.isoformat())
        except Exception as exc:
            log.error("scheduler.shadow_predict_failed", symbol=symbol, as_of_ts=as_of_ts.isoformat(), error=str(exc))
    await _resolve_filled(filled)


def _on_job_max_instances(event: JobSubmissionEvent) -> None:
    """EVENT_JOB_MAX_INSTANCES — новый тик пришёл, пока предыдущий запуск той
    же джобы ещё не закончился, и он молча не стартовал. Раньше (до перехода
    на backfill, см. incident 2026-08-13 в docstring модуля) это означало
    реально потерянные production-строки; теперь скипнутый тик просто
    откладывает обработку дыры до следующего — не теряет её. Явный error-лог
    здесь всё равно полезен для мониторинга (регулярные пропуски — сигнал,
    что тик стал занимать больше бюджета, даже если сами данные не теряются)."""
    log.error(
        "scheduler.tick_skipped",
        reason="max_instances",
        job_id=event.job_id,
        scheduled_run_times=[str(t) for t in event.scheduled_run_times],
    )


def _on_job_missed(event: JobExecutionEvent) -> None:
    """EVENT_JOB_MISSED — тик настолько опоздал (за misfire_grace_time), что
    APScheduler даже не пытался его выполнить. Тот же смысл, что и
    _on_job_max_instances, другой механизм пропуска."""
    log.error(
        "scheduler.tick_skipped",
        reason="misfire_grace_time_exceeded",
        job_id=event.job_id,
        scheduled_run_time=str(event.scheduled_run_time),
    )


def init_scheduler() -> AsyncIOScheduler:
    """Вызывается один раз в lifespan FastAPI (после init_redis). Идемпотентно."""
    global _scheduler
    if _scheduler is not None:
        return _scheduler

    settings = get_settings()
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_listener(_on_job_max_instances, EVENT_JOB_MAX_INSTANCES)
    scheduler.add_listener(_on_job_missed, EVENT_JOB_MISSED)
    scheduler.add_job(
        sync_predictions,
        trigger=CronTrigger(
            minute=f"*/{settings.ml_poll_interval_minutes}",
            second=_POLL_OFFSET_SECONDS,
        ),
        id=_JOB_ID,
        misfire_grace_time=60,
        coalesce=True,
        # 2, не 1: запас на пересечение тиков — sync_predictions лёгкая в
        # штатном режиме (refresh вынесен отдельно, дыр обычно нет), но
        # задел на будущее разовое замедление (VDS2 подвис, большой бэклог
        # после рестарта) не помешает. Идемпотентно по построению (upsert по
        # PK, gap-запрос не находит уже сохранённые строки) — пересечение
        # тиков не может задвоить данные, максимум лишний вызов VDS2.
        max_instances=2,
    )

    scheduler.add_job(
        refresh_metrics_cache,
        trigger=CronTrigger(minute=f"*/{settings.metrics_refresh_interval_minutes}"),
        id=_REFRESH_JOB_ID,
        misfire_grace_time=_REFRESH_MISFIRE_GRACE_SECONDS,
        coalesce=True,
        max_instances=1,
    )

    if settings.ml_shadow_enabled:
        scheduler.add_job(
            sync_predictions_shadow,
            trigger=CronTrigger(
                minute=f"*/{settings.ml_poll_interval_minutes}",
                second=_SHADOW_POLL_OFFSET_SECONDS,
            ),
            id=_SHADOW_JOB_ID,
            misfire_grace_time=60,
            coalesce=True,
            max_instances=1,
        )
        log.info("scheduler.shadow_enabled")

    scheduler.start()
    log.info(
        "scheduler.started",
        poll_interval_minutes=settings.ml_poll_interval_minutes,
        refresh_interval_minutes=settings.metrics_refresh_interval_minutes,
    )

    _scheduler = scheduler
    return scheduler


def shutdown_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        log.info("scheduler.stopped")
