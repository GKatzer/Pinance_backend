"""SQLAlchemy 2.0 declarative models.

Таблицы:
  - Candle: OHLCV 1-минутные свечи (hypertable в TimescaleDB по ts).
  - Prediction: прогнозы модели с актуальным результатом для расчёта метрик.

Alembic читает этот модуль и генерирует миграции автоматически.
Hypertable-специфичные команды (SELECT create_hypertable(...)) выполняются
отдельным SQL-скриптом при первом деплое — SQLAlchemy их не знает.
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Float,
    Index,
    Integer,
    SmallInteger,
    String,
    func,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Единый Base для всех моделей → Alembic видит всё в одном месте."""


# ──────────────────────────────────────────────────────────────────────────────
# Candle
# ──────────────────────────────────────────────────────────────────────────────

class Candle(Base):
    """OHLCV 1-минутная свеча.

    Partitioning column — ts. TimescaleDB требует его в составном PK.
    Hypertable создаётся вне SQLAlchemy:
        SELECT create_hypertable('candles', 'ts', chunk_time_interval => INTERVAL '7 days');
    """

    __tablename__ = "candles"

    symbol: Mapped[str]      = mapped_column(String(16), primary_key=True)
    ts:     Mapped[datetime] = mapped_column(primary_key=True)   # UTC, без timezone в БД

    open:   Mapped[float] = mapped_column(Float, nullable=False)
    high:   Mapped[float] = mapped_column(Float, nullable=False)
    low:    Mapped[float] = mapped_column(Float, nullable=False)
    close:  Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        # Descending по ts — основной паттерн запросов: "последние N свечей".
        Index("ix_candles_symbol_ts_desc", "symbol", "ts", postgresql_using="btree"),
    )


# ──────────────────────────────────────────────────────────────────────────────
# Prediction
# ──────────────────────────────────────────────────────────────────────────────

class Prediction(Base):
    """Прогноз модели по одному горизонту, полученный опросом VDS2 (pull).

    VDS1 раз в 5 минут дёргает `GET /predict/{symbol}` на VDS2 и получает
    точечный прогноз + 10%/90% квантили на 12 горизонтов сразу — эта таблица
    хранит их построчно, по одной строке на горизонт. Insert — чисто
    append-only (actual_price и т.д. NULL), но строка дописывается один раз
    позже, когда прогноз дозревает (см. actual_price ниже) — не совсем
    write-once, но новый апдейт бьёт по PK точечно, не сканом.

    slot в PK — это то, что делает возможным shadow deployment: production
    и кандидат пишут в одну и ту же таблицу под одним и тем же
    (symbol, as_of_ts, horizon), но под разными slot, не затирая друг друга.
    Ровно два литеральных значения, известные уже в момент опроса (какой
    URL дёрнули на VDS2 — /predict или /predict/shadow): 'production' и
    'candidate'. Live-фронт (Redis, actualizer, pred_history) видит только
    строки с slot = 'production' — кандидат не публикуется, пишется только
    в БД для последующего сравнения.

    model_version — отдельно от slot: реальная версия обученной модели,
    как её прислал VDS2 (например "202608021530-a1b2c3d4"), чисто
    информационная, не участвует в PK и ни в одном WHERE-фильтре. Раньше
    эти две вещи были одной колонкой — из-за этого продовые прогнозы,
    как только VDS2 начал реально присылать версию, переставали совпадать
    с литералом 'production' в фильтрах и пропадали из pred_history/metrics.

    Оценка "предсказано vs факт" — ДО b6e2c4a8f1d3 была простым JOIN на
    чтении (candles.ts = target_ts / as_of_ts), без отдельной стадии
    актуализации. На продовом объёме (~1М+ строк, 10 read-сайтов с таким
    JOIN'ом) это стало главной причиной деградации — live "all"-окно
    /summary наблюдалось выполняющимся 20+ минут, держа соединение пула,
    и морило голодом refresh_metrics_cache, которая как раз должна была
    экранировать этот путь кэшем (см. app/scheduler/jobs.py). Старые
    дозревшие строки после этого не меняются, новых — по десятку в минуту,
    так что JOIN перенесён из "на каждое чтение" в "один раз при дозревании":
    app.ingest.actualizer.actualize_due_predictions и так уже вычисляет
    actual_price/actual_r/hit для live-ленты фронта — теперь дописывает
    их же point-UPDATE'ом сюда (по PK, не сканом), для обоих slot (не
    только production — admin.compare сравнивает оба). Разовый бэкафилл
    старой истории — app/backfill/resolve_predictions.py, батчами, не
    частью миграции.

    Hypertable по as_of_ts (partitioning column), как и candles:
        SELECT create_hypertable('predictions', 'as_of_ts',
                                 chunk_time_interval => INTERVAL '7 days');
    """

    __tablename__ = "predictions"

    # Составной PK: пара + свеча, на которой считали + горизонт + слот (production/candidate).
    symbol:   Mapped[str]      = mapped_column(String(16), primary_key=True)
    as_of_ts: Mapped[datetime] = mapped_column(primary_key=True)   # свеча, на которой считал VDS2, UTC без tz
    horizon:  Mapped[int]      = mapped_column(SmallInteger, primary_key=True)  # 1..12
    slot:     Mapped[str]      = mapped_column(String(16), primary_key=True)  # 'production' | 'candidate'

    # Реальная версия модели (трейсабилити/сравнение в admin.compare) — НЕ часть PK,
    # НЕ используется ни в одном read-фильтре (см. docstring класса выше).
    model_version: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # Версия квантильной корзины — версионируется на VDS2 НЕЗАВИСИМО от
    # model_version (раздельные MinIO-артефакты, раздельный promote_candidate_point/
    # promote_candidate_quantiles). NULL — либо строка старше момента, когда
    # VDS2 начал присылать это поле, либо слот в тот момент был point-only
    # (см. r_q10 и др. выше). Как и model_version — не PK, не read-фильтр,
    # только трейсабилити (admin.compare группирует coverage по этой колонке
    # отдельно от point-группировки).
    quantile_model_version: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # ── Прогноз (как есть в ответе VDS2) ──────────────────────────────────────
    target_ts:  Mapped[datetime] = mapped_column(nullable=False)
    r_pred:     Mapped[float]    = mapped_column(Float, nullable=False)  # предсказанный log-return
    price_pred: Mapped[float]    = mapped_column(Float, nullable=False)

    # Квантили 10%/90% (VDS2, с 2026-08-02) — когда есть, гарантированно
    # price_q10 <= price_pred <= price_q90, сортировка на стороне VDS2,
    # здесь не перепроверяется. Nullable по двум причинам: строки до этой
    # миграции честно не имеют этих данных (не бэкофиллено фиктивной нулевой
    # шириной), и квантили опциональны per-модель — если у слота пуст
    # quantile_levels, VDS2 отдаёт point-only прогноз без этих ключей вообще
    # (валидное состояние, не ошибка схемы). predictor.py читает их через
    # .get()/_optional_float, не напрямую по ключу.
    r_q10:     Mapped[float | None] = mapped_column(Float, nullable=True)
    r_q90:     Mapped[float | None] = mapped_column(Float, nullable=True)
    price_q10: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_q90: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Время инференса на VDS2 в мс (весь /predict-вызов — все 12 горизонтов
    # сразу, не один запрос на горизонт) — как и model_version, одно значение
    # на весь ответ VDS2, дублируется во все 12 строк одного as_of_ts (тот же
    # паттерн). NULL — строки до того, как VDS2 начал присылать это поле
    # (predictor.py читает через .get()). GET /metrics/{symbol}/latency
    # фильтрует horizon=1, чтобы не считать одно и то же значение 12 раз.
    inference_ms: Mapped[float | None] = mapped_column(Float, nullable=True)

    # ── Резолв (проставляется actualizer'ом, когда target_ts дозревает) ──────
    # NULL = ещё не дозрело (тот же смысл, что раньше "нет пары в JOIN").
    # См. docstring класса выше и миграцию b6e2c4a8f1d3 — почему это здесь,
    # а не JOIN на чтении.
    actual_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    actual_r:     Mapped[float | None] = mapped_column(Float, nullable=True)
    hit:          Mapped[bool | None]  = mapped_column(nullable=True)
    sim_return:   Mapped[float | None] = mapped_column(Float, nullable=True)  # только horizon=1 значим, см. _window_metrics

    created_at: Mapped[datetime] = mapped_column(server_default=func.now())

    __table_args__ = (
        Index("ix_predictions_symbol_as_of_desc", "symbol", "as_of_ts", postgresql_using="btree"),
        Index("ix_predictions_target_ts", "symbol", "target_ts"),
        # Частичный индекс под основной паттерн чтения после b6e2c4a8f1d3:
        # symbol+slot+target_ts range, только резолвнутые строки.
        Index(
            "ix_predictions_resolved", "symbol", "slot", "target_ts",
            postgresql_where=text("actual_price IS NOT NULL"),
        ),
    )


# ──────────────────────────────────────────────────────────────────────────────
# RetrainEvent
# ──────────────────────────────────────────────────────────────────────────────

class RetrainEvent(Base):
    """Лог решений ретрейна — что решили promote_if_better.py/auto_retrain*.py
    на стороне Pinance_ML про конкретный кандидат. Append-only: один POST
    (см. app/api/admin.py POST /retrain-events) на одно решение, вызывается
    из тех же скриптов, что уже дёргают GET /admin/metrics/.../compare.
    Источник для RETRAIN TIMELINE на фронте (GET /metrics/{symbol}/
    retrain-timeline).

    kind — 'point' | 'quantile': какая из двух НЕЗАВИСИМО ретрейнящихся
    моделей (см. Prediction.quantile_model_version — та же пара сущностей,
    точка ретрейнится ежедневно, корзина реже). decision — 'promoted' |
    'rejected'. metric_name/candidate_value/production_value/threshold —
    какую метрику сравнивали и по какому порогу решали (для point —
    directional_accuracy, для quantile — pinball_loss, см. Pinance_ML
    auto_retrain.py/auto_retrain_quantiles.py decide_*_promotion). Ни kind,
    ни decision не проверяются на уровне БД (CHECK) — та же конвенция, что
    у Prediction.slot: значения контролирует пишущая сторона, не схема."""

    __tablename__ = "retrain_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    kind:   Mapped[str] = mapped_column(String(16), nullable=False)   # 'point' | 'quantile'
    symbol: Mapped[str] = mapped_column(String(16), nullable=False)   # форма Binance, как в predictions.symbol

    # Версии, которые сравнивали — model_version/quantile_model_version
    # в зависимости от kind (см. Prediction). Nullable — decision может
    # быть вынесен и без сравнимой production-версии (bootstrap-кейс,
    # см. decide_point_promotion/decide_quantile_promotion "no_production").
    candidate_version:  Mapped[str | None] = mapped_column(String(64), nullable=True)
    production_version: Mapped[str | None] = mapped_column(String(64), nullable=True)

    decision: Mapped[str] = mapped_column(String(16), nullable=False)  # 'promoted' | 'rejected'

    metric_name:      Mapped[str | None]   = mapped_column(String(128), nullable=True)
    candidate_value:  Mapped[float | None] = mapped_column(Float, nullable=True)
    production_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    threshold:        Mapped[float | None] = mapped_column(Float, nullable=True)

    n_samples:          Mapped[int | None]   = mapped_column(Integer, nullable=True)
    train_wall_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Когда решение реально принято на стороне Pinance_ML, не когда долетел
    # POST — присылается вызывающей стороной, не server_default.
    decided_at: Mapped[datetime] = mapped_column(nullable=False)

    __table_args__ = (
        Index("ix_retrain_events_symbol_kind_decided", "symbol", "kind", "decided_at"),
    )