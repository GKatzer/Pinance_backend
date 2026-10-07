"""Application settings.

Все настройки в одном месте, читаются из переменных окружения и валидируются
pydantic'ом. Нигде в коде не пишем os.environ[...] — только через Settings.
"""
from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: Literal["dev", "prod"] = "dev"
    log_level: str = "INFO"

    database_url: str = Field(..., description="async SQLAlchemy URL")
    redis_url: str = Field(default="redis://redis:6379/0")

    # Sane defaults для пула. Под нагрузку подгоним позже.
    db_pool_size: int = 5
    db_max_overflow: int = 5

    # VDS2 ML-инференс, доступен только по Tailscale, напр. http://100.x.x.x:8001
    ml_inference_url: str = Field(..., description="Base URL VDS2 predict API")
    ml_inference_timeout_s: float = 5.0
    ml_poll_interval_minutes: int = 5

    # Окно свечей, отправляемое в теле POST /predict/{symbol} (см.
    # app/inference/predictor.py) — держим в синхроне с MIN_WARMUP_CANDLES на
    # стороне Pinance_ml_inference. rolling(144) на ряде returns требует
    # минимум 145 строк, 150 — согласованный с ML-стороной запас в 5 строк.
    ml_candle_window_size: int = 150

    # Верхняя граница backfill-скана (app/scheduler/jobs.py sync_predictions):
    # свечи без прогноза ищутся не по всей истории, а только за последние N
    # часов. Без этой границы свеча, для которой физически не может набраться
    # ml_candle_window_size истории (первые ~12.5ч жизни нового символа),
    # стучалась бы в VDS2 каждый тик безрезультатно (InsufficientHistoryError)
    # до конца времён. 48ч — запас с большим отрывом от окна в 150 свечей.
    ml_gap_lookback_hours: int = 48

    # Пересчёт кэша /metrics/*, /candles/*/pred_history и т.п. — НАМЕРЕННО
    # отдельный интервал от ml_poll_interval_minutes (см. app/scheduler/jobs.py):
    # это дорогой JOIN predictions×candles по нескольким окнам на символ,
    # раньше сидел в одной джобе с приёмом прогнозов от VDS2 (max_instances=1)
    # — при разгоне predictions (2026-08-13, ML_SHADOW_ENABLED) стал занимать
    # больше времени, чем сам ml_poll_interval_minutes, и последующие тики
    # приёма прогнозов молча скипались (не ретраились) — ~20-25% production
    # строк тихо пропадало. Разведены, чтобы тяжёлый пересчёт мог сколько
    # угодно skip'аться сам на себя, не трогая приём прогнозов.
    metrics_refresh_interval_minutes: int = 5

    # Shadow deployment (см. README): кандидат опрашивается на /predict/{symbol}/shadow,
    # пишется в ту же таблицу под slot='candidate', но НЕ публикуется в Redis —
    # пользователь его не видит. Выключено по умолчанию, включать только пока
    # реально идёт сравнение кандидата с прод-моделью.
    ml_shadow_enabled: bool = False


@lru_cache
def get_settings() -> Settings:
    """Кэшируем — Settings создаётся один раз за процесс."""
    return Settings()  # type: ignore[call-arg]