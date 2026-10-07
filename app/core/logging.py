"""Structured logging.

structlog с JSON-выводом в prod и человекочитаемым в dev. Никаких print()
по коду — только log = structlog.get_logger().
"""
import logging
import sys

import structlog

from app.config import get_settings


def configure_logging() -> None:
    settings = get_settings()

    # Корневой logging.* настраиваем, чтобы сторонние либы (uvicorn, sqlalchemy)
    # тоже шли через нашу инфраструктуру.
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=settings.log_level,
    )

    processors: list = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.app_env == "prod":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelName(settings.log_level)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )