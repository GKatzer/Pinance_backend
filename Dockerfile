# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS base

# Не пишем .pyc, не буферим stdout — стандартный набор для контейнера.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Системные либы. libpq нужен для asyncpg/psycopg в рантайме,
# build-essential — только на момент установки колёс, потом удалим.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Слой с зависимостями отдельно от кода — кэшируется, пока requirements.txt не меняется.
COPY requirements.txt .
RUN pip install -r requirements.txt

# Непривилегированный пользователь.
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

COPY --chown=app:app app ./app
COPY --chown=app:app alembic.ini .
COPY --chown=app:app entrypoint.sh .
RUN chmod +x entrypoint.sh

EXPOSE 8000

# uvicorn без --reload: в проде reload даёт лишний процесс-наблюдатель.

# В dev hot-reload делаем через volume + команду в docker-compose.override.yml.
ENTRYPOINT ["./entrypoint.sh"]
