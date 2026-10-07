"""Redis client.

Два типа клиентов:
  _client            — shared pool (get/set/publish/lrange и т.д.)
  new_pubsub_client  — изолированное соединение для pub/sub broadcaster-задач.
                       Pub/sub держит соединение открытым пока жива подписка,
                       поэтому нельзя брать его из общего пула — иначе при
                       нескольких реконнектах пул заканчивается.
"""
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import redis.asyncio as redis

from app.config import get_settings

_client: redis.Redis | None = None
_redis_url: str = ""


async def init_redis() -> redis.Redis:
    global _client, _redis_url
    if _client is None:
        settings = get_settings()
        _redis_url = settings.redis_url
        _client = redis.from_url(
            _redis_url,
            encoding="utf-8",
            decode_responses=True,
            max_connections=20,
        )
        await _client.ping()
    return _client


def new_pubsub_client() -> redis.Redis:
    """Создаёт изолированный Redis-клиент для одного pub/sub broadcaster.

    single_connection_client=True — один коннект напрямую, без пула.
    Не конкурирует с _client за слоты max_connections.
    Вызывающий код обязан закрыть через aclose() когда подписка больше не нужна.
    """
    if not _redis_url:
        raise RuntimeError("Redis is not initialized. Call init_redis() first.")
    return redis.Redis.from_url(
        _redis_url,
        encoding="utf-8",
        decode_responses=True,
        single_connection_client=True,
    )


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def get_redis() -> redis.Redis:
    """Геттер для DI в FastAPI. После init_redis() гарантированно не None."""
    if _client is None:
        raise RuntimeError("Redis is not initialized. Call init_redis() first.")
    return _client


@asynccontextmanager
async def redis_lifespan() -> AsyncIterator[redis.Redis]:
    """Удобный контекст для startup/shutdown."""
    client = await init_redis()
    try:
        yield client
    finally:
        await close_redis()