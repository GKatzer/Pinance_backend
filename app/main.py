# FastAPI entrypoint

import asyncio
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.health import router as health_router
from app.api.stream import routers as stream_routers, shutdown_broadcasters
from app.config import get_settings
from app.core.logging import configure_logging
from app.core.redis import close_redis, init_redis
from app.db.session import engine
from app.ingest.binance_ws import run_consumer
from app.scheduler.jobs import init_scheduler, shutdown_scheduler
from app.api.candles import router as candles_router
from app.api.metrics import router as metrics_router
from app.api.admin import routers as admin_routers


configure_logging()
log = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    settings = get_settings()
    log.info("startup.begin", env=settings.app_env)

    # 1. Redis-пул.
    await init_redis()
    log.info("startup.redis_ok")

    # 2. APScheduler — периодический опрос VDS2 (predict) + актуализация.
    init_scheduler()
    log.info("startup.scheduler_ok")

    # 3. Binance WebSocket consumer.
    consumer_task = asyncio.create_task(run_consumer(), name="binance-consumer")
    log.info("startup.consumer_started")

    yield

    # ── Graceful shutdown ──────────────────────────────────────────────────────
    log.info("shutdown.begin")
    shutdown_scheduler()
    consumer_task.cancel()
    try:
        await asyncio.wait_for(asyncio.shield(consumer_task), timeout=8.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass
    await shutdown_broadcasters()
    await close_redis()
    await engine.dispose()
    log.info("shutdown.done")


app = FastAPI(
    title="Pinance API",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url=None,
)

# CORS — фронт на Vercel обращается к api.yourdomain.com
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pinance.vercel.app", "http://localhost:3000"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(health_router)
app.include_router(candles_router)
app.include_router(metrics_router)
for r in admin_routers:
    app.include_router(r)
for r in stream_routers:
    app.include_router(r)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "pinance-api", "version": "0.1.0"}