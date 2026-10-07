"""SSE (Server-Sent Events) router.

Архитектура: fan-out через asyncio.Queue
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Старая схема (проблемная):
  каждый SSE-клиент → своя Redis pub/sub подписка → N Redis-соединений

Новая схема (fan-out):
  1 broadcaster task на символ → 1 Redis-соединение на символ (всегда)
  каждый SSE-клиент → своя asyncio.Queue → просто читает из неё

  Binance WS → Redis pub/sub
                    │
              broadcaster         ← 1 соединение Redis на символ
                    │
          ┌─────────┼─────────┐
       Queue#1   Queue#2   Queue#3  ← чистый Python, без Redis
          │         │         │
       клиент1   клиент2   клиент3

Broadcaster запускается лениво при первом клиенте, перезапускается
при обрыве Redis (exponential backoff 1 → 60 сек).

Эндпоинты:
  GET /stream/{symbol}    — единый стрим: тики + прогнозы + результаты
  GET /snapshot/{symbol}  — текущее состояние одним JSON (для initial load)

Формат SSE-событий:
  event: tick         — ценовой тик каждую секунду
  event: prediction   — прогноз при закрытии 5m-свечи
  event: result       — hit/miss предыдущего прогноза
  event: ping         — keepalive раз в 15 секунд
"""

from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import AsyncIterator

import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.core.redis import get_redis, new_pubsub_client
from app.ingest.actualizer import RESULTS_RECENT_MAXLEN
from app.inference.registry import normalize as _normalize_symbol

log = structlog.get_logger()

router = APIRouter(prefix="/stream", tags=["stream"])

_PUBSUB_TIMEOUT     = 15.0     # секунд тишины → шлём ping клиенту
_MAX_CONNECTION_AGE = 3600.0   # после этого клиент должен переподключиться
_QUEUE_MAXSIZE      = 200      # буфер на клиента; при переполнении тик пропускается


# ──────────────────────────────────────────────────────────────────────────────
# Fan-out registry
# ──────────────────────────────────────────────────────────────────────────────

# symbol → множество активных очередей SSE-клиентов
_client_queues: dict[str, set[asyncio.Queue]] = defaultdict(set)

# symbol → фоновая задача broadcaster
_broadcaster_tasks: dict[str, asyncio.Task] = {}


# ──────────────────────────────────────────────────────────────────────────────
# Broadcaster — одна задача на символ, живёт всё время работы приложения
# ──────────────────────────────────────────────────────────────────────────────

async def _symbol_broadcaster(symbol: str) -> None:
    """Одна Redis pub/sub подписка на символ → fan-out во все клиентские очереди.

    При любой ошибке Redis (обрыв соединения, таймаут) переподключается
    с exponential backoff. CancelledError = shutdown, выходим без retry.
    """
    channels = [
        f"candles:{symbol}",
        f"predictions:{symbol}",
        f"results:{symbol}",
    ]
    channel_to_event = {
        f"candles:{symbol}":     "tick",
        f"predictions:{symbol}": "prediction",
        f"results:{symbol}":     "result",
    }
    backoff = 1.0

    while True:
        sub_client = new_pubsub_client()   # изолированное соединение, вне пула
        pubsub     = sub_client.pubsub()

        try:
            await pubsub.subscribe(*channels)
            log.info("broadcaster.subscribed", symbol=symbol)
            backoff = 1.0  # сбрасываем при успешном подключении

            while True:
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=0.1
                )
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue

                channel    = msg.get("channel", "")
                event_type = channel_to_event.get(channel)
                if not event_type:
                    continue

                raw_data = msg.get("data", "")

                # Раздаём всем подключённым клиентам этого символа.
                # list() — снимок, чтобы не упасть если множество меняется параллельно.
                for q in list(_client_queues[symbol]):
                    try:
                        q.put_nowait((event_type, raw_data))
                    except asyncio.QueueFull:
                        pass  # медленный клиент — пропускаем один тик, не блокируем остальных

        except asyncio.CancelledError:
            log.info("broadcaster.cancelled", symbol=symbol)
            return  # shutdown — не retry

        except Exception as exc:
            log.warning(
                "broadcaster.error",
                symbol=symbol, error=str(exc), retry_in=backoff,
            )

        finally:
            # Закрываем соединение в любом случае (ошибка или CancelledError)
            try:
                await pubsub.unsubscribe(*channels)
                await pubsub.aclose()
                await sub_client.aclose()
            except Exception:
                pass

        # Если дошли сюда — была ошибка (не CancelledError). Ждём и пробуем снова.
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)


def _ensure_broadcaster(symbol: str) -> None:
    """Запускает broadcaster для символа, если он ещё не запущен или упал."""
    task = _broadcaster_tasks.get(symbol)
    if task is None or task.done():
        _broadcaster_tasks[symbol] = asyncio.create_task(
            _symbol_broadcaster(symbol),
            name=f"broadcaster-{symbol}",
        )
        log.info("broadcaster.started", symbol=symbol)


async def shutdown_broadcasters() -> None:
    """Останавливает все broadcaster-задачи. Вызывается из lifespan при shutdown."""
    for task in _broadcaster_tasks.values():
        task.cancel()
    if _broadcaster_tasks:
        await asyncio.gather(*_broadcaster_tasks.values(), return_exceptions=True)
    _broadcaster_tasks.clear()
    log.info("broadcasters.stopped")


# ──────────────────────────────────────────────────────────────────────────────
# SSE helpers
# ──────────────────────────────────────────────────────────────────────────────

def _make_event(event_type: str, data: dict) -> str:
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"


def _ping_event() -> str:
    return "event: ping\ndata: {}\n\n"


# ──────────────────────────────────────────────────────────────────────────────
# SSE generator — каждый клиент просто читает из своей asyncio.Queue
# ──────────────────────────────────────────────────────────────────────────────

async def _sse_generator(symbol: str, request: Request) -> AsyncIterator[str]:
    """Генератор SSE для одного клиента.

    Не работает с Redis напрямую — получает данные из Queue,
    которую наполняет broadcaster task.
    """
    _ensure_broadcaster(symbol)

    queue: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAXSIZE)
    _client_queues[symbol].add(queue)
    log.info("sse.connected", symbol=symbol, clients=len(_client_queues[symbol]))

    try:
        elapsed = 0.0
        while elapsed < _MAX_CONNECTION_AGE:
            if await request.is_disconnected():
                log.info("sse.client_disconnected", symbol=symbol)
                break

            try:
                event_type, raw_data = await asyncio.wait_for(
                    queue.get(), timeout=_PUBSUB_TIMEOUT
                )
            except asyncio.TimeoutError:
                # Тишина 15 сек → keepalive ping чтобы браузер не закрыл соединение
                yield _ping_event()
                elapsed += _PUBSUB_TIMEOUT
                continue

            try:
                data = json.loads(raw_data)
            except (json.JSONDecodeError, TypeError):
                log.warning("sse.bad_json", raw=str(raw_data)[:100])
                continue

            yield _make_event(event_type, data)
            elapsed = 0.0  # сбрасываем таймер при любой активности

    except asyncio.CancelledError:
        log.info("sse.cancelled", symbol=symbol)
    finally:
        # Убираем очередь из реестра — broadcaster перестанет в неё писать
        _client_queues[symbol].discard(queue)
        log.info("sse.closed", symbol=symbol, clients=len(_client_queues[symbol]))


# ──────────────────────────────────────────────────────────────────────────────
# HTTP endpoints
# ──────────────────────────────────────────────────────────────────────────────

@router.get("/{symbol:path}")
async def stream(symbol: str, request: Request) -> StreamingResponse:
    symbol = _normalize_symbol(symbol)
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    return StreamingResponse(
        _sse_generator(symbol, request),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",      # отключаем буферизацию в Caddy/nginx
            "Connection":        "keep-alive",
        },
    )


# ──────────────────────────────────────────────────────────────────────────────
# Snapshot endpoint — initial load для фронта
# ──────────────────────────────────────────────────────────────────────────────

_SNAPSHOT_ROUTER = APIRouter(prefix="/snapshot", tags=["stream"])


@_SNAPSHOT_ROUTER.get("/{symbol:path}")
async def snapshot(symbol: str) -> JSONResponse:
    """Текущее состояние одним JSON. Фронт вызывает при монтировании компонента."""
    symbol = _normalize_symbol(symbol)
    if not symbol:
        raise HTTPException(status_code=400, detail="Invalid symbol")

    redis = get_redis()

    candle_raw, pred_raw, results_raw = await asyncio.gather(
        redis.get(f"candle:{symbol}"),
        redis.get(f"prediction:{symbol}"),
        redis.lrange(f"results:{symbol}:recent", 0, RESULTS_RECENT_MAXLEN - 1),
        return_exceptions=True,
    )

    def _safe_json(raw) -> dict | None:
        if isinstance(raw, Exception) or raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    recent_results = []
    if isinstance(results_raw, list):
        for r in results_raw:
            parsed = _safe_json(r)
            if parsed:
                recent_results.append(parsed)

    return JSONResponse({
        "symbol":         symbol,
        "last_candle":    _safe_json(candle_raw),
        "prediction":     _safe_json(pred_raw),
        "recent_results": recent_results,
    })


routers = [router, _SNAPSHOT_ROUTER]
