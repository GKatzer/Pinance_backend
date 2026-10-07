# Design decisions

Why the backend looks the way it does, what was tried first, and what went wrong. Dates and incident descriptions come from the code comments and migration docstrings in this repository (`app/scheduler/jobs.py`, `app/inference/predictor.py`, `app/db/models.py`, `app/ingest/actualizer.py`, `app/api/*`); they were not independently reproduced. The git history was not used as a source of truth.

- [1. The backlog is a query, and the request is self-contained](#1-the-backlog-is-a-query-and-the-request-is-self-contained)
- [2. Forecast polling and metrics refresh are separate jobs](#2-forecast-polling-and-metrics-refresh-are-separate-jobs)
- [3. Resolve a forecast once, when it matures](#3-resolve-a-forecast-once-when-it-matures)
- [4. The actualizer upper bound is the newest stored candle, not the clock](#4-the-actualizer-upper-bound-is-the-newest-stored-candle-not-the-clock)
- [5. The slot is the key, the model version is a label](#5-the-slot-is-the-key-the-model-version-is-a-label)
- [6. Point model and corridor are versioned separately](#6-point-model-and-corridor-are-versioned-separately)
- [7. Shadow rows live in the same table and never reach Redis](#7-shadow-rows-live-in-the-same-table-and-never-reach-redis)
- [8. The backend computes the metrics, and the cache is the main read path](#8-the-backend-computes-the-metrics-and-the-cache-is-the-main-read-path)
- [9. SSE: one Redis subscription per symbol, one queue per client](#9-sse-one-redis-subscription-per-symbol-one-queue-per-client)
- [10. The admin API is outside the public surface](#10-the-admin-api-is-outside-the-public-surface)
- [11. Sharpe from the first horizon only](#11-sharpe-from-the-first-horizon-only)
- [Considered and not done](#considered-and-not-done)

## 1. The backlog is a query, and the request is self-contained

**Before (until 2026-08-25).** Every five minutes the backend asked the inference service "what is the forecast now?" with an empty `GET`. The service decided what "now" was from its own data feed and did not keep its answers. If a poll failed (network, restart), the next poll returned the forecast for a newer candle and the missed one was lost for good.

**Now.** The backend builds the request itself: `POST /predict/{symbol}` with `as_of_ts` and the last 150 candles. The inference service answers with a pure function of that input. The work queue is the difference between two tables: candles that have no row in `predictions` (`LEFT JOIN`, last 48 hours, oldest first).

**Why it is better.**
- A failed request does not lose anything, because the candle is still missing the next time the query runs; repeating a request for an old candle gives the same answer.
- No separate queue infrastructure to run or to keep consistent with the database.
- Idempotent by construction: the write is an upsert on the primary key.

**Cost.** The backend reads 150 candles per request (twice for altcoins, with the BTC window) and the window length must match the inference service's minimum (`ML_CANDLE_WINDOW_SIZE`, kept in sync by hand). The scan is bounded to 48 hours (`ML_GAP_LOOKBACK_HOURS`) because a symbol that cannot yet supply 150 candles would otherwise be retried forever.

**Failure handling.** `404` and `422` from the inference service are final for this attempt (no model loaded; not enough history; last candle does not match `as_of_ts`). Network errors and `5xx` get one retry after 2 s. Whatever is still missing is picked up by the next tick.

## 2. Forecast polling and metrics refresh are separate jobs

**Incident (code comment, 2026-08-13).** The cache refresh used to run inside the forecast-polling job. When shadow deployment doubled the number of rows, the refresh grew slower than the polling interval; because the job was configured with one instance and `coalesce=True`, later ticks were skipped, not queued. Roughly 20-25 % of production forecast rows silently went missing on all symbols from 2026-08-13 on, while the lighter shadow job was never skipped, which showed up as an asymmetry between production and candidate.

**Decision.** Two jobs on their own schedules. The heavy refresh may skip itself as often as it likes; the cost is a cache that is one interval older, not a lost row. The `max_instances` skip is now also logged at error level for monitoring.

The move to gap-filling (decision 1) removed the other half of the problem: a skipped polling tick now delays rows instead of losing them.

## 3. Resolve a forecast once, when it matures

**Before.** "Predicted versus actual" was a join of `predictions` with `candles` (twice) on every read, ten read sites in total.

**What happened (comment in `app/db/models.py`).** At about a million forecast rows the all-time window of the summary endpoint was observed running for more than 20 minutes while holding a pooled connection, starving the very cache refresh that was supposed to shield that path.

**Decision (migration `b6e2c4a8f1d3`).** When a forecast matures, the actualizer computes `actual_price`, `actual_r`, `hit` and `sim_return` and writes them onto the forecast row with a point `UPDATE` by primary key. Reads use a partial index on resolved rows and a range scan. Old rows are filled once by `python -m app.backfill.resolve_predictions`, in batches with pauses, not inside the migration.

**Trade-off.** The write path got more complex: an extra update per row, plus a second path for forecasts that are filled late (decision 4). In return, the read path went from a join over millions of rows to a scan of one table.

## 4. The actualizer upper bound is the newest stored candle, not the clock

The actualizer processes forecasts whose `target_ts` lies in `(watermark, upper]`, then moves the per-symbol watermark (kept in Redis) to `upper`.

**Bug that this avoids (code comment).** With `upper = now()`, a cycle that ran after the wall clock reached `T + 5 min` but before the candle `T` had been written would still advance the watermark past `T`; the forecast for `T` then never matched `target_ts > watermark` again and was lost. Using `MAX(candles.ts)` as the upper bound means the watermark can never run ahead of what is actually in the database.

**Related case.** A forecast that is filled late (after a gap, an outage, a model change) already has a `target_ts` behind the watermark. `resolve_filled_predictions` handles exactly those rows by a point lookup on their `as_of_ts` and does not publish them to Redis, so old results do not appear in the live feed.

**Known exposure.** The watermark lives in Redis, which `docker-compose.yml` runs with a 256 MB limit and the `allkeys-lru` eviction policy. If the key were evicted, the actualizer would restart with a 15-minute lookback and older rows would stay unresolved until `resolve_predictions` is run by hand. This follows from reading the configuration; it has not been observed.

## 5. The slot is the key, the model version is a label

`predictions` has the primary key `(symbol, as_of_ts, horizon, slot)`. `slot` is one of two literals, `production` or `candidate`, known when the request is made (which URL was called). `model_version` is the real version string the inference service returns and is a plain column.

**Why not use the version as the key.** An earlier schema revision put `model_version` in the primary key, with `'production'` as the placeholder value. As soon as the inference service started sending real versions, the production rows stopped matching the literal `'production'` in the read filters and vanished from the history and the metrics. Migration `d2a6f9c1b4e8` split the two: the key says *which role*, the column says *which model*.

## 6. Point model and corridor are versioned separately

`quantile_model_version` is a separate column because the point model and the quantile ("corridor") model are retrained and promoted independently (daily and less often, weekly according to the training repository), so within any window wider than a day, one `slot` normally contains several point versions and possibly different corridor versions.

The comparison endpoint therefore groups point metrics by `(slot, model_version)` and corridor coverage by `(slot, quantile_model_version)` in a separate `quantiles` section. Computing coverage in the point group would tie the corridor's width to the wrong version.

A forecast may also have no corridor at all: when the model has no quantile levels, the service sends no `r_q10 / r_q90 / price_q10 / price_q90`, the columns stay `NULL`, and every coverage query filters on `r_q10 IS NOT NULL`.

## 7. Shadow rows live in the same table and never reach Redis

A candidate model is evaluated against the same candles as production by writing its forecasts into the same table under `slot = 'candidate'`. Public read paths (`/metrics/*`, `/candles/*/pred_history`, `/prediction_log`) filter on `slot = 'production'`; the publish step in the predictor is skipped for shadow, so the live chart cannot show a candidate. The actualizer resolves both slots in one pass (the comparison needs the outcomes) but publishes only production.

A candidate can then be compared with production on identical outcomes, without a second database or a second schema, by `GET /admin/metrics/{symbol}/compare`. The decision to promote is not taken here: the endpoint only returns numbers, and the training repository's `promote_if_better.py` applies its own gates (per the module docstring in `app/api/admin.py`).

## 8. The backend computes the metrics, and the cache is the main read path

**Origin.** The first live "hit rate" was computed in the browser from the last 20 SSE results, so two open tabs could show different values for the same symbol. All windows (1 h, 24 h, 7 d, 30 d, all), trends and distributions are now computed on the backend over the full window.

**Cache as the primary path.** `refresh_metrics_cache` writes exactly the combinations the web UI requests, every 5 minutes, with a 15-minute TTL, three times the refresh period, so that a few missed cycles do not blank the dashboard. A route first looks in Redis; only on a miss does it compute and store with a short TTL (45 s for summary and coverage, 120 s for the rest). Many concurrent viewers therefore cost one computation per cycle, not one per request.

**Not cached on purpose.** `GET /admin/.../compare` serves a person or a promotion job (the code says roughly every six hours), where freshness matters more than saving a query. Deep pages of the prediction log (`before=<cursor>`) are not cached either: every cursor is different, so a shared key would never hit.

## 9. SSE: one Redis subscription per symbol, one queue per client

**First design.** Every browser connection opened its own Redis pub/sub subscription, so N viewers cost N Redis connections and the pool could run out.

**Now.** One broadcaster task per symbol holds one dedicated Redis connection (outside the pool, `single_connection_client=True`) and hands each message to a queue per client. The queue has 200 slots and a full queue drops the new event for that client only, which is the right behaviour for ticks: a slow client misses a price update instead of slowing the others. The task is started lazily by the first client and reconnects with backoff.

## 10. The admin API is outside the public surface

The admin routes are deliberately not under `/metrics`. According to the comment in `app/api/admin.py`, the public reverse proxy forwards only the prefixes the web UI needs (`/candles`, `/stream`, `/snapshot`, `/health`, `/ready`, `/metrics`), so `/admin/*` is reachable only from inside the private network. The application itself has **no authentication** and its CORS policy allows only `GET`; the protection is the network layout. A request to `/admin/...` against the public site returns the web UI's HTML page, which is consistent with that comment (checked on 2026-10-03, see [`examples/live/CAPTURED_AT.txt`](examples/live/CAPTURED_AT.txt)); the proxy configuration itself is not in this repository.

## 11. Sharpe from the first horizon only

The simulated strategy is "long if the forecast return is positive, short if negative", and `sim_return` is the realised return of that position. The Sharpe ratio uses only `horizon = 1` rows: a single strategy needs a single step size to be annualised (`sqrt(105,120)`, the number of 5-minute periods in a year), and `horizon = 1` rows do not overlap (forecast at `T` covers `T..T+5 min`, the next one starts at `T+5 min`). It ignores fees and slippage, so it is a diagnostic, not a trading result. See [metrics.md](metrics.md).

## Considered and not done

- **A separate message queue for forecast tasks.** Replaced by the table difference (decision 1).
- **Computing drift on the backend.** The inference service returns the feature snapshot and the training baseline, but nothing here stores them or computes a drift metric, so there is no `/metrics/{symbol}/drift` endpoint. The web UI has a panel for it and shows an empty state; the live site answers `404` for that path ([`examples/live/drift.json`](examples/live/drift.json)).
- **Authentication on the admin API.** Left to the network layout (decision 10).
- **A test suite.** There is none yet (see the README's Limitations).
