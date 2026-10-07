# API reference

All routes of `app/main.py`, taken from the route table of the running application (`app.routes`, 2026-10-03) and checked against the code. Examples are real responses from the public deployment captured on 2026-10-03 and stored in [`examples/live/`](examples/live/); long arrays are shortened with `...`.

- Base URL in the examples: `https://pinance.katzer.ru`. Locally: `http://127.0.0.1:8002` with the override file from the README.
- `{symbol}` accepts `BTC/USDT`, `ETH/USDT`, `SOL/USDT`, `BNB/USDT` in any of the forms `BTC%2FUSDT`, `BTC-USDT`, `BTCUSDT`, `btcusdt`. Anything else returns `400 {"detail":"Invalid symbol"}`.
- Windows and buckets are durations like `15m`, `24h`, `7d`; a malformed value returns `400`, a bucket larger than its window returns `400`.
- Every route is `GET` except `POST /admin/retrain-events`. CORS allows `GET` only, for the origins `https://pinance.vercel.app` and `http://localhost:3000` (as coded in `app/main.py`).
- The application serves OpenAPI at `/docs`; on the public domain that path returns the web UI page instead (checked 2026-10-03), so use a local instance.
- Timestamps in the candle endpoints are Unix milliseconds. Forecast rows use ISO strings: `as_of_ts` of the `prediction` event and of `/snapshot` carries an offset (`+00:00`); `result` events and the database-backed endpoints (`prediction_log`, `retrain-timeline`) are naive UTC.

## Public routes

### Health

| Route | Response |
|---|---|
| `GET /health` | `{"status":"ok"}` (process is alive) |
| `GET /ready` | `{"status":"ready"}` after `SELECT 1` and a Redis `PING`; otherwise `503` with the failing dependency in `detail` |
| `GET /` | `{"service":"pinance-api","version":"0.1.0"}` (the public site serves its web UI at `/`) |

### Live state

| Route | Description |
|---|---|
| `GET /stream/{symbol}` | Server-Sent Events: `tick`, `prediction`, `result`, `ping`. See [architecture.md](architecture.md#live-fan-out-to-browsers-sse) |
| `GET /snapshot/{symbol}` | one JSON for the first paint: `last_candle`, `prediction`, `recent_results` (up to 240) |
| `GET /candles/{symbol}/latest` | last tick from Redis; `404 {"detail":"No live data yet"}` if none |

```bash
curl -N "https://pinance.katzer.ru/stream/BTC%2FUSDT"
```

From [`examples/live/sse-boundary.txt`](examples/live/sse-boundary.txt) (abridged; a tick, then the forecast made when the 09:00 candle closed, then a matured result):

```
event: tick
data: {"symbol": "BTC/USDT", "ts": 1791018300000, "closed": false, "open": 84641.15, "high": 84641.15, "low": 84641.14, "close": 84641.15, "volume": 0.39327}

event: prediction
data: {"symbol": "BTC/USDT", "as_of_ts": "2026-10-03T09:00:00+00:00", "close": 84641.15, "model_version": "202610021348-4de49a70", "quantile_model_version": "202610021525-4de49a70", "schema_version": "38b98fc7516d", "feature_count": 93, "predictions": [{"horizon": 1, ...}, ... 12 items]}

event: result
data: {"as_of_ts": "2026-10-03T08:45:00", "horizon": 3, "target_ts": "2026-10-03T09:00:00", "r_pred": -1.0898e-06, "price_pred": 84634.37, "price_q10": 84578.77, "price_q90": 84686.92, "actual_r": 7.904e-05, ...}
```

Each element of `predictions` has `horizon` (1..12), `target_ts`, `r_pred` (log-return), `price_pred`, and, when the model has a corridor, `r_q10`, `r_q90`, `price_q10`, `price_q90`.

### Candles

`GET /candles/{symbol}?timeframe=1D|1W|1M|1Y|ALL` (default `1D`; any other value is `422`).

| `timeframe` | Interval | Source | Limit | Count on 2026-10-03 |
|---|---|---|---|---|
| `1D` | 5 m | database | 288 | 288 |
| `1W` | 15 m | Binance REST `klines` | 672 | 672 |
| `1M` | 1 h | Binance REST `klines` | 720 | 720 |
| `1Y` | 1 d | database, daily aggregate (`time_bucket`, last 370 days) | 365 | 365 |
| `ALL` | 1 d | database, daily aggregate, whole history | 5,000 | 3,075 (from 2018-05-04) |

Counts: [`examples/live/candles-counts-by-timeframe.json`](examples/live/candles-counts-by-timeframe.json). If Binance is unreachable for `1W`/`1M`, the route answers `502`.

```json
{"symbol":"BTC/USDT","timeframe":"1D","interval":"5m","candles":[{"ts":1790931000000,"open":86305.99,"high":86334.96,"low":86247.74,"close":86249.99,"volume":64.40351,"closed":true}, ...]}
```

`GET /candles/{symbol}/pred_history?horizon=1&hours=24` (`horizon` 1..12, `hours` 1..168): resolved production forecasts of one horizon, for the chart trail.

```json
{"symbol":"BTC/USDT","horizon":1,"history":[{"ts":1791014100000,"predicted":84599.19,"q10":84568.51,"q90":84631.91,"hit":true}, ...]}
```

`GET /candles/{symbol}/prediction_log?limit=240&before=<ISO time>` (`limit` 1..1000, default 240): resolved production rows of all horizons, newest `target_ts` first, then by horizon. `has_more` is true when the page is full. Use a `limit` that is a multiple of 12, otherwise the next `before` cursor can skip part of a candle's horizons. Only the first page (no `before`, default limit) is cached.

```json
{"symbol":"BTC/USDT","log":[{"as_of_ts":"2026-10-03T08:40:00","horizon":1,"target_ts":"2026-10-03T08:45:00","r_pred":6.45e-06,"price_pred":84626.18,"price_q10":84595.46,"price_q90":84657.42,"actual_r":0.000104,"actual_price":84634.46,"hit":true,"model_version":"202610021348-4de49a70"}, ...],"has_more":true}
```

### Metrics

Definitions and live values: [metrics.md](metrics.md). Every route filters `slot = 'production'`.

| Route | Parameters | Response keys |
|---|---|---|
| `GET /metrics/{symbol}/summary` | | `windows` (`1h`, `24h`, `7d`, `30d`, `all`), each with `directional_accuracy`, `mae`, `rmse`, `delta_accuracy`, `n`, `sharpe`, `sharpe_n`; `updated_at` |
| `GET /metrics/{symbol}/coverage` | | `windows` -> `n`, `q10_coverage`, `q90_coverage`; `updated_at` |
| `GET /metrics/{symbol}/history` | `metric` (required: `accuracy` or `mae`), `window` (24h), `bucket` (1h) | `metric`, `window`, `bucket`, `points[{ts, value}]` |
| `GET /metrics/{symbol}/coverage_history` | `window` (24h), `bucket` (1h) | `points[{ts, q10_coverage, q90_coverage, avg_width, pinball_loss}]` |
| `GET /metrics/{symbol}/errors` | `window` (24h), `bins` (24, 1..100) | `bin_edges`, `counts`, `mean_error`, `std_error` (empty arrays and `null` if no data) |
| `GET /metrics/{symbol}/width_histogram` | `window`, `bins` | `bin_edges`, `counts`, `mean_width`, `std_width` |
| `GET /metrics/{symbol}/scatter` | `window` (24h), `limit` (200, 1..2000) | `points[{actual, predicted, q10, q90}]` (systematic sample), `r2` |
| `GET /metrics/{symbol}/latency` | `window` (24h) | `window`, `n`, `p50`, `p95`, `p99` (ms) |
| `GET /metrics/{symbol}/retrain-timeline` | `limit` (8, 1..100, per kind) | `point[]` and `quantile[]`, each event with versions, `decision`, `metric_name`, values, `threshold`, `n_samples`, `train_wall_seconds`, `decided_at` |

```bash
curl -s "https://pinance.katzer.ru/metrics/BTC%2FUSDT/summary"
```
```json
{"windows":{"1h":{"directional_accuracy":63.3,"mae":27.03,"rmse":32.52,"delta_accuracy":-20.7,"n":120,"sharpe":-31.418,"sharpe_n":10},"24h":{"directional_accuracy":52.4,"mae":126.22,"rmse":216.1,"delta_accuracy":-0.6,"n":3432,"sharpe":2.806,"sharpe_n":286}, ...},"updated_at":"2026-10-03T08:50:05.363891Z"}
```

```bash
curl -s "https://pinance.katzer.ru/metrics/BTC%2FUSDT/latency?window=24h"
```
```json
{"window":"24h","n":286,"p50":118.4,"p95":722.1,"p99":864.5}
```

One of the live retrain events ([`examples/live/retrain-timeline.json`](examples/live/retrain-timeline.json)):

```json
{"candidate_version":null,"production_version":"202610021348-4de49a70","decision":"rejected","metric_name":"avg_mae (rejected)","candidate_value":0.001745106294767002,"production_value":0.0017149575505334906,"threshold":0.005,"n_samples":8634,"train_wall_seconds":458.19509649276733,"decided_at":"2026-10-03T03:16:48.656552"}
```

### Error responses seen in practice

From [`examples/live/errors-sample.txt`](examples/live/errors-sample.txt) and neighbouring files:

| Request | Status | Body (shortened) |
|---|---|---|
| `/metrics/DOGE%2FUSDT/summary` | 400 | `{"detail":"Invalid symbol"}` |
| `/candles/BTC%2FUSDT?timeframe=2D` | 422 | `string_pattern_mismatch ... '^(1D|1W|1M|1Y|ALL)$'` |
| `/metrics/BTC%2FUSDT/history?metric=accuracy&window=24x` | 400 | `{"detail":"Invalid duration: '24x'"}` |
| `/metrics/BTC%2FUSDT/history?metric=accuracy&window=1h&bucket=2h` | 400 | `{"detail":"bucket must not be larger than window"}` |
| `/metrics/BTC%2FUSDT/history` | 422 | `Field required` for `metric` |
| `/candles/BTC%2FUSDT/pred_history?horizon=13` | 422 | `less_than_equal ... 12` |
| `/metrics/BTC%2FUSDT/drift` | 404 | `{"detail":"Not Found"}` (no such endpoint) |

## Internal routes (private network only)

Not reachable through the public domain. The application has no authentication of its own, see [design-decisions.md](design-decisions.md#10-the-admin-api-is-outside-the-public-surface).

### `GET /admin/metrics/{symbol}/compare?window=<label>`

Production versus candidate on the same resolved outcomes. Without `window`, all five (`1h`, `24h`, `7d`, `30d`, `all`) are computed; an unknown label is `400`. Per window: `production` and `candidate`, each a map `model_version -> {n, directional_accuracy, mae, rmse}`, plus `quantiles`, a map `slot -> quantile_model_version -> {n, q10_coverage, q90_coverage}`. A missing key means no resolved rows for that version in the window. Not cached.

Output of the real code on **synthetic** data ([`examples/admin_offline_demo.py`](examples/admin_offline_demo.py), [`examples/admin-offline/compare-24h.json`](examples/admin-offline/compare-24h.json)); the numbers mean nothing about any model:

```bash
curl -s '127.0.0.1:8099/admin/metrics/BTC%2FUSDT/compare?window=24h'
```
```json
{
    "symbol": "BTC/USDT",
    "windows": {
        "24h": {
            "production": {"202610020300-demoprod": {"n": 2946, "directional_accuracy": 58.6, "mae": 88.75, "rmse": 112.52}},
            "candidate":  {"202610030300-democand": {"n": 2946, "directional_accuracy": 58.6, "mae": 89.13, "rmse": 113.17}},
            "quantiles": {
                "production": {"q-202609270430": {"n": 2946, "q10_coverage": 0.3031, "q90_coverage": 0.7098}},
                "candidate":  {"q-202610030430": {"n": 2946, "q10_coverage": 0.2037, "q90_coverage": 0.8082}}
            }
        }
    },
    "updated_at": "2026-10-03T08:57:26.362882Z"
}
```

Errors: `window=3d` -> `400 {"detail":"Invalid window: '3d', expected one of ('1h', '24h', '7d', '30d', 'all')"}`.

### `POST /admin/retrain-events`

Appends one row to `retrain_events`; the caller (the training repository's promotion and retrain scripts) reports each decision. Returns `201 {"status":"created"}`. After the commit it deletes the cached `metrics:retrain_timeline:{symbol}:*` keys; if Redis is unavailable that step is skipped with a warning and the response is still `201`.

| Field | Type | Notes |
|---|---|---|
| `kind` | `"point"` or `"quantile"` | required |
| `symbol` | string | required, stored as sent (Binance form, `BTCUSDT`) |
| `decision` | `"promoted"` or `"rejected"` | required |
| `decided_at` | ISO datetime | required; converted to naive UTC |
| `candidate_version`, `production_version` | string, max 64 | optional |
| `metric_name` | string, max 128 | optional |
| `candidate_value`, `production_value`, `threshold` | float | optional |
| `n_samples` | int | optional |
| `train_wall_seconds` | float | optional |

```bash
curl -s -X POST '127.0.0.1:8099/admin/retrain-events' -H 'Content-Type: application/json' \
  -d '{"kind":"point","symbol":"BTCUSDT","candidate_version":"202610030300-democand","production_version":"202610020300-demoprod","decision":"rejected","metric_name":"directional_accuracy","candidate_value":52.1,"production_value":52.6,"threshold":0.3,"n_samples":2946,"train_wall_seconds":312.5,"decided_at":"2026-10-03T03:12:00Z"}'
# {"status":"created"}
```

An invalid `kind` returns `422` with `Input should be 'point' or 'quantile'` ([`examples/admin-offline/post-invalid.txt`](examples/admin-offline/post-invalid.txt)). The new row then shows up in `GET /metrics/BTC%2FUSDT/retrain-timeline` ([`examples/admin-offline/retrain-timeline.json`](examples/admin-offline/retrain-timeline.json)).

## Command-line tools

```bash
python -m app.backfill.run [--days N] [--symbols SYM ...]          # default: 2 days, all four symbols
python -m app.backfill.resolve_predictions [--batch-hours N] [--sleep S]   # default: 24 h batches, 1.0 s pause
```

`app.backfill.run` loads 5-minute candles from Binance REST (1,000 per request, 0.2 s pause) and upserts them. `resolve_predictions` fills `actual_price`, `actual_r`, `hit`, `sim_return` for old rows in batches by `as_of_ts`, both slots; it is safe to interrupt and restart. Both print progress. `entrypoint.sh` runs `alembic upgrade head` and `app.backfill.run --days 2` before starting the server.
