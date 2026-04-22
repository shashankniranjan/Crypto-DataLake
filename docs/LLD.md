# Low-Level Design: Crypto Data Scheduler

## 1. Document Scope

This LLD describes the concrete implementation currently present in the repository. It is intentionally code-oriented and should be read with [HLD.md](HLD.md) and [ARCHITECTURE_DIAGRAMS.md](ARCHITECTURE_DIAGRAMS.md).

Status labels:

- **Observed**: directly supported by source, scripts, tests, or configs.
- **Inferred**: implementation implies this behavior.
- **Unclear / needs confirmation**: not fully specified in the repository.

## 2. Package and Runtime Entry Points

**Observed**

Package metadata is defined in `pyproject.toml`:

| Item | Value |
| --- | --- |
| Package name | `binance-minute-lake` |
| Python | `>=3.11` |
| CLI script | `bml = binance_minute_lake.cli.app:app` |
| API script | `live-data-api = live_data_api_service.app:main` |
| Packaged modules | `aggregator`, `binance_minute_lake`, `live_data_api_service`, `live_indicators` |

Primary commands are implemented in `src/binance_minute_lake/cli/app.py`:

| CLI command | Implementation behavior |
| --- | --- |
| `init-state` | Creates ingestion SQLite tables. |
| `show-watermark` | Prints current symbol watermark. |
| `cleanup-live-state` | Applies live-event retention cleanup and optional vacuum. |
| `run-once` | Runs one ingestion pass, optionally at a supplied UTC timestamp. |
| `run-daemon` | Runs polling loop with fixed sleep. |
| `run-forever` | Runs minute-aligned polling loop. |
| `run-live-forever` | Starts WebSocket supervisor plus minute ingestion loop. |
| `inspect-metrics-columns` | Downloads a Vision metrics ZIP and prints source columns. |
| `materialize-duckdb` | Creates DuckDB view over parquet. |
| `prepare-intellij` | DuckDB helper with SQL starter file. |
| `backfill-range` | Audits and repairs a supplied range. |
| `backfill-years` | Audits and repairs trailing N years. |
| `backfill-loader` | Interactive or flag-driven Vision-first historical repair. |

## 3. Configuration Model

### 3.1 Minute Lake Settings

**Observed**

`src/binance_minute_lake/core/config.py` defines `Settings` with `BML_` env prefix.

Important fields:

| Setting | Default | Purpose |
| --- | --- | --- |
| `BML_SYMBOL` | `BTCUSDT` | Ingestion symbol. |
| `BML_ROOT_DIR` | `./data` | Parquet root. |
| `BML_STATE_DB` | `./state/ingestion_state.sqlite` | Ingestion state DB. |
| `BML_VISION_BASE_URL` | Binance Vision futures daily URL | Cold data source. |
| `BML_REST_BASE_URL` | `https://fapi.binance.com` | REST source. |
| `BML_WEBSOCKET_BASE_URL` | `wss://fstream.binance.com/ws` | WS source. |
| `BML_SAFETY_LAG_MINUTES` | `3` | Prevents ingesting unstable latest minutes. |
| `BML_MAX_FFILL_MINUTES` | `60` | Snapshot forward-fill limit in transform. |
| `BML_BOOTSTRAP_LOOKBACK_MINUTES` | `120` | Initial watermark bootstrap window. |
| `BML_REST_MAX_RETRIES` | `5` | REST retries. |
| `BML_LIVE_EVENT_RETENTION_HOURS` | `12` | Raw live-event retention. |

### 3.2 API Settings

**Observed**

`src/live_data_api_service/config.py` defines `ApiServiceSettings` with `BML_API_` env prefix.

Important fields:

| Setting | Default | Purpose |
| --- | --- | --- |
| `BML_API_HOST` | `127.0.0.1` | API bind host. |
| `BML_API_PORT` | `8080` | API bind port. |
| `BML_API_DEFAULT_LIMIT` | `200` | Default bars per timeframe. |
| `BML_API_MAX_LIMIT` | `500` | Maximum requested bars. |
| `BML_API_ON_DEMAND_MAX_MINUTES` | `60480` | Cap for Binance 1m patch windows. |
| `BML_API_ENABLE_WS_WARMUP` | `false` | Whether API starts WS subscriptions itself. |
| `BML_API_ENABLE_NATIVE_BINANCE_TF_CANDLES` | `true` | Prefer Binance-native candle intervals. |
| `BML_API_CANDLE_FETCH_MODE` | `native_preferred` | Fetch planner mode. |
| `BML_API_LOCAL_PREFERRED_SYMBOLS` | `BTCUSDT` | Local-first symbols. |
| `BML_API_INCLUDE_DEPRECATED_FIELDS` | `false` | Emits legacy aliases only when enabled. |

### 3.3 Aggregator Settings

**Observed**

`src/aggregator/config.py` defines `AggregatorSettings` with `HTF_` env prefix.

| Setting | Default | Purpose |
| --- | --- | --- |
| `HTF_SYMBOL` | `BTCUSDT` | Aggregated symbol. |
| `HTF_SOURCE_ROOT` | `./data` | Minute lake root parent. |
| `HTF_TARGET_ROOT` | `./data/futures/um/higher_timeframes` | HTF output root. |
| `HTF_STATE_DB` | `./state/aggregator_state.sqlite` | Aggregator checkpoint DB. |
| `HTF_TIMEFRAMES` | `3m,5m,10m,15m,30m,45m,1h,4h,8h,1d,1w,1M` | Materialized HTFs. |
| `HTF_REPAIR_LOOKBACK_MINUTES` | `120` | Continuous recent repair window. |
| `HTF_ALLOW_INCOMPLETE_BUCKETS` | `false` | Controls incomplete HTF writes. |

## 4. Canonical Schema Registry

**Observed**

`src/binance_minute_lake/core/schema.py` defines `ColumnSpec` and `_CANONICAL_COLUMNS`. Current counts:

| Support class | Count |
| --- | ---: |
| `HARD_REQUIRED` | 12 |
| `BACKFILL_AVAILABLE` | 32 |
| `LIVE_ONLY` | 22 |
| Total | 66 |

Helper functions:

| Function | Purpose |
| --- | --- |
| `canonical_columns()` | Returns `ColumnSpec` tuple. |
| `canonical_column_names()` | Ordered canonical column list. |
| `hard_required_columns()` | Columns that must be non-null. |
| `dtype_map()` | Polars dtypes for canonical columns. |
| `schema_hash_input()` | Stable string used for schema hashing. |

**Observed**

Tests in `tests/test_schema_registry.py` assert the schema has 66 columns and includes required support classes.

## 5. Source Adapters

### 5.1 REST Client

**Observed**

`src/binance_minute_lake/sources/rest.py` implements `BinanceRESTClient`.

Low-level behavior:

- Uses a synchronous `httpx.Client`.
- Paces requests with `_min_interval_seconds = 0.1`.
- Retries transport errors, HTTP `429`, and `5xx`.
- Honors `Retry-After` if present.
- Uses exponential backoff capped at 60 seconds plus small jitter.
- Records REST usage and retry events through `binance_usage` helpers.

Implemented Binance endpoints:

| Method | Endpoint |
| --- | --- |
| `fetch_klines` | `/fapi/v1/klines` |
| `fetch_mark_price_klines` | `/fapi/v1/markPriceKlines` |
| `fetch_index_price_klines` | `/fapi/v1/indexPriceKlines` |
| `fetch_premium_index_klines` | `/fapi/v1/premiumIndexKlines` |
| `fetch_agg_trades` | `/fapi/v1/aggTrades` |
| `fetch_book_ticker` | `/fapi/v1/ticker/bookTicker` |
| `fetch_premium_index` | `/fapi/v1/premiumIndex` |
| `fetch_open_interest` | `/fapi/v1/openInterest` |
| `fetch_depth_snapshot` | `/fapi/v1/depth` |
| `fetch_top_trader_long_short_account_ratio` | `/futures/data/topLongShortAccountRatio` |
| `fetch_global_long_short_account_ratio` | `/futures/data/globalLongShortAccountRatio` |
| `fetch_top_trader_long_short_position_ratio` | `/futures/data/topLongShortPositionRatio` |
| `fetch_open_interest_hist` | `/futures/data/openInterestHist` |
| `fetch_funding_rate` | `/fapi/v1/fundingRate` |

### 5.2 Vision Client and Loader

**Observed**

`src/binance_minute_lake/sources/vision.py` defines Binance Vision URL patterns. `src/binance_minute_lake/sources/vision_loader.py` downloads daily ZIPs, caches them locally, and parses CSVs with Polars.

Supported Vision streams:

`klines`, `aggTrades`, `bookTicker`, `bookDepth`, `markPriceKlines`, `indexPriceKlines`, `premiumIndexKlines`, `metrics`, `trades`

Loader behavior:

- Builds expected daily ZIP URL.
- Checks `HEAD`, with range `GET` fallback for `403`/`405`.
- Caches ZIP files under `.cache/vision`.
- Stores `.missing` markers for absent files with a TTL.
- Tolerates extra CSV columns and creates missing expected columns as null.
- Exports filtered rows inside the requested millisecond window.

### 5.3 WebSocket Collector

**Observed**

`src/binance_minute_lake/sources/websocket.py` contains:

| Class | Responsibility |
| --- | --- |
| `LiveMinuteFeatures` | Data object for minute-level live features. |
| `LiveEventStore` | SQLite raw-event and minute-feature persistence. |
| `DepthOrderBook` | Local L2 book with snapshot/diff application and impact calculations. |
| `InMemoryLiveCollector` | Thread-safe accumulator for live depth, liquidation, trade, and funding events. |
| `BinanceWsPayloadProcessor` | Parses Binance WS payloads and calls collector methods. |
| `BinanceWebSocketWorker` | Reconnecting worker for one stream. |
| `BinanceLiveStreamSupervisor` | Starts depth, force-order, aggTrade, and mark-price workers. |

Live-event SQLite tables:

- `ws_events`
- `ws_depth_events`
- `ws_liq_events`
- `ws_trade_events`
- `consumer_heartbeats`
- `minute_live_features`

**Observed**

`InMemoryLiveCollector` calculates:

- p95 engine/network latency from depth events.
- `ws_latency_bad` when engine or network latency breaches 500 ms.
- min/max depth update IDs per minute.
- depth degradation flags using sync state, fillability, spread, and top-level depth thresholds.
- simulated `price_impact_100k`.
- liquidation notional/counts and weighted fill price.
- predicted funding and next funding time from mark-price stream.

## 6. Transform Engine

**Observed**

`MinuteTransformEngine.build_canonical_frame` in `src/binance_minute_lake/transforms/minute_builder.py`:

1. Creates a UTC minute spine for `[start_minute, end_minute]`.
2. Joins kline, mark-price kline, index-price kline, aggTrade, book ticker, funding, premium, metrics, long/short ratio, and live feature frames.
3. Derives canonical fields.
4. Applies forward-fill policies.
5. Casts and selects canonical schema order.

Important formulas and rules:

| Field family | Implementation |
| --- | --- |
| `vwap_1m` | `sum(price * qty) / sum(qty)` from aggTrades, fallback to close. |
| Aggressor side | `is_buyer_maker == false` is taker buy, true is taker sell. |
| Whale/retail | Whale notional `>= 100000`, retail notional `<= 1000`. |
| Realized volatility | `sqrt(sum(log_return^2))` within minute. |
| Microprice | `((bid_price * ask_qty) + (ask_price * bid_qty)) / (bid_qty + ask_qty)`. |
| LS ratios | Backward as-of join with 30 minute tolerance. |
| `premium_index` | `(mark_price_close / index_price_close) - 1` when denominator non-zero. |
| Snapshot ffill | `micro_price_close`, spread/depth metrics, OI, OI value, funding rate. |

**Observed**

Forward-fill limit is `BML_MAX_FFILL_MINUTES`, default 60.

## 7. Validation and Writes

### 7.1 DQ Validation

**Observed**

`DQValidator.validate` enforces:

- All canonical columns exist.
- No duplicate `timestamp` values.
- All `HARD_REQUIRED` columns are non-null.

It returns row count plus min/max timestamp strings for the ledger.

### 7.2 Minute Atomic Writer

**Observed**

`AtomicParquetWriter.write_hour_partition`:

1. Builds final hourly partition path.
2. If a file exists, reads it and merges with the new frame.
3. Deduplicates by `timestamp`, keeping the latest row.
4. Preserves existing live-only data where the new frame lacks it.
5. Runs DQ validation.
6. Writes temp parquet under `root/.tmp/{uuid}.parquet`.
7. Replaces final path atomically.
8. Computes schema hash and file content hash.
9. Upserts `partitions` ledger row.

**Observed**

Live-only preservation logic:

- `has_ws_latency`, `has_depth`, `has_liq` are OR-ed.
- Other live-only fields use `coalesce(new, existing)`.

## 8. Minute Ingestion Orchestrator

**Observed**

`MinuteIngestionPipeline` coordinates ingestion.

Key methods:

| Method | Behavior |
| --- | --- |
| `run_once` | Computes target horizon using safety lag and calls `run_until_target`. |
| `run_until_target` | Processes missing minutes from watermark to target, hour by hour. |
| `run_daemon` | Simple forever loop with sleep. |
| `scan_partition_consistency` | Runs partition audit over a range. |
| `run_consistency_backfill` | Repairs missing/invalid hour partitions. |
| `_collect_and_transform` | Fetches source rows and builds canonical frame. |
| `_choose_band` | Selects hot/warm/cold from window age. |

Hot/warm source behavior:

- Fetches REST klines, mark-price klines, index-price klines.
- Uses live aggTrades if available; hot can fall back to REST aggTrades, warm does not allow REST aggTrade fallback in `_fetch_agg_trades_live_or_rest` call because `allow_rest_fallback=(band == HOT)`.
- Fetches current book ticker, premium index, open interest, funding, and LS ratio enrichments best-effort.

Cold source behavior:

- Loads Vision klines, mark-price klines, index-price klines, aggTrades, bookTicker, metrics.
- Optionally fetches premium/open-interest REST enrichments.
- Falls back to REST for missing hard-required sources if `allow_rest_fallback` is true.

**Observed**

`run_consistency_backfill` can force cold-band operation, disable REST enrichment, and disable REST fallback. `backfill-loader --vision-only` uses those controls.

## 9. API Layer

### 9.1 FastAPI App

**Observed**

`src/live_data_api_service/app.py` creates a FastAPI app with:

- Lifespan startup/shutdown for optional WS prewarm and cleanup.
- Middleware that records response time and Binance usage for `/api/*` paths.
- `/healthz`
- `/api/v1/perpetual-data`
- `/api/v1/live-indicators`

### 9.2 Repositories

**Observed**

`MinuteLakeRepository.load_canonical_minutes`:

- Computes hourly partition paths for the requested window.
- Reads only existing parquet files.
- Filters by `timestamp`.
- Casts to canonical frame.

`HigherTimeframeRepository.load_candle_bars`:

- Normalizes timeframe aliases.
- Computes daily HTF partition paths.
- Filters complete buckets only.
- Converts HTF output back to canonical-like candle frame by dropping metadata and renaming `vwap` to `vwap_1m`, `realized_vol_htf` to `realized_vol_1m`.

### 9.3 Fetch Planning

**Observed**

`src/live_data_api_service/capabilities.py` defines the source planner.

Planner modes:

- `native_preferred`
- `aggregate_from_1m`
- `auto`

Native candle intervals currently mapped from API names:

| API | Binance interval |
| --- | --- |
| `1m` | `1m` |
| `3m` | `3m` |
| `5m` | `5m` |
| `15m` | `15m` |
| `1hr` | `1h` |
| `4hr` | `4h` |

### 9.4 `LiveDataApiService`

**Observed**

`src/live_data_api_service/service.py` is the central API coordinator.

Important behavior:

- Normalizes symbols through `normalize_symbol`.
- Resolves omitted `end_time` to last completed UTC minute, bounded by local watermark when close enough.
- Reads local minute parquet first for local-preferred symbols.
- Treats `BTCUSDT` as local-only in `load_canonical_window`, disabling Binance patching.
- For BTC higher timeframes above `3m`, tries local HTF parquet first.
- For non-local symbols, uses native Binance timeframe candles when enabled.
- Supports legacy aggregation from canonical 1m windows.
- Overlays live features when shared or API-managed collectors are available.
- Serializes response timestamps in UTC ISO-8601.

Caches and de-duplication:

| Cache | Key | Purpose |
| --- | --- | --- |
| Timeframe cache | symbol, timeframe, limit, end_time | Avoid duplicate native candle fetches. |
| Timeframe in-flight map | same | Coalesce concurrent identical requests. |
| Canonical patch cache | symbol, start, end, live flag | Avoid duplicate on-demand 1m patch fetches. |
| Premium snapshot cache | symbol | Short TTL cache for current premium-index snapshot. |

**Observed**

Stable timeframe windows use a longer TTL than recent windows. Default constructor values are 6 hours for stable windows and 15 seconds for recent windows.

### 9.5 API Aggregation

**Observed**

`src/live_data_api_service/aggregation.py` aggregates canonical 1m frames into API timeframes:

- Uses Polars dynamic grouping.
- Emits only complete windows by requiring minute count equal to timeframe width.
- Recomputes VWAP, average trade size, taker ratio, premium index, LS divergence, liquidation weighted price, and realized volatility.
- Adds API-only fields such as `global_long_pct`, USD position estimates, and `cvd_btc`.

**Observed**

Deprecated aliases `vwap_1m` and `realized_vol_1m` are hidden by default at serialization time unless `BML_API_INCLUDE_DEPRECATED_FIELDS=true`.

## 10. Higher-Timeframe Aggregator

### 10.1 Bucketing

**Observed**

`src/aggregator/bucketing.py` defines deterministic bucket alignment:

- Minute intervals floor to multiples inside the UTC day.
- `1h`, `4h`, and `8h` floor to UTC hour boundaries.
- `1d` floors to UTC midnight.
- `1w` floors to Monday 00:00 UTC.
- `1M` floors to first day of month 00:00 UTC.

### 10.2 Aggregation Rules

**Observed**

`src/aggregator/aggregation_rules.py` contains the rule table and implementation.

Major rules:

| Field type | Rule |
| --- | --- |
| OHLC | First open, max high, min low, last close. |
| Volumes/counts | Sum. |
| VWAP | `sum(volume_usdt) / sum(volume_btc)`. |
| Snapshot fields | Last non-null in bucket. |
| Mark/index open | First non-null. |
| Mark/index close | Last non-null. |
| Spread/depth/impact averages | Volume-weighted by `volume_usdt`, fallback simple mean. |
| Boolean coverage | OR. |
| Realized volatility | Recomputed from close-to-close log returns inside bucket. |
| Metadata | Timeframe, symbol, bucket start/end, expected/observed/missing minutes, complete flag. |

### 10.3 Backfill and Incremental Modes

**Observed**

`BackfillRunner`:

- Scans available source minutes.
- Computes complete missing buckets absent from existing HTF output.
- Coalesces adjacent bucket starts into source read windows.
- Writes complete buckets unless incomplete buckets are allowed.
- Updates `aggregation_state`.

`IncrementalRunner`:

- Reads latest source minute.
- Uses prior state plus repair lookback to choose start.
- Reaggregates current work window.
- Rewrites complete buckets idempotently.
- Tracks lag in minutes.

## 11. Live Indicators

**Observed**

`build_indicator_payload`:

- Resolves EMA and pivot timeframe specs.
- Requests EMA warmup bars using `max(length * 3, length + 20)`.
- Uses prior completed pivot period.
- Runs EMA and pivot calculations concurrently.
- Reuses an in-request shared bar cache.
- For BTC higher timeframes, can use local HTF lake.

EMA implementation:

- `calculate_tradingview_ema` seeds with SMA over the first `length` values.
- Then applies `alpha = 2 / (length + 1)`.

Traditional pivot implementation:

- `p = (high + low + close) / 3`
- `r1 = 2p - low`
- `r2 = p + (high - low)`
- `s1 = 2p - high`
- `s2 = p - (high - low)`

## 12. Testing Traceability

**Observed**

The test suite covers the following areas:

| Test file | Coverage focus |
| --- | --- |
| `tests/test_schema_registry.py` | Canonical schema count, support classes, hard-required columns. |
| `tests/test_transform_engine.py` | Canonical transform output, ffill, OI metrics, funding seed, LS ratio, live semantics. |
| `tests/test_atomic_writer.py` | Partition creation, merge behavior, live-column preservation. |
| `tests/test_state_store.py` | Watermark and partition ledger roundtrip. |
| `tests/test_partition_audit.py` | Missing file, full-hour validation, gaps, required columns. |
| `tests/test_rest_client.py` | REST weight, retry behavior, endpoint parsing, usage tracking. |
| `tests/test_vision_loader.py` | Vision CSV parsing, missing ZIP cache, metrics columns. |
| `tests/test_websocket_payload_processor.py` | WS payload ingestion, raw store writes, cleanup, connection handling. |
| `tests/test_live_collector.py` | Depth sync, degradation, liquidation semantics, latency nulls, event-store recovery. |
| `tests/test_orchestrator_live_agg_trades.py` | Live aggTrade preference and warm-band fallback behavior. |
| `tests/test_live_data_api_service.py` | API aggregation, routing, native planner, caching, BTC local path, endpoints. |
| `tests/test_live_indicators.py` | EMA, pivots, shared canonical windows, local HTF for BTC. |
| `tests/test_aggregator.py` | Missing buckets, incremental repair, idempotency, aggregation correctness. |

## 13. Error Handling and Failure Modes

**Observed**

| Area | Behavior |
| --- | --- |
| REST transient failures | Retries on transport error, `429`, and `5xx`; raises on final failure. |
| Optional enrichments | Logged as warnings and omitted/null-filled if unavailable. |
| DQ validation failure | Raises `DataQualityError`; writer does not commit. |
| Backfill repair failure | Counted as failed and logged; other targeted hours continue. |
| API invalid input | Raises `ValueError`, converted to HTTP 400. |
| API missing huge local window | Raises value error if too large for on-demand Binance retrieval. |
| WebSocket depth break | Marks degraded and triggers snapshot resync. |
| Aggregator no source minutes | Returns zero-work result. |

## 14. Unclear or Missing Low-Level Contracts

**Unclear / needs confirmation**

- Exact production process supervisor and restart policy.
- Expected maximum symbol count per deployment.
- Operational alert thresholds for lag, missing partitions, REST errors, and stale live features.
- SQLite migration policy.
- Whether local runtime state files should be excluded from version control.
- Whether the API should enforce authentication or request throttling before external exposure.

