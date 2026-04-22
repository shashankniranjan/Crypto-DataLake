# High-Level Design: Crypto Data Scheduler

## 1. Purpose

This repository implements a local-first Binance USDT-M futures data platform centered on a canonical 1-minute parquet data lake. The system ingests Binance market data, enriches it with derived microstructure and positioning fields, materializes higher-timeframe bars, and exposes query APIs for candles and live indicators.

Status labels used in this document:

- **Observed**: directly visible in source code, configs, scripts, or tests.
- **Inferred**: a design intent strongly implied by implementation.
- **Unclear / needs confirmation**: not fully specified in the current repository.

## 2. System Scope

**Observed**

The repository contains four production runtime surfaces:

| Runtime surface | Entry point | Primary responsibility |
| --- | --- | --- |
| Minute lake ingestion CLI | `binance_minute_lake.cli.app:app` via `bml` | Build and maintain the canonical 66-column 1-minute parquet lake. |
| Live WebSocket ingestion mode | `bml run-live-forever` | Run Binance WebSocket consumers and merge live-only features into minute ingestion. |
| Live data API | `live_data_api_service.app:create_app` via FastAPI/Uvicorn | Serve `/api/v1/perpetual-data` and `/api/v1/live-indicators`. |
| Higher-timeframe aggregator | `python -m aggregator.main` | Read the minute lake and materialize local HTF parquet datasets. |

**Observed**

The project is packaged as `binance-minute-lake` and requires Python `>=3.11`. Runtime dependencies include `httpx`, `fastapi`, `polars`, `pyarrow`, `pydantic-settings`, `uvicorn`, `websockets`, `typer`, and `rich`.

## 3. High-Level Architecture

**Observed**

The system is organized as a data pipeline plus serving layer:

1. Source adapters fetch Binance data from REST, Vision daily ZIPs, and WebSocket streams.
2. `MinuteTransformEngine` normalizes source rows onto a UTC minute spine.
3. `DQValidator` enforces canonical columns, unique timestamps, and non-null hard-required fields.
4. `AtomicParquetWriter` writes hourly parquet partitions and records committed partitions in SQLite.
5. `AggregationService` reads minute parquet and writes higher-timeframe parquet partitions.
6. `LiveDataApiService` reads local parquet when available, optionally patches from Binance, overlays live features, aggregates timeframes, and serializes API responses.
7. `live_indicators` builds EMA and traditional pivot payloads on top of `LiveDataApiService`.

See [ARCHITECTURE_DIAGRAMS.md](ARCHITECTURE_DIAGRAMS.md) for Mermaid diagrams.

## 4. Major Components

### 4.1 Minute Lake Ingestion

**Observed**

Main files:

- `src/binance_minute_lake/pipeline/orchestrator.py`
- `src/binance_minute_lake/transforms/minute_builder.py`
- `src/binance_minute_lake/writer/atomic.py`
- `src/binance_minute_lake/state/store.py`
- `src/binance_minute_lake/validation/dq.py`
- `src/binance_minute_lake/validation/partition_audit.py`

The ingestion pipeline uses three data temperature bands:

| Band | Selection logic in code | Primary sources |
| --- | --- | --- |
| Hot | `age <= 6 hours` | REST plus optional live collector data |
| Warm | `age <= 7 days` | REST plus optional live collector data |
| Cold | older than 7 days | Binance Vision daily ZIPs, with optional REST fallback |

**Observed**

Each ingestion run:

1. Resolves `target_horizon = floor(now_utc - BML_SAFETY_LAG_MINUTES)`.
2. Reads the current watermark from `state/ingestion_state.sqlite`.
3. Computes missing minute ranges.
4. Processes each overlapping hour partition.
5. Fetches source data according to hot/warm/cold selection.
6. Builds a canonical frame.
7. Validates the frame.
8. Writes hourly parquet atomically.
9. Advances the watermark only after the partition write succeeds.

### 4.2 Canonical Data Model

**Observed**

The canonical schema is defined in `src/binance_minute_lake/core/schema.py` and currently has 66 columns:

- 12 `HARD_REQUIRED`
- 32 `BACKFILL_AVAILABLE`
- 22 `LIVE_ONLY`

Hard-required columns:

`timestamp`, `open`, `high`, `low`, `close`, `volume_btc`, `volume_usdt`, `trade_count`, `mark_price_open`, `mark_price_close`, `index_price_open`, `index_price_close`

**Observed**

The primary physical key is the parquet partition path plus `timestamp`. Symbol is implied by partition path.

### 4.3 Live WebSocket Feature Collection

**Observed**

Main file: `src/binance_minute_lake/sources/websocket.py`

The live subsystem supports:

- Depth diffs via `<symbol>@depth@100ms`
- Liquidations via `<symbol>@forceOrder`
- Aggregate trades via `<symbol>@aggTrade`
- Mark price via `<symbol>@markPrice@1s`

The collector maintains in-memory minute accumulators and can persist raw events plus minute-level live features to `state/live_events.sqlite`.

**Observed**

The WebSocket supervisor performs a REST depth snapshot resync before starting depth consumption, and resyncs again when depth continuity breaks.

### 4.4 Higher-Timeframe Aggregator

**Observed**

Main files:

- `src/aggregator/main.py`
- `src/aggregator/backfill.py`
- `src/aggregator/incremental.py`
- `src/aggregator/aggregation_rules.py`

The aggregator reads minute parquet from:

`data/futures/um/minute`

It writes HTF parquet to:

`data/futures/um/higher_timeframes`

It stores independent checkpoints in:

`state/aggregator_state.sqlite`

Supported HTFs are:

`3m`, `5m`, `10m`, `15m`, `30m`, `45m`, `1h`, `4h`, `8h`, `1d`, `1w`, `1M`

**Observed**

Startup mode scans existing HTF output, detects complete missing buckets from minute history, and writes only missing complete buckets unless incomplete buckets are explicitly allowed.

Continuous mode recomputes newly completable buckets plus a configurable repair lookback window.

### 4.5 Live Data API

**Observed**

Main files:

- `src/live_data_api_service/app.py`
- `src/live_data_api_service/service.py`
- `src/live_data_api_service/repository.py`
- `src/live_data_api_service/aggregation.py`
- `src/live_data_api_service/capabilities.py`

Endpoints:

| Endpoint | Purpose |
| --- | --- |
| `GET /healthz` | Liveness check. |
| `GET /api/v1/perpetual-data` | Return candles/enriched bars for requested timeframes. |
| `GET /api/v1/live-indicators` | Return EMA and traditional pivot calculations. |

**Observed**

The API supports local minute-lake reads, local HTF reads for BTC higher timeframes, Binance-native timeframe candles, on-demand canonical 1m patching, and live WebSocket feature overlays from a shared event store or API-managed WebSocket collectors.

### 4.6 Live Indicators

**Observed**

Main files:

- `src/live_indicators/service.py`
- `src/live_indicators/ema.py`
- `src/live_indicators/pivots.py`
- `src/live_indicators/aggregation.py`

The indicators endpoint computes:

- TradingView-style EMA with SMA seed.
- Traditional pivot levels `p`, `r1`, `r2`, `s1`, `s2`.

It delegates candle loading to `LiveDataApiService` and reuses shared bar/cache windows inside a request.

## 5. Data Architecture

### 5.1 Minute Lake Layout

**Observed**

Minute parquet path:

```text
data/futures/um/minute/
  symbol=BTCUSDT/
    year=YYYY/
      month=MM/
        day=DD/
          hour=HH/
            part.parquet
```

Partitions are hourly. The writer uses `zstd` parquet compression and writes through a temporary path under `data/.tmp` before atomic replacement.

### 5.2 Higher-Timeframe Layout

**Observed**

HTF parquet path:

```text
data/futures/um/higher_timeframes/
  timeframe=5m/
    symbol=BTCUSDT/
      year=YYYY/
        month=MM/
          day=DD/
            part.parquet
```

Partitions are daily by bucket start.

### 5.3 State Stores

**Observed**

SQLite databases:

| Database | Owner | Tables |
| --- | --- | --- |
| `state/ingestion_state.sqlite` | Minute ingestion | `watermark`, `partitions` |
| `state/live_events.sqlite` | WebSocket live store | `ws_events`, `ws_depth_events`, `ws_liq_events`, `ws_trade_events`, `consumer_heartbeats`, `minute_live_features` |
| `state/aggregator_state.sqlite` | HTF aggregator | `aggregation_state` |

**Inferred**

The local SQLite files are operational state, not intended to be committed as application source artifacts.

## 6. Runtime and Deployment Model

**Observed**

The repository ships local shell runners:

| Script | Purpose |
| --- | --- |
| `scripts/setup.sh` | Create virtualenv and install editable package plus dev tools. |
| `scripts/run-live.sh` | Start live WebSocket-backed minute ingestion. |
| `scripts/run-live-price-service.sh` | Start FastAPI live data service. |
| `scripts/run-aggregator.sh` | Start or run HTF aggregator. |
| `scripts/stop-aggregator.sh` | Stop background aggregator using PID file. |
| `scripts/stop-live-price-service.sh` | Stop service by port. |
| `scripts/run-intellij.sh` | Prepare DuckDB/IntelliJ view and run ingestion daemon. |

**Inferred**

The current deployment shape is single-host/local-first. The code and path design are compatible with future object-store migration, but no object-store writer is currently implemented.

## 7. Reliability and Data Correctness

**Observed**

Implemented correctness controls:

- Canonical schema registry with explicit support classes and dtypes.
- Non-null validation for `HARD_REQUIRED` fields.
- Unique timestamp validation per partition.
- Hour partition audit for backfill consistency checks.
- Atomic temp-write then replace for minute and HTF parquet writes.
- SQLite partition ledger with content and schema hashes for minute partitions.
- REST retry/backoff with jitter for retryable responses and transport errors.
- Client-side REST pacing in `BinanceRESTClient`.
- WebSocket depth continuity checks and resync path.
- Live-event retention cleanup with optional SQLite `VACUUM`.

**Observed**

The API has in-memory caches and in-flight request de-duplication for native timeframe fetches, canonical Binance patch windows, and current premium-index snapshots.

## 8. Observability

**Observed**

The code uses Python logging and CLI console output. The API records Binance REST usage via `binance_usage_scope`, `record_binance_rest_response`, retry counters, cache events, and estimated native candle request weight.

**Observed**

The aggregator script writes logs to `logs/aggregator.log` by default. Runtime state and logs are local files.

**Unclear / needs confirmation**

There is no repository-visible Prometheus exporter, structured log shipper, alert definition, dashboard, or SLO document.

## 9. Security and Operational Boundaries

**Observed**

No credentials are required for the implemented public Binance market data endpoints. Configuration is read from `.env` through `pydantic-settings`.

**Observed**

The API default bind address is `127.0.0.1:8080`.

**Unclear / needs confirmation**

Authentication, authorization, TLS termination, multi-tenant isolation, and network perimeter controls are not implemented in this repository.

## 10. Scalability Characteristics

**Observed**

Current scalability mechanisms:

- Hourly minute partitions limit rewrite size.
- Daily HTF partitions limit HTF rewrite size.
- Polars lazy scans are used for parquet reads.
- API parallelizes per-timeframe API fetches with `ThreadPoolExecutor`.
- Binance provider has parallel REST fetching.
- API caches stable windows longer than recent windows.
- HTF continuous mode recomputes a bounded repair lookback window.

**Inferred**

The system is optimized for a small number of symbols, with BTCUSDT as the primary local-first symbol. Multi-symbol production scaling would likely need process supervision, data root partition governance, per-symbol scheduling, and stronger rate-limit coordination.

## 11. Key Design Decisions

| Decision | Status | Evidence |
| --- | --- | --- |
| Local parquet is authoritative when local and remote rows overlap. | Observed | `merge_canonical_frames` is called with local rows winning in `LiveDataApiService.load_canonical_window`. |
| Minute lake remains read-only for HTF aggregator. | Observed | Aggregator reads from minute root and writes to separate `higher_timeframes` root. |
| Live-only fields may be absent historically. | Observed | Schema support classes and transform fill policies. |
| BTCUSDT is local-preferred with configurable Binance patching. | Observed | `BML_API_BTC_ALLOW_BINANCE_PATCH=true` allows missing BTC canonical minutes to be rebuilt from Binance; `BML_API_PERSIST_BINANCE_PATCHES=true` persists those patch rows into the minute lake. |
| Incomplete HTF buckets are skipped by default. | Observed | `HTF_ALLOW_INCOMPLETE_BUCKETS=false` default and filter logic in backfill/incremental runners. |
| API native timeframe candles are preferred for non-local symbols when enabled. | Observed | `FetchPlannerConfig` defaults and `plan_timeframe_fetch`. |

## 12. Known Gaps and Confirmation Items

**Unclear / needs confirmation**

- Target production deployment environment and process manager are not defined.
- No CI workflow files are present in the inspected repo.
- No explicit retention policy exists for parquet data.
- No backup/restore runbook exists for `state/*.sqlite` beyond README guidance.
- No migration framework exists for SQLite schema evolution.
- No authentication or rate limiting is implemented at the FastAPI boundary.
- No object-store implementation is present despite object-store-friendly layout.
- No explicit schema version field is stored inside parquet rows; the minute writer records a schema hash in SQLite partition ledger.
