# Operations Guide

## 1. Runtime Processes

**Observed**

This repository can run three long-lived local processes:

| Process | Start command | Stop command |
| --- | --- | --- |
| Minute ingestion with live WS | `./scripts/run-live.sh` | `Ctrl+C` in foreground or process manager stop |
| Live data API | `./scripts/run-live-price-service.sh` | `./scripts/stop-live-price-service.sh [port]` |
| Higher-timeframe aggregator | `./scripts/run-aggregator.sh` | `./scripts/stop-aggregator.sh` |

A non-live minute ingestion daemon is also available:

```bash
PYTHONPATH=src bml run-forever
```

## 2. Filesystem Layout

**Observed**

Important paths:

| Path | Owner | Notes |
| --- | --- | --- |
| `data/futures/um/minute` | Minute ingestion | Canonical hourly parquet partitions. |
| `data/futures/um/higher_timeframes` | HTF aggregator | Daily HTF parquet partitions. |
| `state/ingestion_state.sqlite` | Minute ingestion | Watermark and partition ledger. |
| `state/live_events.sqlite` | Live WS collector and API overlay | Raw WS events, heartbeats, minute live features. |
| `state/aggregator_state.sqlite` | HTF aggregator | Per-symbol/timeframe checkpoint state. |
| `logs/aggregator.log` | Aggregator script | Default background log. |

**Inferred**

In production, `data/`, `state/`, and `logs/` should be on durable storage with explicit backup and retention policies.

## 3. Environment Variables

### 3.1 Ingestion

Common `BML_` variables:

```bash
BML_SYMBOL=BTCUSDT
BML_ROOT_DIR=./data
BML_STATE_DB=./state/ingestion_state.sqlite
BML_SAFETY_LAG_MINUTES=3
BML_MAX_FFILL_MINUTES=60
BML_REST_TIMEOUT_SECONDS=20
BML_REST_MAX_RETRIES=5
BML_LOG_LEVEL=INFO
```

### 3.2 API

Common `BML_API_` variables:

```bash
BML_API_HOST=127.0.0.1
BML_API_PORT=8080
BML_API_DEFAULT_LIMIT=200
BML_API_MAX_LIMIT=500
BML_API_ENABLE_NATIVE_BINANCE_TF_CANDLES=true
BML_API_CANDLE_FETCH_MODE=native_preferred
BML_API_LOCAL_PREFERRED_SYMBOLS=BTCUSDT
BML_API_ENABLE_WS_WARMUP=false
```

### 3.3 Aggregator

Common `HTF_` variables:

```bash
HTF_SYMBOL=BTCUSDT
HTF_SOURCE_ROOT=./data
HTF_TARGET_ROOT=./data/futures/um/higher_timeframes
HTF_STATE_DB=./state/aggregator_state.sqlite
HTF_TIMEFRAMES=3m,5m,10m,15m,30m,45m,1h,4h,8h,1d,1w,1M
HTF_REPAIR_LOOKBACK_MINUTES=120
HTF_ALLOW_INCOMPLETE_BUCKETS=false
```

## 4. Startup Procedures

### 4.1 First-Time Setup

```bash
./scripts/setup.sh
PYTHONPATH=src bml init-state
```

### 4.2 Minute Lake Ingestion

One-off validation:

```bash
PYTHONPATH=src bml run-once
PYTHONPATH=src bml show-watermark --symbol BTCUSDT
```

Continuous live ingestion:

```bash
./scripts/run-live.sh
```

The live runner performs one live-state cleanup before starting `bml run-live-forever`.

### 4.3 Higher-Timeframe Aggregator

Validate one timeframe:

```bash
HTF_TIMEFRAMES=5m ./scripts/run-aggregator.sh --once
```

Start background process:

```bash
./scripts/run-aggregator.sh
```

The script writes a PID file to `state/aggregator.pid` and streams `logs/aggregator.log`.

### 4.4 API Service

```bash
./scripts/run-live-price-service.sh 127.0.0.1 8080 data
curl "http://127.0.0.1:8080/healthz"
```

Example data request:

```bash
curl "http://127.0.0.1:8080/api/v1/perpetual-data?coin=BTC&tfs=1m,5m&limit=5"
```

Example indicator request:

```bash
curl "http://127.0.0.1:8080/api/v1/live-indicators?coin=BTC&ema_tf=5m&ema_length=21&pivot_tf=1d"
```

## 5. Backfill and Repair

### 5.1 Arbitrary Range Repair

```bash
PYTHONPATH=src bml backfill-range \
  --start 2026-04-01T00:00:00Z \
  --end 2026-04-02T00:00:00Z \
  --max-missing-hours 24
```

Force rebuild every hour in a range:

```bash
PYTHONPATH=src bml backfill-range \
  --start 2026-04-01T00:00:00Z \
  --end 2026-04-02T00:00:00Z \
  --force-repair
```

### 5.2 Historical Vision-First Repair

```bash
PYTHONPATH=src bml backfill-loader --year 2024 --vision-only
PYTHONPATH=src bml backfill-loader --last-5-years --max-missing-hours 100
```

Use REST fallback only when acceptable:

```bash
PYTHONPATH=src bml backfill-loader --year 2024 --allow-rest-fallback
```

**Observed**

Vision-only mode clamps the range to completed UTC days and disables REST fallback/enrichment.

## 6. Health Checks and Validation

### 6.1 API Health

```bash
curl "http://127.0.0.1:8080/healthz"
```

Expected response:

```json
{"status":"ok"}
```

### 6.2 Watermark

```bash
PYTHONPATH=src bml show-watermark --symbol BTCUSDT
```

The watermark should trail current UTC time by roughly the configured safety lag during normal operation.

### 6.3 Partition Consistency

Use backfill commands without `--force-repair` to audit and repair only invalid/missing hours. The CLI reports:

- `hours_scanned`
- `issues_found`
- `issues_targeted`
- `hours_repaired`
- `hours_failed`
- `issues_remaining`

### 6.4 Aggregator Lag

**Observed**

`IncrementalRunner` computes `lag_minutes` and logs it. Inspect `logs/aggregator.log`:

```bash
tail -n 100 logs/aggregator.log
```

## 7. Retention and Cleanup

### 7.1 Live Event Store

Run cleanup manually:

```bash
PYTHONPATH=src bml cleanup-live-state \
  --event-db state/live_events.sqlite \
  --event-retention-hours 12 \
  --heartbeat-retention-days 14
```

Reclaim SQLite file size:

```bash
PYTHONPATH=src bml cleanup-live-state --event-db state/live_events.sqlite --vacuum
```

**Observed**

`bml run-live-forever` also runs periodic cleanup and periodic vacuum based on `BML_LIVE_CLEANUP_INTERVAL_MINUTES` and `BML_LIVE_CLEANUP_VACUUM_INTERVAL_HOURS`.

### 7.2 Parquet Retention

**Unclear / needs confirmation**

No parquet retention or compaction policy is implemented in the repository.

## 8. Backup and Restore

**Inferred operational recommendation**

Back up before major repairs:

```bash
mkdir -p backups
cp state/ingestion_state.sqlite "backups/ingestion_state.$(date -u +%Y%m%dT%H%M%SZ).sqlite"
cp state/aggregator_state.sqlite "backups/aggregator_state.$(date -u +%Y%m%dT%H%M%SZ).sqlite"
```

For `state/live_events.sqlite`, include WAL/SHM files if the process is running, or stop the writer before copying:

```bash
cp state/live_events.sqlite* backups/
```

**Unclear / needs confirmation**

There is no tested restore playbook in the repository.

## 9. Common Failure Modes

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `HARD_REQUIRED null violations` | Missing kline/mark/index source rows | Run bounded backfill; check Binance source availability and REST errors. |
| Watermark not advancing | Earlier partition failed validation or source fetch failed | Check CLI output/logs, run `backfill-range` over stuck window. |
| API returns local unavailable notes for BTC | Local minute or HTF data missing | Run ingestion/backfill and aggregator. |
| API native Binance errors | REST failure or rate limit | Check logs for usage summary, retry messages, and request weight estimates. |
| `has_depth=false` or live fields null | Live WS collector not running or no shared live DB | Start `run-live.sh`; verify `state/live_events.sqlite` exists. |
| Aggregator says already running | Stale or active PID file | Run `./scripts/stop-aggregator.sh`; inspect PID file and logs. |
| Aggregator lag grows | Minute lake behind, aggregator failing, or target root unwritable | Check watermark, `logs/aggregator.log`, and filesystem permissions. |

## 10. Monitoring Recommendations

**Observed signals available today**

- Ingestion watermark in SQLite.
- Partition ledger row counts and content hashes.
- API `X-Response-Time-Secs` header.
- API response `response_time_secs`.
- API `timeframe_metadata`.
- API Binance usage logs.
- Aggregator `lag_minutes` log field.
- Live consumer heartbeats in `state/live_events.sqlite`.

**Inferred production monitors**

Add alerts for:

- Watermark lag above configured threshold.
- Missing or invalid hourly partitions.
- Aggregator lag above one or two target bucket widths.
- API p95/p99 latency.
- REST retry spikes or 429 responses.
- Stale `minute_live_features` for expected live symbols.
- Disk utilization for `data/`, `state/`, and `logs/`.

## 11. Production Hardening Gaps

**Unclear / needs confirmation**

- Process supervisor is not defined.
- No external metrics exporter exists.
- No alert configuration exists.
- No TLS/auth boundary exists for the API.
- No documented disaster recovery target exists.
- No object-store sink exists.
- No database migration framework exists.

