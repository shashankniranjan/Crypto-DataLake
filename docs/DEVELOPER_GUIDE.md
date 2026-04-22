# Developer Guide

## 1. Local Setup

**Observed**

Use the project bootstrap script:

```bash
./scripts/setup.sh
```

Manual equivalent:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .[dev]
cp .env.example .env
```

If your shell does not resolve `python`, use `.venv/bin/python` or `python3`.

## 2. Repository Map

**Observed**

| Path | Purpose |
| --- | --- |
| `src/binance_minute_lake/core` | Config, enums, schema registry, time helpers, Binance usage tracking. |
| `src/binance_minute_lake/sources` | REST, Vision, WebSocket, and metrics source adapters. |
| `src/binance_minute_lake/transforms` | Canonical 1-minute transform engine. |
| `src/binance_minute_lake/state` | Ingestion SQLite state store. |
| `src/binance_minute_lake/writer` | Atomic parquet writer. |
| `src/binance_minute_lake/validation` | DQ and partition audit helpers. |
| `src/binance_minute_lake/pipeline` | Minute ingestion orchestrator. |
| `src/binance_minute_lake/cli` | Typer CLI commands. |
| `src/aggregator` | Higher-timeframe materialization service. |
| `src/live_data_api_service` | FastAPI app, API service, repositories, Binance provider, aggregation. |
| `src/live_indicators` | EMA, pivot, and indicator payload logic. |
| `tests` | Unit and integration-style tests for core behavior. |
| `scripts` | Local setup and runtime scripts. |
| `docs` | Technical documentation. |

## 3. Daily Development Commands

**Observed**

```bash
source .venv/bin/activate

ruff check src tests
ruff format src tests
mypy src tests
pytest
```

The Makefile wraps these:

```bash
make lint
make format
make typecheck
make test
```

## 4. Running the System Locally

Minute ingestion:

```bash
PYTHONPATH=src bml init-state
PYTHONPATH=src bml run-once
PYTHONPATH=src bml run-forever
```

Live WebSocket-backed ingestion:

```bash
./scripts/run-live.sh
```

Live data API:

```bash
./scripts/run-live-price-service.sh
curl "http://127.0.0.1:8080/healthz"
curl "http://127.0.0.1:8080/api/v1/perpetual-data?coin=BTC&tfs=1m,5m&limit=5"
```

Higher-timeframe aggregator:

```bash
HTF_TIMEFRAMES=5m ./scripts/run-aggregator.sh --once
./scripts/run-aggregator.sh
./scripts/stop-aggregator.sh
```

DuckDB inspection:

```bash
PYTHONPATH=src bml materialize-duckdb --db-path data/minute.duckdb --symbol BTCUSDT
```

## 5. Development Principles for This Codebase

**Inferred from implementation**

- Treat `src/binance_minute_lake/core/schema.py` as the canonical contract.
- Do not bypass `DQValidator` for committed minute parquet.
- Keep local minute parquet authoritative over on-demand Binance rebuilds when timestamps overlap.
- Preserve live-only fields during partition rewrites.
- Prefer complete bars; incomplete HTF buckets are skipped by default.
- Keep REST enrichments best-effort unless they feed a hard-required field.
- Add tests before changing aggregation formulas, source parsing, or response routing.

## 6. Adding or Changing Canonical Columns

**Observed required touch points**

1. Update `src/binance_minute_lake/core/schema.py`.
2. Update `MinuteTransformEngine` in `src/binance_minute_lake/transforms/minute_builder.py`.
3. Update live feature schema if the column is live-only.
4. Update `AtomicParquetWriter` live preservation rules if needed.
5. Update API serialization or aggregation if the field is returned to clients.
6. Update HTF aggregation rules if the field should materialize above 1m.
7. Add or update tests:
   - `tests/test_schema_registry.py`
   - `tests/test_transform_engine.py`
   - `tests/test_atomic_writer.py`
   - `tests/test_live_data_api_service.py`
   - `tests/test_aggregator.py`

**Observed**

`build_live_feature_snapshot_frame` intentionally fails fast if `LiveMinuteFeatures` and `_LIVE_FEATURE_SCHEMA` drift.

## 7. Adding a New Source Endpoint

Recommended sequence:

1. Add a typed parser method to `BinanceRESTClient` or `VisionLoader`.
2. Add source rows to `_collect_and_transform` only where the source is available for the selected band.
3. Convert source rows into a Polars frame inside `MinuteTransformEngine`.
4. Add derived or fill behavior in `_derive_columns` or `_apply_fill_policies`.
5. Add tests with realistic payload shapes and missing-field cases.

**Observed**

REST optional enrichments are caught and logged as warnings. Preserve that behavior for non-hard-required enrichments.

## 8. Adding an API Timeframe

Required areas:

1. `src/live_data_api_service/timeframes.py` for request parsing.
2. `src/live_data_api_service/capabilities.py` for Binance interval mapping and support notes.
3. `src/live_data_api_service/aggregation.py` if the timeframe aggregates from 1m.
4. `src/aggregator/bucketing.py` if it should be materialized locally.
5. `src/live_indicators/timeframes.py` if indicators should support it.
6. Tests in `tests/test_live_data_api_service.py`, `tests/test_aggregator.py`, and `tests/test_live_indicators.py`.

**Observed**

API timeframes and aggregator timeframes are not identical. The API currently exposes `1m`, `3m`, `5m`, `15m`, `1hr`, and `4hr` for `/api/v1/perpetual-data`; the aggregator supports a broader set.

## 9. Working With State and Data Files

**Observed**

Runtime files may exist locally:

- `state/ingestion_state.sqlite`
- `state/live_events.sqlite`
- `state/aggregator_state.sqlite`
- `logs/aggregator.log`
- `data/futures/um/...`

Use CLI commands rather than editing SQLite manually:

```bash
PYTHONPATH=src bml show-watermark --symbol BTCUSDT
PYTHONPATH=src bml cleanup-live-state --event-db state/live_events.sqlite
```

**Inferred**

State DBs should be backed up before destructive or force-repair operations in a production environment.

## 10. Test Strategy

**Observed**

The repo has focused tests rather than end-to-end tests requiring live Binance access. When changing code:

- Add parser tests using fake payloads instead of calling Binance.
- Add transform tests using in-memory records and temporary parquet roots.
- Add API service tests with stub providers.
- Add aggregator tests with temporary minute partitions.
- Keep null-vs-zero semantics explicit for live-only fields.

Useful targeted runs:

```bash
pytest tests/test_transform_engine.py
pytest tests/test_live_data_api_service.py
pytest tests/test_aggregator.py
pytest tests/test_websocket_payload_processor.py
```

## 11. Code Quality Expectations

**Observed**

Ruff configuration enforces common Python correctness/style rules and import sorting. Mypy is configured in strict mode, though dependency availability and Python runtime version can affect local results.

Before merging functional changes, run:

```bash
ruff check src tests
pytest
```

Run mypy for interface-heavy changes:

```bash
mypy src tests
```

## 12. Developer Review Checklist

- Does the change preserve the 66-column canonical contract or intentionally migrate it?
- Are hard-required fields still non-null before commit?
- Does local data still win over remote data for overlapping timestamps?
- Are partial/incomplete bars handled deliberately?
- Are API metadata notes updated when source behavior changes?
- Are REST weight or retry implications understood?
- Are WebSocket null-vs-zero semantics preserved?
- Are partition rewrites idempotent?
- Are tests added for the behavior, not only the happy path?

## 13. Unclear Items for Future Developers

**Unclear / needs confirmation**

- Whether runtime state files should be gitignored or managed externally.
- Whether schema changes need a formal versioned migration process.
- Whether API response contracts are consumed by external clients that require backward-compatible field handling.
- Whether CI should enforce mypy, ruff, and pytest on every PR.

