# Crypto-Data-Scheduler

**Crypto-Data-Scheduler** is the production-grade monorepo that powers the Binance Minute Lake ingestion system. It maintains the 58-column canonical 1‑minute USD-M futures view via hybrid ingestion (WebSocket, REST, Vision) and enforces the schema, lineage, and consistency requirements described in the Requirements Addendum.

## Why this repo exists

- **Resilience & consistency** – schema-aware parquet commits with atomic writes, DQ gates, error retries, and ledgered partitions.
- **Hybrid sourcing** – live WebSocket ticks, REST snapshots, and Vision daily zips combine to cover hot, warm, and cold data windows.
- **Operational tooling** – CLI commands and daemon-friendly runners to inspect state, repair gaps, and keep partitions healthy.

## Repository layout

- `src/binance_minute_lake/core` – configuration, schema metadata, WAL-style logging helpers, and enums.
- `src/binance_minute_lake/sources` – adapters for REST, Vision, WebSocket, metrics inspectors, and backfill helpers.
- `src/binance_minute_lake/transforms` – minute-builder normalization logic that produces the canonical candlestick view.
- `src/binance_minute_lake/state` – SQLite watermark/partition store plus ledger APIs to track writes and repairs.
- `src/binance_minute_lake/writer` – atomic parquet writer with schema/content hashing and validation.
- `src/binance_minute_lake/pipeline` – orchestrator, collectors, scan/backfill helpers, and live ingestion wiring.
- `src/binance_minute_lake/cli` – Typer-based CLI for state inspection, retries, and audit-style backfills.
- `tests/` – targeted unit and integration coverage for schema handling, writers, orchestrators, and validation.
- `docs/` – implementation plan, requirements addendum, runbooks, and future work notes.

## Production-ready getting started

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
bml init-state      # create SQLite metadata and initial partitions
bml run-once        # verify all components can run end-to-end
PYTHONPATH=src bml run-forever  # poll every minute indefinitely (with rate limiting)
```

### Environment checklist

1. Install system dependencies listed in `REQUIREMENTS_ADDENDUM.md` (e.g., `libpq`, `rust` for `pyarrow` builds).
2. Copy `.env.example` to `.env` and audit values for credentials and S3 paths.
3. Use `poetry shell` or `source .venv/bin/activate` before running `bml` commands.
4. Validate `state/` and `logs/` directories are writable by the service account that will run the CLI.

## CLI reference

- `bml run-once` – executes a single minute job, writing parquet and booking ledger entries.
- `bml run-daemon --poll-seconds 60` – loops the minute job with configurable polling; use a process manager (systemd, supervisord) in prod.
- `PYTHONPATH=src bml run-forever` – alignment-aware infinite poller (adds rate limiting).
- `bml show-watermark` – displays the most recent timestamp and ledger position per symbol.
- `bml backfill-years --years 5 [--max-missing-hours N]` – consistency scan plus repair for the requested horizon.
- `bml backfill-range --start <ISO> --end <ISO>` – fill gaps for arbitrary ranges; schedules heavy compute work.
- `PYTHONPATH=src bml materialize-duckdb [--db-path data/minute.duckdb]` – build/update a DuckDB view over all parquet partitions for IDE/BI inspection.

Each backfill path scans existing partitions for gaps and invokes repairs for any missing hours (bounded by `--max-missing-hours` when provided).

## Observability & production guidance

- **Logging** – `logs/` contains tidy parquet writer logs; rotate them and ship to your aggregator.
- **Monitoring** – hook the CLI metrics (stored to the ledger) into Prometheus/Grafana dashboards.
- **Backups** – regularly snapshot the SQLite metadata under `state/` and validate ledger continuity before restoring.
- **Schema changes** – update `src/binance_minute_lake/core/schema.py` then regenerate docs in `docs/` and rerun `tests/schema`.
- **Data integrity** – run `bml backfill-years --years 1` after each schema change to ensure partitions remain consistent.

## Testing

```bash
pytest tests/
```

Focus areas: writer atomicity, schema validation, and orchestrator retries. Add new tests when altering data shaping logic.

## Maintenance

- Keep dependencies in sync via `pip install -e .[dev]` and `poetry lock` if you add packages.
- Document new pipelines inside `docs/` and register the requirements in `REQUIREMENTS_ADDENDUM.md`.
- Review `state/partition_store.sqlite` schema before modifying the ledger APIs.

## Support

Questions? Open an issue or reach out to the ops channel with `Crypto-Data-Scheduler` context.
