# Crypto-Data-Scheduler

Crypto-Data-Scheduler is the monorepo that houses the Binance Minute Lake ingestion system, a production-grade Python framework that locks in the 58-column canonical 1-minute view of USD-M futures via a blend of WebSocket, REST, and Vision sources. It codifies the Requirements Addendum (https://github.com/???) and delivers tooling for hot polling, historical repair, and reliable parquet partitioning.

## Highlights

- **Hybrid ingestion:** WebSocket/REST for hot/warm and Vision daily zips for cold backfill.
- **Consistency-first writes:** Atomic parquet commits with DQ validation and partition ledger tracking.
- **Schema discipline:** Central metadata describes every column’s source, support class, and fill policy.
- **Operational CLI:** `bml run-once`, `run-daemon`, and `backfill-*` commands plus Airflow orchestration.

## Workspace structure

- `src/binance_minute_lake/core`: configuration, schema metadata, enums, logging helpers.
- `src/binance_minute_lake/sources`: REST, Vision, websocket, metrics inspector, and related adapters.
- `src/binance_minute_lake/transforms`: minute-builder logic that normalizes ticks/snapshots to the canonical view.
- `src/binance_minute_lake/state`: SQLite watermark/partition store plus ledger APIs.
- `src/binance_minute_lake/writer`: atomic parquet writer with content/schema hashing.
- `src/binance_minute_lake/pipeline`: orchestrator, scan/backfill helpers, and live collector wiring.
- `src/binance_minute_lake/cli`: Typer CLI for state inspection, ad-hoc runs, and audit-style backfills.
- `airflow/`: DAGs, scripts, and docs for running the ingestion via Airflow.
- `tests/`: targeted unit coverage for schema/enums/orchestrator/writer/validation behaviors.
- `docs/`: implementation plan, requirements addendum, and future work notes.

## Quickstart

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
cp .env.example .env
bml init-state
bml run-once
```

## Operational commands

```bash
bml run-once
bml run-daemon --poll-seconds 60
bml show-watermark
bml backfill-years --years 5
bml backfill-years --years 5 --max-missing-hours 24
bml backfill-range --start 2021-02-15T00:00:00+00:00 --end 2026-02-14T23:59:00+00:00
```

Each backfill command now performs a full consistency scan over existing partitions and repairs every gap it locates (capped via `--max-missing-hours` when requested).

## Airflow support

Airflow runs inside `.venv-airflow`, uses SQLite metadata, and exposes two DAGs (`bml_minute_incremental` and `bml_historical_backfill`) that call `bml run-once` or the consistency backfill CLI respectively.

Start the scheduler:

```bash
cd "/Users/shashankniranjan/Documents/New project"
./airflow/scripts/start_scheduler.sh
```

In another terminal, start the webserver:

```bash
cd "/Users/shashankniranjan/Documents/New project"
./airflow/scripts/start_webserver.sh
```

Open [http://localhost:8080](http://localhost:8080) and log in with `admin` / `admin`.

## Notes

- WebSocket collectors are optional in this baseline; live-only fields stay `NULL` when no collector is attached.
- Vision and REST downloads include retry backoff, rate-limit handling, and provenance logging so quality is traceable.
