# Crypto-Data-Scheduler

**Crypto-Data-Scheduler** is the production-grade monorepo that powers the Binance Minute Lake ingestion system. It maintains the 66-column canonical 1‑minute USD-M futures view via hybrid ingestion (WebSocket, REST, Vision) and enforces the schema, lineage, and consistency requirements described in the Requirements Addendum.

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

## Minute Lake schema and live API field reference

The minute lake stores one UTC-aligned canonical row per symbol/minute in parquet. The live price service (`/api/v1/perpetual-data`) starts from those canonical minutes, optionally rebuilds missing minutes on demand from Binance, overlays in-memory WebSocket live features, and then aggregates to the requested timeframes (`1m`, `3m`, `5m`, `15m`, `1hr`, `4hr`).

Top-level API response fields are:

- `symbol` – normalized symbol (`BTC` becomes `BTCUSDT`)
- `timeframes` – normalized timeframe list (`1h`/`4h` aliases return as `1hr`/`4hr`)
- `limit` – number of bars returned per timeframe
- `end_time` – resolved UTC bar end used for the query
- `source` – `local`, `binance`, `binance_native`, `local+binance`, or local-preferred live variants such as `local+live`
- `response_time_secs` – end-to-end HTTP handler latency
- `data` – map of timeframe to bar array

### Source, fill, and aggregation rules

- Closed UTC days are primarily built from Binance Vision daily zips; the current UTC day is built from REST endpoints.
- If local parquet does not fully cover the request and the window is small enough, the API rebuilds missing minutes on demand from Binance. When local and remote rows overlap on the same `timestamp`, the local minute-lake row wins.
- Live-only fields are overlaid from the in-memory WebSocket collector (or `state/live_events.sqlite` for the shared symbol): boolean coverage flags are OR-ed, and nullable live fields prefer the live non-null value.
- `HARD_REQUIRED` fields must be present for a minute to commit. `BACKFILL_AVAILABLE` fields are best-effort enrichments with the per-column fill/default behavior declared in `src/binance_minute_lake/core/schema.py`. `LIVE_ONLY` fields are null historically and when no live collector data exists.
- Snapshot-style fields such as order-book summary stats, open interest, and funding are forward-filled in the minute builder for up to `max_ffill_minutes` (default 60). Before higher-timeframe aggregation, the API also forward-fills funding/OI/ratio snapshots so each completed bar carries the latest known value.
- Funding and OI loaders seed the window with the latest known value before the requested range when needed so forward-fill works even if the window itself has no explicit funding/OI update.
- Open interest prefers historical `openInterestHist` (5m) when available; otherwise the API falls back to a single REST open-interest snapshot.
- The API only returns complete windows. A `5m` bar is emitted only when all 5 constituent `1m` rows are present.
- Even `1m` API responses go through the aggregation pipeline. Raw parquet is authoritative for the stored minute row; the API recomputes some derived fields such as `avg_trade_size_btc`, `taker_buy_ratio`, `premium_index`, `ls_ratio_divergence`, and the service-only metrics listed below.

### Calculation constants used in the code

- Whale trade threshold: aggTrade notional `>= 100000 USDT`
- Retail trade threshold: aggTrade notional `<= 1000 USDT`
- Bad WS latency threshold: `500 ms`
- Price impact basket: simulated aggressive buy of `100000 USDT`
- Depth degraded when any of the following is true: book sync broke, no synchronized depth diff was applied for the minute, `impact_fillable=false`, spread is above `2%`, or average top-10 bid/ask quantity drops below `1.0`
- Long/short ratio alignment: backward as-of join with `30 minute` tolerance
- Timestamps are serialized by the API in UTC ISO-8601 with millisecond precision

### Coverage, latency, and depth continuity fields

| Field | Minute lake capture / calculation | Live API behavior |
| --- | --- | --- |
| `has_ws_latency` | `true` when at least one depth-diff WS event in the minute has `event_time`, `transact_time`, and `arrival_time`; else `false` | Logical OR across the window |
| `has_depth` | `true` when at least one depth-diff event exists in the minute; else `false` | Logical OR across the window |
| `has_liq` | `true` when at least one `forceOrder` event exists in the minute; else `false` | Logical OR across the window |
| `has_ls_ratio` | `true` when ratio enrichment is present for the row, or when the live collector marks ratio coverage | `true` only when the returned bar has populated LS-ratio fields |
| `event_time` | Latest WS depth `event_time` in the minute when latency samples exist; otherwise `NULL` | `max()` |
| `transact_time` | Historical rows use the max aggTrade `transact_time`; live overlay can replace it with the latest WS depth `transact_time` for the minute | `max()` |
| `arrival_time` | Latest local capture timestamp for an eligible WS latency sample in the minute | `max()` |
| `latency_engine` | `p95(arrival_time - event_time)` in milliseconds across eligible depth WS events in the minute | `p95()` across minute values in the window |
| `latency_network` | `p95(arrival_time - transact_time)` in milliseconds across eligible depth WS events in the minute | `p95()` across minute values in the window |
| `ws_latency_bad` | `true` if any eligible WS latency sample in the minute breaches `500 ms`; `NULL` when `has_ws_latency=false` | Logical OR when the window has latency data; otherwise `NULL` |
| `update_id_start` | Minimum depth diff `U` / first update id observed in the minute | `min()` |
| `update_id_end` | Maximum depth diff `u` / final update id observed in the minute | `max()` |

### Core bar and basis fields

| Field | Minute lake capture / calculation | Live API behavior |
| --- | --- | --- |
| `timestamp` | UTC minute boundary from the minute spine / kline `open_time` truncated to `1m` | Left edge of the aggregated window |
| `open` | Kline open | First minute's `open` |
| `high` | Kline high | `max()` |
| `low` | Kline low | `min()` |
| `close` | Kline close | Last minute's `close` |
| `mark_price_open` | Mark-price kline open | First minute's `mark_price_open` |
| `mark_price_close` | Mark-price kline close | Last minute's `mark_price_close` |
| `index_price_open` | Index-price kline open | First minute's `index_price_open` |
| `index_price_close` | Index-price kline close | Last minute's `index_price_close` |
| `premium_index` | `(mark_price_close / index_price_close) - 1.0` when `index_price_close != 0`; else `NULL` | Recomputed from aggregated `mark_price_close` and `index_price_close` |

### Volume, aggressor flow, and trade-shape fields

| Field | Minute lake capture / calculation | Live API behavior |
| --- | --- | --- |
| `volume_btc` | Base-asset volume from klines | `sum()` |
| `volume_usdt` | Quote/notional volume from klines | `sum()` |
| `trade_count` | Trade count from klines | `sum()` |
| `vwap_bar` | API field for bar VWAP. For minute-lake rows this is derived from stored `vwap_1m`; for higher-timeframe API bars it is timeframe-neutral. | `sum((minute_vwap_or_close * volume_btc)) / sum(volume_btc)`; falls back to `close` if aggregated volume is zero |
| `avg_trade_size_btc` | `volume_btc / trade_count`; `0` when `trade_count=0` | Recomputed from aggregated `volume_btc / trade_count` |
| `max_trade_size_btc` | Maximum aggTrade `qty` in the minute | `max()` |
| `taker_buy_vol_btc` | Kline taker-buy base volume (schema allows aggTrade fallback, current builder uses the kline field directly) | `sum()` |
| `taker_buy_vol_usdt` | Kline taker-buy quote volume | `sum()` |
| `taker_sell_vol_btc` | Derived sell-side taker base volume | `volume_btc - taker_buy_vol_btc` |
| `taker_sell_vol_usdt` | Derived sell-side taker quote volume | `volume_usdt - taker_buy_vol_usdt` |
| `net_taker_vol_btc` | `sum(buy_qty) - sum(sell_qty)` from aggTrades, where buys are `is_buyer_maker == false` | `sum()` |
| `count_buy_trades` | Count of aggTrades with `is_buyer_maker == false` | `sum()` |
| `count_sell_trades` | Count of aggTrades with `is_buyer_maker == true` | `sum()` |
| `taker_buy_ratio` | `agg_buy_qty / (agg_buy_qty + agg_sell_qty)` from aggTrades; `NULL` if denominator is zero | Recomputed as `taker_buy_vol_btc / volume_btc`; `NULL` if denominator is zero |
| `vol_buy_whale_btc` | Sum of buy aggTrade quantity where trade notional `>= 100000 USDT` | `sum()` |
| `vol_sell_whale_btc` | Sum of sell aggTrade quantity where trade notional `>= 100000 USDT` | `sum()` |
| `vol_buy_retail_btc` | Sum of buy aggTrade quantity where trade notional `<= 1000 USDT` | `sum()` |
| `vol_sell_retail_btc` | Sum of sell aggTrade quantity where trade notional `<= 1000 USDT` | `sum()` |
| `whale_trade_count` | Count of aggTrades where trade notional `>= 100000 USDT` | `sum()` |
| `realized_vol_bar` | API field for bar close-to-close realized movement. | `abs(ln(close_t / close_t-1))`; `NULL` on the first returned bar or when either close is unavailable/non-positive. |

### Book-ticker, microprice, and depth-quality fields

| Field | Minute lake capture / calculation | Live API behavior |
| --- | --- | --- |
| `micro_price_close` | Last minute microprice from book-ticker snapshots: `((bid_price * ask_qty) + (ask_price * bid_qty)) / (bid_qty + ask_qty)` | Last minute's value in the window |
| `avg_spread_usdt` | Mean `ask_price - bid_price` across minute book-ticker snapshots; forward-filled within the minute builder limit | Mean of minute values |
| `bid_ask_imbalance` | Mean `(bid_qty - ask_qty) / (bid_qty + ask_qty)` across minute book-ticker snapshots; forward-filled within the minute builder limit | Mean of minute values |
| `avg_bid_depth` | Mean `bid_qty` across minute book-ticker snapshots; forward-filled within the minute builder limit | Mean of minute values |
| `avg_ask_depth` | Mean `ask_qty` across minute book-ticker snapshots; forward-filled within the minute builder limit | Mean of minute values |
| `spread_pct` | Mean `(ask_price - bid_price) / ((ask_price + bid_price) / 2)` across minute book-ticker snapshots; forward-filled within the minute builder limit | Mean of minute values |
| `price_impact_100k` | Simulated buy-side impact for `100000 USDT` through the synchronized ask book: `(avg_execution_price - mid) / mid`, `mid=(best_bid + best_ask)/2`; `NULL` if not fillable | Last minute's value in the window |
| `impact_fillable` | Whether the synchronized book can fill the `100000 USDT` basket | Last minute's value when `has_depth=true`; else `NULL` |
| `depth_degraded` | `true` when depth sync/health checks fail for the minute | Logical OR across the window when `has_depth=true`; else `NULL` |

### Liquidation fields

| Field | Minute lake capture / calculation | Live API behavior |
| --- | --- | --- |
| `liq_long_vol_usdt` | Sum of `price * quantity` for `SELL` `forceOrder` events (longs liquidated) | `sum()` when any minute in the window has liquidation data; else `NULL` |
| `liq_short_vol_usdt` | Sum of `price * quantity` for `BUY` `forceOrder` events (shorts liquidated) | `sum()` when any minute in the window has liquidation data; else `NULL` |
| `liq_long_count` | Count of `SELL` `forceOrder` events | `sum()` when any minute in the window has liquidation data; else `NULL` |
| `liq_short_count` | Count of `BUY` `forceOrder` events | `sum()` when any minute in the window has liquidation data; else `NULL` |
| `liq_avg_fill_price` | Quantity-weighted liquidation fill price: `sum(price * qty) / sum(qty)` | Recomputed as `total_liq_notional / total_estimated_qty`; `NULL` if no liquidation quantity can be inferred |
| `liq_unfilled_supported` | `true` only when the force-order payload exposes original and executed quantity semantics for the minute | Logical OR across the window when liquidation data exists; else `NULL` |
| `liq_unfilled_ratio` | If supported, `sum(orig_qty - executed_qty) / sum(orig_qty)`; otherwise `NULL` | Mean of minute-level supported ratios across the window; `NULL` when unsupported or absent |

### Open interest, positioning, and funding fields

| Field | Minute lake capture / calculation | Live API behavior |
| --- | --- | --- |
| `oi_contracts` | Direct metrics `oi_contracts` when present, else `sum_open_interest / count_toptrader_long_short_ratio`; forward-filled within the minute builder limit | Forward-filled across the frame, then last value in the window |
| `oi_value_usdt` | Direct metrics `oi_value_usdt` when present, else `sum_open_interest_value / count_toptrader_long_short_ratio`; forward-filled within the minute builder limit | Forward-filled across the frame, then last value in the window |
| `top_trader_ls_ratio_acct` | Top-trader account L/S `ratio` from the 5m REST series, backward as-of joined to each minute with `30 minute` tolerance | Forward-filled across the frame, then last value in the window |
| `global_ls_ratio_acct` | Global account L/S `ratio` from the 5m REST series, backward as-of joined to each minute with `30 minute` tolerance | Forward-filled across the frame, then last value in the window |
| `ls_ratio_divergence` | `top_trader_ls_ratio_acct - global_ls_ratio_acct` when both are present | Recomputed from the aggregated last values |
| `top_trader_long_pct` | Top-trader `long_account` share from the 5m ratio endpoint | Forward-filled across the frame, then last value in the window |
| `top_trader_short_pct` | Top-trader `short_account` share from the 5m ratio endpoint | Forward-filled across the frame, then last value in the window |
| `funding_rate` | Settled funding rate from REST `fundingRate`; falls back to premium-index `last_funding_rate`; forward-filled within the minute builder limit | Event/as-of aligned to the returned bar. This is not a native Binance per-timeframe candle field. |
| `predicted_funding` | Latest mark-price WS predicted funding `r` when live WS capture is present; not synthesized from REST. | Usually `NULL` for historical/native REST-only responses; metadata uses `predicted_funding_live_ws_only`. |
| `next_funding_time` | Current premium-index/mark-price snapshot concept, not historical bar data. | Historical bars usually remain `NULL`; native responses may set the latest bar only from a current snapshot and note `next_funding_time_current_snapshot_only`. |

### Service-only derived fields returned by the live API

These fields are not stored in the canonical 66-column minute lake parquet schema, but they are added by the live price service after timeframe aggregation.

| Field | API-only calculation |
| --- | --- |
| `global_long_pct` | `global_ls_ratio_acct / (1 + global_ls_ratio_acct)` when the ratio is positive |
| `global_short_pct` | `1 / (1 + global_ls_ratio_acct)` when the ratio is positive |
| `top_trader_long_usd` | `oi_value_usdt * top_trader_long_pct` |
| `top_trader_short_usd` | `oi_value_usdt * top_trader_short_pct` |
| `global_long_usd` | `oi_value_usdt * global_long_pct` |
| `global_short_usd` | `oi_value_usdt * global_short_pct` |
| `cvd_btc` | Cumulative sum of `net_taker_vol_btc` across the returned bars; resets at the start of the response window |
| `delta_oi_contracts` | `oi_contracts - previous_bar_oi_contracts`; `NULL` on the first returned bar |
| `delta_oi_value_usdt` | `oi_value_usdt - previous_bar_oi_value_usdt`; `NULL` on the first returned bar |
| `delta_net_long` | `net_long - previous_bar_net_long`; `NULL` on the first returned bar |
| `delta_net_short` | `net_short - previous_bar_net_short`; `NULL` on the first returned bar |
| `delta_funding_rate` | Current aligned `funding_rate` minus previous aligned `funding_rate`; `NULL` when either value is missing |

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

**Quick command cheatsheet**

- One-off minute: `PYTHONPATH=src bml run-once`
- Continuous polling: `PYTHONPATH=src bml run-forever` (minute-aligned, throttled)
- Live WS + polling: `PYTHONPATH=src bml run-live-forever --event-db state/live_events.sqlite`
- Single-command live runner: `./scripts/run-live.sh` (or `make run-live`) (runs one-time cleanup, then starts live daemon)
- Manual live state cleanup: `PYTHONPATH=src bml cleanup-live-state --event-db state/live_events.sqlite --vacuum`
- Bounded daemon: `bml run-daemon --poll-seconds 60`
- Backfill recent days: `PYTHONPATH=src bml backfill-range --start <ISO> --end <ISO> [--max-missing-hours N] [--force-repair]`
- Backfill deep history: `PYTHONPATH=src bml backfill-years --years 5 --max-missing-hours 24 --sleep-seconds 0.05`
- Backfill loader (prompt year or all): `PYTHONPATH=src bml backfill-loader [--year 2024 | --last-5-years] [--max-missing-hours N] [--allow-rest-fallback] [--force-repair]`
- IntelliJ parquet access: `PYTHONPATH=src bml prepare-intellij --db-path data/minute.duckdb --symbol BTCUSDT`
- IntelliJ + daemon runner script: `./scripts/run-intellij.sh [db_path] [symbol] [parquet_root] [poll_seconds]`
- Inspect state: `bml show-watermark --symbol BTCUSDT`
- Browse data in IntelliJ/DataGrip: `PYTHONPATH=src bml materialize-duckdb --db-path data/minute.duckdb` then open the DB and query the `minute` view.

### Environment checklist

1. Install system dependencies listed in `REQUIREMENTS_ADDENDUM.md` (e.g., `libpq`, `rust` for `pyarrow` builds).
2. Copy `.env.example` to `.env` and audit values for credentials and S3 paths.
3. Use `poetry shell` or `source .venv/bin/activate` before running `bml` commands.
4. Validate `state/` and `logs/` directories are writable by the service account that will run the CLI.
5. Optional retention tuning:
   - `BML_LIVE_EVENT_RETENTION_HOURS` (default `12`)
   - `BML_LIVE_HEARTBEAT_RETENTION_DAYS` (default `14`)
   - `BML_LIVE_CLEANUP_INTERVAL_MINUTES` (default `30`)
   - `BML_LIVE_CLEANUP_VACUUM_INTERVAL_HOURS` (default `24`)

## CLI reference

- `bml run-once` – executes a single minute job, writing parquet and booking ledger entries.
- `bml run-daemon --poll-seconds 60` – loops the minute job with configurable polling; use a process manager (systemd, supervisord) in prod.
- `PYTHONPATH=src bml run-forever` – alignment-aware infinite poller (adds rate limiting).

## Higher-timeframe aggregator

The repo now includes a separate local HTF materialization service under [src/aggregator](/Users/shashankniranjan/IdeaProjects/Crypto-Data-Scheduler/src/aggregator) plus shell scripts at [scripts/run-aggregator.sh](/Users/shashankniranjan/IdeaProjects/Crypto-Data-Scheduler/scripts/run-aggregator.sh) and [scripts/stop-aggregator.sh](/Users/shashankniranjan/IdeaProjects/Crypto-Data-Scheduler/scripts/stop-aggregator.sh).

This service keeps the existing minute lake strictly read-only. It only reads from:

- [data/futures/um/minute](/Users/shashankniranjan/IdeaProjects/Crypto-Data-Scheduler/data/futures/um/minute)

And writes its own higher-timeframe outputs to a separate tree:

- [data/futures/um/higher_timeframes](/Users/shashankniranjan/IdeaProjects/Crypto-Data-Scheduler/data/futures/um/higher_timeframes)

State and checkpoints are stored separately in:

- [state/aggregator_state.sqlite](/Users/shashankniranjan/IdeaProjects/Crypto-Data-Scheduler/state/aggregator_state.sqlite)

### Behavior

- Startup runs backfill first, detects missing complete HTF buckets from minute history, writes only those buckets, then enters continuous polling mode.
- Continuous mode only recomputes newly completable buckets plus a recent repair lookback window so late minute arrivals or duplicate-minute corrections can rewrite recent HTF buckets idempotently.
- Incomplete buckets are skipped by default and are only materialized when `HTF_ALLOW_INCOMPLETE_BUCKETS=true`.
- Weekly buckets are aligned to Monday `00:00 UTC`; monthly buckets are aligned to the first day of month `00:00 UTC`.

### Usage

```bash
./scripts/run-aggregator.sh
```

The default background start command now streams the aggregator log to your console. Press `Ctrl+C` to detach from the log stream without stopping the service.

Bounded validation runs:

```bash
HTF_TIMEFRAMES=5m ./scripts/run-aggregator.sh --once
HTF_TIMEFRAMES=5m ./scripts/run-aggregator.sh --startup-only
./scripts/stop-aggregator.sh
```

Optional environment variables:

- `HTF_SYMBOL=BTCUSDT`
- `HTF_SOURCE_ROOT=./data`
- `HTF_TARGET_ROOT=./data/futures/um/higher_timeframes`
- `HTF_STATE_DB=./state/aggregator_state.sqlite`
- `HTF_POLL_INTERVAL_SECONDS=30`
- `HTF_REPAIR_LOOKBACK_MINUTES=120`
- `HTF_ALLOW_INCOMPLETE_BUCKETS=false`
- `HTF_TIMEFRAMES=3m,5m,10m,15m,30m,45m,1h,4h,8h,1d,1w,1M`
- `TAIL_LINES=20`

### Aggregation rule table

The rule mapping used by the service is implemented in [src/aggregator/aggregation_rules.py](/Users/shashankniranjan/IdeaProjects/Crypto-Data-Scheduler/src/aggregator/aggregation_rules.py). Summary:

| Column | Rule |
| --- | --- |
| `open`, `high`, `low`, `close` | TradingView-style OHLC from minute rows |
| `volume_btc`, `volume_usdt`, `trade_count` | Sum |
| `vwap` | `sum(volume_usdt) / sum(volume_btc)` |
| `avg_trade_size_btc` | `sum(volume_btc) / sum(trade_count)` |
| `max_trade_size_btc` | Max |
| `taker_*`, whale/retail flow, liquidation notionals/counts | Sum |
| `liq_avg_fill_price`, `liq_unfilled_ratio` | Weighted average by minute liquidation notional |
| `oi_*`, funding/ratio snapshots, `micro_price_close` | Last non-null in bucket |
| `mark_price_open`, `index_price_open` | First non-null in bucket |
| `mark_price_close`, `index_price_close` | Last non-null in bucket |
| `avg_spread_usdt`, `bid_ask_imbalance`, `avg_bid_depth`, `avg_ask_depth`, `spread_pct`, `price_impact_100k` | Volume-weighted average using `volume_usdt`, fallback simple mean |
| `has_depth`, `impact_fillable`, `depth_degraded`, `has_ws_latency`, `ws_latency_bad`, `has_ls_ratio`, `has_liq`, `liq_unfilled_supported` | Boolean OR |
| `realized_vol_htf` | Recomputed from minute close-to-close log returns inside the HTF bucket |
| `event_time`, `transact_time`, `arrival_time` | Max |
| `update_id_start`, `update_id_end` | Min non-null / max non-null |
| `timeframe`, `symbol`, `bucket_start`, `bucket_end`, `expected_minutes_in_bucket`, `observed_minutes_in_bucket`, `missing_minutes_count`, `bucket_complete` | HTF metadata and quality columns |

For derivatives fields where vendor internals are not public, the implementation uses defensible industry-standard approximations and documents them inline. The minute parquet files, partition layout, schema, and generation logic remain untouched.
- `PYTHONPATH=src bml run-live-forever` – live daemon; includes automatic retention cleanup for `state/live_events.sqlite`.
- `PYTHONPATH=src bml cleanup-live-state` – one-off retention cleanup and optional SQLite `VACUUM`.
- `bml show-watermark` – displays the most recent timestamp and ledger position per symbol.
- `bml backfill-years --years 5 [--max-missing-hours N]` – consistency scan plus repair for the requested horizon.
- `bml backfill-loader [--year YYYY | --last-5-years]` – interactive backfill helper; validates each hour folder/file and repairs missing/invalid hours only (Vision-first by default to avoid REST bans).
- `bml prepare-intellij [--db-path data/minute.duckdb]` – creates a DuckDB view over parquet using absolute paths and writes a `.queries.sql` starter file for IntelliJ Ultimate.
- `bml backfill-range --start <ISO> --end <ISO>` – fill gaps for arbitrary ranges; add `--force-repair` to rebuild all hours in range.
- `PYTHONPATH=src bml materialize-duckdb [--db-path data/minute.duckdb]` – build/update a DuckDB view over all parquet partitions for IDE/BI inspection.

Each backfill path scans existing partitions for gaps and invokes repairs for any missing hours (bounded by `--max-missing-hours` when provided).

## Observability & production guidance

- **Logging** – `logs/` contains tidy parquet writer logs; rotate them and ship to your aggregator.
- **Monitoring** – hook the CLI metrics (stored to the ledger) into Prometheus/Grafana dashboards.
- **Backups** – regularly snapshot the SQLite metadata under `state/` and validate ledger continuity before restoring.
- **Schema changes** – update `src/binance_minute_lake/core/schema.py` then regenerate docs in `docs/` and rerun `tests/schema`.
- **Data integrity** – run `bml backfill-years --years 1` after each schema change to ensure partitions remain consistent.

## Live API timeframe sourcing

The live price API and live indicator API use a capability-driven fetch planner before retrieving candles. By default, supported candle timeframes are fetched directly from Binance Futures REST instead of expanding every request through canonical 1m bars.

Config flags use the `BML_API_` prefix:

- `BML_API_ENABLE_NATIVE_BINANCE_TF_CANDLES=true` enables direct Binance candle timeframes.
- `BML_API_CANDLE_FETCH_MODE=native_preferred` supports `native_preferred`, `auto`, or `aggregate_from_1m`.
- `BML_API_ALLOW_LEGACY_1M_FALLBACK=true` keeps the historical minute-lake aggregation path available.
- `BML_API_ALLOW_PARTIAL_RESPONSE_WITH_NOTES=true` allows unavailable fields to return nulls with response metadata instead of failing the whole request.
- `BML_API_INCLUDE_DEPRECATED_FIELDS=false` keeps default payloads canonical-only. Set it to `true` only for temporary client compatibility with deprecated `vwap_1m` and `realized_vol_1m`; when emitted, those aliases mirror `vwap_bar` and `realized_vol_bar`.
- `BML_API_ENABLE_LOCAL_SYMBOL_FASTPATH=true` enables richness-first routing for configured local minute-lake symbols.
- `BML_API_LOCAL_PREFERRED_SYMBOLS=BTCUSDT` is a comma-separated list of symbols that should prefer local 1m parquet plus live WS overlay before native Binance timeframe candles.
- `BML_API_LOCAL_SYMBOL_REQUIRE_FULL_COVERAGE=false` falls back when the local minute lake cannot fully satisfy the requested aggregation window.
- `BML_API_LOCAL_SYMBOL_ALLOW_BINANCE_PATCH=true` allows the existing canonical 1m Binance rebuild path to patch missing local minutes before aggregation.
- `BML_API_ENABLE_BTC_COMPLEXITY_GUARD=true` prevents oversized BTC requests from forcing expensive local 1m aggregation.
- `BML_API_BTC_LOCAL_MAX_1M_BARS=500` and `BML_API_BTC_LOCAL_MAX_3M_BARS=300` bound BTC local-path bar counts for low timeframes.
- `BML_API_BTC_LOCAL_MAX_HIGHER_TF_BARS=200` bounds BTC local-path requests for timeframes above `3m`.
- `BML_API_BTC_FORCE_BINANCE_FOR_HEAVY_HIGHER_TF=true` sends BTC `5m+` requests above the threshold to Binance-native timeframe fetch.

Source routing:

- Symbols in `BML_API_LOCAL_PREFERRED_SYMBOLS` use the local 1m minute lake first. If local coverage can satisfy the requested bars and the BTC complexity guard allows the timeframe, the API aggregates locally and overlays live WS snapshots when available. Metadata includes `source_strategy=local_minute_lake_preferred`, `local_minute_lake_used`, `live_ws_overlay_used`, and notes such as `using_local_btc_minute_lake`, `btc_local_path_selected`, or `using_live_ws_overlay`.
- BTC requests can split by timeframe: `1m`/`3m` stay local while heavy `5m+` timeframes use Binance-native candles. Heavy fallback metadata includes `btc_local_path_skipped_due_to_request_complexity`, `btc_higher_tf_binance_fallback`, and `btc_mixed_source_plan`.
- If a local-preferred symbol lacks sufficient local coverage, the API either patches missing canonical 1m rows through the existing Binance rebuild path or falls back to the native Binance timeframe planner. Metadata notes include `local_btc_missing_required_window`, `local_btc_coverage_incomplete_patched_from_binance`, or `local_btc_coverage_incomplete_fallback_to_binance` as applicable.
- Symbols not in `BML_API_LOCAL_PREFERRED_SYMBOLS` keep the efficiency-first path: native Binance timeframe candles plus REST enrichment for OI, long/short ratios, mark/index/premium bars, and funding alignment.

Supported source matrix:

| Data type | Source | Historical timeframes | Notes |
| --- | --- | --- | --- |
| Candles | `/fapi/v1/klines` | `1m`, `3m`, `5m`, `15m`, `1h`, `4h`, `1d` | API aliases such as `1hr` and `60m` normalize to `1h`. |
| Open interest history | `/futures/data/openInterestHist` | `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d` | Binance keeps only the latest month; `3m` OI is not natively available. |
| Long/short account ratios | `/futures/data/globalLongShortAccountRatio`, `/futures/data/topLongShortAccountRatio`, `/futures/data/topLongShortPositionRatio` | `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d` | `1m` and `3m` are not natively available and stay null unless an explicit synthetic mode is added. |
| Current open interest | `/fapi/v1/openInterest` | current snapshot only | Not a historical bar source. |
| Depth/order book | `/fapi/v1/depth` | current snapshot only | Historical depth bars must come from locally captured WS data or remain unavailable. |

Responses include `timeframe_metadata` with `source`, `fetch_mode`, `fallback_used`, `binance_interval`, `latency_secs`, and notes such as `open_interest_hist_not_supported_for_3m`, `historical_depth_not_available_from_binance_rest`, or `using_legacy_1m_aggregation_fallback`.
Unsupported OI uses the canonical note prefix `open_interest_hist_not_supported_for_*`; older `oi_hist_not_supported_for_*` variants are not emitted.
Internal alignment helper columns, including temporary `_aux_ts` values, are not part of the default API payload.

Native timeframe responses also enrich supported bars with non-candle Binance REST series where available:

- `oi_contracts` and `oi_value_usdt` come from `/futures/data/openInterestHist` for `5m`, `15m`, `1h`, `4h`, and other Binance-supported OI periods. `1m` and `3m` remain null with explicit metadata notes.
- `global_ls_ratio_acct` and `top_trader_ls_ratio_acct` come from the matching account-ratio endpoints, while `top_trader_long_pct` and `top_trader_short_pct` come from `topLongShortPositionRatio`.
- `funding_rate` is event-based and aligned as last-known funding event as of the bar close; it is not treated as a native candle series.
- Timeframe metadata uses `funding_rate_aligned_asof_backward` and `funding_rate_event_series_aligned_not_native_tf` to make that alignment explicit.
- `net_long = top_trader_long_pct - top_trader_short_pct` and `net_short = top_trader_short_pct - top_trader_long_pct`. These are sentiment-style net measures, not exchange-wide inventory.
- Delta fields (`delta_oi_contracts`, `delta_oi_value_usdt`, `delta_funding_rate`, `delta_net_long`, `delta_net_short`) are bar-over-bar changes and are null on the first returned bar.

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
