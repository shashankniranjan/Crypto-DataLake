# Missing Fields Implementation Spec (USDⓈ-M Futures, BTCUSDT)

## 0. Goal and current state (from your CSV)

Your sample shows:

- `has_ls_ratio=true` and LS ratio columns are populated.
- `has_ws_latency=false`, `has_depth=false`, `has_liq=false` and all dependent fields are empty.

Therefore this doc implements three missing families:

- WS latency fields
- Order book update IDs + local order book features (price impact)
- Liquidations

All work targets Binance USDⓈ-M Futures streams/APIs.

## 1. Hard rules (do not deviate)

### 1.1 Canonical time key

All per-minute feature rows are keyed by:

`minute_ts = floor_to_minute(exchange_time_ms)`

Use exchange time where available (e.g., WS event time). For kline rows, timestamp is already minute-aligned (your `timestamp` column is the candle open time in ms).

### 1.2 Null vs 0 semantics (data quality)

`NULL` means: source not ingested / consumer down / join not possible

`0` means: source ingested successfully and there were no events

Implement per-minute flags:

- `has_ws_latency`, `has_depth`, `has_liq`, `has_ls_ratio` (you already have the last one)

Never output false for a derived “bad flag” if the source is absent.
Example: if `has_ws_latency=false`, then `ws_latency_bad` must be `NULL`, not false.

## 2. Sources (exact Binance documentation)

### 2.1 WS market streams base URL

Use:

- `wss://fstream.binance.com/ws/<streamName>` (single stream)
- `wss://fstream.binance.com/stream?streams=<a>/<b>` (combined streams)

### 2.2 Diff book depth stream (for update IDs and local book)

Stream names:

- `<symbol>@depth` / `<symbol>@depth@500ms` / `<symbol>@depth@100ms`

Local order book correctness procedure (snapshot + diffs) including:

- snapshot REST: `https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`
- dropping old events (`u < lastUpdateId`)
- first processed event condition: `U <= lastUpdateId AND u >= lastUpdateId`

### 2.3 Liquidation order stream

Stream name:

- `<symbol>@forceOrder`

Behavior: if no liquidation happens in the 1000ms interval, nothing is pushed.

### 2.4 Long/Short ratio endpoints (already implemented but keep for completeness)

- Long/Short ratio endpoints have period enums (`5m..1d`) and only last 30 days available.
- Top trader long/short account ratio definition.

## 3. Data model (tables / topics)

### 3.1 Raw WS event tables (append-only)

Table: `ws_depth_events`

- `symbol` (TEXT)
- `event_time` (BIGINT, nullable)
- `arrival_time` (BIGINT, NOT NULL)
- `U` (BIGINT) // first update id in event
- `u` (BIGINT) // last update id in event
- `bids` (JSON)
- `asks` (JSON)

Table: `ws_liq_events`

- `symbol`
- `event_time`
- `arrival_time`
- `side` (`BUY`/`SELL`)
- `price` (DOUBLE)
- `qty` (DOUBLE)
- `raw_json` (JSON)

Table: `ws_trade_events` (optional but recommended if you want latency from trade stream)

- `symbol`
- `event_time`
- `arrival_time`
- `transact_time`
- `raw_json`

Arrival time must always be captured locally at the socket callback boundary.

### 3.2 Minute feature table (upsert by minute)

Table: `features_1m` keyed by (`symbol`, `minute_ts`).
Contains your columns:

- `has_ws_latency`, `has_depth`, `has_liq`, `has_ls_ratio`
- latency fields
- update_id fields
- liquidation fields
- impact fields
- plus existing candle/trade-derived metrics

All writers must be idempotent upserts by (`symbol`, `minute_ts`).

## 4. Implementation: field-by-field

### A) WS latency family

#### A.1 Fields

- `event_time`
- `arrival_time`
- `latency_engine`
- `latency_network`
- `ws_latency_bad`
- `has_ws_latency`

#### A.2 Source and capture

- `arrival_time`: local `now_ms()` on depth message receipt (always).
- `event_time`: parse from depth WS payload `E`.
- `transact_time`: parse from depth WS payload `T`.

WS market streams docs show how to connect and subscribe.

#### A.3 Computation (per event)

- `latency_engine = arrival_time - event_time` (ms)
- `latency_network = arrival_time - transact_time` (ms)

#### A.4 Aggregation to 1m

For each `minute_ts`:

- `event_time = max(event_time)` from events in that minute
- `transact_time = max(transact_time)` from events in that minute
- `arrival_time = max(arrival_time)`
- `latency_engine = p95(latency_engine)` (robust)
- `latency_network = p95(latency_network)`

#### A.5 Flags

- `has_ws_latency=true` iff at least one depth event in the minute has `E`, `T`, and local receipt time.

`ws_latency_bad`:

- `NULL` if `has_ws_latency=false`
- else `true` if `latency_engine > 500` OR `latency_network > 500` (ms), else `false`

#### A.6 Acceptance tests

- When WS consumer is running for 10 minutes, `has_ws_latency=true` each minute and latency fields non-null.
- When WS consumer is stopped, `has_ws_latency=false` and all latency fields + `ws_latency_bad` become `NULL`.

### B) Depth family (`update_id_*` + price impact)

#### B.1 Fields

- `update_id_start`, `update_id_end`
- `has_depth`
- `depth_degraded`
- `price_impact_100k`, `impact_fillable`

#### B.2 Sources

- WS diff depth stream: `<symbol>@depth@100ms`
- REST snapshot depth endpoint and sync procedure

#### B.3 update_id computation

For each depth event you store `U` and `u` from the message (these are the update IDs).
Per minute:

- `update_id_start = min(U)`
- `update_id_end = max(u)`

#### B.4 Local order book (mandatory for impact)

Follow Binance “local order book correctly” procedure (do not improvise):

Algorithm:

1. Open stream `btcusdt@depth@100ms` and buffer events.
2. Fetch snapshot from REST: `GET https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000`.
3. Set local book to snapshot; let `lastUpdateId = snapshot.lastUpdateId`.
4. Drop buffered events where `u < lastUpdateId`.
5. First processed event must satisfy: `U <= lastUpdateId AND u >= lastUpdateId`.
6. Apply updates: data is absolute quantity at price; `qty=0` removes price level.
7. If continuity breaks, resync from step 2 and mark `depth_degraded=true` for the current minute.

#### B.5 `price_impact_100k` definition (lock it)

At minute close (or at last depth update in minute):

- `mid = (best_bid + best_ask)/2`
- simulate market BUY of `$100,000`:
- walk asks from best upward accumulating notional until `100k`
- compute `avg_exec = total_cost / total_qty`
- `price_impact_100k = (avg_exec - mid) / mid`

`impact_fillable = true` if you could fill full `100k` else false (and impact `NULL`).

#### B.6 Flags

- `has_depth=true` if at least one depth event arrived in that minute.

`depth_degraded`:

- `NULL` if `has_depth=false`
- else `true` if any resync/continuity break occurred, or the minute is not fillable for `$100k`, or spread/depth health thresholds fail.

#### B.7 Acceptance tests

- With depth consumer running, `has_depth=true` and update_id fields non-null for each minute.
- Force a disconnect/resync; only the impacted minute(s) show `depth_degraded=true`.
- `price_impact_100k` is usually non-null and `impact_fillable=true` for BTCUSDT.

### C) Liquidation family

#### C.1 Fields

- `liq_long_vol_usdt`, `liq_short_vol_usdt`
- `liq_long_count`, `liq_short_count`
- `liq_avg_fill_price`
- `liq_unfilled_ratio`
- `liq_unfilled_supported`
- `has_liq`

#### C.2 Source

WS liquidation stream:

- `<symbol>@forceOrder`, update speed 1000ms.

If no liquidation happens in a 1000ms interval, nothing is pushed.

#### C.3 Event capture

On each message:

- capture `arrival_time=now_ms()`
- parse exchange event time from payload
- extract order side and quantities from the liquidation order object, using `ap` as price (fallback to `p`)

#### C.4 Classification rule (lock it)

- If liquidation side is `SELL` -> classify as long liquidation (forced sell closes longs)
- If side is `BUY` -> classify as short liquidation

#### C.5 Aggregation to 1m

For each minute:

- `notional_usdt = ap * qty` (fallback to `p * qty`)
- `liq_long_vol_usdt = sum(notional_usdt for SELL)`
- `liq_short_vol_usdt = sum(notional_usdt for BUY)`
- counts similarly
- `liq_avg_fill_price = weighted_avg(price, weight=qty)` across all liquidation events in that minute
- if no events: leave `NULL` (avg price `0` is meaningless)

#### C.6 Unfilled ratio support

Only compute `liq_unfilled_ratio` if your parsed payload provides both `q` and `l`.

If supported:

- `unfilled = max(q - l, 0)`
- `liq_unfilled_ratio = sum(q - l) / sum(q)` per minute
- `liq_unfilled_supported=true`

If not supported:

- `liq_unfilled_ratio=NULL`
- `liq_unfilled_supported=false`

#### C.7 Null vs 0 requirements

`has_liq=true` only when at least one `forceOrder` event arrived in the minute.

If the consumer is down:

- `has_liq=false`
- liquidation fields must be `NULL`

#### C.8 Acceptance tests

- Run for 30 minutes: some minutes should have `has_liq=false` with liquidation fields `NULL` (because no `forceOrder` event was emitted).
- Stop consumer: `has_liq=false` and fields become `NULL`.

## 5. Joining into your existing CSV rows

### 5.1 Join key

Always join WS-derived aggregates onto your candle rows by:

- `symbol`
- `minute_ts == timestamp` (your `timestamp` appears to be minute open time)

### 5.2 Forward-fill is NOT allowed for WS features

Do not forward-fill:

- depth update ids
- latency
- liquidations
- price impact

These are per-minute facts. If absent, they must remain `NULL`.

Forward-fill is fine only for LS ratio REST series (coarser period) as long as freshness gate is enforced.

## 6. Operational hardening

### 6.1 Heartbeats

Each consumer writes a heartbeat row every minute:

`consumer_name, minute_ts, alive=true/false, last_message_time`

Feature builder uses heartbeats to decide `has_*`.

### 6.2 Restart safety

On restart: local order book must resync using snapshot+diff procedure.
Mark minutes during resync as `depth_degraded=true`.

### 6.3 Rate limits

Depth snapshot is called only:

- at startup
- after continuity break

Avoid polling snapshots constantly.

## 7. What “done” looks like (expected CSV change)

In your sample, after implementation you should see:

- `has_ws_latency=true` and `event_time`/`arrival_time`/`latency_*` populated
- `has_depth=true` and `update_id_start/end` populated + `price_impact_100k` mostly populated
- `has_liq=true` and liquidation volumes/counts are mostly `0` with occasional spikes

If any of these remain false while the code is running, it’s not a “feature engineering problem.” It’s a missing source ingestion problem.
