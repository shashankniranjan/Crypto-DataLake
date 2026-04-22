# Architecture Diagrams

These diagrams describe the repository as implemented today. Labels use the same convention as the HLD and LLD: observed, inferred, and unclear / needs confirmation.

## 1. System Context

**Observed**

```mermaid
flowchart LR
    BinanceRest["Binance Futures REST"]
    BinanceVision["Binance Vision Daily ZIPs"]
    BinanceWs["Binance Futures WebSocket"]
    Operators["Developers / Operators"]
    Clients["Local API Clients / Trading Tools"]

    Repo["Crypto-Data-Scheduler"]
    DataLake["Local Parquet Data Lake"]
    SQLite["Local SQLite State"]

    BinanceRest --> Repo
    BinanceVision --> Repo
    BinanceWs --> Repo
    Operators --> Repo
    Repo --> DataLake
    Repo --> SQLite
    Clients --> Repo
```

## 2. Container View

**Observed**

```mermaid
flowchart TB
    subgraph Sources["External Sources"]
        REST["REST API"]
        Vision["Vision ZIPs"]
        WS["WebSocket Streams"]
    end

    subgraph Ingestion["Minute Lake Ingestion"]
        CLI["Typer CLI bml"]
        Orchestrator["MinuteIngestionPipeline"]
        Transform["MinuteTransformEngine"]
        DQ["DQValidator"]
        Writer["AtomicParquetWriter"]
    end

    subgraph Live["Live Feature Capture"]
        Supervisor["BinanceLiveStreamSupervisor"]
        Processor["BinanceWsPayloadProcessor"]
        Collector["InMemoryLiveCollector"]
        LiveStore["LiveEventStore SQLite"]
    end

    subgraph Storage["Local Storage"]
        MinuteParquet["Hourly Minute Parquet"]
        IngestionState["ingestion_state.sqlite"]
        HtfParquet["Daily HTF Parquet"]
        AggregatorState["aggregator_state.sqlite"]
    end

    subgraph Aggregation["Higher-Timeframe Aggregator"]
        AggService["AggregationService"]
        Backfill["BackfillRunner"]
        Incremental["IncrementalRunner"]
        HtfWriter["HigherTimeframeWriter"]
    end

    subgraph API["Serving Layer"]
        FastAPI["FastAPI App"]
        ApiService["LiveDataApiService"]
        Provider["ParallelLiveBinanceProvider"]
        Repositories["Minute + HTF Repositories"]
        Indicators["live_indicators"]
    end

    REST --> Orchestrator
    Vision --> Orchestrator
    WS --> Supervisor
    Supervisor --> Processor --> Collector
    Collector --> LiveStore
    Collector --> Orchestrator
    CLI --> Orchestrator --> Transform --> DQ --> Writer
    Writer --> MinuteParquet
    Writer --> IngestionState
    MinuteParquet --> AggService
    AggService --> Backfill
    AggService --> Incremental
    Backfill --> HtfWriter
    Incremental --> HtfWriter
    HtfWriter --> HtfParquet
    AggService --> AggregatorState
    FastAPI --> ApiService
    ApiService --> Repositories
    ApiService --> Provider
    ApiService --> Indicators
    Repositories --> MinuteParquet
    Repositories --> HtfParquet
    Provider --> REST
    ApiService --> LiveStore
```

## 3. Minute Ingestion Sequence

**Observed**

```mermaid
sequenceDiagram
    participant CLI as bml run-once/run-forever
    participant Pipeline as MinuteIngestionPipeline
    participant State as SQLiteStateStore
    participant Sources as REST / Vision / LiveCollector
    participant Transform as MinuteTransformEngine
    participant DQ as DQValidator
    participant Writer as AtomicParquetWriter
    participant Parquet as Hourly Parquet

    CLI->>Pipeline: run_once(now, max_hours)
    Pipeline->>State: get_watermark(symbol)
    State-->>Pipeline: watermark or null
    Pipeline->>Pipeline: compute target horizon and hour windows
    loop each hour
        Pipeline->>Pipeline: choose hot / warm / cold band
        Pipeline->>Sources: fetch source rows
        Sources-->>Pipeline: kline, trade, metrics, funding, live rows
        Pipeline->>Transform: build_canonical_frame(...)
        Transform-->>Pipeline: canonical 66-column frame
        Pipeline->>Writer: write_hour_partition(symbol, hour_start, frame)
        Writer->>DQ: validate(frame)
        DQ-->>Writer: DQResult
        Writer->>Parquet: write temp parquet and replace final file
        Writer->>State: upsert_partition(...)
        Pipeline->>State: upsert_watermark(window_end)
    end
    Pipeline-->>CLI: PipelineRunSummary
```

## 4. WebSocket Live Feature Flow

**Observed**

```mermaid
flowchart LR
    Depth["depth@100ms"]
    ForceOrder["forceOrder"]
    AggTrade["aggTrade"]
    MarkPrice["markPrice@1s"]
    Snapshot["REST depth snapshot"]

    Supervisor["BinanceLiveStreamSupervisor"]
    Processor["BinanceWsPayloadProcessor"]
    Collector["InMemoryLiveCollector"]
    OrderBook["DepthOrderBook"]
    EventStore["state/live_events.sqlite"]
    MinuteFeatures["minute_live_features"]

    Snapshot --> Supervisor
    Depth --> Supervisor
    ForceOrder --> Supervisor
    AggTrade --> Supervisor
    MarkPrice --> Supervisor
    Supervisor --> Processor
    Processor --> Collector
    Collector --> OrderBook
    Collector --> EventStore
    EventStore --> MinuteFeatures
```

## 5. Live Data API Request Flow

**Observed**

```mermaid
sequenceDiagram
    participant Client
    participant App as FastAPI App
    participant Service as LiveDataApiService
    participant Local as Minute/HTF Repositories
    participant Provider as ParallelLiveBinanceProvider
    participant Live as LiveEventStore / WS Manager

    Client->>App: GET /api/v1/perpetual-data
    App->>Service: fetch_perpetual_data(coin, tfs, limit, end_time)
    Service->>Service: parse timeframes and resolve end_time
    loop each timeframe
        Service->>Service: load_candle_bars(...)
        alt BTCUSDT local-only path
            Service->>Local: read HTF parquet when timeframe > 3m
            Local-->>Service: complete HTF bars or empty
            Service->>Local: read minute parquet fallback
            Local-->>Service: canonical minutes
            Service->>Live: overlay live minute features when available
        else non-local native path
            Service->>Provider: fetch native candles and auxiliary series
            Provider-->>Service: candle and enrichment rows
        else legacy 1m aggregation fallback
            Service->>Local: read local minutes
            Service->>Provider: patch missing minutes when allowed
            Provider-->>Service: canonical patch frame
            Service->>Live: overlay live features when available
        end
        Service->>Service: aggregate/enrich/serialize bars
    end
    Service-->>App: payload with timeframe_metadata
    App-->>Client: JSON response
```

## 6. Higher-Timeframe Aggregator Flow

**Observed**

```mermaid
flowchart TB
    MinuteLake["Minute Parquet Root"]
    Reader["MinuteLakeReader"]
    Existing["Existing HTF Index"]
    Missing["detect_missing_buckets"]
    Rules["aggregate_minutes"]
    Writer["HigherTimeframeWriter"]
    HtfLake["HTF Parquet Root"]
    State["aggregator_state.sqlite"]

    MinuteLake --> Reader
    Reader --> Missing
    Existing --> Missing
    Missing --> Rules
    Reader --> Rules
    Rules --> Writer
    Writer --> HtfLake
    Writer --> Existing
    Missing --> State
    Writer --> State
```

## 7. Data Lake Layout

**Observed**

```mermaid
flowchart TB
    Root["data/"]
    Futures["futures/um/"]
    Minute["minute/"]
    Htf["higher_timeframes/"]

    SymbolMinute["symbol=BTCUSDT/"]
    YearMinute["year=YYYY/"]
    MonthMinute["month=MM/"]
    DayMinute["day=DD/"]
    HourMinute["hour=HH/part.parquet"]

    Tf["timeframe=5m/"]
    SymbolHtf["symbol=BTCUSDT/"]
    YearHtf["year=YYYY/"]
    MonthHtf["month=MM/"]
    DayHtf["day=DD/part.parquet"]

    Root --> Futures
    Futures --> Minute --> SymbolMinute --> YearMinute --> MonthMinute --> DayMinute --> HourMinute
    Futures --> Htf --> Tf --> SymbolHtf --> YearHtf --> MonthHtf --> DayHtf
```

## 8. SQLite State Model

**Observed**

```mermaid
erDiagram
    WATERMARK {
        text symbol PK
        text last_complete_minute_utc
        text updated_at_utc
    }

    PARTITIONS {
        text symbol PK
        text day PK
        integer hour PK
        text path
        integer row_count
        text min_ts
        text max_ts
        text schema_hash
        text content_hash
        text status
        text committed_at_utc
    }

    AGGREGATION_STATE {
        text symbol PK
        text timeframe PK
        text last_completed_bucket_start
        text last_seen_source_minute
        text last_run_at
        text status
    }
```

## 9. Live Event Store Model

**Observed**

```mermaid
erDiagram
    WS_EVENTS {
        text ingest_id PK
        text stream
        text symbol
        integer event_time
        integer transact_time
        integer arrival_time
        text raw_json
    }

    WS_DEPTH_EVENTS {
        text ingest_id PK
        text symbol
        integer event_time
        integer arrival_time
        integer first_update_id
        integer final_update_id
        text bids_json
        text asks_json
        text raw_json
    }

    WS_LIQ_EVENTS {
        text ingest_id PK
        text symbol
        integer event_time
        integer arrival_time
        text side
        real price
        real qty
        text raw_json
    }

    WS_TRADE_EVENTS {
        text ingest_id PK
        text symbol
        integer event_time
        integer arrival_time
        integer transact_time
        text raw_json
    }

    CONSUMER_HEARTBEATS {
        text consumer_name PK
        integer minute_ts PK
        integer alive
        integer last_message_time
    }

    MINUTE_LIVE_FEATURES {
        text symbol PK
        integer minute_ts PK
        integer has_ws_latency
        integer has_depth
        integer has_liq
        integer has_ls_ratio
        integer event_time
        integer transact_time
        integer arrival_time
        integer latency_engine
        integer latency_network
        integer ws_latency_bad
        integer update_id_start
        integer update_id_end
        real price_impact_100k
        integer impact_fillable
        integer depth_degraded
        real liq_long_vol_usdt
        real liq_short_vol_usdt
        integer liq_long_count
        integer liq_short_count
        real liq_avg_fill_price
        real liq_unfilled_ratio
        integer liq_unfilled_supported
        real predicted_funding
        integer next_funding_time
    }
```

