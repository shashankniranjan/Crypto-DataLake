from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from aggregator.aggregation_rules import aggregate_minutes
from aggregator.backfill import BackfillRunner
from aggregator.bucketing import parse_timeframe
from aggregator.incremental import IncrementalRunner
from aggregator.source_reader import MinuteLakeReader
from aggregator.state_store import AggregatorStateStore
from aggregator.target_writer import HigherTimeframeWriter
from aggregator.validator import detect_missing_buckets
from binance_minute_lake.core.schema import canonical_column_names


def _minute_row(
    timestamp: datetime,
    *,
    open_price: float = 100.0,
    high: float | None = None,
    low: float | None = None,
    close: float | None = None,
    volume_btc: float = 1.0,
    volume_usdt: float = 100.0,
    trade_count: int = 10,
    arrival_time: int | None = None,
    **overrides: object,
) -> dict[str, object]:
    row: dict[str, object] = {column: None for column in canonical_column_names()}
    row.update(
        {
            "has_ws_latency": False,
            "has_depth": False,
            "has_liq": False,
            "has_ls_ratio": False,
            "timestamp": timestamp,
            "open": open_price,
            "high": open_price + 1.0 if high is None else high,
            "low": open_price - 1.0 if low is None else low,
            "close": open_price + 0.5 if close is None else close,
            "vwap_1m": volume_usdt / volume_btc if volume_btc else None,
            "micro_price_close": close if close is not None else open_price + 0.5,
            "volume_btc": volume_btc,
            "volume_usdt": volume_usdt,
            "trade_count": trade_count,
            "avg_trade_size_btc": (volume_btc / trade_count) if trade_count else None,
            "max_trade_size_btc": volume_btc,
            "taker_buy_vol_btc": volume_btc / 2.0,
            "taker_buy_vol_usdt": volume_usdt / 2.0,
            "net_taker_vol_btc": volume_btc / 10.0,
            "count_buy_trades": trade_count // 2,
            "count_sell_trades": trade_count - (trade_count // 2),
            "taker_buy_ratio": 0.5,
            "vol_buy_whale_btc": 0.1,
            "vol_sell_whale_btc": 0.2,
            "vol_buy_retail_btc": 0.3,
            "vol_sell_retail_btc": 0.4,
            "whale_trade_count": 1,
            "liq_long_vol_usdt": 0.0,
            "liq_short_vol_usdt": 0.0,
            "liq_long_count": 0,
            "liq_short_count": 0,
            "liq_unfilled_supported": False,
            "avg_spread_usdt": 1.0,
            "bid_ask_imbalance": 0.2,
            "avg_bid_depth": 10.0,
            "avg_ask_depth": 11.0,
            "spread_pct": 0.001,
            "price_impact_100k": 0.002,
            "impact_fillable": True,
            "depth_degraded": False,
            "mark_price_open": open_price + 0.1,
            "mark_price_close": (close if close is not None else open_price + 0.5) + 0.1,
            "index_price_open": open_price,
            "index_price_close": close if close is not None else open_price + 0.5,
            "premium_index": 0.0001,
            "funding_rate": 0.0002,
            "predicted_funding": 0.0003,
            "next_funding_time": int((timestamp + timedelta(hours=8)).timestamp() * 1000),
            "event_time": int(timestamp.timestamp() * 1000),
            "transact_time": int(timestamp.timestamp() * 1000) + 1,
            "arrival_time": int(timestamp.timestamp() * 1000) + 2 if arrival_time is None else arrival_time,
            "update_id_start": int(timestamp.timestamp()),
            "update_id_end": int(timestamp.timestamp()) + 1,
        }
    )
    row.update(overrides)
    return row


def _canonical_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows)


def _write_minute_partitions(root_dir: Path, symbol: str, rows: list[dict[str, object]]) -> None:
    grouped: dict[datetime, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        timestamp = row["timestamp"]
        assert isinstance(timestamp, datetime)
        hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
        grouped[hour_start].append(row)

    for hour_start, hour_rows in grouped.items():
        target = (
            root_dir
            / "futures"
            / "um"
            / "minute"
            / f"symbol={symbol}"
            / f"year={hour_start:%Y}"
            / f"month={hour_start:%m}"
            / f"day={hour_start:%d}"
            / f"hour={hour_start:%H}"
            / "part.parquet"
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        _canonical_frame(hour_rows).write_parquet(target)


def _scan_target(root: Path) -> pl.DataFrame:
    return pl.scan_parquet(str(root / "**" / "*.parquet")).sort("bucket_start").collect()


def test_missing_bucket_detection(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    rows = [_minute_row(start + timedelta(minutes=offset), open_price=100.0 + offset) for offset in range(10)]
    _write_minute_partitions(tmp_path, symbol, rows)

    reader = MinuteLakeReader(tmp_path / "futures" / "um" / "minute")
    writer = HigherTimeframeWriter(tmp_path / "htf")
    existing_bucket = aggregate_minutes(_canonical_frame(rows[:5]), parse_timeframe("5m"), symbol)
    writer.write_buckets(existing_bucket.with_columns(
        pl.col("bucket_start").dt.year().alias("year"),
        pl.col("bucket_start").dt.month().alias("month"),
        pl.col("bucket_start").dt.day().alias("day"),
    ))

    missing = detect_missing_buckets(
        reader.scan_available_minutes(symbol),
        writer.scan_existing_index(symbol, "5m"),
        parse_timeframe("5m"),
    )
    assert missing.height == 1
    assert missing.item(0, "bucket_start") == start + timedelta(minutes=5)


def test_incremental_bucket_completion(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    rows = [_minute_row(start + timedelta(minutes=offset)) for offset in range(4)]
    _write_minute_partitions(tmp_path, symbol, rows)

    reader = MinuteLakeReader(tmp_path / "futures" / "um" / "minute")
    writer = HigherTimeframeWriter(tmp_path / "htf")
    store = AggregatorStateStore(tmp_path / "state.sqlite")
    store.initialize()
    runner = IncrementalRunner(reader, writer, store, allow_incomplete_buckets=False, repair_lookback_minutes=120)

    first = runner.run_once(symbol, parse_timeframe("5m"))
    assert first.buckets_written == 0

    rows.append(_minute_row(start + timedelta(minutes=4)))
    _write_minute_partitions(tmp_path, symbol, rows)
    second = runner.run_once(symbol, parse_timeframe("5m"))
    assert second.buckets_written == 1
    written = _scan_target(tmp_path / "htf")
    assert written.height == 1
    assert written.item(0, "bucket_complete") is True


def test_late_arrival_repair_rewrites_recent_bucket(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    rows = [_minute_row(start + timedelta(minutes=offset), close=100.0 + offset) for offset in range(5)]
    _write_minute_partitions(tmp_path, symbol, rows)

    reader = MinuteLakeReader(tmp_path / "futures" / "um" / "minute")
    writer = HigherTimeframeWriter(tmp_path / "htf")
    store = AggregatorStateStore(tmp_path / "state.sqlite")
    store.initialize()
    backfill = BackfillRunner(reader, writer, store, allow_incomplete_buckets=False)
    backfill.run_for_timeframe(symbol, parse_timeframe("5m"))

    repaired_rows = [
        *rows,
        _minute_row(
            start + timedelta(minutes=4),
            close=500.0,
            high=500.0,
            low=99.0,
            arrival_time=int((start + timedelta(minutes=4)).timestamp() * 1000) + 999,
        )
    ]
    _write_minute_partitions(tmp_path, symbol, repaired_rows)
    incremental = IncrementalRunner(reader, writer, store, allow_incomplete_buckets=False, repair_lookback_minutes=120)
    incremental.run_once(symbol, parse_timeframe("5m"))

    written = _scan_target(tmp_path / "htf")
    assert written.height == 1
    assert written.item(0, "close") == 500.0
    assert written.item(0, "high") == 500.0


def test_idempotent_rewrites_do_not_duplicate_rows(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    rows = [_minute_row(start + timedelta(minutes=offset)) for offset in range(5)]
    _write_minute_partitions(tmp_path, symbol, rows)

    reader = MinuteLakeReader(tmp_path / "futures" / "um" / "minute")
    writer = HigherTimeframeWriter(tmp_path / "htf")
    store = AggregatorStateStore(tmp_path / "state.sqlite")
    store.initialize()
    runner = BackfillRunner(reader, writer, store, allow_incomplete_buckets=False)

    runner.run_for_timeframe(symbol, parse_timeframe("5m"))
    runner.run_for_timeframe(symbol, parse_timeframe("5m"))

    written = _scan_target(tmp_path / "htf")
    assert written.height == 1


def test_ohlc_correctness() -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    frame = _canonical_frame(
        [
            _minute_row(start + timedelta(minutes=0), open_price=100.0, high=101.0, low=99.0, close=100.5),
            _minute_row(start + timedelta(minutes=1), open_price=101.0, high=105.0, low=100.0, close=104.0),
            _minute_row(start + timedelta(minutes=2), open_price=104.0, high=104.5, low=95.0, close=96.0),
            _minute_row(start + timedelta(minutes=3), open_price=96.0, high=97.0, low=94.0, close=95.0),
            _minute_row(start + timedelta(minutes=4), open_price=95.0, high=96.0, low=93.0, close=94.5),
        ]
    )

    result = aggregate_minutes(frame, parse_timeframe("5m"), symbol)
    assert result.item(0, "open") == 100.0
    assert result.item(0, "high") == 105.0
    assert result.item(0, "low") == 93.0
    assert result.item(0, "close") == 94.5


def test_weighted_average_correctness() -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    frame = _canonical_frame(
        [
            _minute_row(start, volume_usdt=100.0, avg_spread_usdt=1.0, price_impact_100k=2.0),
            _minute_row(start + timedelta(minutes=1), volume_usdt=300.0, avg_spread_usdt=3.0, price_impact_100k=4.0),
            _minute_row(start + timedelta(minutes=2), volume_usdt=0.0, avg_spread_usdt=100.0, price_impact_100k=100.0),
            _minute_row(start + timedelta(minutes=3), volume_usdt=0.0, avg_spread_usdt=100.0, price_impact_100k=100.0),
            _minute_row(start + timedelta(minutes=4), volume_usdt=0.0, avg_spread_usdt=100.0, price_impact_100k=100.0),
        ]
    )

    result = aggregate_minutes(frame, parse_timeframe("5m"), symbol)
    assert result.item(0, "avg_spread_usdt") == 2.5
    assert result.item(0, "price_impact_100k") == 3.5


def test_snapshot_last_value_correctness() -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    frame = _canonical_frame(
        [
            _minute_row(start, oi_contracts=None, funding_rate=None),
            _minute_row(start + timedelta(minutes=1), oi_contracts=1000.0, funding_rate=0.001),
            _minute_row(start + timedelta(minutes=2), oi_contracts=None, funding_rate=None),
            _minute_row(start + timedelta(minutes=3), oi_contracts=1100.0, funding_rate=0.002),
            _minute_row(start + timedelta(minutes=4), oi_contracts=None, funding_rate=None),
        ]
    )

    result = aggregate_minutes(frame, parse_timeframe("5m"), symbol)
    assert result.item(0, "oi_contracts") == 1100.0
    assert result.item(0, "funding_rate") == 0.002


def test_weekly_and_monthly_boundary_correctness() -> None:
    symbol = "BTCUSDT"
    week_rows = [
        _minute_row(datetime(2026, 1, 4, 23, 59, tzinfo=UTC), close=100.0),
        _minute_row(datetime(2026, 1, 5, 0, 0, tzinfo=UTC), close=101.0),
    ]
    month_rows = [
        _minute_row(datetime(2026, 1, 31, 23, 59, tzinfo=UTC), close=200.0),
        _minute_row(datetime(2026, 2, 1, 0, 0, tzinfo=UTC), close=201.0),
    ]

    weekly = aggregate_minutes(_canonical_frame(week_rows), parse_timeframe("1w"), symbol).sort("bucket_start")
    monthly = aggregate_minutes(_canonical_frame(month_rows), parse_timeframe("1M"), symbol).sort("bucket_start")

    assert weekly.get_column("bucket_start").to_list() == [
        datetime(2025, 12, 29, 0, 0, tzinfo=UTC),
        datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
    ]
    assert monthly.get_column("bucket_start").to_list() == [
        datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
    ]


def test_incomplete_bucket_handling(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    rows = [_minute_row(start + timedelta(minutes=offset)) for offset in range(4)]
    _write_minute_partitions(tmp_path, symbol, rows)

    reader = MinuteLakeReader(tmp_path / "futures" / "um" / "minute")
    writer = HigherTimeframeWriter(tmp_path / "htf")
    store = AggregatorStateStore(tmp_path / "state.sqlite")
    store.initialize()
    runner = BackfillRunner(reader, writer, store, allow_incomplete_buckets=False)

    result = runner.run_for_timeframe(symbol, parse_timeframe("5m"))
    assert result.buckets_written == 0
    assert not (tmp_path / "htf").exists()
