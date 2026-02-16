from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from binance_minute_lake.core.schema import canonical_column_names
from binance_minute_lake.state.store import SQLiteStateStore
from binance_minute_lake.validation.dq import DQValidator
from binance_minute_lake.writer.atomic import AtomicParquetWriter


def _canonical_row(ts: datetime, open_price: float) -> dict[str, object]:
    row: dict[str, object] = {column: None for column in canonical_column_names()}
    row["timestamp"] = ts
    row["open"] = open_price
    row["high"] = open_price + 1.0
    row["low"] = open_price - 1.0
    row["close"] = open_price + 0.5
    row["volume_btc"] = 1.2
    row["volume_usdt"] = 120000.0
    row["trade_count"] = 10
    row["mark_price_open"] = open_price + 0.1
    row["mark_price_close"] = open_price + 0.4
    row["index_price_open"] = open_price
    row["index_price_close"] = open_price + 0.2
    return row


def test_atomic_write_creates_partition_and_ledger(tmp_path: Path) -> None:
    state_store = SQLiteStateStore(tmp_path / "state.sqlite")
    state_store.initialize()

    frame = pl.DataFrame([_canonical_row(datetime(2026, 1, 15, 10, 0, tzinfo=UTC), 100.0)])

    writer = AtomicParquetWriter(tmp_path, state_store, DQValidator())
    output = writer.write_hour_partition(
        symbol="BTCUSDT",
        hour_start=datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
        frame=frame,
    )

    assert output.exists()
    latest = state_store.latest_partition("BTCUSDT")
    assert latest is not None
    assert latest.row_count == 1
    assert latest.path == str(output)


def test_atomic_write_merges_existing_partition_without_losing_rows(tmp_path: Path) -> None:
    state_store = SQLiteStateStore(tmp_path / "state.sqlite")
    state_store.initialize()
    writer = AtomicParquetWriter(tmp_path, state_store, DQValidator())
    hour_start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)

    writer.write_hour_partition(
        symbol="BTCUSDT",
        hour_start=hour_start,
        frame=pl.DataFrame([_canonical_row(datetime(2026, 1, 15, 10, 0, tzinfo=UTC), 100.0)]),
    )
    output = writer.write_hour_partition(
        symbol="BTCUSDT",
        hour_start=hour_start,
        frame=pl.DataFrame([_canonical_row(datetime(2026, 1, 15, 10, 1, tzinfo=UTC), 101.0)]),
    )
    writer.write_hour_partition(
        symbol="BTCUSDT",
        hour_start=hour_start,
        frame=pl.DataFrame([_canonical_row(datetime(2026, 1, 15, 10, 0, tzinfo=UTC), 200.0)]),
    )

    merged = pl.read_parquet(output).sort("timestamp")
    assert merged.height == 2
    row_open_10_00 = (
        merged.filter(pl.col("timestamp") == datetime(2026, 1, 15, 10, 0, tzinfo=UTC))
        .select("open")
        .item()
    )
    row_open_10_01 = (
        merged.filter(pl.col("timestamp") == datetime(2026, 1, 15, 10, 1, tzinfo=UTC))
        .select("open")
        .item()
    )
    assert row_open_10_00 == 200.0
    assert row_open_10_01 == 101.0


def test_atomic_write_preserves_live_columns_when_rewritten_without_live_data(tmp_path: Path) -> None:
    state_store = SQLiteStateStore(tmp_path / "state.sqlite")
    state_store.initialize()
    writer = AtomicParquetWriter(tmp_path, state_store, DQValidator())
    hour_start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    minute_ts = datetime(2026, 1, 15, 10, 3, tzinfo=UTC)

    live_row = _canonical_row(minute_ts, 100.0)
    live_row.update(
        {
            "has_ws_latency": True,
            "has_depth": True,
            "event_time": int(minute_ts.timestamp() * 1000) + 10,
            "arrival_time": int(minute_ts.timestamp() * 1000) + 30,
            "latency_engine": 20,
            "latency_network": 21,
            "update_id_start": 100,
            "update_id_end": 110,
            "price_impact_100k": 0.0002,
            "impact_fillable": True,
        }
    )
    writer.write_hour_partition(
        symbol="BTCUSDT",
        hour_start=hour_start,
        frame=pl.DataFrame([live_row]),
    )

    rewritten_row = _canonical_row(minute_ts, 101.0)
    rewritten_row.update(
        {
            "has_ws_latency": False,
            "has_depth": False,
            "has_liq": False,
        }
    )
    output = writer.write_hour_partition(
        symbol="BTCUSDT",
        hour_start=hour_start,
        frame=pl.DataFrame([rewritten_row]),
    )

    merged = pl.read_parquet(output)
    row = merged.row(0, named=True)
    assert row["open"] == 101.0
    assert row["has_ws_latency"] is True
    assert row["has_depth"] is True
    assert row["event_time"] == int(minute_ts.timestamp() * 1000) + 10
    assert row["update_id_start"] == 100
    assert row["price_impact_100k"] == 0.0002
