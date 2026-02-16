from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

import pytest

from binance_minute_lake.sources.websocket import (
    BinanceWsPayloadProcessor,
    InMemoryLiveCollector,
    LiquidationOrderEvent,
    LiveEventStore,
    floor_to_minute_ms,
)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _open_file_descriptor_count() -> int | None:
    for path in (Path("/dev/fd"), Path("/proc/self/fd")):
        if path.exists():
            return len(list(path.iterdir()))
    return None


def test_payload_processor_ingests_depth_liquidation_and_trade() -> None:
    collector = InMemoryLiveCollector()
    processor = BinanceWsPayloadProcessor(collector=collector, symbol="BTCUSDT")
    minute = _ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC))

    collector.set_depth_snapshot(
        symbol="BTCUSDT",
        last_update_id=100,
        bids=[(99.0, 10_000.0)],
        asks=[(101.0, 10_000.0)],
        minute_timestamp_ms=minute,
    )

    processor.process_stream_payload(
        stream_name="btcusdt@depth@100ms",
        payload={
            "e": "depthUpdate",
            "E": minute + 5_000,
            "T": minute + 4_990,
            "s": "BTCUSDT",
            "U": 101,
            "u": 105,
            "pu": 100,
            "b": [["99.5", "12.0"]],
            "a": [["100.5", "15.0"]],
        },
        arrival_time_ms=minute + 5_020,
    )
    processor.process_stream_payload(
        stream_name="btcusdt@forceOrder",
        payload={
            "e": "forceOrder",
            "E": minute + 10_000,
            "o": {
                "s": "BTCUSDT",
                "S": "SELL",
                "p": "100.0",
                "ap": "100.0",
                "q": "3.0",
                "l": "2.5",
                "T": minute + 10_000,
            },
        },
        arrival_time_ms=minute + 10_050,
    )
    processor.process_stream_payload(
        stream_name="btcusdt@aggTrade",
        payload={
            "e": "aggTrade",
            "E": minute + 20_000,
            "s": "BTCUSDT",
            "T": minute + 19_980,
        },
        arrival_time_ms=minute + 20_030,
    )

    snapshot = collector.snapshot_for_minute(minute)
    assert snapshot.has_depth is True
    assert snapshot.update_id_start == 101
    assert snapshot.update_id_end == 105
    assert snapshot.has_liq is True
    assert snapshot.liq_long_count == 1
    assert snapshot.liq_short_count == 0
    assert snapshot.has_ws_latency is True
    assert snapshot.transact_time == minute + 4_990
    assert snapshot.latency_engine == 20
    assert snapshot.latency_network == 30


def test_live_event_store_writes_raw_tables_and_heartbeats(tmp_path: Path) -> None:
    store = LiveEventStore(tmp_path / "live_events.sqlite")
    collector = InMemoryLiveCollector(event_store=store)
    minute = floor_to_minute_ms(_ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC)))

    collector.mark_ws_heartbeat(minute, alive=True, last_message_time=minute + 1_000)
    collector.mark_depth_heartbeat(minute, alive=True, last_message_time=minute + 2_000)
    collector.mark_liquidation_heartbeat(minute, alive=True, last_message_time=minute + 3_000)
    collector.ingest_trade_event(
        symbol="BTCUSDT",
        event_time=minute + 1_000,
        transact_time=minute + 950,
        arrival_time=minute + 1_020,
    )
    collector.set_depth_snapshot(
        symbol="BTCUSDT",
        last_update_id=100,
        bids=[(99.0, 10_000.0)],
        asks=[(101.0, 10_000.0)],
        minute_timestamp_ms=minute,
    )
    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=minute + 2_000,
        transact_time=minute + 1_980,
        first_update_id=101,
        final_update_id=102,
        bid_deltas=[(99.5, 12.0)],
        ask_deltas=[(100.5, 13.0)],
        previous_final_update_id=100,
        arrival_time=minute + 2_030,
    )
    collector.ingest_liquidation_event(
        LiquidationOrderEvent(
            symbol="BTCUSDT",
            event_time=minute + 3_000,
            side="BUY",
            price=100.0,
            quantity=1.0,
            arrival_time=minute + 3_020,
            orig_quantity=1.0,
            executed_quantity=1.0,
        )
    )

    with closing(sqlite3.connect(tmp_path / "live_events.sqlite")) as connection:
        depth_rows = connection.execute("SELECT COUNT(*) FROM ws_depth_events").fetchone()[0]
        liq_rows = connection.execute("SELECT COUNT(*) FROM ws_liq_events").fetchone()[0]
        trade_rows = connection.execute("SELECT COUNT(*) FROM ws_trade_events").fetchone()[0]
        heartbeat_rows = connection.execute("SELECT COUNT(*) FROM consumer_heartbeats").fetchone()[0]

    assert depth_rows >= 1
    assert liq_rows >= 1
    assert trade_rows >= 1
    assert heartbeat_rows >= 3


def test_live_event_store_cleanup_prunes_old_rows_but_keeps_recent(tmp_path: Path) -> None:
    store = LiveEventStore(tmp_path / "live_events.sqlite")
    collector = InMemoryLiveCollector(event_store=store, symbol="BTCUSDT")

    old_minute = floor_to_minute_ms(_ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC)))
    recent_minute = old_minute + (3 * 60 * 60 * 1000)

    collector.mark_ws_heartbeat(old_minute, alive=True, last_message_time=old_minute + 1_000)
    collector.ingest_trade_event(
        symbol="BTCUSDT",
        event_time=old_minute + 1_000,
        transact_time=old_minute + 990,
        arrival_time=old_minute + 1_020,
    )
    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=old_minute + 2_000,
        transact_time=old_minute + 1_980,
        first_update_id=1,
        final_update_id=2,
        bid_deltas=[(99.5, 12.0)],
        ask_deltas=[(100.5, 13.0)],
        arrival_time=old_minute + 2_030,
    )
    collector.ingest_liquidation_event(
        LiquidationOrderEvent(
            symbol="BTCUSDT",
            event_time=old_minute + 3_000,
            side="SELL",
            price=100.0,
            quantity=1.0,
            arrival_time=old_minute + 3_010,
            orig_quantity=1.0,
            executed_quantity=1.0,
        )
    )

    collector.mark_ws_heartbeat(recent_minute, alive=True, last_message_time=recent_minute + 1_000)
    collector.ingest_trade_event(
        symbol="BTCUSDT",
        event_time=recent_minute + 1_000,
        transact_time=recent_minute + 990,
        arrival_time=recent_minute + 1_020,
    )
    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=recent_minute + 2_000,
        transact_time=recent_minute + 1_980,
        first_update_id=10,
        final_update_id=11,
        bid_deltas=[(99.5, 12.0)],
        ask_deltas=[(100.5, 13.0)],
        arrival_time=recent_minute + 2_030,
    )
    collector.ingest_liquidation_event(
        LiquidationOrderEvent(
            symbol="BTCUSDT",
            event_time=recent_minute + 3_000,
            side="BUY",
            price=101.0,
            quantity=1.5,
            arrival_time=recent_minute + 3_010,
            orig_quantity=1.5,
            executed_quantity=1.5,
        )
    )

    cutoff = old_minute + (2 * 60 * 60 * 1000)
    cleanup = store.cleanup_before_cutoff(
        event_cutoff_ms=cutoff,
        heartbeat_cutoff_ms=cutoff,
        vacuum=False,
    )
    assert cleanup.ws_events_deleted == 1
    assert cleanup.ws_depth_events_deleted == 1
    assert cleanup.ws_liq_events_deleted == 1
    assert cleanup.ws_trade_events_deleted == 1
    assert cleanup.consumer_heartbeats_deleted == 3

    with closing(sqlite3.connect(tmp_path / "live_events.sqlite")) as connection:
        ws_rows = connection.execute("SELECT COUNT(*) FROM ws_events").fetchone()[0]
        depth_rows = connection.execute("SELECT COUNT(*) FROM ws_depth_events").fetchone()[0]
        liq_rows = connection.execute("SELECT COUNT(*) FROM ws_liq_events").fetchone()[0]
        trade_rows = connection.execute("SELECT COUNT(*) FROM ws_trade_events").fetchone()[0]
        heartbeat_rows = connection.execute("SELECT COUNT(*) FROM consumer_heartbeats").fetchone()[0]

    assert ws_rows == 1
    assert depth_rows == 1
    assert liq_rows == 1
    assert trade_rows == 1
    assert heartbeat_rows == 3


def test_live_event_store_closes_sqlite_connections_per_write(tmp_path: Path) -> None:
    before = _open_file_descriptor_count()
    if before is None:
        pytest.skip("File descriptor introspection unavailable on this platform")

    store = LiveEventStore(tmp_path / "live_events.sqlite")
    collector = InMemoryLiveCollector(event_store=store, symbol="BTCUSDT")
    minute = floor_to_minute_ms(_ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC)))

    for idx in range(200):
        timestamp = minute + (idx * 60_000)
        collector.mark_ws_heartbeat(timestamp, alive=True, last_message_time=timestamp + 100)
        collector.ingest_trade_event(
            symbol="BTCUSDT",
            event_time=timestamp + 1_000,
            transact_time=timestamp + 980,
            arrival_time=timestamp + 1_010,
        )

    after = _open_file_descriptor_count()
    assert after is not None
    assert (after - before) < 25
