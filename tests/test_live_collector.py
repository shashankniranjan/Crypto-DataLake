from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from binance_minute_lake.sources.websocket import (
    DepthDiffEvent,
    DepthOrderBook,
    DepthSyncError,
    InMemoryLiveCollector,
    LiquidationOrderEvent,
    LiveEventStore,
    floor_to_minute_ms,
)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def test_depth_sync_requires_first_event_to_bridge_snapshot() -> None:
    book = DepthOrderBook()
    book.buffer_event(
        DepthDiffEvent(
            symbol="BTCUSDT",
            event_time=_ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC)),
            first_update_id=120,
            final_update_id=125,
            bid_deltas=((99.0, 10.0),),
            ask_deltas=((101.0, 10.0),),
        )
    )

    with pytest.raises(DepthSyncError):
        book.sync_from_snapshot(
            last_update_id=100,
            bids=[(99.0, 10.0)],
            asks=[(101.0, 10.0)],
        )


def test_depth_sync_restart_path_and_impact_projection() -> None:
    collector = InMemoryLiveCollector()
    event_minute = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    event_time = _ms(event_minute)

    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=event_time,
        transact_time=event_time - 10,
        first_update_id=101,
        final_update_id=105,
        bid_deltas=[(99.0, 1000.0)],
        ask_deltas=[(101.0, 1000.0)],
        arrival_time=event_time + 20,
    )
    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=event_time + 500,
        transact_time=event_time + 480,
        first_update_id=106,
        final_update_id=110,
        bid_deltas=[(99.5, 1100.0)],
        ask_deltas=[(101.5, 1100.0)],
        arrival_time=event_time + 530,
    )

    collector.set_depth_snapshot(
        symbol="BTCUSDT",
        last_update_id=102,
        bids=[(99.0, 2000.0), (98.5, 2000.0)],
        asks=[(100.5, 2000.0), (101.0, 2000.0), (101.5, 2000.0)],
        minute_timestamp_ms=event_time,
    )
    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=event_time + 900,
        transact_time=event_time + 890,
        first_update_id=111,
        final_update_id=115,
        bid_deltas=[(99.75, 1200.0)],
        ask_deltas=[(100.75, 2200.0)],
        arrival_time=event_time + 920,
    )

    snapshot = collector.snapshot_for_minute(event_time)
    assert snapshot.has_depth is True
    assert snapshot.update_id_start == 101
    assert snapshot.update_id_end == 115
    assert snapshot.impact_fillable is True
    assert snapshot.price_impact_100k is not None
    assert snapshot.depth_degraded is False


def test_depth_has_depth_when_events_arrive_even_if_unsynced() -> None:
    collector = InMemoryLiveCollector()
    event_minute = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    event_time = _ms(event_minute)

    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=event_time,
        transact_time=event_time - 10,
        first_update_id=101,
        final_update_id=105,
        bid_deltas=[(99.0, 1000.0)],
        ask_deltas=[(101.0, 1000.0)],
        arrival_time=event_time + 20,
    )

    unsynced = collector.snapshot_for_minute(event_time)
    assert unsynced.has_depth is True
    assert unsynced.depth_degraded is True
    assert unsynced.update_id_start == 101
    assert unsynced.update_id_end == 105


def test_liquidation_event_driven_semantics() -> None:
    collector = InMemoryLiveCollector(liquidation_unfilled_supported=True)
    minute = floor_to_minute_ms(_ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC)))
    next_minute = minute + 60_000

    collector.mark_liquidation_heartbeat(minute)
    healthy_empty = collector.snapshot_for_minute(minute)
    assert healthy_empty.has_liq is False
    assert healthy_empty.liq_long_vol_usdt is None
    assert healthy_empty.liq_short_vol_usdt is None
    assert healthy_empty.liq_long_count is None
    assert healthy_empty.liq_short_count is None
    assert healthy_empty.liq_avg_fill_price is None
    assert healthy_empty.liq_unfilled_ratio is None
    assert healthy_empty.liq_unfilled_supported is None

    unhealthy_empty = collector.snapshot_for_minute(next_minute)
    assert unhealthy_empty.has_liq is False
    assert unhealthy_empty.liq_long_vol_usdt is None
    assert unhealthy_empty.liq_short_vol_usdt is None
    assert unhealthy_empty.liq_long_count is None
    assert unhealthy_empty.liq_short_count is None
    assert unhealthy_empty.liq_unfilled_supported is None

    collector.ingest_liquidation_event(
        LiquidationOrderEvent(
            symbol="BTCUSDT",
            event_time=next_minute + 1_000,
            side="SELL",
            price=100.0,
            quantity=3.0,
            orig_quantity=3.0,
            executed_quantity=2.5,
        )
    )
    populated = collector.snapshot_for_minute(next_minute)
    assert populated.has_liq is True
    assert populated.liq_long_vol_usdt == 300.0
    assert populated.liq_short_vol_usdt == 0.0
    assert populated.liq_long_count == 1
    assert populated.liq_short_count == 0
    assert populated.liq_avg_fill_price == 100.0
    assert populated.liq_unfilled_ratio == pytest.approx(1.0 / 6.0)
    assert populated.liq_unfilled_supported is True


def test_ws_latency_bad_is_null_when_source_absent() -> None:
    collector = InMemoryLiveCollector()
    minute = floor_to_minute_ms(_ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC)))

    empty = collector.snapshot_for_minute(minute)
    assert empty.has_ws_latency is False
    assert empty.ws_latency_bad is None

    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=minute + 1_000,
        transact_time=minute + 990,
        first_update_id=101,
        final_update_id=105,
        bid_deltas=[(99.0, 1000.0)],
        ask_deltas=[(101.0, 1000.0)],
        arrival_time=minute + 1_025,
    )
    collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=minute + 2_000,
        transact_time=minute + 1_400,
        first_update_id=106,
        final_update_id=110,
        bid_deltas=[(99.0, 1000.0)],
        ask_deltas=[(101.0, 1000.0)],
        arrival_time=minute + 2_100,
    )

    populated = collector.snapshot_for_minute(minute)
    assert populated.has_ws_latency is True
    assert populated.event_time == minute + 2_000
    assert populated.transact_time == minute + 1_400
    assert populated.arrival_time == minute + 2_100
    assert populated.latency_engine == 100
    assert populated.latency_network == 700
    assert populated.ws_latency_bad is True


def test_snapshot_recovers_live_features_from_event_store(tmp_path: Path) -> None:
    store = LiveEventStore(tmp_path / "live_events.sqlite")
    writer_collector = InMemoryLiveCollector(event_store=store, symbol="BTCUSDT")
    minute = floor_to_minute_ms(_ms(datetime(2026, 1, 15, 10, 0, tzinfo=UTC)))

    writer_collector.set_depth_snapshot(
        symbol="BTCUSDT",
        last_update_id=100,
        bids=[(99.0, 10_000.0)],
        asks=[(101.0, 10_000.0)],
        minute_timestamp_ms=minute,
    )
    writer_collector.ingest_depth_diff(
        symbol="BTCUSDT",
        event_time=minute + 1_000,
        transact_time=minute + 990,
        first_update_id=101,
        final_update_id=105,
        bid_deltas=[(99.5, 12.0)],
        ask_deltas=[(100.5, 13.0)],
        previous_final_update_id=100,
        arrival_time=minute + 1_025,
    )
    writer_collector.ingest_liquidation_event(
        LiquidationOrderEvent(
            symbol="BTCUSDT",
            event_time=minute + 2_000,
            side="SELL",
            price=100.0,
            quantity=2.0,
            arrival_time=minute + 2_020,
            orig_quantity=2.0,
            executed_quantity=1.5,
        ),
        raw_payload={
            "o": {
                "q": "2.0",
                "l": "1.5",
            }
        },
    )

    recovered_collector = InMemoryLiveCollector(event_store=store, symbol="BTCUSDT")
    recovered = recovered_collector.snapshot_for_minute(minute)

    assert recovered.has_ws_latency is True
    assert recovered.has_depth is True
    assert recovered.has_liq is True
    assert recovered.event_time == minute + 1_000
    assert recovered.transact_time == minute + 990
    assert recovered.update_id_start == 101
    assert recovered.update_id_end == 105
    assert recovered.liq_long_count == 1
    assert recovered.liq_short_count == 0
    assert recovered.liq_unfilled_supported is True
    assert recovered.liq_unfilled_ratio == pytest.approx(0.25)
