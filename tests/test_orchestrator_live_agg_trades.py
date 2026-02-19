from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from binance_minute_lake.core.config import Settings
from binance_minute_lake.core.enums import IngestionBand
from binance_minute_lake.pipeline.orchestrator import MinuteIngestionPipeline
from binance_minute_lake.sources.websocket import LiveCollector, LiveMinuteFeatures


class _StubLiveCollector(LiveCollector):
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.calls = 0

    def snapshot_for_minute(self, minute_timestamp_ms: int) -> LiveMinuteFeatures | None:
        return None

    def agg_trades_for_window(
        self,
        *,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, object]]:
        self.calls += 1
        return list(self.rows)


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        symbol="BTCUSDT",
        root_dir=tmp_path / "data",
        state_db=tmp_path / "state" / "ingestion_state.sqlite",
    )


def test_collect_uses_live_ws_agg_trades_before_rest(tmp_path: Path) -> None:
    minute = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    minute_ms = int(minute.timestamp() * 1000)
    live_rows = [
        {
            "agg_trade_id": 1,
            "price": 100.0,
            "qty": 1.5,
            "first_trade_id": 10,
            "last_trade_id": 10,
            "transact_time": minute_ms + 10_000,
            "is_buyer_maker": False,
        },
        {
            "agg_trade_id": 2,
            "price": 100.2,
            "qty": 1.0,
            "first_trade_id": 11,
            "last_trade_id": 11,
            "transact_time": minute_ms + 20_000,
            "is_buyer_maker": True,
        },
    ]
    collector = _StubLiveCollector(rows=live_rows)
    pipeline = MinuteIngestionPipeline(settings=_settings(tmp_path), live_collector=collector)

    def _raise_if_called(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("REST aggTrades should not be called when live rows are available")

    try:
        pipeline._rest.fetch_agg_trades = _raise_if_called  # type: ignore[method-assign]
        pipeline._rest.fetch_klines = lambda *args, **kwargs: [  # type: ignore[method-assign]
            {
                "open_time": minute_ms,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume_btc": 2.0,
                "close_time": minute_ms + 59_999,
                "volume_usdt": 200000.0,
                "trade_count": 2,
                "taker_buy_vol_btc": 1.0,
                "taker_buy_vol_usdt": 100000.0,
            }
        ]
        pipeline._rest.fetch_mark_price_klines = lambda *args, **kwargs: [  # type: ignore[method-assign]
            {
                "open_time": minute_ms,
                "mark_price_open": 100.0,
                "mark_price_high": 101.0,
                "mark_price_low": 99.0,
                "mark_price_close": 100.5,
            }
        ]
        pipeline._rest.fetch_index_price_klines = lambda *args, **kwargs: [  # type: ignore[method-assign]
            {
                "open_time": minute_ms,
                "index_price_open": 100.0,
                "index_price_high": 101.0,
                "index_price_low": 99.0,
                "index_price_close": 100.5,
            }
        ]
        pipeline._rest.fetch_book_ticker = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "bid_price": 100.0,
            "bid_qty": 10.0,
            "ask_price": 100.1,
            "ask_qty": 12.0,
            "event_time": minute_ms,
        }

        frame = pipeline._collect_and_transform(
            window_start=minute,
            window_end=minute,
            band=IngestionBand.HOT,
            include_rest_enrichment=False,
        )
    finally:
        pipeline.close()

    assert collector.calls == 1
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row["count_buy_trades"] == 1
    assert row["count_sell_trades"] == 1


def test_collect_warm_band_skips_rest_agg_trades_fallback(tmp_path: Path) -> None:
    minute = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    minute_ms = int(minute.timestamp() * 1000)
    collector = _StubLiveCollector(rows=[])
    pipeline = MinuteIngestionPipeline(settings=_settings(tmp_path), live_collector=collector)

    def _raise_if_called(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("REST aggTrades should be skipped for warm windows without live rows")

    try:
        pipeline._rest.fetch_agg_trades = _raise_if_called  # type: ignore[method-assign]
        pipeline._rest.fetch_klines = lambda *args, **kwargs: [  # type: ignore[method-assign]
            {
                "open_time": minute_ms,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume_btc": 2.0,
                "close_time": minute_ms + 59_999,
                "volume_usdt": 200000.0,
                "trade_count": 2,
                "taker_buy_vol_btc": 1.0,
                "taker_buy_vol_usdt": 100000.0,
            }
        ]
        pipeline._rest.fetch_mark_price_klines = lambda *args, **kwargs: [  # type: ignore[method-assign]
            {
                "open_time": minute_ms,
                "mark_price_open": 100.0,
                "mark_price_high": 101.0,
                "mark_price_low": 99.0,
                "mark_price_close": 100.5,
            }
        ]
        pipeline._rest.fetch_index_price_klines = lambda *args, **kwargs: [  # type: ignore[method-assign]
            {
                "open_time": minute_ms,
                "index_price_open": 100.0,
                "index_price_high": 101.0,
                "index_price_low": 99.0,
                "index_price_close": 100.5,
            }
        ]
        pipeline._rest.fetch_book_ticker = lambda *args, **kwargs: {  # type: ignore[method-assign]
            "bid_price": 100.0,
            "bid_qty": 10.0,
            "ask_price": 100.1,
            "ask_qty": 12.0,
            "event_time": minute_ms,
        }

        frame = pipeline._collect_and_transform(
            window_start=minute,
            window_end=minute,
            band=IngestionBand.WARM,
            include_rest_enrichment=False,
        )
    finally:
        pipeline.close()

    assert collector.calls == 1
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row["count_buy_trades"] == 0
    assert row["count_sell_trades"] == 0
