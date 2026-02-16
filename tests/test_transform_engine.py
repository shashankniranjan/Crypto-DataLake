from datetime import UTC, datetime, timedelta

import polars as pl
import pytest

from binance_minute_lake.core.schema import canonical_column_names
from binance_minute_lake.pipeline.orchestrator import MinuteIngestionPipeline
from binance_minute_lake.sources.websocket import LiveMinuteFeatures
from binance_minute_lake.transforms.minute_builder import MinuteTransformEngine


def test_transform_engine_returns_canonical_columns() -> None:
    engine = MinuteTransformEngine(max_ffill_minutes=60)
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    end = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)

    frame = engine.build_canonical_frame(
        start_minute=start,
        end_minute=end,
        klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume_btc": 2.0,
                "volume_usdt": 200000.0,
                "trade_count": 20,
                "taker_buy_vol_btc": 1.1,
                "taker_buy_vol_usdt": 110000.0,
            }
        ],
        mark_price_klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "mark_price_open": 100.1,
                "mark_price_high": 101.2,
                "mark_price_low": 99.1,
                "mark_price_close": 100.4,
            }
        ],
        index_price_klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "index_price_open": 100.0,
                "index_price_high": 101.1,
                "index_price_low": 99.0,
                "index_price_close": 100.2,
            }
        ],
        agg_trades=[],
        funding_rates=[],
    )

    assert frame.height == 1
    assert frame.width == len(canonical_column_names())
    row = frame.row(0, named=True)
    assert row["vwap_1m"] == row["close"]
    assert row["open"] == 100.0


def test_transform_engine_ffills_book_ticker_metrics_from_snapshot() -> None:
    engine = MinuteTransformEngine(max_ffill_minutes=60)
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    next_minute = datetime(2026, 1, 15, 10, 1, tzinfo=UTC)

    frame = engine.build_canonical_frame(
        start_minute=start,
        end_minute=next_minute,
        klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume_btc": 2.0,
                "volume_usdt": 200000.0,
                "trade_count": 20,
                "taker_buy_vol_btc": 1.1,
                "taker_buy_vol_usdt": 110000.0,
            },
            {
                "open_time": int(next_minute.timestamp() * 1000),
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume_btc": 1.5,
                "volume_usdt": 151000.0,
                "trade_count": 10,
                "taker_buy_vol_btc": 0.8,
                "taker_buy_vol_usdt": 81000.0,
            },
        ],
        mark_price_klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "mark_price_open": 100.1,
                "mark_price_high": 101.2,
                "mark_price_low": 99.1,
                "mark_price_close": 100.4,
            },
            {
                "open_time": int(next_minute.timestamp() * 1000),
                "mark_price_open": 101.1,
                "mark_price_high": 102.2,
                "mark_price_low": 100.1,
                "mark_price_close": 101.4,
            },
        ],
        index_price_klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "index_price_open": 100.0,
                "index_price_high": 101.1,
                "index_price_low": 99.0,
                "index_price_close": 100.2,
            },
            {
                "open_time": int(next_minute.timestamp() * 1000),
                "index_price_open": 101.0,
                "index_price_high": 102.1,
                "index_price_low": 100.0,
                "index_price_close": 101.2,
            },
        ],
        agg_trades=[],
        funding_rates=[],
        book_ticker_snapshots=[
            {
                "event_time": int(start.timestamp() * 1000),
                "bid_price": 100.0,
                "bid_qty": 10.0,
                "ask_price": 101.0,
                "ask_qty": 8.0,
            }
        ],
    )

    assert frame.height == 2
    spreads = frame.select("avg_spread_usdt").to_series().to_list()
    micro_prices = frame.select("micro_price_close").to_series().to_list()
    assert spreads == [1.0, 1.0]
    assert all(value is not None for value in micro_prices)


def test_transform_engine_supports_direct_oi_metric_rows() -> None:
    engine = MinuteTransformEngine(max_ffill_minutes=60)
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    next_minute = datetime(2026, 1, 15, 10, 1, tzinfo=UTC)

    frame = engine.build_canonical_frame(
        start_minute=start,
        end_minute=next_minute,
        klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume_btc": 2.0,
                "volume_usdt": 200000.0,
                "trade_count": 20,
                "taker_buy_vol_btc": 1.1,
                "taker_buy_vol_usdt": 110000.0,
            },
            {
                "open_time": int(next_minute.timestamp() * 1000),
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume_btc": 1.5,
                "volume_usdt": 151000.0,
                "trade_count": 10,
                "taker_buy_vol_btc": 0.8,
                "taker_buy_vol_usdt": 81000.0,
            },
        ],
        mark_price_klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "mark_price_open": 100.1,
                "mark_price_high": 101.2,
                "mark_price_low": 99.1,
                "mark_price_close": 100.4,
            },
            {
                "open_time": int(next_minute.timestamp() * 1000),
                "mark_price_open": 101.1,
                "mark_price_high": 102.2,
                "mark_price_low": 100.1,
                "mark_price_close": 101.4,
            },
        ],
        index_price_klines=[
            {
                "open_time": int(start.timestamp() * 1000),
                "index_price_open": 100.0,
                "index_price_high": 101.1,
                "index_price_low": 99.0,
                "index_price_close": 100.2,
            },
            {
                "open_time": int(next_minute.timestamp() * 1000),
                "index_price_open": 101.0,
                "index_price_high": 102.1,
                "index_price_low": 100.0,
                "index_price_close": 101.2,
            },
        ],
        agg_trades=[],
        funding_rates=[],
        metrics_rows=[
            {
                "create_time": int(start.timestamp() * 1000),
                "oi_contracts": 12345.0,
                "oi_value_usdt": 987654321.0,
            }
        ],
    )

    oi_contracts = frame.select("oi_contracts").to_series().to_list()
    oi_value = frame.select("oi_value_usdt").to_series().to_list()
    assert oi_contracts == [12345.0, 12345.0]
    assert oi_value == [987654321.0, 987654321.0]


def test_seed_funding_rates_for_window_promotes_latest_rate() -> None:
    window_start = datetime(2026, 1, 15, 13, 0, tzinfo=UTC)
    window_end = datetime(2026, 1, 15, 13, 59, tzinfo=UTC)
    source = [
        {
            "symbol": "BTCUSDT",
            "funding_rate": 0.0001,
            "funding_time": int(datetime(2026, 1, 15, 8, 0, tzinfo=UTC).timestamp() * 1000),
            "mark_price": 69000.0,
        }
    ]

    seeded = MinuteIngestionPipeline._seed_funding_rates_for_window(source, window_start, window_end)
    assert len(seeded) == 2
    assert seeded[0]["funding_time"] == int(window_start.timestamp() * 1000)


def test_transform_engine_ls_ratio_forward_fill_with_freshness_gate() -> None:
    engine = MinuteTransformEngine(max_ffill_minutes=60)
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    end = datetime(2026, 1, 15, 10, 40, tzinfo=UTC)
    minute_count = int((end - start).total_seconds() // 60) + 1

    def _minute_rows(base_open: float) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for offset in range(minute_count):
            minute = start + timedelta(minutes=offset)
            open_time = int(minute.timestamp() * 1000)
            rows.append(
                {
                    "open_time": open_time,
                    "open": base_open + offset,
                    "high": base_open + offset + 1.0,
                    "low": base_open + offset - 1.0,
                    "close": base_open + offset + 0.5,
                    "volume_btc": 1.0,
                    "volume_usdt": 100000.0,
                    "trade_count": 10,
                    "taker_buy_vol_btc": 0.5,
                    "taker_buy_vol_usdt": 50000.0,
                }
            )
        return rows

    klines = _minute_rows(100.0)
    mark_price_klines = [
        {
            "open_time": item["open_time"],
            "mark_price_open": item["open"],
            "mark_price_high": item["high"],
            "mark_price_low": item["low"],
            "mark_price_close": item["close"],
        }
        for item in klines
    ]
    index_price_klines = [
        {
            "open_time": item["open_time"],
            "index_price_open": item["open"],
            "index_price_high": item["high"],
            "index_price_low": item["low"],
            "index_price_close": item["close"],
        }
        for item in klines
    ]

    top_rows = [
        {
            "data_time": int(datetime(2026, 1, 15, 10, 0, tzinfo=UTC).timestamp() * 1000),
            "ratio": 1.2,
            "long_account": 0.55,
            "short_account": 0.45,
        },
        {
            "data_time": int(datetime(2026, 1, 15, 10, 5, tzinfo=UTC).timestamp() * 1000),
            "ratio": 1.3,
            "long_account": 0.57,
            "short_account": 0.43,
        },
    ]
    global_rows = [
        {
            "data_time": int(datetime(2026, 1, 15, 10, 0, tzinfo=UTC).timestamp() * 1000),
            "ratio": 1.0,
            "long_account": 0.51,
            "short_account": 0.49,
        },
        {
            "data_time": int(datetime(2026, 1, 15, 10, 5, tzinfo=UTC).timestamp() * 1000),
            "ratio": 1.1,
            "long_account": 0.52,
            "short_account": 0.48,
        },
    ]

    frame = engine.build_canonical_frame(
        start_minute=start,
        end_minute=end,
        klines=klines,
        mark_price_klines=mark_price_klines,
        index_price_klines=index_price_klines,
        agg_trades=[],
        funding_rates=[],
        top_trader_ratio_rows=top_rows,
        global_ratio_rows=global_rows,
    )

    row_1004 = frame.filter(pl.col("timestamp") == datetime(2026, 1, 15, 10, 4, tzinfo=UTC)).row(0, named=True)
    row_1006 = frame.filter(pl.col("timestamp") == datetime(2026, 1, 15, 10, 6, tzinfo=UTC)).row(0, named=True)
    row_1036 = frame.filter(pl.col("timestamp") == datetime(2026, 1, 15, 10, 36, tzinfo=UTC)).row(0, named=True)

    assert row_1004["top_trader_ls_ratio_acct"] == 1.2
    assert row_1004["global_ls_ratio_acct"] == 1.0
    assert row_1004["has_ls_ratio"] is True

    assert row_1006["top_trader_ls_ratio_acct"] == 1.3
    assert row_1006["global_ls_ratio_acct"] == 1.1
    assert row_1006["ls_ratio_divergence"] == pytest.approx(0.2)
    assert row_1006["has_ls_ratio"] is True

    assert row_1036["top_trader_ls_ratio_acct"] is None
    assert row_1036["global_ls_ratio_acct"] is None
    assert row_1036["has_ls_ratio"] is False


def test_transform_engine_preserves_live_null_zero_semantics() -> None:
    engine = MinuteTransformEngine(max_ffill_minutes=60)
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    start_ms = int(start.timestamp() * 1000)

    frame = engine.build_canonical_frame(
        start_minute=start,
        end_minute=start,
        klines=[
            {
                "open_time": start_ms,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume_btc": 2.0,
                "volume_usdt": 200000.0,
                "trade_count": 20,
                "taker_buy_vol_btc": 1.0,
                "taker_buy_vol_usdt": 100000.0,
            }
        ],
        mark_price_klines=[
            {
                "open_time": start_ms,
                "mark_price_open": 100.0,
                "mark_price_high": 101.0,
                "mark_price_low": 99.0,
                "mark_price_close": 100.5,
            }
        ],
        index_price_klines=[
            {
                "open_time": start_ms,
                "index_price_open": 100.0,
                "index_price_high": 101.0,
                "index_price_low": 99.0,
                "index_price_close": 100.4,
            }
        ],
        agg_trades=[],
        funding_rates=[],
        live_features=[
            LiveMinuteFeatures(
                timestamp_ms=start_ms,
                has_ws_latency=False,
                has_depth=False,
                has_liq=True,
                ws_latency_bad=None,
                depth_degraded=None,
                liq_long_vol_usdt=0.0,
                liq_short_vol_usdt=0.0,
                liq_long_count=0,
                liq_short_count=0,
                liq_avg_fill_price=None,
                liq_unfilled_ratio=None,
                liq_unfilled_supported=True,
            )
        ],
    )

    row = frame.row(0, named=True)
    assert row["ws_latency_bad"] is None
    assert row["depth_degraded"] is None
    assert row["liq_long_vol_usdt"] == 0.0
    assert row["liq_short_vol_usdt"] == 0.0
    assert row["liq_long_count"] == 0
    assert row["liq_short_count"] == 0
    assert row["liq_avg_fill_price"] is None
    assert row["liq_unfilled_ratio"] is None
    assert row["liq_unfilled_supported"] is True
