from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
from fastapi.testclient import TestClient

from live_data_api_service.app import create_app
from live_data_api_service.repository import HigherTimeframeRepository, MinuteLakeRepository
from live_data_api_service.service import LiveDataApiService
from live_data_api_service.utils import cast_canonical_frame
from live_indicators.ema import calculate_tradingview_ema


def _canonical_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return cast_canonical_frame(pl.DataFrame(rows))


def _write_partitions(root_dir: Path, symbol: str, frame: pl.DataFrame) -> None:
    rows_by_hour: dict[datetime, list[dict[str, object]]] = defaultdict(list)
    for row in frame.to_dicts():
        timestamp = row["timestamp"]
        assert isinstance(timestamp, datetime)
        hour_start = timestamp.replace(minute=0, second=0, microsecond=0)
        rows_by_hour[hour_start].append(row)

    for hour_start, rows in rows_by_hour.items():
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
        _canonical_frame(rows).write_parquet(target)


def _write_higher_timeframe_partition(
    root_dir: Path,
    timeframe: str,
    symbol: str,
    day_start: datetime,
    frame: pl.DataFrame,
) -> None:
    target = (
        root_dir
        / "timeframe={timeframe}".format(timeframe=timeframe)
        / f"symbol={symbol}"
        / f"year={day_start:%Y}"
        / f"month={day_start:%m}"
        / f"day={day_start:%d}"
        / "part.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(target)


def test_calculate_tradingview_ema_uses_sma_seed() -> None:
    result = calculate_tradingview_ema([1.0, 2.0, 3.0, 4.0, 5.0], 3)
    assert result == [None, None, 2.0, 3.0, 4.0]


def test_live_indicators_endpoint_returns_ema_and_traditional_pivots(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    previous_day_start = datetime(2026, 1, 14, 0, 0, tzinfo=UTC)
    current_day_start = datetime(2026, 1, 15, 0, 0, tzinfo=UTC)

    rows: list[dict[str, object]] = []
    for minute in range(24 * 60):
        timestamp = previous_day_start + timedelta(minutes=minute)
        high = 110.0 if minute == 720 else 100.0
        low = 90.0 if minute == 360 else 100.0
        close = 105.0 if minute == (24 * 60) - 1 else 100.0
        rows.append(
            {
                "timestamp": timestamp,
                "open": 100.0,
                "high": high,
                "low": low,
                "close": close,
                "volume_btc": 1.0,
                "volume_usdt": 100.0,
                "trade_count": 1,
                "taker_buy_vol_btc": 0.5,
                "taker_buy_vol_usdt": 50.0,
                "mark_price_open": 100.0,
                "mark_price_close": close,
                "index_price_open": 100.0,
                "index_price_close": close,
            }
        )

    hourly_closes = {0: 200.0, 1: 210.0, 2: 220.0}
    for hour, close in hourly_closes.items():
        for minute in range(60):
            timestamp = current_day_start + timedelta(hours=hour, minutes=minute)
            rows.append(
                {
                    "timestamp": timestamp,
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume_btc": 1.0,
                    "volume_usdt": close,
                    "trade_count": 1,
                    "taker_buy_vol_btc": 0.5,
                    "taker_buy_vol_usdt": close / 2.0,
                    "mark_price_open": close,
                    "mark_price_close": close,
                    "index_price_open": close,
                    "index_price_close": close,
                }
            )

    _write_partitions(tmp_path, symbol, _canonical_frame(rows))

    class NoopProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            raise AssertionError("provider should not be called when the local lake fully covers the request")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=NoopProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/live-indicators",
        params={
            "coin": "btc",
            "ema_tf": "1hr",
            "ema_length": 3,
            "pivot_tf": "1d",
            "end_time": "2026-01-15T02:59:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == symbol
    assert payload["source"] == "local"
    assert payload["ema"]["timeframe"] == "1hr"
    assert payload["ema"]["length"] == 3
    assert payload["ema"]["bar_timestamp"] == "2026-01-15T02:00:00.000Z"
    assert payload["ema"]["bar_close"] == 220.0
    expected_hourly_closes = ([100.0] * 20) + [105.0, 200.0, 210.0, 220.0]
    expected_ema = calculate_tradingview_ema(expected_hourly_closes, 3)[-1]
    assert expected_ema is not None
    assert payload["ema"]["bars_used"] == len(expected_hourly_closes)
    assert payload["ema"]["value"] == expected_ema

    assert payload["pivots"]["type"] == "traditional"
    assert payload["pivots"]["timeframe"] == "1d"
    assert payload["pivots"]["reference_timestamp"] == "2026-01-14T00:00:00.000Z"
    assert payload["pivots"]["reference_ohlc"] == {
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
    }
    assert payload["pivots"]["p"] == 101.66666666666667
    assert payload["pivots"]["r1"] == 113.33333333333334
    assert payload["pivots"]["r2"] == 121.66666666666667
    assert payload["pivots"]["s1"] == 93.33333333333334
    assert payload["pivots"]["s2"] == 81.66666666666667
    assert payload["response_time_secs"] >= 0.0


def test_live_indicators_reuses_shared_canonical_window_for_native_ema_when_pivot_needs_legacy(tmp_path: Path) -> None:
    symbol = "ETHUSDT"
    start = datetime(2026, 1, 5, 0, 0, tzinfo=UTC)
    end = datetime(2026, 1, 15, 0, 0, tzinfo=UTC)

    class CanonicalOnlyProvider:
        def __init__(self) -> None:
            self.build_calls = 0

        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            live_collector: object | None = None,
        ) -> pl.DataFrame:
            assert requested_symbol == symbol
            assert live_collector is None
            self.build_calls += 1

            rows: list[dict[str, object]] = []
            cursor = start_time
            value = 100.0
            while cursor <= end_time:
                rows.append(
                    {
                        "timestamp": cursor,
                        "open": value,
                        "high": value + 1.0,
                        "low": value - 1.0,
                        "close": value,
                        "volume_btc": 1.0,
                        "volume_usdt": value,
                        "trade_count": 1,
                        "taker_buy_vol_btc": 0.5,
                        "taker_buy_vol_usdt": value / 2.0,
                        "mark_price_open": value,
                        "mark_price_close": value,
                        "index_price_open": value,
                        "index_price_close": value,
                    }
                )
                cursor += timedelta(minutes=1)
                value += 0.01
            return _canonical_frame(rows)

        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("ema 1m should reuse the shared canonical indicator window")

        def close(self) -> None:
            return None

    provider = CanonicalOnlyProvider()
    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=provider,
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/live-indicators",
        params={
            "coin": "eth",
            "ema_tf": "1m",
            "ema_length": 3,
            "pivot_tf": "1w",
            "end_time": "2026-01-15T00:10:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == symbol
    assert payload["ema"]["timeframe"] == "1m"
    assert payload["ema"]["metadata"]["fetch_mode"] == "aggregate_from_1m"
    assert "using_shared_indicator_canonical_window" in payload["ema"]["metadata"]["notes"]
    assert payload["pivots"]["timeframe"] == "1w"
    assert payload["pivots"]["metadata"]["fetch_mode"] == "aggregate_from_1m"
    assert provider.build_calls == 1


def test_live_indicators_uses_local_higher_timeframe_lake_for_btc_weekly_pivots(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    minute_rows = _canonical_frame(
        [
            {
                "timestamp": datetime(2026, 1, 18, 23, 36, tzinfo=UTC) + timedelta(minutes=offset),
                "open": 100.0 + offset,
                "high": 100.0 + offset,
                "low": 100.0 + offset,
                "close": 100.0 + offset,
                "volume_btc": 1.0,
                "volume_usdt": 100.0 + offset,
                "trade_count": 1,
                "taker_buy_vol_btc": 0.5,
                "taker_buy_vol_usdt": 50.0 + (offset / 2.0),
                "mark_price_open": 100.0 + offset,
                "mark_price_close": 100.0 + offset,
                "index_price_open": 100.0 + offset,
                "index_price_close": 100.0 + offset,
            }
            for offset in range(24)
        ]
    )
    _write_partitions(tmp_path, symbol, minute_rows)
    higher_tf_root = tmp_path / "futures" / "um" / "higher_timeframes"
    weekly_rows = pl.DataFrame(
        {
            "timeframe": ["1w"],
            "symbol": [symbol],
            "timestamp": [datetime(2026, 1, 5, 0, 0, tzinfo=UTC)],
            "bucket_start": [datetime(2026, 1, 5, 0, 0, tzinfo=UTC)],
            "bucket_end": [datetime(2026, 1, 12, 0, 0, tzinfo=UTC)],
            "open": [100.0],
            "high": [120.0],
            "low": [90.0],
            "close": [110.0],
            "volume_btc": [100.0],
            "volume_usdt": [11_000.0],
            "trade_count": [100],
            "vwap": [110.0],
            "avg_trade_size_btc": [1.0],
            "max_trade_size_btc": [4.0],
            "taker_buy_vol_btc": [60.0],
            "taker_buy_vol_usdt": [6_600.0],
            "net_taker_vol_btc": [20.0],
            "count_buy_trades": [60],
            "count_sell_trades": [40],
            "taker_buy_ratio": [0.6],
            "vol_buy_whale_btc": [10.0],
            "vol_sell_whale_btc": [8.0],
            "vol_buy_retail_btc": [6.0],
            "vol_sell_retail_btc": [4.0],
            "whale_trade_count": [5],
            "liq_long_vol_usdt": [0.0],
            "liq_short_vol_usdt": [0.0],
            "liq_long_count": [0],
            "liq_short_count": [0],
            "liq_avg_fill_price": [None],
            "liq_unfilled_ratio": [None],
            "liq_unfilled_supported": [False],
            "has_liq": [False],
            "oi_contracts": [1000.0],
            "oi_value_usdt": [1_000_000.0],
            "top_trader_ls_ratio_acct": [1.1],
            "global_ls_ratio_acct": [0.9],
            "ls_ratio_divergence": [0.2],
            "top_trader_long_pct": [0.55],
            "top_trader_short_pct": [0.45],
            "premium_index": [0.0001],
            "funding_rate": [0.0003],
            "predicted_funding": [0.0005],
            "next_funding_time": [1],
            "micro_price_close": [109.0],
            "mark_price_open": [100.0],
            "mark_price_close": [110.0],
            "index_price_open": [100.0],
            "index_price_close": [110.0],
            "avg_spread_usdt": [1.0],
            "bid_ask_imbalance": [0.1],
            "avg_bid_depth": [10.0],
            "avg_ask_depth": [11.0],
            "spread_pct": [0.001],
            "price_impact_100k": [0.002],
            "has_depth": [True],
            "impact_fillable": [True],
            "depth_degraded": [False],
            "has_ws_latency": [False],
            "ws_latency_bad": [False],
            "has_ls_ratio": [True],
            "realized_vol_htf": [0.02],
            "event_time": [1],
            "transact_time": [1],
            "arrival_time": [1],
            "update_id_start": [10],
            "update_id_end": [20],
            "expected_minutes_in_bucket": [10_080],
            "observed_minutes_in_bucket": [10_080],
            "missing_minutes_count": [0],
            "bucket_complete": [True],
        }
    )
    _write_higher_timeframe_partition(
        higher_tf_root,
        "1w",
        symbol,
        datetime(2026, 1, 5, 0, 0, tzinfo=UTC),
        weekly_rows,
    )

    class GuardProvider:
        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("BTC weekly pivots should come from the higher timeframe lake")

        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("BTC indicators should not fall back to Binance")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        higher_timeframe_repository=HigherTimeframeRepository(higher_tf_root),
        provider=GuardProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/live-indicators",
        params={
            "coin": "btc",
            "ema_tf": "1m",
            "ema_length": 3,
            "pivot_tf": "1w",
            "end_time": "2026-01-18T23:59:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "local"
    assert payload["ema"]["metadata"]["fetch_mode"] == "aggregate_from_1m"
    assert payload["pivots"]["metadata"]["fetch_mode"] == "direct_local_higher_tf"
    assert "using_local_btc_higher_timeframe_lake" in payload["pivots"]["metadata"]["notes"]
