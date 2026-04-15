from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
from fastapi.testclient import TestClient

from live_data_api_service.app import create_app
from live_data_api_service.repository import MinuteLakeRepository
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
