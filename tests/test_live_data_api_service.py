from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from math import log
from pathlib import Path
from threading import Event
from unittest.mock import patch

import polars as pl
import pytest
from fastapi.testclient import TestClient

from binance_minute_lake.sources.websocket import InMemoryLiveCollector, LiquidationOrderEvent, LiveEventStore
from binance_minute_lake.state.store import SQLiteStateStore
from live_data_api_service.aggregation import aggregate_canonical_frame
from live_data_api_service.alignment import AlignmentMode, align_series, normalize_bar_timestamp
from live_data_api_service.app import create_app
from live_data_api_service.binance_provider import BinanceCanonicalMinuteProvider
from live_data_api_service.capabilities import CandleFetchMode, FetchPlannerConfig, plan_timeframe_fetch
from live_data_api_service.repository import HigherTimeframeRepository, MinuteLakeRepository
from live_data_api_service.service import LiveDataApiService
from live_data_api_service.timeframes import parse_timeframe_requests, parse_timeframes
from live_data_api_service.utils import cast_canonical_frame, serialize_frame


def _canonical_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return cast_canonical_frame(pl.DataFrame(rows))


def _write_partition(root_dir: Path, symbol: str, hour_start: datetime, frame: pl.DataFrame) -> None:
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
    frame.write_parquet(target)


def _write_higher_timeframe_partition(
    root_dir: Path,
    timeframe: str,
    symbol: str,
    day_start: datetime,
    frame: pl.DataFrame,
) -> None:
    target = (
        root_dir
        / f"timeframe={timeframe}"
        / f"symbol={symbol}"
        / f"year={day_start:%Y}"
        / f"month={day_start:%m}"
        / f"day={day_start:%d}"
        / "part.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(target)


def test_aggregate_canonical_frame_builds_complete_5m_bar() -> None:
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = []
    for minute in range(5):
        timestamp = start + timedelta(minutes=minute)
        rows.append(
            {
                "timestamp": timestamp,
                "has_ws_latency": minute in {1, 2, 4},
                "has_depth": minute in {1, 4},
                "has_liq": minute == 2,
                "has_ls_ratio": True,
                "event_time": int(timestamp.timestamp() * 1000) + 10 if minute in {1, 2, 4} else None,
                "transact_time": int(timestamp.timestamp() * 1000) + 5 if minute in {1, 2, 4} else None,
                "arrival_time": int(timestamp.timestamp() * 1000) + 50 if minute in {1, 2, 4} else None,
                "latency_engine": {1: 100, 2: 200, 4: 300}.get(minute),
                "latency_network": {1: 80, 2: 180, 4: 280}.get(minute),
                "ws_latency_bad": minute == 4 if minute in {1, 2, 4} else None,
                "update_id_start": {1: 10, 4: 15}.get(minute),
                "update_id_end": {1: 20, 4: 30}.get(minute),
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "vwap_1m": 100.25 + minute,
                "micro_price_close": 100.4 + minute,
                "volume_btc": float(minute + 1),
                "volume_usdt": float((minute + 1) * 1000),
                "trade_count": (minute + 1) * 10,
                "max_trade_size_btc": 0.1 * (minute + 1),
                "taker_buy_vol_btc": 0.5 * (minute + 1),
                "taker_buy_vol_usdt": 500.0 * (minute + 1),
                "net_taker_vol_btc": 0.1 * (minute + 1),
                "count_buy_trades": minute + 1,
                "count_sell_trades": minute + 2,
                "vol_buy_whale_btc": 0.05 * (minute + 1),
                "vol_sell_whale_btc": 0.04 * (minute + 1),
                "vol_buy_retail_btc": 0.03 * (minute + 1),
                "vol_sell_retail_btc": 0.02 * (minute + 1),
                "whale_trade_count": minute,
                "realized_vol_1m": 0.01 * (minute + 1),
                "liq_long_vol_usdt": 1250.0 if minute == 2 else None,
                "liq_short_vol_usdt": 750.0 if minute == 2 else None,
                "liq_long_count": 1 if minute == 2 else None,
                "liq_short_count": 2 if minute == 2 else None,
                "liq_avg_fill_price": 102.5 if minute == 2 else None,
                "liq_unfilled_ratio": 0.2 if minute == 2 else None,
                "liq_unfilled_supported": True if minute == 2 else None,
                "avg_spread_usdt": 1.0 + (minute * 0.1),
                "bid_ask_imbalance": 0.01 * minute,
                "avg_bid_depth": 10.0 + minute,
                "avg_ask_depth": 11.0 + minute,
                "spread_pct": 0.001 + (minute * 0.0001),
                "price_impact_100k": 0.002 * minute if minute in {1, 4} else None,
                "impact_fillable": True if minute in {1, 4} else None,
                "depth_degraded": True if minute == 4 else (False if minute == 1 else None),
                "oi_contracts": 1000.0 + minute,
                "oi_value_usdt": 1_000_000.0 + (minute * 1000),
                "top_trader_ls_ratio_acct": 1.1 + (minute * 0.01),
                "global_ls_ratio_acct": 0.9 + (minute * 0.01),
                "ls_ratio_divergence": 0.2,
                "top_trader_long_pct": 0.55,
                "top_trader_short_pct": 0.45,
                "mark_price_open": 100.1 + minute,
                "mark_price_close": 100.2 + minute,
                "index_price_open": 99.9 + minute,
                "index_price_close": 100.0 + minute,
                "premium_index": 0.0,
                "funding_rate": 0.0001,
                "predicted_funding": 0.0002,
                "next_funding_time": int((timestamp + timedelta(hours=8)).timestamp() * 1000),
            }
        )

    frame = _canonical_frame(rows)
    aggregated = aggregate_canonical_frame(frame, parse_timeframes("5m")[0], limit=5)

    assert aggregated.height == 1
    row = aggregated.row(0, named=True)
    assert row["timestamp"] == start
    assert row["open"] == 100.0
    assert row["high"] == 105.0
    assert row["low"] == 99.0
    assert row["close"] == 104.5
    assert row["volume_btc"] == 15.0
    assert row["trade_count"] == 150
    assert row["avg_trade_size_btc"] == 0.1
    assert row["has_depth"] is True
    assert row["update_id_start"] == 10
    assert row["update_id_end"] == 30
    assert row["has_liq"] is True
    assert row["liq_long_vol_usdt"] == 1250.0
    assert row["liq_short_count"] == 2
    assert row["depth_degraded"] is True
    assert row["ws_latency_bad"] is True


def test_parse_timeframe_requests_supports_inline_limits_and_aliases() -> None:
    requests = parse_timeframe_requests("1m=50, 1h=15, 60m=9, 5m")

    assert [request.api_name for request in requests] == ["1m", "1hr", "5m"]
    assert [request.limit for request in requests] == [50, 15, None]


def test_fetch_planner_prefers_native_binance_candles() -> None:
    spec = parse_timeframes("3m")[0]

    decision = plan_timeframe_fetch(spec, FetchPlannerConfig())

    assert decision.candle_source == "binance_native"
    assert decision.fetch_mode == "direct_tf"
    assert decision.binance_interval == "3m"
    assert "open_interest_hist_not_supported_for_3m" in decision.notes
    assert "oi_hist_not_supported_for_3m" not in decision.notes
    assert "historical_depth_not_available_from_binance_rest" in decision.notes


def test_fetch_planner_keeps_legacy_path_when_native_disabled() -> None:
    spec = parse_timeframes("5m")[0]

    decision = plan_timeframe_fetch(
        spec,
        FetchPlannerConfig(
            enable_native_binance_tf_candles=False,
            candle_fetch_mode=CandleFetchMode.AGGREGATE_FROM_1M,
            allow_legacy_1m_fallback=True,
        ),
    )

    assert decision.candle_source == "legacy_1m"
    assert decision.fetch_mode == "aggregate_from_1m"
    assert decision.fallback_used is True
    assert "using_legacy_1m_aggregation_fallback" in decision.notes


def test_align_series_supports_exact_and_asof_bar_close() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
                datetime(2026, 1, 15, 10, 5, tzinfo=UTC),
            ]
        }
    ).with_columns(pl.col("timestamp").cast(pl.Datetime("ms", "UTC")))

    exact = align_series(
        frame,
        [
            {"ts": int(datetime(2026, 1, 15, 10, 0, tzinfo=UTC).timestamp() * 1000), "value": 1.0},
            {"ts": int(datetime(2026, 1, 15, 10, 5, tzinfo=UTC).timestamp() * 1000), "value": 2.0},
        ],
        source_time_col="ts",
        value_map={"value": "exact_value"},
        mode=AlignmentMode.EXACT_TIMESTAMP,
    )
    assert exact.get_column("exact_value").to_list() == [1.0, 2.0]

    asof = align_series(
        frame,
        [
            {"ts": int(datetime(2026, 1, 15, 10, 5, tzinfo=UTC).timestamp() * 1000), "rate": 0.001},
            {"ts": int(datetime(2026, 1, 15, 10, 10, tzinfo=UTC).timestamp() * 1000), "rate": 0.002},
        ],
        source_time_col="ts",
        value_map={"rate": "funding_rate"},
        mode=AlignmentMode.ASOF_BACKWARD,
        align_at_bar_close=True,
        bar_minutes=5,
    )
    assert asof.get_column("funding_rate").to_list() == [0.001, 0.002]


def test_normalize_bar_timestamp_maps_aux_and_candles_to_bar_open() -> None:
    candle_ts = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    aux_ts = datetime(2026, 1, 15, 10, 4, 59, tzinfo=UTC)

    assert normalize_bar_timestamp(candle_ts, 5) == candle_ts
    assert normalize_bar_timestamp(aux_ts, 5) == candle_ts


def test_align_series_falls_back_to_one_bar_asof_and_enforces_max_age() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [
                datetime(2026, 1, 15, 10, 0, tzinfo=UTC),
                datetime(2026, 1, 15, 10, 5, tzinfo=UTC),
                datetime(2026, 1, 15, 10, 10, tzinfo=UTC),
                datetime(2026, 1, 15, 10, 15, tzinfo=UTC),
            ]
        }
    ).with_columns(pl.col("timestamp").cast(pl.Datetime("ms", "UTC")))

    aligned = align_series(
        frame,
        [
            {"ts": int(datetime(2026, 1, 15, 10, 5, tzinfo=UTC).timestamp() * 1000), "value": 1.0},
            {"ts": int(datetime(2026, 1, 15, 10, 10, tzinfo=UTC).timestamp() * 1000), "value": 2.0},
        ],
        source_time_col="ts",
        value_map={"value": "oi_contracts"},
        mode=AlignmentMode.FORWARD_FILL_WITH_MAX_AGE,
        align_at_bar_close=True,
        bar_minutes=5,
        max_age=timedelta(minutes=5),
        normalize_source_timeframe_minutes=5,
    )

    assert aligned.get_column("oi_contracts").to_list() == [1.0, 2.0, 2.0, None]


def test_serialize_frame_emits_canonical_bar_fields_and_hides_internal_columns() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 1, 15, 10, 0, tzinfo=UTC)],
            "vwap_1m": [100.5],
            "realized_vol_1m": [0.0123],
            "_aux_ts": [datetime(2026, 1, 15, 10, 0, tzinfo=UTC)],
        }
    ).with_columns(pl.col("timestamp").cast(pl.Datetime("ms", "UTC")))

    rows = serialize_frame(frame)

    assert rows == [
        {
            "timestamp": "2026-01-15T10:00:00.000Z",
            "vwap_bar": 100.5,
            "realized_vol_bar": 0.0123,
        }
    ]


def test_serialize_frame_can_emit_deprecated_aliases_when_explicitly_enabled() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 1, 15, 10, 0, tzinfo=UTC)],
            "vwap_1m": [1.0],
            "realized_vol_1m": [2.0],
            "vwap_bar": [100.5],
            "realized_vol_bar": [0.0123],
        }
    ).with_columns(pl.col("timestamp").cast(pl.Datetime("ms", "UTC")))

    row = serialize_frame(frame, include_deprecated_fields=True)[0]

    assert row["vwap_bar"] == 100.5
    assert row["realized_vol_bar"] == 0.0123
    assert row["vwap_1m"] == row["vwap_bar"]
    assert row["realized_vol_1m"] == row["realized_vol_bar"]


def test_service_merges_local_rows_over_provider_rows(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    local_rows = [
        {
            "timestamp": start,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume_btc": 1.0,
            "volume_usdt": 1000.0,
            "trade_count": 10,
            "taker_buy_vol_btc": 0.5,
            "taker_buy_vol_usdt": 500.0,
            "has_depth": True,
            "update_id_start": 111,
            "update_id_end": 222,
            "mark_price_open": 100.0,
            "mark_price_close": 100.0,
            "index_price_open": 100.0,
            "index_price_close": 100.0,
        }
    ]
    local_frame = _canonical_frame(local_rows)
    _write_partition(tmp_path, symbol, start.replace(minute=0), local_frame)

    remote_rows = [
        {
            "timestamp": start,
            "open": 90.0,
            "high": 91.0,
            "low": 89.0,
            "close": 90.5,
            "volume_btc": 2.0,
            "volume_usdt": 1800.0,
            "trade_count": 20,
            "taker_buy_vol_btc": 1.0,
            "taker_buy_vol_usdt": 900.0,
            "has_depth": False,
            "update_id_start": None,
            "update_id_end": None,
            "mark_price_open": 90.0,
            "mark_price_close": 90.0,
            "index_price_open": 90.0,
            "index_price_close": 90.0,
        },
        {
            "timestamp": start - timedelta(minutes=1),
            "open": 89.0,
            "high": 90.0,
            "low": 88.0,
            "close": 89.5,
            "volume_btc": 1.0,
            "volume_usdt": 890.0,
            "trade_count": 10,
            "taker_buy_vol_btc": 0.5,
            "taker_buy_vol_usdt": 445.0,
            "mark_price_open": 89.0,
            "mark_price_close": 89.0,
            "index_price_open": 89.0,
            "index_price_close": 89.0,
        },
        {
            "timestamp": start + timedelta(minutes=1),
            "open": 101.0,
            "high": 102.0,
            "low": 100.0,
            "close": 101.5,
            "volume_btc": 1.5,
            "volume_usdt": 1500.0,
            "trade_count": 15,
            "taker_buy_vol_btc": 0.7,
            "taker_buy_vol_usdt": 700.0,
            "mark_price_open": 101.0,
            "mark_price_close": 101.0,
            "index_price_open": 101.0,
            "index_price_close": 101.0,
        },
    ]

    class PatchProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            assert requested_symbol == symbol
            assert start_time == start - timedelta(minutes=1)
            assert end_time == start + timedelta(minutes=1)
            return _canonical_frame(remote_rows)

        def close(self) -> None:
            return None

    state_store = SQLiteStateStore(tmp_path / "state.sqlite")
    state_store.initialize()
    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=PatchProvider(),
        default_limit=3,
        max_limit=10,
        on_demand_max_minutes=60,
        state_store=state_store,
    )

    payload = service.fetch_perpetual_data(
        coin="btc",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )

    assert payload["source"] == "local+binance"
    assert len(payload["data"]["1m"]) == 2
    local_row = payload["data"]["1m"][0]
    assert local_row["timestamp"] == "2026-01-15T10:00:00.000Z"
    assert local_row["open"] == 100.0
    assert local_row["has_depth"] is True
    assert local_row["update_id_start"] == 111
    assert local_row["update_id_end"] == 222
    assert payload["data"]["1m"][1]["timestamp"] == "2026-01-15T10:01:00.000Z"

    class GuardProvider:
        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("persisted missing rows should avoid another Binance patch")

        def close(self) -> None:
            return None

    persisted_service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=GuardProvider(),
        default_limit=3,
        max_limit=10,
        on_demand_max_minutes=60,
    )
    persisted_payload = persisted_service.fetch_perpetual_data(
        coin="btc",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )
    assert persisted_payload["source"] == "local"
    assert persisted_payload["data"]["1m"][0]["open"] == 100.0
    assert persisted_payload["data"]["1m"][1]["timestamp"] == "2026-01-15T10:01:00.000Z"


def test_btc_binance_patch_materializes_to_primary_minute_lake_and_prunes_vision_cache(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = [
        {
            "timestamp": start + timedelta(minutes=minute - 1),
            "open": 100.0 + minute,
            "high": 101.0 + minute,
            "low": 99.0 + minute,
            "close": 100.5 + minute,
            "volume_btc": 1.0,
            "volume_usdt": 100.0,
            "trade_count": 1,
            "taker_buy_vol_btc": 0.5,
            "taker_buy_vol_usdt": 50.0,
            "mark_price_open": 100.0 + minute,
            "mark_price_close": 100.5 + minute,
            "index_price_open": 100.0 + minute,
            "index_price_close": 100.5 + minute,
        }
        for minute in range(3)
    ]

    class PatchProvider:
        def __init__(self) -> None:
            self.build_calls: list[tuple[str, datetime, datetime]] = []
            self.delete_calls: list[tuple[str, datetime, datetime]] = []

        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            self.build_calls.append((requested_symbol, start_time, end_time))
            return _canonical_frame(rows)

        def delete_cached_vision_files(self, requested_symbol: str, start_time: datetime, end_time: datetime) -> int:
            self.delete_calls.append((requested_symbol, start_time, end_time))
            return 3

        def close(self) -> None:
            return None

    provider = PatchProvider()
    state_store = SQLiteStateStore(tmp_path / "state.sqlite")
    state_store.initialize()
    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=provider,
        default_limit=2,
        max_limit=10,
        on_demand_max_minutes=60,
        state_store=state_store,
    )

    payload = service.fetch_perpetual_data(
        coin="btc",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )

    assert payload["source"] == "binance"
    assert provider.build_calls == [(symbol, start - timedelta(minutes=1), start + timedelta(minutes=1))]
    assert provider.delete_calls == [(symbol, start - timedelta(minutes=1), start + timedelta(minutes=1))]
    assert (
        tmp_path
        / "futures"
        / "um"
        / "minute"
        / "symbol=BTCUSDT"
        / "year=2026"
        / "month=01"
        / "day=15"
        / "hour=10"
        / "part.parquet"
    ).exists()


def test_non_btc_binance_patch_materializes_to_flattened_cache_lake(tmp_path: Path) -> None:
    symbol = "ETHUSDT"
    main_root = tmp_path / "data"
    cache_root = tmp_path / ".cache" / "canonical_lake"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = [
        {
            "timestamp": start + timedelta(minutes=minute - 1),
            "open": 2000.0 + minute,
            "high": 2010.0 + minute,
            "low": 1990.0 + minute,
            "close": 2005.0 + minute,
            "volume_btc": 1.0,
            "volume_usdt": 2000.0,
            "trade_count": 1,
            "taker_buy_vol_btc": 0.5,
            "taker_buy_vol_usdt": 1000.0,
            "mark_price_open": 2000.0 + minute,
            "mark_price_close": 2005.0 + minute,
            "index_price_open": 1999.0 + minute,
            "index_price_close": 2004.0 + minute,
        }
        for minute in range(3)
    ]

    class PatchProvider:
        def __init__(self) -> None:
            self.build_calls = 0
            self.delete_calls = 0

        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            assert requested_symbol == symbol
            self.build_calls += 1
            return _canonical_frame(rows)

        def delete_cached_vision_files(self, requested_symbol: str, start_time: datetime, end_time: datetime) -> int:
            assert requested_symbol == symbol
            self.delete_calls += 1
            return 2

        def close(self) -> None:
            return None

    cache_state_store = SQLiteStateStore(cache_root / "state.sqlite")
    cache_state_store.initialize()
    provider = PatchProvider()
    service = LiveDataApiService(
        repository=MinuteLakeRepository(main_root),
        provider=provider,
        default_limit=2,
        max_limit=10,
        on_demand_max_minutes=60,
        canonical_cache_repository=MinuteLakeRepository(cache_root),
        canonical_cache_state_store=cache_state_store,
    )

    payload = service.fetch_perpetual_data(
        coin="eth",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )

    assert payload["source"] == "binance"
    assert provider.build_calls == 1
    assert provider.delete_calls == 1
    assert (
        cache_root
        / "futures"
        / "um"
        / "minute"
        / "symbol=ETHUSDT"
        / "year=2026"
        / "month=01"
        / "day=15"
        / "hour=10"
        / "part.parquet"
    ).exists()

    class GuardProvider:
        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("flattened cache lake should satisfy the second request")

        def close(self) -> None:
            return None

    cached_service = LiveDataApiService(
        repository=MinuteLakeRepository(main_root),
        provider=GuardProvider(),
        default_limit=2,
        max_limit=10,
        on_demand_max_minutes=60,
        canonical_cache_repository=MinuteLakeRepository(cache_root),
        canonical_cache_state_store=cache_state_store,
    )
    cached_payload = cached_service.fetch_perpetual_data(
        coin="eth",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )
    assert cached_payload["source"] == "cache"
    assert cached_payload["data"]["1m"][0]["timestamp"] == "2026-01-15T10:00:00.000Z"


def test_service_uses_shared_minute_lake_live_store_for_btc_without_ws_warmup(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = []
    for minute in range(2):
        timestamp = start + timedelta(minutes=minute)
        rows.append(
            {
                "timestamp": timestamp,
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume_btc": 1.0,
                "volume_usdt": 1000.0,
                "trade_count": 10,
                "taker_buy_vol_btc": 0.5,
                "taker_buy_vol_usdt": 500.0,
                "mark_price_open": 100.0 + minute,
                "mark_price_close": 100.0 + minute,
                "index_price_open": 100.0 + minute,
                "index_price_close": 100.0 + minute,
            }
        )
    _write_partition(tmp_path, symbol, start.replace(minute=0), _canonical_frame(rows))

    store = LiveEventStore(tmp_path / "live_events.sqlite")
    writer_collector = InMemoryLiveCollector(event_store=store, symbol=symbol)
    live_minute_ms = int((start + timedelta(minutes=1)).timestamp() * 1000)
    writer_collector.set_depth_snapshot(
        symbol=symbol,
        last_update_id=100,
        bids=[(100.0, 25_000.0)],
        asks=[(101.0, 25_000.0)],
        minute_timestamp_ms=live_minute_ms,
    )
    writer_collector.ingest_depth_diff(
        symbol=symbol,
        event_time=live_minute_ms + 1_000,
        transact_time=live_minute_ms + 950,
        first_update_id=101,
        final_update_id=105,
        bid_deltas=[(100.0, 12.0)],
        ask_deltas=[(101.0, 13.0)],
        previous_final_update_id=100,
        arrival_time=live_minute_ms + 1_025,
    )
    writer_collector.ingest_liquidation_event(
        LiquidationOrderEvent(
            symbol=symbol,
            event_time=live_minute_ms + 2_000,
            side="SELL",
            price=101.0,
            quantity=2.0,
            arrival_time=live_minute_ms + 2_020,
            orig_quantity=2.0,
            executed_quantity=1.5,
        ),
        raw_payload={"o": {"q": "2.0", "l": "1.5"}},
    )
    expected_live_snapshot = writer_collector.snapshot_for_minute(live_minute_ms)

    class NoopProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            raise AssertionError("provider should not be called when local coverage is complete")

        def close(self) -> None:
            return None

    class GuardWsManager:
        def touch(self, requested_symbol: str) -> object:
            raise AssertionError(f"ws_manager.touch should not be called for shared symbol {requested_symbol}")

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=NoopProvider(),
        default_limit=2,
        max_limit=10,
        on_demand_max_minutes=60,
        ws_manager=GuardWsManager(),
        shared_live_symbol=symbol,
        shared_live_collector=InMemoryLiveCollector(event_store=store, symbol=symbol),
    )

    payload = service.fetch_perpetual_data(
        coin="btc",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )

    assert payload["source"] == "local+live"
    assert payload["timeframe_metadata"]["1m"]["source_strategy"] == "local_minute_lake_preferred"
    assert payload["timeframe_metadata"]["1m"]["live_ws_overlay_used"] is True
    assert "using_live_ws_overlay" in payload["timeframe_metadata"]["1m"]["notes"]
    assert len(payload["data"]["1m"]) == 2
    shared_live_row = payload["data"]["1m"][1]
    assert shared_live_row["timestamp"] == "2026-01-15T10:01:00.000Z"
    assert shared_live_row["has_depth"] is True
    assert shared_live_row["update_id_start"] == 101
    assert shared_live_row["update_id_end"] == 105
    assert shared_live_row["price_impact_100k"] == expected_live_snapshot.price_impact_100k
    assert shared_live_row["impact_fillable"] is expected_live_snapshot.impact_fillable
    assert shared_live_row["depth_degraded"] is expected_live_snapshot.depth_degraded
    assert shared_live_row["has_liq"] is True
    assert shared_live_row["liq_long_count"] == 1
    assert shared_live_row["liq_unfilled_supported"] is True
    assert shared_live_row["liq_unfilled_ratio"] == 0.25


def test_service_defaults_to_recent_local_watermark_when_end_time_is_omitted(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = []
    for minute in range(5):
        timestamp = start + timedelta(minutes=minute)
        rows.append(
            {
                "timestamp": timestamp,
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume_btc": 1.0,
                "volume_usdt": 1000.0,
                "trade_count": 10,
                "taker_buy_vol_btc": 0.5,
                "taker_buy_vol_usdt": 500.0,
                "mark_price_open": 100.0 + minute,
                "mark_price_close": 100.0 + minute,
                "index_price_open": 100.0 + minute,
                "index_price_close": 100.0 + minute,
            }
        )
    _write_partition(tmp_path, symbol, start.replace(minute=0), _canonical_frame(rows))

    state_store = SQLiteStateStore(tmp_path / "state.sqlite")
    state_store.initialize()
    state_store.upsert_watermark(symbol, start + timedelta(minutes=4))

    class NoopProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            raise AssertionError("provider should not be called when the recent local watermark covers the window")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=NoopProvider(),
        default_limit=5,
        max_limit=10,
        on_demand_max_minutes=60,
        state_store=state_store,
        local_watermark_tolerance_minutes=3,
    )

    with patch("live_data_api_service.service.last_completed_utc_minute", return_value=start + timedelta(minutes=5)):
        payload = service.fetch_perpetual_data(
            coin="btc",
            tfs="1m",
            limit=5,
        )

    assert payload["source"] == "local"
    assert payload["end_time"] == "2026-01-15T10:04:00Z"
    assert len(payload["data"]["1m"]) == 5
    assert payload["data"]["1m"][-1]["timestamp"] == "2026-01-15T10:04:00.000Z"


def test_service_does_not_warm_non_shared_symbol_and_reuses_existing_collector(tmp_path: Path) -> None:
    touched_symbols: list[str] = []
    provider_live_collectors: list[object | None] = []
    collector = InMemoryLiveCollector(symbol="ETHUSDT")
    collector_available = False

    class RecordingWsManager:
        def get_collector(self, requested_symbol: str) -> InMemoryLiveCollector | None:
            assert requested_symbol == "ETHUSDT"
            return collector if collector_available else None

        def touch(self, requested_symbol: str) -> InMemoryLiveCollector:
            touched_symbols.append(requested_symbol)
            return collector

    class FakeProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            live_collector: object | None = None,
        ) -> pl.DataFrame:
            assert requested_symbol == "ETHUSDT"
            provider_live_collectors.append(live_collector)
            remote_rows = []
            cursor = start_time
            while cursor <= end_time:
                remote_rows.append(
                    {
                        "timestamp": cursor,
                        "open": 2000.0,
                        "high": 2010.0,
                        "low": 1990.0,
                        "close": 2005.0,
                        "volume_btc": 1.0,
                        "volume_usdt": 2000.0,
                        "trade_count": 20,
                        "taker_buy_vol_btc": 0.5,
                        "taker_buy_vol_usdt": 1000.0,
                        "mark_price_open": 2000.0,
                        "mark_price_close": 2005.0,
                        "index_price_open": 1999.0,
                        "index_price_close": 2004.0,
                    }
                )
                cursor += timedelta(minutes=1)
            return _canonical_frame(remote_rows)

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=FakeProvider(),
        default_limit=2,
        max_limit=10,
        on_demand_max_minutes=60,
        ws_manager=RecordingWsManager(),
        shared_live_symbol="BTCUSDT",
        shared_live_collector=object(),
    )

    payload_first = service.fetch_perpetual_data(
        coin="eth",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )

    assert payload_first["source"] == "binance"
    assert touched_symbols == []
    assert provider_live_collectors == [None]
    first_row = payload_first["data"]["1m"][1]
    assert first_row["has_depth"] is False
    assert first_row["has_liq"] is False
    assert first_row["liq_long_vol_usdt"] is None

    collector_available = True
    live_minute_ms = int(datetime(2026, 1, 15, 10, 1, tzinfo=UTC).timestamp() * 1000)
    collector.set_depth_snapshot(
        symbol="ETHUSDT",
        last_update_id=100,
        bids=[(2000.0, 100.0)],
        asks=[(2001.0, 100.0)],
        minute_timestamp_ms=live_minute_ms,
    )
    collector.ingest_depth_diff(
        symbol="ETHUSDT",
        event_time=live_minute_ms + 1_000,
        transact_time=live_minute_ms + 990,
        first_update_id=101,
        final_update_id=105,
        bid_deltas=[(2000.0, 12.0)],
        ask_deltas=[(2001.0, 13.0)],
        previous_final_update_id=100,
        arrival_time=live_minute_ms + 1_020,
    )
    collector.ingest_liquidation_event(
        LiquidationOrderEvent(
            symbol="ETHUSDT",
            event_time=live_minute_ms + 2_000,
            side="BUY",
            price=2002.0,
            quantity=1.5,
            arrival_time=live_minute_ms + 2_020,
            orig_quantity=2.0,
            executed_quantity=1.5,
        )
    )

    payload_second = service.fetch_perpetual_data(
        coin="eth",
        tfs="1m",
        limit=2,
        end_time="2026-01-15T10:01:00Z",
    )

    assert payload_second["source"] == "binance"
    assert touched_symbols == ["ETHUSDT"]
    assert provider_live_collectors == [None, collector]
    second_row = payload_second["data"]["1m"][1]
    assert second_row["has_depth"] is True
    assert second_row["update_id_start"] == 101
    assert second_row["update_id_end"] == 105
    assert second_row["has_liq"] is True
    assert second_row["liq_short_count"] == 1
    assert second_row["liq_unfilled_supported"] is True
    assert second_row["liq_unfilled_ratio"] == 0.25


def test_app_does_not_prewarm_ws_symbols_when_warmup_disabled() -> None:
    touched_symbols: list[str] = []

    class RecordingWsManager:
        def touch(self, requested_symbol: str) -> object:
            touched_symbols.append(requested_symbol)
            return object()

        def stop(self) -> None:
            return None

    class FakeService:
        def __init__(self) -> None:
            self._ws_manager = RecordingWsManager()

        def close(self) -> None:
            return None

    with patch.dict("os.environ", {"BML_API_WS_SYMBOLS": "BTC,ETH"}, clear=False):
        with TestClient(create_app(FakeService())):
            pass

    assert touched_symbols == []


def test_api_endpoint_returns_requested_timeframes_from_local_store(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = []
    for minute in range(5):
        timestamp = start + timedelta(minutes=minute)
        rows.append(
            {
                "timestamp": timestamp,
                "open": 100.0 + minute,
                "high": 101.0 + minute,
                "low": 99.0 + minute,
                "close": 100.5 + minute,
                "volume_btc": 1.0,
                "volume_usdt": 1000.0,
                "trade_count": 10,
                "taker_buy_vol_btc": 0.5,
                "taker_buy_vol_usdt": 500.0,
                "mark_price_open": 100.0 + minute,
                "mark_price_close": 100.0 + minute,
                "index_price_open": 100.0 + minute,
                "index_price_close": 100.0 + minute,
            }
        )
    frame = _canonical_frame(rows)
    _write_partition(tmp_path, symbol, start.replace(minute=0), frame)

    class NoopProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            raise AssertionError("provider should not be called when local coverage is complete")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=NoopProvider(),
        default_limit=2,
        max_limit=10,
        on_demand_max_minutes=60,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/perpetual-data",
        params={
            "coin": "btc",
            "tfs": "1m,5m",
            "limit": 1,
            "end_time": "2026-01-15T10:04:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == symbol
    assert payload["timeframes"] == ["1m", "5m"]
    assert len(payload["data"]["1m"]) == 1
    assert len(payload["data"]["5m"]) == 1
    assert payload["data"]["1m"][0]["timestamp"] == "2026-01-15T10:04:00.000Z"
    assert payload["data"]["5m"][0]["timestamp"] == "2026-01-15T10:00:00.000Z"
    assert payload["response_time_secs"] >= 0.0
    assert response.headers["X-Response-Time-Secs"] == f"{payload['response_time_secs']:.6f}"


def test_api_endpoint_supports_per_timeframe_limits(tmp_path: Path) -> None:
    symbol = "ETHUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = []
    for minute in range(5):
        timestamp = start + timedelta(minutes=minute)
        rows.append(
            {
                "timestamp": timestamp,
                "open": 2000.0 + minute,
                "high": 2001.0 + minute,
                "low": 1999.0 + minute,
                "close": 2000.5 + minute,
                "volume_btc": 1.0,
                "volume_usdt": 2000.0,
                "trade_count": 20,
                "taker_buy_vol_btc": 0.5,
                "taker_buy_vol_usdt": 1000.0,
                "mark_price_open": 2000.0 + minute,
                "mark_price_close": 2000.0 + minute,
                "index_price_open": 2000.0 + minute,
                "index_price_close": 2000.0 + minute,
            }
        )
    frame = _canonical_frame(rows)
    _write_partition(tmp_path, symbol, start.replace(minute=0), frame)

    class NoopProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            raise AssertionError("provider should not be called when local coverage is complete")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=NoopProvider(),
        default_limit=2,
        max_limit=100,
        on_demand_max_minutes=60,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/perpetual-data",
        params={
            "coin": "eth",
            "tfs": "1m=4,5m=1",
            "end_time": "2026-01-15T10:04:00Z",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["symbol"] == symbol
    assert payload["timeframes"] == ["1m", "5m"]
    assert payload["limits"] == {"1m": 4, "5m": 1}
    assert len(payload["data"]["1m"]) == 4
    assert len(payload["data"]["5m"]) == 1
    assert payload["data"]["1m"][0]["timestamp"] == "2026-01-15T10:01:00.000Z"
    assert payload["data"]["1m"][-1]["timestamp"] == "2026-01-15T10:04:00.000Z"


def test_api_endpoint_fetches_native_binance_timeframes_when_available(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[tuple[str, str, int]] = []
    oi_calls: list[tuple[str, str, int]] = []
    ratio_calls: list[tuple[str, str, int]] = []

    class NativeProvider:
        @staticmethod
        def _step(value: str) -> int:
            return {"1m": 1, "3m": 3, "5m": 5, "15m": 15, "1h": 60}[value]

        @staticmethod
        def _base() -> datetime:
            return datetime(2026, 1, 15, 10, 0, tzinfo=UTC)

        def fetch_native_candles(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            calls.append((symbol, interval, limit))
            step = self._step(interval)
            base = self._base()
            return [
                {
                    "open_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "open": 100.0 + idx,
                    "high": 101.0 + idx,
                    "low": 99.0 + idx,
                    "close": 100.5 + idx,
                    "volume_btc": 10.0,
                    "close_time": int((base + timedelta(minutes=step * idx + step)).timestamp() * 1000),
                    "volume_usdt": 1000.0,
                    "trade_count": 20,
                    "taker_buy_vol_btc": 6.0,
                    "taker_buy_vol_usdt": 600.0,
                }
                for idx in range(limit)
            ]

        def fetch_native_open_interest_hist(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            period: str,
            limit: int,
        ) -> list[dict[str, object]]:
            oi_calls.append((symbol, period, limit))
            base = self._base()
            step = self._step(period)
            return [
                {
                    "symbol": symbol,
                    "oi_contracts": 1000.0 + idx,
                    "oi_value_usdt": 2000.0 + idx,
                    "create_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                }
                for idx in range(limit)
            ]

        def fetch_native_mark_price_klines(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            step = self._step(interval)
            base = self._base()
            return [
                {
                    "open_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "mark_price_open": 99.0 + idx,
                    "mark_price_close": 100.0 + idx,
                }
                for idx in range(limit)
            ]

        def fetch_native_index_price_klines(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            step = self._step(interval)
            base = self._base()
            return [
                {
                    "open_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "index_price_open": 98.0 + idx,
                    "index_price_close": 99.0 + idx,
                }
                for idx in range(limit)
            ]

        def fetch_native_premium_index_klines(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            step = self._step(interval)
            base = self._base()
            return [
                {
                    "open_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "premium_index_close": 0.001 + (0.001 * idx),
                }
                for idx in range(limit)
            ]

        def fetch_native_global_long_short_account_ratio(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            period: str,
            limit: int,
        ) -> list[dict[str, object]]:
            step = self._step(period)
            base = self._base()
            ratio_calls.append(("global", period, limit))
            return [
                {
                    "symbol": symbol,
                    "data_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "ratio": 1.0 + (0.1 * idx),
                }
                for idx in range(limit)
            ]

        def fetch_native_top_trader_long_short_account_ratio(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            period: str,
            limit: int,
        ) -> list[dict[str, object]]:
            step = self._step(period)
            base = self._base()
            ratio_calls.append(("top_account", period, limit))
            return [
                {
                    "symbol": symbol,
                    "data_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "ratio": 1.2 + (0.2 * idx),
                }
                for idx in range(limit)
            ]

        def fetch_native_top_trader_long_short_position_ratio(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            period: str,
            limit: int,
        ) -> list[dict[str, object]]:
            step = self._step(period)
            base = self._base()
            ratio_calls.append(("top_position", period, limit))
            return [
                {
                    "symbol": symbol,
                    "data_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "long_account": 0.60 + (0.05 * idx),
                    "short_account": 0.40 - (0.05 * idx),
                }
                for idx in range(limit)
            ]

        def fetch_native_funding_rate(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            limit: int = 1000,
        ) -> list[dict[str, object]]:
            base = self._base()
            return [
                {
                    "symbol": symbol,
                    "funding_time": int((base + timedelta(minutes=5)).timestamp() * 1000),
                    "funding_rate": 0.001,
                },
                {
                    "symbol": symbol,
                    "funding_time": int((base + timedelta(minutes=10)).timestamp() * 1000),
                    "funding_rate": 0.002,
                },
            ]

        def build_canonical_minutes(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            raise AssertionError("native candle requests should not build a 1m canonical frame")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=NativeProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    client = TestClient(create_app(service))
    caplog.set_level(logging.INFO, logger="live_data_api_service.service")

    response = client.get(
        "/api/v1/perpetual-data",
        params={
            "coin": "KITE",
            "tfs": "1m=2,3m=2,5m=2,15m=2,1hr=2",
            "end_time": "2026-01-15T11:59:00Z",
        },
    )

    assert response.status_code == 200
    weight_records = [
        record
        for record in caplog.records
        if record.message.startswith("Perpetual data Binance candle REST weight estimate:")
    ]
    assert weight_records
    weight_record = weight_records[-1]
    assert weight_record.parsed_timeframe_limits == {"1m": 2, "3m": 2, "5m": 2, "15m": 2, "1hr": 2}
    assert weight_record.binance_candle_weight_per_timeframe_total == {
        "1m": 4,
        "3m": 4,
        "5m": 4,
        "15m": 4,
        "1hr": 4,
    }
    assert weight_record.binance_candle_endpoint_family_subtotals == {
        "/fapi/v1/klines": 5,
        "/fapi/v1/markPriceKlines": 5,
        "/fapi/v1/indexPriceKlines": 5,
        "/fapi/v1/premiumIndexKlines": 5,
    }
    assert weight_record.binance_candle_endpoint_family_count == 4
    assert weight_record.binance_candle_grand_total_estimated_weight == 20
    assert weight_record.binance_futures_request_weight_limit_1m == 2400
    assert weight_record.binance_candle_estimated_remaining_weight_1m == 2380
    assert weight_record.binance_candle_estimated_weight_pct_1m == 0.833333
    payload = response.json()
    assert sorted(calls) == sorted(
        [
        ("KITEUSDT", "1m", 2),
        ("KITEUSDT", "3m", 2),
        ("KITEUSDT", "5m", 2),
        ("KITEUSDT", "15m", 2),
        ("KITEUSDT", "1h", 2),
        ]
    )
    assert sorted(oi_calls) == sorted(
        [
            ("KITEUSDT", "5m", 7),
            ("KITEUSDT", "15m", 7),
            ("KITEUSDT", "1h", 7),
        ]
    )
    assert ("global", "5m", 7) in ratio_calls
    assert ("top_account", "15m", 7) in ratio_calls
    assert ("top_position", "1h", 7) in ratio_calls
    assert payload["source"] == "binance_native"
    assert payload["timeframe_metadata"]["3m"]["fetch_mode"] == "direct_tf"
    assert payload["timeframe_metadata"]["3m"]["fallback_used"] is False
    assert "open_interest_hist_not_supported_for_3m" in payload["timeframe_metadata"]["3m"]["notes"]
    assert "oi_hist_not_supported_for_3m" not in payload["timeframe_metadata"]["3m"]["notes"]
    assert "ls_ratio_not_supported_for_3m" in payload["timeframe_metadata"]["3m"]["notes"]
    five_minute_notes = payload["timeframe_metadata"]["5m"]["notes"]
    assert "funding_rate_aligned_asof_backward" in five_minute_notes
    assert "funding_rate_event_series_aligned_not_native_tf" in five_minute_notes
    assert "next_funding_time_current_snapshot_only" in five_minute_notes
    assert "predicted_funding_live_ws_only" in five_minute_notes
    assert "native_alignment_used" not in " ".join(five_minute_notes)
    assert len(five_minute_notes) == len(set(five_minute_notes))
    assert "historical_depth_not_available_from_binance_rest" in payload["timeframe_metadata"]["15m"]["notes"]
    assert payload["data"]["5m"][0]["delta_oi_contracts"] is None
    assert payload["data"]["5m"][0]["has_ls_ratio"] is True
    assert payload["data"]["5m"][-1]["oi_contracts"] == 1001.0
    assert payload["data"]["5m"][-1]["delta_oi_contracts"] == 1.0
    assert payload["data"]["5m"][-1]["delta_oi_value_usdt"] == 1.0
    assert "oi_" + "delta_contracts" not in payload["data"]["5m"][-1]
    assert payload["data"]["5m"][-1]["funding_rate"] == 0.002
    assert payload["data"]["5m"][-1]["delta_funding_rate"] == 0.001
    assert payload["data"]["5m"][-1]["next_funding_time"] is None
    assert payload["data"]["5m"][-1]["predicted_funding"] is None
    assert payload["data"]["5m"][-1]["global_ls_ratio_acct"] == 1.1
    assert payload["data"]["5m"][-1]["top_trader_ls_ratio_acct"] == 1.4
    assert payload["data"]["5m"][-1]["top_trader_long_pct"] == 0.65
    assert payload["data"]["5m"][-1]["top_trader_short_pct"] == pytest.approx(0.35)
    assert payload["data"]["5m"][-1]["ls_ratio_divergence"] == pytest.approx(0.3)
    assert payload["data"]["5m"][-1]["net_long"] == pytest.approx(0.3)
    assert payload["data"]["5m"][-1]["net_short"] == pytest.approx(-0.3)
    assert payload["data"]["5m"][-1]["delta_net_long"] == pytest.approx(0.1)
    assert payload["data"]["5m"][-1]["delta_net_short"] == pytest.approx(-0.1)
    assert payload["data"]["5m"][-1]["has_ls_ratio"] is True
    assert payload["data"]["5m"][-1]["mark_price_close"] == 101.0
    assert payload["data"]["5m"][-1]["index_price_close"] == 100.0
    assert payload["data"]["5m"][-1]["premium_index"] == 0.002
    assert payload["data"]["5m"][-1]["vwap_bar"] == 100.0
    assert payload["data"]["5m"][-1]["realized_vol_bar"] == pytest.approx(abs(log(101.5 / 100.5)))
    assert "vwap_1m" not in payload["data"]["5m"][-1]
    assert "realized_vol_1m" not in payload["data"]["5m"][-1]
    assert payload["data"]["5m"][-1]["taker_sell_vol_btc"] == 4.0
    assert payload["data"]["5m"][-1]["taker_sell_vol_usdt"] == 400.0
    assert "_aux_ts" not in payload["data"]["5m"][-1]
    assert payload["data"]["1m"][-1]["oi_contracts"] is None
    assert payload["data"]["1m"][-1]["has_ls_ratio"] is False
    assert payload["data"]["1m"][-1]["net_long"] is None
    assert payload["data"]["1m"][-1]["delta_oi_contracts"] is None
    assert payload["data"]["1m"][-1]["delta_net_long"] is None
    assert payload["data"]["3m"][-1]["oi_contracts"] is None
    assert payload["data"]["3m"][-1]["top_trader_ls_ratio_acct"] is None
    assert payload["data"]["3m"][-1]["has_ls_ratio"] is False
    assert payload["data"]["3m"][-1]["net_long"] is None
    assert payload["data"]["3m"][-1]["delta_oi_contracts"] is None
    assert payload["data"]["3m"][-1]["delta_net_long"] is None
    assert len(payload["data"]["1hr"]) == 2


def test_api_endpoint_reuses_cached_native_candle_response_for_same_request(tmp_path: Path) -> None:
    native_calls: list[tuple[str, str, int]] = []

    class CountingNativeProvider:
        def fetch_native_candles(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            native_calls.append((symbol, interval, limit))
            base = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
            return [
                {
                    "open_time": int((base + timedelta(minutes=5 * idx)).timestamp() * 1000),
                    "open": 100.0 + idx,
                    "high": 101.0 + idx,
                    "low": 99.0 + idx,
                    "close": 100.5 + idx,
                    "volume_btc": 10.0,
                    "close_time": int((base + timedelta(minutes=5 * idx + 5)).timestamp() * 1000),
                    "volume_usdt": 1000.0,
                    "trade_count": 20,
                    "taker_buy_vol_btc": 6.0,
                    "taker_buy_vol_usdt": 600.0,
                }
                for idx in range(limit)
            ]

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=CountingNativeProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    client = TestClient(create_app(service))

    params = {"coin": "ETH", "tfs": "5m=2", "end_time": "2026-01-15T10:09:00Z"}
    first = client.get("/api/v1/perpetual-data", params=params)
    second = client.get("/api/v1/perpetual-data", params=params)

    assert first.status_code == 200
    assert second.status_code == 200
    assert native_calls == [("ETHUSDT", "5m", 2)]
    assert first.json()["data"]["5m"] == second.json()["data"]["5m"]
    assert first.json()["timeframe_metadata"]["5m"]["fetch_mode"] == "direct_tf"
    assert first.json()["timeframe_metadata"]["5m"]["served_from_cache"] is False
    assert second.json()["timeframe_metadata"]["5m"]["fetch_mode"] == "direct_tf"
    assert second.json()["timeframe_metadata"]["5m"]["served_from_cache"] is True
    assert second.json()["timeframe_metadata"]["5m"]["cache_hit_type"] == "exact"


def test_service_deduplicates_concurrent_native_candle_request(tmp_path: Path) -> None:
    native_calls: list[tuple[str, str, int]] = []
    fetch_started = Event()
    release_fetch = Event()

    class BlockingNativeProvider:
        def fetch_native_candles(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            native_calls.append((symbol, interval, limit))
            fetch_started.set()
            assert release_fetch.wait(timeout=2.0)
            base = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
            return [
                {
                    "open_time": int((base + timedelta(minutes=5 * idx)).timestamp() * 1000),
                    "open": 100.0 + idx,
                    "high": 101.0 + idx,
                    "low": 99.0 + idx,
                    "close": 100.5 + idx,
                    "volume_btc": 10.0,
                    "close_time": int((base + timedelta(minutes=5 * idx + 5)).timestamp() * 1000),
                    "volume_usdt": 1000.0,
                    "trade_count": 20,
                    "taker_buy_vol_btc": 6.0,
                    "taker_buy_vol_usdt": 600.0,
                }
                for idx in range(limit)
            ]

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=BlockingNativeProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )

    spec = parse_timeframe_requests("5m")[0].spec
    end_time = datetime(2026, 1, 15, 10, 9, tzinfo=UTC)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            service.load_candle_bars,
            coin="ETH",
            spec=spec,
            limit=2,
            end_time=end_time,
        )
        assert fetch_started.wait(timeout=2.0)
        second = executor.submit(
            service.load_candle_bars,
            coin="ETH",
            spec=spec,
            limit=2,
            end_time=end_time,
        )
        time.sleep(0.05)
        release_fetch.set()
        results = [first.result(timeout=2.0), second.result(timeout=2.0)]

    cache_metadata = [result.metadata for result in results]
    assert native_calls == [("ETHUSDT", "5m", 2)]
    assert any(metadata["served_from_cache"] is False for metadata in cache_metadata)
    assert any(metadata.get("cache_hit_type") == "inflight" for metadata in cache_metadata)


def test_service_extends_cached_native_candle_window_with_only_missing_bars(tmp_path: Path) -> None:
    native_calls: list[tuple[str, str, int, datetime]] = []

    class CountingNativeProvider:
        def fetch_native_candles(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            native_calls.append((symbol, interval, limit, end_time))
            step = 5
            base = datetime(2026, 1, 15, 9, 40, tzinfo=UTC)
            bars = []
            cursor = base
            while cursor <= end_time and len(bars) < 32:
                bars.append(
                    {
                        "open_time": int(cursor.timestamp() * 1000),
                        "open": 100.0 + len(bars),
                        "high": 101.0 + len(bars),
                        "low": 99.0 + len(bars),
                        "close": 100.5 + len(bars),
                        "volume_btc": 10.0,
                        "close_time": int((cursor + timedelta(minutes=step)).timestamp() * 1000),
                        "volume_usdt": 1000.0,
                        "trade_count": 20,
                        "taker_buy_vol_btc": 6.0,
                        "taker_buy_vol_usdt": 600.0,
                    }
                )
                cursor += timedelta(minutes=step)
            return bars[-limit:]

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=CountingNativeProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )

    spec = parse_timeframe_requests("5m")[0].spec
    first = service.load_candle_bars(
        coin="ETH",
        spec=spec,
        limit=2,
        end_time=datetime(2026, 1, 15, 10, 9, tzinfo=UTC),
    )
    second = service.load_candle_bars(
        coin="ETH",
        spec=spec,
        limit=5,
        end_time=datetime(2026, 1, 15, 10, 9, tzinfo=UTC),
    )

    assert first.frame.height == 2
    assert second.frame.height == 5
    assert first.metadata["served_from_cache"] is False
    assert second.metadata["served_from_cache"] is True
    assert second.metadata["cache_hit_type"] == "partial_extended"
    assert [call[:3] for call in native_calls] == [
        ("ETHUSDT", "5m", 2),
        ("ETHUSDT", "5m", 3),
    ]


def test_service_deduplicates_concurrent_canonical_binance_patch(tmp_path: Path) -> None:
    build_calls: list[tuple[str, datetime, datetime]] = []
    build_started = Event()
    release_build = Event()

    class BlockingCanonicalProvider:
        def build_canonical_minutes(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            build_calls.append((symbol, start_time, end_time))
            build_started.set()
            assert release_build.wait(timeout=2.0)
            return _canonical_frame(
                [
                    {
                        "timestamp": start_time,
                        "open": 100.0,
                        "high": 101.0,
                        "low": 99.0,
                        "close": 100.5,
                        "volume_btc": 10.0,
                        "volume_usdt": 1000.0,
                        "trade_count": 20,
                    }
                ]
            )

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=BlockingCanonicalProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    start_time = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    end_time = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(
            service.load_canonical_window,
            coin="SAPIEN",
            start_time=start_time,
            end_time=end_time,
        )
        assert build_started.wait(timeout=2.0)
        second = executor.submit(
            service.load_canonical_window,
            coin="SAPIEN",
            start_time=start_time,
            end_time=end_time,
        )
        time.sleep(0.05)
        release_build.set()
        results = [first.result(timeout=2.0), second.result(timeout=2.0)]

    assert len(build_calls) == 1
    assert [result.frame.height for result in results] == [1, 1]


def test_service_deduplicates_concurrent_premium_index_snapshots(tmp_path: Path) -> None:
    snapshot_calls: list[str] = []
    snapshot_started = Event()
    release_snapshot = Event()

    class SnapshotProvider:
        def fetch_native_candles(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            *,
            interval: str,
            limit: int,
        ) -> list[dict[str, object]]:
            step = 60 if interval == "1h" else int(interval.removesuffix("m"))
            base = end_time - timedelta(minutes=step * (limit - 1))
            return [
                {
                    "open_time": int((base + timedelta(minutes=step * idx)).timestamp() * 1000),
                    "open": 100.0 + idx,
                    "high": 101.0 + idx,
                    "low": 99.0 + idx,
                    "close": 100.5 + idx,
                    "volume_btc": 10.0,
                    "close_time": int((base + timedelta(minutes=step * idx + step)).timestamp() * 1000),
                    "volume_usdt": 1000.0,
                    "trade_count": 20,
                    "taker_buy_vol_btc": 6.0,
                    "taker_buy_vol_usdt": 600.0,
                }
                for idx in range(limit)
            ]

        def fetch_native_premium_index_snapshot(self, symbol: str) -> dict[str, object]:
            snapshot_calls.append(symbol)
            snapshot_started.set()
            assert release_snapshot.wait(timeout=2.0)
            event_time = int(datetime(2026, 1, 15, 10, 14, tzinfo=UTC).timestamp() * 1000)
            return {
                "mark_price": 101.0,
                "index_price": 100.0,
                "last_funding_rate": 0.0001,
                "next_funding_time": event_time + 8 * 60 * 60 * 1000,
                "predicted_funding": 0.0002,
                "event_time": event_time,
            }

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=SnapshotProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            service.fetch_perpetual_data,
            coin="RLC",
            tfs="5m=2,15m=2",
            end_time="2026-01-15T10:14:00Z",
        )
        assert snapshot_started.wait(timeout=2.0)
        time.sleep(0.05)
        release_snapshot.set()
        payload = future.result(timeout=2.0)

    assert snapshot_calls == ["RLCUSDT"]
    assert payload["timeframes"] == ["5m", "15m"]


def test_btc_prefers_local_minute_lake_when_coverage_exists(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = [
        {
            "timestamp": start + timedelta(minutes=minute),
            "open": 100.0 + minute,
            "high": 101.0 + minute,
            "low": 99.0 + minute,
            "close": 100.5 + minute,
            "volume_btc": 2.0,
            "volume_usdt": 200.0,
            "trade_count": 2,
            "taker_buy_vol_btc": 1.2,
            "taker_buy_vol_usdt": 120.0,
            "has_depth": minute == 9,
            "update_id_start": 10 if minute == 9 else None,
            "update_id_end": 20 if minute == 9 else None,
        }
        for minute in range(10)
    ]
    _write_partition(tmp_path, symbol, start, _canonical_frame(rows))

    class GuardProvider:
        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("BTC should use the local minute lake when coverage exists")

        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("local coverage should avoid Binance patching")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=GuardProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/perpetual-data",
        params={"coin": "BTC", "tfs": "5m=1", "end_time": "2026-01-15T10:09:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    metadata = payload["timeframe_metadata"]["5m"]
    assert payload["source"] == "local"
    assert metadata["source_strategy"] == "local_minute_lake_preferred"
    assert metadata["local_minute_lake_used"] is True
    assert metadata["live_ws_overlay_used"] is False
    assert metadata["fallback_used"] is False
    assert "using_local_btc_minute_lake" in metadata["notes"]
    assert payload["data"]["5m"][0]["has_depth"] is True
    assert payload["data"]["5m"][0]["update_id_start"] == 10


def test_btc_local_only_config_does_not_fall_back_to_binance_when_local_missing(tmp_path: Path) -> None:
    class GuardProvider:
        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("BTC should not fall back to Binance candles")

        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("BTC should not patch missing local minutes from Binance")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=GuardProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
        btc_allow_binance_patch=False,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/perpetual-data",
        params={"coin": "BTC", "tfs": "5m=1", "end_time": "2026-01-15T10:09:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    metadata = payload["timeframe_metadata"]["5m"]
    assert payload["source"] == "local_unavailable"
    assert metadata["fetch_mode"] == "local_only_unavailable"
    assert "local_btc_missing_required_window" in metadata["notes"]
    assert "btc_local_only_no_binance_fallback" in metadata["notes"]
    assert payload["data"]["5m"] == []


def test_btc_missing_local_window_patches_from_binance_and_persists(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    remote_rows = [
        {
            "timestamp": start - timedelta(minutes=1) + timedelta(minutes=minute),
            "open": 100.0 + minute,
            "high": 101.0 + minute,
            "low": 99.0 + minute,
            "close": 100.5 + minute,
            "volume_btc": 1.0,
            "volume_usdt": 100.0,
            "trade_count": 1,
            "taker_buy_vol_btc": 0.5,
            "taker_buy_vol_usdt": 50.0,
            "mark_price_open": 100.0 + minute,
            "mark_price_close": 100.0 + minute,
            "index_price_open": 100.0 + minute,
            "index_price_close": 100.0 + minute,
        }
        for minute in range(3)
    ]
    provider_calls: list[tuple[str, datetime, datetime]] = []

    class PatchProvider:
        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            provider_calls.append((requested_symbol, start_time, end_time))
            return _canonical_frame(remote_rows)

        def close(self) -> None:
            return None

    state_store = SQLiteStateStore(tmp_path / "state.sqlite")
    state_store.initialize()
    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=PatchProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
        state_store=state_store,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/perpetual-data",
        params={"coin": "BTC", "tfs": "1m=2", "end_time": "2026-01-15T10:01:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    metadata = payload["timeframe_metadata"]["1m"]
    assert provider_calls == [(symbol, start - timedelta(minutes=1), start + timedelta(minutes=1))]
    assert payload["source"] == "binance"
    assert metadata["fallback_used"] is True
    assert "local_btc_coverage_incomplete_patched_from_binance" in metadata["notes"]

    class GuardProvider:
        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("persisted BTC patch should be read from the local minute lake")

        def close(self) -> None:
            return None

    persisted_service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=GuardProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
    )
    persisted_payload = persisted_service.fetch_perpetual_data(
        coin="BTC",
        tfs="1m=2",
        end_time="2026-01-15T10:01:00Z",
    )

    assert persisted_payload["source"] == "local"
    assert len(persisted_payload["data"]["1m"]) == 2
    assert persisted_payload["data"]["1m"][0]["timestamp"] == "2026-01-15T10:00:00.000Z"


def test_btc_higher_timeframe_at_complexity_threshold_stays_local(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 0, 0, tzinfo=UTC)
    rows = [
        {
            "timestamp": start + timedelta(minutes=minute),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume_btc": 1.0,
            "volume_usdt": 100.0,
            "trade_count": 1,
            "taker_buy_vol_btc": 0.5,
            "taker_buy_vol_usdt": 50.0,
        }
        for minute in range(10)
    ]
    _write_partition(tmp_path, symbol, start, _canonical_frame(rows))

    class GuardProvider:
        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("threshold-sized BTC higher TF should still use local path")

        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("local coverage should avoid Binance patching")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=GuardProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
        btc_local_max_higher_tf_bars=1,
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/perpetual-data",
        params={"coin": "BTC", "tfs": "5m=1", "end_time": "2026-01-15T00:09:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    metadata = payload["timeframe_metadata"]["5m"]
    assert payload["source"] == "local"
    assert metadata["source_strategy"] == "local_minute_lake_preferred"
    assert "btc_local_path_skipped_due_to_request_complexity" not in metadata["notes"]


def test_btc_heavy_higher_timeframe_reads_local_higher_timeframe_lake(tmp_path: Path) -> None:
    higher_tf_root = tmp_path / "futures" / "um" / "higher_timeframes"
    higher_tf_rows = pl.DataFrame(
        {
            "timeframe": ["5m", "5m"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "timestamp": [
                datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 15, 0, 5, tzinfo=UTC),
            ],
            "bucket_start": [
                datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 15, 0, 5, tzinfo=UTC),
            ],
            "bucket_end": [
                datetime(2026, 1, 15, 0, 5, tzinfo=UTC),
                datetime(2026, 1, 15, 0, 10, tzinfo=UTC),
            ],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume_btc": [10.0, 11.0],
            "volume_usdt": [1000.0, 1100.0],
            "trade_count": [20, 21],
            "vwap": [100.2, 101.2],
            "avg_trade_size_btc": [0.5, 0.52],
            "max_trade_size_btc": [1.0, 1.1],
            "taker_buy_vol_btc": [6.0, 6.5],
            "taker_buy_vol_usdt": [600.0, 650.0],
            "net_taker_vol_btc": [1.0, 1.1],
            "count_buy_trades": [12, 13],
            "count_sell_trades": [8, 8],
            "taker_buy_ratio": [0.6, 0.59],
            "vol_buy_whale_btc": [0.5, 0.6],
            "vol_sell_whale_btc": [0.4, 0.5],
            "vol_buy_retail_btc": [0.3, 0.4],
            "vol_sell_retail_btc": [0.2, 0.3],
            "whale_trade_count": [2, 3],
            "liq_long_vol_usdt": [0.0, 0.0],
            "liq_short_vol_usdt": [0.0, 0.0],
            "liq_long_count": [0, 0],
            "liq_short_count": [0, 0],
            "liq_avg_fill_price": [None, None],
            "liq_unfilled_ratio": [None, None],
            "liq_unfilled_supported": [False, False],
            "has_liq": [False, False],
            "oi_contracts": [1000.0, 1001.0],
            "oi_value_usdt": [1_000_000.0, 1_001_000.0],
            "top_trader_ls_ratio_acct": [1.1, 1.2],
            "global_ls_ratio_acct": [0.9, 0.95],
            "ls_ratio_divergence": [0.2, 0.25],
            "top_trader_long_pct": [0.55, 0.56],
            "top_trader_short_pct": [0.45, 0.44],
            "premium_index": [0.0001, 0.0002],
            "funding_rate": [0.0003, 0.0004],
            "predicted_funding": [0.0005, 0.0006],
            "next_funding_time": [1, 2],
            "micro_price_close": [100.4, 101.4],
            "mark_price_open": [100.1, 101.1],
            "mark_price_close": [100.2, 101.2],
            "index_price_open": [100.0, 101.0],
            "index_price_close": [100.0, 101.0],
            "avg_spread_usdt": [1.0, 1.1],
            "bid_ask_imbalance": [0.1, 0.2],
            "avg_bid_depth": [10.0, 11.0],
            "avg_ask_depth": [12.0, 13.0],
            "spread_pct": [0.001, 0.0011],
            "price_impact_100k": [0.002, 0.0021],
            "has_depth": [True, True],
            "impact_fillable": [True, True],
            "depth_degraded": [False, False],
            "has_ws_latency": [False, False],
            "ws_latency_bad": [False, False],
            "has_ls_ratio": [True, True],
            "realized_vol_htf": [0.01, 0.02],
            "event_time": [1, 2],
            "transact_time": [1, 2],
            "arrival_time": [1, 2],
            "update_id_start": [10, 20],
            "update_id_end": [19, 29],
            "expected_minutes_in_bucket": [5, 5],
            "observed_minutes_in_bucket": [5, 5],
            "missing_minutes_count": [0, 0],
            "bucket_complete": [True, True],
        }
    )
    _write_higher_timeframe_partition(
        higher_tf_root,
        "5m",
        "BTCUSDT",
        datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
        higher_tf_rows,
    )

    class GuardProvider:
        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("BTC should not use Binance candles for higher timeframes")

        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("BTC higher timeframe request should not patch from Binance")

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
        "/api/v1/perpetual-data",
        params={"coin": "BTC", "tfs": "5m=2", "end_time": "2026-01-15T00:09:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    metadata = payload["timeframe_metadata"]["5m"]
    assert payload["source"] == "local"
    assert metadata["fetch_mode"] == "direct_local_higher_tf"
    assert metadata["source_strategy"] == "local_higher_timeframe_lake"
    assert metadata["local_higher_timeframe_lake_used"] is True
    assert "using_local_btc_higher_timeframe_lake" in metadata["notes"]


def test_btc_mixed_complexity_plan_keeps_1m_local_and_reads_higher_tf_lake(tmp_path: Path) -> None:
    symbol = "BTCUSDT"
    start = datetime(2026, 1, 15, 0, 0, tzinfo=UTC)
    _write_partition(
        tmp_path,
        symbol,
        start,
        _canonical_frame(
            [
                {
                    "timestamp": start + timedelta(minutes=minute),
                    "open": 100.0 + minute,
                    "high": 101.0 + minute,
                    "low": 99.0 + minute,
                    "close": 100.5 + minute,
                    "volume_btc": 1.0,
                    "volume_usdt": 100.0,
                    "trade_count": 1,
                    "taker_buy_vol_btc": 0.5,
                    "taker_buy_vol_usdt": 50.0,
                }
                for minute in range(10)
            ]
        ),
    )
    higher_tf_root = tmp_path / "futures" / "um" / "higher_timeframes"
    higher_tf_rows = pl.DataFrame(
        {
            "timeframe": ["5m", "5m"],
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "timestamp": [
                datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 15, 0, 5, tzinfo=UTC),
            ],
            "bucket_start": [
                datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 15, 0, 5, tzinfo=UTC),
            ],
            "bucket_end": [
                datetime(2026, 1, 15, 0, 5, tzinfo=UTC),
                datetime(2026, 1, 15, 0, 10, tzinfo=UTC),
            ],
            "open": [200.0, 201.0],
            "high": [201.0, 202.0],
            "low": [199.0, 200.0],
            "close": [200.5, 201.5],
            "volume_btc": [10.0, 11.0],
            "volume_usdt": [1000.0, 1100.0],
            "trade_count": [20, 21],
            "vwap": [200.2, 201.2],
            "avg_trade_size_btc": [0.5, 0.52],
            "max_trade_size_btc": [1.0, 1.1],
            "taker_buy_vol_btc": [6.0, 6.5],
            "taker_buy_vol_usdt": [600.0, 650.0],
            "net_taker_vol_btc": [1.0, 1.1],
            "count_buy_trades": [12, 13],
            "count_sell_trades": [8, 8],
            "taker_buy_ratio": [0.6, 0.59],
            "vol_buy_whale_btc": [0.5, 0.6],
            "vol_sell_whale_btc": [0.4, 0.5],
            "vol_buy_retail_btc": [0.3, 0.4],
            "vol_sell_retail_btc": [0.2, 0.3],
            "whale_trade_count": [2, 3],
            "liq_long_vol_usdt": [0.0, 0.0],
            "liq_short_vol_usdt": [0.0, 0.0],
            "liq_long_count": [0, 0],
            "liq_short_count": [0, 0],
            "liq_avg_fill_price": [None, None],
            "liq_unfilled_ratio": [None, None],
            "liq_unfilled_supported": [False, False],
            "has_liq": [False, False],
            "oi_contracts": [1000.0, 1001.0],
            "oi_value_usdt": [1_000_000.0, 1_001_000.0],
            "top_trader_ls_ratio_acct": [1.1, 1.2],
            "global_ls_ratio_acct": [0.9, 0.95],
            "ls_ratio_divergence": [0.2, 0.25],
            "top_trader_long_pct": [0.55, 0.56],
            "top_trader_short_pct": [0.45, 0.44],
            "premium_index": [0.0001, 0.0002],
            "funding_rate": [0.0003, 0.0004],
            "predicted_funding": [0.0005, 0.0006],
            "next_funding_time": [1, 2],
            "micro_price_close": [200.4, 201.4],
            "mark_price_open": [200.1, 201.1],
            "mark_price_close": [200.2, 201.2],
            "index_price_open": [200.0, 201.0],
            "index_price_close": [200.0, 201.0],
            "avg_spread_usdt": [1.0, 1.1],
            "bid_ask_imbalance": [0.1, 0.2],
            "avg_bid_depth": [10.0, 11.0],
            "avg_ask_depth": [12.0, 13.0],
            "spread_pct": [0.001, 0.0011],
            "price_impact_100k": [0.002, 0.0021],
            "has_depth": [True, True],
            "impact_fillable": [True, True],
            "depth_degraded": [False, False],
            "has_ws_latency": [False, False],
            "ws_latency_bad": [False, False],
            "has_ls_ratio": [True, True],
            "realized_vol_htf": [0.01, 0.02],
            "event_time": [1, 2],
            "transact_time": [1, 2],
            "arrival_time": [1, 2],
            "update_id_start": [10, 20],
            "update_id_end": [19, 29],
            "expected_minutes_in_bucket": [5, 5],
            "observed_minutes_in_bucket": [5, 5],
            "missing_minutes_count": [0, 0],
            "bucket_complete": [True, True],
        }
    )
    _write_higher_timeframe_partition(
        higher_tf_root,
        "5m",
        "BTCUSDT",
        datetime(2026, 1, 15, 0, 0, tzinfo=UTC),
        higher_tf_rows,
    )

    class GuardProvider:
        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("BTC higher timeframe should use the local higher timeframe lake")

        def build_canonical_minutes(self, *args: object, **kwargs: object) -> pl.DataFrame:
            raise AssertionError("mixed split should not patch through Binance")

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
        "/api/v1/perpetual-data",
        params={"coin": "BTC", "tfs": "1m=2,5m=2", "end_time": "2026-01-15T00:09:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    one_minute_metadata = payload["timeframe_metadata"]["1m"]
    five_minute_metadata = payload["timeframe_metadata"]["5m"]
    assert payload["source"] == "local"
    assert one_minute_metadata["source_strategy"] == "local_minute_lake_preferred"
    assert one_minute_metadata["local_minute_lake_used"] is True
    assert "btc_local_path_selected" in one_minute_metadata["notes"]
    assert five_minute_metadata["source"] == "local"
    assert five_minute_metadata["source_strategy"] == "local_higher_timeframe_lake"
    assert "using_local_btc_higher_timeframe_lake" in five_minute_metadata["notes"]


def test_api_endpoint_uses_legacy_aggregation_when_feature_disabled(tmp_path: Path) -> None:
    symbol = "ETHUSDT"
    start = datetime(2026, 1, 15, 10, 0, tzinfo=UTC)
    rows = [
        {
            "timestamp": start + timedelta(minutes=minute),
            "open": 100.0 + minute,
            "high": 101.0 + minute,
            "low": 99.0 + minute,
            "close": 100.5 + minute,
            "volume_btc": 1.0,
            "volume_usdt": 100.0,
            "trade_count": 1,
            "taker_buy_vol_btc": 0.5,
            "taker_buy_vol_usdt": 50.0,
        }
        for minute in range(10)
    ]
    _write_partition(tmp_path, symbol, start, _canonical_frame(rows))

    class NoopProvider:
        def fetch_native_candles(self, *args: object, **kwargs: object) -> list[dict[str, object]]:
            raise AssertionError("native candles should be disabled")

        def build_canonical_minutes(
            self,
            requested_symbol: str,
            start_time: datetime,
            end_time: datetime,
        ) -> pl.DataFrame:
            raise AssertionError("local lake should cover the legacy request")

        def close(self) -> None:
            return None

    service = LiveDataApiService(
        repository=MinuteLakeRepository(tmp_path),
        provider=NoopProvider(),
        default_limit=20,
        max_limit=500,
        on_demand_max_minutes=60_480,
        fetch_planner_config=FetchPlannerConfig(
            enable_native_binance_tf_candles=False,
            candle_fetch_mode=CandleFetchMode.AGGREGATE_FROM_1M,
            allow_legacy_1m_fallback=True,
        ),
    )
    client = TestClient(create_app(service))

    response = client.get(
        "/api/v1/perpetual-data",
        params={"coin": "ETH", "tfs": "5m=1", "end_time": "2026-01-15T10:09:00Z"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "local"
    assert payload["timeframe_metadata"]["5m"]["fetch_mode"] == "aggregate_from_1m"
    assert payload["timeframe_metadata"]["5m"]["fallback_used"] is True
    assert "using_legacy_1m_aggregation_fallback" in payload["timeframe_metadata"]["5m"]["notes"]
    assert payload["data"]["5m"][0]["timestamp"] == "2026-01-15T10:05:00.000Z"


def test_binance_provider_ffills_historical_oi_from_metrics_lookback(tmp_path: Path) -> None:
    provider = BinanceCanonicalMinuteProvider(
        root_dir=tmp_path / "data",
        rest_base_url="https://fapi.binance.com",
        vision_base_url="https://data.binance.vision/data/futures/um/daily",
        max_ffill_minutes=60,
        rest_timeout_seconds=1,
        rest_max_retries=1,
    )

    class FakeVisionLoader:
        def load_klines(
            self,
            symbol: str,
            start: datetime,
            end: datetime,
            interval: str = "1m",
        ) -> list[dict[str, object]]:
            assert symbol == "KITEUSDT"
            return [
                {
                    "open_time": int(datetime(2026, 4, 10, 23, 59, tzinfo=UTC).timestamp() * 1000),
                    "open": 0.12998,
                    "high": 0.13009,
                    "low": 0.12993,
                    "close": 0.13005,
                    "volume_btc": 40934.0,
                    "volume_usdt": 5321.28162,
                    "trade_count": 224,
                    "taker_buy_vol_btc": 34835.0,
                    "taker_buy_vol_usdt": 4528.44928,
                }
            ]

        def load_mark_price_klines(
            self,
            symbol: str,
            start: datetime,
            end: datetime,
            interval: str = "1m",
        ) -> list[dict[str, object]]:
            return []

        def load_index_price_klines(
            self,
            symbol: str,
            start: datetime,
            end: datetime,
            interval: str = "1m",
        ) -> list[dict[str, object]]:
            return []

        def load_agg_trades(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, object]]:
            return []

        def load_book_ticker(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, object]]:
            return []

        def load_metrics(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, object]]:
            assert start == datetime(2026, 4, 10, 22, 59, tzinfo=UTC)
            return [
                {
                    "create_time": int(datetime(2026, 4, 10, 23, 55, tzinfo=UTC).timestamp() * 1000),
                    "oi_contracts": 173966503.0,
                    "oi_value_usdt": 22257274.39382,
                }
            ]

    class FakeRest:
        def fetch_funding_rate(
            self,
            symbol: str,
            start_time: datetime | None = None,
            end_time: datetime | None = None,
            limit: int = 1000,
        ) -> list[dict[str, object]]:
            return []

        def fetch_top_trader_long_short_account_ratio(
            self,
            symbol: str,
            *,
            period: str = "5m",
            start_time: datetime | None = None,
            end_time: datetime | None = None,
            limit: int = 500,
        ) -> list[dict[str, object]]:
            return []

        def fetch_global_long_short_account_ratio(
            self,
            symbol: str,
            *,
            period: str = "5m",
            start_time: datetime | None = None,
            end_time: datetime | None = None,
            limit: int = 500,
        ) -> list[dict[str, object]]:
            return []

    provider._vision_loader = FakeVisionLoader()
    provider._rest = FakeRest()

    frame = provider.build_canonical_minutes(
        "KITEUSDT",
        start_time=datetime(2026, 4, 10, 23, 59, tzinfo=UTC),
        end_time=datetime(2026, 4, 10, 23, 59, tzinfo=UTC),
    )

    row = frame.row(0, named=True)
    assert row["oi_contracts"] == 173966503.0
    assert row["oi_value_usdt"] == 22257274.39382


def test_binance_provider_tolerates_optional_rest_failures(tmp_path: Path) -> None:
    provider = BinanceCanonicalMinuteProvider(
        root_dir=tmp_path / "data",
        rest_base_url="https://fapi.binance.com",
        vision_base_url="https://data.binance.vision/data/futures/um/daily",
        max_ffill_minutes=60,
        rest_timeout_seconds=1,
        rest_max_retries=1,
    )

    class FakeRest:
        def fetch_klines(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            interval: str = "1m",
            limit: int = 1500,
        ) -> list[dict[str, object]]:
            return [
                {
                    "open_time": int(start_time.timestamp() * 1000),
                    "open": 2500.0,
                    "high": 2510.0,
                    "low": 2495.0,
                    "close": 2505.0,
                    "volume_btc": 10.0,
                    "close_time": int((start_time + timedelta(minutes=1)).timestamp() * 1000),
                    "volume_usdt": 25050.0,
                    "trade_count": 20,
                    "taker_buy_vol_btc": 6.0,
                    "taker_buy_vol_usdt": 15030.0,
                }
            ]

        def fetch_mark_price_klines(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            interval: str = "1m",
            limit: int = 1500,
        ) -> list[dict[str, object]]:
            return [
                {
                    "open_time": int(start_time.timestamp() * 1000),
                    "mark_price_open": 2500.0,
                    "mark_price_close": 2505.0,
                }
            ]

        def fetch_index_price_klines(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            interval: str = "1m",
            limit: int = 1500,
        ) -> list[dict[str, object]]:
            return [
                {
                    "open_time": int(start_time.timestamp() * 1000),
                    "index_price_open": 2499.0,
                    "index_price_close": 2504.0,
                }
            ]

        def fetch_agg_trades(
            self,
            symbol: str,
            start_time: datetime,
            end_time: datetime,
            limit: int = 1000,
        ) -> list[dict[str, object]]:
            raise RuntimeError("agg trades unavailable")

        def fetch_book_ticker(self, symbol: str) -> dict[str, object]:
            raise RuntimeError("book ticker unavailable")

        def fetch_premium_index(self, symbol: str) -> dict[str, object]:
            raise RuntimeError("premium unavailable")

        def fetch_open_interest(self, symbol: str) -> dict[str, object]:
            raise RuntimeError("open interest unavailable")

        def fetch_funding_rate(
            self,
            symbol: str,
            start_time: datetime | None = None,
            end_time: datetime | None = None,
            limit: int = 1000,
        ) -> list[dict[str, object]]:
            raise RuntimeError("funding unavailable")

        def fetch_top_trader_long_short_account_ratio(
            self,
            symbol: str,
            *,
            period: str = "5m",
            start_time: datetime | None = None,
            end_time: datetime | None = None,
            limit: int = 500,
        ) -> list[dict[str, object]]:
            raise RuntimeError("top trader ratio unavailable")

        def fetch_global_long_short_account_ratio(
            self,
            symbol: str,
            *,
            period: str = "5m",
            start_time: datetime | None = None,
            end_time: datetime | None = None,
            limit: int = 500,
        ) -> list[dict[str, object]]:
            raise RuntimeError("global ratio unavailable")

    provider._rest = FakeRest()

    import live_data_api_service.binance_provider as provider_module

    original_utc_now = provider_module.utc_now
    provider_module.utc_now = lambda: datetime(2026, 4, 11, 12, 0, tzinfo=UTC)
    try:
        frame = provider.build_canonical_minutes(
            "ETHUSDT",
            start_time=datetime(2026, 4, 11, 11, 59, tzinfo=UTC),
            end_time=datetime(2026, 4, 11, 11, 59, tzinfo=UTC),
        )
    finally:
        provider_module.utc_now = original_utc_now

    row = frame.row(0, named=True)
    assert row["open"] == 2500.0
    assert row["close"] == 2505.0
    assert row["trade_count"] == 20
    assert row["net_taker_vol_btc"] == 0.0
    assert row["funding_rate"] is None
    assert row["top_trader_ls_ratio_acct"] is None
