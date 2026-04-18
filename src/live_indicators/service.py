from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from datetime import UTC, datetime, timedelta
from threading import Lock

import polars as pl

from binance_minute_lake.core.binance_usage import record_binance_cache_event
from live_data_api_service.service import LiveDataApiService
from live_data_api_service.timeframes import TimeframeSpec, normalize_symbol

from .aggregation import aggregate_ohlc_bars
from .ema import calculate_tradingview_ema
from .pivots import calculate_traditional_pivots
from .timeframes import floor_to_timeframe, parse_indicator_timeframe, shift_period_start

EMA_WARMUP_MULTIPLIER = 3
EMA_MIN_EXTRA_BARS = 20
_NATIVE_ELIGIBLE_MINUTES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "1hr": 60,
    "4hr": 240,
    "1d": 1440,
}


def build_indicator_payload(
    *,
    service: LiveDataApiService,
    coin: str,
    ema_tf: str,
    ema_length: int,
    pivot_tf: str,
    end_time: str | None = None,
) -> dict[str, object]:
    if ema_length < 1:
        raise ValueError("ema_length must be at least 1")

    normalized_symbol = normalize_symbol(coin)
    resolved_end = service.resolve_end_time(coin=coin, end_time=end_time)
    ema_spec = parse_indicator_timeframe(ema_tf)
    pivot_spec = parse_indicator_timeframe(pivot_tf)

    ema_history_bars = max(ema_length * EMA_WARMUP_MULTIPLIER, ema_length + EMA_MIN_EXTRA_BARS)
    ema_window_start = shift_period_start(
        floor_to_timeframe(resolved_end, ema_spec),
        ema_spec,
        -ema_history_bars,
    )
    pivot_period_start = floor_to_timeframe(resolved_end, pivot_spec)
    pivot_reference_end = pivot_period_start - timedelta(minutes=1)
    pivot_window_start = shift_period_start(pivot_period_start, pivot_spec, -1)

    combined_window_start = min(ema_window_start, pivot_window_start)
    legacy_window = None
    legacy_window_lock = Lock()
    shared_bar_cache: dict[tuple[str, datetime, int], tuple[pl.DataFrame, dict[str, object]]] = {}
    requires_legacy_window = (
        ema_spec.api_name not in _NATIVE_ELIGIBLE_MINUTES
        or pivot_spec.api_name not in _NATIVE_ELIGIBLE_MINUTES
    )

    def _legacy_window():
        nonlocal legacy_window
        if legacy_window is None:
            with legacy_window_lock:
                if legacy_window is None:
                    legacy_window = service.load_canonical_window(
                        coin=coin,
                        start_time=combined_window_start,
                        end_time=resolved_end,
                        allow_binance_patch=normalized_symbol != "BTCUSDT",
                    )
        return legacy_window

    def _load_btc_higher_timeframe_bars(
        api_name: str,
        limit: int,
        end: datetime,
    ) -> tuple[pl.DataFrame, dict[str, object]] | None:
        if normalized_symbol != "BTCUSDT" or api_name in {"1m", "3m"}:
            return None

        direct_spec = parse_indicator_timeframe(api_name)
        window_start = shift_period_start(floor_to_timeframe(end, direct_spec), direct_spec, -limit)
        frame = service.load_local_higher_timeframe_window(
            coin=coin,
            timeframe=api_name,
            start_time=window_start,
            end_time=end,
        )
        if frame.height == 0:
            return None
        bars = frame.select("timestamp", "open", "high", "low", "close").sort("timestamp").tail(limit)
        metadata = {
            "source": "local",
            "fetch_mode": "direct_local_higher_tf",
            "fallback_used": False,
            "notes": ["using_local_btc_higher_timeframe_lake"],
        }
        return bars, metadata

    def _load_bars(api_name: str, polars_every: str, limit: int, end: datetime) -> tuple[pl.DataFrame, dict[str, object]]:
        cache_key = (api_name, end, limit)
        cached = shared_bar_cache.get(cache_key)
        if cached is not None:
            record_binance_cache_event("indicator_shared_bar_cache_hit_exact")
            bars, metadata = cached
            return bars.clone(), dict(metadata)

        larger_cached = [
            (cached_limit, bars, metadata)
            for (cached_api_name, cached_end, cached_limit), (bars, metadata) in shared_bar_cache.items()
            if cached_api_name == api_name and cached_end == end and cached_limit >= limit
        ]
        if larger_cached:
            record_binance_cache_event("indicator_shared_bar_cache_hit_superset")
            _, bars, metadata = min(larger_cached, key=lambda item: item[0])
            sliced = bars.tail(limit).clone()
            shared_bar_cache[cache_key] = (sliced, dict(metadata))
            return sliced, dict(metadata)

        spec = parse_indicator_timeframe(api_name)
        direct_btc_higher_tf = _load_btc_higher_timeframe_bars(api_name, limit, end)
        if direct_btc_higher_tf is not None and direct_btc_higher_tf[0].height >= limit:
            bars, metadata = direct_btc_higher_tf
            shared_bar_cache[cache_key] = (bars.clone(), dict(metadata))
            return bars, metadata

        if requires_legacy_window:
            record_binance_cache_event("indicator_shared_canonical_window_used")
            window = _legacy_window()
            bars = aggregate_ohlc_bars(window.frame, spec, end_time=end).tail(limit)
            metadata = {
                "source": window.source,
                "fetch_mode": "aggregate_from_1m",
                "fallback_used": "binance" in window.source,
                "notes": ["using_shared_indicator_canonical_window"],
            }
            shared_bar_cache[cache_key] = (bars.clone(), dict(metadata))
            return bars, metadata

        minutes = _NATIVE_ELIGIBLE_MINUTES.get(api_name)
        if minutes is None:
            window = _legacy_window()
            bars = aggregate_ohlc_bars(window.frame, spec, end_time=end).tail(limit)
            metadata = {
                "source": window.source,
                "fetch_mode": "aggregate_from_1m",
                "fallback_used": True,
                "notes": ["using_legacy_1m_aggregation_fallback"],
            }
            shared_bar_cache[cache_key] = (bars.clone(), dict(metadata))
            return bars, metadata
        result = service.load_candle_bars(
            coin=coin,
            spec=TimeframeSpec(api_name=api_name, minutes=minutes, polars_every=polars_every),
            limit=limit,
            end_time=end,
        )
        bars = result.frame.tail(limit).clone()
        metadata = dict(result.metadata)
        shared_bar_cache[cache_key] = (bars.clone(), dict(metadata))
        return bars, metadata

    def _compute_ema() -> tuple[pl.DataFrame, float, dict]:
        bars, metadata = _load_bars(ema_spec.api_name, ema_spec.polars_every, ema_history_bars + 1, resolved_end)
        if bars.height < ema_length:
            raise ValueError(
                f"Not enough completed {ema_spec.api_name} bars to calculate EMA({ema_length}). "
                f"Need at least {ema_length} completed bars."
            )
        values = calculate_tradingview_ema(_float_values(bars.get_column("close")), ema_length)
        value = values[-1]
        if value is None:
            raise ValueError(
                f"Not enough completed {ema_spec.api_name} bars to calculate EMA({ema_length}). "
                f"Need at least {ema_length} completed bars."
            )
        return bars, value, {**bars.row(-1, named=True), "_metadata": metadata}

    def _compute_pivots() -> tuple[dict, dict]:
        bars, metadata = _load_bars(pivot_spec.api_name, pivot_spec.polars_every, 1, pivot_reference_end)
        if bars.height == 0:
            raise ValueError(
                f"Not enough completed {pivot_spec.api_name} bars to calculate traditional pivots."
            )
        bar = bars.row(-1, named=True)
        pts = calculate_traditional_pivots(
            high=float(bar["high"]),
            low=float(bar["low"]),
            close=float(bar["close"]),
        )
        return pts, {**bar, "_metadata": metadata}

    with ThreadPoolExecutor(max_workers=2) as executor:
        ema_future = executor.submit(copy_context().run, _compute_ema)
        pivot_future = executor.submit(copy_context().run, _compute_pivots)
        ema_bars, ema_value, ema_bar = ema_future.result()
        pivots, pivot_bar = pivot_future.result()

    return {
        "symbol": normalize_symbol(coin) if legacy_window is None else legacy_window.symbol,
        "end_time": _serialize_timestamp(resolved_end),
        "source": "mixed"
        if ema_bar["_metadata"]["source"] != pivot_bar["_metadata"]["source"]
        else ema_bar["_metadata"]["source"],
        "ema": {
            "timeframe": ema_spec.api_name,
            "length": ema_length,
            "warmup_bars_requested": ema_history_bars,
            "bars_used": ema_bars.height,
            "bar_timestamp": _serialize_timestamp(ema_bar["timestamp"]),
            "bar_close": float(ema_bar["close"]),
            "value": ema_value,
            "metadata": ema_bar["_metadata"],
        },
        "pivots": {
            "type": "traditional",
            "timeframe": pivot_spec.api_name,
            "reference_timestamp": _serialize_timestamp(pivot_bar["timestamp"]),
            "reference_ohlc": {
                "open": float(pivot_bar["open"]),
                "high": float(pivot_bar["high"]),
                "low": float(pivot_bar["low"]),
                "close": float(pivot_bar["close"]),
            },
            "metadata": pivot_bar["_metadata"],
            **pivots,
        },
    }


def _float_values(series: pl.Series) -> list[float]:
    return [float(value) for value in series.to_list()]


def _serialize_timestamp(value: object) -> str:
    if not isinstance(value, datetime):
        raise TypeError(f"Expected datetime timestamp, got {type(value)!r}")
    normalized = value.astimezone(UTC)
    return normalized.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"
