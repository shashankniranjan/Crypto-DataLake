from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import polars as pl

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

    def _legacy_window():
        nonlocal legacy_window
        if legacy_window is None:
            legacy_window = service.load_canonical_window(
                coin=coin,
                start_time=combined_window_start,
                end_time=resolved_end,
            )
        return legacy_window

    def _load_bars(api_name: str, polars_every: str, limit: int, end: datetime) -> tuple[pl.DataFrame, dict[str, object]]:
        minutes = _NATIVE_ELIGIBLE_MINUTES.get(api_name)
        if minutes is None:
            window = _legacy_window()
            return aggregate_ohlc_bars(window.frame, parse_indicator_timeframe(api_name), end_time=end).tail(limit), {
                "source": window.source,
                "fetch_mode": "aggregate_from_1m",
                "fallback_used": True,
                "notes": ["using_legacy_1m_aggregation_fallback"],
            }
        result = service.load_candle_bars(
            coin=coin,
            spec=TimeframeSpec(api_name=api_name, minutes=minutes, polars_every=polars_every),
            limit=limit,
            end_time=end,
        )
        return result.frame.tail(limit), result.metadata

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
        ema_future = executor.submit(_compute_ema)
        pivot_future = executor.submit(_compute_pivots)
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
