from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .timeframes import TimeframeSpec


class CandleFetchMode(StrEnum):
    NATIVE_PREFERRED = "native_preferred"
    AGGREGATE_FROM_1M = "aggregate_from_1m"
    AUTO = "auto"


BINANCE_NATIVE_CANDLE_TFS = frozenset({"1m", "3m", "5m", "15m", "1h", "4h", "1d"})
BINANCE_OI_HIST_TFS = frozenset({"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"})
BINANCE_LS_RATIO_TFS = BINANCE_OI_HIST_TFS

API_TO_BINANCE_INTERVAL = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "1hr": "1h",
    "4hr": "4h",
    "1d": "1d",
}


@dataclass(frozen=True, slots=True)
class FetchPlannerConfig:
    enable_native_binance_tf_candles: bool = True
    candle_fetch_mode: CandleFetchMode = CandleFetchMode.NATIVE_PREFERRED
    allow_legacy_1m_fallback: bool = True
    allow_partial_response_with_notes: bool = True


@dataclass(frozen=True, slots=True)
class TimeframeFetchDecision:
    api_name: str
    binance_interval: str | None
    candle_source: str
    fetch_mode: str
    fallback_used: bool
    notes: tuple[str, ...] = ()


def plan_timeframe_fetch(spec: TimeframeSpec, config: FetchPlannerConfig) -> TimeframeFetchDecision:
    interval = API_TO_BINANCE_INTERVAL.get(spec.api_name)
    native_supported = interval in BINANCE_NATIVE_CANDLE_TFS if interval is not None else False
    notes: list[str] = []

    if (
        config.enable_native_binance_tf_candles
        and config.candle_fetch_mode in {CandleFetchMode.NATIVE_PREFERRED, CandleFetchMode.AUTO}
        and native_supported
    ):
        if interval not in BINANCE_OI_HIST_TFS:
            notes.append(f"open_interest_hist_not_supported_for_{interval}")
        if interval not in BINANCE_LS_RATIO_TFS:
            notes.append(f"ls_ratio_not_supported_for_{interval}")
        if spec.api_name != "1m":
            notes.append("historical_depth_not_available_from_binance_rest")
        return TimeframeFetchDecision(
            api_name=spec.api_name,
            binance_interval=interval,
            candle_source="binance_native",
            fetch_mode="direct_tf",
            fallback_used=False,
            notes=tuple(notes),
        )

    if config.allow_legacy_1m_fallback:
        reason = "native_candle_fetch_disabled"
        if config.candle_fetch_mode == CandleFetchMode.AGGREGATE_FROM_1M:
            reason = "candle_fetch_mode_aggregate_from_1m"
        elif not native_supported:
            reason = "native_candle_timeframe_not_supported"
        return TimeframeFetchDecision(
            api_name=spec.api_name,
            binance_interval=interval,
            candle_source="legacy_1m",
            fetch_mode="aggregate_from_1m",
            fallback_used=True,
            notes=("using_legacy_1m_aggregation_fallback", reason),
        )

    return TimeframeFetchDecision(
        api_name=spec.api_name,
        binance_interval=interval,
        candle_source="unavailable",
        fetch_mode="unavailable",
        fallback_used=False,
        notes=("native_candle_timeframe_not_supported",),
    )


def response_capability_matrix() -> dict[str, object]:
    return {
        "candles": {
            "source": "binance_native",
            "kind": "historical_series",
            "supported_tfs": sorted(BINANCE_NATIVE_CANDLE_TFS),
        },
        "oi_hist": {
            "source": "binance_native",
            "kind": "historical_series",
            "supported_tfs": sorted(BINANCE_OI_HIST_TFS),
            "retention": "latest_1_month",
        },
        "ls_ratios": {
            "source": "binance_native",
            "kind": "historical_series",
            "supported_tfs": sorted(BINANCE_LS_RATIO_TFS),
        },
        "mark_price_klines": {
            "source": "binance_native",
            "kind": "historical_series",
            "supported_tfs": sorted(BINANCE_NATIVE_CANDLE_TFS),
        },
        "index_price_klines": {
            "source": "binance_native",
            "kind": "historical_series",
            "supported_tfs": sorted(BINANCE_NATIVE_CANDLE_TFS),
        },
        "premium_index_klines": {
            "source": "binance_native",
            "kind": "historical_series",
            "supported_tfs": sorted(BINANCE_NATIVE_CANDLE_TFS),
        },
        "funding_rate": {
            "source": "binance_native",
            "kind": "event_series",
            "alignment": "asof_backward",
        },
        "open_interest_current": {"source": "binance_snapshot", "kind": "current_only"},
        "depth": {"source": "binance_snapshot", "kind": "current_only"},
        "historical_depth_bars": {"source": "local_only_or_not_supported", "kind": "derived_or_unavailable"},
    }
