from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta


@dataclass(frozen=True, slots=True)
class IndicatorTimeframeSpec:
    api_name: str
    polars_every: str
    offset_by: str


_SUPPORTED_TIMEFRAMES: dict[str, IndicatorTimeframeSpec] = {
    "1m": IndicatorTimeframeSpec(api_name="1m", polars_every="1m", offset_by="1m"),
    "3m": IndicatorTimeframeSpec(api_name="3m", polars_every="3m", offset_by="3m"),
    "5m": IndicatorTimeframeSpec(api_name="5m", polars_every="5m", offset_by="5m"),
    "15m": IndicatorTimeframeSpec(api_name="15m", polars_every="15m", offset_by="15m"),
    "1hr": IndicatorTimeframeSpec(api_name="1hr", polars_every="1h", offset_by="1h"),
    "4hr": IndicatorTimeframeSpec(api_name="4hr", polars_every="4h", offset_by="4h"),
    "1d": IndicatorTimeframeSpec(api_name="1d", polars_every="1d", offset_by="1d"),
    "1w": IndicatorTimeframeSpec(api_name="1w", polars_every="1w", offset_by="1w"),
    "1mo": IndicatorTimeframeSpec(api_name="1mo", polars_every="1mo", offset_by="1mo"),
}

_ALIASES: dict[str, str] = {
    "1h": "1hr",
    "60m": "1hr",
    "4h": "4hr",
    "240m": "4hr",
    "1day": "1d",
    "1week": "1w",
    "1mon": "1mo",
    "1month": "1mo",
}


def parse_indicator_timeframe(value: str) -> IndicatorTimeframeSpec:
    raw = value.strip()
    if not raw:
        raise ValueError("timeframe is required")

    token = "1mo" if raw == "1M" else raw.lower()
    token = _ALIASES.get(token, token)
    if token not in _SUPPORTED_TIMEFRAMES:
        supported = ", ".join(_SUPPORTED_TIMEFRAMES)
        raise ValueError(f"Unsupported timeframe '{raw}'. Supported values: {supported}, 1M")
    return _SUPPORTED_TIMEFRAMES[token]


def floor_to_timeframe(value: datetime, spec: IndicatorTimeframeSpec) -> datetime:
    current = value.astimezone(UTC).replace(second=0, microsecond=0)
    name = spec.api_name
    if name == "1m":
        return current
    if name == "3m":
        return current.replace(minute=(current.minute // 3) * 3)
    if name == "5m":
        return current.replace(minute=(current.minute // 5) * 5)
    if name == "15m":
        return current.replace(minute=(current.minute // 15) * 15)
    if name == "1hr":
        return current.replace(minute=0)
    if name == "4hr":
        return current.replace(hour=(current.hour // 4) * 4, minute=0)
    if name == "1d":
        return current.replace(hour=0, minute=0)
    if name == "1w":
        week_start = current - timedelta(days=current.weekday())
        return week_start.replace(hour=0, minute=0)
    if name == "1mo":
        return current.replace(day=1, hour=0, minute=0)
    raise ValueError(f"Unsupported timeframe '{spec.api_name}'")


def shift_period_start(value: datetime, spec: IndicatorTimeframeSpec, periods: int) -> datetime:
    current = floor_to_timeframe(value, spec)
    name = spec.api_name
    if name == "1m":
        return current + timedelta(minutes=periods)
    if name == "3m":
        return current + timedelta(minutes=3 * periods)
    if name == "5m":
        return current + timedelta(minutes=5 * periods)
    if name == "15m":
        return current + timedelta(minutes=15 * periods)
    if name == "1hr":
        return current + timedelta(hours=periods)
    if name == "4hr":
        return current + timedelta(hours=4 * periods)
    if name == "1d":
        return current + timedelta(days=periods)
    if name == "1w":
        return current + timedelta(weeks=periods)
    if name == "1mo":
        return _add_months(current, periods)
    raise ValueError(f"Unsupported timeframe '{spec.api_name}'")


def _add_months(value: datetime, months: int) -> datetime:
    month_index = (value.year * 12 + (value.month - 1)) + months
    target_year, target_month_index = divmod(month_index, 12)
    return value.replace(year=target_year, month=target_month_index + 1, day=1)
