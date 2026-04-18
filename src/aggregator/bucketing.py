from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import polars as pl


@dataclass(frozen=True, slots=True)
class TimeframeSpec:
    name: str
    polars_every: str
    offset_by: str


_SUPPORTED_TIMEFRAMES: dict[str, TimeframeSpec] = {
    "3m": TimeframeSpec(name="3m", polars_every="3m", offset_by="3m"),
    "5m": TimeframeSpec(name="5m", polars_every="5m", offset_by="5m"),
    "10m": TimeframeSpec(name="10m", polars_every="10m", offset_by="10m"),
    "15m": TimeframeSpec(name="15m", polars_every="15m", offset_by="15m"),
    "30m": TimeframeSpec(name="30m", polars_every="30m", offset_by="30m"),
    "45m": TimeframeSpec(name="45m", polars_every="45m", offset_by="45m"),
    "1h": TimeframeSpec(name="1h", polars_every="1h", offset_by="1h"),
    "4h": TimeframeSpec(name="4h", polars_every="4h", offset_by="4h"),
    "8h": TimeframeSpec(name="8h", polars_every="8h", offset_by="8h"),
    "1d": TimeframeSpec(name="1d", polars_every="1d", offset_by="1d"),
    "1w": TimeframeSpec(name="1w", polars_every="1w", offset_by="1w"),
    "1M": TimeframeSpec(name="1M", polars_every="1mo", offset_by="1mo"),
}

_ALIASES = {"1hr": "1h", "4hr": "4h", "8hr": "8h", "1mo": "1M", "1month": "1M"}


def supported_timeframes() -> tuple[str, ...]:
    return tuple(_SUPPORTED_TIMEFRAMES)


def parse_timeframe(value: str) -> TimeframeSpec:
    token = value.strip()
    if not token:
        raise ValueError("timeframe is required")
    token = _ALIASES.get(token.lower(), token)
    if token not in _SUPPORTED_TIMEFRAMES:
        supported = ", ".join(_SUPPORTED_TIMEFRAMES)
        raise ValueError(f"Unsupported timeframe '{value}'. Supported values: {supported}")
    return _SUPPORTED_TIMEFRAMES[token]


def timeframe_expr(timestamp_col: str = "timestamp") -> pl.Expr:
    return pl.col(timestamp_col).cast(pl.Datetime("ms", "UTC"))


def bucket_start_expr(spec: TimeframeSpec, timestamp_col: str = "timestamp") -> pl.Expr:
    return timeframe_expr(timestamp_col).dt.truncate(spec.polars_every).alias("bucket_start")


def bucket_end_expr(spec: TimeframeSpec, bucket_col: str = "bucket_start") -> pl.Expr:
    return pl.col(bucket_col).dt.offset_by(spec.offset_by).alias("bucket_end")


def expected_minutes_expr(spec: TimeframeSpec, bucket_col: str = "bucket_start") -> pl.Expr:
    return (
        (
            pl.col(bucket_col).dt.offset_by(spec.offset_by).dt.timestamp("ms")
            - pl.col(bucket_col).dt.timestamp("ms")
        )
        / 60_000
    ).cast(pl.Int64).alias("expected_minutes_in_bucket")


def floor_to_bucket(value: datetime, spec: TimeframeSpec) -> datetime:
    current = value.astimezone(UTC).replace(second=0, microsecond=0)
    name = spec.name
    if name.endswith("m") and name not in {"1M"}:
        minutes = int(name[:-1])
        total_minutes = current.hour * 60 + current.minute
        floored_total = (total_minutes // minutes) * minutes
        return current.replace(hour=floored_total // 60, minute=floored_total % 60)
    if name == "1h":
        return current.replace(minute=0)
    if name == "4h":
        return current.replace(hour=(current.hour // 4) * 4, minute=0)
    if name == "8h":
        return current.replace(hour=(current.hour // 8) * 8, minute=0)
    if name == "1d":
        return current.replace(hour=0, minute=0)
    if name == "1w":
        week_start = current - timedelta(days=current.weekday())
        return week_start.replace(hour=0, minute=0)
    if name == "1M":
        return current.replace(day=1, hour=0, minute=0)
    raise ValueError(f"Unsupported timeframe '{spec.name}'")


def add_bucket(value: datetime, spec: TimeframeSpec, periods: int = 1) -> datetime:
    current = floor_to_bucket(value, spec)
    name = spec.name
    if name.endswith("m") and name not in {"1M"}:
        return current + timedelta(minutes=int(name[:-1]) * periods)
    if name == "1h":
        return current + timedelta(hours=periods)
    if name == "4h":
        return current + timedelta(hours=4 * periods)
    if name == "8h":
        return current + timedelta(hours=8 * periods)
    if name == "1d":
        return current + timedelta(days=periods)
    if name == "1w":
        return current + timedelta(weeks=periods)
    if name == "1M":
        month_index = (current.year * 12 + (current.month - 1)) + periods
        target_year, target_month_index = divmod(month_index, 12)
        return current.replace(year=target_year, month=target_month_index + 1, day=1)
    raise ValueError(f"Unsupported timeframe '{spec.name}'")


def latest_complete_bucket_start(latest_source_minute: datetime, spec: TimeframeSpec) -> datetime | None:
    current_bucket = floor_to_bucket(latest_source_minute, spec)
    if add_bucket(current_bucket, spec) <= latest_source_minute + timedelta(minutes=1):
        return current_bucket
    previous = add_bucket(current_bucket, spec, periods=-1)
    if previous > latest_source_minute:
        return None
    return previous
