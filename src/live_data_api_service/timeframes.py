from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class TimeframeSpec:
    api_name: str
    minutes: int
    polars_every: str


@dataclass(frozen=True, slots=True)
class TimeframeRequest:
    spec: TimeframeSpec
    limit: int | None = None

    @property
    def api_name(self) -> str:
        return self.spec.api_name


_SUPPORTED_TIMEFRAMES: dict[str, TimeframeSpec] = {
    "1m": TimeframeSpec(api_name="1m", minutes=1, polars_every="1m"),
    "3m": TimeframeSpec(api_name="3m", minutes=3, polars_every="3m"),
    "5m": TimeframeSpec(api_name="5m", minutes=5, polars_every="5m"),
    "15m": TimeframeSpec(api_name="15m", minutes=15, polars_every="15m"),
    "1hr": TimeframeSpec(api_name="1hr", minutes=60, polars_every="1h"),
    "4hr": TimeframeSpec(api_name="4hr", minutes=240, polars_every="4h"),
}

_ALIASES: dict[str, str] = {
    "1h": "1hr",
    "60m": "1hr",
    "4h": "4hr",
    "240m": "4hr",
}

_KNOWN_QUOTES = ("USDT", "BUSD", "USDC")


def normalize_symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError("coin is required")
    if normalized.endswith(_KNOWN_QUOTES):
        return normalized
    return f"{normalized}USDT"


def _raw_timeframe_items(value: str | Iterable[str]) -> list[str]:
    if isinstance(value, str):
        return value.split(",")
    return list(value)


def _parse_timeframe_spec(value: str) -> TimeframeSpec:
    token = value.strip().lower()
    if not token:
        raise ValueError("tfs must contain at least one timeframe")
    token = _ALIASES.get(token, token)
    if token not in _SUPPORTED_TIMEFRAMES:
        supported = ", ".join(_SUPPORTED_TIMEFRAMES)
        raise ValueError(f"Unsupported timeframe '{value.strip()}'. Supported values: {supported}")
    return _SUPPORTED_TIMEFRAMES[token]


def parse_timeframe_requests(value: str | Iterable[str]) -> list[TimeframeRequest]:
    raw_items = _raw_timeframe_items(value)

    resolved: list[TimeframeRequest] = []
    seen: set[str] = set()
    for raw_item in raw_items:
        token = raw_item.strip()
        if not token:
            continue

        timeframe_token, separator, limit_token = token.partition("=")
        spec = _parse_timeframe_spec(timeframe_token)

        explicit_limit: int | None = None
        if separator:
            limit_text = limit_token.strip()
            if not limit_text:
                raise ValueError(f"Missing limit for timeframe '{timeframe_token.strip()}'")
            try:
                explicit_limit = int(limit_text)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid limit '{limit_text}' for timeframe '{timeframe_token.strip()}'"
                ) from exc
            if explicit_limit < 1:
                raise ValueError(f"Limit for timeframe '{spec.api_name}' must be at least 1")

        if spec.api_name in seen:
            continue
        seen.add(spec.api_name)
        resolved.append(TimeframeRequest(spec=spec, limit=explicit_limit))

    if not resolved:
        raise ValueError("tfs must contain at least one timeframe")
    return resolved


def parse_timeframes(value: str | Iterable[str]) -> list[TimeframeSpec]:
    return [request.spec for request in parse_timeframe_requests(value)]


def requested_window_start(
    end_time: datetime,
    *,
    specs: list[TimeframeSpec],
    limit: int | None = None,
    timeframe_limits: Mapping[str, int] | None = None,
) -> datetime:
    if timeframe_limits is None:
        if limit is None:
            raise ValueError("limit is required when timeframe_limits are not provided")
        requested_minutes = max((limit + 1) * spec.minutes for spec in specs)
    else:
        requested_minutes = max((timeframe_limits[spec.api_name] + 1) * spec.minutes for spec in specs)
    return end_time - timedelta(minutes=requested_minutes - 1)
