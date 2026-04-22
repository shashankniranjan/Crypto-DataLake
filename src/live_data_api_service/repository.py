from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar

import polars as pl

from binance_minute_lake.core.time_utils import iter_hours

from .utils import cast_canonical_frame, empty_canonical_frame


class MinuteLakeRepository:
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir

    @property
    def root_dir(self) -> Path:
        return self._root_dir

    def load_canonical_minutes(self, symbol: str, start_time: datetime, end_time: datetime) -> pl.DataFrame:
        partition_paths = [
            self._partition_path(symbol, hour_start)
            for hour_start in iter_hours(start_time, end_time)
        ]
        existing_paths = [path for path in partition_paths if path.exists()]
        if not existing_paths:
            return empty_canonical_frame()

        frame = (
            pl.scan_parquet([str(path) for path in existing_paths])
            .filter((pl.col("timestamp") >= start_time) & (pl.col("timestamp") <= end_time))
            .collect()
        )
        if frame.height == 0:
            return empty_canonical_frame()
        return cast_canonical_frame(frame)

    def _partition_path(self, symbol: str, hour_start: datetime) -> Path:
        return (
            self._root_dir
            / "futures"
            / "um"
            / "minute"
            / f"symbol={symbol.upper()}"
            / f"year={hour_start:%Y}"
            / f"month={hour_start:%m}"
            / f"day={hour_start:%d}"
            / f"hour={hour_start:%H}"
            / "part.parquet"
        )


class HigherTimeframeRepository:
    _TIMEFRAME_ALIASES: ClassVar[dict[str, str]] = {
        "1h": "1h",
        "1hr": "1h",
        "4h": "4h",
        "4hr": "4h",
        "8h": "8h",
        "8hr": "8h",
        "1d": "1d",
        "1w": "1w",
        "1mo": "1M",
        "1month": "1M",
        "1M": "1M",
        "3m": "3m",
        "5m": "5m",
        "10m": "10m",
        "15m": "15m",
        "30m": "30m",
        "45m": "45m",
    }

    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir

    def load_candle_bars(
        self,
        *,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
    ) -> pl.DataFrame:
        resolved_timeframe = self._normalize_timeframe(timeframe)
        if resolved_timeframe is None:
            return empty_canonical_frame()

        partition_paths = [
            self._partition_path(resolved_timeframe, symbol, day_start)
            for day_start in _iter_days(start_time, end_time)
        ]
        existing_paths = [path for path in partition_paths if path.exists()]
        if not existing_paths:
            return empty_canonical_frame()

        end_exclusive = end_time.astimezone(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
        frame = (
            pl.scan_parquet([str(path) for path in existing_paths])
            .filter(
                (pl.col("bucket_start") >= start_time)
                & (pl.col("bucket_end") <= end_exclusive)
                & pl.col("bucket_complete").fill_null(False)
            )
            .collect()
        )
        if frame.height == 0:
            return empty_canonical_frame()
        return self._to_canonical_frame(frame)

    def _partition_path(self, timeframe: str, symbol: str, day_start: datetime) -> Path:
        return (
            self._root_dir
            / f"timeframe={timeframe}"
            / f"symbol={symbol.upper()}"
            / f"year={day_start:%Y}"
            / f"month={day_start:%m}"
            / f"day={day_start:%d}"
            / "part.parquet"
        )

    def _normalize_timeframe(self, timeframe: str) -> str | None:
        token = timeframe.strip()
        if not token:
            return None
        return self._TIMEFRAME_ALIASES.get(token, self._TIMEFRAME_ALIASES.get(token.lower()))

    @staticmethod
    def _to_canonical_frame(frame: pl.DataFrame) -> pl.DataFrame:
        result = frame
        rename_map: dict[str, str] = {}
        if "vwap" in result.columns:
            rename_map["vwap"] = "vwap_1m"
        if "realized_vol_htf" in result.columns:
            rename_map["realized_vol_htf"] = "realized_vol_1m"
        if rename_map:
            result = result.rename(rename_map)

        extra_columns = [
            column
            for column in (
                "timeframe",
                "symbol",
                "bucket_start",
                "bucket_end",
                "expected_minutes_in_bucket",
                "observed_minutes_in_bucket",
                "missing_minutes_count",
                "bucket_complete",
            )
            if column in result.columns
        ]
        if extra_columns:
            result = result.drop(extra_columns)
        return cast_canonical_frame(result)


def _iter_days(start_time: datetime, end_time: datetime) -> list[datetime]:
    cursor = start_time.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    last_day = end_time.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    days: list[datetime] = []
    while cursor <= last_day:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days
