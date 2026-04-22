from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl


class MinuteLakeReader:
    def __init__(self, minute_root: Path) -> None:
        self._minute_root = minute_root

    def inspect_range(self, symbol: str) -> tuple[datetime | None, datetime | None]:
        frame = self._scan_symbol(symbol).select(
            pl.col("timestamp").min().alias("min_ts"),
            pl.col("timestamp").max().alias("max_ts"),
        ).collect()
        if frame.height == 0:
            return None, None
        min_ts = frame.item(0, "min_ts")
        max_ts = frame.item(0, "max_ts")
        return _as_datetime(min_ts), _as_datetime(max_ts)

    def latest_minute(self, symbol: str) -> datetime | None:
        latest = self._scan_symbol(symbol).select(pl.col("timestamp").max().alias("max_ts")).collect()
        if latest.height == 0:
            return None
        return _as_datetime(latest.item(0, "max_ts"))

    def scan_available_minutes(
        self,
        symbol: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> pl.LazyFrame:
        scan = self._scan_symbol(symbol).select("timestamp")
        if start is not None:
            scan = scan.filter(pl.col("timestamp") >= start)
        if end is not None:
            scan = scan.filter(pl.col("timestamp") <= end)
        return scan.sort("timestamp").unique(subset=["timestamp"], keep="last").sort("timestamp")

    def read_window(self, symbol: str, start: datetime, end: datetime) -> pl.DataFrame:
        frame = (
            self._scan_symbol(symbol)
            .filter((pl.col("timestamp") >= start) & (pl.col("timestamp") <= end))
            .collect()
        )
        if frame.height == 0:
            return frame
        return (
            frame.sort(
                ["timestamp", "arrival_time", "event_time", "transact_time", "update_id_end"],
                nulls_last=True,
            )
            .unique(subset=["timestamp"], keep="last")
            .sort("timestamp")
        )

    def partition_directories(self, symbol: str) -> frozenset[str]:
        root = self._symbol_root(symbol)
        if not root.exists():
            return frozenset()
        return frozenset(
            str(path.parent.relative_to(root))
            for path in root.rglob("*.parquet")
            if path.is_file()
        )

    def _scan_symbol(self, symbol: str) -> pl.LazyFrame:
        path = self._symbol_root(symbol)
        if not path.exists():
            return pl.LazyFrame(schema={"timestamp": pl.Datetime("ms", "UTC")})
        return pl.scan_parquet(str(path / "**" / "*.parquet"))

    def _symbol_root(self, symbol: str) -> Path:
        return self._minute_root / f"symbol={symbol.upper()}"


def _as_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    raise TypeError(f"Expected datetime or None, got {type(value)!r}")
