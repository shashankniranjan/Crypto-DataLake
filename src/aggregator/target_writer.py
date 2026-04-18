from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path

import polars as pl


class HigherTimeframeWriter:
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir

    def write_buckets(self, frame: pl.DataFrame) -> int:
        if frame.height == 0:
            return 0

        count = 0
        for partition_frame in frame.partition_by("timeframe", "symbol", "year", "month", "day", as_dict=False):
            row = partition_frame.row(0, named=True)
            path = self._partition_path(
                timeframe=str(row["timeframe"]),
                symbol=str(row["symbol"]),
                year=int(row["year"]),
                month=int(row["month"]),
                day=int(row["day"]),
            )
            path.parent.mkdir(parents=True, exist_ok=True)

            effective = partition_frame.drop("year", "month", "day")
            if path.exists():
                existing = pl.read_parquet(path)
                effective = (
                    pl.concat([existing, effective], how="vertical_relaxed")
                    .sort("bucket_start")
                    .unique(subset=["bucket_start"], keep="last")
                    .sort("bucket_start")
                )

            tmp_dir = self._root_dir / ".tmp"
            tmp_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = tmp_dir / f"{uuid.uuid4().hex}.parquet"
            effective.write_parquet(tmp_path, compression="zstd", statistics=True)
            tmp_path.replace(path)
            count += partition_frame.height
        return count

    def scan_existing_index(self, symbol: str, timeframe: str) -> pl.DataFrame:
        dataset_root = self._dataset_root(timeframe, symbol)
        if not dataset_root.exists():
            return pl.DataFrame(schema={"bucket_start": pl.Datetime("ms", "UTC"), "bucket_complete": pl.Boolean})
        return (
            pl.scan_parquet(str(dataset_root / "**" / "*.parquet"))
            .select("bucket_start", "bucket_complete")
            .sort("bucket_start")
            .collect()
        )

    def _partition_path(self, *, timeframe: str, symbol: str, year: int, month: int, day: int) -> Path:
        return (
            self._dataset_root(timeframe, symbol)
            / f"year={year:04d}"
            / f"month={month:02d}"
            / f"day={day:02d}"
            / "part.parquet"
        )

    def _dataset_root(self, timeframe: str, symbol: str) -> Path:
        return self._root_dir / f"timeframe={timeframe}" / f"symbol={symbol.upper()}"


def add_partition_columns(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.with_columns(
        pl.col("bucket_start").dt.year().alias("year"),
        pl.col("bucket_start").dt.month().alias("month"),
        pl.col("bucket_start").dt.day().alias("day"),
    )


def latest_bucket_start(frame: pl.DataFrame) -> datetime | None:
    if frame.height == 0:
        return None
    value = frame.select(pl.col("bucket_start").max().alias("max_bucket")).item(0, "max_bucket")
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    raise TypeError(f"Expected datetime or None, got {type(value)!r}")
