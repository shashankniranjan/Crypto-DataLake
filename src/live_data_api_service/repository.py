from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl

from binance_minute_lake.core.time_utils import iter_hours

from .utils import cast_canonical_frame, empty_canonical_frame


class MinuteLakeRepository:
    def __init__(self, root_dir: Path) -> None:
        self._root_dir = root_dir

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

