from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import polars as pl

from .bucketing import TimeframeSpec, add_bucket, bucket_end_expr, bucket_start_expr, expected_minutes_expr


@dataclass(frozen=True, slots=True)
class MissingBucketWindow:
    start: datetime
    end: datetime


def detect_missing_buckets(
    minute_timestamps: pl.LazyFrame,
    existing_index: pl.DataFrame,
    spec: TimeframeSpec,
) -> pl.DataFrame:
    available = (
        minute_timestamps
        .with_columns(bucket_start_expr(spec))
        .group_by("bucket_start")
        .agg(pl.len().alias("observed_minutes_in_bucket"))
        .with_columns(
            bucket_end_expr(spec),
            expected_minutes_expr(spec),
        )
        .with_columns(
            (pl.col("observed_minutes_in_bucket") == pl.col("expected_minutes_in_bucket")).alias("bucket_complete")
        )
        .filter(pl.col("bucket_complete"))
        .select("bucket_start")
        .collect()
    )
    if available.height == 0:
        return available

    existing = existing_index
    if existing.height == 0:
        return available.sort("bucket_start")

    return (
        available.join(
            existing.filter(pl.col("bucket_complete")).select("bucket_start"),
            on="bucket_start",
            how="anti",
        )
        .sort("bucket_start")
    )


def coalesce_bucket_windows(bucket_starts: pl.DataFrame, spec: TimeframeSpec) -> list[MissingBucketWindow]:
    if bucket_starts.height == 0:
        return []

    values = bucket_starts.get_column("bucket_start").to_list()
    windows: list[MissingBucketWindow] = []
    current_start = values[0]
    current_end = values[0]
    for value in values[1:]:
        expected_next = add_bucket(current_end, spec)
        if value == expected_next:
            current_end = value
            continue
        windows.append(MissingBucketWindow(start=current_start, end=current_end))
        current_start = value
        current_end = value
    windows.append(MissingBucketWindow(start=current_start, end=current_end))
    return windows
