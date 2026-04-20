from __future__ import annotations

from datetime import timedelta

import polars as pl

from live_data_api_service.utils import floor_utc_minute

from .timeframes import IndicatorTimeframeSpec

_EMPTY_BAR_FRAME = pl.DataFrame(
    schema={
        "timestamp": pl.Datetime("ms", "UTC"),
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
    }
)


def aggregate_ohlc_bars(
    frame: pl.DataFrame,
    spec: IndicatorTimeframeSpec,
    *,
    end_time: object,
) -> pl.DataFrame:
    if frame.height == 0:
        return _EMPTY_BAR_FRAME

    end_exclusive = floor_utc_minute(end_time) + timedelta(minutes=1)
    end_lit = pl.lit(end_exclusive, dtype=pl.Datetime("ms", "UTC"))

    return (
        frame.sort("timestamp")
        .group_by_dynamic(
            "timestamp",
            every=spec.polars_every,
            period=spec.polars_every,
            closed="left",
            label="left",
            start_by="window",
        )
        .agg(
            pl.len().alias("_minute_count"),
            pl.col("open").drop_nulls().first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").drop_nulls().last().alias("close"),
        )
        .with_columns(
            pl.col("timestamp").dt.offset_by(spec.offset_by).alias("_period_end"),
        )
        .with_columns(
            (
                (
                    pl.col("_period_end").dt.timestamp("ms")
                    - pl.col("timestamp").dt.timestamp("ms")
                )
                / 60_000
            )
            .cast(pl.Int64)
            .alias("_expected_minutes"),
        )
        .filter(
            (pl.col("_period_end") <= end_lit)
            & (pl.col("_minute_count") == pl.col("_expected_minutes"))
            & pl.col("open").is_not_null()
            & pl.col("high").is_not_null()
            & pl.col("low").is_not_null()
            & pl.col("close").is_not_null()
        )
        .drop("_minute_count", "_period_end", "_expected_minutes")
        .sort("timestamp")
    )
