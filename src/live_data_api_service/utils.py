from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from binance_minute_lake.core.schema import canonical_column_names, dtype_map


def empty_canonical_frame() -> pl.DataFrame:
    columns = {
        name: pl.Series(name=name, values=[], dtype=data_type)
        for name, data_type in dtype_map().items()
    }
    return pl.DataFrame(columns).select(canonical_column_names())


def cast_canonical_frame(frame: pl.DataFrame) -> pl.DataFrame:
    canonical_types = dtype_map()
    result = frame
    for column in canonical_column_names():
        if column not in result.columns:
            result = result.with_columns(pl.lit(None).cast(canonical_types[column]).alias(column))
    expressions = [
        pl.col(column).cast(canonical_types[column], strict=False).alias(column)
        for column in canonical_column_names()
    ]
    return result.select(*expressions).sort("timestamp")


def floor_utc_minute(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(second=0, microsecond=0)


def last_completed_utc_minute(now: datetime | None = None) -> datetime:
    current = floor_utc_minute(now or datetime.now(tz=UTC))
    return current - timedelta(minutes=1)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return floor_utc_minute(parsed)


def expected_minute_count(start: datetime, end: datetime) -> int:
    if end < start:
        return 0
    total_minutes = int((end - start).total_seconds() // 60)
    return total_minutes + 1


def merge_canonical_frames(primary: pl.DataFrame, secondary: pl.DataFrame) -> pl.DataFrame:
    if primary.height == 0:
        return cast_canonical_frame(secondary)
    if secondary.height == 0:
        return cast_canonical_frame(primary)
    merged = (
        pl.concat([primary, secondary], how="vertical_relaxed")
        .sort("timestamp")
        .unique(subset=["timestamp"], keep="last")
        .sort("timestamp")
    )
    return cast_canonical_frame(merged)


def serialize_frame(frame: pl.DataFrame, *, include_deprecated_fields: bool = False) -> list[dict[str, object]]:
    if frame.height == 0:
        return []
    serializable = frame
    # The minute-lake storage schema still has 1m-specific column names. The API
    # exposes timeframe-neutral names by default; old names are emitted only
    # through an explicit compatibility flag and mirror the canonical values.
    alias_exprs: list[pl.Expr] = []
    if "vwap_bar" not in serializable.columns and "vwap_1m" in serializable.columns:
        alias_exprs.append(pl.col("vwap_1m").alias("vwap_bar"))
    if "realized_vol_bar" not in serializable.columns and "realized_vol_1m" in serializable.columns:
        alias_exprs.append(pl.col("realized_vol_1m").alias("realized_vol_bar"))
    if alias_exprs:
        serializable = serializable.with_columns(alias_exprs)

    legacy_cols = [column for column in ("vwap_1m", "realized_vol_1m") if column in serializable.columns]
    if include_deprecated_fields:
        legacy_exprs: list[pl.Expr] = []
        if "vwap_bar" in serializable.columns:
            legacy_exprs.append(pl.col("vwap_bar").alias("vwap_1m"))
        if "realized_vol_bar" in serializable.columns:
            legacy_exprs.append(pl.col("realized_vol_bar").alias("realized_vol_1m"))
        if legacy_exprs:
            serializable = serializable.with_columns(legacy_exprs)
    elif legacy_cols:
        serializable = serializable.drop(legacy_cols)

    internal_cols = [column for column in serializable.columns if column.startswith("_")]
    if internal_cols:
        serializable = serializable.drop(internal_cols)

    serializable = serializable.with_columns(
        pl.col("timestamp")
        .dt.convert_time_zone("UTC")
        .dt.strftime("%Y-%m-%dT%H:%M:%S%.3fZ")
        .alias("timestamp")
    )
    return serializable.to_dicts()
