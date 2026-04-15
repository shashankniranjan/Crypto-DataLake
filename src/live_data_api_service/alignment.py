from __future__ import annotations

from datetime import UTC, datetime, timedelta
from enum import StrEnum

import polars as pl


class AlignmentMode(StrEnum):
    EXACT_TIMESTAMP = "exact_timestamp"
    ASOF_BACKWARD = "asof_backward"
    FORWARD_FILL_WITH_MAX_AGE = "forward_fill_with_max_age"


def normalize_bar_timestamp(
    value: datetime | int,
    timeframe_minutes: int,
    *,
    convention: str = "bar_open",
) -> datetime:
    """Normalize a timestamp onto the repo's canonical bar timestamp.

    API candle bars use bar-open timestamps. Auxiliary Binance series are
    normalized to that same bucket before exact alignment.
    """
    if convention != "bar_open":
        raise ValueError(f"Unsupported timestamp convention '{convention}'")
    if timeframe_minutes < 1:
        raise ValueError("timeframe_minutes must be at least 1")

    timestamp = _to_utc_datetime(value)
    total_minutes = int(timestamp.timestamp() // 60)
    bucket_minutes = (total_minutes // timeframe_minutes) * timeframe_minutes
    return datetime.fromtimestamp(bucket_minutes * 60, tz=UTC).replace(second=0, microsecond=0)


def align_series(
    frame: pl.DataFrame,
    rows: list[dict[str, object]],
    *,
    source_time_col: str,
    value_map: dict[str, str],
    mode: AlignmentMode,
    align_at_bar_close: bool = False,
    bar_minutes: int = 0,
    max_age: timedelta | None = None,
    normalize_source_timeframe_minutes: int | None = None,
) -> pl.DataFrame:
    """Align an auxiliary Binance series onto API bars.

    API bar timestamps are the bar-open timestamps used throughout this repo.
    Set align_at_bar_close=True for event series, such as funding, whose
    meaning is "last known value as of the bar close".
    """
    if frame.height == 0 or not rows:
        return frame

    aux = _rows_to_frame(
        rows,
        source_time_col=source_time_col,
        value_map=value_map,
        normalize_source_timeframe_minutes=normalize_source_timeframe_minutes,
    )
    if aux.height == 0:
        return frame
    temp_value_cols = {target: f"__aligned_{target}" for target in value_map.values()}
    aux = aux.rename(temp_value_cols)

    base = frame.sort("timestamp").with_columns(
        pl.col("timestamp").cast(pl.Datetime("ms", "UTC")).alias("timestamp")
    )
    if align_at_bar_close:
        base = base.with_columns((pl.col("timestamp") + pl.duration(minutes=bar_minutes)).alias("_align_ts"))
    else:
        base = base.with_columns(pl.col("timestamp").alias("_align_ts"))

    if mode == AlignmentMode.EXACT_TIMESTAMP:
        merged = base.join(aux.drop("_source_ts"), left_on="_align_ts", right_on="_aux_ts", how="left")
    else:
        merged = base.join_asof(aux, left_on="_align_ts", right_on="_aux_ts", strategy="backward")
        if mode == AlignmentMode.FORWARD_FILL_WITH_MAX_AGE:
            if max_age is None:
                raise ValueError("max_age is required for forward_fill_with_max_age alignment")
            max_age_ms = int(max_age.total_seconds() * 1000)
            age_expr = (pl.col("_align_ts") - pl.col("_source_ts")).dt.total_milliseconds()
            for target_col in temp_value_cols.values():
                merged = merged.with_columns(
                    pl.when(age_expr <= max_age_ms)
                    .then(pl.col(target_col))
                    .otherwise(None)
                    .alias(target_col)
                )

    updates: list[pl.Expr] = []
    for target_col, temp_col in temp_value_cols.items():
        if temp_col not in merged.columns:
            continue
        if target_col in merged.columns:
            updates.append(pl.coalesce([pl.col(temp_col), pl.col(target_col)]).alias(target_col))
        else:
            updates.append(pl.col(temp_col).alias(target_col))
    if updates:
        merged = merged.with_columns(updates)

    drop_cols = [
        col
        for col in ("_align_ts", "_aux_ts", "_source_ts", *temp_value_cols.values())
        if col in merged.columns
    ]
    return merged.drop(drop_cols) if drop_cols else merged


def _rows_to_frame(
    rows: list[dict[str, object]],
    *,
    source_time_col: str,
    value_map: dict[str, str],
    normalize_source_timeframe_minutes: int | None = None,
) -> pl.DataFrame:
    records: list[dict[str, object]] = []
    for row in rows:
        raw_ts = row.get(source_time_col)
        if raw_ts is None:
            continue
        source_dt = _to_utc_datetime(raw_ts)
        aux_dt = (
            normalize_bar_timestamp(source_dt, normalize_source_timeframe_minutes)
            if normalize_source_timeframe_minutes is not None
            else source_dt
        )
        record = {
            "_aux_ts": int(aux_dt.timestamp() * 1000),
            "_source_ts": int(source_dt.timestamp() * 1000),
        }
        for source_col, target_col in value_map.items():
            record[target_col] = row.get(source_col)
        records.append(record)
    if not records:
        return pl.DataFrame()
    return (
        pl.DataFrame(records)
        .with_columns(
            pl.from_epoch(pl.col("_aux_ts"), time_unit="ms").dt.replace_time_zone("UTC").alias("_aux_ts"),
            pl.from_epoch(pl.col("_source_ts"), time_unit="ms").dt.replace_time_zone("UTC").alias("_source_ts"),
        )
        .sort("_aux_ts")
    )


def _to_utc_datetime(value: datetime | int | object) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC).replace(second=0, microsecond=0)
