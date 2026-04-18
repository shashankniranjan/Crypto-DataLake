from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta

import polars as pl

from .aggregation_rules import aggregate_minutes
from .bucketing import TimeframeSpec, add_bucket, floor_to_bucket
from .source_reader import MinuteLakeReader
from .state_store import AggregatorStateStore
from .target_writer import HigherTimeframeWriter, add_partition_columns, latest_bucket_start

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IncrementalResult:
    buckets_scanned: int
    buckets_written: int
    incomplete_buckets_skipped: int
    repaired_buckets: int
    lag_minutes: int | None


class IncrementalRunner:
    def __init__(
        self,
        reader: MinuteLakeReader,
        writer: HigherTimeframeWriter,
        state_store: AggregatorStateStore,
        *,
        allow_incomplete_buckets: bool,
        repair_lookback_minutes: int,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._state_store = state_store
        self._allow_incomplete_buckets = allow_incomplete_buckets
        self._repair_lookback_minutes = repair_lookback_minutes

    def run_once(self, symbol: str, spec: TimeframeSpec) -> IncrementalResult:
        latest_minute = self._reader.latest_minute(symbol)
        if latest_minute is None:
            return IncrementalResult(0, 0, 0, 0, None)

        state = self._state_store.get_state(symbol, spec.name)
        existing_index = self._writer.scan_existing_index(symbol, spec.name)

        repair_anchor = latest_minute - timedelta(minutes=self._repair_lookback_minutes)
        repair_start = floor_to_bucket(repair_anchor, spec)

        if state is None or state.last_completed_bucket_start is None:
            start = repair_start
        else:
            start = min(add_bucket(state.last_completed_bucket_start, spec), repair_start)

        source = self._reader.read_window(symbol, start, latest_minute)
        aggregated = aggregate_minutes(source, spec, symbol)
        scanned = aggregated.height
        repaired = 0

        if not self._allow_incomplete_buckets:
            skipped = aggregated.filter(~pl.col("bucket_complete")).height
            aggregated = aggregated.filter(pl.col("bucket_complete"))
        else:
            skipped = 0

        if aggregated.height > 0:
            existing_complete = set(
                existing_index.filter(pl.col("bucket_complete")).get_column("bucket_start").to_list()
            )
            repaired = sum(1 for value in aggregated.get_column("bucket_start").to_list() if value in existing_complete)
            written = self._writer.write_buckets(add_partition_columns(aggregated))
        else:
            written = 0

        last_completed = latest_bucket_start(
            self._writer.scan_existing_index(symbol, spec.name).filter(pl.col("bucket_complete"))
        )
        self._state_store.upsert_state(
            symbol=symbol,
            timeframe=spec.name,
            last_completed_bucket_start=last_completed,
            last_seen_source_minute=latest_minute,
            status="continuous",
        )

        lag_minutes = None
        if last_completed is not None:
            lag_delta = latest_minute - add_bucket(last_completed, spec) + timedelta(minutes=1)
            lag_minutes = max(int(lag_delta.total_seconds() // 60), 0)

        logger.info(
            "incremental run timeframe=%s scanned=%s written=%s skipped_incomplete=%s repaired=%s lag_minutes=%s",
            spec.name,
            scanned,
            written,
            skipped,
            repaired,
            lag_minutes,
        )
        return IncrementalResult(scanned, written, skipped, repaired, lag_minutes)
