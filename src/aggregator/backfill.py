from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta

import polars as pl

from .aggregation_rules import aggregate_minutes
from .bucketing import TimeframeSpec, add_bucket
from .source_reader import MinuteLakeReader
from .state_store import AggregatorStateStore
from .target_writer import HigherTimeframeWriter, add_partition_columns, latest_bucket_start
from .validator import MissingBucketWindow, detect_missing_buckets

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BackfillResult:
    buckets_scanned: int
    buckets_written: int
    incomplete_buckets_skipped: int
    repaired_buckets: int


class BackfillRunner:
    def __init__(
        self,
        reader: MinuteLakeReader,
        writer: HigherTimeframeWriter,
        state_store: AggregatorStateStore,
        *,
        allow_incomplete_buckets: bool,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._state_store = state_store
        self._allow_incomplete_buckets = allow_incomplete_buckets

    def run_for_timeframe(self, symbol: str, spec: TimeframeSpec) -> BackfillResult:
        existing = self._writer.scan_existing_index(symbol, spec.name)
        missing = detect_missing_buckets(
            self._reader.scan_available_minutes(symbol),
            existing,
            spec,
        )

        if missing.height == 0:
            latest_minute = self._reader.latest_minute(symbol)
            latest_existing = latest_bucket_start(existing)
            self._state_store.upsert_state(
                symbol=symbol,
                timeframe=spec.name,
                last_completed_bucket_start=latest_existing,
                last_seen_source_minute=latest_minute,
                status="backfill_idle",
            )
            return BackfillResult(0, 0, 0, 0)

        windows = _coalesce_windows(missing, spec)
        repaired = 0
        written_rows = 0
        scanned = missing.height
        skipped = 0
        existing_complete = set(existing.filter(pl.col("bucket_complete")).get_column("bucket_start").to_list())

        for window in windows:
            end_exclusive = add_bucket(window.end, spec)
            source = self._reader.read_window(symbol, window.start, end_exclusive - timedelta(minutes=1))
            aggregated = aggregate_minutes(source, spec, symbol)
            if not self._allow_incomplete_buckets:
                skipped += aggregated.filter(~pl.col("bucket_complete")).height
                aggregated = aggregated.filter(pl.col("bucket_complete"))

            if aggregated.height == 0:
                continue

            repaired += sum(
                1 for value in aggregated.get_column("bucket_start").to_list() if value in existing_complete
            )
            written_rows += self._writer.write_buckets(add_partition_columns(aggregated))

        latest_minute = self._reader.latest_minute(symbol)
        self._state_store.upsert_state(
            symbol=symbol,
            timeframe=spec.name,
            last_completed_bucket_start=latest_bucket_start(
                self._writer.scan_existing_index(symbol, spec.name).filter(pl.col("bucket_complete"))
            ),
            last_seen_source_minute=latest_minute,
            status="backfill_complete",
        )
        logger.info(
            "backfill complete timeframe=%s scanned=%s written=%s skipped_incomplete=%s repaired=%s",
            spec.name,
            scanned,
            written_rows,
            skipped,
            repaired,
        )
        return BackfillResult(scanned, written_rows, skipped, repaired)


def _coalesce_windows(bucket_starts: pl.DataFrame, spec: TimeframeSpec) -> list[MissingBucketWindow]:
    values = bucket_starts.get_column("bucket_start").to_list()
    if not values:
        return []

    windows: list[MissingBucketWindow] = []
    start = values[0]
    end = values[0]
    for value in values[1:]:
        if value == add_bucket(end, spec):
            end = value
            continue
        windows.append(MissingBucketWindow(start=start, end=end))
        start = value
        end = value
    windows.append(MissingBucketWindow(start=start, end=end))
    return windows
