from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from binance_minute_lake.core.config import Settings
from binance_minute_lake.core.enums import IngestionBand
from binance_minute_lake.core.time_utils import floor_to_hour, floor_to_minute, iter_hours, utc_now
from binance_minute_lake.sources.rest import BinanceRESTClient
from binance_minute_lake.sources.vision import VisionClient
from binance_minute_lake.sources.vision_loader import VisionLoader
from binance_minute_lake.sources.websocket import LiveCollector, LiveMinuteFeatures
from binance_minute_lake.state.store import SQLiteStateStore
from binance_minute_lake.transforms.minute_builder import MinuteTransformEngine
from binance_minute_lake.validation.dq import DataQualityError, DQValidator
from binance_minute_lake.validation.partition_audit import audit_hour_partition_file
from binance_minute_lake.writer.atomic import AtomicParquetWriter

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PipelineRunSummary:
    symbol: str
    target_horizon: datetime
    watermark_before: datetime
    watermark_after: datetime
    partitions_committed: int


@dataclass(frozen=True, slots=True)
class HourPartitionIssue:
    hour_start: datetime
    reason: str


@dataclass(frozen=True, slots=True)
class ConsistencyScanSummary:
    symbol: str
    start_minute: datetime
    end_minute: datetime
    hours_scanned: int
    issues: tuple[HourPartitionIssue, ...]


@dataclass(frozen=True, slots=True)
class ConsistencyBackfillSummary:
    symbol: str
    start_minute: datetime
    end_minute: datetime
    hours_scanned: int
    issues_found: int
    issues_targeted: int
    hours_repaired: int
    hours_failed: int
    issues_remaining: int


class MinuteIngestionPipeline:
    def __init__(
        self,
        settings: Settings,
        live_collector: LiveCollector | None = None,
    ) -> None:
        self._settings = settings
        self._state_store = SQLiteStateStore(settings.state_db)
        self._state_store.initialize()

        self._rest = BinanceRESTClient(
            base_url=settings.rest_base_url,
            timeout_seconds=settings.rest_timeout_seconds,
            retries=settings.rest_max_retries,
        )
        self._vision = VisionClient(base_url=settings.vision_base_url, timeout_seconds=20)
        self._vision_loader = VisionLoader(self._vision, cache_dir=settings.root_dir.parent / ".cache" / "vision")
        self._transform = MinuteTransformEngine(max_ffill_minutes=settings.max_ffill_minutes)
        self._writer = AtomicParquetWriter(settings.root_dir, self._state_store, DQValidator())
        self._live_collector = live_collector or LiveCollector()

    def close(self) -> None:
        self._rest.close()
        self._vision.close()

    @property
    def state_store(self) -> SQLiteStateStore:
        return self._state_store

    def rewind_watermark(self, minute_utc: datetime) -> None:
        self._state_store.upsert_watermark(
            self._settings.symbol,
            floor_to_minute(minute_utc.astimezone(UTC)),
        )

    def run_once(
        self,
        now: datetime | None = None,
        max_hours: int | None = None,
    ) -> PipelineRunSummary:
        now_utc = (now or utc_now()).astimezone(UTC)
        target_horizon = floor_to_minute(now_utc - timedelta(minutes=self._settings.safety_lag_minutes))
        return self.run_until_target(
            target_horizon=target_horizon,
            now_for_band=now_utc,
            max_hours=max_hours,
        )

    def run_until_target(
        self,
        target_horizon: datetime,
        now_for_band: datetime | None = None,
        max_hours: int | None = None,
    ) -> PipelineRunSummary:
        target_horizon_utc = floor_to_minute(target_horizon.astimezone(UTC))
        now_utc = (now_for_band or utc_now()).astimezone(UTC)

        watermark = self._state_store.get_watermark(self._settings.symbol)
        if watermark is None:
            watermark = target_horizon_utc - timedelta(minutes=self._settings.bootstrap_lookback_minutes + 1)
            self._state_store.upsert_watermark(self._settings.symbol, watermark)
            logger.info(
                "Initialized watermark",
                extra={
                    "symbol": self._settings.symbol,
                    "watermark": watermark.isoformat(),
                    "target_horizon": target_horizon_utc.isoformat(),
                },
            )

        if watermark >= target_horizon_utc:
            return PipelineRunSummary(
                symbol=self._settings.symbol,
                target_horizon=target_horizon_utc,
                watermark_before=watermark,
                watermark_after=watermark,
                partitions_committed=0,
            )

        missing_start = watermark + timedelta(minutes=1)
        capped_target = target_horizon_utc
        if max_hours is not None and max_hours > 0:
            max_end = missing_start + timedelta(hours=max_hours) - timedelta(minutes=1)
            capped_target = min(capped_target, max_end)

        partitions = 0
        current_watermark = watermark

        for hour_start in iter_hours(missing_start, capped_target):
            hour_end = hour_start + timedelta(minutes=59)
            window_start = max(missing_start, hour_start)
            window_end = min(capped_target, hour_end)
            band = self._choose_band(now_utc=now_utc, window_end=window_end)

            frame = self._collect_and_transform(window_start, window_end, band)
            if frame.height == 0:
                raise DataQualityError(
                    f"No rows produced for window {window_start.isoformat()}..{window_end.isoformat()}"
                )

            self._writer.write_hour_partition(symbol=self._settings.symbol, hour_start=hour_start, frame=frame)
            current_watermark = window_end
            self._state_store.upsert_watermark(self._settings.symbol, current_watermark)
            partitions += 1

        return PipelineRunSummary(
            symbol=self._settings.symbol,
            target_horizon=capped_target,
            watermark_before=watermark,
            watermark_after=current_watermark,
            partitions_committed=partitions,
        )

    def run_daemon(self, poll_seconds: int = 60) -> None:
        logger.info("Starting minute ingestion daemon", extra={"poll_seconds": poll_seconds})
        while True:
            try:
                summary = self.run_once()
                logger.info(
                    "Poll tick complete",
                    extra={
                        "symbol": summary.symbol,
                        "partitions_committed": summary.partitions_committed,
                        "watermark_before": summary.watermark_before.isoformat(),
                        "watermark_after": summary.watermark_after.isoformat(),
                        "target_horizon": summary.target_horizon.isoformat(),
                    },
                )
            except Exception:
                logger.exception("Poll tick failed")
            time.sleep(poll_seconds)

    def scan_partition_consistency(
        self,
        start: datetime,
        end: datetime,
    ) -> ConsistencyScanSummary:
        start_utc = floor_to_minute(start.astimezone(UTC))
        end_utc = floor_to_minute(end.astimezone(UTC))
        if end_utc < start_utc:
            raise ValueError("end must be >= start")

        issues: list[HourPartitionIssue] = []
        hours = iter_hours(start_utc, end_utc)

        for hour_start in hours:
            window_start = max(start_utc, hour_start)
            window_end = min(end_utc, hour_start + timedelta(minutes=59))
            partition_path = self._partition_output_path(self._settings.symbol, hour_start)

            audit_result = audit_hour_partition_file(
                path=partition_path,
                expected_start=window_start,
                expected_end=window_end,
            )
            if not audit_result.is_valid:
                issues.append(HourPartitionIssue(hour_start=hour_start, reason=audit_result.reason))

        return ConsistencyScanSummary(
            symbol=self._settings.symbol,
            start_minute=start_utc,
            end_minute=end_utc,
            hours_scanned=len(hours),
            issues=tuple(issues),
        )

    def run_consistency_backfill(
        self,
        start: datetime,
        end: datetime,
        *,
        now_for_band: datetime | None = None,
        sleep_seconds: float = 0.0,
        max_missing_hours: int | None = None,
    ) -> ConsistencyBackfillSummary:
        scan = self.scan_partition_consistency(start=start, end=end)
        issues = list(scan.issues)

        if max_missing_hours is not None and max_missing_hours > 0:
            target_issues = issues[:max_missing_hours]
        else:
            target_issues = issues

        now_utc = (now_for_band or utc_now()).astimezone(UTC)
        repaired = 0
        failed = 0

        for index, issue in enumerate(target_issues):
            window_start = max(scan.start_minute, issue.hour_start)
            window_end = min(scan.end_minute, issue.hour_start + timedelta(minutes=59))
            band = self._choose_band(now_utc=now_utc, window_end=window_end)

            try:
                frame = self._collect_and_transform(window_start, window_end, band)
                expected_rows = int((window_end - window_start).total_seconds() // 60) + 1
                if frame.height != expected_rows:
                    raise DataQualityError(
                        "Unexpected row count for repaired partition: "
                        f"expected={expected_rows}, actual={frame.height}, "
                        f"window={window_start.isoformat()}..{window_end.isoformat()}"
                    )
                self._writer.write_hour_partition(
                    symbol=self._settings.symbol,
                    hour_start=issue.hour_start,
                    frame=frame,
                )
                repaired += 1
            except Exception:
                failed += 1
                logger.exception(
                    "Consistency repair failed",
                    extra={
                        "symbol": self._settings.symbol,
                        "hour_start": issue.hour_start.isoformat(),
                        "reason": issue.reason,
                    },
                )

            if sleep_seconds > 0 and index < len(target_issues) - 1:
                time.sleep(sleep_seconds)

        if max_missing_hours is None:
            remaining_scan = self.scan_partition_consistency(start=start, end=end)
            issues_remaining = len(remaining_scan.issues)
        else:
            issues_remaining = max(len(scan.issues) - repaired, 0)

        return ConsistencyBackfillSummary(
            symbol=self._settings.symbol,
            start_minute=scan.start_minute,
            end_minute=scan.end_minute,
            hours_scanned=scan.hours_scanned,
            issues_found=len(scan.issues),
            issues_targeted=len(target_issues),
            hours_repaired=repaired,
            hours_failed=failed,
            issues_remaining=issues_remaining,
        )

    def _collect_and_transform(
        self,
        window_start: datetime,
        window_end: datetime,
        band: IngestionBand,
    ) -> pl.DataFrame:
        window_end_inclusive = window_end + timedelta(minutes=1)

        if band == IngestionBand.COLD:
            self._log_vision_availability(window_start.date())
            klines = self._vision_loader.load_klines(self._settings.symbol, window_start, window_end_inclusive)
            mark_klines = self._vision_loader.load_mark_price_klines(
                self._settings.symbol, window_start, window_end_inclusive
            )
            index_klines = self._vision_loader.load_index_price_klines(
                self._settings.symbol, window_start, window_end_inclusive
            )
            agg_trades = self._vision_loader.load_agg_trades(self._settings.symbol, window_start, window_end_inclusive)
            book_ticker_snapshots = self._vision_loader.load_book_ticker(
                self._settings.symbol, window_start, window_end_inclusive
            )
            metrics_rows = self._vision_loader.load_metrics(self._settings.symbol, window_start, window_end_inclusive)
            premium_snapshot = self._rest.fetch_premium_index(self._settings.symbol)
            if not klines:
                klines = self._rest.fetch_klines(self._settings.symbol, window_start, window_end_inclusive)
            if not agg_trades:
                agg_trades = self._fetch_agg_trades_paginated(window_start, window_end_inclusive)
            if not mark_klines:
                mark_klines = self._rest.fetch_mark_price_klines(
                    self._settings.symbol, window_start, window_end_inclusive
                )
            if not index_klines:
                index_klines = self._rest.fetch_index_price_klines(
                    self._settings.symbol, window_start, window_end_inclusive
                )
        else:
            klines = self._rest.fetch_klines(self._settings.symbol, window_start, window_end_inclusive)
            mark_klines = self._rest.fetch_mark_price_klines(
                self._settings.symbol, window_start, window_end_inclusive
            )
            index_klines = self._rest.fetch_index_price_klines(
                self._settings.symbol, window_start, window_end_inclusive
            )
            agg_trades = self._fetch_agg_trades_paginated(window_start, window_end_inclusive)
            # Use a trailing bookTicker snapshot to backfill spread/imbalance for hot/warm minutes.
            ticker_snapshot = self._rest.fetch_book_ticker(self._settings.symbol)
            ticker_snapshot["event_time"] = int(window_end_inclusive.timestamp() * 1000)
            book_ticker_snapshots = [ticker_snapshot]
            metrics_rows = []  # optional: could add openInterest snapshots here if needed
            premium_snapshot = self._rest.fetch_premium_index(self._settings.symbol)

        funding_rates = self._rest.fetch_funding_rate(
            self._settings.symbol,
            start_time=window_start - timedelta(hours=8),
            end_time=window_end_inclusive,
            limit=1000,
        )
        live_points = self._live_points(window_start, window_end)

        return self._transform.build_canonical_frame(
            start_minute=window_start,
            end_minute=window_end,
            klines=klines,
            mark_price_klines=mark_klines,
            index_price_klines=index_klines,
            agg_trades=agg_trades,
            funding_rates=funding_rates,
            book_ticker_snapshots=book_ticker_snapshots,
            premium_index_snapshots=[premium_snapshot],
            metrics_rows=metrics_rows,
            live_features=live_points,
        )

    def _fetch_agg_trades_paginated(
        self,
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, object]]:
        all_rows: list[dict[str, object]] = []
        cursor = window_start

        for _ in range(10_000):
            batch = self._rest.fetch_agg_trades(
                symbol=self._settings.symbol,
                start_time=cursor,
                end_time=window_end,
                limit=1000,
            )
            if not batch:
                break

            all_rows.extend(batch)
            last_transact_ms = int(batch[-1]["transact_time"])
            next_cursor = datetime.fromtimestamp(last_transact_ms / 1000, tz=UTC) + timedelta(milliseconds=1)
            if next_cursor >= window_end:
                break
            if next_cursor <= cursor:
                break
            cursor = next_cursor

            if len(batch) < 1000:
                break
            # soften burst rate against REST 429 limits
            time.sleep(0.05)

        return all_rows

    def _live_points(self, start: datetime, end: datetime) -> list[LiveMinuteFeatures]:
        points = []
        cursor = start
        while cursor <= end:
            snapshot = self._live_collector.snapshot_for_minute(int(cursor.timestamp() * 1000))
            if snapshot is not None:
                points.append(snapshot)
            cursor += timedelta(minutes=1)
        return points

    def _log_vision_availability(self, trade_date: date) -> None:
        required_streams = ["klines", "markPriceKlines", "indexPriceKlines", "aggTrades"]
        statuses = [
            self._vision.object_status(stream=stream, symbol=self._settings.symbol, trade_date=trade_date)
            for stream in required_streams
        ]
        missing = [item.stream for item in statuses if not item.exists]
        if missing:
            logger.warning(
                "Cold path Vision object(s) missing; falling back to REST",
                extra={"symbol": self._settings.symbol, "date": trade_date.isoformat(), "missing": missing},
            )

    @staticmethod
    def _choose_band(now_utc: datetime, window_end: datetime) -> IngestionBand:
        age = now_utc - window_end
        if age <= timedelta(hours=6):
            return IngestionBand.HOT
        if age <= timedelta(days=7):
            return IngestionBand.WARM
        return IngestionBand.COLD

    def hour_start_for(self, value: datetime) -> datetime:
        return floor_to_hour(value)

    def _partition_output_path(self, symbol: str, hour_start: datetime) -> Path:
        symbol_upper = symbol.upper()
        return (
            self._settings.root_dir
            / "futures"
            / "um"
            / "minute"
            / f"symbol={symbol_upper}"
            / f"year={hour_start:%Y}"
            / f"month={hour_start:%m}"
            / f"day={hour_start:%d}"
            / f"hour={hour_start:%H}"
            / "part.parquet"
        )
