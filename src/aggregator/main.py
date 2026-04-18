from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass

from .backfill import BackfillResult, BackfillRunner
from .bucketing import TimeframeSpec, parse_timeframe
from .config import AggregatorSettings
from .incremental import IncrementalResult, IncrementalRunner
from .source_reader import MinuteLakeReader
from .state_store import AggregatorStateStore
from .target_writer import HigherTimeframeWriter


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


@dataclass(frozen=True, slots=True)
class AggregationService:
    settings: AggregatorSettings
    reader: MinuteLakeReader
    writer: HigherTimeframeWriter
    state_store: AggregatorStateStore

    @classmethod
    def from_settings(cls, settings: AggregatorSettings | None = None) -> AggregationService:
        resolved = settings or AggregatorSettings()
        _configure_logging(resolved.log_level)
        reader = MinuteLakeReader(resolved.minute_lake_root)
        writer = HigherTimeframeWriter(resolved.target_root)
        state_store = AggregatorStateStore(resolved.state_db)
        state_store.initialize()
        return cls(settings=resolved, reader=reader, writer=writer, state_store=state_store)

    def run_startup(self) -> dict[str, BackfillResult]:
        logger = logging.getLogger(__name__)
        source_min, source_max = self.reader.inspect_range(self.settings.symbol)
        logger.info(
            "source minute lake inspected symbol=%s min=%s max=%s source_root=%s target_root=%s",
            self.settings.symbol,
            source_min,
            source_max,
            self.settings.minute_lake_root,
            self.settings.target_root,
        )
        runner = BackfillRunner(
            self.reader,
            self.writer,
            self.state_store,
            allow_incomplete_buckets=self.settings.allow_incomplete_buckets,
        )
        results: dict[str, BackfillResult] = {}
        for spec in self._timeframe_specs():
            results[spec.name] = runner.run_for_timeframe(self.settings.symbol, spec)
        return results

    def run_continuous_once(self) -> dict[str, IncrementalResult]:
        runner = IncrementalRunner(
            self.reader,
            self.writer,
            self.state_store,
            allow_incomplete_buckets=self.settings.allow_incomplete_buckets,
            repair_lookback_minutes=self.settings.repair_lookback_minutes,
        )
        results: dict[str, IncrementalResult] = {}
        for spec in self._timeframe_specs():
            results[spec.name] = runner.run_once(self.settings.symbol, spec)
        return results

    def run_forever(self) -> None:
        self.run_startup()
        logger = logging.getLogger(__name__)
        logger.info(
            "entering continuous mode symbol=%s poll_interval_seconds=%s",
            self.settings.symbol,
            self.settings.poll_interval_seconds,
        )
        while True:
            self.run_continuous_once()
            time.sleep(self.settings.poll_interval_seconds)

    def _timeframe_specs(self) -> list[TimeframeSpec]:
        return [parse_timeframe(value) for value in self.settings.timeframes]


def run() -> None:
    parser = argparse.ArgumentParser(description="Higher-timeframe aggregation service")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run startup backfill and one incremental pass, then exit.",
    )
    parser.add_argument(
        "--startup-only",
        action="store_true",
        help="Run startup backfill only, then exit.",
    )
    args = parser.parse_args()

    service = AggregationService.from_settings()
    if args.startup_only:
        service.run_startup()
        return
    if args.once:
        service.run_startup()
        service.run_continuous_once()
        return
    service.run_forever()


if __name__ == "__main__":
    run()
