from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TypeVar

import polars as pl

from binance_minute_lake.core.time_utils import floor_to_minute, utc_now
from binance_minute_lake.sources.rest import BinanceRESTClient
from binance_minute_lake.sources.vision import VisionClient
from binance_minute_lake.sources.vision_loader import VisionLoader
from binance_minute_lake.sources.websocket import InMemoryLiveCollector, LiveMinuteFeatures
from binance_minute_lake.transforms.minute_builder import MinuteTransformEngine

from .utils import empty_canonical_frame

LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


class BinanceCanonicalMinuteProvider:
    def __init__(
        self,
        *,
        root_dir: Path,
        rest_base_url: str,
        vision_base_url: str,
        max_ffill_minutes: int,
        rest_timeout_seconds: int,
        rest_max_retries: int,
    ) -> None:
        self._max_ffill_minutes = max_ffill_minutes
        # The API is synchronous, so fail fast enough for upstream callers to handle degraded data.
        self._rest = BinanceRESTClient(
            base_url=rest_base_url,
            timeout_seconds=rest_timeout_seconds,
            retries=max(1, min(rest_max_retries, 2)),
        )
        self._vision = VisionClient(base_url=vision_base_url, timeout_seconds=rest_timeout_seconds)
        self._vision_loader = VisionLoader(self._vision, cache_dir=root_dir.parent / ".cache" / "vision_api")
        self._transform = MinuteTransformEngine(max_ffill_minutes=max_ffill_minutes)

    def close(self) -> None:
        self._rest.close()
        self._vision.close()

    def _best_effort_optional_enrichment(
        self,
        *,
        symbol: str,
        enrichment: str,
        fetch: Callable[[], T],
        default: T,
    ) -> T:
        try:
            return fetch()
        except Exception as exc:
            LOGGER.warning(
                "Optional Binance enrichment failed",
                extra={"symbol": symbol, "enrichment": enrichment, "error": str(exc)},
            )
            return default

    def build_canonical_minutes(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        live_collector: InMemoryLiveCollector | None = None,
    ) -> pl.DataFrame:
        start_utc = floor_to_minute(start_time.astimezone(UTC))
        end_utc = floor_to_minute(end_time.astimezone(UTC))
        if end_utc < start_utc:
            return empty_canonical_frame()

        today_start_utc = floor_to_minute(utc_now()).replace(hour=0, minute=0)
        historical_end = min(end_utc, today_start_utc - timedelta(minutes=1))

        klines: list[dict[str, object]] = []
        mark_price_klines: list[dict[str, object]] = []
        index_price_klines: list[dict[str, object]] = []
        agg_trades: list[dict[str, object]] = []
        book_ticker_snapshots: list[dict[str, object]] = []
        metrics_rows: list[dict[str, object]] = []
        premium_snapshots: list[dict[str, object]] = []

        if start_utc <= historical_end:
            vision_end_inclusive = historical_end + timedelta(minutes=1)
            metrics_start = start_utc - timedelta(minutes=self._max_ffill_minutes)
            klines.extend(self._vision_loader.load_klines(symbol, start_utc, vision_end_inclusive))
            mark_price_klines.extend(
                self._vision_loader.load_mark_price_klines(symbol, start_utc, vision_end_inclusive)
            )
            index_price_klines.extend(
                self._vision_loader.load_index_price_klines(symbol, start_utc, vision_end_inclusive)
            )
            agg_trades.extend(self._vision_loader.load_agg_trades(symbol, start_utc, vision_end_inclusive))
            book_ticker_snapshots.extend(self._vision_loader.load_book_ticker(symbol, start_utc, vision_end_inclusive))
            metrics_rows.extend(self._vision_loader.load_metrics(symbol, metrics_start, vision_end_inclusive))

        rest_start = max(start_utc, today_start_utc)
        if rest_start <= end_utc:
            rest_end_inclusive = end_utc + timedelta(minutes=1)
            anchor_ms = int(rest_start.timestamp() * 1000)

            klines.extend(self._paginate_klines(symbol, rest_start, rest_end_inclusive))
            mark_price_klines.extend(self._paginate_mark_price_klines(symbol, rest_start, rest_end_inclusive))
            index_price_klines.extend(self._paginate_index_price_klines(symbol, rest_start, rest_end_inclusive))
            agg_trades.extend(
                self._best_effort_optional_enrichment(
                    symbol=symbol,
                    enrichment="agg_trades",
                    fetch=lambda: self._paginate_agg_trades(symbol, rest_start, rest_end_inclusive),
                    default=[],
                )
            )

            book_ticker = self._best_effort_optional_enrichment(
                symbol=symbol,
                enrichment="book_ticker",
                fetch=lambda: self._rest.fetch_book_ticker(symbol),
                default=None,
            )
            if book_ticker is not None:
                book_ticker["event_time"] = anchor_ms
                book_ticker_snapshots.append(book_ticker)

            premium_snapshot = self._best_effort_optional_enrichment(
                symbol=symbol,
                enrichment="premium_index",
                fetch=lambda: self._rest.fetch_premium_index(symbol),
                default=None,
            )
            if premium_snapshot is not None:
                premium_snapshot["event_time"] = anchor_ms
                premium_snapshots.append(premium_snapshot)

            # Fetch historical OI series (5m granularity) so OI moves per bar instead of
            # being a single snapshot pinned to the start of the window.
            oi_hist = self._best_effort_optional_enrichment(
                symbol=symbol,
                enrichment="open_interest_hist",
                fetch=lambda: self._paginate_oi_hist(symbol, rest_start - timedelta(minutes=5), rest_end_inclusive),
                default=[],
            )
            if oi_hist:
                metrics_rows.extend(oi_hist)
            else:
                # Fall back to single snapshot if hist endpoint unavailable.
                open_interest = self._best_effort_optional_enrichment(
                    symbol=symbol,
                    enrichment="open_interest",
                    fetch=lambda: self._rest.fetch_open_interest(symbol),
                    default=None,
                )
                if open_interest is not None:
                    mark_price = None
                    if premium_snapshot is not None and premium_snapshot.get("mark_price") is not None:
                        mark_price = float(premium_snapshot["mark_price"])
                    oi_contracts = float(open_interest["open_interest"])
                    metrics_rows.append(
                        {
                            "create_time": anchor_ms,
                            "oi_contracts": oi_contracts,
                            "oi_value_usdt": oi_contracts * mark_price if mark_price is not None else None,
                        }
                    )

        metrics_rows = self._seed_metrics_for_window(metrics_rows, start_utc, end_utc)
        funding_rates = self._seed_funding_rates_for_window(
            self._best_effort_optional_enrichment(
                symbol=symbol,
                enrichment="funding_rate",
                fetch=lambda: self._rest.fetch_funding_rate(
                    symbol,
                    start_time=start_utc - timedelta(hours=8),
                    end_time=end_utc + timedelta(minutes=1),
                    limit=1000,
                ),
                default=[],
            ),
            start_utc,
            end_utc,
        )
        top_trader_ratio_rows = self._best_effort_optional_enrichment(
            symbol=symbol,
            enrichment="top_trader_ls_ratio",
            fetch=lambda: self._paginate_ls_ratio(
                lambda cursor, end: self._rest.fetch_top_trader_long_short_account_ratio(
                    symbol,
                    period="5m",
                    start_time=cursor,
                    end_time=end,
                    limit=500,
                ),
                start_utc - timedelta(minutes=30),
                end_utc + timedelta(minutes=1),
            ),
            default=[],
        )
        global_ratio_rows = self._best_effort_optional_enrichment(
            symbol=symbol,
            enrichment="global_ls_ratio",
            fetch=lambda: self._paginate_ls_ratio(
                lambda cursor, end: self._rest.fetch_global_long_short_account_ratio(
                    symbol,
                    period="5m",
                    start_time=cursor,
                    end_time=end,
                    limit=500,
                ),
                start_utc - timedelta(minutes=30),
                end_utc + timedelta(minutes=1),
            ),
            default=[],
        )

        live_features = self._collect_live_features(live_collector, start_utc, end_utc) if live_collector else []
        return self._transform.build_canonical_frame(
            start_minute=start_utc,
            end_minute=end_utc,
            klines=klines,
            mark_price_klines=mark_price_klines,
            index_price_klines=index_price_klines,
            agg_trades=agg_trades,
            funding_rates=funding_rates,
            book_ticker_snapshots=book_ticker_snapshots,
            premium_index_snapshots=premium_snapshots,
            metrics_rows=metrics_rows,
            top_trader_ratio_rows=top_trader_ratio_rows,
            global_ratio_rows=global_ratio_rows,
            live_features=live_features,
        )

    def fetch_native_candles(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        interval: str,
        limit: int,
    ) -> list[dict[str, object]]:
        """Fetch Binance native candle bars without expanding through 1m."""
        return self._paginate_native_klines(
            symbol,
            floor_to_minute(start_time.astimezone(UTC)),
            floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            interval=interval,
            limit=limit,
        )

    def fetch_native_open_interest_hist(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_open_interest_hist(
            symbol,
            period=period,
            start_time=floor_to_minute(start_time.astimezone(UTC)),
            end_time=floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            limit=min(max(limit, 1), 500),
        )

    def fetch_native_mark_price_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        interval: str,
        limit: int,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_mark_price_klines(
            symbol,
            floor_to_minute(start_time.astimezone(UTC)),
            floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            interval=interval,
            limit=min(max(limit, 1), 1500),
        )

    def fetch_native_index_price_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        interval: str,
        limit: int,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_index_price_klines(
            symbol,
            floor_to_minute(start_time.astimezone(UTC)),
            floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            interval=interval,
            limit=min(max(limit, 1), 1500),
        )

    def fetch_native_premium_index_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        interval: str,
        limit: int,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_premium_index_klines(
            symbol,
            floor_to_minute(start_time.astimezone(UTC)),
            floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            interval=interval,
            limit=min(max(limit, 1), 1500),
        )

    def fetch_native_global_long_short_account_ratio(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_global_long_short_account_ratio(
            symbol,
            period=period,
            start_time=floor_to_minute(start_time.astimezone(UTC)),
            end_time=floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            limit=min(max(limit, 1), 500),
        )

    def fetch_native_top_trader_long_short_account_ratio(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_top_trader_long_short_account_ratio(
            symbol,
            period=period,
            start_time=floor_to_minute(start_time.astimezone(UTC)),
            end_time=floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            limit=min(max(limit, 1), 500),
        )

    def fetch_native_top_trader_long_short_position_ratio(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        period: str,
        limit: int,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_top_trader_long_short_position_ratio(
            symbol,
            period=period,
            start_time=floor_to_minute(start_time.astimezone(UTC)),
            end_time=floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            limit=min(max(limit, 1), 500),
        )

    def fetch_native_funding_rate(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        limit: int = 1000,
    ) -> list[dict[str, object]]:
        return self._rest.fetch_funding_rate(
            symbol,
            start_time=floor_to_minute(start_time.astimezone(UTC)),
            end_time=floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            limit=min(max(limit, 1), 1000),
        )

    def fetch_native_premium_index_snapshot(self, symbol: str) -> dict[str, object]:
        return self._rest.fetch_premium_index(symbol)

    @staticmethod
    def _collect_live_features(
        collector: InMemoryLiveCollector,
        start_utc: datetime,
        end_utc: datetime,
    ) -> list[LiveMinuteFeatures]:
        features: list[LiveMinuteFeatures] = []
        cursor = start_utc
        while cursor <= end_utc:
            snap = collector.snapshot_for_minute(int(cursor.timestamp() * 1000))
            if snap is not None:
                features.append(snap)
            cursor += timedelta(minutes=1)
        return features

    @staticmethod
    def _seed_funding_rates_for_window(
        funding_rates: list[dict[str, object]],
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, object]]:
        if not funding_rates:
            return funding_rates

        window_start_ms = int(window_start.timestamp() * 1000)
        window_end_ms = int(window_end.timestamp() * 1000)
        if any(window_start_ms <= int(item["funding_time"]) <= window_end_ms for item in funding_rates):
            return funding_rates

        latest_before_end: dict[str, object] | None = None
        for row in sorted(funding_rates, key=lambda item: int(item["funding_time"])):
            if int(row["funding_time"]) <= window_end_ms:
                latest_before_end = row

        if latest_before_end is None:
            return funding_rates

        seeded = dict(latest_before_end)
        seeded["funding_time"] = window_start_ms
        return [seeded, *funding_rates]

    @staticmethod
    def _seed_metrics_for_window(
        metrics_rows: list[dict[str, object]],
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict[str, object]]:
        if not metrics_rows:
            return metrics_rows

        window_start_ms = int(window_start.timestamp() * 1000)
        window_end_ms = int(window_end.timestamp() * 1000)
        if any(window_start_ms <= int(item.get("create_time", -1)) <= window_end_ms for item in metrics_rows):
            return metrics_rows

        latest_before_end: dict[str, object] | None = None
        for row in sorted(metrics_rows, key=lambda item: int(item.get("create_time", -1))):
            if int(row.get("create_time", -1)) <= window_end_ms:
                latest_before_end = row

        if latest_before_end is None:
            return metrics_rows

        seeded = dict(latest_before_end)
        seeded["create_time"] = window_start_ms
        return [seeded, *metrics_rows]

    def _paginate_oi_hist(self, symbol: str, start_time: datetime, end_time: datetime) -> list[dict[str, object]]:
        """Paginate /futures/data/openInterestHist at 5m granularity to get real OI movement."""
        rows: list[dict[str, object]] = []
        cursor = start_time
        for _ in range(200):
            batch = self._rest.fetch_open_interest_hist(
                symbol, period="5m", start_time=cursor, end_time=end_time, limit=500
            )
            if not batch:
                break
            rows.extend(batch)
            last_ts = int(batch[-1]["create_time"])
            next_cursor = datetime.fromtimestamp(last_ts / 1000, tz=UTC) + timedelta(minutes=5)
            if next_cursor <= cursor or next_cursor >= end_time:
                break
            cursor = next_cursor
            if len(batch) < 500:
                break
        return rows

    def _paginate_klines(self, symbol: str, start_time: datetime, end_time: datetime) -> list[dict[str, object]]:
        return self._paginate_open_time(
            lambda cursor, limit: self._rest.fetch_klines(symbol, cursor, end_time, interval="1m", limit=limit),
            start_time,
            end_time,
            page_limit=1500,
        )

    @staticmethod
    def _interval_minutes(interval: str) -> int:
        if interval.endswith("m"):
            return int(interval[:-1])
        if interval.endswith("h"):
            return int(interval[:-1]) * 60
        if interval.endswith("d"):
            return int(interval[:-1]) * 1440
        raise ValueError(f"Unsupported Binance interval '{interval}'")

    def _paginate_native_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        interval: str,
        limit: int,
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        cursor = start_time
        interval_minutes = self._interval_minutes(interval)
        page_limit = min(max(limit, 1), 1500)
        for _ in range(10_000):
            batch = self._rest.fetch_klines(symbol, cursor, end_time, interval=interval, limit=page_limit)
            if not batch:
                break
            rows.extend(batch)
            if len(rows) >= limit:
                break
            last_open_time = int(batch[-1]["open_time"])
            next_cursor = datetime.fromtimestamp(last_open_time / 1000, tz=UTC) + timedelta(minutes=interval_minutes)
            if next_cursor <= cursor or next_cursor >= end_time:
                break
            cursor = next_cursor
            if len(batch) < page_limit:
                break
        return rows[-limit:]

    def _paginate_mark_price_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, object]]:
        return self._paginate_open_time(
            lambda cursor, limit: self._rest.fetch_mark_price_klines(
                symbol,
                cursor,
                end_time,
                interval="1m",
                limit=limit,
            ),
            start_time,
            end_time,
            page_limit=1500,
        )

    def _paginate_index_price_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, object]]:
        return self._paginate_open_time(
            lambda cursor, limit: self._rest.fetch_index_price_klines(
                symbol,
                cursor,
                end_time,
                interval="1m",
                limit=limit,
            ),
            start_time,
            end_time,
            page_limit=1500,
        )

    def _paginate_open_time(
        self,
        fetch_page: Callable[[datetime, int], list[dict[str, object]]],
        start_time: datetime,
        end_time: datetime,
        *,
        page_limit: int,
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        cursor = start_time
        for _ in range(10_000):
            batch = fetch_page(cursor, page_limit)
            if not batch:
                break
            rows.extend(batch)
            last_open_time = int(batch[-1]["open_time"])
            next_cursor = datetime.fromtimestamp(last_open_time / 1000, tz=UTC) + timedelta(minutes=1)
            if next_cursor <= cursor or next_cursor >= end_time:
                break
            cursor = next_cursor
            if len(batch) < page_limit:
                break
        return rows

    def _paginate_agg_trades(self, symbol: str, start_time: datetime, end_time: datetime) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        cursor = start_time
        for _ in range(10_000):
            batch = self._rest.fetch_agg_trades(symbol, cursor, end_time, limit=1000)
            if not batch:
                break
            rows.extend(batch)
            last_transact_time = int(batch[-1]["transact_time"])
            next_cursor = datetime.fromtimestamp(last_transact_time / 1000, tz=UTC) + timedelta(milliseconds=1)
            if next_cursor <= cursor or next_cursor >= end_time:
                break
            cursor = next_cursor
            if len(batch) < 1000:
                break
        return rows

    def _paginate_ls_ratio(
        self,
        fetch_page: Callable[[datetime, datetime], list[dict[str, object]]],
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        cursor = start_time
        for _ in range(1_000):
            batch = fetch_page(cursor, end_time)
            if not batch:
                break
            rows.extend(batch)
            last_data_time = int(batch[-1]["data_time"])
            next_cursor = datetime.fromtimestamp(last_data_time / 1000, tz=UTC) + timedelta(milliseconds=1)
            if next_cursor <= cursor or next_cursor >= end_time:
                break
            cursor = next_cursor
            if len(batch) < 500:
                break
        if not rows:
            return []
        deduped = (
            pl.DataFrame(rows)
            .sort("data_time")
            .unique(subset=["data_time"], keep="last")
            .to_dicts()
        )
        return deduped
