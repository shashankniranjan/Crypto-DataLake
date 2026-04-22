from __future__ import annotations

import logging
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from binance_minute_lake.core.time_utils import floor_to_minute, utc_now
from binance_minute_lake.sources.rest import BinanceRESTClient
from binance_minute_lake.sources.vision import VisionClient
from binance_minute_lake.sources.vision_loader import VisionLoader
from binance_minute_lake.sources.websocket import InMemoryLiveCollector, LiveMinuteFeatures
from binance_minute_lake.transforms.minute_builder import MinuteTransformEngine

from .utils import empty_canonical_frame

LOGGER = logging.getLogger(__name__)

# Independent REST fetch groups — each gets its own BinanceRESTClient so rate
# limiters don't serialize each other.  8 groups cover all FAPI endpoints used
# to build a canonical minute frame.
_NUM_CLIENTS = 8
_VISION_CANONICAL_STREAMS = (
    "klines",
    "markPriceKlines",
    "indexPriceKlines",
    "aggTrades",
    "bookTicker",
    "metrics",
)


class ParallelLiveBinanceProvider:
    """Parallel Binance data provider for the live API service.

    Fires all independent FAPI endpoint groups concurrently by giving each
    group its own BinanceRESTClient instance (and therefore its own rate
    limiter).  For typical live windows (< 1 500 minutes, single page per
    endpoint) this cuts the REST-fetch phase from ~1.5-2.5 s down to
    ~200-500 ms - the slowest parallel group, not the sum of all groups.

    Parallel groups
    ---------------
    0  klines (paginated)
    1  markPriceKlines (paginated)
    2  indexPriceKlines (paginated)
    3  aggTrades (paginated, best-effort)
    4  bookTicker + premiumIndex + openInterest (3 fast single calls)
    5  openInterestHist (paginated, best-effort)
    6  topLongShortAccountRatio (paginated, best-effort)
    7  globalLongShortAccountRatio + fundingRate (2 sequential calls)

    The minute data lake ingestion (BinanceCanonicalMinuteProvider) is
    completely unaffected — this class is only wired into the API service.
    """

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
        retries = max(1, min(rest_max_retries, 2))
        self._clients = [
            BinanceRESTClient(
                base_url=rest_base_url,
                timeout_seconds=rest_timeout_seconds,
                retries=retries,
            )
            for _ in range(_NUM_CLIENTS)
        ]
        self._vision = VisionClient(base_url=vision_base_url, timeout_seconds=rest_timeout_seconds)
        self._vision_loader = VisionLoader(
            self._vision,
            cache_dir=root_dir.parent / ".cache" / "vision_api",
        )
        self._transform = MinuteTransformEngine(max_ffill_minutes=max_ffill_minutes)

    def close(self) -> None:
        for client in self._clients:
            client.close()
        self._vision.close()

    # ------------------------------------------------------------------ #
    # Public interface (same signature as BinanceCanonicalMinuteProvider) #
    # ------------------------------------------------------------------ #

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

        klines: list[dict] = []
        mark_price_klines: list[dict] = []
        index_price_klines: list[dict] = []
        agg_trades: list[dict] = []
        book_ticker_snapshots: list[dict] = []
        metrics_rows: list[dict] = []
        premium_snapshots: list[dict] = []
        funding_rates_raw: list[dict] = []
        top_trader_ratio_rows: list[dict] = []
        global_ratio_rows: list[dict] = []

        # Historical path — Vision API responses are cached on disk so this is
        # already fast; keep it synchronous to avoid complicating the logic.
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
            book_ticker_snapshots.extend(
                self._vision_loader.load_book_ticker(symbol, start_utc, vision_end_inclusive)
            )
            metrics_rows.extend(self._vision_loader.load_metrics(symbol, metrics_start, vision_end_inclusive))

        # REST path — fire all groups in parallel.
        rest_start = max(start_utc, today_start_utc)
        if rest_start <= end_utc:
            anchor_ms = int(rest_start.timestamp() * 1000)
            rest_end_inclusive = end_utc + timedelta(minutes=1)
            rest = self._fetch_rest_parallel(symbol, rest_start, rest_end_inclusive, start_utc, end_utc)

            klines.extend(rest["klines"])
            mark_price_klines.extend(rest["mark_price_klines"])
            index_price_klines.extend(rest["index_price_klines"])
            agg_trades.extend(rest["agg_trades"])

            bt = rest["book_ticker"]
            if bt is not None:
                bt["event_time"] = anchor_ms
                book_ticker_snapshots.append(bt)

            pi = rest["premium_index"]
            if pi is not None:
                pi = dict(pi)
                pi["event_time"] = anchor_ms
                premium_snapshots.append(pi)

            oi_hist = rest["oi_hist"]
            if oi_hist:
                metrics_rows.extend(oi_hist)
            else:
                oi = rest["open_interest"]
                if oi is not None:
                    mark_price = (
                        float(pi["mark_price"]) if pi is not None and pi.get("mark_price") is not None else None
                    )
                    oi_contracts = float(oi["open_interest"])
                    metrics_rows.append(
                        {
                            "create_time": anchor_ms,
                            "oi_contracts": oi_contracts,
                            "oi_value_usdt": oi_contracts * mark_price if mark_price is not None else None,
                        }
                    )

            funding_rates_raw = rest["funding_rates"]
            top_trader_ratio_rows = rest["top_trader_ratio"]
            global_ratio_rows = rest["global_ratio"]

        metrics_rows = self._seed_metrics_for_window(metrics_rows, start_utc, end_utc)
        funding_rates = self._seed_funding_rates_for_window(funding_rates_raw, start_utc, end_utc)
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
    ) -> list[dict]:
        """Fetch Binance native candle bars without expanding through 1m.

        This is intentionally candle-only. Depth/liquidation/tick-derived
        historical fields are not available as simple Binance REST bars and
        must continue to come from local capture or the legacy 1m path.
        """
        return self._paginate_native_klines(
            self._clients[0],
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
    ) -> list[dict]:
        return self._clients[5].fetch_open_interest_hist(
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
    ) -> list[dict]:
        return self._clients[1].fetch_mark_price_klines(
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
    ) -> list[dict]:
        return self._clients[2].fetch_index_price_klines(
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
    ) -> list[dict]:
        return self._clients[4].fetch_premium_index_klines(
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
    ) -> list[dict]:
        return self._clients[7].fetch_global_long_short_account_ratio(
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
    ) -> list[dict]:
        return self._clients[6].fetch_top_trader_long_short_account_ratio(
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
    ) -> list[dict]:
        return self._clients[6].fetch_top_trader_long_short_position_ratio(
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
    ) -> list[dict]:
        return self._clients[7].fetch_funding_rate(
            symbol,
            start_time=floor_to_minute(start_time.astimezone(UTC)),
            end_time=floor_to_minute(end_time.astimezone(UTC)) + timedelta(minutes=1),
            limit=min(max(limit, 1), 1000),
        )

    def fetch_native_premium_index_snapshot(self, symbol: str) -> dict:
        return self._clients[4].fetch_premium_index(symbol)

    def delete_cached_vision_files(self, symbol: str, start_time: datetime, end_time: datetime) -> int:
        return self._vision_loader.delete_cached_files(
            symbol,
            floor_to_minute(start_time.astimezone(UTC)),
            floor_to_minute(end_time.astimezone(UTC)),
            streams=_VISION_CANONICAL_STREAMS,
        )

    # ------------------------------------------------------------------ #
    # Parallel REST orchestration                                          #
    # ------------------------------------------------------------------ #

    def _fetch_rest_parallel(
        self,
        symbol: str,
        rest_start: datetime,
        rest_end_inclusive: datetime,
        start_utc: datetime,
        end_utc: datetime,
    ) -> dict:
        """Submit all 8 endpoint groups to a thread pool and collect results.

        Each group uses a dedicated client (self._clients[n]) so concurrent
        calls don't block each other on the per-client rate limiter.
        """
        ls_start = rest_start - timedelta(minutes=30)
        ls_end = end_utc + timedelta(minutes=1)
        funding_start = start_utc - timedelta(hours=8)

        tasks: dict[str, Callable[[], object]] = {
            "klines": lambda: self._paginate_klines(
                self._clients[0], symbol, rest_start, rest_end_inclusive
            ),
            "mark_price_klines": lambda: self._paginate_mark_price_klines(
                self._clients[1], symbol, rest_start, rest_end_inclusive
            ),
            "index_price_klines": lambda: self._paginate_index_price_klines(
                self._clients[2], symbol, rest_start, rest_end_inclusive
            ),
            "agg_trades": lambda: self._safe(
                symbol,
                "agg_trades",
                lambda: self._paginate_agg_trades(self._clients[3], symbol, rest_start, rest_end_inclusive),
                [],
            ),
            # Group 4: three fast single-call endpoints share one client
            "snapshots": lambda: self._fetch_snapshots(self._clients[4], symbol),
            "oi_hist": lambda: self._safe(
                symbol,
                "open_interest_hist",
                lambda: self._paginate_oi_hist(
                    self._clients[5], symbol, rest_start - timedelta(minutes=5), rest_end_inclusive
                ),
                [],
            ),
            "top_trader_ratio": lambda: self._safe(
                symbol,
                "top_trader_ls_ratio",
                lambda: self._paginate_ls_ratio(
                    lambda cursor, end: self._clients[6].fetch_top_trader_long_short_account_ratio(
                        symbol, period="5m", start_time=cursor, end_time=end, limit=500
                    ),
                    ls_start,
                    ls_end,
                ),
                [],
            ),
            # Group 7: global LS ratio (paginated) then funding rate (single)
            # two sequential calls on one client, ~200-300 ms for small windows.
            "global_and_funding": lambda: self._fetch_global_and_funding(
                self._clients[7], symbol, ls_start, ls_end, funding_start, end_utc
            ),
        }

        raw: dict[str, object] = {}
        with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {name: executor.submit(copy_context().run, fn) for name, fn in tasks.items()}
            for name, future in futures.items():
                try:
                    raw[name] = future.result()
                except Exception as exc:
                    LOGGER.warning(
                        "Parallel REST fetch group failed",
                        extra={"symbol": symbol, "group": name, "error": str(exc)},
                    )
                    raw[name] = {} if name in ("snapshots", "global_and_funding") else []

        # Flatten composite groups into the top-level result dict.
        snapshots: dict = raw.pop("snapshots", {})  # type: ignore[assignment]
        gf: dict = raw.pop("global_and_funding", {})  # type: ignore[assignment]
        return {
            **raw,
            "book_ticker": snapshots.get("book_ticker"),
            "premium_index": snapshots.get("premium_index"),
            "open_interest": snapshots.get("open_interest"),
            "global_ratio": gf.get("global_ratio", []),
            "funding_rates": gf.get("funding_rates", []),
        }

    def _fetch_snapshots(self, client: BinanceRESTClient, symbol: str) -> dict:
        """Fetch book_ticker, premium_index, and open_interest sequentially on one client."""
        result: dict = {"book_ticker": None, "premium_index": None, "open_interest": None}
        for key, fetch in [
            ("book_ticker", lambda: client.fetch_book_ticker(symbol)),
            ("premium_index", lambda: client.fetch_premium_index(symbol)),
            ("open_interest", lambda: client.fetch_open_interest(symbol)),
        ]:
            try:
                result[key] = fetch()
            except Exception as exc:
                LOGGER.warning(
                    "Optional Binance enrichment failed",
                    extra={"symbol": symbol, "enrichment": key, "error": str(exc)},
                )
        return result

    def _fetch_global_and_funding(
        self,
        client: BinanceRESTClient,
        symbol: str,
        ls_start: datetime,
        ls_end: datetime,
        funding_start: datetime,
        end_utc: datetime,
    ) -> dict:
        """Fetch global LS ratio and funding rate sequentially on one client."""
        global_ratio: list[dict] = []
        funding_rates: list[dict] = []
        try:
            global_ratio = self._paginate_ls_ratio(
                lambda cursor, end: client.fetch_global_long_short_account_ratio(
                    symbol, period="5m", start_time=cursor, end_time=end, limit=500
                ),
                ls_start,
                ls_end,
            )
        except Exception as exc:
            LOGGER.warning(
                "Optional Binance enrichment failed",
                extra={"symbol": symbol, "enrichment": "global_ls_ratio", "error": str(exc)},
            )
        try:
            funding_rates = client.fetch_funding_rate(
                symbol,
                start_time=funding_start,
                end_time=end_utc + timedelta(minutes=1),
                limit=1000,
            )
        except Exception as exc:
            LOGGER.warning(
                "Optional Binance enrichment failed",
                extra={"symbol": symbol, "enrichment": "funding_rate", "error": str(exc)},
            )
        return {"global_ratio": global_ratio, "funding_rates": funding_rates}

    # ------------------------------------------------------------------ #
    # Pagination helpers (per-client versions of binance_provider helpers) #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _paginate_open_time(
        fetch_page: Callable[[datetime, int], list[dict]],
        start_time: datetime,
        end_time: datetime,
        *,
        page_limit: int,
    ) -> list[dict]:
        rows: list[dict] = []
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

    def _paginate_klines(
        self, client: BinanceRESTClient, symbol: str, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        return self._paginate_open_time(
            lambda cursor, limit: client.fetch_klines(symbol, cursor, end_time, interval="1m", limit=limit),
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
        client: BinanceRESTClient,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        *,
        interval: str,
        limit: int,
    ) -> list[dict]:
        rows: list[dict] = []
        cursor = start_time
        interval_minutes = self._interval_minutes(interval)
        page_limit = min(max(limit, 1), 1500)
        for _ in range(10_000):
            batch = client.fetch_klines(symbol, cursor, end_time, interval=interval, limit=page_limit)
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
        self, client: BinanceRESTClient, symbol: str, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        return self._paginate_open_time(
            lambda cursor, limit: client.fetch_mark_price_klines(symbol, cursor, end_time, interval="1m", limit=limit),
            start_time,
            end_time,
            page_limit=1500,
        )

    def _paginate_index_price_klines(
        self, client: BinanceRESTClient, symbol: str, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        return self._paginate_open_time(
            lambda cursor, limit: client.fetch_index_price_klines(symbol, cursor, end_time, interval="1m", limit=limit),
            start_time,
            end_time,
            page_limit=1500,
        )

    def _paginate_agg_trades(
        self, client: BinanceRESTClient, symbol: str, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        rows: list[dict] = []
        cursor = start_time
        for _ in range(10_000):
            batch = client.fetch_agg_trades(symbol, cursor, end_time, limit=1000)
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

    def _paginate_oi_hist(
        self, client: BinanceRESTClient, symbol: str, start_time: datetime, end_time: datetime
    ) -> list[dict]:
        rows: list[dict] = []
        cursor = start_time
        for _ in range(200):
            batch = client.fetch_open_interest_hist(
                symbol,
                period="5m",
                start_time=cursor,
                end_time=end_time,
                limit=500,
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

    @staticmethod
    def _paginate_ls_ratio(
        fetch_page: Callable[[datetime, datetime], list[dict]],
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict]:
        rows: list[dict] = []
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
        return (
            pl.DataFrame(rows)
            .sort("data_time")
            .unique(subset=["data_time"], keep="last")
            .to_dicts()
        )

    # ------------------------------------------------------------------ #
    # Static helpers (identical to BinanceCanonicalMinuteProvider)        #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _safe(symbol: str, enrichment: str, fetch: Callable[[], object], default: object) -> object:
        try:
            return fetch()
        except Exception as exc:
            LOGGER.warning(
                "Optional Binance enrichment failed",
                extra={"symbol": symbol, "enrichment": enrichment, "error": str(exc)},
            )
            return default

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
        funding_rates: list[dict],
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict]:
        if not funding_rates:
            return funding_rates
        window_start_ms = int(window_start.timestamp() * 1000)
        window_end_ms = int(window_end.timestamp() * 1000)
        if any(window_start_ms <= int(item["funding_time"]) <= window_end_ms for item in funding_rates):
            return funding_rates
        latest_before_end: dict | None = None
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
        metrics_rows: list[dict],
        window_start: datetime,
        window_end: datetime,
    ) -> list[dict]:
        if not metrics_rows:
            return metrics_rows
        window_start_ms = int(window_start.timestamp() * 1000)
        window_end_ms = int(window_end.timestamp() * 1000)
        if any(window_start_ms <= int(item.get("create_time", -1)) <= window_end_ms for item in metrics_rows):
            return metrics_rows
        latest_before_end: dict | None = None
        for row in sorted(metrics_rows, key=lambda item: int(item.get("create_time", -1))):
            if int(row.get("create_time", -1)) <= window_end_ms:
                latest_before_end = row
        if latest_before_end is None:
            return metrics_rows
        seeded = dict(latest_before_end)
        seeded["create_time"] = window_start_ms
        return [seeded, *metrics_rows]
