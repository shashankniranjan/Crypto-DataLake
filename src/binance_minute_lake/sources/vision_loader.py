from __future__ import annotations

import io
import logging
import time
import zipfile
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from binance_minute_lake.sources.vision import VisionClient

logger = logging.getLogger(__name__)


class VisionLoader:
    """Helper that downloads and parses Binance Vision daily zip files into record lists."""

    def __init__(self, client: VisionClient, cache_dir: Path, missing_cache_ttl_seconds: int = 1_800) -> None:
        self._client = client
        self._cache_dir = cache_dir
        self._missing_cache_ttl_seconds = max(missing_cache_ttl_seconds, 0)
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    # public API -------------------------------------------------------------
    def load_klines(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, object]]:
        frames = [
            self._read_zip_csv(
                stream="klines",
                symbol=symbol,
                trade_date=day,
                interval=interval,
                schema={
                    "open_time": pl.Int64,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Float64,
                    "close_time": pl.Int64,
                    "quote_volume": pl.Float64,
                    "count": pl.Int64,
                    "taker_buy_volume": pl.Float64,
                    "taker_buy_quote_volume": pl.Float64,
                    "ignore": pl.Int64,
                },
            ).with_columns(
                pl.col("open_time").alias("open_time"),
                pl.col("open"),
                pl.col("high"),
                pl.col("low"),
                pl.col("close"),
                pl.col("volume").alias("volume_btc"),
                pl.col("quote_volume").alias("volume_usdt"),
                pl.col("count").alias("trade_count"),
                pl.col("taker_buy_volume").alias("taker_buy_vol_btc"),
                pl.col("taker_buy_quote_volume").alias("taker_buy_vol_usdt"),
            )
            for day in self._days_in_window(start, end)
        ]
        return self._filter_and_export(frames, start, end, ts_column="open_time")

    def load_mark_price_klines(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, object]]:
        frames = [
            self._read_zip_csv(
                stream="markPriceKlines",
                symbol=symbol,
                trade_date=day,
                interval=interval,
                schema={
                    "open_time": pl.Int64,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                },
            ).with_columns(
                pl.col("open_time"),
                pl.col("open").alias("mark_price_open"),
                pl.col("high").alias("mark_price_high"),
                pl.col("low").alias("mark_price_low"),
                pl.col("close").alias("mark_price_close"),
            )
            for day in self._days_in_window(start, end)
        ]
        return self._filter_and_export(frames, start, end, ts_column="open_time")

    def load_index_price_klines(
        self, symbol: str, start: datetime, end: datetime, interval: str = "1m"
    ) -> list[dict[str, object]]:
        frames = [
            self._read_zip_csv(
                stream="indexPriceKlines",
                symbol=symbol,
                trade_date=day,
                interval=interval,
                schema={
                    "open_time": pl.Int64,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                },
            ).with_columns(
                pl.col("open_time"),
                pl.col("open").alias("index_price_open"),
                pl.col("high").alias("index_price_high"),
                pl.col("low").alias("index_price_low"),
                pl.col("close").alias("index_price_close"),
            )
            for day in self._days_in_window(start, end)
        ]
        return self._filter_and_export(frames, start, end, ts_column="open_time")

    def load_agg_trades(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, object]]:
        frames = [
            self._read_zip_csv(
                stream="aggTrades",
                symbol=symbol,
                trade_date=day,
                schema={
                    "agg_trade_id": pl.Int64,
                    "aggregate_trade_id": pl.Int64,
                    "price": pl.Float64,
                    "quantity": pl.Float64,
                    "first_trade_id": pl.Int64,
                    "last_trade_id": pl.Int64,
                    "transact_time": pl.Int64,
                    "timestamp": pl.Int64,
                    "is_buyer_maker": pl.Boolean,
                    "was_buyer_maker": pl.Boolean,
                },
            ).with_columns(
                pl.coalesce([pl.col("agg_trade_id"), pl.col("aggregate_trade_id")]).alias("agg_trade_id"),
                pl.col("quantity").alias("qty"),
                pl.coalesce([pl.col("transact_time"), pl.col("timestamp")]).alias("transact_time"),
                pl.coalesce([pl.col("is_buyer_maker"), pl.col("was_buyer_maker")]).alias("is_buyer_maker"),
            ).select(
                "agg_trade_id",
                "price",
                "qty",
                "first_trade_id",
                "last_trade_id",
                "transact_time",
                "is_buyer_maker",
            )
            for day in self._days_in_window(start, end)
        ]
        return self._filter_and_export(frames, start, end, ts_column="transact_time")

    def load_book_ticker(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, object]]:
        frames = [
            self._read_zip_csv(
                stream="bookTicker",
                symbol=symbol,
                trade_date=day,
                schema={
                    "update_id": pl.Int64,
                    "best_bid_price": pl.Float64,
                    "best_bid_qty": pl.Float64,
                    "best_ask_price": pl.Float64,
                    "best_ask_qty": pl.Float64,
                    "transaction_time": pl.Int64,
                    "event_time": pl.Int64,
                },
            ).rename(
                {
                    "best_bid_price": "bid_price",
                    "best_bid_qty": "bid_qty",
                    "best_ask_price": "ask_price",
                    "best_ask_qty": "ask_qty",
                }
            )
            for day in self._days_in_window(start, end)
        ]
        return self._filter_and_export(frames, start, end, ts_column="event_time")

    def load_metrics(self, symbol: str, start: datetime, end: datetime) -> list[dict[str, object]]:
        frames = [
            self._read_zip_csv(
                stream="metrics",
                symbol=symbol,
                trade_date=day,
                schema={
                    "create_time": pl.Utf8,
                    "symbol": pl.Utf8,
                    "sum_open_interest": pl.Float64,
                    "sum_open_interest_value": pl.Float64,
                    "count_toptrader_long_short_ratio": pl.Float64,
                    "sum_toptrader_long_short_ratio": pl.Float64,
                    "count_long_short_ratio": pl.Float64,
                    "sum_taker_long_short_vol_ratio": pl.Float64,
                },
            ).with_columns(
                pl.col("create_time")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                .dt.replace_time_zone("UTC")
                .dt.timestamp("ms")
                .alias("create_time"),
                pl.col("sum_open_interest").alias("oi_contracts"),
                pl.col("sum_open_interest_value").alias("oi_value_usdt"),
            )
            for day in self._days_in_window(start, end)
        ]
        return self._filter_and_export(frames, start, end, ts_column="create_time")

    # internals --------------------------------------------------------------
    def _days_in_window(self, start: datetime, end: datetime) -> Iterable[date]:
        day = start.date()
        end_day = end.date()
        while day <= end_day:
            yield day
            day = day + timedelta(days=1)

    def delete_cached_files(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        *,
        streams: Iterable[str],
        interval: str = "1m",
    ) -> int:
        """Delete cached ZIP files for a materialized window.

        Missing markers are intentionally preserved; they are small and keep
        repeated API misses from probing Binance Vision too aggressively.
        """
        removed = 0
        for stream in streams:
            stream_interval = interval if VisionClient.requires_interval(stream) else "1m"
            for day in self._days_in_window(start, end):
                path = self._cache_path(stream, symbol, day, stream_interval)
                if path.exists() and path.suffix == ".zip":
                    path.unlink()
                    removed += 1
        return removed

    def _filter_and_export(
        self,
        frames: list[pl.DataFrame],
        start: datetime,
        end: datetime,
        ts_column: str,
    ) -> list[dict[str, object]]:
        if not frames:
            return []
        combined = pl.concat(frames, how="vertical_relaxed")
        if combined.height == 0:
            return []
        start_ms = int(start.astimezone(UTC).timestamp() * 1000)
        end_ms = int(end.astimezone(UTC).timestamp() * 1000)
        filtered = combined.filter((pl.col(ts_column) >= start_ms) & (pl.col(ts_column) <= end_ms))
        if filtered.height == 0:
            return []
        return filtered.to_dicts()

    def _read_zip_csv(
        self,
        *,
        stream: str,
        symbol: str,
        trade_date: date,
        schema: dict[str, pl.DataType],
        interval: str = "1m",
    ) -> pl.DataFrame:
        url = self._client.build_daily_zip_url(stream=stream, symbol=symbol, trade_date=trade_date, interval=interval)
        zip_path = self._cache_path(stream, symbol, trade_date, interval)
        missing_path = self._missing_cache_path(zip_path)
        if not zip_path.exists():
            if self._missing_cache_is_fresh(missing_path):
                logger.debug(
                    "vision object missing cache hit",
                    extra={"stream": stream, "symbol": symbol, "date": trade_date.isoformat(), "url": url},
                )
                return pl.DataFrame(schema=schema)
            if not self._client.exists(url):
                logger.warning(
                    "vision object missing",
                    extra={"stream": stream, "symbol": symbol, "date": trade_date.isoformat(), "url": url},
                )
                self._write_missing_cache_marker(missing_path)
                return pl.DataFrame(schema=schema)
            self._client.download_zip(url, zip_path)
        else:
            self._clear_missing_cache_marker(missing_path)

        with zipfile.ZipFile(zip_path, mode="r") as archive:
            csv_files = [name for name in archive.namelist() if name.endswith(".csv")]
            if not csv_files:
                logger.warning("vision zip empty", extra={"path": str(zip_path)})
                return pl.DataFrame(schema=schema)
            with archive.open(csv_files[0], mode="r") as handle:
                text_stream = io.BytesIO(handle.read())
        df = pl.read_csv(text_stream)

        # Ensure expected columns exist and types are enforced; tolerate extra columns in source.
        if schema:
            for name, dtype in schema.items():
                if name not in df.columns:
                    df = df.with_columns(pl.lit(None).cast(dtype).alias(name))
                else:
                    df = df.with_columns(pl.col(name).cast(dtype, strict=False))
            return df.select(list(schema.keys()))

        return df

    def _cache_path(self, stream: str, symbol: str, trade_date: date, interval: str) -> Path:
        filename = self._client.expected_filename(
            stream=stream,
            symbol=symbol,
            trade_date=trade_date,
            interval=interval,
        )
        return self._cache_dir / stream / symbol.upper() / filename

    @staticmethod
    def _missing_cache_path(zip_path: Path) -> Path:
        return zip_path.with_name(f"{zip_path.name}.missing")

    def _missing_cache_is_fresh(self, missing_path: Path) -> bool:
        if self._missing_cache_ttl_seconds < 1 or not missing_path.exists():
            return False
        age_seconds = time.time() - missing_path.stat().st_mtime
        if age_seconds <= self._missing_cache_ttl_seconds:
            return True
        self._clear_missing_cache_marker(missing_path)
        return False

    @staticmethod
    def _write_missing_cache_marker(missing_path: Path) -> None:
        missing_path.parent.mkdir(parents=True, exist_ok=True)
        missing_path.touch()

    @staticmethod
    def _clear_missing_cache_marker(missing_path: Path) -> None:
        try:
            missing_path.unlink()
        except FileNotFoundError:
            return
