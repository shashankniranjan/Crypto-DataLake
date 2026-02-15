from __future__ import annotations

import io
import logging
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import polars as pl

from binance_minute_lake.sources.vision import VisionClient

logger = logging.getLogger(__name__)


class VisionLoader:
    """Helper that downloads and parses Binance Vision daily zip files into record lists."""

    def __init__(self, client: VisionClient, cache_dir: Path) -> None:
        self._client = client
        self._cache_dir = cache_dir
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
                    "aggregate_trade_id": pl.Int64,
                    "price": pl.Float64,
                    "quantity": pl.Float64,
                    "first_trade_id": pl.Int64,
                    "last_trade_id": pl.Int64,
                    "timestamp": pl.Int64,
                    "was_buyer_maker": pl.Boolean,
                },
            ).rename({"quantity": "qty", "timestamp": "transact_time", "was_buyer_maker": "is_buyer_maker"})
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
                    "count_toptrader_long_short_ratio": pl.Int64,
                    "sum_toptrader_long_short_ratio": pl.Float64,
                    "count_long_short_ratio": pl.Int64,
                    "sum_taker_long_short_vol_ratio": pl.Float64,
                },
            ).with_columns(
                pl.col("create_time")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                .dt.replace_time_zone("UTC")
                .dt.timestamp("ms")
                .alias("create_time")
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
        if not zip_path.exists():
            if not self._client.exists(url):
                logger.warning(
                    "vision object missing",
                    extra={"stream": stream, "symbol": symbol, "date": trade_date.isoformat(), "url": url},
                )
                return pl.DataFrame(schema=schema)
            self._client.download_zip(url, zip_path)

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
        filename = self._client.expected_filename(stream=stream, symbol=symbol, trade_date=trade_date, interval=interval)
        return self._cache_dir / stream / symbol.upper() / filename
