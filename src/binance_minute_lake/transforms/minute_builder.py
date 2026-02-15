from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from binance_minute_lake.core.schema import canonical_column_names, dtype_map
from binance_minute_lake.sources.websocket import LiveMinuteFeatures


class MinuteTransformEngine:
    def __init__(self, max_ffill_minutes: int = 60) -> None:
        self._max_ffill_minutes = max_ffill_minutes
        self._dtype_map = dtype_map()

    def build_canonical_frame(
        self,
        start_minute: datetime,
        end_minute: datetime,
        klines: list[dict[str, object]],
        mark_price_klines: list[dict[str, object]],
        index_price_klines: list[dict[str, object]],
        agg_trades: list[dict[str, object]],
        funding_rates: list[dict[str, object]],
        book_ticker_snapshots: list[dict[str, object]] | None = None,
        premium_index_snapshots: list[dict[str, object]] | None = None,
        metrics_rows: list[dict[str, object]] | None = None,
        live_features: list[LiveMinuteFeatures] | None = None,
    ) -> pl.DataFrame:
        frame = self._minute_spine(start_minute, end_minute)

        frame = frame.join(self._klines_frame(klines), on="timestamp", how="left")
        frame = frame.join(self._mark_price_frame(mark_price_klines), on="timestamp", how="left")
        frame = frame.join(self._index_price_frame(index_price_klines), on="timestamp", how="left")
        frame = frame.join(self._agg_trade_frame(agg_trades), on="timestamp", how="left")
        frame = frame.join(
            self._book_ticker_frame(book_ticker_snapshots or []),
            on="timestamp",
            how="left",
        )
        frame = frame.join(self._funding_frame(funding_rates), on="timestamp", how="left")
        frame = frame.join(
            self._premium_frame(premium_index_snapshots or []),
            on="timestamp",
            how="left",
        )
        frame = frame.join(self._metrics_frame(metrics_rows or []), on="timestamp", how="left")
        frame = frame.join(self._live_frame(live_features or []), on="timestamp", how="left")

        frame = self._derive_columns(frame)
        frame = self._apply_fill_policies(frame)
        return self._finalize_schema(frame)

    @staticmethod
    def _minute_spine(start_minute: datetime, end_minute: datetime) -> pl.DataFrame:
        start = start_minute.astimezone(UTC)
        end = end_minute.astimezone(UTC)
        if end < start:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        return pl.DataFrame(
            {
                "timestamp": pl.datetime_range(
                    start,
                    end,
                    interval="1m",
                    closed="both",
                    time_unit="ms",
                    time_zone="UTC",
                    eager=True,
                )
            }
        )

    @staticmethod
    def _to_minute_timestamp(column: str) -> pl.Expr:
        return (
            pl.from_epoch(pl.col(column).cast(pl.Int64), time_unit="ms")
            .dt.replace_time_zone("UTC")
            .dt.truncate("1m")
            .alias("timestamp")
        )

    def _klines_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        return (
            pl.DataFrame(records)
            .with_columns(self._to_minute_timestamp("open_time"))
            .select(
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume_btc",
                "volume_usdt",
                "trade_count",
                "taker_buy_vol_btc",
                "taker_buy_vol_usdt",
            )
            .unique(subset=["timestamp"], keep="last")
        )

    def _mark_price_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        return (
            pl.DataFrame(records)
            .with_columns(self._to_minute_timestamp("open_time"))
            .select("timestamp", "mark_price_open", "mark_price_close")
            .unique(subset=["timestamp"], keep="last")
        )

    def _index_price_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        return (
            pl.DataFrame(records)
            .with_columns(self._to_minute_timestamp("open_time"))
            .select("timestamp", "index_price_open", "index_price_close")
            .unique(subset=["timestamp"], keep="last")
        )

    def _agg_trade_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})

        trades = (
            pl.DataFrame(records)
            .with_columns(
                self._to_minute_timestamp("transact_time"),
            )
            .with_columns(
                (pl.col("price") * pl.col("qty")).alias("notional"),
            )
            .with_columns(
                pl.when(pl.col("is_buyer_maker") == False)
                .then(pl.col("qty"))
                .otherwise(0.0)
                .alias("buy_qty"),
                pl.when(pl.col("is_buyer_maker") == True)
                .then(pl.col("qty"))
                .otherwise(0.0)
                .alias("sell_qty"),
                pl.when((pl.col("is_buyer_maker") == False) & (pl.col("notional") >= 100000.0))
                .then(pl.col("qty"))
                .otherwise(0.0)
                .alias("buy_whale_qty"),
                pl.when((pl.col("is_buyer_maker") == True) & (pl.col("notional") >= 100000.0))
                .then(pl.col("qty"))
                .otherwise(0.0)
                .alias("sell_whale_qty"),
                pl.when((pl.col("is_buyer_maker") == False) & (pl.col("notional") <= 1000.0))
                .then(pl.col("qty"))
                .otherwise(0.0)
                .alias("buy_retail_qty"),
                pl.when((pl.col("is_buyer_maker") == True) & (pl.col("notional") <= 1000.0))
                .then(pl.col("qty"))
                .otherwise(0.0)
                .alias("sell_retail_qty"),
                pl.when(pl.col("notional") >= 100000.0).then(1).otherwise(0).alias("whale_trade"),
                pl.when(pl.col("is_buyer_maker") == False).then(1).otherwise(0).alias("is_buy"),
                pl.when(pl.col("is_buyer_maker") == True).then(1).otherwise(0).alias("is_sell"),
            )
            .sort(["timestamp", "transact_time"])
            .with_columns(
                (pl.col("price").log() - pl.col("price").shift(1).log())
                .over("timestamp")
                .alias("log_return")
            )
        )

        return trades.group_by("timestamp").agg(
            pl.col("transact_time").max().alias("transact_time"),
            (pl.col("notional").sum() / pl.col("qty").sum()).alias("vwap_1m"),
            pl.col("qty").max().alias("max_trade_size_btc"),
            pl.col("buy_qty").sum().alias("agg_buy_qty"),
            pl.col("sell_qty").sum().alias("agg_sell_qty"),
            (pl.col("buy_qty").sum() - pl.col("sell_qty").sum()).alias("net_taker_vol_btc"),
            pl.col("is_buy").sum().alias("count_buy_trades"),
            pl.col("is_sell").sum().alias("count_sell_trades"),
            pl.col("buy_whale_qty").sum().alias("vol_buy_whale_btc"),
            pl.col("sell_whale_qty").sum().alias("vol_sell_whale_btc"),
            pl.col("buy_retail_qty").sum().alias("vol_buy_retail_btc"),
            pl.col("sell_retail_qty").sum().alias("vol_sell_retail_btc"),
            pl.col("whale_trade").sum().alias("whale_trade_count"),
            pl.col("log_return").fill_null(0.0).pow(2).sum().sqrt().alias("realized_vol_1m"),
        )

    def _book_ticker_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})

        ticker = (
            pl.DataFrame(records)
            .with_columns(
                self._to_minute_timestamp("event_time"),
                (pl.col("ask_price") - pl.col("bid_price")).alias("spread"),
                (
                    (pl.col("bid_qty") - pl.col("ask_qty"))
                    / (pl.col("bid_qty") + pl.col("ask_qty"))
                ).alias("imbalance"),
                (
                    (pl.col("ask_price") - pl.col("bid_price"))
                    / ((pl.col("ask_price") + pl.col("bid_price")) / 2.0)
                ).alias("spread_pct"),
                (
                    (
                        (pl.col("bid_price") * pl.col("ask_qty"))
                        + (pl.col("ask_price") * pl.col("bid_qty"))
                    )
                    / (pl.col("bid_qty") + pl.col("ask_qty"))
                ).alias("micro_price"),
            )
            .sort(["timestamp", "event_time"])
        )

        return ticker.group_by("timestamp").agg(
            pl.col("spread").mean().alias("avg_spread_usdt"),
            pl.col("imbalance").mean().alias("bid_ask_imbalance"),
            pl.col("bid_qty").mean().alias("avg_bid_depth"),
            pl.col("ask_qty").mean().alias("avg_ask_depth"),
            pl.col("spread_pct").mean().alias("spread_pct"),
            pl.col("micro_price").last().alias("micro_price_close"),
        )

    def _funding_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        return (
            pl.DataFrame(records)
            .with_columns(self._to_minute_timestamp("funding_time"))
            .select("timestamp", "funding_rate")
            .sort("timestamp")
            .unique(subset=["timestamp"], keep="last")
        )

    def _premium_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        return (
            pl.DataFrame(records)
            .with_columns(self._to_minute_timestamp("event_time"))
            .select(
                "timestamp",
                pl.col("predicted_funding").cast(pl.Float64),
                pl.col("next_funding_time").cast(pl.Int64),
                pl.col("last_funding_rate").cast(pl.Float64).alias("premium_last_funding_rate"),
            )
            .group_by("timestamp")
            .agg(
                pl.col("predicted_funding").last().alias("predicted_funding"),
                pl.col("next_funding_time").last().alias("next_funding_time"),
                pl.col("premium_last_funding_rate").last().alias("premium_last_funding_rate"),
            )
        )

    def _metrics_frame(self, records: list[dict[str, object]]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})

        frame = (
            pl.DataFrame(records)
            .with_columns(self._to_minute_timestamp("create_time"))
            .with_columns(
                pl.when(pl.col("count_toptrader_long_short_ratio") > 0)
                .then(pl.col("sum_open_interest") / pl.col("count_toptrader_long_short_ratio"))
                .otherwise(None)
                .alias("oi_contracts"),
                pl.when(pl.col("count_toptrader_long_short_ratio") > 0)
                .then(pl.col("sum_open_interest_value") / pl.col("count_toptrader_long_short_ratio"))
                .otherwise(None)
                .alias("oi_value_usdt"),
                pl.when(pl.col("count_toptrader_long_short_ratio") > 0)
                .then(pl.col("sum_toptrader_long_short_ratio") / pl.col("count_toptrader_long_short_ratio"))
                .otherwise(None)
                .alias("top_trader_ls_ratio_acct"),
                pl.when(pl.col("count_long_short_ratio") > 0)
                .then(pl.col("sum_taker_long_short_vol_ratio") / pl.col("count_long_short_ratio"))
                .otherwise(None)
                .alias("global_ls_ratio_acct"),
            )
            .with_columns(
                (pl.col("top_trader_ls_ratio_acct") - pl.col("global_ls_ratio_acct")).alias("ls_ratio_divergence")
            )
            .with_columns(
                pl.lit(None).cast(pl.Float64).alias("top_trader_long_pct"),
                pl.lit(None).cast(pl.Float64).alias("top_trader_short_pct"),
            )
        )

        return (
            frame.select(
                "timestamp",
                "oi_contracts",
                "oi_value_usdt",
                "top_trader_ls_ratio_acct",
                "global_ls_ratio_acct",
                "ls_ratio_divergence",
                "top_trader_long_pct",
                "top_trader_short_pct",
            )
            .unique(subset=["timestamp"], keep="last")
        )

    def _live_frame(self, records: list[LiveMinuteFeatures]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        frame = pl.DataFrame([item.__dict__ for item in records]).with_columns(
            self._to_minute_timestamp("timestamp_ms")
        )
        return (
            frame.group_by("timestamp")
            .agg(
                pl.col("event_time").last().alias("event_time"),
                pl.col("arrival_time").last().alias("arrival_time"),
                pl.col("latency_engine").last().alias("latency_engine"),
                pl.col("latency_network").last().alias("latency_network"),
                pl.col("update_id_start").last().alias("update_id_start"),
                pl.col("update_id_end").last().alias("update_id_end"),
                pl.col("price_impact_100k").last().alias("price_impact_100k"),
                pl.col("predicted_funding").last().alias("predicted_funding"),
                pl.col("next_funding_time").last().alias("next_funding_time"),
            )
            .sort("timestamp")
        )

    @staticmethod
    def _derive_columns(frame: pl.DataFrame) -> pl.DataFrame:
        default_columns: dict[str, float | int | None] = {
            "trade_count": 0,
            "volume_btc": 0.0,
            "close": None,
            "vwap_1m": None,
            "agg_buy_qty": 0.0,
            "agg_sell_qty": 0.0,
            "net_taker_vol_btc": 0.0,
            "max_trade_size_btc": 0.0,
            "count_buy_trades": 0,
            "count_sell_trades": 0,
            "vol_buy_whale_btc": 0.0,
            "vol_sell_whale_btc": 0.0,
            "vol_buy_retail_btc": 0.0,
            "vol_sell_retail_btc": 0.0,
            "whale_trade_count": 0,
            "realized_vol_1m": 0.0,
            "mark_price_close": None,
            "index_price_close": None,
            "funding_rate": None,
            "premium_last_funding_rate": None,
        }
        for column, default in default_columns.items():
            if column not in frame.columns:
                frame = frame.with_columns(pl.lit(default).alias(column))

        return frame.with_columns(
            pl.when(pl.col("trade_count").fill_null(0) > 0)
            .then(pl.col("volume_btc") / pl.col("trade_count"))
            .otherwise(0.0)
            .alias("avg_trade_size_btc"),
            pl.when((pl.col("agg_buy_qty") + pl.col("agg_sell_qty")) > 0)
            .then(pl.col("agg_buy_qty") / (pl.col("agg_buy_qty") + pl.col("agg_sell_qty")))
            .otherwise(None)
            .alias("taker_buy_ratio"),
            pl.when(pl.col("index_price_close") != 0)
            .then((pl.col("mark_price_close") / pl.col("index_price_close")) - 1.0)
            .otherwise(None)
            .alias("premium_index"),
            pl.coalesce([pl.col("funding_rate"), pl.col("premium_last_funding_rate")]).alias("funding_rate"),
            pl.when(pl.col("vwap_1m").is_null()).then(pl.col("close")).otherwise(pl.col("vwap_1m")).alias("vwap_1m"),
            pl.coalesce([pl.col("net_taker_vol_btc"), pl.lit(0.0)]).alias("net_taker_vol_btc"),
            pl.coalesce([pl.col("max_trade_size_btc"), pl.lit(0.0)]).alias("max_trade_size_btc"),
            pl.coalesce([pl.col("count_buy_trades"), pl.lit(0)]).alias("count_buy_trades"),
            pl.coalesce([pl.col("count_sell_trades"), pl.lit(0)]).alias("count_sell_trades"),
            pl.coalesce([pl.col("vol_buy_whale_btc"), pl.lit(0.0)]).alias("vol_buy_whale_btc"),
            pl.coalesce([pl.col("vol_sell_whale_btc"), pl.lit(0.0)]).alias("vol_sell_whale_btc"),
            pl.coalesce([pl.col("vol_buy_retail_btc"), pl.lit(0.0)]).alias("vol_buy_retail_btc"),
            pl.coalesce([pl.col("vol_sell_retail_btc"), pl.lit(0.0)]).alias("vol_sell_retail_btc"),
            pl.coalesce([pl.col("whale_trade_count"), pl.lit(0)]).alias("whale_trade_count"),
            pl.coalesce([pl.col("realized_vol_1m"), pl.lit(0.0)]).alias("realized_vol_1m"),
        )

    def _apply_fill_policies(self, frame: pl.DataFrame) -> pl.DataFrame:
        ffill_columns = [
            "avg_spread_usdt",
            "bid_ask_imbalance",
            "avg_bid_depth",
            "avg_ask_depth",
            "spread_pct",
            "oi_contracts",
            "oi_value_usdt",
            "top_trader_ls_ratio_acct",
            "global_ls_ratio_acct",
            "ls_ratio_divergence",
            "top_trader_long_pct",
            "top_trader_short_pct",
            "funding_rate",
        ]
        expressions: list[pl.Expr] = []
        for column in ffill_columns:
            if column in frame.columns:
                expressions.append(pl.col(column).forward_fill(limit=self._max_ffill_minutes).alias(column))
        if not expressions:
            return frame
        return frame.with_columns(*expressions)

    def _finalize_schema(self, frame: pl.DataFrame) -> pl.DataFrame:
        columns = canonical_column_names()
        for column in columns:
            if column not in frame.columns:
                frame = frame.with_columns(pl.lit(None).alias(column))

        typed_exprs = [pl.col(name).cast(self._dtype_map[name], strict=False).alias(name) for name in columns]
        return frame.select(*typed_exprs).sort("timestamp")
