from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime, timedelta

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
        top_trader_ratio_rows: list[dict[str, object]] | None = None,
        global_ratio_rows: list[dict[str, object]] | None = None,
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
        frame = frame.join(
            self._ls_ratio_frame(
                start_minute=start_minute,
                end_minute=end_minute,
                top_trader_records=top_trader_ratio_rows or [],
                global_records=global_ratio_rows or [],
            ),
            on="timestamp",
            how="left",
        )
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

        frame = pl.DataFrame(records)

        if "create_time" not in frame.columns:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})

        create_time_expr = pl.col("create_time")
        if frame.schema["create_time"] == pl.Utf8:
            create_time_expr = (
                pl.col("create_time")
                .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                .dt.replace_time_zone("UTC")
                .dt.timestamp("ms")
            )

        frame = frame.with_columns(create_time_expr.cast(pl.Int64, strict=False).alias("create_time"))
        frame = frame.with_columns(self._to_minute_timestamp("create_time"))

        has_direct_metrics = "oi_contracts" in frame.columns or "oi_value_usdt" in frame.columns
        if has_direct_metrics:
            direct_columns = ["oi_contracts", "oi_value_usdt"]
            for column in direct_columns:
                if column not in frame.columns:
                    frame = frame.with_columns(pl.lit(None).cast(pl.Float64).alias(column))
        else:
            frame = (
                frame.with_columns(
                    pl.when(pl.col("count_toptrader_long_short_ratio") > 0)
                    .then(pl.col("sum_open_interest") / pl.col("count_toptrader_long_short_ratio"))
                    .otherwise(None)
                    .alias("oi_contracts"),
                    pl.when(pl.col("count_toptrader_long_short_ratio") > 0)
                    .then(pl.col("sum_open_interest_value") / pl.col("count_toptrader_long_short_ratio"))
                    .otherwise(None)
                    .alias("oi_value_usdt"),
                )
            )

        return (
            frame.select(
                "timestamp",
                "oi_contracts",
                "oi_value_usdt",
            )
            .unique(subset=["timestamp"], keep="last")
        )

    @staticmethod
    def _ratio_point_frame(
        records: list[dict[str, object]],
        *,
        ratio_column: str,
    ) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"data_timestamp": []}, schema={"data_timestamp": pl.Datetime("ms", "UTC")})

        frame = pl.DataFrame(records)
        if "data_time" not in frame.columns:
            return pl.DataFrame({"data_timestamp": []}, schema={"data_timestamp": pl.Datetime("ms", "UTC")})

        expressions: list[pl.Expr] = [
            pl.from_epoch(pl.col("data_time").cast(pl.Int64), time_unit="ms")
            .dt.replace_time_zone("UTC")
            .alias("data_timestamp"),
            pl.col("ratio").cast(pl.Float64).alias(ratio_column),
        ]
        if "long_account" in frame.columns:
            expressions.append(pl.col("long_account").cast(pl.Float64).alias("top_trader_long_pct"))
        if "short_account" in frame.columns:
            expressions.append(pl.col("short_account").cast(pl.Float64).alias("top_trader_short_pct"))
        ratio_frame = pl.DataFrame(records).with_columns(*expressions)
        if "top_trader_long_pct" not in ratio_frame.columns:
            ratio_frame = ratio_frame.with_columns(pl.lit(None).cast(pl.Float64).alias("top_trader_long_pct"))
        if "top_trader_short_pct" not in ratio_frame.columns:
            ratio_frame = ratio_frame.with_columns(pl.lit(None).cast(pl.Float64).alias("top_trader_short_pct"))
        return ratio_frame.sort("data_timestamp")

    def _ls_ratio_frame(
        self,
        *,
        start_minute: datetime,
        end_minute: datetime,
        top_trader_records: list[dict[str, object]],
        global_records: list[dict[str, object]],
    ) -> pl.DataFrame:
        spine = self._minute_spine(start_minute, end_minute).sort("timestamp")
        top = self._ratio_point_frame(top_trader_records, ratio_column="top_trader_ls_ratio_acct")
        if top.height > 0:
            spine = spine.join_asof(
                top.select("data_timestamp", "top_trader_ls_ratio_acct", "top_trader_long_pct", "top_trader_short_pct"),
                left_on="timestamp",
                right_on="data_timestamp",
                strategy="backward",
                tolerance=timedelta(minutes=30),
            ).drop("data_timestamp")
        else:
            spine = spine.with_columns(
                pl.lit(None).cast(pl.Float64).alias("top_trader_ls_ratio_acct"),
                pl.lit(None).cast(pl.Float64).alias("top_trader_long_pct"),
                pl.lit(None).cast(pl.Float64).alias("top_trader_short_pct"),
            )

        global_frame = self._ratio_point_frame(global_records, ratio_column="global_ls_ratio_acct")
        if global_frame.height > 0:
            spine = spine.join_asof(
                global_frame.select("data_timestamp", "global_ls_ratio_acct"),
                left_on="timestamp",
                right_on="data_timestamp",
                strategy="backward",
                tolerance=timedelta(minutes=30),
            ).drop("data_timestamp")
        else:
            spine = spine.with_columns(pl.lit(None).cast(pl.Float64).alias("global_ls_ratio_acct"))

        return spine.with_columns(
            pl.when(pl.col("top_trader_ls_ratio_acct").is_not_null() & pl.col("global_ls_ratio_acct").is_not_null())
            .then(pl.col("top_trader_ls_ratio_acct") - pl.col("global_ls_ratio_acct"))
            .otherwise(None)
            .alias("ls_ratio_divergence"),
            (pl.col("top_trader_ls_ratio_acct").is_not_null() & pl.col("global_ls_ratio_acct").is_not_null()).alias(
                "has_ls_ratio"
            ),
        )

    def _live_frame(self, records: list[LiveMinuteFeatures]) -> pl.DataFrame:
        if not records:
            return pl.DataFrame({"timestamp": []}, schema={"timestamp": pl.Datetime("ms", "UTC")})
        frame = pl.DataFrame([asdict(item) for item in records]).with_columns(
            self._to_minute_timestamp("timestamp_ms")
        )
        return (
            frame.group_by("timestamp")
            .agg(
                pl.col("has_ws_latency").last().alias("has_ws_latency"),
                pl.col("has_depth").last().alias("has_depth"),
                pl.col("has_liq").last().alias("has_liq"),
                pl.col("has_ls_ratio").last().alias("has_ls_ratio"),
                pl.col("event_time").last().alias("event_time"),
                pl.col("transact_time").last().alias("transact_time"),
                pl.col("arrival_time").last().alias("arrival_time"),
                pl.col("latency_engine").last().alias("latency_engine"),
                pl.col("latency_network").last().alias("latency_network"),
                pl.col("ws_latency_bad").last().alias("ws_latency_bad"),
                pl.col("update_id_start").last().alias("update_id_start"),
                pl.col("update_id_end").last().alias("update_id_end"),
                pl.col("price_impact_100k").last().alias("price_impact_100k"),
                pl.col("impact_fillable").last().alias("impact_fillable"),
                pl.col("depth_degraded").last().alias("depth_degraded"),
                pl.col("liq_long_vol_usdt").last().alias("liq_long_vol_usdt"),
                pl.col("liq_short_vol_usdt").last().alias("liq_short_vol_usdt"),
                pl.col("liq_long_count").last().alias("liq_long_count"),
                pl.col("liq_short_count").last().alias("liq_short_count"),
                pl.col("liq_avg_fill_price").last().alias("liq_avg_fill_price"),
                pl.col("liq_unfilled_ratio").last().alias("liq_unfilled_ratio"),
                pl.col("liq_unfilled_supported").last().alias("liq_unfilled_supported"),
                pl.col("predicted_funding").last().alias("predicted_funding"),
                pl.col("next_funding_time").last().alias("next_funding_time"),
            )
            .sort("timestamp")
        )

    @staticmethod
    def _derive_columns(frame: pl.DataFrame) -> pl.DataFrame:
        for column in ("predicted_funding", "next_funding_time", "has_ls_ratio", "transact_time"):
            right_column = f"{column}_right"
            if right_column in frame.columns:
                frame = frame.with_columns(pl.coalesce([pl.col(right_column), pl.col(column)]).alias(column)).drop(
                    right_column
                )

        default_columns: dict[str, float | int | bool | None] = {
            "has_ws_latency": False,
            "has_depth": False,
            "has_liq": False,
            "has_ls_ratio": False,
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
            "ws_latency_bad": None,
            "impact_fillable": None,
            "depth_degraded": None,
            "liq_long_vol_usdt": None,
            "liq_short_vol_usdt": None,
            "liq_long_count": None,
            "liq_short_count": None,
            "liq_avg_fill_price": None,
            "liq_unfilled_ratio": None,
            "liq_unfilled_supported": None,
            "top_trader_ls_ratio_acct": None,
            "global_ls_ratio_acct": None,
            "ls_ratio_divergence": None,
            "top_trader_long_pct": None,
            "top_trader_short_pct": None,
        }
        for column, default in default_columns.items():
            if column not in frame.columns:
                frame = frame.with_columns(pl.lit(default).alias(column))

        frame = frame.with_columns(
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
            pl.when(pl.col("top_trader_ls_ratio_acct").is_not_null() & pl.col("global_ls_ratio_acct").is_not_null())
            .then(True)
            .otherwise(pl.col("has_ls_ratio").fill_null(False))
            .alias("has_ls_ratio"),
            pl.col("has_ws_latency").fill_null(False).alias("has_ws_latency"),
            pl.col("has_depth").fill_null(False).alias("has_depth"),
            pl.col("has_liq").fill_null(False).alias("has_liq"),
            pl.when(pl.col("has_ws_latency").fill_null(False))
            .then(pl.coalesce([pl.col("ws_latency_bad"), pl.lit(False)]))
            .otherwise(None)
            .alias("ws_latency_bad"),
            pl.when(pl.col("has_depth").fill_null(False))
            .then(pl.coalesce([pl.col("depth_degraded"), pl.lit(False)]))
            .otherwise(None)
            .alias("depth_degraded"),
            pl.when(pl.col("has_liq").fill_null(False))
            .then(pl.coalesce([pl.col("liq_unfilled_supported"), pl.lit(False)]))
            .otherwise(None)
            .alias("liq_unfilled_supported"),
            pl.when(pl.col("has_liq").fill_null(False))
            .then(pl.coalesce([pl.col("liq_long_vol_usdt"), pl.lit(0.0)]))
            .otherwise(None)
            .alias("liq_long_vol_usdt"),
            pl.when(pl.col("has_liq").fill_null(False))
            .then(pl.coalesce([pl.col("liq_short_vol_usdt"), pl.lit(0.0)]))
            .otherwise(None)
            .alias("liq_short_vol_usdt"),
            pl.when(pl.col("has_liq").fill_null(False))
            .then(pl.coalesce([pl.col("liq_long_count"), pl.lit(0)]))
            .otherwise(None)
            .alias("liq_long_count"),
            pl.when(pl.col("has_liq").fill_null(False))
            .then(pl.coalesce([pl.col("liq_short_count"), pl.lit(0)]))
            .otherwise(None)
            .alias("liq_short_count"),
            pl.when(pl.col("has_liq").fill_null(False))
            .then(pl.col("liq_avg_fill_price"))
            .otherwise(None)
            .alias("liq_avg_fill_price"),
            pl.when(pl.col("has_liq").fill_null(False))
            .then(
                pl.when(pl.col("liq_unfilled_supported") == True)
                .then(pl.col("liq_unfilled_ratio"))
                .otherwise(None)
            )
            .otherwise(None)
            .alias("liq_unfilled_ratio"),
            pl.when(pl.col("top_trader_ls_ratio_acct").is_not_null() & pl.col("global_ls_ratio_acct").is_not_null())
            .then(pl.col("top_trader_ls_ratio_acct") - pl.col("global_ls_ratio_acct"))
            .otherwise(None)
            .alias("ls_ratio_divergence"),
        )
        return frame

    def _apply_fill_policies(self, frame: pl.DataFrame) -> pl.DataFrame:
        ffill_columns = [
            "micro_price_close",
            "avg_spread_usdt",
            "bid_ask_imbalance",
            "avg_bid_depth",
            "avg_ask_depth",
            "spread_pct",
            "oi_contracts",
            "oi_value_usdt",
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
