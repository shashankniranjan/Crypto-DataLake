from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from .bucketing import TimeframeSpec, bucket_end_expr, bucket_start_expr, expected_minutes_expr


@dataclass(frozen=True, slots=True)
class AggregationRule:
    column: str
    rule: str


AGGREGATION_RULES: tuple[AggregationRule, ...] = (
    AggregationRule("timestamp", "bucket start in UTC"),
    AggregationRule("open", "first open by time in bucket"),
    AggregationRule("high", "max high"),
    AggregationRule("low", "min low"),
    AggregationRule("close", "last close by time in bucket"),
    AggregationRule("volume_btc", "sum"),
    AggregationRule("volume_usdt", "sum"),
    AggregationRule("trade_count", "sum"),
    AggregationRule("vwap", "sum(volume_usdt) / nullif(sum(volume_btc), 0)"),
    AggregationRule("avg_trade_size_btc", "sum(volume_btc) / nullif(sum(trade_count), 0)"),
    AggregationRule("max_trade_size_btc", "max"),
    AggregationRule("taker_buy_vol_btc", "sum"),
    AggregationRule("taker_buy_vol_usdt", "sum"),
    AggregationRule("net_taker_vol_btc", "sum"),
    AggregationRule("count_buy_trades", "sum"),
    AggregationRule("count_sell_trades", "sum"),
    AggregationRule("taker_buy_ratio", "sum(taker_buy_vol_btc) / nullif(sum(volume_btc), 0)"),
    AggregationRule("vol_buy_whale_btc", "sum"),
    AggregationRule("vol_sell_whale_btc", "sum"),
    AggregationRule("vol_buy_retail_btc", "sum"),
    AggregationRule("vol_sell_retail_btc", "sum"),
    AggregationRule("whale_trade_count", "sum"),
    AggregationRule("liq_long_vol_usdt", "sum"),
    AggregationRule("liq_short_vol_usdt", "sum"),
    AggregationRule("liq_long_count", "sum"),
    AggregationRule("liq_short_count", "sum"),
    AggregationRule("liq_avg_fill_price", "weighted average by liquidation notional"),
    AggregationRule("liq_unfilled_ratio", "weighted average by liquidation notional"),
    AggregationRule("liq_unfilled_supported", "OR"),
    AggregationRule("has_liq", "OR"),
    AggregationRule("oi_contracts", "last non-null in bucket"),
    AggregationRule("oi_value_usdt", "last non-null in bucket"),
    AggregationRule("top_trader_ls_ratio_acct", "last non-null in bucket"),
    AggregationRule("global_ls_ratio_acct", "last non-null in bucket"),
    AggregationRule("ls_ratio_divergence", "last non-null in bucket"),
    AggregationRule("top_trader_long_pct", "last non-null in bucket"),
    AggregationRule("top_trader_short_pct", "last non-null in bucket"),
    AggregationRule("premium_index", "last non-null in bucket"),
    AggregationRule("funding_rate", "last non-null in bucket"),
    AggregationRule("predicted_funding", "last non-null in bucket"),
    AggregationRule("next_funding_time", "last non-null in bucket"),
    AggregationRule("micro_price_close", "last non-null in bucket"),
    AggregationRule("mark_price_open", "first non-null in bucket"),
    AggregationRule("mark_price_close", "last non-null in bucket"),
    AggregationRule("index_price_open", "first non-null in bucket"),
    AggregationRule("index_price_close", "last non-null in bucket"),
    AggregationRule("avg_spread_usdt", "weighted average by volume_usdt, fallback simple mean"),
    AggregationRule("bid_ask_imbalance", "weighted average by volume_usdt, fallback simple mean"),
    AggregationRule("avg_bid_depth", "weighted average by volume_usdt, fallback simple mean"),
    AggregationRule("avg_ask_depth", "weighted average by volume_usdt, fallback simple mean"),
    AggregationRule("spread_pct", "weighted average by volume_usdt, fallback simple mean"),
    AggregationRule("price_impact_100k", "weighted average by volume_usdt, fallback simple mean"),
    AggregationRule("has_depth", "OR"),
    AggregationRule("impact_fillable", "OR"),
    AggregationRule("depth_degraded", "OR"),
    AggregationRule("has_ws_latency", "OR"),
    AggregationRule("ws_latency_bad", "OR"),
    AggregationRule("has_ls_ratio", "OR"),
    AggregationRule("realized_vol_htf", "sqrt(sum(close-to-close log return^2 within bucket))"),
    AggregationRule("event_time", "max"),
    AggregationRule("transact_time", "max"),
    AggregationRule("arrival_time", "max"),
    AggregationRule("update_id_start", "min non-null"),
    AggregationRule("update_id_end", "max non-null"),
    AggregationRule("timeframe", "literal timeframe label"),
    AggregationRule("symbol", "literal symbol label"),
    AggregationRule("bucket_start", "deterministic UTC bucket start"),
    AggregationRule("bucket_end", "exclusive bucket end"),
    AggregationRule("expected_minutes_in_bucket", "derived from bucket width"),
    AggregationRule("observed_minutes_in_bucket", "count of unique source minutes observed"),
    AggregationRule("missing_minutes_count", "expected - observed"),
    AggregationRule("bucket_complete", "observed == expected"),
)

_BOOL_COLUMNS = (
    "has_depth",
    "impact_fillable",
    "depth_degraded",
    "has_ws_latency",
    "ws_latency_bad",
    "has_ls_ratio",
)

_SNAPSHOT_LAST_COLUMNS = (
    "oi_contracts",
    "oi_value_usdt",
    "top_trader_ls_ratio_acct",
    "global_ls_ratio_acct",
    "ls_ratio_divergence",
    "top_trader_long_pct",
    "top_trader_short_pct",
    "premium_index",
    "funding_rate",
    "predicted_funding",
    "next_funding_time",
    "micro_price_close",
)

_WEIGHTED_BY_VOLUME_COLUMNS = (
    "avg_spread_usdt",
    "bid_ask_imbalance",
    "avg_bid_depth",
    "avg_ask_depth",
    "spread_pct",
    "price_impact_100k",
)


def aggregation_rule_table() -> list[dict[str, str]]:
    return [{"column": rule.column, "rule": rule.rule} for rule in AGGREGATION_RULES]


def aggregate_minutes(frame: pl.DataFrame, spec: TimeframeSpec, symbol: str) -> pl.DataFrame:
    if frame.height == 0:
        return pl.DataFrame(schema=_target_schema())

    ordered = (
        frame.sort("timestamp")
        .with_columns(
            bucket_start_expr(spec),
            (
                pl.col("liq_long_vol_usdt").fill_null(0.0) + pl.col("liq_short_vol_usdt").fill_null(0.0)
            ).alias("_liq_weight"),
        )
        .with_columns(
            pl.col("close").shift(1).over("bucket_start").alias("_prev_close"),
            *[
                (
                    pl.when(pl.col(column).is_not_null() & pl.col("volume_usdt").is_not_null())
                    .then(pl.col(column) * pl.col("volume_usdt"))
                    .otherwise(None)
                ).alias(f"_{column}_weighted")
                for column in _WEIGHTED_BY_VOLUME_COLUMNS
            ],
            *[
                (
                    pl.when(pl.col(column).is_not_null() & pl.col("volume_usdt").is_not_null())
                    .then(pl.col("volume_usdt"))
                    .otherwise(None)
                ).alias(f"_{column}_weight")
                for column in _WEIGHTED_BY_VOLUME_COLUMNS
            ],
            (pl.col("liq_avg_fill_price") * pl.col("_liq_weight")).alias("_liq_avg_fill_price_weighted"),
            (pl.col("liq_unfilled_ratio") * pl.col("_liq_weight")).alias("_liq_unfilled_ratio_weighted"),
        )
        .with_columns(
            pl.when((pl.col("_prev_close") > 0) & (pl.col("close") > 0))
            .then((pl.col("close") / pl.col("_prev_close")).log().pow(2))
            .otherwise(None)
            .alias("_log_return_sq")
        )
    )

    grouped = ordered.group_by("bucket_start", maintain_order=True).agg(
        pl.len().alias("observed_minutes_in_bucket"),
        pl.col("open").first().alias("open"),
        pl.col("high").max().alias("high"),
        pl.col("low").min().alias("low"),
        pl.col("close").last().alias("close"),
        pl.col("volume_btc").fill_null(0.0).sum().alias("volume_btc"),
        pl.col("volume_usdt").fill_null(0.0).sum().alias("volume_usdt"),
        pl.col("trade_count").fill_null(0).sum().alias("trade_count"),
        pl.col("max_trade_size_btc").max().alias("max_trade_size_btc"),
        pl.col("taker_buy_vol_btc").fill_null(0.0).sum().alias("taker_buy_vol_btc"),
        pl.col("taker_buy_vol_usdt").fill_null(0.0).sum().alias("taker_buy_vol_usdt"),
        pl.col("net_taker_vol_btc").fill_null(0.0).sum().alias("net_taker_vol_btc"),
        pl.col("count_buy_trades").fill_null(0).sum().alias("count_buy_trades"),
        pl.col("count_sell_trades").fill_null(0).sum().alias("count_sell_trades"),
        pl.col("vol_buy_whale_btc").fill_null(0.0).sum().alias("vol_buy_whale_btc"),
        pl.col("vol_sell_whale_btc").fill_null(0.0).sum().alias("vol_sell_whale_btc"),
        pl.col("vol_buy_retail_btc").fill_null(0.0).sum().alias("vol_buy_retail_btc"),
        pl.col("vol_sell_retail_btc").fill_null(0.0).sum().alias("vol_sell_retail_btc"),
        pl.col("whale_trade_count").fill_null(0).sum().alias("whale_trade_count"),
        pl.col("liq_long_vol_usdt").fill_null(0.0).sum().alias("liq_long_vol_usdt"),
        pl.col("liq_short_vol_usdt").fill_null(0.0).sum().alias("liq_short_vol_usdt"),
        pl.col("liq_long_count").fill_null(0).sum().alias("liq_long_count"),
        pl.col("liq_short_count").fill_null(0).sum().alias("liq_short_count"),
        pl.col("_liq_weight").sum().alias("_liq_weight_sum"),
        pl.col("_liq_avg_fill_price_weighted").sum().alias("_liq_avg_fill_price_sum"),
        pl.col("_liq_unfilled_ratio_weighted").sum().alias("_liq_unfilled_ratio_sum"),
        pl.col("liq_unfilled_supported").fill_null(False).any().alias("liq_unfilled_supported"),
        pl.col("has_liq").fill_null(False).any().alias("has_liq"),
        pl.col("mark_price_open").drop_nulls().first().alias("mark_price_open"),
        pl.col("mark_price_close").drop_nulls().last().alias("mark_price_close"),
        pl.col("index_price_open").drop_nulls().first().alias("index_price_open"),
        pl.col("index_price_close").drop_nulls().last().alias("index_price_close"),
        *[pl.col(column).drop_nulls().last().alias(column) for column in _SNAPSHOT_LAST_COLUMNS],
        *[pl.col(column).fill_null(False).any().alias(column) for column in _BOOL_COLUMNS],
        *[
            pl.col(f"_{column}_weighted").sum().alias(f"_{column}_weighted_sum")
            for column in _WEIGHTED_BY_VOLUME_COLUMNS
        ],
        *[
            pl.col(f"_{column}_weight").sum().alias(f"_{column}_weight_sum")
            for column in _WEIGHTED_BY_VOLUME_COLUMNS
        ],
        *[
            pl.col(column).mean().alias(f"_{column}_mean")
            for column in _WEIGHTED_BY_VOLUME_COLUMNS
        ],
        pl.col("event_time").max().alias("event_time"),
        pl.col("transact_time").max().alias("transact_time"),
        pl.col("arrival_time").max().alias("arrival_time"),
        pl.col("update_id_start").min().alias("update_id_start"),
        pl.col("update_id_end").max().alias("update_id_end"),
        pl.col("_log_return_sq").sum().alias("_realized_var"),
    )

    enriched = grouped.with_columns(
        bucket_end_expr(spec),
        expected_minutes_expr(spec),
    ).with_columns(
        pl.lit(spec.name).alias("timeframe"),
        pl.lit(symbol).alias("symbol"),
        pl.col("bucket_start").alias("timestamp"),
        (pl.col("expected_minutes_in_bucket") - pl.col("observed_minutes_in_bucket")).alias("missing_minutes_count"),
        (pl.col("observed_minutes_in_bucket") == pl.col("expected_minutes_in_bucket")).alias("bucket_complete"),
        pl.when(pl.col("volume_btc") > 0)
        .then(pl.col("volume_usdt") / pl.col("volume_btc"))
        .otherwise(None)
        .alias("vwap"),
        pl.when(pl.col("trade_count") > 0)
        .then(pl.col("volume_btc") / pl.col("trade_count"))
        .otherwise(None)
        .alias("avg_trade_size_btc"),
        pl.when(pl.col("volume_btc") > 0)
        .then(pl.col("taker_buy_vol_btc") / pl.col("volume_btc"))
        .otherwise(None)
        .alias("taker_buy_ratio"),
        pl.when(pl.col("_liq_weight_sum") > 0)
        .then(pl.col("_liq_avg_fill_price_sum") / pl.col("_liq_weight_sum"))
        .otherwise(None)
        .alias("liq_avg_fill_price"),
        pl.when(pl.col("_liq_weight_sum") > 0)
        .then(pl.col("_liq_unfilled_ratio_sum") / pl.col("_liq_weight_sum"))
        .otherwise(None)
        .alias("liq_unfilled_ratio"),
        pl.col("_realized_var").fill_null(0.0).sqrt().alias("realized_vol_htf"),
        *[
            pl.when(pl.col(f"_{column}_weight_sum") > 0)
            .then(pl.col(f"_{column}_weighted_sum") / pl.col(f"_{column}_weight_sum"))
            .otherwise(pl.col(f"_{column}_mean"))
            .alias(column)
            for column in _WEIGHTED_BY_VOLUME_COLUMNS
        ],
    ).drop(
        "_liq_weight_sum",
        "_liq_avg_fill_price_sum",
        "_liq_unfilled_ratio_sum",
        "_realized_var",
        *[f"_{column}_weighted_sum" for column in _WEIGHTED_BY_VOLUME_COLUMNS],
        *[f"_{column}_weight_sum" for column in _WEIGHTED_BY_VOLUME_COLUMNS],
        *[f"_{column}_mean" for column in _WEIGHTED_BY_VOLUME_COLUMNS],
    )

    return enriched.select(list(_target_schema())).sort("bucket_start")


def _target_schema() -> dict[str, pl.DataType]:
    return {
        "timeframe": pl.String,
        "symbol": pl.String,
        "timestamp": pl.Datetime("ms", "UTC"),
        "bucket_start": pl.Datetime("ms", "UTC"),
        "bucket_end": pl.Datetime("ms", "UTC"),
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume_btc": pl.Float64,
        "volume_usdt": pl.Float64,
        "trade_count": pl.Int64,
        "vwap": pl.Float64,
        "avg_trade_size_btc": pl.Float64,
        "max_trade_size_btc": pl.Float64,
        "taker_buy_vol_btc": pl.Float64,
        "taker_buy_vol_usdt": pl.Float64,
        "net_taker_vol_btc": pl.Float64,
        "count_buy_trades": pl.Int64,
        "count_sell_trades": pl.Int64,
        "taker_buy_ratio": pl.Float64,
        "vol_buy_whale_btc": pl.Float64,
        "vol_sell_whale_btc": pl.Float64,
        "vol_buy_retail_btc": pl.Float64,
        "vol_sell_retail_btc": pl.Float64,
        "whale_trade_count": pl.Int64,
        "liq_long_vol_usdt": pl.Float64,
        "liq_short_vol_usdt": pl.Float64,
        "liq_long_count": pl.Int64,
        "liq_short_count": pl.Int64,
        "liq_avg_fill_price": pl.Float64,
        "liq_unfilled_ratio": pl.Float64,
        "liq_unfilled_supported": pl.Boolean,
        "has_liq": pl.Boolean,
        "oi_contracts": pl.Float64,
        "oi_value_usdt": pl.Float64,
        "top_trader_ls_ratio_acct": pl.Float64,
        "global_ls_ratio_acct": pl.Float64,
        "ls_ratio_divergence": pl.Float64,
        "top_trader_long_pct": pl.Float64,
        "top_trader_short_pct": pl.Float64,
        "premium_index": pl.Float64,
        "funding_rate": pl.Float64,
        "predicted_funding": pl.Float64,
        "next_funding_time": pl.Int64,
        "micro_price_close": pl.Float64,
        "mark_price_open": pl.Float64,
        "mark_price_close": pl.Float64,
        "index_price_open": pl.Float64,
        "index_price_close": pl.Float64,
        "avg_spread_usdt": pl.Float64,
        "bid_ask_imbalance": pl.Float64,
        "avg_bid_depth": pl.Float64,
        "avg_ask_depth": pl.Float64,
        "spread_pct": pl.Float64,
        "price_impact_100k": pl.Float64,
        "has_depth": pl.Boolean,
        "impact_fillable": pl.Boolean,
        "depth_degraded": pl.Boolean,
        "has_ws_latency": pl.Boolean,
        "ws_latency_bad": pl.Boolean,
        "has_ls_ratio": pl.Boolean,
        "realized_vol_htf": pl.Float64,
        "event_time": pl.Int64,
        "transact_time": pl.Int64,
        "arrival_time": pl.Int64,
        "update_id_start": pl.Int64,
        "update_id_end": pl.Int64,
        "expected_minutes_in_bucket": pl.Int64,
        "observed_minutes_in_bucket": pl.Int64,
        "missing_minutes_count": pl.Int64,
        "bucket_complete": pl.Boolean,
    }
