from __future__ import annotations

import polars as pl

from .timeframes import TimeframeSpec
from .utils import cast_canonical_frame, empty_canonical_frame


def aggregate_canonical_frame(frame: pl.DataFrame, spec: TimeframeSpec, *, limit: int) -> pl.DataFrame:
    if frame.height == 0:
        return empty_canonical_frame()

    # Forward-fill snapshot metrics so every minute carries the most recent known value.
    # These are point-in-time observations (funding every 8h, OI on REST poll, etc.) that
    # must propagate across all candles in the aggregation window.
    frame = frame.sort("timestamp").with_columns(
        pl.col("funding_rate").forward_fill(),
        pl.col("oi_contracts").forward_fill(),
        pl.col("oi_value_usdt").forward_fill(),
        pl.col("top_trader_ls_ratio_acct").forward_fill(),
        pl.col("global_ls_ratio_acct").forward_fill(),
        pl.col("top_trader_long_pct").forward_fill(),
        pl.col("top_trader_short_pct").forward_fill(),
    )

    weighted_vwap_notional = (
        pl.when(pl.col("vwap_1m").is_not_null())
        .then(pl.col("vwap_1m"))
        .otherwise(pl.col("close"))
        * pl.col("volume_btc").fill_null(0.0)
    )
    liq_total_notional = pl.col("liq_long_vol_usdt").fill_null(0.0) + pl.col("liq_short_vol_usdt").fill_null(0.0)
    liq_estimated_qty = (
        pl.when(pl.col("liq_avg_fill_price").is_not_null() & (pl.col("liq_avg_fill_price") > 0))
        .then(liq_total_notional / pl.col("liq_avg_fill_price"))
        .otherwise(0.0)
    )

    grouped = (
        frame.sort("timestamp")
        .group_by_dynamic(
            "timestamp",
            every=spec.polars_every,
            period=spec.polars_every,
            closed="left",
            label="left",
            start_by="window",
        )
        .agg(
            pl.len().alias("_minute_count"),
            pl.col("has_ws_latency").fill_null(False).any().alias("has_ws_latency"),
            pl.col("has_depth").fill_null(False).any().alias("has_depth"),
            pl.col("has_liq").fill_null(False).any().alias("has_liq"),
            pl.col("has_ls_ratio").fill_null(False).any().alias("has_ls_ratio"),
            pl.col("event_time").max().alias("event_time"),
            pl.col("transact_time").max().alias("transact_time"),
            pl.col("arrival_time").max().alias("arrival_time"),
            pl.col("latency_engine").quantile(0.95, interpolation="nearest").alias("latency_engine"),
            pl.col("latency_network").quantile(0.95, interpolation="nearest").alias("latency_network"),
            pl.col("ws_latency_bad").fill_null(False).any().alias("_ws_latency_bad"),
            pl.col("update_id_start").min().alias("update_id_start"),
            pl.col("update_id_end").max().alias("update_id_end"),
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            weighted_vwap_notional.sum().alias("_vwap_notional"),
            pl.col("micro_price_close").last().alias("micro_price_close"),
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
            pl.col("realized_vol_1m").fill_null(0.0).pow(2).sum().alias("_realized_var"),
            pl.col("liq_long_vol_usdt").fill_null(0.0).sum().alias("_liq_long_vol_usdt"),
            pl.col("liq_short_vol_usdt").fill_null(0.0).sum().alias("_liq_short_vol_usdt"),
            pl.col("liq_long_count").fill_null(0).sum().alias("_liq_long_count"),
            pl.col("liq_short_count").fill_null(0).sum().alias("_liq_short_count"),
            liq_total_notional.sum().alias("_liq_total_notional"),
            liq_estimated_qty.sum().alias("_liq_estimated_qty"),
            pl.col("liq_unfilled_ratio").mean().alias("_liq_unfilled_ratio"),
            pl.col("liq_unfilled_supported").fill_null(False).any().alias("_liq_unfilled_supported"),
            pl.col("avg_spread_usdt").mean().alias("avg_spread_usdt"),
            pl.col("bid_ask_imbalance").mean().alias("bid_ask_imbalance"),
            pl.col("avg_bid_depth").mean().alias("avg_bid_depth"),
            pl.col("avg_ask_depth").mean().alias("avg_ask_depth"),
            pl.col("spread_pct").mean().alias("spread_pct"),
            pl.col("price_impact_100k").last().alias("price_impact_100k"),
            pl.col("impact_fillable").last().alias("_impact_fillable"),
            pl.col("depth_degraded").fill_null(False).any().alias("_depth_degraded"),
            pl.col("oi_contracts").last().alias("oi_contracts"),
            pl.col("oi_value_usdt").last().alias("oi_value_usdt"),
            pl.col("top_trader_ls_ratio_acct").last().alias("top_trader_ls_ratio_acct"),
            pl.col("global_ls_ratio_acct").last().alias("global_ls_ratio_acct"),
            pl.col("top_trader_long_pct").last().alias("top_trader_long_pct"),
            pl.col("top_trader_short_pct").last().alias("top_trader_short_pct"),
            pl.col("mark_price_open").first().alias("mark_price_open"),
            pl.col("mark_price_close").last().alias("mark_price_close"),
            pl.col("index_price_open").first().alias("index_price_open"),
            pl.col("index_price_close").last().alias("index_price_close"),
            pl.col("funding_rate").last().alias("funding_rate"),
            pl.col("predicted_funding").last().alias("predicted_funding"),
            pl.col("next_funding_time").last().alias("next_funding_time"),
        )
        .filter(pl.col("_minute_count") == spec.minutes)
        .with_columns(
            pl.when(pl.col("volume_btc") > 0)
            .then(pl.col("_vwap_notional") / pl.col("volume_btc"))
            .otherwise(pl.col("close"))
            .alias("vwap_1m"),
            pl.when(pl.col("trade_count") > 0)
            .then(pl.col("volume_btc") / pl.col("trade_count"))
            .otherwise(0.0)
            .alias("avg_trade_size_btc"),
            pl.when(pl.col("volume_btc") > 0)
            .then(pl.col("taker_buy_vol_btc") / pl.col("volume_btc"))
            .otherwise(None)
            .alias("taker_buy_ratio"),
            pl.col("_realized_var").sqrt().alias("realized_vol_1m"),
            pl.when(pl.col("has_ws_latency")).then(pl.col("_ws_latency_bad")).otherwise(None).alias("ws_latency_bad"),
            pl.when(pl.col("has_depth")).then(pl.col("_impact_fillable")).otherwise(None).alias("impact_fillable"),
            pl.when(pl.col("has_depth")).then(pl.col("_depth_degraded")).otherwise(None).alias("depth_degraded"),
            pl.when(pl.col("has_liq")).then(pl.col("_liq_long_vol_usdt")).otherwise(None).alias("liq_long_vol_usdt"),
            pl.when(pl.col("has_liq")).then(pl.col("_liq_short_vol_usdt")).otherwise(None).alias("liq_short_vol_usdt"),
            pl.when(pl.col("has_liq")).then(pl.col("_liq_long_count")).otherwise(None).alias("liq_long_count"),
            pl.when(pl.col("has_liq")).then(pl.col("_liq_short_count")).otherwise(None).alias("liq_short_count"),
            pl.when(pl.col("has_liq") & (pl.col("_liq_estimated_qty") > 0))
            .then(pl.col("_liq_total_notional") / pl.col("_liq_estimated_qty"))
            .otherwise(None)
            .alias("liq_avg_fill_price"),
            pl.when(pl.col("has_liq")).then(pl.col("_liq_unfilled_supported")).otherwise(None).alias("liq_unfilled_supported"),
            pl.when(pl.col("has_liq") & pl.col("_liq_unfilled_supported"))
            .then(pl.col("_liq_unfilled_ratio"))
            .otherwise(None)
            .alias("liq_unfilled_ratio"),
            pl.when(pl.col("index_price_close").is_not_null() & (pl.col("index_price_close") != 0))
            .then((pl.col("mark_price_close") / pl.col("index_price_close")) - 1.0)
            .otherwise(None)
            .alias("premium_index"),
            pl.when(pl.col("top_trader_ls_ratio_acct").is_not_null() & pl.col("global_ls_ratio_acct").is_not_null())
            .then(pl.col("top_trader_ls_ratio_acct") - pl.col("global_ls_ratio_acct"))
            .otherwise(None)
            .alias("ls_ratio_divergence"),
        )
        .drop(
            "_minute_count",
            "_vwap_notional",
            "_realized_var",
            "_ws_latency_bad",
            "_impact_fillable",
            "_depth_degraded",
            "_liq_long_vol_usdt",
            "_liq_short_vol_usdt",
            "_liq_long_count",
            "_liq_short_count",
            "_liq_total_notional",
            "_liq_estimated_qty",
            "_liq_unfilled_ratio",
            "_liq_unfilled_supported",
        )
        .sort("timestamp")
        .tail(limit)
    )
    result = cast_canonical_frame(grouped)

    # Derive global long/short percentages from the stored ratio, and compute
    # USD-denominated position sizes (coinglass-style net longs/shorts).
    result = result.with_columns(
        pl.when(
            pl.col("global_ls_ratio_acct").is_not_null() & (pl.col("global_ls_ratio_acct") > 0)
        )
        .then(pl.col("global_ls_ratio_acct") / (1.0 + pl.col("global_ls_ratio_acct")))
        .otherwise(None)
        .alias("global_long_pct"),

        pl.when(
            pl.col("global_ls_ratio_acct").is_not_null() & (pl.col("global_ls_ratio_acct") > 0)
        )
        .then(1.0 / (1.0 + pl.col("global_ls_ratio_acct")))
        .otherwise(None)
        .alias("global_short_pct"),
    )
    result = result.with_columns(
        pl.when(
            pl.col("oi_value_usdt").is_not_null() & pl.col("top_trader_long_pct").is_not_null()
        )
        .then(pl.col("oi_value_usdt") * pl.col("top_trader_long_pct"))
        .otherwise(None)
        .alias("top_trader_long_usd"),

        pl.when(
            pl.col("oi_value_usdt").is_not_null() & pl.col("top_trader_short_pct").is_not_null()
        )
        .then(pl.col("oi_value_usdt") * pl.col("top_trader_short_pct"))
        .otherwise(None)
        .alias("top_trader_short_usd"),

        pl.when(
            pl.col("oi_value_usdt").is_not_null() & pl.col("global_long_pct").is_not_null()
        )
        .then(pl.col("oi_value_usdt") * pl.col("global_long_pct"))
        .otherwise(None)
        .alias("global_long_usd"),

        pl.when(
            pl.col("oi_value_usdt").is_not_null() & pl.col("global_short_pct").is_not_null()
        )
        .then(pl.col("oi_value_usdt") * pl.col("global_short_pct"))
        .otherwise(None)
        .alias("global_short_usd"),
    )

    # CVD: cumulative sum of net_taker_vol_btc across the returned window.
    # Resets to 0 at the start of the response window — gives sequence context
    # that individual per-bar net flow cannot.
    result = result.with_columns(
        pl.col("net_taker_vol_btc").fill_null(0.0).cum_sum().alias("cvd_btc"),
    )

    return result
