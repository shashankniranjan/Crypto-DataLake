from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import polars as pl

from .enums import SupportClass


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    name: str
    dtype: str
    source: str
    support_class: SupportClass
    fill_policy: str


_CANONICAL_COLUMNS: Final[tuple[ColumnSpec, ...]] = (
    ColumnSpec("has_ws_latency", "Bool", "coverage", SupportClass.LIVE_ONLY, "False when unavailable"),
    ColumnSpec("has_depth", "Bool", "coverage", SupportClass.LIVE_ONLY, "False when unavailable"),
    ColumnSpec("has_liq", "Bool", "coverage", SupportClass.LIVE_ONLY, "False when unavailable"),
    ColumnSpec("has_ls_ratio", "Bool", "coverage", SupportClass.BACKFILL_AVAILABLE, "False when unavailable"),
    ColumnSpec("event_time", "BigInt", "websocket", SupportClass.LIVE_ONLY, "NULL if not collected"),
    ColumnSpec(
        "transact_time",
        "BigInt",
        "agg_trades_or_mark_price",
        SupportClass.BACKFILL_AVAILABLE,
        "NULL if no trade",
    ),
    ColumnSpec("arrival_time", "BigInt", "local_capture", SupportClass.LIVE_ONLY, "NULL historically"),
    ColumnSpec("latency_engine", "Int", "derived", SupportClass.LIVE_ONLY, "NULL if missing inputs"),
    ColumnSpec("latency_network", "Int", "derived", SupportClass.LIVE_ONLY, "NULL if missing inputs"),
    ColumnSpec("ws_latency_bad", "Bool", "derived", SupportClass.LIVE_ONLY, "False unless out-of-range"),
    ColumnSpec("update_id_start", "BigInt", "depth_update", SupportClass.LIVE_ONLY, "NULL if no depth"),
    ColumnSpec("update_id_end", "BigInt", "depth_update", SupportClass.LIVE_ONLY, "NULL if no depth"),
    ColumnSpec("timestamp", "Datetime", "klines", SupportClass.HARD_REQUIRED, "no nulls"),
    ColumnSpec("open", "Float", "klines", SupportClass.HARD_REQUIRED, "no nulls"),
    ColumnSpec("high", "Float", "klines", SupportClass.HARD_REQUIRED, "no nulls"),
    ColumnSpec("low", "Float", "klines", SupportClass.HARD_REQUIRED, "no nulls"),
    ColumnSpec("close", "Float", "klines", SupportClass.HARD_REQUIRED, "no nulls"),
    ColumnSpec("vwap_1m", "Float", "agg_trades", SupportClass.BACKFILL_AVAILABLE, "close if no qty"),
    ColumnSpec(
        "micro_price_close",
        "Float",
        "book_ticker",
        SupportClass.BACKFILL_AVAILABLE,
        "NULL if no snapshot",
    ),
    ColumnSpec("volume_btc", "Float", "klines", SupportClass.HARD_REQUIRED, "0 allowed"),
    ColumnSpec("volume_usdt", "Float", "klines", SupportClass.HARD_REQUIRED, "0 allowed"),
    ColumnSpec("trade_count", "Int", "klines", SupportClass.HARD_REQUIRED, "0 allowed"),
    ColumnSpec(
        "avg_trade_size_btc",
        "Float",
        "derived",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if trade_count=0",
    ),
    ColumnSpec(
        "max_trade_size_btc",
        "Float",
        "agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if no trades",
    ),
    ColumnSpec(
        "taker_buy_vol_btc",
        "Float",
        "klines_or_agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if none",
    ),
    ColumnSpec(
        "taker_buy_vol_usdt",
        "Float",
        "klines_or_agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if none",
    ),
    ColumnSpec("net_taker_vol_btc", "Float", "agg_trades", SupportClass.BACKFILL_AVAILABLE, "0 if none"),
    ColumnSpec("count_buy_trades", "Int", "agg_trades", SupportClass.BACKFILL_AVAILABLE, "0 if none"),
    ColumnSpec("count_sell_trades", "Int", "agg_trades", SupportClass.BACKFILL_AVAILABLE, "0 if none"),
    ColumnSpec(
        "taker_buy_ratio",
        "Float",
        "derived",
        SupportClass.BACKFILL_AVAILABLE,
        "NULL if denom=0",
    ),
    ColumnSpec(
        "vol_buy_whale_btc",
        "Float",
        "agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if none",
    ),
    ColumnSpec(
        "vol_sell_whale_btc",
        "Float",
        "agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if none",
    ),
    ColumnSpec(
        "vol_buy_retail_btc",
        "Float",
        "agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if none",
    ),
    ColumnSpec(
        "vol_sell_retail_btc",
        "Float",
        "agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if none",
    ),
    ColumnSpec(
        "whale_trade_count",
        "Int",
        "agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if none",
    ),
    ColumnSpec(
        "realized_vol_1m",
        "Float",
        "agg_trades",
        SupportClass.BACKFILL_AVAILABLE,
        "0 if <2 ticks",
    ),
    ColumnSpec(
        "liq_long_vol_usdt",
        "Float",
        "force_order_ws_rest",
        SupportClass.LIVE_ONLY,
        "NULL unless collected",
    ),
    ColumnSpec(
        "liq_short_vol_usdt",
        "Float",
        "force_order_ws_rest",
        SupportClass.LIVE_ONLY,
        "NULL unless collected",
    ),
    ColumnSpec(
        "liq_long_count",
        "Int",
        "force_order_ws_rest",
        SupportClass.LIVE_ONLY,
        "NULL unless collected",
    ),
    ColumnSpec(
        "liq_short_count",
        "Int",
        "force_order_ws_rest",
        SupportClass.LIVE_ONLY,
        "NULL unless collected",
    ),
    ColumnSpec(
        "liq_avg_fill_price",
        "Float",
        "force_order",
        SupportClass.LIVE_ONLY,
        "NULL unless collected",
    ),
    ColumnSpec(
        "liq_unfilled_ratio",
        "Float",
        "force_order",
        SupportClass.LIVE_ONLY,
        "NULL unless collected",
    ),
    ColumnSpec(
        "liq_unfilled_supported",
        "Bool",
        "force_order",
        SupportClass.LIVE_ONLY,
        "False when unfilled semantics unavailable",
    ),
    ColumnSpec(
        "avg_spread_usdt",
        "Float",
        "book_ticker",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "bid_ask_imbalance",
        "Float",
        "book_ticker",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "avg_bid_depth",
        "Float",
        "book_ticker",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "avg_ask_depth",
        "Float",
        "book_ticker",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "spread_pct",
        "Float",
        "book_ticker",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "price_impact_100k",
        "Float",
        "depth_book",
        SupportClass.LIVE_ONLY,
        "NULL unless collected",
    ),
    ColumnSpec("impact_fillable", "Bool", "depth_book", SupportClass.LIVE_ONLY, "NULL unless collected"),
    ColumnSpec("depth_degraded", "Bool", "depth_sync", SupportClass.LIVE_ONLY, "False unless sync degraded"),
    ColumnSpec(
        "oi_contracts",
        "Float",
        "rest_or_metrics",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "oi_value_usdt",
        "Float",
        "rest_or_metrics",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "top_trader_ls_ratio_acct",
        "Float",
        "rest_or_metrics",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "global_ls_ratio_acct",
        "Float",
        "rest_or_metrics",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "ls_ratio_divergence",
        "Float",
        "derived",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "top_trader_long_pct",
        "Float",
        "rest_or_metrics",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "top_trader_short_pct",
        "Float",
        "rest_or_metrics",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill within limit",
    ),
    ColumnSpec(
        "mark_price_open",
        "Float",
        "mark_price_klines",
        SupportClass.HARD_REQUIRED,
        "no nulls",
    ),
    ColumnSpec(
        "mark_price_close",
        "Float",
        "mark_price_klines",
        SupportClass.HARD_REQUIRED,
        "no nulls",
    ),
    ColumnSpec(
        "index_price_open",
        "Float",
        "index_price_klines",
        SupportClass.HARD_REQUIRED,
        "no nulls",
    ),
    ColumnSpec(
        "index_price_close",
        "Float",
        "index_price_klines",
        SupportClass.HARD_REQUIRED,
        "no nulls",
    ),
    ColumnSpec(
        "premium_index",
        "Float",
        "premium_or_index_or_mark",
        SupportClass.BACKFILL_AVAILABLE,
        "computed; no nulls if inputs available",
    ),
    ColumnSpec(
        "funding_rate",
        "Float",
        "funding_rate_rest_or_premium_index",
        SupportClass.BACKFILL_AVAILABLE,
        "ffill settles every 8h",
    ),
    ColumnSpec(
        "predicted_funding",
        "Float",
        "ws_mark_price_or_premium_index_rest",
        SupportClass.LIVE_ONLY,
        "NULL historically",
    ),
    ColumnSpec(
        "next_funding_time",
        "BigInt",
        "ws_mark_price_or_premium_index_rest",
        SupportClass.LIVE_ONLY,
        "NULL historically",
    ),
)


_TYPE_TO_POLARS: Final[dict[str, pl.DataType]] = {
    "BigInt": pl.Int64(),
    "Int": pl.Int64(),
    "Float": pl.Float64(),
    "Bool": pl.Boolean(),
    "Datetime": pl.Datetime("ms", time_zone="UTC"),
}


def canonical_columns() -> tuple[ColumnSpec, ...]:
    return _CANONICAL_COLUMNS


def canonical_column_names() -> list[str]:
    return [column.name for column in _CANONICAL_COLUMNS]


def hard_required_columns() -> list[str]:
    return [
        column.name
        for column in _CANONICAL_COLUMNS
        if column.support_class == SupportClass.HARD_REQUIRED
    ]


def dtype_map() -> dict[str, pl.DataType]:
    return {column.name: _TYPE_TO_POLARS[column.dtype] for column in _CANONICAL_COLUMNS}


def schema_hash_input() -> str:
    rows = [
        f"{col.name}|{col.dtype}|{col.source}|{col.support_class.value}|{col.fill_policy}"
        for col in _CANONICAL_COLUMNS
    ]
    return "\n".join(rows)
