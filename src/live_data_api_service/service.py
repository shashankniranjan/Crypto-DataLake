from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Any, cast

import polars as pl

from binance_minute_lake.state.store import SQLiteStateStore

from .aggregation import aggregate_canonical_frame
from .alignment import AlignmentMode, align_series, normalize_bar_timestamp
from .capabilities import (
    BINANCE_LS_RATIO_TFS,
    BINANCE_OI_HIST_TFS,
    FetchPlannerConfig,
    plan_timeframe_fetch,
    response_capability_matrix,
)
from .repository import MinuteLakeRepository
from .timeframes import TimeframeSpec, normalize_symbol, parse_timeframe_requests, requested_window_start
from .utils import (
    cast_canonical_frame,
    empty_canonical_frame,
    expected_minute_count,
    floor_utc_minute,
    last_completed_utc_minute,
    merge_canonical_frames,
    parse_iso_datetime,
    serialize_frame,
)

LOGGER = logging.getLogger(__name__)
AUXILIARY_FETCH_EXTRA_ROWS = 5


@dataclass(frozen=True, slots=True)
class CanonicalWindowResult:
    symbol: str
    start_time: datetime
    end_time: datetime
    source: str
    frame: pl.DataFrame
    live_overlay_used: bool = False


@dataclass(frozen=True, slots=True)
class TimeframeCandleResult:
    frame: pl.DataFrame
    metadata: dict[str, object]


class LiveDataApiService:
    def __init__(
        self,
        *,
        repository: MinuteLakeRepository,
        provider: object,
        default_limit: int,
        max_limit: int,
        on_demand_max_minutes: int,
        ws_manager: object | None = None,
        shared_live_symbol: str | None = None,
        shared_live_collector: object | None = None,
        state_store: SQLiteStateStore | None = None,
        local_watermark_tolerance_minutes: int | None = None,
        include_deprecated_fields: bool = False,
        enable_local_symbol_fastpath: bool = True,
        local_preferred_symbols: Iterable[str] | None = None,
        local_symbol_require_full_coverage: bool = False,
        local_symbol_allow_binance_patch: bool = True,
        enable_btc_complexity_guard: bool = True,
        btc_local_max_1m_bars: int = 500,
        btc_local_max_3m_bars: int = 300,
        btc_local_max_higher_tf_bars: int = 200,
        btc_force_binance_for_heavy_higher_tf: bool = True,
        fetch_planner_config: FetchPlannerConfig | None = None,
    ) -> None:
        self._repository = repository
        self._provider = provider
        self._default_limit = default_limit
        self._max_limit = max_limit
        self._on_demand_max_minutes = on_demand_max_minutes
        self._ws_manager = ws_manager
        self._shared_live_symbol = normalize_symbol(shared_live_symbol) if shared_live_symbol else None
        self._shared_live_collector = shared_live_collector
        self._state_store = state_store
        self._local_watermark_tolerance_minutes = local_watermark_tolerance_minutes
        self._include_deprecated_fields = include_deprecated_fields
        self._enable_local_symbol_fastpath = enable_local_symbol_fastpath
        raw_local_symbols = local_preferred_symbols if local_preferred_symbols is not None else ("BTCUSDT",)
        self._local_preferred_symbols = frozenset(
            normalize_symbol(symbol)
            for symbol in raw_local_symbols
            if str(symbol).strip()
        )
        self._local_symbol_require_full_coverage = local_symbol_require_full_coverage
        self._local_symbol_allow_binance_patch = local_symbol_allow_binance_patch
        self._enable_btc_complexity_guard = enable_btc_complexity_guard
        self._btc_local_max_1m_bars = btc_local_max_1m_bars
        self._btc_local_max_3m_bars = btc_local_max_3m_bars
        self._btc_local_max_higher_tf_bars = btc_local_max_higher_tf_bars
        self._btc_force_binance_for_heavy_higher_tf = btc_force_binance_for_heavy_higher_tf
        self._fetch_planner_config = fetch_planner_config or FetchPlannerConfig()

    def close(self) -> None:
        close = getattr(self._provider, "close", None)
        if callable(close):
            close()
        # WS manager lifecycle is owned by the app lifespan (or the singleton),
        # not by the service — do not stop it here.

    def _resolve_live_collector(self, symbol: str) -> object | None:
        if self._shared_live_collector is not None and symbol == self._shared_live_symbol:
            return self._shared_live_collector

        if self._ws_manager is None:
            return None

        get_collector = getattr(self._ws_manager, "get_collector", None)
        touch = getattr(self._ws_manager, "touch", None)

        # Non-shared symbols should warm the WS subscription on the first query,
        # but live-only fields only become eligible for the *next* request.
        if callable(get_collector):
            existing = get_collector(symbol)
            if existing is not None:
                if callable(touch):
                    return touch(symbol)
                return existing
            if callable(touch):
                touch(symbol)
            return None

        if callable(touch):
            touch(symbol)
        return None

    def _local_watermark_for(self, symbol: str) -> datetime | None:
        if self._state_store is None:
            return None
        try:
            watermark = self._state_store.get_watermark(symbol)
        except Exception:
            return None
        return floor_utc_minute(watermark) if watermark is not None else None

    def _is_local_preferred_symbol(self, symbol: str) -> bool:
        return self._enable_local_symbol_fastpath and symbol in self._local_preferred_symbols

    def _btc_local_complexity_notes(self, spec: TimeframeSpec, limit: int) -> list[str]:
        if not self._enable_btc_complexity_guard:
            return []
        if spec.minutes == 1 and limit > self._btc_local_max_1m_bars:
            return [
                "btc_local_path_skipped_due_to_request_complexity",
                "btc_1m_binance_fallback",
            ]
        if spec.minutes == 3 and limit > self._btc_local_max_3m_bars:
            return [
                "btc_local_path_skipped_due_to_request_complexity",
                "btc_3m_binance_fallback",
            ]
        if (
            spec.minutes > 3
            and self._btc_force_binance_for_heavy_higher_tf
            and limit > self._btc_local_max_higher_tf_bars
        ):
            return [
                "btc_local_path_skipped_due_to_request_complexity",
                "btc_higher_tf_binance_fallback",
                "btc_mixed_source_plan",
            ]
        return []

    def resolve_end_time(
        self,
        *,
        coin: str,
        end_time: str | datetime | None = None,
    ) -> datetime:
        if isinstance(end_time, str):
            return parse_iso_datetime(end_time) or last_completed_utc_minute()
        if end_time is not None:
            return floor_utc_minute(end_time)

        resolved_end = last_completed_utc_minute()
        watermark = self._local_watermark_for(normalize_symbol(coin))
        if watermark is None:
            return resolved_end

        effective_end = min(resolved_end, watermark)
        if self._local_watermark_tolerance_minutes is None:
            return effective_end

        if resolved_end - effective_end <= timedelta(minutes=self._local_watermark_tolerance_minutes):
            return effective_end
        return resolved_end

    @staticmethod
    def _overlay_live_features(
        frame: pl.DataFrame,
        live_collector: object,
        window_start: datetime,
        window_end: datetime,
    ) -> tuple[pl.DataFrame, bool]:
        """Overlay per-minute live WS features (liq, depth, mark price) onto the frame.

        The local parquet doesn't contain live WS data unless the ingestion
        pipeline is running alongside the API. This method fetches minute
        snapshots from the in-memory collector and merges them in.
        """
        from binance_minute_lake.transforms.minute_builder import build_live_feature_snapshot_frame

        # Use bulk range fetch when available — avoids N individual SQLite queries.
        snapshots_for_range = getattr(live_collector, "snapshots_for_range", None)
        if callable(snapshots_for_range):
            start_ms = int(window_start.timestamp() * 1000)
            end_ms = int(window_end.timestamp() * 1000)
            snapshots = snapshots_for_range(start_ms, end_ms)
            records = list(snapshots.values())
        else:
            snapshot_for = getattr(live_collector, "snapshot_for_minute", None)
            if not callable(snapshot_for):
                return frame, False
            # Fallback: per-minute loop (used when collector doesn't support bulk fetch).
            records = []
            cursor = window_start
            end = window_end
            while cursor <= end:
                snap = snapshot_for(int(cursor.timestamp() * 1000))
                if snap is not None:
                    records.append(snap)
                cursor += timedelta(minutes=1)

        if not records:
            return frame, False

        live_df = build_live_feature_snapshot_frame(records).drop("timestamp_ms")

        # Columns that are boolean flags — OR with existing.
        bool_or_cols = {"has_ws_latency", "has_depth", "has_liq", "has_ls_ratio"}
        # Nullable booleans / floats / ints — coalesce: prefer live if not null.
        coalesce_cols = {c for c in live_df.columns if c != "timestamp"} - bool_or_cols

        merged = frame.join(live_df, on="timestamp", how="left", suffix="_live")

        updates: list[pl.Expr] = []
        for col in bool_or_cols:
            live_col = f"{col}_live"
            if live_col in merged.columns and col in merged.columns:
                updates.append(
                    (pl.col(col) | pl.col(live_col).fill_null(False)).alias(col)
                )
        for col in coalesce_cols:
            live_col = f"{col}_live"
            if live_col in merged.columns and col in merged.columns:
                updates.append(
                    pl.coalesce([pl.col(live_col), pl.col(col)]).alias(col)
                )

        if updates:
            merged = merged.with_columns(updates)

        drop_cols = [c for c in merged.columns if c.endswith("_live")]
        return (merged.drop(drop_cols) if drop_cols else merged), True

    def load_canonical_window(
        self,
        *,
        coin: str,
        start_time: datetime,
        end_time: str | datetime | None = None,
        local_can_serve_predicate: Callable[[pl.DataFrame], bool] | None = None,
        allow_binance_patch: bool = True,
    ) -> CanonicalWindowResult:
        resolved_end = self.resolve_end_time(coin=coin, end_time=end_time)
        window_start = floor_utc_minute(start_time)
        if resolved_end < window_start:
            raise ValueError("end_time must be on or after start_time")

        symbol = normalize_symbol(coin)
        expected_rows = expected_minute_count(window_start, resolved_end)

        local_frame = self._repository.load_canonical_minutes(symbol, window_start, resolved_end)
        combined_frame = local_frame
        source = "local"

        # Prefer the shared minute-lake live store for its configured symbol.
        # Other symbols continue to warm up through the API process WS manager.
        live_collector = self._resolve_live_collector(symbol)

        local_can_serve = (
            local_can_serve_predicate(local_frame)
            if local_can_serve_predicate is not None
            else local_frame.height >= expected_rows
        )
        should_try_binance = allow_binance_patch and not local_can_serve and expected_rows <= self._on_demand_max_minutes
        if should_try_binance:
            provider_build = cast(Any, self._provider).build_canonical_minutes
            provider_signature = inspect.signature(provider_build)
            provider_kwargs = {}
            if live_collector is not None and "live_collector" in provider_signature.parameters:
                provider_kwargs["live_collector"] = live_collector
            remote_frame = provider_build(symbol, window_start, resolved_end, **provider_kwargs)
            if local_frame.height > 0:
                combined_frame = merge_canonical_frames(remote_frame, local_frame)
                source = "local+binance"
            else:
                combined_frame = remote_frame
                source = "binance"
        elif local_frame.height == 0 and expected_rows > self._on_demand_max_minutes:
            raise ValueError(
                "Requested window is not available locally and is too large for on-demand Binance retrieval. "
                "Reduce limit or materialize the symbol into the minute lake first."
            )

        # When local parquet serves the request, live WS data is NOT baked in
        # (only the ingestion pipeline does that). Overlay the collector's
        # per-minute live features so liq/depth/funding WS data shows up
        # regardless of which data source was used.
        live_overlay_used = False
        if live_collector is not None and combined_frame.height > 0:
            combined_frame, live_overlay_used = self._overlay_live_features(
                combined_frame,
                live_collector,
                window_start,
                resolved_end,
            )
            if live_overlay_used and self._is_local_preferred_symbol(symbol):
                source = _source_with_live(source)

        return CanonicalWindowResult(
            symbol=symbol,
            start_time=window_start,
            end_time=resolved_end,
            source=source,
            frame=cast_canonical_frame(combined_frame),
            live_overlay_used=live_overlay_used,
        )

    @staticmethod
    def _native_klines_to_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
        if not rows:
            return empty_canonical_frame()

        records: list[dict[str, object]] = []
        for row in rows:
            volume_btc = float(row.get("volume_btc") or 0.0)
            volume_usdt = float(row.get("volume_usdt") or 0.0)
            taker_buy_btc = float(row.get("taker_buy_vol_btc") or 0.0)
            taker_buy_usdt = float(row.get("taker_buy_vol_usdt") or 0.0)
            trade_count = int(row.get("trade_count") or 0)
            close = float(row["close"])
            records.append(
                {
                    "timestamp": datetime.fromtimestamp(int(row["open_time"]) / 1000, tz=UTC),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": close,
                    "vwap_1m": (volume_usdt / volume_btc) if volume_btc > 0 else close,
                    "volume_btc": volume_btc,
                    "volume_usdt": volume_usdt,
                    "trade_count": trade_count,
                    "avg_trade_size_btc": (volume_btc / trade_count) if trade_count > 0 else 0.0,
                    "taker_buy_vol_btc": taker_buy_btc,
                    "taker_buy_vol_usdt": taker_buy_usdt,
                    "net_taker_vol_btc": taker_buy_btc - max(volume_btc - taker_buy_btc, 0.0),
                    "taker_buy_ratio": (taker_buy_btc / volume_btc) if volume_btc > 0 else None,
                    "has_depth": False,
                    "has_liq": False,
                    "has_ws_latency": False,
                    "has_ls_ratio": False,
                }
            )
        return cast_canonical_frame(pl.DataFrame(records))

    @staticmethod
    def _overlay_native_oi_hist(frame: pl.DataFrame, rows: list[dict[str, object]]) -> pl.DataFrame:
        if frame.height == 0 or not rows:
            return frame
        return LiveDataApiService._align_exact_with_one_bar_fallback(
            frame,
            rows,
            source_time_col="create_time",
            value_map={"oi_contracts": "oi_contracts", "oi_value_usdt": "oi_value_usdt"},
            timeframe_minutes=_infer_frame_minutes(frame),
            notes=[],
            note_prefix="oi_hist",
        )

    @staticmethod
    def _add_native_derived_fields(frame: pl.DataFrame) -> pl.DataFrame:
        if frame.height == 0:
            return frame

        result = frame.sort("timestamp")
        result = result.with_columns(
            pl.when(pl.col("premium_index").is_not_null())
            .then(pl.col("premium_index"))
            .when(pl.col("index_price_close").is_not_null() & (pl.col("index_price_close") != 0))
            .then((pl.col("mark_price_close") / pl.col("index_price_close")) - 1.0)
            .otherwise(None)
            .alias("premium_index"),
            pl.when(pl.col("top_trader_ls_ratio_acct").is_not_null() & pl.col("global_ls_ratio_acct").is_not_null())
            .then(pl.col("top_trader_ls_ratio_acct") - pl.col("global_ls_ratio_acct"))
            .otherwise(None)
            .alias("ls_ratio_divergence"),
            (
                pl.col("global_ls_ratio_acct").is_not_null()
                & pl.col("top_trader_ls_ratio_acct").is_not_null()
                & pl.col("top_trader_long_pct").is_not_null()
                & pl.col("top_trader_short_pct").is_not_null()
            ).alias("has_ls_ratio"),
            pl.when(pl.col("volume_btc").is_not_null() & pl.col("taker_buy_vol_btc").is_not_null())
            .then(pl.col("volume_btc") - pl.col("taker_buy_vol_btc"))
            .otherwise(None)
            .alias("taker_sell_vol_btc"),
            pl.when(pl.col("volume_usdt").is_not_null() & pl.col("taker_buy_vol_usdt").is_not_null())
            .then(pl.col("volume_usdt") - pl.col("taker_buy_vol_usdt"))
            .otherwise(None)
            .alias("taker_sell_vol_usdt"),
            pl.when((pl.col("close") > 0) & (pl.col("close").shift(1) > 0))
            .then((pl.col("close") / pl.col("close").shift(1)).log().abs())
            .otherwise(None)
            .alias("realized_vol_bar"),
        )
        # Sentiment-style net measures from top-trader position percentages.
        # These are not exchange-wide position inventory and should not be read
        # as absolute long/short exposure.
        result = result.with_columns(
            pl.when(pl.col("top_trader_long_pct").is_not_null() & pl.col("top_trader_short_pct").is_not_null())
            .then(pl.col("top_trader_long_pct") - pl.col("top_trader_short_pct"))
            .otherwise(None)
            .alias("net_long"),
            pl.when(pl.col("top_trader_long_pct").is_not_null() & pl.col("top_trader_short_pct").is_not_null())
            .then(pl.col("top_trader_short_pct") - pl.col("top_trader_long_pct"))
            .otherwise(None)
            .alias("net_short"),
        )
        return result.with_columns(
            pl.when(pl.col("oi_contracts").is_not_null() & pl.col("oi_contracts").shift(1).is_not_null())
            .then(pl.col("oi_contracts") - pl.col("oi_contracts").shift(1))
            .otherwise(None)
            .alias("delta_oi_contracts"),
            pl.when(pl.col("oi_value_usdt").is_not_null() & pl.col("oi_value_usdt").shift(1).is_not_null())
            .then(pl.col("oi_value_usdt") - pl.col("oi_value_usdt").shift(1))
            .otherwise(None)
            .alias("delta_oi_value_usdt"),
            pl.when(pl.col("funding_rate").is_not_null() & pl.col("funding_rate").shift(1).is_not_null())
            .then(pl.col("funding_rate") - pl.col("funding_rate").shift(1))
            .otherwise(None)
            .alias("delta_funding_rate"),
            pl.when(pl.col("net_long").is_not_null() & pl.col("net_long").shift(1).is_not_null())
            .then(pl.col("net_long") - pl.col("net_long").shift(1))
            .otherwise(None)
            .alias("delta_net_long"),
            pl.when(pl.col("net_short").is_not_null() & pl.col("net_short").shift(1).is_not_null())
            .then(pl.col("net_short") - pl.col("net_short").shift(1))
            .otherwise(None)
            .alias("delta_net_short"),
        )

    @staticmethod
    def _align_exact_with_one_bar_fallback(
        frame: pl.DataFrame,
        rows: list[dict[str, object]],
        *,
        source_time_col: str,
        value_map: dict[str, str],
        timeframe_minutes: int,
        notes: list[str],
        note_prefix: str,
    ) -> pl.DataFrame:
        if frame.height == 0 or not rows:
            return frame

        target_cols = list(value_map.values())
        before = _populated_count(frame, target_cols)
        exact = align_series(
            frame,
            rows,
            source_time_col=source_time_col,
            value_map=value_map,
            mode=AlignmentMode.EXACT_TIMESTAMP,
            normalize_source_timeframe_minutes=timeframe_minutes,
        )
        exact_count = _populated_count(exact, target_cols)
        notes.append(f"{note_prefix}_alignment_exact_timestamp")
        if exact_count >= exact.height:
            return exact

        fallback = align_series(
            exact,
            rows,
            source_time_col=source_time_col,
            value_map=value_map,
            mode=AlignmentMode.FORWARD_FILL_WITH_MAX_AGE,
            align_at_bar_close=True,
            bar_minutes=timeframe_minutes,
            max_age=timedelta(minutes=timeframe_minutes),
            normalize_source_timeframe_minutes=timeframe_minutes,
        )
        fallback_count = _populated_count(fallback, target_cols)
        if fallback_count > exact_count:
            notes.append(f"{note_prefix}_alignment_asof_backward_max_age_1bar")
        return fallback

    def _fetch_optional_native_series(
        self,
        *,
        fetcher_name: str,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        notes: list[str],
        failure_note: str,
        **kwargs: object,
    ) -> list[dict[str, object]]:
        fetcher = getattr(self._provider, fetcher_name, None)
        if not callable(fetcher):
            notes.append(f"{fetcher_name}_unavailable")
            return []
        try:
            return fetcher(symbol, start_time, end_time, **kwargs)
        except Exception as exc:
            LOGGER.warning(
                "Native Binance auxiliary fetch failed",
                extra={"symbol": symbol, "fetcher": fetcher_name, "error": str(exc)},
            )
            notes.append(failure_note)
            return []

    def _enrich_native_frame(
        self,
        frame: pl.DataFrame,
        *,
        symbol: str,
        spec: TimeframeSpec,
        start_time: datetime,
        end_time: datetime,
        interval: str,
        limit: int,
        notes: list[str],
    ) -> pl.DataFrame:
        result = frame
        aux_limit = max(limit + AUXILIARY_FETCH_EXTRA_ROWS, limit)
        auxiliary_debug: dict[str, dict[str, object]] = {}

        for fetcher_name, value_map, failure_note, no_rows_note in [
            (
                "fetch_native_mark_price_klines",
                {"mark_price_open": "mark_price_open", "mark_price_close": "mark_price_close"},
                "mark_price_klines_fetch_failed",
                "mark_price_klines_no_rows",
            ),
            (
                "fetch_native_index_price_klines",
                {"index_price_open": "index_price_open", "index_price_close": "index_price_close"},
                "index_price_klines_fetch_failed",
                "index_price_klines_no_rows",
            ),
            (
                "fetch_native_premium_index_klines",
                {"premium_index_close": "premium_index"},
                "premium_index_klines_fetch_failed",
                "premium_index_klines_no_rows",
            ),
        ]:
            rows = self._fetch_optional_native_series(
                fetcher_name=fetcher_name,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                notes=notes,
                failure_note=failure_note,
                interval=interval,
                limit=aux_limit,
            )
            auxiliary_debug[fetcher_name] = _series_debug_summary(rows, "open_time", spec.minutes)
            if rows:
                result = self._align_exact_with_one_bar_fallback(
                    result,
                    rows,
                    source_time_col="open_time",
                    value_map=value_map,
                    timeframe_minutes=spec.minutes,
                    notes=notes,
                    note_prefix=fetcher_name.removeprefix("fetch_native_").removesuffix("_klines"),
                )
            else:
                notes.append(no_rows_note)

        if interval in BINANCE_OI_HIST_TFS:
            oi_rows = self._fetch_optional_native_series(
                fetcher_name="fetch_native_open_interest_hist",
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                notes=notes,
                failure_note="open_interest_hist_fetch_failed",
                period=interval,
                limit=aux_limit,
            )
            auxiliary_debug["fetch_native_open_interest_hist"] = _series_debug_summary(
                oi_rows,
                "create_time",
                spec.minutes,
            )
            if oi_rows:
                result = self._align_exact_with_one_bar_fallback(
                    result,
                    oi_rows,
                    source_time_col="create_time",
                    value_map={"oi_contracts": "oi_contracts", "oi_value_usdt": "oi_value_usdt"},
                    timeframe_minutes=spec.minutes,
                    notes=notes,
                    note_prefix="oi_hist",
                )
            else:
                notes.append("oi_hist_no_rows")

        if interval in BINANCE_LS_RATIO_TFS:
            ratio_fetches = [
                (
                    "fetch_native_global_long_short_account_ratio",
                    {"ratio": "global_ls_ratio_acct"},
                    "global_ls_ratio_fetch_failed",
                    "global_ls_ratio_no_rows",
                ),
                (
                    "fetch_native_top_trader_long_short_account_ratio",
                    {"ratio": "top_trader_ls_ratio_acct"},
                    "top_trader_ls_ratio_fetch_failed",
                    "top_trader_ls_ratio_no_rows",
                ),
                (
                    "fetch_native_top_trader_long_short_position_ratio",
                    {"long_account": "top_trader_long_pct", "short_account": "top_trader_short_pct"},
                    "top_trader_position_ratio_fetch_failed",
                    "top_trader_position_ratio_no_rows",
                ),
            ]
            for fetcher_name, value_map, failure_note, no_rows_note in ratio_fetches:
                rows = self._fetch_optional_native_series(
                    fetcher_name=fetcher_name,
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                    notes=notes,
                    failure_note=failure_note,
                    period=interval,
                    limit=aux_limit,
                )
                auxiliary_debug[fetcher_name] = _series_debug_summary(rows, "data_time", spec.minutes)
                if rows:
                    result = self._align_exact_with_one_bar_fallback(
                        result,
                        rows,
                        source_time_col="data_time",
                        value_map=value_map,
                        timeframe_minutes=spec.minutes,
                        notes=notes,
                        note_prefix=fetcher_name.removeprefix("fetch_native_"),
                    )
                else:
                    notes.append(no_rows_note)

        funding_rows = self._fetch_optional_native_series(
            fetcher_name="fetch_native_funding_rate",
            symbol=symbol,
            start_time=start_time - timedelta(hours=8),
            end_time=end_time,
            notes=notes,
            failure_note="funding_rate_fetch_failed",
            limit=max(limit + AUXILIARY_FETCH_EXTRA_ROWS, 1000),
        )
        auxiliary_debug["fetch_native_funding_rate"] = _series_debug_summary(
            funding_rows,
            "funding_time",
            spec.minutes,
        )
        if funding_rows:
            result = align_series(
                result,
                funding_rows,
                source_time_col="funding_time",
                value_map={"funding_rate": "funding_rate"},
                mode=AlignmentMode.ASOF_BACKWARD,
                align_at_bar_close=True,
                bar_minutes=spec.minutes,
            )
            notes.append("funding_rate_aligned_asof_backward")
            notes.append("funding_rate_event_series_aligned_not_native_tf")
            if result.get_column("funding_rate").drop_nulls().is_empty():
                notes.append("funding_rate_no_prior_event_found")
        else:
            notes.append("funding_rate_no_prior_event_found")
        notes.append("next_funding_time_current_snapshot_only")
        notes.append("predicted_funding_live_ws_only")

        LOGGER.debug(
            "Native timeframe enrichment alignment summary",
            extra={
                "symbol": symbol,
                "timeframe": spec.api_name,
                "interval": interval,
                "requested_candle_bars": limit,
                "aux_fetch_limit": aux_limit,
                "bar_count": result.height,
                "oi_populated_bars": _populated_count(result, ["oi_contracts", "oi_value_usdt"]),
                "ls_populated_bars": _populated_count(
                    result,
                    [
                        "global_ls_ratio_acct",
                        "top_trader_ls_ratio_acct",
                        "top_trader_long_pct",
                        "top_trader_short_pct",
                    ],
                ),
                "delta_populated_bars": _populated_count(
                    result,
                    ["delta_oi_contracts", "delta_oi_value_usdt", "delta_net_long", "delta_net_short"],
                ),
                "auxiliary_series": auxiliary_debug,
                "notes": tuple(notes),
            },
        )

        snapshot_fetcher = getattr(self._provider, "fetch_native_premium_index_snapshot", None)
        if callable(snapshot_fetcher) and result.height > 0:
            try:
                snapshot = snapshot_fetcher(symbol)
                event_time = snapshot.get("event_time")
                next_funding_time = snapshot.get("next_funding_time")
                if event_time and next_funding_time:
                    latest_timestamp = result.get_column("timestamp").max()
                    if isinstance(latest_timestamp, datetime):
                        latest_ms = int(latest_timestamp.timestamp() * 1000)
                        bar_close_ms = latest_ms + spec.minutes * 60_000
                        if latest_ms <= int(event_time) <= bar_close_ms:
                            result = result.with_columns(
                                pl.when(pl.col("timestamp") == latest_timestamp)
                                .then(pl.lit(int(next_funding_time)))
                                .otherwise(pl.col("next_funding_time"))
                                .alias("next_funding_time")
                            )
                            notes.append("using_current_snapshot_only_for_next_funding_time")
            except Exception as exc:
                LOGGER.warning(
                    "Native Binance premium snapshot fetch failed",
                    extra={"symbol": symbol, "error": str(exc)},
                )
                notes.append("premium_index_snapshot_fetch_failed")

        return self._add_native_derived_fields(result)

    def _try_load_local_preferred_bars(
        self,
        *,
        symbol: str,
        coin: str,
        spec: TimeframeSpec,
        limit: int,
        resolved_end: datetime,
        binance_interval: str | None,
        started_at: float,
    ) -> tuple[TimeframeCandleResult | None, list[str]]:
        if not self._is_local_preferred_symbol(symbol):
            return None, []

        note_prefix = "local_btc" if symbol == "BTCUSDT" else "local_symbol"
        if symbol == "BTCUSDT":
            complexity_notes = self._btc_local_complexity_notes(spec, limit)
            if complexity_notes:
                return None, complexity_notes

        start_time = requested_window_start(
            resolved_end,
            specs=[spec],
            timeframe_limits={spec.api_name: limit},
        )

        def local_can_serve(local_frame: pl.DataFrame) -> bool:
            return aggregate_canonical_frame(local_frame, spec, limit=limit).height >= limit

        patch_enabled = (
            self._local_symbol_allow_binance_patch
            and not self._local_symbol_require_full_coverage
        )
        notes: list[str] = []

        if not patch_enabled:
            local_frame = self._repository.load_canonical_minutes(symbol, start_time, resolved_end)
            if not local_can_serve(local_frame):
                return None, [
                    f"{note_prefix}_missing_required_window",
                    f"{note_prefix}_coverage_incomplete_fallback_to_binance",
                ]

        try:
            window = self.load_canonical_window(
                coin=coin,
                start_time=start_time,
                end_time=resolved_end,
                local_can_serve_predicate=local_can_serve,
                allow_binance_patch=patch_enabled,
            )
        except Exception as exc:
            LOGGER.warning(
                "Local-preferred symbol path failed; falling back to native planner",
                extra={"symbol": symbol, "timeframe": spec.api_name, "error": str(exc)},
            )
            return None, [
                f"{note_prefix}_missing_required_window",
                f"{note_prefix}_coverage_incomplete_fallback_to_binance",
            ]

        aggregate = aggregate_canonical_frame(window.frame, spec, limit=limit)
        if aggregate.height < limit:
            return None, [
                f"{note_prefix}_missing_required_window",
                f"{note_prefix}_coverage_incomplete_fallback_to_binance",
            ]

        if "local" in window.source:
            notes.append(f"using_{note_prefix}_minute_lake")
            if symbol == "BTCUSDT":
                notes.append("btc_local_path_selected")
        if "binance" in window.source:
            notes.append(f"{note_prefix}_coverage_incomplete_patched_from_binance")
        if window.live_overlay_used:
            notes.append("using_live_ws_overlay")
        else:
            notes.append(f"{note_prefix}_stale_live_overlay_unavailable")

        frame = self._add_native_derived_fields(aggregate)
        elapsed = round(max(perf_counter() - started_at, 0.0), 6)
        LOGGER.info(
            "Local-preferred symbol minute-lake aggregation selected",
            extra={
                "symbol": symbol,
                "timeframe": spec.api_name,
                "source": window.source,
                "rows": frame.height,
                "live_overlay_used": window.live_overlay_used,
                "latency_secs": elapsed,
            },
        )
        return TimeframeCandleResult(
            frame=frame,
            metadata={
                "source": window.source,
                "source_strategy": "local_minute_lake_preferred",
                "fetch_mode": "aggregate_from_1m",
                "fallback_used": "binance" in window.source,
                "local_minute_lake_used": "local" in window.source,
                "live_ws_overlay_used": window.live_overlay_used,
                "binance_interval": binance_interval,
                "notes": _clean_metadata_notes(notes),
                "latency_secs": elapsed,
            },
        ), []

    def load_candle_bars(
        self,
        *,
        coin: str,
        spec: TimeframeSpec,
        limit: int,
        end_time: str | datetime | None = None,
    ) -> TimeframeCandleResult:
        resolved_end = self.resolve_end_time(coin=coin, end_time=end_time)
        symbol = normalize_symbol(coin)
        decision = plan_timeframe_fetch(spec, self._fetch_planner_config)
        started_at = perf_counter()
        notes = list(decision.notes)

        local_result, local_fallback_notes = self._try_load_local_preferred_bars(
            symbol=symbol,
            coin=coin,
            spec=spec,
            limit=limit,
            resolved_end=resolved_end,
            binance_interval=decision.binance_interval,
            started_at=started_at,
        )
        if local_result is not None:
            return local_result
        notes = local_fallback_notes + notes

        if decision.fetch_mode == "direct_tf" and decision.binance_interval is not None:
            fetch_native = getattr(self._provider, "fetch_native_candles", None)
            if callable(fetch_native):
                start_time = requested_window_start(
                    resolved_end,
                    specs=[spec],
                    timeframe_limits={spec.api_name: limit},
                )
                rows = fetch_native(
                    symbol,
                    start_time,
                    resolved_end,
                    interval=decision.binance_interval,
                    limit=limit,
                )
                frame = self._native_klines_to_frame(rows).tail(limit)
                frame = self._enrich_native_frame(
                    frame,
                    symbol=symbol,
                    spec=spec,
                    start_time=start_time,
                    end_time=resolved_end,
                    interval=decision.binance_interval,
                    limit=limit,
                    notes=notes,
                )
                elapsed = round(max(perf_counter() - started_at, 0.0), 6)
                LOGGER.info(
                    "Native Binance candle fetch selected",
                    extra={
                        "symbol": symbol,
                        "timeframe": spec.api_name,
                        "interval": decision.binance_interval,
                        "rows": frame.height,
                        "latency_secs": elapsed,
                    },
                )
                return TimeframeCandleResult(
                    frame=frame,
                    metadata={
                        "source": decision.candle_source,
                        "fetch_mode": decision.fetch_mode,
                        "fallback_used": False,
                        "binance_interval": decision.binance_interval,
                        "notes": _clean_metadata_notes(notes),
                        "latency_secs": elapsed,
                    },
                )
            notes.append("native_candle_provider_method_unavailable")

        if self._fetch_planner_config.allow_legacy_1m_fallback:
            if "using_legacy_1m_aggregation_fallback" not in notes:
                notes.append("using_legacy_1m_aggregation_fallback")
            start_time = requested_window_start(
                resolved_end,
                specs=[spec],
                timeframe_limits={spec.api_name: limit},
            )
            window = self.load_canonical_window(
                coin=coin,
                start_time=start_time,
                end_time=resolved_end,
                local_can_serve_predicate=lambda local_frame: aggregate_canonical_frame(
                    local_frame,
                    spec,
                    limit=limit,
                ).height
                >= limit,
            )
            frame = self._add_native_derived_fields(aggregate_canonical_frame(window.frame, spec, limit=limit))
            elapsed = round(max(perf_counter() - started_at, 0.0), 6)
            LOGGER.info(
                "Legacy 1m candle aggregation selected",
                extra={
                    "symbol": symbol,
                    "timeframe": spec.api_name,
                    "source": window.source,
                    "rows": frame.height,
                    "latency_secs": elapsed,
                },
            )
            return TimeframeCandleResult(
                frame=frame,
                metadata={
                    "source": window.source,
                    "fetch_mode": "aggregate_from_1m",
                    "fallback_used": True,
                    "binance_interval": decision.binance_interval,
                    "notes": _clean_metadata_notes(notes),
                    "latency_secs": elapsed,
                },
            )

        if not self._fetch_planner_config.allow_partial_response_with_notes:
            raise ValueError(f"No candle source available for timeframe {spec.api_name}")
        notes.append("candle_data_unavailable")
        return TimeframeCandleResult(
            frame=empty_canonical_frame(),
            metadata={
                "source": "unavailable",
                "fetch_mode": "unavailable",
                "fallback_used": False,
                "binance_interval": decision.binance_interval,
                "notes": _clean_metadata_notes(notes),
                "latency_secs": round(max(perf_counter() - started_at, 0.0), 6),
            },
        )

    def fetch_perpetual_data(
        self,
        *,
        coin: str,
        tfs: str,
        limit: int | None = None,
        end_time: str | None = None,
    ) -> dict[str, object]:
        resolved_limit = limit if limit is not None else self._default_limit
        if resolved_limit < 1 or resolved_limit > self._max_limit:
            raise ValueError(f"limit must be between 1 and {self._max_limit}")

        timeframe_requests = parse_timeframe_requests(tfs)
        effective_limits = {
            request.api_name: request.limit if request.limit is not None else resolved_limit
            for request in timeframe_requests
        }
        for api_name, effective_limit in effective_limits.items():
            if effective_limit < 1 or effective_limit > self._max_limit:
                raise ValueError(f"limit for {api_name} must be between 1 and {self._max_limit}")

        resolved_end = self.resolve_end_time(coin=coin, end_time=end_time)
        plan = {
            request.api_name: plan_timeframe_fetch(request.spec, self._fetch_planner_config)
            for request in timeframe_requests
        }
        LOGGER.info(
            "Perpetual data fetch plan selected",
            extra={
                "coin": coin,
                "timeframes": [request.api_name for request in timeframe_requests],
                "plan": {
                    name: {
                        "source": decision.candle_source,
                        "fetch_mode": decision.fetch_mode,
                        "interval": decision.binance_interval,
                    }
                    for name, decision in plan.items()
                },
            },
        )

        def _aggregate_one(request: Any) -> tuple[str, object]:
            result = self.load_candle_bars(
                coin=coin,
                spec=request.spec,
                limit=effective_limits[request.api_name],
                end_time=resolved_end,
            )
            return request.api_name, (
                serialize_frame(result.frame, include_deprecated_fields=self._include_deprecated_fields),
                result.metadata,
            )

        if len(timeframe_requests) > 1:
            with ThreadPoolExecutor(max_workers=len(timeframe_requests)) as executor:
                fetched = dict(executor.map(_aggregate_one, timeframe_requests))
        else:
            fetched = dict(map(_aggregate_one, timeframe_requests))
        payload = {name: rows for name, (rows, _) in fetched.items()}
        timeframe_metadata = {name: metadata for name, (_, metadata) in fetched.items()}
        return {
            "symbol": normalize_symbol(coin),
            "timeframes": [request.api_name for request in timeframe_requests],
            "limit": resolved_limit,
            "limits": effective_limits,
            "end_time": resolved_end.isoformat().replace("+00:00", "Z"),
            "source": "mixed" if len({meta["source"] for meta in timeframe_metadata.values()}) > 1 else next(
                iter(timeframe_metadata.values())
            )["source"],
            "timeframe_metadata": timeframe_metadata,
            "capabilities": response_capability_matrix(),
            "data": payload,
        }


def _populated_count(frame: pl.DataFrame, columns: list[str]) -> int:
    if frame.height == 0 or not columns or any(column not in frame.columns for column in columns):
        return 0
    return int(
        frame.select(
            pl.all_horizontal([pl.col(column).is_not_null() for column in columns]).sum().alias("count")
        ).item()
    )


def _infer_frame_minutes(frame: pl.DataFrame) -> int:
    if frame.height < 2 or "timestamp" not in frame.columns:
        return 1
    timestamps = frame.sort("timestamp").get_column("timestamp").head(2).to_list()
    first, second = timestamps
    if not isinstance(first, datetime) or not isinstance(second, datetime):
        return 1
    minutes = int((second - first).total_seconds() // 60)
    return max(minutes, 1)


def _series_debug_summary(
    rows: list[dict[str, object]],
    source_time_col: str,
    timeframe_minutes: int,
) -> dict[str, object]:
    timestamps = [row.get(source_time_col) for row in rows if row.get(source_time_col) is not None]
    if not timestamps:
        return {"row_count": len(rows), "raw_range": None, "normalized_range": None}
    raw_values = [int(value) for value in timestamps]
    normalized_values = [
        int(normalize_bar_timestamp(value, timeframe_minutes).timestamp() * 1000)
        for value in raw_values
    ]
    return {
        "row_count": len(rows),
        "raw_range": (min(raw_values), max(raw_values)),
        "normalized_range": (min(normalized_values), max(normalized_values)),
    }


def _clean_metadata_notes(notes: list[str]) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for note in notes:
        if note.startswith("oi_hist_not_supported_for_"):
            note = note.replace("oi_hist_not_supported_for_", "open_interest_hist_not_supported_for_", 1)
        if note in seen:
            continue
        seen.add(note)
        cleaned.append(note)
    return cleaned


def _source_with_live(source: str) -> str:
    parts = source.split("+") if source else []
    if "live" in parts:
        return source
    if "local" in parts:
        insert_at = parts.index("local") + 1
        parts.insert(insert_at, "live")
    else:
        parts.insert(0, "live")
    return "+".join(parts)
