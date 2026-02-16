from __future__ import annotations

import asyncio
import json
import logging
import math
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

MINUTE_MS = 60_000
PRICE_IMPACT_NOTIONAL_USDT = 100_000.0
LATENCY_BAD_MS = 500
DEPTH_DEGRADED_SPREAD_MAX_PCT = 0.02
DEPTH_DEGRADED_MIN_AVG_LEVEL_QTY = 1.0
DEPTH_HEALTH_LEVEL_COUNT = 10

CONSUMER_WS_LATENCY = "ws_latency"
CONSUMER_DEPTH = "depth"
CONSUMER_LIQUIDATION = "liquidation"
CONSUMER_LS_RATIO = "ls_ratio"

logger = logging.getLogger(__name__)


def floor_to_minute_ms(value_ms: int) -> int:
    return (value_ms // MINUTE_MS) * MINUTE_MS


def now_ms() -> int:
    return int(datetime.now(tz=UTC).timestamp() * 1000)


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized == "":
            return None
        try:
            return int(normalized)
        except ValueError:
            try:
                return int(float(normalized))
            except ValueError:
                return None
    return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip()
        if normalized == "":
            return None
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _payload_to_json(payload: dict[str, Any] | None) -> str:
    if payload is None:
        return "{}"
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def _parse_depth_levels(value: Any) -> tuple[tuple[float, float], ...]:
    if not isinstance(value, list):
        return ()

    levels: list[tuple[float, float]] = []
    for level in value:
        if not isinstance(level, (list, tuple)) or len(level) < 2:
            continue
        price = _coerce_float(level[0])
        quantity = _coerce_float(level[1])
        if price is None or quantity is None:
            continue
        levels.append((price, quantity))
    return tuple(levels)


def _p95_int(values: list[int]) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, math.ceil(0.95 * len(ordered)))
    return int(ordered[rank - 1])


@dataclass(frozen=True, slots=True)
class LiveMinuteFeatures:
    timestamp_ms: int
    has_ws_latency: bool = False
    has_depth: bool = False
    has_liq: bool = False
    has_ls_ratio: bool = False
    event_time: int | None = None
    transact_time: int | None = None
    arrival_time: int | None = None
    latency_engine: int | None = None
    latency_network: int | None = None
    ws_latency_bad: bool | None = None
    update_id_start: int | None = None
    update_id_end: int | None = None
    price_impact_100k: float | None = None
    impact_fillable: bool | None = None
    depth_degraded: bool | None = None
    liq_long_vol_usdt: float | None = None
    liq_short_vol_usdt: float | None = None
    liq_long_count: int | None = None
    liq_short_count: int | None = None
    liq_avg_fill_price: float | None = None
    liq_unfilled_ratio: float | None = None
    liq_unfilled_supported: bool | None = None
    predicted_funding: float | None = None
    next_funding_time: int | None = None


@dataclass(frozen=True, slots=True)
class LiveEventCleanupSummary:
    event_cutoff_ms: int
    heartbeat_cutoff_ms: int
    ws_events_deleted: int
    ws_depth_events_deleted: int
    ws_liq_events_deleted: int
    ws_trade_events_deleted: int
    consumer_heartbeats_deleted: int
    vacuumed: bool

    @property
    def total_deleted(self) -> int:
        return (
            self.ws_events_deleted
            + self.ws_depth_events_deleted
            + self.ws_liq_events_deleted
            + self.ws_trade_events_deleted
            + self.consumer_heartbeats_deleted
        )


class LiveCollector:
    """Live collector contract.

    Implementations are optional in baseline mode. When no collector is provided,
    all live-only columns remain NULL by schema contract.
    """

    def snapshot_for_minute(self, minute_timestamp_ms: int) -> LiveMinuteFeatures | None:
        return None


class DepthSyncError(RuntimeError):
    """Raised when depth diff continuity is broken."""


@dataclass(frozen=True, slots=True)
class DepthDiffEvent:
    symbol: str
    event_time: int
    first_update_id: int
    final_update_id: int
    bid_deltas: tuple[tuple[float, float], ...]
    ask_deltas: tuple[tuple[float, float], ...]
    previous_final_update_id: int | None = None


@dataclass(frozen=True, slots=True)
class LiquidationOrderEvent:
    symbol: str
    event_time: int
    side: str
    price: float
    quantity: float
    arrival_time: int | None = None
    orig_quantity: float | None = None
    executed_quantity: float | None = None


@dataclass(frozen=True, slots=True)
class ConsumerHeartbeat:
    consumer_name: str
    minute_timestamp_ms: int
    alive: bool
    last_message_time: int | None = None


@dataclass(slots=True)
class _WorkerConnectionState:
    connected: bool = False
    last_message_time: int | None = None


class DepthOrderBook:
    def __init__(self) -> None:
        self._bids: dict[float, float] = {}
        self._asks: dict[float, float] = {}
        self._buffer: list[DepthDiffEvent] = []
        self._last_update_id: int | None = None
        self._synchronized = False
        self._degraded = False

    @property
    def is_synchronized(self) -> bool:
        return self._synchronized

    @property
    def degraded(self) -> bool:
        return self._degraded

    @property
    def last_update_id(self) -> int | None:
        return self._last_update_id

    def mark_degraded(self) -> None:
        self._degraded = True
        self._synchronized = False

    def clear_degraded(self) -> None:
        self._degraded = False

    def buffer_event(self, event: DepthDiffEvent) -> None:
        self._buffer.append(event)
        self._buffer.sort(key=lambda item: item.final_update_id)

    def sync_from_snapshot(
        self,
        *,
        last_update_id: int,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
    ) -> None:
        self._bids = {price: qty for price, qty in bids if qty > 0}
        self._asks = {price: qty for price, qty in asks if qty > 0}
        self._last_update_id = int(last_update_id)
        self._synchronized = True
        self.clear_degraded()

        if not self._buffer:
            self._validate_book_spread()
            return

        filtered = [item for item in self._buffer if item.final_update_id >= self._last_update_id]
        self._buffer = []
        if not filtered:
            self._validate_book_spread()
            return

        first = filtered[0]
        if not (first.first_update_id <= self._last_update_id <= first.final_update_id):
            self.mark_degraded()
            raise DepthSyncError(
                "Invalid first diff event after snapshot: expected U <= lastUpdateId <= u, "
                f"got U={first.first_update_id}, u={first.final_update_id}, lastUpdateId={self._last_update_id}"
            )

        for event in filtered:
            self.apply_event(event)

    def apply_event(self, event: DepthDiffEvent) -> None:
        if not self._synchronized or self._last_update_id is None:
            self.buffer_event(event)
            return

        if event.final_update_id <= self._last_update_id:
            return

        expected_next = self._last_update_id + 1
        if event.previous_final_update_id is not None and event.previous_final_update_id != self._last_update_id:
            self.mark_degraded()
            raise DepthSyncError(
                "Depth continuity broken on pu check: "
                f"pu={event.previous_final_update_id}, last_u={self._last_update_id}"
            )
        if event.previous_final_update_id is None and event.first_update_id > expected_next:
            self.mark_degraded()
            raise DepthSyncError(
                "Depth continuity broken on U check: "
                f"U={event.first_update_id}, expected<={expected_next}"
            )

        self._apply_deltas(self._bids, event.bid_deltas)
        self._apply_deltas(self._asks, event.ask_deltas)
        self._last_update_id = event.final_update_id
        self._validate_book_spread()

    @staticmethod
    def _apply_deltas(book_side: dict[float, float], deltas: tuple[tuple[float, float], ...]) -> None:
        for price, quantity in deltas:
            if quantity <= 0:
                book_side.pop(price, None)
            else:
                book_side[price] = quantity

    def _validate_book_spread(self) -> None:
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        if best_bid is None or best_ask is None:
            return
        if best_bid >= best_ask:
            self.mark_degraded()
            raise DepthSyncError(
                "Order book invariant broken: best_bid must be < best_ask "
                f"(best_bid={best_bid}, best_ask={best_ask})"
            )

    def best_bid(self) -> float | None:
        if not self._bids:
            return None
        return max(self._bids)

    def best_ask(self) -> float | None:
        if not self._asks:
            return None
        return min(self._asks)

    def compute_buy_price_impact(self, notional_usdt: float = PRICE_IMPACT_NOTIONAL_USDT) -> tuple[float | None, bool]:
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        if best_bid is None or best_ask is None:
            return None, False

        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None, False

        remaining = float(notional_usdt)
        total_cost = 0.0
        total_qty = 0.0

        for ask_price in sorted(self._asks):
            ask_qty = self._asks[ask_price]
            if ask_qty <= 0:
                continue
            level_notional = ask_price * ask_qty
            take_notional = min(remaining, level_notional)
            qty_taken = take_notional / ask_price
            total_cost += take_notional
            total_qty += qty_taken
            remaining -= take_notional
            if remaining <= 1e-9:
                break

        if remaining > 1e-9 or total_qty <= 0:
            return None, False

        average_execution_price = total_cost / total_qty
        impact = (average_execution_price - mid) / mid
        return impact, True

    def compute_health_metrics(
        self,
        *,
        level_count: int = DEPTH_HEALTH_LEVEL_COUNT,
    ) -> tuple[float | None, float | None, float | None]:
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        if best_bid is None or best_ask is None:
            return None, None, None

        mid = (best_bid + best_ask) / 2.0
        if mid <= 0:
            return None, None, None

        spread_pct = (best_ask - best_bid) / mid
        bid_levels = [
            qty
            for _, qty in sorted(self._bids.items(), key=lambda item: item[0], reverse=True)[:level_count]
        ]
        ask_levels = [qty for _, qty in sorted(self._asks.items(), key=lambda item: item[0])[:level_count]]
        avg_bid_qty = (sum(bid_levels) / len(bid_levels)) if bid_levels else None
        avg_ask_qty = (sum(ask_levels) / len(ask_levels)) if ask_levels else None
        return spread_pct, avg_bid_qty, avg_ask_qty


class LiveEventStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self, *, timeout: float = 5.0) -> Iterator[sqlite3.Connection]:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self._db_path, timeout=timeout)
        try:
            yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ws_events (
                    ingest_id TEXT PRIMARY KEY,
                    stream TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    event_time INTEGER,
                    transact_time INTEGER,
                    arrival_time INTEGER NOT NULL,
                    raw_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ws_depth_events (
                    ingest_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    event_time INTEGER,
                    arrival_time INTEGER NOT NULL,
                    first_update_id INTEGER NOT NULL,
                    final_update_id INTEGER NOT NULL,
                    bids_json TEXT NOT NULL,
                    asks_json TEXT NOT NULL,
                    raw_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ws_liq_events (
                    ingest_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    event_time INTEGER,
                    arrival_time INTEGER NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    qty REAL NOT NULL,
                    raw_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ws_trade_events (
                    ingest_id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    event_time INTEGER,
                    arrival_time INTEGER NOT NULL,
                    transact_time INTEGER,
                    raw_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS consumer_heartbeats (
                    consumer_name TEXT NOT NULL,
                    minute_ts INTEGER NOT NULL,
                    alive INTEGER NOT NULL,
                    last_message_time INTEGER,
                    PRIMARY KEY (consumer_name, minute_ts)
                )
                """
            )

            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_ws_events_symbol_arrival ON ws_events(symbol, arrival_time)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_ws_events_event_time ON ws_events(event_time)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_ws_events_arrival_time ON ws_events(arrival_time)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_ws_depth_symbol_event ON ws_depth_events(symbol, event_time)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_ws_depth_event_time ON ws_depth_events(event_time)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_ws_liq_symbol_event ON ws_liq_events(symbol, event_time)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_ws_liq_event_time ON ws_liq_events(event_time)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_ws_trade_symbol_event ON ws_trade_events(symbol, event_time)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS idx_ws_trade_event_time ON ws_trade_events(event_time)")
            connection.execute("CREATE INDEX IF NOT EXISTS idx_ws_trade_arrival_time ON ws_trade_events(arrival_time)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_consumer_heartbeats_minute_ts ON consumer_heartbeats(minute_ts)"
            )
            connection.commit()

    @staticmethod
    def _rows_deleted(cursor: sqlite3.Cursor) -> int:
        return max(int(cursor.rowcount), 0)

    def append_event(
        self,
        *,
        stream: str,
        symbol: str,
        event_time: int | None,
        transact_time: int | None,
        arrival_time: int,
        raw_payload: dict[str, Any] | None,
    ) -> str:
        ingest_id = uuid.uuid4().hex
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ws_events(
                    ingest_id, stream, symbol, event_time, transact_time, arrival_time, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingest_id,
                    stream,
                    symbol.upper(),
                    event_time,
                    transact_time,
                    arrival_time,
                    _payload_to_json(raw_payload),
                ),
            )
            connection.commit()
        return ingest_id

    def append_depth_event(
        self,
        *,
        symbol: str,
        event_time: int,
        arrival_time: int,
        first_update_id: int,
        final_update_id: int,
        bids: tuple[tuple[float, float], ...],
        asks: tuple[tuple[float, float], ...],
        raw_payload: dict[str, Any] | None,
    ) -> str:
        ingest_id = uuid.uuid4().hex
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ws_depth_events(
                    ingest_id, symbol, event_time, arrival_time, first_update_id, final_update_id,
                    bids_json, asks_json, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingest_id,
                    symbol.upper(),
                    event_time,
                    arrival_time,
                    first_update_id,
                    final_update_id,
                    json.dumps(bids, separators=(",", ":")),
                    json.dumps(asks, separators=(",", ":")),
                    _payload_to_json(raw_payload),
                ),
            )
            connection.commit()
        return ingest_id

    def append_liquidation_event(
        self,
        *,
        symbol: str,
        event_time: int,
        arrival_time: int,
        side: str,
        price: float,
        quantity: float,
        raw_payload: dict[str, Any] | None,
    ) -> str:
        ingest_id = uuid.uuid4().hex
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ws_liq_events(
                    ingest_id, symbol, event_time, arrival_time, side, price, qty, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ingest_id,
                    symbol.upper(),
                    event_time,
                    arrival_time,
                    side.upper(),
                    float(price),
                    float(quantity),
                    _payload_to_json(raw_payload),
                ),
            )
            connection.commit()
        return ingest_id

    def append_trade_event(
        self,
        *,
        symbol: str,
        event_time: int | None,
        arrival_time: int,
        transact_time: int | None,
        raw_payload: dict[str, Any] | None,
    ) -> str:
        ingest_id = uuid.uuid4().hex
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO ws_trade_events(
                    ingest_id, symbol, event_time, arrival_time, transact_time, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    ingest_id,
                    symbol.upper(),
                    event_time,
                    arrival_time,
                    transact_time,
                    _payload_to_json(raw_payload),
                ),
            )
            connection.commit()
        return ingest_id

    def upsert_heartbeat(
        self,
        *,
        consumer_name: str,
        minute_timestamp_ms: int,
        alive: bool,
        last_message_time: int | None,
    ) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO consumer_heartbeats(
                    consumer_name, minute_ts, alive, last_message_time
                ) VALUES (?, ?, ?, ?)
                ON CONFLICT(consumer_name, minute_ts)
                DO UPDATE SET alive=excluded.alive, last_message_time=excluded.last_message_time
                """,
                (
                    consumer_name,
                    minute_timestamp_ms,
                    1 if alive else 0,
                    last_message_time,
                ),
            )
            connection.commit()

    def cleanup_by_retention(
        self,
        *,
        now_timestamp_ms: int | None = None,
        event_retention_hours: int,
        heartbeat_retention_days: int,
        vacuum: bool = False,
    ) -> LiveEventCleanupSummary:
        now_value = now_ms() if now_timestamp_ms is None else int(now_timestamp_ms)
        event_cutoff_ms = now_value - (int(event_retention_hours) * 60 * 60 * 1000)
        heartbeat_cutoff_ms = now_value - (int(heartbeat_retention_days) * 24 * 60 * 60 * 1000)
        return self.cleanup_before_cutoff(
            event_cutoff_ms=event_cutoff_ms,
            heartbeat_cutoff_ms=heartbeat_cutoff_ms,
            vacuum=vacuum,
        )

    def cleanup_before_cutoff(
        self,
        *,
        event_cutoff_ms: int,
        heartbeat_cutoff_ms: int,
        vacuum: bool = False,
    ) -> LiveEventCleanupSummary:
        with self._connect(timeout=15.0) as connection:
            connection.execute("PRAGMA busy_timeout=15000")

            ws_events_deleted = self._rows_deleted(
                connection.execute("DELETE FROM ws_events WHERE stream NOT LIKE ?", ("%@depth@100ms",))
            )
            ws_events_deleted += self._rows_deleted(
                connection.execute(
                    "DELETE FROM ws_events WHERE event_time IS NOT NULL AND event_time < ?",
                    (event_cutoff_ms,),
                )
            )
            ws_events_deleted += self._rows_deleted(
                connection.execute(
                    "DELETE FROM ws_events WHERE event_time IS NULL AND arrival_time < ?",
                    (event_cutoff_ms,),
                )
            )

            ws_depth_events_deleted = self._rows_deleted(
                connection.execute(
                    "DELETE FROM ws_depth_events WHERE event_time < ?",
                    (event_cutoff_ms,),
                )
            )
            ws_liq_events_deleted = self._rows_deleted(
                connection.execute(
                    "DELETE FROM ws_liq_events WHERE event_time < ?",
                    (event_cutoff_ms,),
                )
            )

            ws_trade_events_deleted = self._rows_deleted(
                connection.execute(
                    "DELETE FROM ws_trade_events WHERE event_time IS NOT NULL AND event_time < ?",
                    (event_cutoff_ms,),
                )
            )
            ws_trade_events_deleted += self._rows_deleted(
                connection.execute(
                    "DELETE FROM ws_trade_events WHERE event_time IS NULL AND arrival_time < ?",
                    (event_cutoff_ms,),
                )
            )

            consumer_heartbeats_deleted = self._rows_deleted(
                connection.execute(
                    "DELETE FROM consumer_heartbeats WHERE minute_ts < ?",
                    (heartbeat_cutoff_ms,),
                )
            )
            connection.commit()

        vacuumed = False
        if vacuum:
            with self._connect(timeout=15.0) as connection:
                connection.execute("PRAGMA busy_timeout=15000")
                connection.execute("VACUUM")
                vacuumed = True

        return LiveEventCleanupSummary(
            event_cutoff_ms=event_cutoff_ms,
            heartbeat_cutoff_ms=heartbeat_cutoff_ms,
            ws_events_deleted=ws_events_deleted,
            ws_depth_events_deleted=ws_depth_events_deleted,
            ws_liq_events_deleted=ws_liq_events_deleted,
            ws_trade_events_deleted=ws_trade_events_deleted,
            consumer_heartbeats_deleted=consumer_heartbeats_deleted,
            vacuumed=vacuumed,
        )

    def snapshot_for_minute(
        self,
        *,
        minute_timestamp_ms: int,
        symbol: str | None = None,
    ) -> LiveMinuteFeatures | None:
        minute_key = floor_to_minute_ms(minute_timestamp_ms)
        minute_end = minute_key + MINUTE_MS
        symbol_upper = symbol.upper() if symbol is not None else None

        with self._connect() as connection:
            depth_query = """
                SELECT event_time, arrival_time, first_update_id, final_update_id
                FROM ws_depth_events
                WHERE event_time >= ? AND event_time < ?
            """
            depth_params: list[int | str] = [minute_key, minute_end]
            if symbol_upper is not None:
                depth_query += " AND symbol = ?"
                depth_params.append(symbol_upper)
            depth_rows = connection.execute(depth_query, depth_params).fetchall()

            latency_query = """
                SELECT event_time, transact_time, arrival_time
                FROM ws_events
                WHERE event_time >= ? AND event_time < ? AND stream LIKE ?
            """
            latency_params: list[int | str] = [minute_key, minute_end, "%@depth@100ms"]
            if symbol_upper is not None:
                latency_query += " AND symbol = ?"
                latency_params.append(symbol_upper)
            latency_rows = connection.execute(latency_query, latency_params).fetchall()

            liq_query = """
                SELECT side, price, qty, raw_json
                FROM ws_liq_events
                WHERE event_time >= ? AND event_time < ?
            """
            liq_params: list[int | str] = [minute_key, minute_end]
            if symbol_upper is not None:
                liq_query += " AND symbol = ?"
                liq_params.append(symbol_upper)
            liq_rows = connection.execute(liq_query, liq_params).fetchall()

        if not depth_rows and not latency_rows and not liq_rows:
            return None

        latency_engine_values: list[int] = []
        latency_network_values: list[int] = []
        latency_event_times: list[int] = []
        latency_transact_times: list[int] = []
        latency_arrivals: list[int] = []
        ws_latency_bad = False

        for event_time, transact_time, arrival_time in latency_rows:
            if event_time is None or transact_time is None or arrival_time is None:
                continue
            event_time_int = int(event_time)
            transact_time_int = int(transact_time)
            arrival_time_int = int(arrival_time)
            engine_latency = int(arrival_time_int - event_time_int)
            network_latency = int(arrival_time_int - transact_time_int)
            latency_engine_values.append(engine_latency)
            latency_network_values.append(network_latency)
            latency_event_times.append(event_time_int)
            latency_transact_times.append(transact_time_int)
            latency_arrivals.append(arrival_time_int)
            if engine_latency > LATENCY_BAD_MS or network_latency > LATENCY_BAD_MS:
                ws_latency_bad = True

        has_ws_latency = len(latency_engine_values) > 0
        has_depth = len(depth_rows) > 0
        has_liq = len(liq_rows) > 0

        update_id_start = (
            min(int(first_update_id) for _, _, first_update_id, _ in depth_rows) if has_depth else None
        )
        update_id_end = (
            max(int(final_update_id) for _, _, _, final_update_id in depth_rows) if has_depth else None
        )

        liq_long_vol_usdt: float | None = None
        liq_short_vol_usdt: float | None = None
        liq_long_count: int | None = None
        liq_short_count: int | None = None
        liq_avg_fill_price: float | None = None
        liq_unfilled_supported: bool | None = None
        liq_unfilled_ratio: float | None = None

        if has_liq:
            long_vol = 0.0
            short_vol = 0.0
            long_count = 0
            short_count = 0
            qty_total = 0.0
            weighted_price_sum = 0.0
            unfilled_supported = True
            unfilled_total = 0.0
            original_qty_total = 0.0

            for side, price, quantity, raw_json in liq_rows:
                side_upper = str(side).upper()
                price_float = float(price)
                quantity_float = float(quantity)
                notional = price_float * quantity_float

                if side_upper == "SELL":
                    long_vol += notional
                    long_count += 1
                elif side_upper == "BUY":
                    short_vol += notional
                    short_count += 1

                qty_total += quantity_float
                weighted_price_sum += price_float * quantity_float

                parsed_payload: dict[str, Any] = {}
                if isinstance(raw_json, str):
                    try:
                        payload = json.loads(raw_json)
                    except json.JSONDecodeError:
                        payload = {}
                    if isinstance(payload, dict):
                        parsed_payload = payload

                order_payload = parsed_payload.get("o")
                if not isinstance(order_payload, dict):
                    unfilled_supported = False
                    continue

                original_qty = _coerce_float(order_payload.get("q"))
                executed_qty = _coerce_float(order_payload.get("l"))
                if original_qty is None or executed_qty is None or original_qty <= 0:
                    unfilled_supported = False
                    continue

                executed_qty = min(max(executed_qty, 0.0), original_qty)
                unfilled_total += max(original_qty - executed_qty, 0.0)
                original_qty_total += original_qty

            liq_long_vol_usdt = long_vol
            liq_short_vol_usdt = short_vol
            liq_long_count = long_count
            liq_short_count = short_count
            liq_avg_fill_price = (weighted_price_sum / qty_total) if qty_total > 0 else None
            liq_unfilled_supported = unfilled_supported
            if unfilled_supported and original_qty_total > 0:
                liq_unfilled_ratio = unfilled_total / original_qty_total
            else:
                liq_unfilled_ratio = None

        return LiveMinuteFeatures(
            timestamp_ms=minute_key,
            has_ws_latency=has_ws_latency,
            has_depth=has_depth,
            has_liq=has_liq,
            has_ls_ratio=False,
            event_time=max(latency_event_times) if has_ws_latency else None,
            transact_time=max(latency_transact_times) if has_ws_latency else None,
            arrival_time=max(latency_arrivals) if has_ws_latency else None,
            latency_engine=_p95_int(latency_engine_values) if has_ws_latency else None,
            latency_network=_p95_int(latency_network_values) if has_ws_latency else None,
            ws_latency_bad=ws_latency_bad if has_ws_latency else None,
            update_id_start=update_id_start,
            update_id_end=update_id_end,
            price_impact_100k=None,
            impact_fillable=None,
            depth_degraded=None,
            liq_long_vol_usdt=liq_long_vol_usdt,
            liq_short_vol_usdt=liq_short_vol_usdt,
            liq_long_count=liq_long_count,
            liq_short_count=liq_short_count,
            liq_avg_fill_price=liq_avg_fill_price,
            liq_unfilled_ratio=liq_unfilled_ratio,
            liq_unfilled_supported=liq_unfilled_supported,
            predicted_funding=None,
            next_funding_time=None,
        )


@dataclass(slots=True)
class _MinuteAccumulator:
    event_time: int | None = None
    transact_time: int | None = None
    arrival_time: int | None = None
    latency_event_count: int = 0
    latency_engine_values: list[int] = field(default_factory=list)
    latency_network_values: list[int] = field(default_factory=list)
    ws_latency_bad: bool = False
    depth_event_count: int = 0
    depth_synced_event_count: int = 0
    update_id_start: int | None = None
    update_id_end: int | None = None
    price_impact_100k: float | None = None
    impact_fillable: bool | None = None
    depth_spread_pct: float | None = None
    depth_avg_bid_qty: float | None = None
    depth_avg_ask_qty: float | None = None
    depth_degraded: bool = False
    liq_long_vol_usdt: float = 0.0
    liq_short_vol_usdt: float = 0.0
    liq_long_count: int = 0
    liq_short_count: int = 0
    liq_qty_total: float = 0.0
    liq_weighted_price_sum: float = 0.0
    liq_orig_qty_total: float = 0.0
    liq_executed_qty_total: float = 0.0
    liq_unfilled_supported: bool = True
    liq_event_count: int = 0
    has_ls_ratio: bool = False
    predicted_funding: float | None = None
    next_funding_time: int | None = None


class InMemoryLiveCollector(LiveCollector):
    """In-memory implementation with minute-level health flags and 0-vs-NULL semantics."""

    def __init__(
        self,
        *,
        event_store: LiveEventStore | None = None,
        liquidation_unfilled_supported: bool = True,
        symbol: str | None = None,
    ) -> None:
        self._event_store = event_store
        self._liquidation_unfilled_supported = liquidation_unfilled_supported
        self._symbol = symbol.upper() if symbol is not None else None
        self._minutes: dict[int, _MinuteAccumulator] = {}
        self._depth_books: dict[str, DepthOrderBook] = {}
        self._heartbeats: dict[tuple[str, int], ConsumerHeartbeat] = {}
        self._lock = threading.RLock()

    def mark_consumer_heartbeat(
        self,
        *,
        consumer_name: str,
        minute_timestamp_ms: int,
        alive: bool,
        last_message_time: int | None = None,
    ) -> None:
        minute_key = floor_to_minute_ms(minute_timestamp_ms)
        heartbeat = ConsumerHeartbeat(
            consumer_name=consumer_name,
            minute_timestamp_ms=minute_key,
            alive=alive,
            last_message_time=last_message_time,
        )
        with self._lock:
            self._heartbeats[(consumer_name, minute_key)] = heartbeat
            if self._event_store is not None:
                self._event_store.upsert_heartbeat(
                    consumer_name=consumer_name,
                    minute_timestamp_ms=minute_key,
                    alive=alive,
                    last_message_time=last_message_time,
                )

    def mark_ws_heartbeat(
        self,
        minute_timestamp_ms: int,
        *,
        alive: bool = True,
        last_message_time: int | None = None,
    ) -> None:
        self.mark_consumer_heartbeat(
            consumer_name=CONSUMER_WS_LATENCY,
            minute_timestamp_ms=minute_timestamp_ms,
            alive=alive,
            last_message_time=last_message_time,
        )

    def mark_depth_heartbeat(
        self,
        minute_timestamp_ms: int,
        *,
        alive: bool = True,
        last_message_time: int | None = None,
    ) -> None:
        self.mark_consumer_heartbeat(
            consumer_name=CONSUMER_DEPTH,
            minute_timestamp_ms=minute_timestamp_ms,
            alive=alive,
            last_message_time=last_message_time,
        )

    def mark_liquidation_heartbeat(
        self,
        minute_timestamp_ms: int,
        *,
        alive: bool = True,
        last_message_time: int | None = None,
    ) -> None:
        self.mark_consumer_heartbeat(
            consumer_name=CONSUMER_LIQUIDATION,
            minute_timestamp_ms=minute_timestamp_ms,
            alive=alive,
            last_message_time=last_message_time,
        )

    def mark_ls_ratio_heartbeat(self, minute_timestamp_ms: int, *, has_data: bool) -> None:
        with self._lock:
            self._bucket(minute_timestamp_ms).has_ls_ratio = has_data
        self.mark_consumer_heartbeat(
            consumer_name=CONSUMER_LS_RATIO,
            minute_timestamp_ms=minute_timestamp_ms,
            alive=True,
            last_message_time=minute_timestamp_ms,
        )

    def ingest_ws_event(
        self,
        *,
        stream: str,
        symbol: str,
        event_time: int | None,
        transact_time: int | None = None,
        arrival_time: int | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._remember_symbol(symbol)
            arrival = arrival_time if arrival_time is not None else now_ms()
            minute_key = floor_to_minute_ms(event_time if event_time is not None else arrival)
            self.mark_ws_heartbeat(minute_key, alive=True, last_message_time=arrival)

            if self._event_store is not None:
                self._event_store.append_event(
                    stream=stream,
                    symbol=symbol,
                    event_time=event_time,
                    transact_time=transact_time,
                    arrival_time=arrival,
                    raw_payload=raw_payload,
                )

    def ingest_trade_event(
        self,
        *,
        symbol: str,
        event_time: int | None,
        transact_time: int | None,
        arrival_time: int | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._remember_symbol(symbol)
            arrival = arrival_time if arrival_time is not None else now_ms()
            if self._event_store is not None:
                self._event_store.append_trade_event(
                    symbol=symbol,
                    event_time=event_time,
                    arrival_time=arrival,
                    transact_time=transact_time,
                    raw_payload=raw_payload,
                )

    def set_depth_snapshot(
        self,
        *,
        symbol: str,
        last_update_id: int,
        bids: list[tuple[float, float]],
        asks: list[tuple[float, float]],
        minute_timestamp_ms: int | None = None,
    ) -> None:
        with self._lock:
            self._remember_symbol(symbol)
            symbol_upper = symbol.upper()
            book = self._depth_books.setdefault(symbol_upper, DepthOrderBook())
            try:
                book.sync_from_snapshot(last_update_id=last_update_id, bids=bids, asks=asks)
            except DepthSyncError:
                if minute_timestamp_ms is not None:
                    minute_key = floor_to_minute_ms(minute_timestamp_ms)
                    bucket = self._bucket(minute_key)
                    bucket.depth_degraded = True
                    bucket.impact_fillable = False
                raise

            if minute_timestamp_ms is not None and book.is_synchronized and not book.degraded:
                minute_key = floor_to_minute_ms(minute_timestamp_ms)
                bucket = self._bucket(minute_key)
                if bucket.depth_event_count > 0:
                    bucket.depth_synced_event_count = max(bucket.depth_synced_event_count, 1)
                impact, fillable = book.compute_buy_price_impact()
                spread_pct, avg_bid_qty, avg_ask_qty = book.compute_health_metrics()
                bucket.price_impact_100k = impact
                bucket.impact_fillable = fillable
                bucket.depth_spread_pct = spread_pct
                bucket.depth_avg_bid_qty = avg_bid_qty
                bucket.depth_avg_ask_qty = avg_ask_qty

    def ingest_depth_diff(
        self,
        *,
        symbol: str,
        event_time: int,
        transact_time: int | None,
        first_update_id: int,
        final_update_id: int,
        bid_deltas: list[tuple[float, float]],
        ask_deltas: list[tuple[float, float]],
        arrival_time: int | None = None,
        previous_final_update_id: int | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._remember_symbol(symbol)
            arrival = arrival_time if arrival_time is not None else now_ms()
            minute_key = floor_to_minute_ms(event_time)
            bucket = self._bucket(minute_key)
            bucket.event_time = event_time if bucket.event_time is None else max(bucket.event_time, event_time)
            if transact_time is not None:
                bucket.transact_time = (
                    transact_time if bucket.transact_time is None else max(bucket.transact_time, transact_time)
                )
            bucket.arrival_time = arrival if bucket.arrival_time is None else max(bucket.arrival_time, arrival)
            bucket.depth_event_count += 1
            bucket.update_id_start = (
                first_update_id if bucket.update_id_start is None else min(bucket.update_id_start, first_update_id)
            )
            bucket.update_id_end = (
                final_update_id if bucket.update_id_end is None else max(bucket.update_id_end, final_update_id)
            )

            if transact_time is not None:
                bucket.latency_event_count += 1
                latency_engine = int(arrival - event_time)
                latency_network = int(arrival - transact_time)
                bucket.latency_engine_values.append(latency_engine)
                bucket.latency_network_values.append(latency_network)
                if latency_engine > LATENCY_BAD_MS or latency_network > LATENCY_BAD_MS:
                    bucket.ws_latency_bad = True

            self.mark_ws_heartbeat(minute_key, alive=True, last_message_time=arrival)
            self.mark_depth_heartbeat(minute_key, alive=True, last_message_time=arrival)

            bid_tuple = tuple((float(price), float(quantity)) for price, quantity in bid_deltas)
            ask_tuple = tuple((float(price), float(quantity)) for price, quantity in ask_deltas)

            if self._event_store is not None:
                self._event_store.append_depth_event(
                    symbol=symbol,
                    event_time=event_time,
                    arrival_time=arrival,
                    first_update_id=first_update_id,
                    final_update_id=final_update_id,
                    bids=bid_tuple,
                    asks=ask_tuple,
                    raw_payload=None,
                )
                self._event_store.append_event(
                    stream=f"{symbol.lower()}@depth@100ms",
                    symbol=symbol,
                    event_time=event_time,
                    transact_time=transact_time,
                    arrival_time=arrival,
                    raw_payload=None,
                )

            symbol_upper = symbol.upper()
            book = self._depth_books.setdefault(symbol_upper, DepthOrderBook())
            event = DepthDiffEvent(
                symbol=symbol_upper,
                event_time=event_time,
                first_update_id=first_update_id,
                final_update_id=final_update_id,
                bid_deltas=bid_tuple,
                ask_deltas=ask_tuple,
                previous_final_update_id=previous_final_update_id,
            )

            try:
                book.apply_event(event)
                if book.is_synchronized and not book.degraded:
                    bucket.depth_synced_event_count += 1
                    impact, fillable = book.compute_buy_price_impact()
                    spread_pct, avg_bid_qty, avg_ask_qty = book.compute_health_metrics()
                    bucket.price_impact_100k = impact
                    bucket.impact_fillable = fillable
                    bucket.depth_spread_pct = spread_pct
                    bucket.depth_avg_bid_qty = avg_bid_qty
                    bucket.depth_avg_ask_qty = avg_ask_qty
            except DepthSyncError:
                bucket.depth_degraded = True
                bucket.impact_fillable = False
                bucket.price_impact_100k = None
                raise

    def ingest_liquidation_event(
        self,
        event: LiquidationOrderEvent,
        *,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            self._remember_symbol(event.symbol)
            arrival = event.arrival_time if event.arrival_time is not None else now_ms()
            minute_key = floor_to_minute_ms(event.event_time)
            bucket = self._bucket(minute_key)
            self.mark_liquidation_heartbeat(minute_key, alive=True, last_message_time=arrival)

            side = event.side.upper()
            notional = float(event.price) * float(event.quantity)
            if side == "SELL":
                bucket.liq_long_vol_usdt += notional
                bucket.liq_long_count += 1
            elif side == "BUY":
                bucket.liq_short_vol_usdt += notional
                bucket.liq_short_count += 1

            bucket.liq_qty_total += float(event.quantity)
            bucket.liq_weighted_price_sum += float(event.price) * float(event.quantity)
            bucket.liq_event_count += 1

            if (
                self._liquidation_unfilled_supported
                and event.orig_quantity is not None
                and event.executed_quantity is not None
                and bucket.liq_unfilled_supported
            ):
                orig_quantity = max(float(event.orig_quantity), 0.0)
                executed_quantity = min(max(float(event.executed_quantity), 0.0), orig_quantity)
                bucket.liq_orig_qty_total += orig_quantity
                bucket.liq_executed_qty_total += executed_quantity
            else:
                bucket.liq_unfilled_supported = False

            if self._event_store is not None:
                self._event_store.append_liquidation_event(
                    symbol=event.symbol,
                    event_time=event.event_time,
                    arrival_time=arrival,
                    side=side,
                    price=event.price,
                    quantity=event.quantity,
                    raw_payload=raw_payload,
                )

    def ingest_predicted_funding(
        self,
        *,
        event_time: int,
        predicted_funding: float | None,
        next_funding_time: int | None,
        arrival_time: int | None = None,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        with self._lock:
            minute_key = floor_to_minute_ms(event_time)
            bucket = self._bucket(minute_key)
            bucket.predicted_funding = predicted_funding
            bucket.next_funding_time = next_funding_time

    def snapshot_for_minute(self, minute_timestamp_ms: int) -> LiveMinuteFeatures:
        minute_key = floor_to_minute_ms(minute_timestamp_ms)
        with self._lock:
            bucket = self._minutes.get(minute_key)
            if bucket is None:
                bucket = _MinuteAccumulator(liq_unfilled_supported=self._liquidation_unfilled_supported)

            has_ws_latency = bucket.latency_event_count > 0
            has_depth = bucket.depth_event_count > 0
            has_liq = bucket.liq_event_count > 0

            latency_engine = _p95_int(bucket.latency_engine_values) if has_ws_latency else None
            latency_network = _p95_int(bucket.latency_network_values) if has_ws_latency else None
            depth_degraded = self._depth_degraded_for_bucket(bucket) if has_depth else None

            liq_unfilled_supported: bool | None
            liq_unfilled_ratio: float | None
            liq_avg_fill_price: float | None
            liq_long_vol: float | None
            liq_short_vol: float | None
            liq_long_count: int | None
            liq_short_count: int | None

            if has_liq:
                liq_long_vol = bucket.liq_long_vol_usdt
                liq_short_vol = bucket.liq_short_vol_usdt
                liq_long_count = bucket.liq_long_count
                liq_short_count = bucket.liq_short_count
                liq_avg_fill_price = (
                    bucket.liq_weighted_price_sum / bucket.liq_qty_total if bucket.liq_event_count > 0 else None
                )
                liq_unfilled_supported = bool(self._liquidation_unfilled_supported and bucket.liq_unfilled_supported)
                if liq_unfilled_supported and bucket.liq_orig_qty_total > 0:
                    liq_unfilled_ratio = max(
                        0.0,
                        (bucket.liq_orig_qty_total - bucket.liq_executed_qty_total) / bucket.liq_orig_qty_total,
                    )
                else:
                    liq_unfilled_ratio = None
            else:
                liq_long_vol = None
                liq_short_vol = None
                liq_long_count = None
                liq_short_count = None
                liq_avg_fill_price = None
                liq_unfilled_ratio = None
                liq_unfilled_supported = None

            event_time = bucket.event_time if has_ws_latency else None
            transact_time = bucket.transact_time if has_ws_latency else None
            arrival_time = bucket.arrival_time if has_ws_latency else None
            ws_latency_bad: bool | None = bucket.ws_latency_bad if has_ws_latency else None

            update_id_start = bucket.update_id_start if has_depth else None
            update_id_end = bucket.update_id_end if has_depth else None
            price_impact_100k = bucket.price_impact_100k if has_depth else None
            impact_fillable = bucket.impact_fillable if has_depth else None

            db_snapshot = (
                self._event_store.snapshot_for_minute(minute_timestamp_ms=minute_key, symbol=self._symbol)
                if self._event_store is not None
                else None
            )
            if db_snapshot is not None:
                if not has_ws_latency and db_snapshot.has_ws_latency:
                    has_ws_latency = True
                    event_time = db_snapshot.event_time
                    transact_time = db_snapshot.transact_time
                    arrival_time = db_snapshot.arrival_time
                    latency_engine = db_snapshot.latency_engine
                    latency_network = db_snapshot.latency_network
                    ws_latency_bad = db_snapshot.ws_latency_bad

                if not has_depth and db_snapshot.has_depth:
                    has_depth = True
                    update_id_start = db_snapshot.update_id_start
                    update_id_end = db_snapshot.update_id_end
                    price_impact_100k = db_snapshot.price_impact_100k
                    impact_fillable = db_snapshot.impact_fillable
                    depth_degraded = db_snapshot.depth_degraded

                if not has_liq and db_snapshot.has_liq:
                    has_liq = True
                    liq_long_vol = db_snapshot.liq_long_vol_usdt
                    liq_short_vol = db_snapshot.liq_short_vol_usdt
                    liq_long_count = db_snapshot.liq_long_count
                    liq_short_count = db_snapshot.liq_short_count
                    liq_avg_fill_price = db_snapshot.liq_avg_fill_price
                    liq_unfilled_ratio = db_snapshot.liq_unfilled_ratio
                    liq_unfilled_supported = db_snapshot.liq_unfilled_supported

            return LiveMinuteFeatures(
                timestamp_ms=minute_key,
                has_ws_latency=has_ws_latency,
                has_depth=has_depth,
                has_liq=has_liq,
                has_ls_ratio=bucket.has_ls_ratio,
                event_time=event_time,
                transact_time=transact_time,
                arrival_time=arrival_time,
                latency_engine=latency_engine,
                latency_network=latency_network,
                ws_latency_bad=ws_latency_bad,
                update_id_start=update_id_start,
                update_id_end=update_id_end,
                price_impact_100k=price_impact_100k,
                impact_fillable=impact_fillable,
                depth_degraded=depth_degraded,
                liq_long_vol_usdt=liq_long_vol,
                liq_short_vol_usdt=liq_short_vol,
                liq_long_count=liq_long_count,
                liq_short_count=liq_short_count,
                liq_avg_fill_price=liq_avg_fill_price,
                liq_unfilled_ratio=liq_unfilled_ratio,
                liq_unfilled_supported=liq_unfilled_supported,
                predicted_funding=bucket.predicted_funding,
                next_funding_time=bucket.next_funding_time,
            )

    @staticmethod
    def _depth_degraded_for_bucket(bucket: _MinuteAccumulator) -> bool:
        if bucket.depth_degraded:
            return True
        if bucket.depth_synced_event_count == 0:
            return True
        if bucket.impact_fillable is False:
            return True
        if bucket.depth_spread_pct is not None and bucket.depth_spread_pct > DEPTH_DEGRADED_SPREAD_MAX_PCT:
            return True
        if (
            bucket.depth_avg_bid_qty is not None
            and bucket.depth_avg_bid_qty < DEPTH_DEGRADED_MIN_AVG_LEVEL_QTY
        ):
            return True
        if (
            bucket.depth_avg_ask_qty is not None
            and bucket.depth_avg_ask_qty < DEPTH_DEGRADED_MIN_AVG_LEVEL_QTY
        ):
            return True
        return False

    def _heartbeat_for(self, consumer_name: str, minute_timestamp_ms: int) -> ConsumerHeartbeat | None:
        return self._heartbeats.get((consumer_name, minute_timestamp_ms))

    def _bucket(self, minute_timestamp_ms: int) -> _MinuteAccumulator:
        minute_key = floor_to_minute_ms(minute_timestamp_ms)
        bucket = self._minutes.get(minute_key)
        if bucket is None:
            bucket = _MinuteAccumulator(liq_unfilled_supported=self._liquidation_unfilled_supported)
            self._minutes[minute_key] = bucket
        return bucket

    def _remember_symbol(self, symbol: str | None) -> None:
        if self._symbol is not None:
            return
        if symbol is None:
            return
        normalized = symbol.strip().upper()
        if normalized:
            self._symbol = normalized


class BinanceWsPayloadProcessor:
    """Parse Binance futures WS payloads and feed the live collector."""

    def __init__(self, collector: InMemoryLiveCollector, symbol: str) -> None:
        self._collector = collector
        self._symbol = symbol.upper()

    def process_stream_payload(
        self,
        *,
        stream_name: str,
        payload: dict[str, Any],
        arrival_time_ms: int | None = None,
    ) -> None:
        stream_lower = stream_name.lower()

        if "@depth" in stream_lower:
            self._process_depth_payload(stream_name=stream_name, payload=payload, arrival_time_ms=arrival_time_ms)
            return
        if "@forceorder" in stream_lower:
            self._process_liquidation_payload(stream_name=stream_name, payload=payload, arrival_time_ms=arrival_time_ms)
            return
        if "@aggtrade" in stream_lower:
            self._process_agg_trade_payload(stream_name=stream_name, payload=payload, arrival_time_ms=arrival_time_ms)
            return
        if "@markprice" in stream_lower:
            self._process_mark_price_payload(payload=payload, arrival_time_ms=arrival_time_ms)

    def process_combined_payload(self, payload: dict[str, Any], arrival_time_ms: int | None = None) -> None:
        stream = payload.get("stream")
        data = payload.get("data")
        if isinstance(stream, str) and isinstance(data, dict):
            self.process_stream_payload(stream_name=stream, payload=data, arrival_time_ms=arrival_time_ms)

    def _process_depth_payload(
        self,
        *,
        stream_name: str,
        payload: dict[str, Any],
        arrival_time_ms: int | None,
    ) -> None:
        event_time = _coerce_int(payload.get("E"))
        transact_time = _coerce_int(payload.get("T"))
        first_update_id = _coerce_int(payload.get("U"))
        final_update_id = _coerce_int(payload.get("u"))
        if event_time is None or first_update_id is None or final_update_id is None:
            return

        symbol = str(payload.get("s") or self._symbol_from_stream(stream_name))
        bid_deltas = list(_parse_depth_levels(payload.get("b")))
        ask_deltas = list(_parse_depth_levels(payload.get("a")))
        previous_final_update_id = _coerce_int(payload.get("pu"))

        self._collector.ingest_depth_diff(
            symbol=symbol,
            event_time=event_time,
            transact_time=transact_time,
            first_update_id=first_update_id,
            final_update_id=final_update_id,
            bid_deltas=bid_deltas,
            ask_deltas=ask_deltas,
            arrival_time=arrival_time_ms,
            previous_final_update_id=previous_final_update_id,
            raw_payload=payload,
        )

    def _process_liquidation_payload(
        self,
        *,
        stream_name: str,
        payload: dict[str, Any],
        arrival_time_ms: int | None,
    ) -> None:
        order_payload = payload.get("o")
        if not isinstance(order_payload, dict):
            return

        side = str(order_payload.get("S") or "").upper()
        if side not in {"BUY", "SELL"}:
            return

        average_price = _coerce_float(order_payload.get("ap"))
        price = average_price if average_price is not None else _coerce_float(order_payload.get("p"))
        quantity = _coerce_float(order_payload.get("q"))
        if price is None or quantity is None or quantity <= 0:
            return

        event_time = _coerce_int(payload.get("E"))
        if event_time is None:
            event_time = _coerce_int(order_payload.get("T"))
        if event_time is None:
            return

        symbol = str(order_payload.get("s") or payload.get("s") or self._symbol_from_stream(stream_name))
        orig_qty = _coerce_float(order_payload.get("q"))
        executed_qty = _coerce_float(order_payload.get("l"))

        self._collector.ingest_liquidation_event(
            LiquidationOrderEvent(
                symbol=symbol,
                event_time=event_time,
                side=side,
                price=price,
                quantity=quantity,
                arrival_time=arrival_time_ms,
                orig_quantity=orig_qty,
                executed_quantity=executed_qty,
            ),
            raw_payload=payload,
        )

    def _process_agg_trade_payload(
        self,
        *,
        stream_name: str,
        payload: dict[str, Any],
        arrival_time_ms: int | None,
    ) -> None:
        symbol = str(payload.get("s") or self._symbol_from_stream(stream_name))
        event_time = _coerce_int(payload.get("E"))
        transact_time = _coerce_int(payload.get("T"))
        if event_time is None:
            event_time = transact_time

        self._collector.ingest_trade_event(
            symbol=symbol,
            event_time=event_time,
            transact_time=transact_time,
            arrival_time=arrival_time_ms,
            raw_payload=payload,
        )

    def _process_mark_price_payload(self, *, payload: dict[str, Any], arrival_time_ms: int | None) -> None:
        event_time = _coerce_int(payload.get("E"))
        if event_time is None:
            return

        predicted_funding = _coerce_float(payload.get("r"))
        next_funding_time = _coerce_int(payload.get("T"))

        self._collector.ingest_predicted_funding(
            event_time=event_time,
            predicted_funding=predicted_funding,
            next_funding_time=next_funding_time,
            arrival_time=arrival_time_ms,
            raw_payload=payload,
        )

    def _symbol_from_stream(self, stream_name: str) -> str:
        prefix = stream_name.split("@", maxsplit=1)[0]
        if prefix:
            return prefix.upper()
        return self._symbol


class BinanceWebSocketWorker:
    def __init__(
        self,
        *,
        name: str,
        url: str,
        on_message: Callable[[dict[str, Any], int], None],
        on_connection_change: Callable[[bool], None] | None = None,
        reconnect_seconds: float = 2.0,
        read_timeout_seconds: float = 1.0,
    ) -> None:
        self._name = name
        self._url = url
        self._on_message = on_message
        self._on_connection_change = on_connection_change
        self._reconnect_seconds = reconnect_seconds
        self._read_timeout_seconds = read_timeout_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name=f"ws-worker-{self._name}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _run_loop(self) -> None:
        try:
            import websockets  # type: ignore
        except ImportError:
            logger.error(
                "websockets package is required for live stream workers; install with `pip install websockets`"
            )
            return

        with asyncio.Runner() as runner:
            while not self._stop_event.is_set():
                try:
                    runner.run(self._run_once(websockets))
                except Exception:
                    logger.exception("WebSocket worker failed", extra={"worker": self._name, "url": self._url})
                finally:
                    self._publish_connection(False)

                if self._stop_event.is_set():
                    break
                time.sleep(self._reconnect_seconds)

    async def _run_once(self, websockets_module: Any) -> None:
        async with websockets_module.connect(
            self._url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=2**22,
        ) as websocket:
            self._publish_connection(True)

            while not self._stop_event.is_set():
                try:
                    payload = await asyncio.wait_for(websocket.recv(), timeout=self._read_timeout_seconds)
                except TimeoutError:
                    continue

                raw_text: str
                if isinstance(payload, bytes):
                    raw_text = payload.decode("utf-8")
                else:
                    raw_text = payload

                try:
                    message = json.loads(raw_text)
                except json.JSONDecodeError:
                    logger.debug("Dropping non-JSON WebSocket payload", extra={"worker": self._name})
                    continue

                self._on_message(message, now_ms())

    def _publish_connection(self, connected: bool) -> None:
        if self._on_connection_change is None:
            return
        self._on_connection_change(connected)


class BinanceLiveStreamSupervisor:
    """Supervisor for depth, forceOrder, and aggTrade stream ingestion."""

    def __init__(
        self,
        *,
        symbol: str,
        websocket_base_url: str,
        rest_client: Any,
        collector: InMemoryLiveCollector,
        depth_snapshot_limit: int = 1000,
        reconnect_seconds: float = 2.0,
        heartbeat_interval_seconds: float = 1.0,
    ) -> None:
        self._symbol = symbol.upper()
        self._symbol_lower = self._symbol.lower()
        self._websocket_base_url = websocket_base_url
        self._rest_client = rest_client
        self._collector = collector
        self._depth_snapshot_limit = depth_snapshot_limit
        self._reconnect_seconds = reconnect_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._processor = BinanceWsPayloadProcessor(collector=collector, symbol=self._symbol)

        self._connection_state: dict[str, _WorkerConnectionState] = {
            CONSUMER_WS_LATENCY: _WorkerConnectionState(),
            CONSUMER_DEPTH: _WorkerConnectionState(),
            CONSUMER_LIQUIDATION: _WorkerConnectionState(),
        }
        self._state_lock = threading.RLock()

        self._workers: list[BinanceWebSocketWorker] = []
        self._heartbeat_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._workers:
            return

        self._stop_event.clear()
        self._resync_depth_book(minute_timestamp_ms=now_ms())

        depth_stream = f"{self._symbol_lower}@depth@100ms"
        liq_stream = f"{self._symbol_lower}@forceOrder"
        trade_stream = f"{self._symbol_lower}@aggTrade"
        mark_price_stream = f"{self._symbol_lower}@markPrice@1s"

        self._workers = [
            BinanceWebSocketWorker(
                name="depth",
                url=self._stream_url(depth_stream),
                on_message=lambda payload, arrival: self._on_depth_message(depth_stream, payload, arrival),
                on_connection_change=lambda connected: self._on_connection_change(CONSUMER_DEPTH, connected),
                reconnect_seconds=self._reconnect_seconds,
            ),
            BinanceWebSocketWorker(
                name="force_order",
                url=self._stream_url(liq_stream),
                on_message=lambda payload, arrival: self._on_liq_message(liq_stream, payload, arrival),
                on_connection_change=lambda connected: self._on_connection_change(CONSUMER_LIQUIDATION, connected),
                reconnect_seconds=self._reconnect_seconds,
            ),
            BinanceWebSocketWorker(
                name="agg_trade",
                url=self._stream_url(trade_stream),
                on_message=lambda payload, arrival: self._on_trade_message(trade_stream, payload, arrival),
                on_connection_change=lambda connected: self._on_connection_change(CONSUMER_WS_LATENCY, connected),
                reconnect_seconds=self._reconnect_seconds,
            ),
            BinanceWebSocketWorker(
                name="mark_price",
                url=self._stream_url(mark_price_stream),
                on_message=lambda payload, arrival: self._processor.process_stream_payload(
                    stream_name=mark_price_stream,
                    payload=payload,
                    arrival_time_ms=arrival,
                ),
                reconnect_seconds=self._reconnect_seconds,
            ),
        ]

        for worker in self._workers:
            worker.start()

        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, name="ws-heartbeats", daemon=True)
        self._heartbeat_thread.start()

    def stop(self) -> None:
        self._stop_event.set()

        for worker in self._workers:
            worker.stop()
        self._workers = []

        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5.0)
            self._heartbeat_thread = None

    def _on_connection_change(self, consumer_name: str, connected: bool) -> None:
        with self._state_lock:
            state = self._connection_state.setdefault(consumer_name, _WorkerConnectionState())
            state.connected = connected

    def _record_message(self, consumer_name: str, arrival_time_ms: int) -> None:
        with self._state_lock:
            state = self._connection_state.setdefault(consumer_name, _WorkerConnectionState())
            state.last_message_time = arrival_time_ms

    def _on_depth_message(self, stream_name: str, payload: dict[str, Any], arrival_time_ms: int) -> None:
        self._record_message(CONSUMER_DEPTH, arrival_time_ms)
        try:
            self._processor.process_stream_payload(
                stream_name=stream_name,
                payload=payload,
                arrival_time_ms=arrival_time_ms,
            )
        except DepthSyncError:
            logger.warning("Depth continuity broken; resyncing snapshot", extra={"symbol": self._symbol})
            self._resync_depth_book(minute_timestamp_ms=arrival_time_ms)

    def _on_liq_message(self, stream_name: str, payload: dict[str, Any], arrival_time_ms: int) -> None:
        self._record_message(CONSUMER_LIQUIDATION, arrival_time_ms)
        self._processor.process_stream_payload(
            stream_name=stream_name,
            payload=payload,
            arrival_time_ms=arrival_time_ms,
        )

    def _on_trade_message(self, stream_name: str, payload: dict[str, Any], arrival_time_ms: int) -> None:
        self._record_message(CONSUMER_WS_LATENCY, arrival_time_ms)
        self._processor.process_stream_payload(
            stream_name=stream_name,
            payload=payload,
            arrival_time_ms=arrival_time_ms,
        )

    def _heartbeat_loop(self) -> None:
        last_minute: int | None = None
        while not self._stop_event.is_set():
            current_minute = floor_to_minute_ms(now_ms())
            if current_minute != last_minute:
                self._emit_heartbeats(current_minute)
                last_minute = current_minute
            time.sleep(self._heartbeat_interval_seconds)

    def _emit_heartbeats(self, minute_timestamp_ms: int) -> None:
        with self._state_lock:
            ws_state = self._connection_state[CONSUMER_WS_LATENCY]
            depth_state = self._connection_state[CONSUMER_DEPTH]
            liq_state = self._connection_state[CONSUMER_LIQUIDATION]

        self._collector.mark_ws_heartbeat(
            minute_timestamp_ms,
            alive=ws_state.connected,
            last_message_time=ws_state.last_message_time,
        )
        self._collector.mark_depth_heartbeat(
            minute_timestamp_ms,
            alive=depth_state.connected,
            last_message_time=depth_state.last_message_time,
        )
        self._collector.mark_liquidation_heartbeat(
            minute_timestamp_ms,
            alive=liq_state.connected,
            last_message_time=liq_state.last_message_time,
        )

    def _stream_url(self, stream_name: str) -> str:
        base = self._websocket_base_url.rstrip("/")
        if base.endswith("/ws"):
            return f"{base}/{stream_name}"
        if base.endswith("/stream"):
            return f"{base}?streams={stream_name}"
        return f"{base}/ws/{stream_name}"

    def _resync_depth_book(self, *, minute_timestamp_ms: int) -> None:
        try:
            snapshot = self._rest_client.fetch_depth_snapshot(self._symbol, limit=self._depth_snapshot_limit)
            self._collector.set_depth_snapshot(
                symbol=self._symbol,
                last_update_id=int(snapshot["last_update_id"]),
                bids=list(snapshot["bids"]),
                asks=list(snapshot["asks"]),
                minute_timestamp_ms=minute_timestamp_ms,
            )
        except Exception:
            logger.exception("Depth snapshot resync failed", extra={"symbol": self._symbol})
