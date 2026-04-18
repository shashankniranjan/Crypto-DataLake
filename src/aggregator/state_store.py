from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True, slots=True)
class AggregationState:
    symbol: str
    timeframe: str
    last_completed_bucket_start: datetime | None
    last_seen_source_minute: datetime | None
    last_run_at: datetime | None
    status: str


class AggregatorStateStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self._db_path)
        connection.row_factory = sqlite3.Row
        try:
            yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS aggregation_state (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    last_completed_bucket_start TEXT,
                    last_seen_source_minute TEXT,
                    last_run_at TEXT,
                    status TEXT NOT NULL,
                    PRIMARY KEY (symbol, timeframe)
                )
                """
            )
            conn.commit()

    def get_state(self, symbol: str, timeframe: str) -> AggregationState | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT symbol, timeframe, last_completed_bucket_start, last_seen_source_minute, last_run_at, status
                FROM aggregation_state
                WHERE symbol = ? AND timeframe = ?
                """,
                (symbol, timeframe),
            ).fetchone()
        if row is None:
            return None
        return AggregationState(
            symbol=str(row["symbol"]),
            timeframe=str(row["timeframe"]),
            last_completed_bucket_start=_parse_dt(row["last_completed_bucket_start"]),
            last_seen_source_minute=_parse_dt(row["last_seen_source_minute"]),
            last_run_at=_parse_dt(row["last_run_at"]),
            status=str(row["status"]),
        )

    def upsert_state(
        self,
        *,
        symbol: str,
        timeframe: str,
        last_completed_bucket_start: datetime | None,
        last_seen_source_minute: datetime | None,
        status: str,
    ) -> None:
        now = datetime.now(tz=UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO aggregation_state(
                    symbol, timeframe, last_completed_bucket_start, last_seen_source_minute, last_run_at, status
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe) DO UPDATE SET
                    last_completed_bucket_start = excluded.last_completed_bucket_start,
                    last_seen_source_minute = excluded.last_seen_source_minute,
                    last_run_at = excluded.last_run_at,
                    status = excluded.status
                """,
                (
                    symbol.upper(),
                    timeframe,
                    _dt_iso(last_completed_bucket_start),
                    _dt_iso(last_seen_source_minute),
                    now,
                    status,
                ),
            )
            conn.commit()


def _dt_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _parse_dt(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, str):
        return datetime.fromisoformat(value).astimezone(UTC)
    raise TypeError(f"Expected ISO datetime string or None, got {type(value)!r}")
