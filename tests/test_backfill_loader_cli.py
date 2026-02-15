from datetime import UTC, datetime, timedelta

import pytest
import typer

from binance_minute_lake.cli.app import (
    _compute_backfill_loader_window,
    _last_completed_utc_day_end,
    _parse_backfill_loader_target,
)


def test_parse_backfill_loader_target_accepts_all_keywords() -> None:
    assert _parse_backfill_loader_target("all") is None
    assert _parse_backfill_loader_target("last-5-years") is None
    assert _parse_backfill_loader_target("last_5_years") is None
    assert _parse_backfill_loader_target("last5") is None


def test_parse_backfill_loader_target_accepts_year() -> None:
    assert _parse_backfill_loader_target("2024") == 2024


def test_parse_backfill_loader_target_rejects_invalid_values() -> None:
    with pytest.raises(typer.BadParameter):
        _parse_backfill_loader_target("abc")
    with pytest.raises(typer.BadParameter):
        _parse_backfill_loader_target("1969")


def test_compute_backfill_loader_window_for_last_five_years() -> None:
    now = datetime(2026, 2, 15, 12, 34, tzinfo=UTC)

    start, end = _compute_backfill_loader_window(
        year=None,
        now_utc=now,
        safety_lag_minutes=3,
        lookback_years=5,
    )

    assert end == datetime(2026, 2, 15, 12, 31, tzinfo=UTC)
    assert end - start == timedelta(days=365 * 5)


def test_compute_backfill_loader_window_for_completed_year() -> None:
    now = datetime(2026, 2, 15, 12, 34, tzinfo=UTC)

    start, end = _compute_backfill_loader_window(
        year=2024,
        now_utc=now,
        safety_lag_minutes=3,
    )

    assert start == datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    assert end == datetime(2024, 12, 31, 23, 59, tzinfo=UTC)


def test_compute_backfill_loader_window_clamps_current_year_to_now() -> None:
    now = datetime(2026, 2, 15, 12, 34, tzinfo=UTC)

    start, end = _compute_backfill_loader_window(
        year=2026,
        now_utc=now,
        safety_lag_minutes=3,
    )

    assert start == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert end == datetime(2026, 2, 15, 12, 31, tzinfo=UTC)


def test_compute_backfill_loader_window_rejects_future_year() -> None:
    now = datetime(2026, 2, 15, 12, 34, tzinfo=UTC)

    with pytest.raises(typer.BadParameter):
        _compute_backfill_loader_window(
            year=2027,
            now_utc=now,
            safety_lag_minutes=3,
        )


def test_last_completed_utc_day_end() -> None:
    now = datetime(2026, 2, 15, 12, 34, tzinfo=UTC)
    assert _last_completed_utc_day_end(now) == datetime(2026, 2, 14, 23, 59, tzinfo=UTC)
