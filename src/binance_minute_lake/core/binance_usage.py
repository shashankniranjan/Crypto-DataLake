from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Iterator


@dataclass(slots=True)
class BinanceUsageTracker:
    request_path: str
    rest_call_count: int = 0
    retry_count: int = 0
    status_429_count: int = 0
    status_418_count: int = 0
    status_403_count: int = 0
    max_retry_after_seconds: float | None = None
    endpoint_counts: Counter[str] = field(default_factory=Counter)
    first_weight_headers: dict[str, int] = field(default_factory=dict)
    last_weight_headers: dict[str, int] = field(default_factory=dict)
    max_weight_headers: dict[str, int] = field(default_factory=dict)
    cache_event_counts: Counter[str] = field(default_factory=Counter)

    def record_rest_response(self, *, path: str, status_code: int, headers: dict[str, str]) -> None:
        self.rest_call_count += 1
        self.endpoint_counts[path] += 1

        if status_code == 429:
            self.status_429_count += 1
        elif status_code == 418:
            self.status_418_count += 1
        elif status_code == 403:
            self.status_403_count += 1

        retry_after_raw = headers.get("retry-after")
        if retry_after_raw is not None:
            try:
                retry_after = float(retry_after_raw)
            except ValueError:
                retry_after = None
            if retry_after is not None:
                if self.max_retry_after_seconds is None:
                    self.max_retry_after_seconds = retry_after
                else:
                    self.max_retry_after_seconds = max(self.max_retry_after_seconds, retry_after)

        for key, value in headers.items():
            if not key.startswith("x-mbx-used-weight"):
                continue
            try:
                parsed = int(value)
            except ValueError:
                continue
            self.first_weight_headers.setdefault(key, parsed)
            self.last_weight_headers[key] = parsed
            current_max = self.max_weight_headers.get(key)
            self.max_weight_headers[key] = parsed if current_max is None else max(current_max, parsed)

    def record_retry(self) -> None:
        self.retry_count += 1

    def record_cache_event(self, name: str) -> None:
        self.cache_event_counts[name] += 1

    def as_log_fields(self) -> dict[str, object]:
        observed_weight_progress = {
            header: {
                "first": self.first_weight_headers.get(header),
                "last": self.last_weight_headers.get(header),
                "max": self.max_weight_headers.get(header),
                "delta_after_first": max(
                    (self.last_weight_headers.get(header) or 0) - (self.first_weight_headers.get(header) or 0),
                    0,
                ),
            }
            for header in sorted(self.last_weight_headers)
        }
        return {
            "request_path": self.request_path,
            "binance_rest_call_count": self.rest_call_count,
            "binance_retry_count": self.retry_count,
            "binance_429_count": self.status_429_count,
            "binance_418_count": self.status_418_count,
            "binance_403_count": self.status_403_count,
            "binance_max_retry_after_seconds": self.max_retry_after_seconds,
            "binance_endpoint_counts": dict(self.endpoint_counts),
            "binance_observed_weight_headers": observed_weight_progress,
            "binance_cache_events": dict(self.cache_event_counts),
        }


_CURRENT_TRACKER: ContextVar[BinanceUsageTracker | None] = ContextVar("binance_usage_tracker", default=None)


@contextmanager
def binance_usage_scope(request_path: str) -> Iterator[BinanceUsageTracker]:
    tracker = BinanceUsageTracker(request_path=request_path)
    token = _CURRENT_TRACKER.set(tracker)
    try:
        yield tracker
    finally:
        _CURRENT_TRACKER.reset(token)


def current_binance_usage_tracker() -> BinanceUsageTracker | None:
    return _CURRENT_TRACKER.get()


def record_binance_rest_response(*, path: str, status_code: int, headers: dict[str, str]) -> None:
    tracker = current_binance_usage_tracker()
    if tracker is None:
        return
    tracker.record_rest_response(path=path, status_code=status_code, headers=headers)


def record_binance_retry() -> None:
    tracker = current_binance_usage_tracker()
    if tracker is None:
        return
    tracker.record_retry()


def record_binance_cache_event(name: str) -> None:
    tracker = current_binance_usage_tracker()
    if tracker is None:
        return
    tracker.record_cache_event(name)
