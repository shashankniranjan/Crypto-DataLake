from __future__ import annotations


def calculate_traditional_pivots(*, high: float, low: float, close: float) -> dict[str, float]:
    pivot = (high + low + close) / 3.0
    return {
        "p": pivot,
        "r1": (2.0 * pivot) - low,
        "r2": pivot + (high - low),
        "s1": (2.0 * pivot) - high,
        "s2": pivot - (high - low),
    }
