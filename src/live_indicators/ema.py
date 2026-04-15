from __future__ import annotations

from collections.abc import Sequence


def calculate_tradingview_ema(values: Sequence[float], length: int) -> list[float | None]:
    if length < 1:
        raise ValueError("EMA length must be at least 1")

    result: list[float | None] = [None] * len(values)
    if len(values) < length:
        return result

    alpha = 2.0 / (float(length) + 1.0)
    seed = sum(float(item) for item in values[:length]) / float(length)
    result[length - 1] = seed
    previous = seed

    for index in range(length, len(values)):
        current = (alpha * float(values[index])) + ((1.0 - alpha) * previous)
        result[index] = current
        previous = current

    return result
