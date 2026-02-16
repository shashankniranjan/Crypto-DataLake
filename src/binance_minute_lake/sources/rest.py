from __future__ import annotations

import logging
import random
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class BinanceRESTClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 20,
        retries: int = 5,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            timeout=timeout_seconds,
            transport=transport,
        )
        self._retries = max(1, retries)
        self._min_retry_delay_seconds = 1.0
        self._max_backoff_seconds = 60.0
        self._min_interval_seconds = 0.1  # ~10 requests/sec to avoid burst 429s
        self._last_request_monotonic: float | None = None

    def close(self) -> None:
        self._client.close()

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        last_transport_error: httpx.TransportError | None = None

        for attempt in range(1, self._retries + 1):
            try:
                # simple client-side rate limiter to avoid 429 bursts
                now = time.monotonic()
                if self._last_request_monotonic is not None:
                    wait = self._min_interval_seconds - (now - self._last_request_monotonic)
                    if wait > 0:
                        time.sleep(wait)
                response = self._client.get(path, params=params)
                self._last_request_monotonic = time.monotonic()
            except httpx.TransportError as exc:
                last_transport_error = exc
                if attempt >= self._retries:
                    raise
                self._sleep_before_retry(attempt=attempt, path=path, status_code=None, reason=exc.__class__.__name__)
                continue

            if response.status_code < 400:
                return response.json()

            if self._is_retryable_status(response.status_code) and attempt < self._retries:
                retry_after_seconds = self._parse_retry_after_seconds(response=response)
                self._sleep_before_retry(
                    attempt=attempt,
                    path=path,
                    status_code=response.status_code,
                    reason=f"HTTP {response.status_code}",
                    retry_after_seconds=retry_after_seconds,
                )
                continue

            response.raise_for_status()

        if last_transport_error is not None:
            raise last_transport_error
        raise RuntimeError("REST call exhausted retries without a concrete error")

    @staticmethod
    def _is_retryable_status(status_code: int) -> bool:
        return status_code == 429 or 500 <= status_code < 600

    @staticmethod
    def _parse_retry_after_seconds(response: httpx.Response) -> float | None:
        raw_value = response.headers.get("Retry-After")
        if raw_value is None:
            return None

        raw_value = raw_value.strip()
        try:
            return max(0.0, float(raw_value))
        except ValueError:
            pass

        try:
            parsed = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError):
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        delay_seconds = float((parsed.astimezone(UTC) - datetime.now(tz=UTC)).total_seconds())
        return max(0.0, delay_seconds)

    def _sleep_before_retry(
        self,
        *,
        attempt: int,
        path: str,
        status_code: int | None,
        reason: str,
        retry_after_seconds: float | None = None,
    ) -> None:
        if retry_after_seconds is not None:
            delay = retry_after_seconds
        else:
            delay = min(
                self._max_backoff_seconds,
                self._min_retry_delay_seconds * (2 ** max(attempt - 1, 0)),
            )
        # add small jitter to desynchronize from exchange rate limits
        delay += random.uniform(0.0, 0.3)  # noqa: S311

        logger.warning(
            "Retrying Binance REST request",
            extra={
                "path": path,
                "attempt": attempt,
                "max_attempts": self._retries,
                "status_code": status_code,
                "reason": reason,
                "sleep_seconds": round(delay, 3),
            },
        )
        time.sleep(delay)

    @staticmethod
    def _to_ms(value: datetime) -> int:
        return int(value.timestamp() * 1000)

    def fetch_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1m",
        limit: int = 1500,
    ) -> list[dict[str, Any]]:
        payload = self._get(
            "/fapi/v1/klines",
            {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": self._to_ms(start_time),
                "endTime": self._to_ms(end_time),
                "limit": limit,
            },
        )
        return [
            {
                "open_time": int(item[0]),
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume_btc": float(item[5]),
                "close_time": int(item[6]),
                "volume_usdt": float(item[7]),
                "trade_count": int(item[8]),
                "taker_buy_vol_btc": float(item[9]),
                "taker_buy_vol_usdt": float(item[10]),
            }
            for item in payload
        ]

    def fetch_mark_price_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1m",
        limit: int = 1500,
    ) -> list[dict[str, Any]]:
        payload = self._get(
            "/fapi/v1/markPriceKlines",
            {
                "symbol": symbol.upper(),
                "interval": interval,
                "startTime": self._to_ms(start_time),
                "endTime": self._to_ms(end_time),
                "limit": limit,
            },
        )
        return [
            {
                "open_time": int(item[0]),
                "mark_price_open": float(item[1]),
                "mark_price_high": float(item[2]),
                "mark_price_low": float(item[3]),
                "mark_price_close": float(item[4]),
            }
            for item in payload
        ]

    def fetch_index_price_klines(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        interval: str = "1m",
        limit: int = 1500,
    ) -> list[dict[str, Any]]:
        payload = self._get(
            "/fapi/v1/indexPriceKlines",
            {
                "pair": symbol.upper(),
                "interval": interval,
                "startTime": self._to_ms(start_time),
                "endTime": self._to_ms(end_time),
                "limit": limit,
            },
        )
        return [
            {
                "open_time": int(item[0]),
                "index_price_open": float(item[1]),
                "index_price_high": float(item[2]),
                "index_price_low": float(item[3]),
                "index_price_close": float(item[4]),
            }
            for item in payload
        ]

    def fetch_agg_trades(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        payload = self._get(
            "/fapi/v1/aggTrades",
            {
                "symbol": symbol.upper(),
                "startTime": self._to_ms(start_time),
                "endTime": self._to_ms(end_time),
                "limit": limit,
            },
        )
        return [
            {
                "agg_trade_id": int(item["a"]),
                "price": float(item["p"]),
                "qty": float(item["q"]),
                "first_trade_id": int(item["f"]),
                "last_trade_id": int(item["l"]),
                "transact_time": int(item["T"]),
                "is_buyer_maker": bool(item["m"]),
            }
            for item in payload
        ]

    def fetch_book_ticker(self, symbol: str) -> dict[str, Any]:
        payload = self._get(
            "/fapi/v1/ticker/bookTicker",
            {
                "symbol": symbol.upper(),
            },
        )
        return {
            "bid_price": float(payload["bidPrice"]),
            "bid_qty": float(payload["bidQty"]),
            "ask_price": float(payload["askPrice"]),
            "ask_qty": float(payload["askQty"]),
            "event_time": int(payload.get("time", 0)),
        }

    def fetch_premium_index(self, symbol: str) -> dict[str, Any]:
        payload = self._get(
            "/fapi/v1/premiumIndex",
            {
                "symbol": symbol.upper(),
            },
        )
        return {
            "mark_price": float(payload["markPrice"]),
            "index_price": float(payload["indexPrice"]),
            "last_funding_rate": float(payload["lastFundingRate"]),
            "next_funding_time": int(payload.get("nextFundingTime", 0)),
            "predicted_funding": float(payload.get("predictedFundingRate", 0.0)),
            "event_time": int(payload.get("time", 0)),
        }

    def fetch_open_interest(self, symbol: str) -> dict[str, Any]:
        payload = self._get(
            "/fapi/v1/openInterest",
            {
                "symbol": symbol.upper(),
            },
        )
        return {
            "symbol": payload["symbol"],
            "open_interest": float(payload["openInterest"]),
            "event_time": int(payload.get("time", 0)),
        }

    def fetch_depth_snapshot(self, symbol: str, limit: int = 1000) -> dict[str, Any]:
        payload = self._get(
            "/fapi/v1/depth",
            {
                "symbol": symbol.upper(),
                "limit": limit,
            },
        )
        return {
            "symbol": symbol.upper(),
            "last_update_id": int(payload["lastUpdateId"]),
            "bids": [(float(price), float(quantity)) for price, quantity in payload.get("bids", [])],
            "asks": [(float(price), float(quantity)) for price, quantity in payload.get("asks", [])],
            "event_time": int(payload.get("E", 0) or 0),
            "transact_time": int(payload.get("T", 0) or 0),
        }

    def fetch_top_trader_long_short_account_ratio(
        self,
        symbol: str,
        *,
        period: str = "5m",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "period": period,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = self._to_ms(start_time)
        if end_time is not None:
            params["endTime"] = self._to_ms(end_time)

        payload = self._get("/futures/data/topLongShortAccountRatio", params)
        return [
            {
                "symbol": item["symbol"],
                "data_time": int(item["timestamp"]),
                "ratio": float(item["longShortRatio"]),
                "long_account": float(item["longAccount"]),
                "short_account": float(item["shortAccount"]),
            }
            for item in payload
        ]

    def fetch_global_long_short_account_ratio(
        self,
        symbol: str,
        *,
        period: str = "5m",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "period": period,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = self._to_ms(start_time)
        if end_time is not None:
            params["endTime"] = self._to_ms(end_time)

        payload = self._get("/futures/data/globalLongShortAccountRatio", params)
        return [
            {
                "symbol": item["symbol"],
                "data_time": int(item["timestamp"]),
                "ratio": float(item["longShortRatio"]),
                "long_account": float(item["longAccount"]),
                "short_account": float(item["shortAccount"]),
            }
            for item in payload
        ]

    def fetch_funding_rate(
        self,
        symbol: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"symbol": symbol.upper(), "limit": limit}
        if start_time is not None:
            params["startTime"] = self._to_ms(start_time)
        if end_time is not None:
            params["endTime"] = self._to_ms(end_time)

        payload = self._get("/fapi/v1/fundingRate", params)
        rows: list[dict[str, Any]] = []
        for item in payload:
            mark_price_raw = item.get("markPrice")
            mark_price = None
            if mark_price_raw not in {"", None}:
                mark_price = float(mark_price_raw)
            rows.append(
                {
                    "symbol": item["symbol"],
                    "funding_rate": float(item["fundingRate"]),
                    "funding_time": int(item["fundingTime"]),
                    "mark_price": mark_price,
                }
            )
        return rows
