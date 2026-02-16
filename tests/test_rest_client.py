import httpx
import pytest

from binance_minute_lake.sources.rest import BinanceRESTClient


def test_rest_client_retries_on_429_then_succeeds() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return httpx.Response(
                status_code=429,
                request=request,
                headers={"Retry-After": "0"},
                json={"code": -1003, "msg": "Too many requests"},
            )
        return httpx.Response(
            status_code=200,
            request=request,
            json={
                "markPrice": "100.0",
                "indexPrice": "99.0",
                "lastFundingRate": "0.0001",
                "nextFundingTime": 0,
                "predictedFundingRate": "0.0002",
                "time": 123,
            },
        )

    client = BinanceRESTClient(
        base_url="https://fapi.binance.com",
        retries=3,
        transport=httpx.MockTransport(handler),
    )
    try:
        payload = client.fetch_premium_index("BTCUSDT")
    finally:
        client.close()

    assert call_count == 3
    assert payload["mark_price"] == 100.0
    assert payload["index_price"] == 99.0
    assert payload["predicted_funding"] == 0.0002


def test_rest_client_does_not_retry_on_400() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(status_code=400, request=request, json={"code": -1100, "msg": "Bad request"})

    client = BinanceRESTClient(
        base_url="https://fapi.binance.com",
        retries=5,
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(httpx.HTTPStatusError):
            client.fetch_open_interest("BTCUSDT")
    finally:
        client.close()

    assert call_count == 1


def test_rest_client_parses_long_short_ratio_endpoints() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/topLongShortAccountRatio"):
            return httpx.Response(
                status_code=200,
                request=request,
                json=[
                    {
                        "symbol": "BTCUSDT",
                        "longShortRatio": "1.25",
                        "longAccount": "0.55",
                        "shortAccount": "0.44",
                        "timestamp": 1_735_689_600_000,
                    }
                ],
            )
        return httpx.Response(
            status_code=200,
            request=request,
            json=[
                {
                    "symbol": "BTCUSDT",
                    "longShortRatio": "1.10",
                    "longAccount": "0.52",
                    "shortAccount": "0.47",
                    "timestamp": 1_735_689_600_000,
                }
            ],
        )

    client = BinanceRESTClient(
        base_url="https://fapi.binance.com",
        transport=httpx.MockTransport(handler),
    )
    try:
        top = client.fetch_top_trader_long_short_account_ratio("BTCUSDT", period="5m")
        global_ratio = client.fetch_global_long_short_account_ratio("BTCUSDT", period="5m")
    finally:
        client.close()

    assert top[0]["ratio"] == 1.25
    assert top[0]["long_account"] == 0.55
    assert global_ratio[0]["ratio"] == 1.10
    assert global_ratio[0]["short_account"] == 0.47
