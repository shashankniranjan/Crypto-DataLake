from __future__ import annotations

import zipfile
from datetime import UTC, datetime
from pathlib import Path

from binance_minute_lake.sources.vision_loader import VisionLoader


class _FakeVisionClient:
    def build_daily_zip_url(self, stream: str, symbol: str, trade_date: datetime.date, interval: str = "1m") -> str:
        return f"https://example.test/{stream}/{symbol}/{trade_date.isoformat()}/{interval}"

    def expected_filename(self, stream: str, symbol: str, trade_date: datetime.date, interval: str = "1m") -> str:
        if stream == "aggTrades":
            return f"{symbol.upper()}-aggTrades-{trade_date.isoformat()}.zip"
        return f"{symbol.upper()}-{stream}-{trade_date.isoformat()}.zip"

    def exists(self, url: str) -> bool:
        return False

    def download_zip(self, url: str, destination: Path) -> Path:
        raise AssertionError(f"unexpected download attempt for {url} -> {destination}")


class _CountingMissingVisionClient(_FakeVisionClient):
    def __init__(self) -> None:
        self.exists_calls = 0

    def exists(self, url: str) -> bool:
        self.exists_calls += 1
        return False


def _write_zip_csv(path: Path, filename: str, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, content)


def test_vision_loader_parses_current_agg_trades_headers(tmp_path: Path) -> None:
    loader = VisionLoader(_FakeVisionClient(), tmp_path)
    zip_path = tmp_path / "aggTrades" / "KITEUSDT" / "KITEUSDT-aggTrades-2026-04-10.zip"
    _write_zip_csv(
        zip_path,
        "KITEUSDT-aggTrades-2026-04-10.csv",
        "\n".join(
            [
                "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker",
                "1,0.12782,129.0,10,11,1775779201085,false",
                "2,0.12781,82.0,12,12,1775779202091,true",
            ]
        ),
    )

    rows = loader.load_agg_trades(
        "KITEUSDT",
        start=datetime(2026, 4, 10, 0, 0, tzinfo=UTC),
        end=datetime(2026, 4, 10, 0, 1, tzinfo=UTC),
    )

    assert len(rows) == 2
    assert rows[0]["agg_trade_id"] == 1
    assert rows[0]["transact_time"] == 1775779201085
    assert rows[0]["is_buyer_maker"] is False


def test_vision_loader_caches_missing_zip_probe(tmp_path: Path) -> None:
    client = _CountingMissingVisionClient()
    loader = VisionLoader(client, tmp_path, missing_cache_ttl_seconds=3600)

    for _ in range(2):
        rows = loader.load_klines(
            "KITEUSDT",
            start=datetime(2026, 4, 11, 0, 0, tzinfo=UTC),
            end=datetime(2026, 4, 11, 0, 1, tzinfo=UTC),
        )
        assert rows == []

    assert client.exists_calls == 1
    assert (
        tmp_path
        / "klines"
        / "KITEUSDT"
        / "KITEUSDT-klines-2026-04-11.zip.missing"
    ).exists()


def test_vision_loader_exposes_direct_oi_columns_from_metrics(tmp_path: Path) -> None:
    loader = VisionLoader(_FakeVisionClient(), tmp_path)
    zip_path = tmp_path / "metrics" / "KITEUSDT" / "KITEUSDT-metrics-2026-04-10.zip"
    _write_zip_csv(
        zip_path,
        "KITEUSDT-metrics-2026-04-10.csv",
        "\n".join(
            [
                (
                    "create_time,symbol,sum_open_interest,sum_open_interest_value,"
                    "count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,"
                    "count_long_short_ratio,sum_taker_long_short_vol_ratio"
                ),
                "2026-04-10 00:05:00,KITEUSDT,173966503.0,22257274.39382,0.38989272,1.054545,0.33400904,1.67605",
            ]
        ),
    )

    rows = loader.load_metrics(
        "KITEUSDT",
        start=datetime(2026, 4, 10, 0, 0, tzinfo=UTC),
        end=datetime(2026, 4, 10, 0, 10, tzinfo=UTC),
    )

    assert len(rows) == 1
    assert rows[0]["oi_contracts"] == 173966503.0
    assert rows[0]["oi_value_usdt"] == 22257274.39382
