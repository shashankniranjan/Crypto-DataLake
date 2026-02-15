from pathlib import Path

from binance_minute_lake.cli.app import _duckdb_parquet_pattern, _intellij_queries_sql


def test_duckdb_parquet_pattern_is_absolute_and_symbol_scoped(tmp_path: Path) -> None:
    pattern = _duckdb_parquet_pattern(tmp_path, "btcusdt")

    assert pattern.startswith(str(tmp_path.resolve()))
    assert "symbol=BTCUSDT" in pattern
    assert pattern.endswith("part.parquet")


def test_intellij_queries_sql_contains_symbol_filter() -> None:
    sql = _intellij_queries_sql("ethusdt")
    assert "symbol = 'ETHUSDT'" in sql
    assert "FROM minute" in sql
