from binance_minute_lake.core.enums import SupportClass
from binance_minute_lake.core.schema import canonical_columns, hard_required_columns


def test_schema_has_66_columns() -> None:
    columns = canonical_columns()
    assert len(columns) == 66
    assert len({column.name for column in columns}) == 66


def test_schema_contains_required_support_classes() -> None:
    support_classes = {column.support_class for column in canonical_columns()}
    assert SupportClass.HARD_REQUIRED in support_classes
    assert SupportClass.BACKFILL_AVAILABLE in support_classes
    assert SupportClass.LIVE_ONLY in support_classes


def test_hard_required_columns_include_prices() -> None:
    required = set(hard_required_columns())
    for name in [
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "mark_price_open",
        "mark_price_close",
        "index_price_open",
        "index_price_close",
    ]:
        assert name in required
