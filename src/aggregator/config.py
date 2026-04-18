from __future__ import annotations

from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from .bucketing import parse_timeframe, supported_timeframes


class AggregatorSettings(BaseSettings):
    symbol: str = Field(default="BTCUSDT")
    source_root: Path = Field(default=Path("./data"))
    target_root: Path = Field(default=Path("./data/futures/um/higher_timeframes"))
    state_db: Path = Field(default=Path("./state/aggregator_state.sqlite"))
    timeframes: Annotated[tuple[str, ...], NoDecode] = Field(
        default=("3m", "5m", "10m", "15m", "30m", "45m", "1h", "4h", "8h", "1d", "1w", "1M")
    )
    poll_interval_seconds: int = Field(default=30, ge=1)
    repair_lookback_minutes: int = Field(default=120, ge=1)
    allow_incomplete_buckets: bool = Field(default=False)
    log_level: str = Field(default="INFO")

    model_config = SettingsConfigDict(
        env_prefix="HTF_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        return normalized

    @field_validator("timeframes", mode="before")
    @classmethod
    def _parse_timeframes(cls, value: object) -> tuple[str, ...] | object:
        if isinstance(value, str):
            return tuple(part.strip() for part in value.split(",") if part.strip())
        return value

    @field_validator("timeframes")
    @classmethod
    def _validate_timeframes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        resolved = tuple(parse_timeframe(item).name for item in value)
        if not resolved:
            supported = ", ".join(supported_timeframes())
            raise ValueError(f"At least one timeframe is required. Supported values: {supported}")
        return resolved

    @property
    def minute_lake_root(self) -> Path:
        return self.source_root / "futures" / "um" / "minute"
