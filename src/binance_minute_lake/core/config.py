from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    symbol: str = Field(default="BTCUSDT")

    root_dir: Path = Field(default=Path("./data"))
    state_db: Path = Field(default=Path("./state/ingestion_state.sqlite"))

    vision_base_url: str = Field(default="https://data.binance.vision/data/futures/um/daily")
    rest_base_url: str = Field(default="https://fapi.binance.com")
    websocket_base_url: str = Field(default="wss://fstream.binance.com/ws")

    safety_lag_minutes: int = Field(default=3, ge=1)
    max_ffill_minutes: int = Field(default=60, ge=1)
    bootstrap_lookback_minutes: int = Field(default=120, ge=1)

    rest_timeout_seconds: int = Field(default=20, ge=1)
    rest_max_retries: int = Field(default=5, ge=1)
    rest_concurrency: int = Field(default=4, ge=1)
    live_event_retention_hours: int = Field(default=72, ge=1)
    live_heartbeat_retention_days: int = Field(default=14, ge=1)
    live_cleanup_interval_minutes: int = Field(default=30, ge=1)
    live_cleanup_vacuum_interval_hours: int = Field(default=24, ge=1)

    log_level: str = Field(default="INFO")

    model_config = SettingsConfigDict(
        env_prefix="BML_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
