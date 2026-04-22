from __future__ import annotations

from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .capabilities import CandleFetchMode


class ApiServiceSettings(BaseSettings):
    host: str = Field(default="127.0.0.1")
    port: int = Field(default=8080, ge=1, le=65535)
    default_limit: int = Field(default=200, ge=1)
    max_limit: int = Field(default=500, ge=1)
    on_demand_max_minutes: int = Field(default=60_480, ge=60)
    ws_idle_timeout_seconds: int = Field(default=3600, ge=30)
    ws_max_subscriptions: int = Field(default=50, ge=1)
    # When false, the API service will reuse already-running WS collectors but
    # will not start or pre-warm subscriptions on its own.
    enable_ws_warmup: bool = Field(default=False)
    # Comma-separated symbols to subscribe on startup, e.g. "BTC,ETH,GIGGLE"
    # Each value is normalised to USDT perp format before subscribing.
    ws_symbols: str = Field(default="")
    # Symbol whose continuously running minute-lake WS state should be reused
    # from SQLite rather than requiring API-process warmup.
    shared_live_symbol: str = Field(default="")
    # Optional override for the minute-lake WS event database. When omitted,
    # the app derives it from BML_STATE_DB as <state dir>/live_events.sqlite.
    shared_live_event_db: Path | None = Field(default=None)
    enable_native_binance_tf_candles: bool = Field(default=True)
    candle_fetch_mode: CandleFetchMode = Field(default=CandleFetchMode.NATIVE_PREFERRED)
    allow_legacy_1m_fallback: bool = Field(default=True)
    allow_partial_response_with_notes: bool = Field(default=True)
    include_deprecated_fields: bool = Field(default=False)
    enable_local_symbol_fastpath: bool = Field(default=True)
    local_preferred_symbols: str = Field(default="BTCUSDT")
    local_symbol_require_full_coverage: bool = Field(default=False)
    local_symbol_allow_binance_patch: bool = Field(default=True)
    btc_allow_binance_patch: bool = Field(default=True)
    persist_binance_patches: bool = Field(default=True)
    enable_btc_complexity_guard: bool = Field(default=True)
    btc_local_max_1m_bars: int = Field(default=500, ge=1)
    btc_local_max_3m_bars: int = Field(default=300, ge=1)
    btc_local_max_higher_tf_bars: int = Field(default=200, ge=1)
    btc_force_binance_for_heavy_higher_tf: bool = Field(default=True)

    @field_validator("candle_fetch_mode", mode="before")
    @classmethod
    def _normalize_candle_fetch_mode(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    model_config = SettingsConfigDict(
        env_prefix="BML_API_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )
