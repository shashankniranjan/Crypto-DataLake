from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from time import perf_counter

from fastapi import FastAPI, HTTPException, Query, Request, Response

from binance_minute_lake.core.binance_usage import binance_usage_scope
from binance_minute_lake.core.config import Settings
from binance_minute_lake.core.logging import configure_logging
from binance_minute_lake.sources.rest import BinanceRESTClient
from binance_minute_lake.sources.websocket import InMemoryLiveCollector, LiveEventStore
from binance_minute_lake.state.store import SQLiteStateStore
from live_indicators.service import build_indicator_payload

from .capabilities import FetchPlannerConfig
from .config import ApiServiceSettings
from .parallel_provider import ParallelLiveBinanceProvider
from .repository import HigherTimeframeRepository, MinuteLakeRepository
from .service import LiveDataApiService
from .timeframes import normalize_symbol
from .ws_manager import get_ws_manager

LOGGER = logging.getLogger(__name__)


def _configure_app_logging(level: str) -> None:
    if not logging.getLogger().handlers:
        configure_logging(level)
    resolved_level = getattr(logging, level.upper(), logging.INFO)
    logging.getLogger("live_data_api_service").setLevel(resolved_level)
    logging.getLogger("binance_minute_lake").setLevel(resolved_level)


def _default_service() -> LiveDataApiService:
    lake_settings = Settings()
    _configure_app_logging(lake_settings.log_level)
    api_settings = ApiServiceSettings()

    repository = MinuteLakeRepository(lake_settings.root_dir)
    canonical_cache_root = lake_settings.root_dir.parent / ".cache" / "canonical_lake"
    canonical_cache_repository = MinuteLakeRepository(canonical_cache_root)
    canonical_cache_state_store = SQLiteStateStore(canonical_cache_root / "state.sqlite")
    canonical_cache_state_store.initialize()
    higher_timeframe_repository = HigherTimeframeRepository(
        lake_settings.root_dir / "futures" / "um" / "higher_timeframes"
    )
    state_store = SQLiteStateStore(lake_settings.state_db)
    state_store.initialize()
    shared_live_symbol = normalize_symbol(api_settings.shared_live_symbol or lake_settings.symbol)
    shared_live_event_db = (
        api_settings.shared_live_event_db
        if api_settings.shared_live_event_db is not None
        else lake_settings.state_db.with_name("live_events.sqlite")
    )
    shared_live_collector = None
    shared_live_event_db = shared_live_event_db.expanduser().resolve()
    if shared_live_event_db.exists():
        shared_live_collector = InMemoryLiveCollector(
            event_store=LiveEventStore(shared_live_event_db),
            symbol=shared_live_symbol,
        )
        LOGGER.info(
            "Shared minute-lake live store enabled",
            extra={"symbol": shared_live_symbol, "event_db": str(shared_live_event_db)},
        )
    else:
        LOGGER.info(
            "Shared minute-lake live store unavailable",
            extra={"symbol": shared_live_symbol, "event_db": str(shared_live_event_db)},
        )

    # Shared REST client — used by both the provider and the WS manager
    # (the WS supervisor needs it for depth-snapshot resyncs).
    rest_client = BinanceRESTClient(
        base_url=lake_settings.rest_base_url,
        timeout_seconds=lake_settings.rest_timeout_seconds,
        retries=lake_settings.rest_max_retries,
    )

    provider = ParallelLiveBinanceProvider(
        root_dir=lake_settings.root_dir,
        rest_base_url=lake_settings.rest_base_url,
        vision_base_url=lake_settings.vision_base_url,
        max_ffill_minutes=lake_settings.max_ffill_minutes,
        rest_timeout_seconds=lake_settings.rest_timeout_seconds,
        rest_max_retries=lake_settings.rest_max_retries,
    )

    # Resolve the singleton WS manager for this process.
    ws_manager = get_ws_manager(
        ws_base_url=lake_settings.websocket_base_url,
        rest_client=rest_client,
        idle_timeout_seconds=api_settings.ws_idle_timeout_seconds,
        max_subscriptions=api_settings.ws_max_subscriptions,
    )

    return LiveDataApiService(
        repository=repository,
        higher_timeframe_repository=higher_timeframe_repository,
        provider=provider,
        default_limit=api_settings.default_limit,
        max_limit=api_settings.max_limit,
        on_demand_max_minutes=api_settings.on_demand_max_minutes,
        ws_manager=ws_manager,
        shared_live_symbol=shared_live_symbol,
        shared_live_collector=shared_live_collector,
        state_store=state_store,
        local_watermark_tolerance_minutes=lake_settings.safety_lag_minutes,
        include_deprecated_fields=api_settings.include_deprecated_fields,
        enable_ws_warmup=api_settings.enable_ws_warmup,
        enable_local_symbol_fastpath=api_settings.enable_local_symbol_fastpath,
        local_preferred_symbols=api_settings.local_preferred_symbols.split(","),
        local_symbol_require_full_coverage=api_settings.local_symbol_require_full_coverage,
        local_symbol_allow_binance_patch=api_settings.local_symbol_allow_binance_patch,
        btc_allow_binance_patch=api_settings.btc_allow_binance_patch,
        persist_binance_patches=api_settings.persist_binance_patches,
        enable_btc_complexity_guard=api_settings.enable_btc_complexity_guard,
        btc_local_max_1m_bars=api_settings.btc_local_max_1m_bars,
        btc_local_max_3m_bars=api_settings.btc_local_max_3m_bars,
        btc_local_max_higher_tf_bars=api_settings.btc_local_max_higher_tf_bars,
        btc_force_binance_for_heavy_higher_tf=api_settings.btc_force_binance_for_heavy_higher_tf,
        fetch_planner_config=FetchPlannerConfig(
            enable_native_binance_tf_candles=api_settings.enable_native_binance_tf_candles,
            candle_fetch_mode=api_settings.candle_fetch_mode,
            allow_legacy_1m_fallback=api_settings.allow_legacy_1m_fallback,
            allow_partial_response_with_notes=api_settings.allow_partial_response_with_notes,
        ),
        canonical_cache_repository=canonical_cache_repository,
        canonical_cache_state_store=canonical_cache_state_store,
    )


def _elapsed_seconds(started_at: float) -> float:
    return round(max(perf_counter() - started_at, 0.0), 6)


def create_app(service: LiveDataApiService | None = None) -> FastAPI:
    active_service = service or _default_service()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        ws_manager = getattr(active_service, "_ws_manager", None)
        api_settings = ApiServiceSettings()

        # Optional startup pre-warm for symbols listed in BML_API_WS_SYMBOLS.
        # Disabled by default so the live price service does not open WS
        # subscriptions on its own unless explicitly configured to do so.
        if ws_manager is not None and api_settings.enable_ws_warmup:
            raw_symbols = [s.strip() for s in api_settings.ws_symbols.split(",") if s.strip()]
            for raw in raw_symbols:
                try:
                    symbol = normalize_symbol(raw)
                    ws_manager.touch(symbol)
                    LOGGER.info("WS pre-warmed on startup", extra={"symbol": symbol})
                except Exception as exc:
                    LOGGER.warning(
                        "WS pre-warm failed",
                        extra={"symbol": raw, "error": str(exc)},
                    )

        try:
            yield
        finally:
            if ws_manager is not None:
                ws_manager.stop()
            active_service.close()

    app = FastAPI(title="Crypto Live Data API", version="0.1.0", lifespan=lifespan)

    @app.middleware("http")
    async def capture_response_time(request: Request, call_next):
        request.state.started_at = perf_counter()
        if not request.url.path.startswith("/api/"):
            response = await call_next(request)
            if "X-Response-Time-Secs" not in response.headers:
                response.headers["X-Response-Time-Secs"] = f"{_elapsed_seconds(request.state.started_at):.6f}"
            return response

        with binance_usage_scope(request.url.path) as usage_tracker:
            try:
                response = await call_next(request)
            finally:
                LOGGER.info("Binance usage summary", extra=usage_tracker.as_log_fields())
            if "X-Response-Time-Secs" not in response.headers:
                response.headers["X-Response-Time-Secs"] = f"{_elapsed_seconds(request.state.started_at):.6f}"
            return response

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/perpetual-data")
    def perpetual_data(
        request: Request,
        response: Response,
        coin: str = Query(..., description="BTC or BTCUSDT-style symbol"),
        tfs: str = Query(
            ...,
            description="Comma-separated timeframes, e.g. 1m,5m,1hr or per-timeframe limits like 1m=50,5m=25",
        ),
        limit: int | None = Query(
            default=None,
            ge=1,
            description="Fallback bar limit for timeframes in tfs without an inline limit",
        ),
        end_time: str | None = Query(default=None, description="ISO-8601 end timestamp in UTC"),
    ) -> dict[str, object]:
        try:
            payload = active_service.fetch_perpetual_data(
                coin=coin,
                tfs=tfs,
                limit=limit,
                end_time=end_time,
            )
            elapsed = _elapsed_seconds(request.state.started_at)
            response.headers["X-Response-Time-Secs"] = f"{elapsed:.6f}"
            payload["response_time_secs"] = elapsed
            return payload
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.get("/api/v1/live-indicators")
    def live_indicators(
        request: Request,
        response: Response,
        coin: str = Query(..., description="BTC or BTCUSDT-style symbol"),
        ema_tf: str = Query(..., description="EMA timeframe, e.g. 5m,15m,1hr,1d,1w,1M"),
        ema_length: int = Query(..., ge=1, description="TradingView-style EMA length"),
        pivot_tf: str = Query(..., description="Traditional pivot timeframe, e.g. 1hr,4hr,1d,1w,1M"),
        end_time: str | None = Query(default=None, description="ISO-8601 end timestamp in UTC"),
    ) -> dict[str, object]:
        try:
            payload = build_indicator_payload(
                service=active_service,
                coin=coin,
                ema_tf=ema_tf,
                ema_length=ema_length,
                pivot_tf=pivot_tf,
                end_time=end_time,
            )
            elapsed = _elapsed_seconds(request.state.started_at)
            response.headers["X-Response-Time-Secs"] = f"{elapsed:.6f}"
            payload["response_time_secs"] = elapsed
            return payload
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return app


def main() -> None:
    import uvicorn

    settings = ApiServiceSettings()
    uvicorn.run(
        "live_data_api_service.app:create_app",
        host=settings.host,
        port=settings.port,
        factory=True,
    )
