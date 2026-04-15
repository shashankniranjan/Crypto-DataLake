from __future__ import annotations

"""Process-level singleton WebSocket manager.

Each symbol gets exactly one ``BinanceLiveStreamSupervisor`` (the same
production implementation used by the minute-lake ingestion CLI).  Both the
live-price API service and the minute-lake pipeline resolve to the same
singleton via ``get_ws_manager()``, so there is at most one WS connection per
symbol per OS process.

Capacity and safety
--------------------
``max_subscriptions`` (default 50) caps how many symbols can be subscribed
simultaneously.  Binance allows 300 WS connections per IP; each supervisor
opens 4 streams (depth, forceOrder, aggTrade, markPrice), so 50 symbols ≈ 200
connections — safely under the limit.

When the cap is reached and a *new* symbol is requested, the
least-recently-touched subscription is evicted to make room.  Pre-warmed
symbols (``BML_API_WS_SYMBOLS``) are subscribed on startup and behave like any
other subscription once the service is running.

Typical usage in the API service
---------------------------------
    manager = get_ws_manager(
        ws_base_url=settings.websocket_base_url,
        rest_client=rest_client,
    )
    collector = manager.touch("BTCUSDT") # starts supervisor if not yet running

The minute-lake CLI can call the same ``get_ws_manager()`` and will reuse the
already-running supervisor instead of opening a duplicate stream.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from binance_minute_lake.sources.websocket import (
    BinanceLiveStreamSupervisor,
    InMemoryLiveCollector,
)

LOGGER = logging.getLogger(__name__)

_CLEANUP_INTERVAL_SECONDS = 30

# --------------------------------------------------------------------------- #
# Per-symbol subscription state                                                #
# --------------------------------------------------------------------------- #

@dataclass
class _SubscriptionState:
    collector: InMemoryLiveCollector
    supervisor: BinanceLiveStreamSupervisor
    last_touch: float = field(default_factory=time.monotonic)

    def touch(self) -> None:
        self.last_touch = time.monotonic()

    def is_idle(self, idle_timeout: float) -> bool:
        return time.monotonic() - self.last_touch > idle_timeout

    def stop(self) -> None:
        try:
            self.supervisor.stop()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Manager                                                                      #
# --------------------------------------------------------------------------- #

class SymbolWsManager:
    """Manages on-demand WS subscriptions per symbol via BinanceLiveStreamSupervisor.

    * Thread-safe: all mutations are guarded by ``_lock``.
    * Capacity cap: at most ``max_subscriptions`` active at once.  When the cap
      is reached, the least-recently-touched symbol is evicted to make room for
      the new one.  This prevents unbounded connection growth from random API
      hits.
    * Idle eviction: symbols idle for ``idle_timeout_seconds`` are stopped by a
      background cleanup thread.
    * Singleton: obtain via ``get_ws_manager()`` — do not instantiate directly
      unless you have a good reason to isolate from the global state.
    """

    def __init__(
        self,
        *,
        ws_base_url: str,
        rest_client: Any,
        idle_timeout_seconds: int = 300,
        max_subscriptions: int = 50,
    ) -> None:
        self._ws_base_url = ws_base_url
        self._rest_client = rest_client
        self._idle_timeout = float(idle_timeout_seconds)
        self._max_subscriptions = max(1, max_subscriptions)
        self._subscriptions: dict[str, _SubscriptionState] = {}
        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._cleanup_thread: threading.Thread | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Start the background idle-eviction thread (idempotent)."""
        if self._cleanup_thread is not None and self._cleanup_thread.is_alive():
            return
        self._shutdown.clear()
        t = threading.Thread(
            target=self._cleanup_loop,
            name="ws-manager-cleanup",
            daemon=True,
        )
        t.start()
        self._cleanup_thread = t

    def stop(self) -> None:
        """Stop all supervisors and the cleanup thread."""
        self._shutdown.set()
        with self._lock:
            for state in list(self._subscriptions.values()):
                state.stop()
            self._subscriptions.clear()

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def touch(self, symbol: str) -> InMemoryLiveCollector:
        """Return the collector for *symbol*, starting a supervisor if needed.

        Call this on every API request to keep the subscription alive and
        reset the idle timer.  If the cap is reached, the least-recently-used
        symbol is evicted to make room — so random API hits cannot grow
        connections without bound.
        """
        key = symbol.upper()
        with self._lock:
            state = self._subscriptions.get(key)
            if state is not None:
                state.touch()
                return state.collector

            # Enforce capacity: evict LRU if at the limit.
            if len(self._subscriptions) >= self._max_subscriptions:
                lru_sym = min(self._subscriptions, key=lambda s: self._subscriptions[s].last_touch)
                evicted = self._subscriptions.pop(lru_sym)
                evicted.stop()
                LOGGER.warning(
                    "WS cap reached — evicting LRU symbol",
                    extra={
                        "evicted": lru_sym,
                        "new": key,
                        "cap": self._max_subscriptions,
                    },
                )

            collector = InMemoryLiveCollector(symbol=key)
            supervisor = BinanceLiveStreamSupervisor(
                symbol=key,
                websocket_base_url=self._ws_base_url,
                rest_client=self._rest_client,
                collector=collector,
            )
            supervisor.start()
            state = _SubscriptionState(collector=collector, supervisor=supervisor)
            self._subscriptions[key] = state
            LOGGER.info(
                "WS supervisor started",
                extra={"symbol": key, "active": len(self._subscriptions)},
            )
            return collector

    def get_collector(self, symbol: str) -> InMemoryLiveCollector | None:
        """Return the active collector without starting a subscription."""
        state = self._subscriptions.get(symbol.upper())
        return state.collector if state is not None else None

    def active_symbols(self) -> list[str]:
        with self._lock:
            return list(self._subscriptions.keys())

    # ------------------------------------------------------------------ #
    # Cleanup                                                              #
    # ------------------------------------------------------------------ #

    def _cleanup_loop(self) -> None:
        while not self._shutdown.wait(timeout=_CLEANUP_INTERVAL_SECONDS):
            self._evict_idle()

    def _evict_idle(self) -> None:
        with self._lock:
            idle = [
                sym
                for sym, state in self._subscriptions.items()
                if state.is_idle(self._idle_timeout)
            ]
            for sym in idle:
                state = self._subscriptions.pop(sym)
                state.stop()
                LOGGER.info("WS supervisor evicted (idle)", extra={"symbol": sym})


# --------------------------------------------------------------------------- #
# Process-level singleton                                                      #
# --------------------------------------------------------------------------- #

_INSTANCE: SymbolWsManager | None = None
_INSTANCE_LOCK = threading.Lock()


def get_ws_manager(
    *,
    ws_base_url: str | None = None,
    rest_client: Any | None = None,
    idle_timeout_seconds: int = 300,
    max_subscriptions: int = 50,
) -> SymbolWsManager:
    """Return (or create) the process-level singleton ``SymbolWsManager``.

    The first caller **must** supply ``ws_base_url`` and ``rest_client``.
    Subsequent callers may omit them — the existing instance is returned.

    This ensures that both the live-price API service and the minute-lake
    ingestion pipeline share one manager per process, with at most one WS
    connection per symbol.
    """
    global _INSTANCE
    if _INSTANCE is not None:
        return _INSTANCE
    with _INSTANCE_LOCK:
        if _INSTANCE is not None:
            return _INSTANCE
        if ws_base_url is None or rest_client is None:
            raise RuntimeError(
                "get_ws_manager(): ws_base_url and rest_client are required on first call"
            )
        instance = SymbolWsManager(
            ws_base_url=ws_base_url,
            rest_client=rest_client,
            idle_timeout_seconds=idle_timeout_seconds,
            max_subscriptions=max_subscriptions,
        )
        instance.start()
        _INSTANCE = instance
        LOGGER.info("WS manager singleton created")
        return instance
