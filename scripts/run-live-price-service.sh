#!/usr/bin/env bash
set -euo pipefail

# One-command runner for the live price/data API service.
# Usage:
#   ./scripts/run-live-price-service.sh [host] [port] [root_dir]

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  ./scripts/run-live-price-service.sh [host] [port] [root_dir]

Args:
  host      Optional bind host. Default: 127.0.0.1
  port      Optional bind port. Default: 8080
  root_dir  Optional data root containing futures/um/minute. Default: data

Env:
  VENV_DIR                       Optional virtualenv directory. Default: .venv
  BML_API_DEFAULT_LIMIT          Default response bar limit
  BML_API_MAX_LIMIT              Maximum allowed response bar limit
  BML_API_ON_DEMAND_MAX_MINUTES  Max minute span allowed for Binance fallback
  BML_API_WS_SYMBOLS             Comma-separated symbols to subscribe on startup
                                 e.g. "BTC,ETH,GIGGLE" (USDT perp implied)
                                 Subscriptions start immediately; no need to
                                 wait for the first HTTP request.
  BML_API_WS_IDLE_TIMEOUT_SECONDS  Seconds of inactivity before a WS
                                 subscription is stopped. Default: 300
  BML_API_WS_MAX_SUBSCRIPTIONS   Max simultaneous WS subscriptions.
                                 Oldest-unused symbol is evicted when full.
                                 Default: 50 (≈200 streams, under Binance's
                                 300-connection-per-IP limit)

Examples:
  ./scripts/run-live-price-service.sh
  ./scripts/run-live-price-service.sh 0.0.0.0 8081
  ./scripts/run-live-price-service.sh 127.0.0.1 8080 /absolute/path/to/data
EOF
  exit 0
fi

VENV_DIR="${VENV_DIR:-.venv}"
if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  echo "Missing virtualenv at $VENV_DIR."
  echo "Run ./scripts/setup.sh first."
  exit 1
fi

source "$VENV_DIR/bin/activate"

HOST="${1:-127.0.0.1}"
PORT="${2:-8080}"
ROOT_DIR_ARG="${3:-data}"

export BML_API_HOST="$HOST"
export BML_API_PORT="$PORT"
export BML_ROOT_DIR="$ROOT_DIR_ARG"

echo "==> Starting live price service (Ctrl+C to stop)"
echo "    host:     $BML_API_HOST"
echo "    port:     $BML_API_PORT"
echo "    root_dir: $BML_ROOT_DIR"
echo
echo "    health:   http://$BML_API_HOST:$BML_API_PORT/healthz"
echo "    data:     http://$BML_API_HOST:$BML_API_PORT/api/v1/perpetual-data?coin=BTC&tfs=1m,5m&limit=5"

PYTHONPATH=src exec python -m uvicorn live_data_api_service.app:create_app --factory --host "$BML_API_HOST" --port "$BML_API_PORT"
