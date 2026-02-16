#!/usr/bin/env bash
set -euo pipefail

# Convenience runner for IntelliJ/DataGrip parquet access through DuckDB.
# Usage:
#   ./scripts/run-intellij.sh [db_path] [symbol] [parquet_root] [poll_seconds]

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  ./scripts/run-intellij.sh [db_path] [symbol] [parquet_root] [poll_seconds]

Args:
  db_path       Optional DuckDB output file. Default: data/minute.duckdb
  symbol        Optional symbol filter. Default: BTCUSDT
  parquet_root  Optional root directory that contains futures/um/minute.
  poll_seconds  Optional daemon poll interval. Default: 60
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

DB_PATH="${1:-data/minute.duckdb}"
SYMBOL="${2:-BTCUSDT}"
PARQUET_ROOT="${3:-}"
POLL_SECONDS="${4:-60}"

echo "==> Preparing IntelliJ dataset"
echo "    db_path: $DB_PATH"
echo "    symbol:  $SYMBOL"
if [[ -n "$PARQUET_ROOT" ]]; then
  echo "    parquet_root: $PARQUET_ROOT"
fi
echo "    poll_seconds: $POLL_SECONDS"

CMD=(python -m binance_minute_lake.cli.app prepare-intellij --db-path "$DB_PATH" --symbol "$SYMBOL")
if [[ -n "$PARQUET_ROOT" ]]; then
  CMD+=(--parquet-root "$PARQUET_ROOT")
fi

PYTHONPATH=src "${CMD[@]}"

echo "==> Starting daemon (Ctrl+C to stop)"
PYTHONPATH=src python -m binance_minute_lake.cli.app run-daemon --poll-seconds "$POLL_SECONDS"
