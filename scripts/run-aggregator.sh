#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  ./scripts/run-aggregator.sh [--foreground] [--once] [--startup-only]

Modes:
  default         Start the aggregator in the background and write a PID file.
  --foreground    Run in the foreground.
  --once          Run startup backfill plus one incremental pass, then exit.
  --startup-only  Run startup backfill only, then exit.

Env:
  VENV_DIR        Optional virtualenv directory. Default: .venv
  PID_FILE        Optional PID file path. Default: state/aggregator.pid
  LOG_FILE        Optional log file path. Default: logs/aggregator.log
  TAIL_LINES      Optional number of log lines to show when attaching. Default: 20
  HTF_*           Aggregator config env vars, e.g. HTF_TIMEFRAMES=5m

Examples:
  ./scripts/run-aggregator.sh
  HTF_TIMEFRAMES=5m ./scripts/run-aggregator.sh --once
  HTF_TIMEFRAMES=1h HTF_SYMBOL=BTCUSDT ./scripts/run-aggregator.sh --foreground
EOF
  exit 0
fi

VENV_DIR="${VENV_DIR:-.venv}"
PYTHON_BIN="$ROOT/$VENV_DIR/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing virtualenv at $VENV_DIR."
  echo "Run ./scripts/setup.sh first."
  exit 1
fi

PID_FILE="${PID_FILE:-state/aggregator.pid}"
LOG_FILE="${LOG_FILE:-logs/aggregator.log}"
TAIL_LINES="${TAIL_LINES:-20}"
mkdir -p "$(dirname "$PID_FILE")" "$(dirname "$LOG_FILE")"

FOREGROUND=0
MODE_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --foreground)
      FOREGROUND=1
      ;;
    --once|--startup-only)
      MODE_ARGS+=("$arg")
      ;;
    *)
      echo "Unknown argument: $arg"
      echo "Run ./scripts/run-aggregator.sh --help"
      exit 1
      ;;
  esac
done

if [[ -f "$PID_FILE" ]]; then
  EXISTING_PID="$(cat "$PID_FILE")"
  if [[ -n "$EXISTING_PID" ]] && kill -0 "$EXISTING_PID" 2>/dev/null; then
    echo "Aggregator already running with PID $EXISTING_PID"
    exit 1
  fi
  rm -f "$PID_FILE"
fi

source "$VENV_DIR/bin/activate"

CMD=("$PYTHON_BIN" -m aggregator.main)
if [[ ${#MODE_ARGS[@]} -gt 0 ]]; then
  CMD+=("${MODE_ARGS[@]}")
fi

if [[ ${#MODE_ARGS[@]} -gt 0 || "$FOREGROUND" -eq 1 ]]; then
  echo "==> Running aggregator in foreground"
  echo "    symbol:      ${HTF_SYMBOL:-BTCUSDT}"
  echo "    timeframes:  ${HTF_TIMEFRAMES:-3m,5m,10m,15m,30m,45m,1h,4h,8h,1d,1w,1M}"
  echo "    target_root: ${HTF_TARGET_ROOT:-./data/futures/um/higher_timeframes}"
  PYTHONPATH=src exec "${CMD[@]}"
fi

echo "==> Starting aggregator in background"
echo "    symbol:      ${HTF_SYMBOL:-BTCUSDT}"
echo "    timeframes:  ${HTF_TIMEFRAMES:-3m,5m,10m,15m,30m,45m,1h,4h,8h,1d,1w,1M}"
echo "    target_root: ${HTF_TARGET_ROOT:-./data/futures/um/higher_timeframes}"
echo "    log_file:    $LOG_FILE"

nohup env PYTHONPATH=src "${CMD[@]}" </dev/null >>"$LOG_FILE" 2>&1 &
PID=$!
disown "$PID" 2>/dev/null || true
echo "$PID" > "$PID_FILE"
sleep 1

if ! kill -0 "$PID" 2>/dev/null; then
  echo "Aggregator failed to start. Check $LOG_FILE"
  rm -f "$PID_FILE"
  exit 1
fi

echo "Aggregator started with PID $PID"
echo "==> Streaming logs (Ctrl+C to detach; aggregator keeps running)"

tail -n "$TAIL_LINES" -f "$LOG_FILE" &
TAIL_PID=$!

cleanup() {
  if kill -0 "$TAIL_PID" 2>/dev/null; then
    kill "$TAIL_PID" 2>/dev/null || true
    wait "$TAIL_PID" 2>/dev/null || true
  fi
}

trap 'cleanup; echo; echo "Log stream detached. Aggregator is still running in the background."; exit 0' INT TERM

while kill -0 "$PID" 2>/dev/null; do
  sleep 1
done

cleanup
echo
echo "Aggregator process $PID exited. Recent logs:"
tail -n "$TAIL_LINES" "$LOG_FILE"
