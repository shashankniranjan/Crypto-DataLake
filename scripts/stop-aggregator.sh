#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PID_FILE="${PID_FILE:-state/aggregator.pid}"
LOG_FILE="${LOG_FILE:-logs/aggregator.log}"
TAIL_LINES="${TAIL_LINES:-20}"

print_recent_logs() {
  if [[ -f "$LOG_FILE" ]]; then
    echo "Recent logs from $LOG_FILE:"
    tail -n "$TAIL_LINES" "$LOG_FILE"
  else
    echo "No log file found at $LOG_FILE"
  fi
}

if [[ ! -f "$PID_FILE" ]]; then
  echo "No aggregator PID file found at $PID_FILE"
  print_recent_logs
  exit 0
fi

PID="$(cat "$PID_FILE")"
if [[ -z "$PID" ]]; then
  echo "PID file is empty; removing it."
  rm -f "$PID_FILE"
  print_recent_logs
  exit 0
fi

if ! kill -0 "$PID" 2>/dev/null; then
  echo "Aggregator process $PID is not running; removing stale PID file."
  rm -f "$PID_FILE"
  print_recent_logs
  exit 0
fi

echo "Stopping aggregator PID $PID..."
kill "$PID" 2>/dev/null || true

for _ in {1..10}; do
  if ! kill -0 "$PID" 2>/dev/null; then
    rm -f "$PID_FILE"
    echo "Aggregator stopped."
    print_recent_logs
    exit 0
  fi
  sleep 1
done

echo "Aggregator still running, force killing PID $PID..."
kill -9 "$PID" 2>/dev/null || true
rm -f "$PID_FILE"
echo "Aggregator stopped."
print_recent_logs
