#!/usr/bin/env bash
set -euo pipefail

# One-command live runner for websocket-backed minute ingestion.
# Usage:
#   ./scripts/run-live.sh [poll_seconds] [event_db]

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  cat <<'EOF'
Usage:
  ./scripts/run-live.sh [poll_seconds] [event_db]

Args:
  poll_seconds  Optional poll interval in seconds. Default: 60
  event_db      Optional SQLite path for raw WS events. Default: state/live_events.sqlite
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

POLL_SECONDS="${1:-60}"
EVENT_DB="${2:-state/live_events.sqlite}"

mkdir -p "$(dirname "$EVENT_DB")"

echo "==> Starting live ingestion daemon (Ctrl+C to stop)"
echo "    poll_seconds: $POLL_SECONDS"
echo "    event_db:     $EVENT_DB"

echo "==> Running one-time live state cleanup before start"
PYTHONPATH=src bml cleanup-live-state --event-db "$EVENT_DB"

PYTHONPATH=src bml run-live-forever --poll-seconds "$POLL_SECONDS" --event-db "$EVENT_DB"
