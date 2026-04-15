#!/usr/bin/env bash
set -euo pipefail

# Stop the live price/data API service.
# Usage:
#   ./scripts/stop-live-price-service.sh

PORT="${1:-8080}"

echo "Stopping live price service on port $PORT..."

# Find and kill the uvicorn process on the specified port
PID=$(lsof -t -i :$PORT 2>/dev/null || true)

if [ -z "$PID" ]; then
  echo "No service running on port $PORT"
  exit 0
fi

echo "Found process $PID, terminating..."
kill $PID 2>/dev/null || true

# Wait for graceful shutdown
sleep 2

# Check if still running, force kill if necessary
if kill -0 $PID 2>/dev/null; then
  echo "Process still running, force killing..."
  kill -9 $PID 2>/dev/null || true
  sleep 1
fi

echo "Service stopped."
