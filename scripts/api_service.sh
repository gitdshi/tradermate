#!/usr/bin/env bash
set -euo pipefail

# Starts, stops, restarts the API service (uvicorn) using the project's .venv
# Logs and PID are stored under tradermate/logs

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
VENV_PY="$BASE_DIR/.venv/bin/python3"
PID_FILE="$LOG_DIR/api.pid"
OUT_FILE="$LOG_DIR/api.out"

stop() {
  echo "Stopping API..."
  if [ -f "$PID_FILE" ]; then
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  pkill -f "uvicorn.*app.api.main" || true

  # wait for process to exit
  for i in {1..15}; do
    if pgrep -f "uvicorn.*app.api.main" >/dev/null; then
      sleep 1
    else
      echo "API stopped"
      return 0
    fi
  done
  echo "Warning: API did not stop within timeout" >&2
  return 1
}

start() {
  stop || true
  echo "Starting API..."
  # Load environment variables from .env if it exists
  if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment variables from .env"
  fi
  nohup "$VENV_PY" -m uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 1
  echo "API started (pid $(cat "$PID_FILE"))"
}

status() {
  if pgrep -f "uvicorn.*app.api.main" >/dev/null; then
    echo "API: running"
  else
    echo "API: stopped"
  fi
}

case "${1-}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop && start
    ;;
  status)
    status
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 2
    ;;
esac
