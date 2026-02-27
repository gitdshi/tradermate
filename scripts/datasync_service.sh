#!/usr/bin/env bash
set -euo pipefail

# Start/stop/restart data sync daemon using project's .venv
# Logs: tradermate/logs/data_sync.out, PID: tradermate/logs/data_sync.pid

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
VENV_PY="$BASE_DIR/.venv/bin/python3"
PID_FILE="$LOG_DIR/data_sync.pid"
OUT_FILE="$LOG_DIR/data_sync.out"

DAEMON_PATTERN="app.datasync.service.data_sync_daemon"

stop() {
  echo "Stopping DataSync..."
  if [ -f "$PID_FILE" ]; then
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  pkill -f "$DAEMON_PATTERN" || true

  for i in {1..15}; do
    if pgrep -f "$DAEMON_PATTERN" >/dev/null; then
      sleep 1
    else
      echo "DataSync stopped"
      return 0
    fi
  done
  echo "Warning: DataSync did not stop within timeout" >&2
  return 1
}

start() {
  stop || true
  echo "Starting DataSync daemon..."
  # Load environment variables from .env if it exists
  if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment variables from .env"
  fi
  nohup "$VENV_PY" -u -m app.datasync.service.data_sync_daemon --daemon >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 1
  echo "DataSync started (pid $(cat "$PID_FILE"))"
}

status() {
  if pgrep -f "$DAEMON_PATTERN" >/dev/null; then
    echo "DataSync: running"
  else
    echo "DataSync: stopped"
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
