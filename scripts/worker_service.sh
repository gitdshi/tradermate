#!/usr/bin/env bash
set -euo pipefail

# Start/stop/restart worker (RQ) using project's .venv
# Logs: tradermate/logs/worker.out, PID: tradermate/logs/worker.pid

BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$BASE_DIR"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"
VENV_PY="$BASE_DIR/.venv/bin/python3"
PID_FILE="$LOG_DIR/worker.pid"
OUT_FILE="$LOG_DIR/worker.out"

WORKER_PATTERN="app.worker.service.run_worker"
DEFAULT_QUEUES=(backtest optimization default)

stop() {
  echo "Stopping worker..."
  if [ -f "$PID_FILE" ]; then
    pid=$(cat "$PID_FILE" 2>/dev/null || true)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  pkill -f "$WORKER_PATTERN" || true

  for i in {1..15}; do
    if pgrep -f "$WORKER_PATTERN" >/dev/null; then
      sleep 1
    else
      echo "Worker stopped"
      return 0
    fi
  done
  echo "Warning: Worker did not stop within timeout" >&2
  return 1
}

start() {
  stop || true
  echo "Starting worker..."
  # Load environment variables from .env if it exists
  if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment variables from .env"
  fi
  # allow passing queues: ./worker_service.sh start backtest optimization
  if [ "$#" -gt 1 ]; then
    shift
    QUEUES=("$@")
  else
    QUEUES=("${DEFAULT_QUEUES[@]}")
  fi
  # Start with provided queues
  nohup "$VENV_PY" -u -m app.worker.service.run_worker "${QUEUES[@]}" >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 1
  echo "Worker started (pid $(cat "$PID_FILE")) queues: ${QUEUES[*]}"
}

status() {
  if pgrep -f "$WORKER_PATTERN" >/dev/null; then
    echo "Worker: running"
  else
    echo "Worker: stopped"
  fi
}

case "${1-}" in
  start)
    start "$@"
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
    echo "Usage: $0 {start|stop|restart|status} [queues...]"
    exit 2
    ;;
esac
