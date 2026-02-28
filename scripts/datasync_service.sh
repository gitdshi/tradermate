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

load_env() {
  if [ -f ".env" ]; then
    set -a
    source .env
    set +a
    echo "Loaded environment variables from .env"
  fi
}

ensure_db_host_reachable() {
  local host="${MYSQL_HOST:-}"
  if [ -z "$host" ]; then
    return 0
  fi

  if [ "$host" != "mysql" ]; then
    return 0
  fi

  if "$VENV_PY" - <<'PY' >/dev/null 2>&1
import os
import pymysql

host = os.getenv('MYSQL_HOST', 'mysql')
port = int(os.getenv('MYSQL_PORT', '3306'))
user = os.getenv('MYSQL_USER', 'root')
password = os.getenv('MYSQL_PASSWORD', '')

conn = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    connect_timeout=3,
    read_timeout=3,
    write_timeout=3,
)
with conn.cursor() as cur:
    cur.execute('SELECT 1')
conn.close()
PY
  then
    return 0
  fi

  echo "Warning: MYSQL_HOST=mysql is not reachable from this shell; falling back to 127.0.0.1 for this run"
  export MYSQL_HOST=127.0.0.1

  if [ -n "${TUSHARE_DATABASE_URL:-}" ]; then
    export TUSHARE_DATABASE_URL="${TUSHARE_DATABASE_URL/@mysql:/@127.0.0.1:}"
  fi
  if [ -n "${AKSHARE_DATABASE_URL:-}" ]; then
    export AKSHARE_DATABASE_URL="${AKSHARE_DATABASE_URL/@mysql:/@127.0.0.1:}"
  fi
  if [ -n "${VNPY_DATABASE_URL:-}" ]; then
    export VNPY_DATABASE_URL="${VNPY_DATABASE_URL/@mysql:/@127.0.0.1:}"
  fi
  if [ -n "${TRADERMATE_DATABASE_URL:-}" ]; then
    export TRADERMATE_DATABASE_URL="${TRADERMATE_DATABASE_URL/@mysql:/@127.0.0.1:}"
  fi

  if ! "$VENV_PY" - <<'PY' >/dev/null 2>&1
import os
import pymysql

host = os.getenv('MYSQL_HOST', '127.0.0.1')
port = int(os.getenv('MYSQL_PORT', '3306'))
user = os.getenv('MYSQL_USER', 'root')
password = os.getenv('MYSQL_PASSWORD', '')

conn = pymysql.connect(
    host=host,
    port=port,
    user=user,
    password=password,
    connect_timeout=3,
    read_timeout=3,
    write_timeout=3,
)
with conn.cursor() as cur:
    cur.execute('SELECT 1')
conn.close()
PY
  then
    echo "Error: MySQL still not reachable with MYSQL_HOST=127.0.0.1. Check MYSQL_PORT/MYSQL_USER/MYSQL_PASSWORD in .env" >&2
    return 1
  fi
}

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
  load_env
  ensure_db_host_reachable
  nohup "$VENV_PY" -u -m app.datasync.service.data_sync_daemon --daemon >>"$OUT_FILE" 2>&1 &
  echo $! > "$PID_FILE"
  sleep 1
  echo "DataSync started (pid $(cat "$PID_FILE"))"
}

init_data() {
  stop || true
  load_env
  ensure_db_host_reachable

  local start_date="${INIT_START_DATE:-2005-01-01}"
  local lookback_years="${INIT_LOOKBACK_YEARS:-21}"
  local lookback_days="${INIT_LOOKBACK_DAYS:-8000}"

  local init_args=("--start-date" "$start_date")
  if [ "${INIT_SKIP_AUX:-0}" = "1" ]; then
    init_args+=("--skip-aux")
  fi
  if [ "${INIT_SKIP_VNPY:-0}" = "1" ]; then
    init_args+=("--skip-vnpy")
  fi
  if [ "${INIT_SKIP_SCHEMA:-0}" = "1" ]; then
    init_args+=("--skip-schema")
  fi

  echo "Starting DataSync initialization sequence..."
  echo "  start_date=${start_date} lookback_years=${lookback_years} lookback_days=${lookback_days}"
  echo "  logs: $OUT_FILE"

  echo "[1/3] Initialize market data (tushare/akshare/vnpy)..."
  PYTHONPATH=. "$VENV_PY" -u scripts/init_market_data.py "${init_args[@]}" >>"$OUT_FILE" 2>&1

  echo "[2/3] Initialize sync status table..."
  PYTHONPATH=. "$VENV_PY" -u -m app.datasync.service.data_sync_daemon --init --lookback-years "$lookback_years" >>"$OUT_FILE" 2>&1

  echo "[3/3] Run historical backfill pass..."
  PYTHONPATH=. "$VENV_PY" -u -m app.datasync.service.data_sync_daemon --backfill --lookback-days "$lookback_days" >>"$OUT_FILE" 2>&1

  echo "Initialization sequence complete"
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
  init)
    init_data
    ;;
  unlock)
    echo "Releasing backfill_lock via DAO..."
    "$VENV_PY" - <<'PY'
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.abspath('.'))
from app.domains.extdata.dao import data_sync_status_dao as dao
from sqlalchemy import text
try:
    with dao.engine_tm.connect() as conn:
        row = conn.execute(text('SELECT id,is_locked,locked_at,locked_by FROM backfill_lock WHERE id = 1')).fetchone()
        print('BEFORE:', row)
except Exception as e:
    print('ERR reading before:', e)
try:
    dao.release_backfill_lock()
    print('Called release_backfill_lock()')
except Exception as e:
    print('ERR releasing:', e)
try:
    with dao.engine_tm.connect() as conn:
        row = conn.execute(text('SELECT id,is_locked,locked_at,locked_by FROM backfill_lock WHERE id = 1')).fetchone()
        print('AFTER:', row)
except Exception as e:
    print('ERR reading after:', e)
PY
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|init|unlock}"
    echo ""
    echo "Init options via env vars:"
    echo "  INIT_START_DATE=2005-01-01   # historical start date"
    echo "  INIT_LOOKBACK_YEARS=21       # for --init"
    echo "  INIT_LOOKBACK_DAYS=8000      # for --backfill"
    echo "  INIT_SKIP_AUX=1              # optional: skip adj/dividend/top10"
    echo "  INIT_SKIP_VNPY=1             # optional: skip vnpy sync"
    echo "  INIT_SKIP_SCHEMA=1           # optional: skip schema init"
    exit 2
    ;;
esac
