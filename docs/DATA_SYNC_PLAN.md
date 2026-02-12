# Data Sync Daemon - Implementation Plan

## Overview
The data sync daemon runs daily at 2:00 AM to synchronize market data from remote APIs (Tushare and AkShare) to local databases, with comprehensive error tracking, retry logic, and backfill capabilities.

## Architecture

### Three-Tier Database Structure
1. **akshare DB**: Raw market data from AkShare API (free, no token required)
2. **tushare DB**: Raw market data from Tushare API (requires token and permissions)
3. **vnpy DB**: Formatted data for VNPy trading platform (derived from tushare)

### Sync Status Tracking
- **Table**: `tushare.sync_log` (centralized audit log)
- **Fields**:
  - `sync_date`: Trading date being synced
  - `endpoint`: Sync task identifier
  - `status`: 'success', 'partial', 'error', 'pending'
  - `rows_synced`: Number of rows successfully synced
  - `error_message`: Error details if failed
  - `started_at`, `finished_at`: Timestamps

## Three Main Sync Tasks

### Task 1: AkShare Index Daily (`akshare_index_daily`)
- **Source**: AkShare API
- **Target**: `akshare.index_daily`
- **Data**: Index daily OHLCV (HS300, SSE50, CSI500, etc.)
- **API**: `ak.stock_zh_index_daily(symbol='sh000300')`
- **Symbols**: Defined in `INDEX_MAPPING` (sh000300, sh000001, sh000016, sh000905, etc.)

### Task 2: Tushare Daily Data (`tushare_daily`)
- **Source**: Tushare API
- **Target**: `tushare.*` tables
- **Tables**:
  1. `stock_basic` - Stock metadata (code, name, industry, listing date)
  2. `stock_daily` - Daily OHLCV
  3. `stock_dividend` - Dividends, splits, bonuses
  4. `adj_factor` - Adjustment factors
  5. `top10_holders` - Top 10 shareholders
- **API Endpoints**:
  - `pro.stock_basic()`
  - `pro.daily(ts_code=X, trade_date=Y)`
  - `pro.dividend(ts_code=X)`
  - `pro.adj_factor(ts_code=X, trade_date=Y)`
  - `pro.top10_holders(ts_code=X)`

### Task 3: VNPy Sync (`vnpy_sync`)
- **Source**: `tushare.stock_daily`
- **Target**: `vnpy.dbbardata`, `vnpy.dbbaroverview`
- **Process**:
  1. Read from `tushare.stock_daily`
  2. Convert format (ts_code → symbol+exchange, trade_date → datetime)
  3. Upsert to `vnpy.dbbardata`
  4. Update `vnpy.dbbaroverview` statistics

## Sync Flow

### Daily Automatic Sync (2:00 AM)
```
1. Determine yesterday's trade date (skip if not trading day)
2. Check sync_log for status
3. Execute tasks in order:
   a. akshare_index_daily
   b. tushare_daily
   c. vnpy_sync (only if tushare_daily succeeds)
4. Log status for each task
```

### Startup Backfill
```
1. Scan sync_log for last 60 days
2. Identify failed or missing dates
3. Retry failed dates with exponential backoff
4. Backfill missing dates in chronological order
```

### Retry Logic
- **Max Retries**: 3 per task
- **Backoff**: 10 * attempt seconds
- **Status Progression**:
  - Attempt 1 fail → status='error', schedule retry
  - Attempt 2 fail → status='error', longer backoff
  - Attempt 3 fail → status='error', manual intervention required

## Implementation Components

### 1. Enhanced Sync Status Methods
```python
def check_sync_status(sync_date: date) -> Dict[str, str]:
    """Check status of all endpoints for a given date."""
    
def mark_sync_pending(sync_date: date, endpoint: str):
    """Mark sync task as pending before starting."""
    
def is_sync_complete(sync_date: date) -> bool:
    """Check if all required endpoints succeeded for a date."""
```

### 2. Backfill Methods
```python
def backfill_missing_dates(lookback_days: int = 60):
    """Scan and backfill any missing trade dates."""
    
def retry_failed_syncs(max_age_days: int = 7):
    """Retry recently failed sync tasks."""
```

### 3. Enhanced Task Runners
Each task runner returns `(status, rows_synced, error_message)`:
- `run_akshare_index_daily_for_date(sync_date)` ✓ (exists)
- `run_tushare_daily_for_date(sync_date)` ✓ (exists, needs enhancement)
- `run_vnpy_sync_for_date(sync_date)` (new, date-specific version)

### 4. Tushare Task Enhancement
Add support for additional tables:
```python
def run_tushare_daily_for_date(sync_date: date):
    """Enhanced to sync all required tables."""
    # 1. stock_basic (if empty or weekly refresh)
    # 2. stock_daily (all stocks for date)
    # 3. stock_dividend (if earnings season)
    # 4. adj_factor (all stocks for date)
    # 5. top10_holders (quarterly data)
```

### 5. VNPy Sync Enhancement
Make date-specific:
```python
def run_vnpy_sync_for_date(sync_date: date):
    """Sync specific date from tushare to vnpy."""
    # Read tushare.stock_daily WHERE trade_date = sync_date
    # Convert and upsert to vnpy.dbbardata
    # Update vnpy.dbbaroverview
```

## Configuration

### Environment Variables
```bash
SYNC_HOUR=2                    # Hour to run daily sync (24h format)
SYNC_MINUTE=0                  # Minute to run daily sync
BACKFILL_DAYS=60              # How many days back to check for gaps
MAX_RETRIES=3                 # Max retry attempts per task
RETRY_BACKOFF_BASE=10         # Base seconds for exponential backoff
TUSHARE_CALLS_PER_MIN=50      # Rate limit for Tushare API
AKSHARE_CALLS_PER_MIN=30      # Rate limit for AkShare API
```

### Required Endpoints List
```python
REQUIRED_ENDPOINTS = [
    'akshare_index_daily',     # AkShare index data
    'tushare_daily',           # Tushare stock data
    'vnpy_sync'                # VNPy database sync
]
```

## Monitoring & Alerts

### Health Checks
```sql
-- Check recent sync status
SELECT sync_date, endpoint, status, rows_synced, error_message
FROM sync_log
WHERE sync_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
ORDER BY sync_date DESC, endpoint;

-- Find failed syncs
SELECT sync_date, endpoint, error_message
FROM sync_log
WHERE status = 'error' AND sync_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY);

-- Check data freshness
SELECT MAX(trade_date) as latest_date, COUNT(*) as stock_count
FROM stock_daily
WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY trade_date
ORDER BY trade_date DESC LIMIT 10;
```

### Logging
- INFO: Normal operations, task completion
- WARNING: Partial success, retries
- ERROR: Task failures, permission issues
- CRITICAL: Daemon crash, database connection loss

## Deployment

### As Daemon
```bash
cd /Users/mac/Workspace/Projects/TraderMate/tradermate
.venv/bin/python3 app/services/data_sync_daemon.py --daemon
```

### Manual Run (One-time)
```bash
# Sync yesterday's data
.venv/bin/python3 app/services/data_sync_daemon.py --once

# Backfill specific date
.venv/bin/python3 app/services/data_sync_daemon.py --backfill 2026-02-01

# Retry failed syncs
.venv/bin/python3 app/services/data_sync_daemon.py --retry-failed
```

### As SystemD Service (Production)
```ini
[Unit]
Description=TraderMate Data Sync Daemon
After=network.target mysql.service

[Service]
Type=simple
User=trader
WorkingDirectory=/opt/tradermate
Environment="PYTHONPATH=/opt/tradermate"
ExecStart=/opt/tradermate/.venv/bin/python3 app/services/data_sync_daemon.py --daemon
Restart=always
RestartSec=30

[Install]
WantedBy=multi-user.target
```

## Testing Plan

### Unit Tests
1. Test trade calendar functions
2. Test sync status tracking
3. Test retry logic with mock failures
4. Test backfill date calculation

### Integration Tests
1. Test full sync cycle for one date
2. Test failure recovery
3. Test database constraint violations
4. Test rate limiting

### Manual Verification
```bash
# 1. Check audit logs
mysql -u root -p tushare -e "SELECT * FROM sync_log ORDER BY sync_date DESC LIMIT 20;"

# 2. Verify data counts
mysql -u root -p tushare -e "SELECT COUNT(*) FROM stock_daily WHERE trade_date = '2026-02-09';"

# 3. Check vnpy sync
mysql -u root -p vnpy -e "SELECT COUNT(*) FROM dbbardata WHERE DATE(datetime) = '2026-02-09';"
```

## Future Enhancements

1. **Parallel Processing**: Use thread pool for multiple stock codes
2. **Incremental Updates**: Track last sync timestamp for efficient updates
3. **Data Validation**: Add checksums and row count verification
4. **Alert System**: Email/SMS notifications for persistent failures
5. **Dashboard**: Web UI for monitoring sync status
6. **Compression**: Archive old data to reduce database size
7. **Multi-Region**: Support for Hong Kong and US market data

## Maintenance

### Weekly
- Review error logs
- Check disk space
- Verify data completeness

### Monthly
- Archive old audit logs (keep 6 months)
- Update stock_basic metadata
- Refresh index constituent lists

### Quarterly
- Backup databases
- Review API usage and costs
- Update documentation

## Operational Notes (moved code & service scripts)

- Implementation locations: canonical datasync code now lives under `app/datasync/service/` (examples: `app/datasync/service/data_sync_daemon.py`, `app/datasync/service/tushare_ingest.py`, `app/datasync/service/akshare_ingest.py`). Package-root `app/datasync/main.py` is a minimal entrypoint.
- Runtime: use the prepared lifecycle script to run the daemon and ensure the virtualenv, logging, and PID handling are correct:

```bash
cd /Users/mac/Workspace/Projects/TraderMate/tradermate
./scripts/datasync_service.sh start|stop|restart|status
```

- Developer note: after editing any datasync module, restart the daemon using the lifecycle script to ensure the process loads the updated code (no auto-reload for the daemon).
