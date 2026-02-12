# TraderMate Database Architecture

## Overview

TraderMate uses a two-database architecture to separate concerns:

1. **tushare** - Raw market data from Tushare API (source of truth)
2. **vnpy** - Formatted data for vnpy trading platform (derived data)

## Database Structure

### tushare Database

This database stores raw market data fetched from the Tushare API.

**Main Tables:**
- `stock_basic` - Stock basic information (company, exchange, industry)
- `stock_daily` - Daily OHLCV data (primary market data)
- `adj_factor` - Price adjustment factors
- `daily_basic` - Daily derived indicators (PE, PB, turnover, etc.)
- `stock_moneyflow` - Capital flow data
- `stock_dividend` - Dividend information
- `top10_holders` - Major shareholder data
- `stock_margin` - Margin trading data
- `ingest_audit` - API call audit log
- `sync_log` - Daily sync job log

### vnpy Database

This database stores data in vnpy's expected format for backtesting and trading.

**Main Tables:**
- `dbbardata` - Bar data (OHLCV) in vnpy format
- `dbbaroverview` - Summary of available bar data
- `dbtickdata` - Tick data in vnpy format
- `dbtickoverview` - Summary of available tick data
- `sync_status` - Track sync progress from tushare

## Data Flow

```
Tushare API
    │
    ▼
┌─────────────────┐
│  tushare   │  ← Raw data ingestion
│   (MySQL DB)    │
└────────┬────────┘
         │
         │  Sync daemon / Import script
         ▼
┌─────────────────┐
│   vnpy     │  ← Formatted for vnpy
│   (MySQL DB)    │
└────────┬────────┘
         │
         ▼
    vnpy Platform
    (Backtest/Trade)
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TUSHARE_DATABASE_URL` | `mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8mb4` | Tushare database connection |
| `VNPY_DATABASE_URL` | `mysql+pymysql://root:password@127.0.0.1/vnpy?charset=utf8mb4` | vnpy database connection |
| `TUSHARE_TOKEN` | (required) | Tushare API token |
| `SYNC_HOUR` | `18` | Hour to run daily sync (24h format) |
| `SYNC_MINUTE` | `0` | Minute to run daily sync |
| `BATCH_SIZE` | `100` | Symbols to process per batch |

## Usage

### 1. Initial Data Ingestion

Fetch stock list and historical data from Tushare:

```bash
# Set environment variables
export TUSHARE_TOKEN=your_token_here

# Ingest stock basic info
INGEST_STOCK_BASIC=1 python app/datasync/service/tushare_ingest.py

# Ingest all daily data
INGEST_ALL_DAILY=1 python app/datasync/service/tushare_ingest.py
```

### 2. Sync to vnpy

Transfer data from tushare to vnpy:

```bash
# Sync a single symbol
python app/scripts/import_tushare_to_vnpy.py --symbol 000001.SZ

# Sync all symbols
python app/scripts/import_tushare_to_vnpy.py --all

# Full refresh (re-sync all data)
python app/scripts/import_tushare_to_vnpy.py --all --full-refresh
```

### 3. Daily Sync Daemon

Run automated daily sync using the canonical datasync service and lifecycle script:

```bash
# Use the lifecycle script (recommended)
cd /Users/mac/Workspace/Projects/TraderMate/tradermate
./scripts/datasync_service.sh start|stop|restart|status

# Or run once (for quick manual operations)
.venv/bin/python3 app/datasync/service/data_sync_daemon.py --once

# Run as daemon (manual)
.venv/bin/python3 app/datasync/service/data_sync_daemon.py --daemon

# Sync to vnpy only (no Tushare fetch)
.venv/bin/python3 app/datasync/service/data_sync_daemon.py --sync-vnpy
```

Developer note: after editing datasync modules, restart the datasync service via the lifecycle script to ensure the updated code is loaded.

### 4. vnpy Configuration

vnpy settings file (`~/.vntrader/vt_setting.json`):

```json
{
    "database.name": "mysql",
    "database.database": "vnpy",
    "database.host": "127.0.0.1",
    "database.port": 3306,
    "database.user": "root",
    "database.password": "password"
}
```

## Database Initialization

The init script creates both databases and all tables:

```bash
# Using Docker
docker exec -i tradermate_mysql mysql -uroot -ppassword < mysql/init/init.sql

# Or direct MySQL connection
mysql -uroot -ppassword < mysql/init/init.sql
```

## Migration from Old Structure

If migrating from the old single-database structure:

```sql
-- Migrate daily data
INSERT INTO tushare.stock_daily
SELECT * FROM tradermate.tushare_stock_daily
ON DUPLICATE KEY UPDATE open=VALUES(open);

-- Migrate stock basic
INSERT INTO tushare.stock_basic
SELECT * FROM tradermate.tushare_stock_stock_basic
ON DUPLICATE KEY UPDATE name=VALUES(name);
```

## Maintenance

### Check Data Status

```bash
# Check tushare
docker exec tradermate_mysql mysql -uroot -ppassword -e \
    "SELECT COUNT(*) FROM tushare.stock_daily;"

# Check vnpy
docker exec tradermate_mysql mysql -uroot -ppassword -e \
    "SELECT * FROM vnpy.dbbaroverview;"
```

### Verify Sync

```python
# Run check script
python app/scripts/check_vnpy.py
```
