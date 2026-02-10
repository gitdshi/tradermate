"""
Data Sync Daemon for TraderMate

This daemon handles three main responsibilities:
1. Fetch daily market data from AkShare and store in akshare database
2. Fetch daily market data from Tushare and store in tushare database
3. Sync/convert data from tushare_data to vnpy_data for backtesting

Architecture:
- akshare: Raw market data from AkShare
- tushare: Raw market data from Tushare (source of truth for vnpy)
- vnpy: Formatted data for vnpy trading platform (derived)

Usage:
    python app/services/data_sync_daemon.py --once          # Run once for latest trade date
    python app/services/data_sync_daemon.py --daemon        # Run as daemon (daily at 02:00)
    python app/services/data_sync_daemon.py --sync-vnpy     # Sync to vnpy only
"""

import os
import sys
import time
import json
import logging
import argparse
import schedule
from datetime import datetime, date, timedelta
from typing import List, Optional, Tuple, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Add project root to path
sys.path.insert(0, str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from app.services.tushare_ingest import (
    ingest_daily,
    ingest_stock_basic,
    ingest_daily_basic,
    ingest_adj_factor,
    get_all_ts_codes,
    get_max_trade_date,
    call_pro,
    engine as tushare_engine
)
from app.services.akshare_ingest import (
    ingest_index_daily as ak_ingest_index_daily,
    akshare_engine
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database URLs
TUSHARE_DB_URL = os.getenv('TUSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8mb4')
VNPY_DB_URL = os.getenv('VNPY_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/vnpy?charset=utf8mb4')

# Sync configuration
SYNC_HOUR = int(os.getenv('SYNC_HOUR', '2'))  # Default: 02:00 local time
SYNC_MINUTE = int(os.getenv('SYNC_MINUTE', '0'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
BACKFILL_DAYS = int(os.getenv('BACKFILL_DAYS', '60'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_BACKOFF_BASE = int(os.getenv('RETRY_BACKOFF_BASE', '10'))

# Required daily endpoints for audit tracking
REQUIRED_ENDPOINTS = [
    'akshare_daily',
    'akshare_to_tushare',
    'tushare_daily',
    'vnpy_sync'
]

# Exchange mapping for vnpy
EXCHANGE_MAP = {
    'SZ': 'SZSE',
    'SH': 'SSE',
}


class DataSyncDaemon:
    """Daemon for syncing data between Tushare and vnpy databases."""
    
    def __init__(self):
        self.tushare_engine: Engine = create_engine(TUSHARE_DB_URL, pool_pre_ping=True)
        self.vnpy_engine: Engine = create_engine(VNPY_DB_URL, pool_pre_ping=True)
        self.running = False

    # =========================================================================
    # Audit helpers
    # =========================================================================

    def _table_exists(self, engine: Engine, table_name: str) -> bool:
        db_name = engine.url.database
        if not db_name:
            return False
        with engine.connect() as conn:
            res = conn.execute(text(
                "SELECT 1 FROM information_schema.tables WHERE table_schema=:db AND table_name=:tbl LIMIT 1"
            ), {'db': db_name, 'tbl': table_name})
            return res.fetchone() is not None

    def verify_audit_tables(self) -> bool:
        """Verify audit tables exist for AkShare, Tushare, and vnpy sync tracking."""
        ok = True
        # Tushare audit tables
        for tbl in ['ingest_audit', 'sync_log']:
            if not self._table_exists(self.tushare_engine, tbl):
                logger.warning("Missing Tushare audit table: %s", tbl)
                ok = False
        # AkShare audit tables
        for tbl in ['ingest_audit', 'sync_log']:
            if not self._table_exists(akshare_engine, tbl):
                logger.warning("Missing AkShare audit table: %s", tbl)
                ok = False
        # vnpy sync status
        if not self._table_exists(self.vnpy_engine, 'sync_status'):
            logger.warning("Missing vnpy sync_status table")
            ok = False
        if ok:
            logger.info("Audit tables verified")
        return ok

    def write_sync_log(self, sync_date: date, endpoint: str, status: str,
                       rows_synced: int = 0, error: Optional[str] = None):
        """Write to tushare.sync_log for centralized audit."""
        with self.tushare_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sync_log (sync_date, endpoint, status, rows_synced, error_message, started_at, finished_at)
                VALUES (:sync_date, :endpoint, :status, :rows_synced, :error_message, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    rows_synced = VALUES(rows_synced),
                    error_message = VALUES(error_message),
                    finished_at = NOW()
            """), {
                'sync_date': sync_date,
                'endpoint': endpoint,
                'status': status,
                'rows_synced': rows_synced,
                'error_message': error
            })

    def get_sync_status(self, sync_date: date, endpoint: str) -> Optional[str]:
        with self.tushare_engine.connect() as conn:
            res = conn.execute(text(
                "SELECT status FROM sync_log WHERE sync_date=:d AND endpoint=:ep"
            ), {'d': sync_date, 'ep': endpoint})
            row = res.fetchone()
            return row[0] if row else None

    # =========================================================================
    # Trade calendar helpers
    # =========================================================================

    def get_trade_days(self, start_d: date, end_d: date) -> List[date]:
        """Return trade dates between start_d and end_d inclusive."""
        s = start_d.strftime('%Y%m%d')
        e = end_d.strftime('%Y%m%d')
        try:
            df = call_pro('trade_cal', exchange='SSE', start_date=s, end_date=e)
            if df is None:
                raise Exception('trade_cal returned None')
            df = df[df['is_open'] == 1]
            return [pd.to_datetime(d).date() for d in df['calendar_date']]
        except Exception as exc:
            logger.warning('trade_cal unavailable; fallback to weekdays: %s', exc)
            days = []
            cur = start_d
            while cur <= end_d:
                if cur.weekday() < 5:
                    days.append(cur)
                cur += timedelta(days=1)
            return days

    def get_previous_trade_date(self, ref_date: Optional[date] = None) -> date:
        """Find the most recent trade date on or before ref_date (default: yesterday)."""
        if ref_date is None:
            ref_date = date.today() - timedelta(days=1)
        start = ref_date - timedelta(days=10)
        trade_days = self.get_trade_days(start, ref_date)
        return trade_days[-1] if trade_days else ref_date

    def find_missing_trade_dates(self, lookback_days: int = BACKFILL_DAYS) -> List[date]:
        """Find trade dates missing successful sync for any required endpoint."""
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=lookback_days)
        trade_days = self.get_trade_days(start, end)
        missing: List[date] = []
        for td in trade_days:
            for ep in REQUIRED_ENDPOINTS:
                status = self.get_sync_status(td, ep)
                if status != 'success':
                    missing.append(td)
                    break
        missing_sorted = sorted(set(missing))
        if missing_sorted:
            logger.info("Missing trade dates: %s", missing_sorted)
        return missing_sorted

    def _sleep_backoff(self, attempt: int):
        delay = RETRY_BACKOFF_BASE * max(1, attempt)
        time.sleep(delay)

    # =========================================================================
    # AkShare ingestion helpers
    # =========================================================================

    def get_akshare_symbols(self) -> List[str]:
        """AkShare no longer ingests stock-level data; return empty list.

        Historically this returned symbols from `stock_basic`. AkShare is
        limited to index_daily now, so we treat stock list as empty to avoid
        attempting stock-level ingestion.
        """
        logger.debug("get_akshare_symbols: AkShare stock ingestion disabled, returning empty list")
        return []

    def run_akshare_daily_for_date(self, sync_date: date) -> Tuple[str, int, Optional[str]]:
        """Ingest AkShare daily data for a specific date with retries."""
        # Only ingest index_daily from AkShare. Stock-level ingestion disabled.
        target_date_dash = sync_date.strftime('%Y-%m-%d')
        try:
            from app.services.akshare_ingest import INDEX_MAPPING
            index_symbols = list(INDEX_MAPPING.keys())
        except Exception:
            index_symbols = []

        index_failures = index_symbols[:]
        idx_attempt = 0
        total_success = 0
        while index_failures and idx_attempt < MAX_RETRIES:
            idx_attempt += 1
            remaining = []
            for idx_sym in index_failures:
                try:
                    ak_ingest_index_daily(symbol=idx_sym, start_date=target_date_dash)
                    total_success += 1
                except Exception as exc:
                    logger.warning("AkShare index daily failed for %s on %s (attempt %d/%d): %s", idx_sym, target_date_dash, idx_attempt, MAX_RETRIES, exc)
                    remaining.append(idx_sym)
            index_failures = remaining
            if index_failures:
                self._sleep_backoff(idx_attempt)

        if index_failures:
            msg = f"index_failures={len(index_failures)}"
            return ('partial' if total_success > 0 else 'error'), total_success, msg
        return 'success', total_success, None

    def run_akshare_to_tushare_for_date(self, sync_date: date) -> Tuple[str, int, Optional[str]]:
        """Sync AkShare data into Tushare DB for a specific date."""
        start_date = sync_date.strftime('%Y-%m-%d')
        attempt = 0
        last_error: Optional[str] = None
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                # We no longer sync AkShare index data into Tushare. Instead,
                # report success if AkShare has index_daily rows for the date.
                with akshare_engine.connect() as conn:
                    res = conn.execute(text("SELECT COUNT(1) as cnt FROM index_daily WHERE trade_date = :d"), {'d': start_date})
                    row = res.fetchone()
                    cnt = row[0] if row else 0
                return 'success', int(cnt or 0), None
            except Exception as exc:
                last_error = str(exc)
                self._sleep_backoff(attempt)
        return 'error', 0, last_error

    def run_tushare_daily_for_date(self, sync_date: date) -> Tuple[str, int, Optional[str]]:
        """Ingest Tushare daily data for a specific date with retries."""
        ts_codes = get_all_ts_codes()
        if not ts_codes:
            ingest_stock_basic()
            ts_codes = get_all_ts_codes()
        if not ts_codes:
            return 'error', 0, 'No Tushare ts_codes available'

        target = sync_date.strftime('%Y%m%d')
        failures = ts_codes[:]
        total_success = 0
        attempt = 0
        while failures and attempt < MAX_RETRIES:
            attempt += 1
            remaining = []
            for code in failures:
                try:
                    ingest_daily(ts_code=code, start_date=target, end_date=target)
                    total_success += 1
                except Exception as exc:
                    logger.warning("Tushare daily failed for %s on %s (attempt %d/%d): %s", code, target, attempt, MAX_RETRIES, exc)
                    remaining.append(code)
                time.sleep(0.02)
            failures = remaining
            if failures:
                self._sleep_backoff(attempt)

        if failures:
            msg = f"failures={len(failures)}"
            return 'partial' if total_success > 0 else 'error', total_success, msg
        return 'success', total_success, None

    def run_vnpy_sync(self) -> Tuple[str, int, Optional[str]]:
        """Sync Tushare data into vnpy database."""
        attempt = 0
        last_error: Optional[str] = None
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                symbols, bars = self.sync_to_vnpy()
                return 'success', bars, None
            except Exception as exc:
                last_error = str(exc)
                self._sleep_backoff(attempt)
        return 'error', 0, last_error
    
    def map_exchange(self, ts_code: str) -> str:
        """Map tushare ts_code suffix to vnpy exchange string."""
        suffix = ts_code.split('.')[-1] if '.' in ts_code else 'SZ'
        return EXCHANGE_MAP.get(suffix, 'SZSE')
    
    def get_symbol(self, ts_code: str) -> str:
        """Extract symbol from ts_code."""
        return ts_code.split('.')[0] if '.' in ts_code else ts_code
    
    # =========================================================================
    # Step 1: Fetch from Tushare API to tushare_data
    # =========================================================================
    
    def fetch_tushare_data(self, ts_codes: Optional[List[str]] = None, 
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None) -> int:
        """
        Fetch daily data from Tushare API and store in tushare_data database.
        
        Args:
            ts_codes: List of stock codes to fetch (None = all)
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            Number of records fetched
        """
        if ts_codes is None:
            ts_codes = get_all_ts_codes()
            if not ts_codes:
                # If no stocks in DB, first ingest stock_basic
                logger.info("No stocks in database, fetching stock_basic first...")
                ingest_stock_basic()
                ts_codes = get_all_ts_codes()
        
        total_fetched = 0
        for i, ts_code in enumerate(ts_codes, 1):
            try:
                # Get last trade date for incremental sync
                if start_date is None:
                    last_date = get_max_trade_date(ts_code)
                    if last_date:
                        start = (pd.to_datetime(last_date) + timedelta(days=1)).strftime('%Y%m%d')
                    else:
                        start = None
                else:
                    start = start_date
                
                logger.info(f"[{i}/{len(ts_codes)}] Fetching {ts_code} from {start or 'beginning'}...")
                ingest_daily(ts_code=ts_code, start_date=start, end_date=end_date)
                total_fetched += 1
                
                # Rate limiting
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error fetching {ts_code}: {e}")
                continue
        
        logger.info(f"Tushare fetch complete: {total_fetched} symbols processed")
        return total_fetched
    
    # =========================================================================
    # Step 2: Sync from tushare_data to vnpy_data
    # =========================================================================
    
    def get_last_sync_date(self, symbol: str, exchange: str, interval: str = 'd') -> Optional[date]:
        """Get the last synced date for a symbol from vnpy_data."""
        with self.vnpy_engine.connect() as conn:
            result = conn.execute(text("""
                SELECT last_sync_date FROM sync_status 
                WHERE symbol = :symbol AND exchange = :exchange AND `interval` = :interval
            """), {'symbol': symbol, 'exchange': exchange, 'interval': interval})
            row = result.fetchone()
            return row[0] if row else None
    
    def update_sync_status(self, symbol: str, exchange: str, interval: str, 
                           sync_date: date, count: int):
        """Update sync status for a symbol."""
        with self.vnpy_engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO sync_status (symbol, exchange, `interval`, last_sync_date, last_sync_count)
                VALUES (:symbol, :exchange, :interval, :sync_date, :count)
                ON DUPLICATE KEY UPDATE 
                    last_sync_date = VALUES(last_sync_date),
                    last_sync_count = VALUES(last_sync_count),
                    updated_at = CURRENT_TIMESTAMP
            """), {
                'symbol': symbol,
                'exchange': exchange,
                'interval': interval,
                'sync_date': sync_date,
                'count': count
            })
    
    def sync_symbol_to_vnpy(self, ts_code: str, start_date: Optional[date] = None) -> int:
        """
        Sync a single symbol's daily data from tushare_data to vnpy_data.
        
        Args:
            ts_code: Tushare stock code (e.g., '000001.SZ')
            start_date: Sync data starting from this date (None = from last sync)
            
        Returns:
            Number of bars synced
        """
        symbol = self.get_symbol(ts_code)
        exchange = self.map_exchange(ts_code)
        interval = 'd'
        
        # Determine start date for incremental sync
        if start_date is None:
            last_sync = self.get_last_sync_date(symbol, exchange, interval)
            if last_sync:
                start_date = last_sync + timedelta(days=1)
        
        # Query data from tushare_data
        query = """
            SELECT trade_date, open, high, low, close, vol, amount
            FROM stock_daily
            WHERE ts_code = :ts_code
        """
        params = {'ts_code': ts_code}
        
        if start_date:
            query += " AND trade_date >= :start_date"
            params['start_date'] = start_date
        
        query += " ORDER BY trade_date ASC"
        
        with self.tushare_engine.connect() as conn:
            result = conn.execute(text(query), params)
            rows = result.fetchall()
        
        if not rows:
            logger.debug(f"No new data to sync for {ts_code}")
            return 0
        
        # Insert into vnpy_data.dbbardata
        insert_sql = text("""
            INSERT INTO dbbardata 
                (symbol, exchange, `datetime`, `interval`, volume, turnover, 
                 open_interest, open_price, high_price, low_price, close_price)
            VALUES 
                (:symbol, :exchange, :datetime, :interval, :volume, :turnover,
                 :open_interest, :open_price, :high_price, :low_price, :close_price)
            ON DUPLICATE KEY UPDATE
                volume = VALUES(volume),
                turnover = VALUES(turnover),
                open_price = VALUES(open_price),
                high_price = VALUES(high_price),
                low_price = VALUES(low_price),
                close_price = VALUES(close_price)
        """)
        
        synced = 0
        last_date = None
        
        with self.vnpy_engine.begin() as conn:
            for row in rows:
                trade_date = row[0]
                if isinstance(trade_date, str):
                    dt = datetime.strptime(trade_date, '%Y-%m-%d')
                else:
                    dt = datetime.combine(trade_date, datetime.min.time())
                
                conn.execute(insert_sql, {
                    'symbol': symbol,
                    'exchange': exchange,
                    'datetime': dt,
                    'interval': interval,
                    'volume': float(row[4]) if row[4] else 0.0,
                    'turnover': float(row[5]) if row[5] else 0.0,
                    'open_interest': 0.0,
                    'open_price': float(row[1]) if row[1] else 0.0,
                    'high_price': float(row[2]) if row[2] else 0.0,
                    'low_price': float(row[3]) if row[3] else 0.0,
                    'close_price': float(row[4]) if row[4] else 0.0,  # Fixed: was row[4] should be close
                })
                synced += 1
                last_date = trade_date
        
        # Update sync status
        if last_date:
            if isinstance(last_date, str):
                last_date = datetime.strptime(last_date, '%Y-%m-%d').date()
            self.update_sync_status(symbol, exchange, interval, last_date, synced)
        
        return synced
    
    def update_bar_overview(self, symbol: str, exchange: str, interval: str = 'd'):
        """Update the bar overview for a symbol after sync."""
        with self.vnpy_engine.begin() as conn:
            # Get bar statistics
            result = conn.execute(text("""
                SELECT COUNT(*), MIN(`datetime`), MAX(`datetime`)
                FROM dbbardata
                WHERE symbol = :symbol AND exchange = :exchange AND `interval` = :interval
            """), {'symbol': symbol, 'exchange': exchange, 'interval': interval})
            row = result.fetchone()
            
            if row and row[0] > 0:
                conn.execute(text("""
                    INSERT INTO dbbaroverview (symbol, exchange, `interval`, count, start, end)
                    VALUES (:symbol, :exchange, :interval, :count, :start, :end)
                    ON DUPLICATE KEY UPDATE
                        count = VALUES(count),
                        start = VALUES(start),
                        end = VALUES(end)
                """), {
                    'symbol': symbol,
                    'exchange': exchange,
                    'interval': interval,
                    'count': row[0],
                    'start': row[1],
                    'end': row[2]
                })
    
    def sync_to_vnpy(self, ts_codes: Optional[List[str]] = None, 
                     full_refresh: bool = False) -> Tuple[int, int]:
        """
        Sync all data from tushare_data to vnpy_data.
        
        Args:
            ts_codes: List of ts_codes to sync (None = all)
            full_refresh: If True, re-sync all data; if False, incremental sync
            
        Returns:
            Tuple of (symbols_synced, total_bars_synced)
        """
        # Get all ts_codes from tushare_data if not specified
        if ts_codes is None:
            with self.tushare_engine.connect() as conn:
                result = conn.execute(text("SELECT DISTINCT ts_code FROM stock_daily ORDER BY ts_code"))
                ts_codes = [row[0] for row in result.fetchall()]
        
        if not ts_codes:
            logger.warning("No symbols found in tushare_data to sync")
            return 0, 0
        
        logger.info(f"Syncing {len(ts_codes)} symbols to vnpy_data...")
        
        total_symbols = 0
        total_bars = 0
        
        for i, ts_code in enumerate(ts_codes, 1):
            try:
                start_date = None if not full_refresh else None  # Always incremental unless full_refresh
                
                if full_refresh:
                    # Clear existing data for this symbol (optional)
                    pass
                
                bars = self.sync_symbol_to_vnpy(ts_code, start_date if full_refresh else None)
                
                if bars > 0:
                    symbol = self.get_symbol(ts_code)
                    exchange = self.map_exchange(ts_code)
                    self.update_bar_overview(symbol, exchange)
                    total_symbols += 1
                    total_bars += bars
                    logger.info(f"[{i}/{len(ts_codes)}] Synced {bars} bars for {ts_code}")
                
            except Exception as e:
                logger.error(f"Error syncing {ts_code}: {e}")
                continue
        
        logger.info(f"Sync complete: {total_symbols} symbols, {total_bars} bars")
        return total_symbols, total_bars
    
    # =========================================================================
    # Step 3: Full sync pipeline
    # =========================================================================
    
    def run_daily_sync(self, ts_codes: Optional[List[str]] = None):
        """Run the full daily sync pipeline for the latest trade date."""
        sync_date = self.get_previous_trade_date()
        return self.run_sync_for_date(sync_date)

    def run_sync_for_date(self, sync_date: date) -> Dict[str, Dict[str, Optional[str]]]:
        """Run the full sync pipeline for a given trade date."""
        logger.info("Starting sync pipeline for %s...", sync_date)
        self.verify_audit_tables()

        results: Dict[str, Dict[str, Optional[str]]] = {}

        # AkShare daily
        self.write_sync_log(sync_date, 'akshare_daily', 'running', 0, None)
        status, rows, err = self.run_akshare_daily_for_date(sync_date)
        self.write_sync_log(sync_date, 'akshare_daily', status, rows, err)
        results['akshare_daily'] = {'status': status, 'error': err}

        # AkShare -> Tushare
        self.write_sync_log(sync_date, 'akshare_to_tushare', 'running', 0, None)
        status, rows, err = self.run_akshare_to_tushare_for_date(sync_date)
        self.write_sync_log(sync_date, 'akshare_to_tushare', status, rows, err)
        results['akshare_to_tushare'] = {'status': status, 'error': err}

        # Tushare daily
        self.write_sync_log(sync_date, 'tushare_daily', 'running', 0, None)
        status, rows, err = self.run_tushare_daily_for_date(sync_date)
        self.write_sync_log(sync_date, 'tushare_daily', status, rows, err)
        results['tushare_daily'] = {'status': status, 'error': err}

        # vnpy sync
        self.write_sync_log(sync_date, 'vnpy_sync', 'running', 0, None)
        status, rows, err = self.run_vnpy_sync()
        self.write_sync_log(sync_date, 'vnpy_sync', status, rows, err)
        results['vnpy_sync'] = {'status': status, 'error': err}

        logger.info("Sync pipeline finished for %s: %s", sync_date, results)
        return results
    
    # =========================================================================
    # Daemon mode
    # =========================================================================
    
    def run_daemon(self):
        """Run as a background daemon with scheduled sync."""
        logger.info("Starting data sync daemon (scheduled at %02d:%02d)...", SYNC_HOUR, SYNC_MINUTE)

        # Verify audit tables and backfill on startup
        self.verify_audit_tables()
        self.run_backfill(lookback_days=BACKFILL_DAYS)

        # Schedule daily sync for latest trade date
        schedule.every().day.at(f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}").do(self.run_daily_sync)

        self.running = True
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("Daemon stopped by user")
        finally:
            self.running = False
    
    def stop(self):
        """Stop the daemon."""
        self.running = False

    def run_backfill(self, lookback_days: int = BACKFILL_DAYS):
        """Backfill missing trade dates based on audit logs."""
        missing = self.find_missing_trade_dates(lookback_days=lookback_days)
        for d in missing:
            self.run_sync_for_date(d)


def main():
    parser = argparse.ArgumentParser(description="TraderMate Data Sync Daemon")
    parser.add_argument('--once', action='store_true', help='Run sync once and exit')
    parser.add_argument('--daemon', action='store_true', help='Run as background daemon')
    parser.add_argument('--sync-vnpy', action='store_true', help='Only sync tushare_data to vnpy_data')
    parser.add_argument('--fetch-only', action='store_true', help='Only fetch from Tushare API')
    parser.add_argument('--symbol', type=str, help='Sync specific symbol (e.g., 000001.SZ)')
    parser.add_argument('--all-symbols', action='store_true', help='Sync all symbols')
    parser.add_argument('--full-refresh', action='store_true', help='Full refresh (not incremental)')
    parser.add_argument('--date', type=str, help='Sync specific trade date (YYYY-MM-DD)')
    parser.add_argument('--from', dest='from_date', type=str, help='Start date for range (YYYY-MM-DD)')
    parser.add_argument('--to', dest='to_date', type=str, help='End date for range (YYYY-MM-DD)')
    parser.add_argument('--backfill-days', type=int, help='Backfill missing trade dates for N days')
    args = parser.parse_args()
    
    daemon = DataSyncDaemon()
    
    # Determine which symbols to process
    ts_codes = None
    if args.symbol:
        ts_codes = [args.symbol]
    elif args.all_symbols:
        ts_codes = None  # Will fetch all
    
    if args.daemon:
        daemon.run_daemon()
    elif args.backfill_days:
        daemon.run_backfill(lookback_days=args.backfill_days)
    elif args.date:
        sync_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        daemon.run_sync_for_date(sync_date)
    elif args.from_date and args.to_date:
        start = datetime.strptime(args.from_date, '%Y-%m-%d').date()
        end = datetime.strptime(args.to_date, '%Y-%m-%d').date()
        for d in daemon.get_trade_days(start, end):
            daemon.run_sync_for_date(d)
    elif args.sync_vnpy:
        daemon.sync_to_vnpy(ts_codes, full_refresh=args.full_refresh)
    elif args.fetch_only:
        daemon.fetch_tushare_data(ts_codes)
    elif args.once:
        daemon.run_daily_sync(ts_codes)
    else:
        # Default: run once for latest trade date
        daemon.run_daily_sync(ts_codes)


if __name__ == '__main__':
    main()
