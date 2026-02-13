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
    python -m app.datasync.service.data_sync_daemon.py --once          # Run once for latest trade date
    python -m app.datasync.service.data_sync_daemon.py --daemon        # Run as daemon (daily at 02:00)
    python -m app.datasync.service.data_sync_daemon.py --sync-vnpy     # Sync to vnpy only
"""

import os
import sys
import time
import json
import logging
import argparse
import schedule
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Tuple, Dict

import pandas as pd
# SQLAlchemy engines are provided by DAO modules; avoid importing SQLAlchemy here

# Add project root to path
sys.path.insert(0, str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from app.datasync.service.tushare_ingest import (
    ingest_daily,
    ingest_stock_basic,
    ingest_daily_basic,
    ingest_adj_factor,
    ingest_dividend,
    ingest_top10_holders,
    ingest_dividend_by_date_range,
    ingest_top10_holders_by_date_range,
    ingest_adj_factor_by_date_range,
    get_all_ts_codes,
    get_max_trade_date,
    call_pro,
    pro
)
from app.datasync.service.akshare_ingest import (
    ingest_index_daily as ak_ingest_index_daily,
)

# DAO imports to replace raw SQL
from app.domains.extdata.dao.sync_log_dao import (
    write_sync_log as dao_write_sync_log,
    get_sync_status as dao_get_sync_status,
    find_failed_syncs as dao_find_failed_syncs,
)
from app.domains.extdata.dao.tushare_dao import engine as tushare_engine  # use DAO engine
from app.domains.extdata.dao.akshare_dao import engine as akshare_engine  # use DAO engine
from app.domains.extdata.dao.vnpy_dao import engine as vnpy_engine  # use DAO engine
from app.domains.extdata.dao.data_sync_status_dao import text
from app.domains.extdata.dao.data_sync_status_dao import (
    get_index_daily_count_for_date,
    get_stock_basic_count,
    get_adj_factor_count_for_date,
    get_stock_daily_ts_codes_for_date
)
from app.domains.extdata.dao.tushare_dao import fetch_stock_daily_rows
from app.domains.extdata.dao.vnpy_dao import (
    get_last_sync_date as dao_get_last_sync_date,
    update_sync_status as dao_update_sync_status,
    bulk_upsert_dbbardata as dao_bulk_upsert_dbbardata,
    get_bar_stats as dao_get_bar_stats,
    upsert_dbbaroverview as dao_upsert_dbbaroverview,
)

# Import AkShare for trade calendar fallback
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    ak = None

# Import refactored sync coordinator
from app.datasync.service.sync_coordinator import (
    daily_ingest,
    missing_data_backfill,
    initialize_sync_status_table,
    refresh_trade_calendar
)

from app.infrastructure.logging import configure_logging, get_logger  # noqa: E402
configure_logging()
logger = get_logger(__name__)

# Database URLs are provided via infrastructure connections/DAOs

# Sync configuration
SYNC_HOUR = int(os.getenv('SYNC_HOUR', '2'))  # Default: 02:00 local time
SYNC_MINUTE = int(os.getenv('SYNC_MINUTE', '0'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
BACKFILL_DAYS = int(os.getenv('BACKFILL_DAYS', '60'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_BACKOFF_BASE = int(os.getenv('RETRY_BACKOFF_BASE', '10'))

# Required daily endpoints for audit tracking
REQUIRED_ENDPOINTS = [
    'akshare_index_daily',   # AkShare index data ingestion
    'tushare_daily',         # Tushare stock data ingestion
    'vnpy_sync'              # VNPy database conversion
]

# Exchange mapping for vnpy
EXCHANGE_MAP = {
    'SZ': 'SZSE',
    'SH': 'SSE',
}


class DataSyncDaemon:
    """Daemon for syncing data between Tushare and vnpy databases."""
    
    def __init__(self):
        # Use DAO-provided engines instead of creating engines in service layer
        self.tushare_engine = tushare_engine
        self.vnpy_engine = vnpy_engine
        self.running = False

    # =========================================================================
    # Audit helpers
    # =========================================================================

    def _table_exists(self, engine, table_name: str) -> bool:
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
        dao_write_sync_log(sync_date, endpoint, status, rows_synced=rows_synced, error_message=error)

    def get_sync_status(self, sync_date: date, endpoint: str) -> Optional[str]:
        return dao_get_sync_status(sync_date, endpoint)

    # =========================================================================
    # Trade calendar helpers
    # =========================================================================

    def get_trade_days(self, start_d: date, end_d: date) -> List[date]:
        """Return trade dates between start_d and end_d inclusive.
        
        Tries three methods in order:
        1. AkShare tool_trade_date_hist_sina (preferred, free)
        2. Tushare trade_cal API (fallback, may require permissions)
        3. Weekday fallback (Monday-Friday)
        """
        s = start_d.strftime('%Y%m%d')
        e = end_d.strftime('%Y%m%d')
        # Method 1: Prefer AkShare trade calendar
        if AKSHARE_AVAILABLE:
            try:
                df = ak.tool_trade_date_hist_sina()
                if df is not None and not df.empty:
                    df['trade_date'] = pd.to_datetime(df['trade_date'])
                    mask = (df['trade_date'].dt.date >= start_d) & (df['trade_date'].dt.date <= end_d)
                    trade_dates = df[mask]['trade_date'].dt.date.tolist()
                    logger.debug('Using AkShare trade calendar: %d dates', len(trade_dates))
                    return trade_dates
            except Exception as exc:
                logger.warning('AkShare trade calendar failed: %s, falling back to Tushare', exc)

        # Method 2: Try Tushare trade_cal as fallback
        try:
            df = call_pro('trade_cal', exchange='SSE', start_date=s, end_date=e)
            if df is not None and not df.empty:
                df = df[df['is_open'] == 1]
                # Tushare may use different column names for the date
                col = 'cal_date' if 'cal_date' in df.columns else ('calendar_date' if 'calendar_date' in df.columns else None)
                if col:
                    trade_dates = [pd.to_datetime(d).date() for d in df[col]]
                    logger.debug('Using Tushare trade_cal: %d dates', len(trade_dates))
                    return trade_dates
        except Exception as exc:
            err_msg = str(exc)
            if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                logger.debug('Tushare trade_cal requires higher permissions or failed: %s', err_msg)
            else:
                logger.warning('Tushare trade_cal failed: %s, falling back to weekday calendar', exc)

        # Method 3: Fallback to weekdays
        logger.debug('Using weekday fallback for trade calendar')
        days = []
        cur = start_d
        while cur <= end_d:
            if cur.weekday() < 5:  # Monday=0, Friday=4
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

    def check_sync_status(self, sync_date: date) -> Dict[str, str]:
        """Check status of all required endpoints for a given date.
        
        Returns:
            Dict mapping endpoint name to status ('success', 'partial', 'error', or None if not synced)
        """
        status_map = {}
        for ep in REQUIRED_ENDPOINTS:
            status = self.get_sync_status(sync_date, ep)
            status_map[ep] = status or 'not_synced'
        return status_map

    def find_failed_syncs(self, max_age_days: int = 7) -> List[Tuple[date, str]]:
        """Find failed or partial syncs within the last N days.
        
        Returns:
            List of (sync_date, endpoint) tuples for failed/partial syncs
        """
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=max_age_days)
        failures = dao_find_failed_syncs(start, end)
        if failures:
            logger.info("Found %d failed/partial syncs in last %d days", len(failures), max_age_days)
        return failures

    def retry_failed_syncs(self, max_age_days: int = 7):
        """Retry recently failed or partial sync tasks."""
        failures = self.find_failed_syncs(max_age_days)
        if not failures:
            logger.info("No failed syncs to retry")
            return
        
        logger.info("Retrying %d failed syncs...", len(failures))
        # Group by date
        dates_to_retry = sorted(set(d for d, _ in failures))
        for sync_date in dates_to_retry:
            logger.info("Retrying sync for %s", sync_date)
            self.run_sync_for_date(sync_date)

    def backfill_missing_dates(self, lookback_days: int = BACKFILL_DAYS):
        """Scan and backfill any missing trade dates."""
        missing = self.find_missing_trade_dates(lookback_days)
        if not missing:
            logger.info("No missing dates to backfill")
            return
        
        logger.info("Backfilling %d missing dates...", len(missing))
        for sync_date in missing:
            logger.info("Backfilling sync for %s", sync_date)
            self.run_sync_for_date(sync_date)

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
            from app.datasync.service.akshare_ingest import INDEX_MAPPING
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
                # Use DAO helper to count index_daily rows for date
                cnt = get_index_daily_count_for_date(start_date)
                return 'success', int(cnt or 0), None
            except Exception as exc:
                last_error = str(exc)
                self._sleep_backoff(attempt)
        return 'error', 0, last_error

    def run_tushare_daily_for_date(self, sync_date: date) -> Tuple[str, int, Optional[str]]:
        """Ingest comprehensive Tushare data for a specific date with retries.
        
        This includes:
        1. stock_basic (metadata, refreshed daily)
        2. stock_daily (OHLCV for all stocks, using efficient batch API)
        3. adj_factor (adjustment factors for all stocks)
        4. stock_dividend (dividend data, will skip if permission denied)
        5. top10_holders (top shareholders, will skip if permission denied)
        """
        target = sync_date.strftime('%Y%m%d')
        total_rows = 0
        errors = []
        
        # Step 1: Refresh stock_basic (metadata)
        logger.info("[Tushare %s] Step 1/5: Refreshing stock_basic...", target)
        try:
            ingest_stock_basic()
            count = get_stock_basic_count()
            logger.info("[Tushare %s] stock_basic refreshed: %d stocks", target, count)
        except Exception as exc:
            err_msg = str(exc)
            if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                logger.warning("[Tushare %s] stock_basic skipped: permission denied", target)
                errors.append("stock_basic: permission denied")
            else:
                logger.error("[Tushare %s] stock_basic failed: %s", target, exc)
                errors.append(f"stock_basic: {exc}")
        
        # Step 2: Ingest stock_daily using efficient batch API (pro.daily(trade_date='YYYYMMDD'))
        logger.info("[Tushare %s] Step 2/5: Ingesting stock_daily (batch API)...", target)
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                # Use efficient batch API to fetch all stocks for this date
                df = call_pro('daily', trade_date=target)
                if df is not None and not df.empty:
                    # Use existing upsert_daily function from tushare_ingest.py
                    from app.datasync.service.tushare_ingest import upsert_daily
                    rows = upsert_daily(df)
                    total_rows += rows
                    logger.info("[Tushare %s] stock_daily ingested: %d rows", target, rows)
                else:
                    logger.warning("[Tushare %s] stock_daily returned no data (non-trading day?)", target)
                break
            except Exception as exc:
                err_msg = str(exc)
                if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                    logger.warning("[Tushare %s] stock_daily skipped: permission denied", target)
                    errors.append("stock_daily: permission denied")
                    break
                elif attempt < MAX_RETRIES:
                    logger.warning("[Tushare %s] stock_daily attempt %d/%d failed: %s", target, attempt, MAX_RETRIES, exc)
                    self._sleep_backoff(attempt)
                else:
                    logger.error("[Tushare %s] stock_daily failed after retries: %s", target, exc)
                    errors.append(f"stock_daily: {exc}")
        
        # Step 3: Ingest adj_factor using batch API
        logger.info("[Tushare %s] Step 3/5: Ingesting adj_factor...", target)
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                ingest_adj_factor(trade_date=target)
                count = get_adj_factor_count_for_date(sync_date)
                total_rows += count
                logger.info("[Tushare %s] adj_factor ingested: %d rows", target, count)
                break
            except Exception as exc:
                err_msg = str(exc)
                if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                    logger.warning("[Tushare %s] adj_factor skipped: permission denied", target)
                    errors.append("adj_factor: permission denied")
                    break
                elif attempt < MAX_RETRIES:
                    logger.warning("[Tushare %s] adj_factor attempt %d/%d failed: %s", target, attempt, MAX_RETRIES, exc)
                    self._sleep_backoff(attempt)
                else:
                    logger.error("[Tushare %s] adj_factor failed after retries: %s", target, exc)
                    errors.append(f"adj_factor: {exc}")
        
        # Step 4: Ingest dividend data (may require higher permissions)
        logger.info("[Tushare %s] Step 4/5: Ingesting stock_dividend...", target)
        try:
                # Prefer range-based ingest which performs a DB-diff and inserts only missing rows.
                # This is more efficient and avoids repeated per-symbol calls.
                s = sync_date.strftime('%Y-%m-%d')
                e = s
                try:
                    ingest_dividend_by_date_range(s, e, batch_size=BATCH_SIZE)
                    logger.info("[Tushare %s] stock_dividend range ingest submitted for %s", target, s)
                except Exception as exc_inner:
                    logger.warning("[Tushare %s] stock_dividend range ingest failed, falling back to sample: %s", target, exc_inner)
                    # Fallback to lightweight sampling to avoid total failure
                    ts_codes = get_all_ts_codes()
                    div_count = 0
                    for code in ts_codes[:100]:  # Sample first 100 stocks to avoid rate limits
                        try:
                            ingest_dividend(ts_code=code)
                            div_count += 1
                        except Exception:
                            pass
                    logger.info("[Tushare %s] stock_dividend sampled: %d stocks checked", target, div_count)
        except Exception as exc:
            err_msg = str(exc)
            if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                logger.warning("[Tushare %s] stock_dividend skipped: permission denied", target)
                errors.append("stock_dividend: permission denied")
            else:
                logger.warning("[Tushare %s] stock_dividend failed: %s", target, exc)
                errors.append(f"stock_dividend: {exc}")
        
        # Step 5: Ingest top10_holders (may require higher permissions)
        logger.info("[Tushare %s] Step 5/5: Ingesting top10_holders...", target)
        try:
                s = sync_date.strftime('%Y-%m-%d')
                e = s
                try:
                    ingest_top10_holders_by_date_range(s, e, batch_size=BATCH_SIZE)
                    logger.info("[Tushare %s] top10_holders range ingest submitted for %s", target, s)
                except Exception as exc_inner:
                    logger.warning("[Tushare %s] top10_holders range ingest failed, falling back to sample: %s", target, exc_inner)
                    ts_codes = get_all_ts_codes()
                    holder_count = 0
                    for code in ts_codes[:50]:  # Sample first 50 stocks to avoid rate limits
                        try:
                            ingest_top10_holders(ts_code=code)
                            holder_count += 1
                        except Exception:
                            pass
                    logger.info("[Tushare %s] top10_holders sampled: %d stocks checked", target, holder_count)
        except Exception as exc:
            err_msg = str(exc)
            if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                logger.warning("[Tushare %s] top10_holders skipped: permission denied", target)
                errors.append("top10_holders: permission denied")
            else:
                logger.warning("[Tushare %s] top10_holders failed: %s", target, exc)
                errors.append(f"top10_holders: {exc}")
        
        # Determine final status
        if errors:
            error_summary = "; ".join(errors)
            if total_rows > 0:
                logger.warning("[Tushare %s] Completed with partial success: %d rows, errors: %s", target, total_rows, error_summary)
                return 'partial', total_rows, error_summary
            else:
                logger.error("[Tushare %s] All steps failed: %s", target, error_summary)
                return 'error', 0, error_summary
        else:
            logger.info("[Tushare %s] All steps completed successfully: %d rows", target, total_rows)
            return 'success', total_rows, None

    def run_vnpy_sync(self) -> Tuple[str, int, Optional[str]]:
        """Sync all Tushare data into vnpy database (full sync)."""
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

    def run_vnpy_sync_for_date(self, sync_date: date) -> Tuple[str, int, Optional[str]]:
        """Sync specific date's Tushare data into vnpy database (date-specific sync).
        
        This is more efficient than syncing all data, as it only processes one date.
        """
        logger.info("[VNPy %s] Starting date-specific sync...", sync_date)
        attempt = 0
        last_error: Optional[str] = None
        
        while attempt < MAX_RETRIES:
            attempt += 1
            try:
                # Get all ts_codes from tushare stock_daily for this date (via DAO)
                ts_codes = get_stock_daily_ts_codes_for_date(sync_date)
                
                if not ts_codes:
                    logger.warning("[VNPy %s] No data found in stock_daily for this date", sync_date)
                    return 'success', 0, None
                
                total_bars = 0
                total_symbols = 0
                
                for ts_code in ts_codes:
                    try:
                        bars = self.sync_symbol_to_vnpy(ts_code, start_date=sync_date)
                        if bars > 0:
                            symbol = self.get_symbol(ts_code)
                            exchange = self.map_exchange(ts_code)
                            self.update_bar_overview(symbol, exchange)
                            total_bars += bars
                            total_symbols += 1
                    except Exception as exc:
                        logger.warning("[VNPy %s] Failed to sync %s: %s", sync_date, ts_code, exc)
                        continue
                
                logger.info("[VNPy %s] Synced %d symbols, %d bars", sync_date, total_symbols, total_bars)
                return 'success', total_bars, None
                
            except Exception as exc:
                last_error = str(exc)
                logger.warning("[VNPy %s] Sync attempt %d/%d failed: %s", sync_date, attempt, MAX_RETRIES, exc)
                if attempt < MAX_RETRIES:
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
        return dao_get_last_sync_date(symbol, exchange, interval)
    
    def update_sync_status(self, symbol: str, exchange: str, interval: str, 
                           sync_date: date, count: int):
        """Update sync status for a symbol."""
        dao_update_sync_status(symbol, exchange, interval, sync_date, count)
    
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

        # Fetch rows via DAO to centralize SQL
        rows = fetch_stock_daily_rows(ts_code, start_date)
        
        if not rows:
            logger.debug(f"No new data to sync for {ts_code}")
            return 0
        
        # Bulk prepare rows for vnpy dbbardata upsert via DAO
        synced = 0
        last_date = None
        
        # Prepare rows for bulk insert into vnpy dbbardata
        to_insert = []
        for row in rows:
            trade_date = row[0]
            if isinstance(trade_date, str):
                dt = datetime.strptime(trade_date, '%Y-%m-%d')
            else:
                dt = datetime.combine(trade_date, datetime.min.time())
            to_insert.append({
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
                'close_price': float(row[4]) if row[4] else 0.0,
            })
            synced += 1
            last_date = trade_date

        # Bulk upsert into vnpy dbbardata via DAO
        dao_bulk_upsert_dbbardata(to_insert)
        
        # Update sync status
        if last_date:
            if isinstance(last_date, str):
                last_date = datetime.strptime(last_date, '%Y-%m-%d').date()
            self.update_sync_status(symbol, exchange, interval, last_date, synced)
        
        return synced
    
    def update_bar_overview(self, symbol: str, exchange: str, interval: str = 'd'):
        """Update the bar overview for a symbol after sync."""
        # Use DAO to compute bar stats and upsert overview
        cnt, start_dt, end_dt = dao_get_bar_stats(symbol, exchange, interval)
        if cnt and cnt > 0:
            dao_upsert_dbbaroverview(symbol, exchange, interval, cnt, start_dt, end_dt)
    
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
            # Use existing helper to get all ts_codes
            ts_codes = get_all_ts_codes()
        
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
        """Run the full sync pipeline for a given trade date.
        
        Three main tasks:
        1. akshare_index_daily: Ingest index data from AkShare API
        2. tushare_daily: Ingest comprehensive stock data from Tushare API
        3. vnpy_sync: Convert and sync Tushare data to VNPy format
        """
        logger.info("="*80)
        logger.info("Starting sync pipeline for %s...", sync_date)
        logger.info("="*80)
        self.verify_audit_tables()

        results: Dict[str, Dict[str, Optional[str]]] = {}

        # Task 1: AkShare index daily
        logger.info("[Task 1/3] AkShare Index Daily")
        self.write_sync_log(sync_date, 'akshare_index_daily', 'running', 0, None)
        status, rows, err = self.run_akshare_daily_for_date(sync_date)
        self.write_sync_log(sync_date, 'akshare_index_daily', status, rows, err)
        results['akshare_index_daily'] = {'status': status, 'error': err}
        logger.info("[Task 1/3] akshare_index_daily: %s (%d rows)", status, rows)

        # Task 2: Tushare comprehensive daily data
        logger.info("[Task 2/3] Tushare Daily (stock_basic, stock_daily, adj_factor, dividend, top10_holders)")
        self.write_sync_log(sync_date, 'tushare_daily', 'running', 0, None)
        status, rows, err = self.run_tushare_daily_for_date(sync_date)
        self.write_sync_log(sync_date, 'tushare_daily', status, rows, err)
        results['tushare_daily'] = {'status': status, 'error': err}
        logger.info("[Task 2/3] tushare_daily: %s (%d rows)", status, rows)

        # Task 3: VNPy sync (only if Tushare sync succeeded or partial)
        logger.info("[Task 3/3] VNPy Sync (tushare -> vnpy conversion)")
        if status in ('success', 'partial'):
            self.write_sync_log(sync_date, 'vnpy_sync', 'running', 0, None)
            status, rows, err = self.run_vnpy_sync_for_date(sync_date)
            self.write_sync_log(sync_date, 'vnpy_sync', status, rows, err)
            results['vnpy_sync'] = {'status': status, 'error': err}
            logger.info("[Task 3/3] vnpy_sync: %s (%d bars)", status, rows)
        else:
            logger.warning("[Task 3/3] vnpy_sync skipped due to tushare_daily failure")
            self.write_sync_log(sync_date, 'vnpy_sync', 'skipped', 0, 'Skipped due to tushare_daily failure')
            results['vnpy_sync'] = {'status': 'skipped', 'error': 'Depends on tushare_daily'}

        logger.info("="*80)
        logger.info("Sync pipeline finished for %s", sync_date)
        logger.info("Results: %s", results)
        logger.info("="*80)
        return results
    
    # =========================================================================
    # Daemon mode
    # =========================================================================
    
    def run_daemon(self):
        """Run as a background daemon with scheduled sync."""
        logger.info("="*80)
        logger.info("TraderMate Data Sync Daemon Starting...")
        logger.info("Scheduled daily sync at %02d:%02d", SYNC_HOUR, SYNC_MINUTE)
        logger.info("="*80)

        # Verify audit tables on startup
        logger.info("[Startup] Verifying audit tables...")
        self.verify_audit_tables()

        # Retry failed syncs from last 7 days
        logger.info("[Startup] Checking for failed syncs to retry...")
        self.retry_failed_syncs(max_age_days=7)

        # Backfill missing dates
        logger.info("[Startup] Checking for missing dates to backfill...")
        self.backfill_missing_dates(lookback_days=BACKFILL_DAYS)

        logger.info("[Startup] Initialization complete. Waiting for scheduled sync...")
        logger.info("="*80)

        # Schedule daily sync for latest trade date using target timezone (Asia/Shanghai by default)
        target_tz_name = os.getenv('SYNC_TIMEZONE', 'Asia/Shanghai')
        try:
            target_tz = ZoneInfo(target_tz_name)
        except Exception:
            logger.warning('Invalid SYNC_TIMEZONE %s, falling back to system local time', target_tz_name)
            target_tz = None

        if target_tz:
            # compute the next run time in target timezone and convert to local system time
            now_target = datetime.now(tz=target_tz)
            next_target = now_target.replace(hour=SYNC_HOUR, minute=SYNC_MINUTE, second=0, microsecond=0)
            if next_target <= now_target:
                next_target = next_target + timedelta(days=1)
            # convert to local system timezone
            next_local = next_target.astimezone()
            local_time_str = f"{next_local.hour:02d}:{next_local.minute:02d}"
            schedule.every().day.at(local_time_str).do(self.run_daily_sync)
            logger.info('Scheduled daily sync at %s (%s timezone) which is %s local time', f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}", target_tz_name, local_time_str)
        else:
            schedule.every().day.at(f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}").do(self.run_daily_sync)

        self.running = True
        try:
            while self.running:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("\nDaemon stopped by user")
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
    parser = argparse.ArgumentParser(
        description="TraderMate Data Sync Daemon",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run as persistent daemon (auto-retry failed, auto-backfill missing)
  python -m app.datasync.service.data_sync_daemon.py --daemon
  
  # Run once for yesterday's trade date
  python -m app.datasync.service.data_sync_daemon.py --once
  
  # Sync specific date
  python -m app.datasync.service.data_sync_daemon.py --date 2026-02-09
  
  # Backfill last 30 days
  python -m app.datasync.service.data_sync_daemon.py --backfill 30
  
  # Retry failed syncs from last 7 days
  python -m app.datasync.service.data_sync_daemon.py --retry-failed
  
  # Check sync status
  python -m app.datasync.service.data_sync_daemon.py --status
        """
    )
    parser.add_argument('--once', action='store_true', help='Run sync once for yesterday and exit')
    parser.add_argument('--daemon', action='store_true', help='Run as persistent background daemon (scheduled at 02:00)')
    parser.add_argument('--sync-vnpy', action='store_true', help='Only sync tushare_data to vnpy_data (full sync)')
    parser.add_argument('--fetch-only', action='store_true', help='Only fetch from Tushare API (deprecated)')
    parser.add_argument('--symbol', type=str, help='Sync specific symbol (e.g., 000001.SZ) - for vnpy-sync only')
    parser.add_argument('--all-symbols', action='store_true', help='Sync all symbols - for vnpy-sync only')
    parser.add_argument('--full-refresh', action='store_true', help='Full refresh (not incremental) - for vnpy-sync only')
    parser.add_argument('--date', type=str, help='Sync specific trade date (YYYY-MM-DD)')
    parser.add_argument('--from', dest='from_date', type=str, help='Start date for range sync (YYYY-MM-DD)')
    parser.add_argument('--to', dest='to_date', type=str, help='End date for range sync (YYYY-MM-DD)')
    parser.add_argument('--backfill', type=int, metavar='DAYS', help='Backfill missing trade dates for last N days')
    parser.add_argument('--retry-failed', action='store_true', help='Retry failed/partial syncs from last 7 days')
    parser.add_argument('--status', action='store_true', help='Check sync status for recent dates')
    parser.add_argument('--dividend-range', nargs=2, metavar=('FROM','TO'), help='Ingest dividend for date range (YYYY-MM-DD YYYY-MM-DD)')
    parser.add_argument('--top10-range', nargs=2, metavar=('FROM','TO'), help='Ingest top10_holders for date range (YYYY-MM-DD YYYY-MM-DD)')
    parser.add_argument('--adj-range', nargs=2, metavar=('FROM','TO'), help='Ingest adj_factor for date range (YYYY-MM-DD YYYY-MM-DD)')
    # New refactored architecture commands
    parser.add_argument('--daily-ingest', action='store_true', help='Run daily ingest using new 7-step pipeline')
    parser.add_argument('--backfill-missing', action='store_true', help='Backfill failed/pending steps using new status table')
    parser.add_argument('--init-status-table', type=int, metavar='YEARS', help='Initialize data_sync_status table by scanning last N years')
    parser.add_argument('--refresh-calendar', action='store_true', help='Refresh cached trade calendar from AkShare')
    args = parser.parse_args()
    
    daemon = DataSyncDaemon()
    
    # Determine which symbols to process
    ts_codes = None
    if args.symbol:
        ts_codes = [args.symbol]
    elif args.all_symbols:
        ts_codes = None  # Will fetch all
    
    # New refactored architecture commands (high priority)
    if args.init_status_table:
        logger.info("Initializing data_sync_status table for last %d years...", args.init_status_table)
        initialize_sync_status_table(lookback_years=args.init_status_table)
    elif args.refresh_calendar:
        logger.info("Refreshing trade calendar from AkShare...")
        refresh_trade_calendar()
    elif args.daily_ingest:
        logger.info("Running daily ingest with new 7-step pipeline...")
        results = daily_ingest()
        logger.info("Daily ingest results: %s", results)
    elif args.backfill_missing:
        logger.info("Running backfill for failed/pending steps...")
        missing_data_backfill(lookback_days=60)
    # Legacy commands
    elif args.daemon:
        daemon.run_daemon()
    elif args.retry_failed:
        logger.info("Retrying failed/partial syncs from last 7 days...")
        daemon.retry_failed_syncs(max_age_days=7)
    elif args.backfill:
        logger.info("Backfilling missing dates for last %d days...", args.backfill)
        daemon.backfill_missing_dates(lookback_days=args.backfill)
    elif args.status:
        logger.info("Checking sync status for last 7 days...")
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=7)
        trade_days = daemon.get_trade_days(start, end)
        for td in trade_days:
            status_map = daemon.check_sync_status(td)
            logger.info("%s: %s", td, status_map)
    elif args.date:
        sync_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        daemon.run_sync_for_date(sync_date)
    elif args.dividend_range:
        s, e = args.dividend_range
        logger.info('Running dividend range ingest %s -> %s', s, e)
        from app.datasync.service.tushare_ingest import ingest_dividend_by_date_range
        ingest_dividend_by_date_range(s, e, batch_size=BATCH_SIZE)
    elif args.top10_range:
        s, e = args.top10_range
        logger.info('Running top10_holders range ingest %s -> %s', s, e)
        from app.datasync.service.tushare_ingest import ingest_top10_holders_by_date_range
        ingest_top10_holders_by_date_range(s, e, batch_size=BATCH_SIZE)
    elif args.adj_range:
        s, e = args.adj_range
        logger.info('Running adj_factor range ingest %s -> %s', s, e)
        from app.datasync.service.tushare_ingest import ingest_adj_factor_by_date_range
        ingest_adj_factor_by_date_range(s, e, batch_size=BATCH_SIZE)
    elif args.from_date and args.to_date:
        start = datetime.strptime(args.from_date, '%Y-%m-%d').date()
        end = datetime.strptime(args.to_date, '%Y-%m-%d').date()
        trade_days = daemon.get_trade_days(start, end)
        logger.info("Syncing %d trade dates from %s to %s", len(trade_days), start, end)
        for d in trade_days:
            daemon.run_sync_for_date(d)
    elif args.sync_vnpy:
        daemon.sync_to_vnpy(ts_codes, full_refresh=args.full_refresh)
    elif args.fetch_only:
        logger.warning("--fetch-only is deprecated, use --date or --once instead")
        daemon.fetch_tushare_data(ts_codes)
    elif args.once:
        daemon.run_daily_sync(ts_codes)
    else:
        # Default: run once for latest trade date
        logger.info("No mode specified, running once for latest trade date (use --help for options)")
        daemon.run_daily_sync(ts_codes)


if __name__ == '__main__':
    main()
