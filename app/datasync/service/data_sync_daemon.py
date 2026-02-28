"""
Data Sync Daemon for TraderMate

Handles scheduled data synchronization with two main jobs:
1. Daily ingest at 2:00 AM Shanghai time (incremental sync)
2. Backfill job every 6 hours (historical gap-filling with DB lock)

Startup behavior:
- Synchronously runs daily ingest first
- Continues with other dates if any fail
- Then runs backfill
- Finally enters scheduler loop

Architecture:
- akshare: Index data from AkShare API
- tushare: Stock data from Tushare API
- vnpy: Converted data for vnpy trading platform

Usage:
    python -m app.datasync.service.data_sync_daemon --daemon        # Run as daemon
    python -m app.datasync.service.data_sync_daemon --daily         # Run daily ingest once
    python -m app.datasync.service.data_sync_daemon --backfill      # Run backfill once
"""

import os
import sys
import time
import logging
import argparse
import schedule
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import List, Optional, Tuple, Dict
from enum import Enum

import pandas as pd

# Add project root to path
sys.path.insert(0, str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

# Import ingest modules
from app.datasync.service.tushare_ingest import (
    ingest_daily,
    ingest_stock_basic,
    ingest_adj_factor,
    ingest_dividend,
    ingest_top10_holders,
    ingest_daily_basic,
    ingest_repo,
    ingest_all_other_data,
    ingest_all_daily,
    ingest_dividend_by_date_range,
    ingest_top10_holders_by_date_range,
    ingest_adj_factor_by_date_range,
    get_all_ts_codes,
    call_pro,
    upsert_daily,
)
from app.datasync.service.akshare_ingest import (
    ingest_index_daily as ak_ingest_index_daily,
    INDEX_MAPPING,
)
from app.datasync.service.vnpy_ingest import (
    sync_date_to_vnpy,
    sync_all_to_vnpy,
)

# Import DAOs
from app.domains.extdata.dao.data_sync_status_dao import (
    ensure_tables,
    write_step_status,
    get_step_status,
    get_failed_steps,
    get_cached_trade_dates,
    upsert_trade_dates,
    get_stock_basic_count,
    get_adj_factor_count_for_date,
    get_stock_daily_ts_codes_for_date,
    truncate_trade_cal,
    get_stock_daily_counts,
    get_adj_factor_counts,
    get_vnpy_counts,
    bulk_upsert_status,
    acquire_backfill_lock,
    release_backfill_lock,
    is_backfill_locked,
)
from app.domains.extdata.dao.tushare_dao import (
    upsert_dividend_df,
    fetch_stock_daily_rows,
)
# Legacy sync log DAO (used by Tushare-specific helpers)
from app.domains.extdata.dao.sync_log_dao import (
    write_tushare_stock_sync_log as dao_write_tushare_stock_sync_log,
    get_last_success_tushare_sync_date as dao_get_last_success_tushare_sync_date,
)

# Tushare daemon compatibility flags
DRY_RUN = os.getenv('DRY_RUN', '0') == '1'


# Import AkShare for trade calendar
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    ak = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
SYNC_HOUR = int(os.getenv('SYNC_HOUR', '2'))
SYNC_MINUTE = int(os.getenv('SYNC_MINUTE', '0'))
BACKFILL_INTERVAL_HOURS = int(os.getenv('BACKFILL_INTERVAL_HOURS', '6'))
BACKFILL_DAYS = int(os.getenv('BACKFILL_DAYS', '30'))  # How many days to look back for missing data
LOOKBACK_DAYS = int(os.getenv('LOOKBACK_DAYS', '60'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
TIMEZONE = 'Asia/Shanghai'

# Endpoints that must be synced (used by SyncStatusService)
REQUIRED_ENDPOINTS = [
    'akshare_index',
    'tushare_stock_basic',
    'tushare_stock_daily',
    'tushare_adj_factor',
    'tushare_dividend',
    'tushare_top10_holders',
]


class SyncStep(str, Enum):
    """Sync step identifiers matching DB enum"""
    AKSHARE_INDEX = 'akshare_index'
    TUSHARE_STOCK_BASIC = 'tushare_stock_basic'
    TUSHARE_STOCK_DAILY = 'tushare_stock_daily'
    TUSHARE_ADJ_FACTOR = 'tushare_adj_factor'
    TUSHARE_DIVIDEND = 'tushare_dividend'
    TUSHARE_TOP10_HOLDERS = 'tushare_top10_holders'
    VNPY_SYNC = 'vnpy_sync'


class SyncStatus(str, Enum):
    """Sync status enum matching DB"""
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCESS = 'success'
    PARTIAL = 'partial'
    ERROR = 'error'


# =============================================================================
# Trade Calendar Management
# =============================================================================

def get_trade_calendar(start_date: date, end_date: date) -> List[date]:
    """Get trade dates from cached calendar or fetch from AkShare.
    
    First tries akshare.trade_cal table (cached), then fetches from AkShare API
    and caches the result for future use.
    """
    # Try cached calendar first (via DAO)
    try:
        dates = get_cached_trade_dates(start_date, end_date)
        if dates:
            logger.debug('Using cached trade calendar: %d dates', len(dates))
            return dates
    except Exception as e:
        logger.debug('trade_cal table not available or empty: %s', e)
    
    # Fetch from AkShare and cache
    if AKSHARE_AVAILABLE:
        try:
            df = ak.tool_trade_date_hist_sina()
            if df is not None and not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                # Cache all dates to DB
                upsert_trade_dates([td.date() for td in df['trade_date']])
                logger.info('Cached %d trade dates to akshare.trade_cal', len(df))
                
                # Filter requested range
                mask = (df['trade_date'].dt.date >= start_date) & (df['trade_date'].dt.date <= end_date)
                dates = df[mask]['trade_date'].dt.date.tolist()
                logger.debug('Fetched trade calendar from AkShare: %d dates', len(dates))
                return dates
        except Exception as e:
            logger.warning('AkShare trade calendar failed: %s', e)
    
    # Fallback to weekdays
    logger.debug('Using weekday fallback for trade calendar')
    days = []
    cur = start_date
    while cur <= end_date:
        if cur.weekday() < 5:  # Monday=0, Friday=4
            days.append(cur)
        cur += timedelta(days=1)
    return days


def get_previous_trade_date(offset: int = 1) -> date:
    """Get the Nth previous trade date."""
    end = date.today()
    start = end - timedelta(days=30)
    trade_days = get_trade_calendar(start, end)
    if trade_days:
        return trade_days[-offset] if len(trade_days) >= offset else trade_days[0]
    return date.today() - timedelta(days=offset)


def refresh_trade_calendar():
    """Refresh cached trade calendar from AkShare (call monthly)."""
    if not AKSHARE_AVAILABLE:
        logger.warning('AkShare not available, cannot refresh trade calendar')
        return
    
    try:
        df = ak.tool_trade_date_hist_sina()
        if df is None or df.empty:
            logger.warning('AkShare returned no trade dates')
            return
        
        df['trade_date'] = pd.to_datetime(df['trade_date'])
        # Truncate and re-insert
        truncate_trade_cal()
        upsert_trade_dates([td.date() for td in df['trade_date']])
        logger.info('Refreshed trade calendar: %d dates cached', len(df))
    except Exception as e:
        logger.exception('Failed to refresh trade calendar: %s', e)
# =========================================================================
# Tushare-sync compatibility helpers (merged from tushare_sync_daemon.py)
# =========================================================================

ENDPOINTS = {
    'daily': lambda dt: ingest_all_daily(start_date=None, sleep_between=0.02) if 'ingest_all_daily' in globals() else None,
    'daily_by_date': None,
    'daily_basic': lambda dt: ingest_daily_basic() if 'ingest_daily_basic' in globals() else None,
    'adj_factor': lambda dt: ingest_all_other_data() if 'ingest_all_other_data' in globals() else None,
    'moneyflow': lambda dt: ingest_all_other_data() if 'ingest_all_other_data' in globals() else None,
    'dividend': lambda dt: ingest_all_other_data() if 'ingest_all_other_data' in globals() else None,
    'top10_holders': lambda dt: ingest_all_other_data() if 'ingest_all_other_data' in globals() else None,
    'margin': lambda dt: ingest_all_other_data() if 'ingest_all_other_data' in globals() else None,
    'block_trade': lambda dt: ingest_all_other_data() if 'ingest_all_other_data' in globals() else None,
    'repo': lambda dt: ingest_repo(repo_date=dt.strftime('%Y-%m-%d')) if 'ingest_repo' in globals() else None,
}


def get_trade_days(start_d: date, end_d: date) -> List[str]:
    s = start_d.strftime('%Y%m%d')
    e = end_d.strftime('%Y%m%d')
    try:
        df = call_pro('trade_cal', exchange='SSE', start_date=s, end_date=e)
        if df is None:
            raise Exception('trade_cal returned None')
        df = df[df['is_open'] == 1]
        col = 'calendar_date' if 'calendar_date' in df.columns else ('cal_date' if 'cal_date' in df.columns else None)
        dates = [str(pd.to_datetime(d).date()) for d in df[col]] if col else []
        return dates
    except Exception as exc:
        logger.warning('Could not use trade_cal (fallback to weekdays): %s', exc)
        days = []
        cur = start_d
        while cur <= end_d:
            if cur.weekday() < 5:
                days.append(str(cur))
            cur = cur + timedelta(days=1)
        return days


def write_sync_log(sync_date: date, endpoint: str, status: str, rows: int = 0, err: Optional[str] = None):
    if DRY_RUN:
        logger.info('DRY RUN - skip writing sync log: %s %s %s', sync_date, endpoint, status)
        return
    dao_write_tushare_stock_sync_log(sync_date, endpoint, status, rows, err)


def get_last_success_date(endpoint: str):
    return dao_get_last_success_tushare_sync_date(endpoint)


def sync_daily_for_date(d: date):
    logger.info('Starting daily sync for %s', d)
    ts_codes = get_all_ts_codes()
    total = len(ts_codes)
    rows_total = 0
    failures = 0
    for i, ts_code in enumerate(ts_codes, start=1):
        try:
            ingest_daily(ts_code=ts_code, start_date=d.strftime('%Y%m%d'), end_date=d.strftime('%Y%m%d'))
        except Exception as e:
            failures += 1
            logger.warning('Failed daily for %s on %s: %s', ts_code, d, e)
        time.sleep(0.02)
        if i % 500 == 0:
            logger.info('Daily sync progress: %d/%d', i, total)
    status = 'success' if failures == 0 else 'partial' if failures < total else 'error'
    write_sync_log(d, 'daily', status, rows_total, f'failures={failures}' if failures else None)
    logger.info('Daily sync finished for %s: status=%s failures=%d', d, status, failures)


def run_sync_for_date(d: date, allowed_endpoints: list):
    logger.info('Running sync for date %s, endpoints: %s', d, allowed_endpoints)
    for ep in allowed_endpoints:
        try:
            if ep == 'daily':
                sync_daily_for_date(d)
            elif ep == 'repo':
                try:
                    if not DRY_RUN:
                        ingest_repo(repo_date=d.strftime('%Y-%m-%d'))
                        write_sync_log(d, 'repo', 'success', 0, None)
                except Exception as e:
                    write_sync_log(d, 'repo', 'error', 0, str(e))
            else:
                try:
                    if ep == 'daily_basic':
                        ingest_daily_basic()
                    if ep in ('daily_basic','adj_factor','moneyflow','dividend','top10_holders','margin','block_trade'):
                        ingest_all_other_data()
                        write_sync_log(d, ep, 'success', 0, None)
                except Exception as e:
                    write_sync_log(d, ep, 'error', 0, str(e))
        except Exception as e:
            logger.exception('Error syncing endpoint %s for %s: %s', ep, d, e)
            write_sync_log(d, ep, 'error', 0, str(e))


# =============================================================================
# Step Implementations
# =============================================================================

def run_akshare_index_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 1: Ingest AkShare index daily data."""
    total_success = 0
    failures = []
    target_date = sync_date.strftime('%Y-%m-%d')
    
    for symbol in INDEX_MAPPING.keys():
        try:
            ak_ingest_index_daily(symbol=symbol, start_date=target_date)
            total_success += 1
        except Exception as e:
            logger.warning('AkShare index %s failed: %s', symbol, e)
            failures.append(symbol)
    
    if failures:
        err_msg = f"Failed symbols: {','.join(failures)}"
        if total_success > 0:
            return SyncStatus.PARTIAL, total_success, err_msg
        return SyncStatus.ERROR, 0, err_msg
    return SyncStatus.SUCCESS, total_success, None


def run_tushare_stock_basic_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2a: Refresh Tushare stock_basic metadata."""
    try:
        ingest_stock_basic()
        count = get_stock_basic_count()
        return SyncStatus.SUCCESS, count, None
    except Exception as e:
        logger.exception('stock_basic failed: %s', e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_stock_daily_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2b: Ingest Tushare stock_daily using batch API."""
    target = sync_date.strftime('%Y%m%d')
    try:
        df = call_pro('daily', trade_date=target)
        if df is None or df.empty:
            return SyncStatus.SUCCESS, 0, 'No trading data (non-trading day?)'
        rows = upsert_daily(df)
        return SyncStatus.SUCCESS, rows, None
    except Exception as e:
        logger.exception('stock_daily failed: %s', e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_adj_factor_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2c: Ingest Tushare adj_factor using batch API."""
    target = sync_date.strftime('%Y%m%d')
    try:
        ingest_adj_factor(trade_date=target)
        count = get_adj_factor_count_for_date(sync_date)
        return SyncStatus.SUCCESS, count, None
    except Exception as e:
        logger.exception('adj_factor failed: %s', e)
        return SyncStatus.ERROR, 0, str(e)


def run_tushare_dividend_step(sync_date: date, use_batch: bool = False) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2d: Ingest Tushare dividend data.
    
    For daily mode: Sample 100 stocks per-symbol
    For backfill mode: Use batch API by ann_date
    """
    if use_batch:
        try:
            target = sync_date.strftime('%Y%m%d')
            df = call_pro('dividend', ann_date=target)
            if df is None or df.empty:
                return SyncStatus.SUCCESS, 0, None
            
            rows = upsert_dividend_df(df)
            return SyncStatus.SUCCESS, rows, None
        except Exception as e:
            err_msg = str(e)
            if '没有接口访问权限' in err_msg or 'permission' in err_msg.lower():
                return SyncStatus.PARTIAL, 0, 'Permission denied'
            logger.exception('dividend batch failed: %s', e)
            return SyncStatus.ERROR, 0, str(e)
    else:
        # Daily mode: sample approach
        ts_codes = get_all_ts_codes()
        total = min(100, len(ts_codes))
        success = 0
        for code in ts_codes[:total]:
            try:
                ingest_dividend(ts_code=code)
                success += 1
            except:
                pass
        if success > 0:
            return SyncStatus.SUCCESS, success, f'Sampled {success}/{total} stocks'
        return SyncStatus.PARTIAL, 0, 'No dividends fetched'


def run_tushare_top10_holders_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 2e: Ingest Tushare top10_holders (sample mode for daily)."""
    ts_codes = get_all_ts_codes()
    total = min(50, len(ts_codes))
    success = 0
    for code in ts_codes[:total]:
        try:
            ingest_top10_holders(ts_code=code)
            success += 1
        except:
            pass
    if success > 0:
        return SyncStatus.SUCCESS, success, f'Sampled {success}/{total} stocks'
    return SyncStatus.PARTIAL, 0, 'No holder data fetched'


def run_vnpy_sync_step(sync_date: date) -> Tuple[SyncStatus, int, Optional[str]]:
    """Step 3: Sync data to VNPy database."""
    try:
        total_symbols, total_bars = sync_date_to_vnpy(sync_date)
        if total_symbols > 0:
            return SyncStatus.SUCCESS, total_bars, None
        return SyncStatus.PARTIAL, 0, 'No symbols synced'
    except Exception as e:
        logger.exception('VNPy sync failed: %s', e)
        return SyncStatus.ERROR, 0, str(e)


# =============================================================================
# Main Functions
# =============================================================================

def daily_ingest(target_date: Optional[date] = None, continue_on_error: bool = True) -> Dict[str, Dict]:
    """Run incremental daily sync for a specific date.
    
    Steps:
    1. AkShare index daily
    2. Tushare stock_basic + stock_daily + adj_factor + dividend + top10_holders
    3. VNPy sync
    
    Args:
        target_date: Date to sync (None = previous trade date)
        continue_on_error: If True, continue with other steps even if one fails
    
    Returns:
        Dict with step results
    """
    if target_date is None:
        target_date = get_previous_trade_date()
    
    logger.info("="*80)
    logger.info("Daily ingest starting for %s", target_date)
    logger.info("="*80)
    
    results = {}
    
    # Step 1: AkShare index
    logger.info("[Step 1/7] AkShare Index Daily")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.AKSHARE_INDEX.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get('status')
        rows_proc = existing_status.get('rows_processed', 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 1/7] akshare_index already synced (status=success), skipping")
        results['akshare_index'] = {'status': 'success', 'rows': rows_proc, 'error': None, 'skipped': True}
    else:
        write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_akshare_index_step(target_date)
            write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, status.value, rows, err)
            results['akshare_index'] = {'status': status.value, 'rows': rows, 'error': err}
            logger.info("[Step 1/7] akshare_index: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 1/7] akshare_index failed: %s", e)
            write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, SyncStatus.ERROR.value, 0, str(e))
            results['akshare_index'] = {'status': 'error', 'rows': 0, 'error': str(e)}
            if not continue_on_error:
                return results
    
    # Step 2a: Stock basic
    logger.info("[Step 2a/7] Tushare Stock Basic")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get('status')
        rows_proc = existing_status.get('rows_processed', 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2a/7] tushare_stock_basic already synced (status=success), skipping")
        results['tushare_stock_basic'] = {'status': 'success', 'rows': rows_proc, 'error': None, 'skipped': True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_stock_basic_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, status.value, rows, err)
            results['tushare_stock_basic'] = {'status': status.value, 'rows': rows, 'error': err}
            logger.info("[Step 2a/7] tushare_stock_basic: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2a/7] tushare_stock_basic failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, SyncStatus.ERROR.value, 0, str(e))
            results['tushare_stock_basic'] = {'status': 'error', 'rows': 0, 'error': str(e)}
            if not continue_on_error:
                return results
    
    # Step 2b: Stock daily
    logger.info("[Step 2b/7] Tushare Stock Daily")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get('status')
        rows_proc = existing_status.get('rows_processed', 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2b/7] tushare_stock_daily already synced (status=success), skipping")
        results['tushare_stock_daily'] = {'status': 'success', 'rows': rows_proc, 'error': None, 'skipped': True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_stock_daily_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, status.value, rows, err)
            results['tushare_stock_daily'] = {'status': status.value, 'rows': rows, 'error': err}
            logger.info("[Step 2b/7] tushare_stock_daily: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2b/7] tushare_stock_daily failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, SyncStatus.ERROR.value, 0, str(e))
            results['tushare_stock_daily'] = {'status': 'error', 'rows': 0, 'error': str(e)}
            if not continue_on_error:
                return results
    
    # Step 2c: Adj factor
    logger.info("[Step 2c/7] Tushare Adj Factor")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get('status')
        rows_proc = existing_status.get('rows_processed', 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2c/7] tushare_adj_factor already synced (status=success), skipping")
        results['tushare_adj_factor'] = {'status': 'success', 'rows': rows_proc, 'error': None, 'skipped': True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_adj_factor_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, status.value, rows, err)
            results['tushare_adj_factor'] = {'status': status.value, 'rows': rows, 'error': err}
            logger.info("[Step 2c/7] tushare_adj_factor: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2c/7] tushare_adj_factor failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.ERROR.value, 0, str(e))
            results['tushare_adj_factor'] = {'status': 'error', 'rows': 0, 'error': str(e)}
            if not continue_on_error:
                return results
    
    # Step 2d: Dividend (daily mode - sampled)
    logger.info("[Step 2d/7] Tushare Dividend")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get('status')
        rows_proc = existing_status.get('rows_processed', 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2d/7] tushare_dividend already synced (status=success), skipping")
        results['tushare_dividend'] = {'status': 'success', 'rows': rows_proc, 'error': None, 'skipped': True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_dividend_step(target_date, use_batch=False)
            write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, status.value, rows, err)
            results['tushare_dividend'] = {'status': status.value, 'rows': rows, 'error': err}
            logger.info("[Step 2d/7] tushare_dividend: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2d/7] tushare_dividend failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.ERROR.value, 0, str(e))
            results['tushare_dividend'] = {'status': 'error', 'rows': 0, 'error': str(e)}
            if not continue_on_error:
                return results
    
    # Step 2e: Top10 holders
    logger.info("[Step 2e/7] Tushare Top10 Holders")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get('status')
        rows_proc = existing_status.get('rows_processed', 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 2e/7] tushare_top10_holders already synced (status=success), skipping")
        results['tushare_top10_holders'] = {'status': 'success', 'rows': rows_proc, 'error': None, 'skipped': True}
    else:
        write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_tushare_top10_holders_step(target_date)
            write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, status.value, rows, err)
            results['tushare_top10_holders'] = {'status': status.value, 'rows': rows, 'error': err}
            logger.info("[Step 2e/7] tushare_top10_holders: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 2e/7] tushare_top10_holders failed: %s", e)
            write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.ERROR.value, 0, str(e))
            results['tushare_top10_holders'] = {'status': 'error', 'rows': 0, 'error': str(e)}
            if not continue_on_error:
                return results
    
    # Step 3: VNPy sync
    logger.info("[Step 3/7] VNPy Sync")
    # Defensive check: skip if already successfully synced
    existing_status = get_step_status(target_date, SyncStep.VNPY_SYNC.value)
    status_val = None
    rows_proc = 0
    if isinstance(existing_status, str):
        status_val = existing_status
    elif isinstance(existing_status, dict):
        status_val = existing_status.get('status')
        rows_proc = existing_status.get('rows_processed', 0)
    if status_val == SyncStatus.SUCCESS.value:
        logger.info("[Step 3/7] vnpy_sync already synced (status=success), skipping")
        results['vnpy_sync'] = {'status': 'success', 'rows': rows_proc, 'error': None, 'skipped': True}
    else:
        write_step_status(target_date, SyncStep.VNPY_SYNC.value, SyncStatus.RUNNING.value)
        try:
            status, rows, err = run_vnpy_sync_step(target_date)
            write_step_status(target_date, SyncStep.VNPY_SYNC.value, status.value, rows, err)
            results['vnpy_sync'] = {'status': status.value, 'rows': rows, 'error': err}
            logger.info("[Step 3/7] vnpy_sync: %s (%d rows)", status.value, rows)
        except Exception as e:
            logger.exception("[Step 3/7] vnpy_sync failed: %s", e)
            write_step_status(target_date, SyncStep.VNPY_SYNC.value, SyncStatus.ERROR.value, 0, str(e))
            results['vnpy_sync'] = {'status': 'error', 'rows': 0, 'error': str(e)}
    
    logger.info("="*80)
    logger.info("Daily ingest finished for %s", target_date)
    logger.info("Results: %s", results)
    logger.info("="*80)
    
    return results


def missing_data_backfill(lookback_days: int = None):
    """Scan for failed/pending steps and backfill using batch APIs.
    
    Uses DB lock to prevent concurrent backfill jobs.
    """
    if lookback_days is None:
        lookback_days = LOOKBACK_DAYS
    
    # Check DB lock
    if is_backfill_locked():
        logger.warning("Backfill already running (DB locked), skipping this run")
        return
    
    # Acquire lock
    try:
        acquire_backfill_lock()
        logger.info("Acquired backfill lock")
    except Exception as e:
        logger.warning("Failed to acquire backfill lock: %s", e)
        return
    
    try:
        logger.info("Starting missing data backfill (lookback=%d days)", lookback_days)
        
        # Get failed steps
        failed = get_failed_steps(lookback_days)
        if not failed:
            logger.info("No failed steps to backfill")
            return
        
        logger.info("Found %d failed step entries", len(failed))
        
        # Group by step name and find contiguous date ranges
        by_step: Dict[str, List[date]] = {}
        for sync_date, step_name in failed:
            if step_name not in by_step:
                by_step[step_name] = []
            by_step[step_name].append(sync_date)
        
        for step_name, dates in by_step.items():
            dates_sorted = sorted(set(dates))
            logger.info("Backfilling %s: %d dates", step_name, len(dates_sorted))
            
            if step_name == SyncStep.TUSHARE_DIVIDEND.value:
                # Group into month ranges
                month_ranges = group_dates_by_month(dates_sorted)
                for start, end in month_ranges:
                    logger.info("  Backfilling dividend %s -> %s", start, end)
                    try:
                        ingest_dividend_by_date_range(
                            start.strftime('%Y-%m-%d'),
                            end.strftime('%Y-%m-%d'),
                            batch_size=BATCH_SIZE
                        )
                        # Mark all dates in range as success
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.SUCCESS.value)
                    except Exception as e:
                        logger.exception("Dividend backfill failed for %s->%s: %s", start, end, e)
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.ERROR.value, error_message=str(e))
            
            elif step_name == SyncStep.TUSHARE_TOP10_HOLDERS.value:
                month_ranges = group_dates_by_month(dates_sorted)
                for start, end in month_ranges:
                    logger.info("  Backfilling top10_holders %s -> %s", start, end)
                    try:
                        ingest_top10_holders_by_date_range(
                            start.strftime('%Y-%m-%d'),
                            end.strftime('%Y-%m-%d'),
                            batch_size=BATCH_SIZE
                        )
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.SUCCESS.value)
                    except Exception as e:
                        logger.exception("Top10 backfill failed for %s->%s: %s", start, end, e)
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.ERROR.value, error_message=str(e))
            
            elif step_name == SyncStep.TUSHARE_ADJ_FACTOR.value:
                month_ranges = group_dates_by_month(dates_sorted)
                for start, end in month_ranges:
                    logger.info("  Backfilling adj_factor %s -> %s", start, end)
                    try:
                        ingest_adj_factor_by_date_range(
                            start.strftime('%Y-%m-%d'),
                            end.strftime('%Y-%m-%d'),
                            batch_size=BATCH_SIZE
                        )
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.SUCCESS.value)
                    except Exception as e:
                        logger.exception("Adj factor backfill failed for %s->%s: %s", start, end, e)
                        for d in dates_sorted:
                            if start <= d <= end:
                                write_step_status(d, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.ERROR.value, error_message=str(e))
            
            else:
                # For other steps, retry one by one with daily APIs
                for d in dates_sorted:
                    logger.info("  Retrying %s for %s", step_name, d)
                    try:
                        daily_ingest(target_date=d, continue_on_error=True)
                    except Exception as e:
                        logger.exception("Daily retry failed for %s on %s: %s", step_name, d, e)
        
        logger.info("Backfill complete")
    
    finally:
        # Release lock
        try:
            release_backfill_lock()
            logger.info("Released backfill lock")
        except Exception as e:
            logger.warning("Failed to release backfill lock: %s", e)


def group_dates_by_month(dates: List[date]) -> List[Tuple[date, date]]:
    """Group dates into contiguous month ranges."""
    if not dates:
        return []
    
    ranges = []
    current_start = dates[0]
    current_end = dates[0]
    
    for d in dates[1:]:
        # Check if within same month or contiguous
        if (d.year == current_end.year and d.month == current_end.month) or \
           (d - current_end).days <= 31:
            current_end = d
        else:
            ranges.append((current_start, current_end))
            current_start = d
            current_end = d
    
    ranges.append((current_start, current_end))
    return ranges


def initialize_sync_status_table(lookback_years: int = 15):
    """Initialize data_sync_status table by scanning existing data."""
    logger.info("Initializing data_sync_status table (lookback=%d years)", lookback_years)
    
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=365 * lookback_years)

    # Get trade calendar
    trade_days = get_trade_calendar(start, end)
    logger.info("Processing %d trade dates from %s to %s", len(trade_days), start, end)

    if not trade_days:
        logger.info('No trade days found for range, exiting')
        return

    s = trade_days[0]
    e = trade_days[-1]

    # Get aggregated counts via DAO
    logger.info('Querying aggregated counts via DAO')
    stock_daily_counts = get_stock_daily_counts(s, e)
    adj_factor_counts = get_adj_factor_counts(s, e)
    vnpy_counts = get_vnpy_counts(s, e)

    # Build rows list
    rows_to_insert = []
    for td in trade_days:
        daily_count = stock_daily_counts.get(td, 0)
        status_daily = SyncStatus.SUCCESS if daily_count > 0 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.TUSHARE_STOCK_DAILY.value, status_daily.value, daily_count, None, None, None))

        adj_count = adj_factor_counts.get(td, 0)
        status_adj = SyncStatus.SUCCESS if adj_count > 0 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.TUSHARE_ADJ_FACTOR.value, status_adj.value, adj_count, None, None, None))

        vnpy_count = vnpy_counts.get(td, 0)
        status_vnpy = SyncStatus.SUCCESS if vnpy_count > 0 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.VNPY_SYNC.value, status_vnpy.value, vnpy_count, None, None, None))

        for step in (SyncStep.AKSHARE_INDEX, SyncStep.TUSHARE_DIVIDEND, SyncStep.TUSHARE_TOP10_HOLDERS):
            rows_to_insert.append((td, step.value, SyncStatus.PENDING.value, 0, None, None, None))

    processed = bulk_upsert_status(rows_to_insert)
    logger.info("Initialization complete: %d step statuses inserted for %d dates", processed, len(trade_days))


# =============================================================================
# Scheduler
# =============================================================================

def run_daily_job():
    """Job: Daily ingest at 2:00 AM Shanghai time."""
    logger.info("="*80)
    logger.info("Scheduled daily job triggered")
    logger.info("="*80)
    try:
        daily_ingest(continue_on_error=True)
    except Exception as e:
        logger.exception("Daily job failed: %s", e)


def run_backfill_job():
    """Job: Backfill every 6 hours with DB lock."""
    logger.info("="*80)
    logger.info("Scheduled backfill job triggered")
    logger.info("="*80)
    try:
        missing_data_backfill()
    except Exception as e:
        logger.exception("Backfill job failed: %s", e)


def run_daemon():
    """Run as daemon with scheduled jobs."""
    logger.info("="*80)
    logger.info("TraderMate Data Sync Daemon Starting")
    logger.info("Timezone: %s", TIMEZONE)
    logger.info("Daily ingest: %02d:%02d", SYNC_HOUR, SYNC_MINUTE)
    logger.info("Backfill: Every %d hours", BACKFILL_INTERVAL_HOURS)
    logger.info("="*80)
    
    # Ensure tables exist
    logger.info("[Startup] Verifying database tables...")
    try:
        ensure_tables()
    except Exception as e:
        logger.warning("Failed to ensure tables: %s", e)
    
    # Synchronously run daily ingest first
    logger.info("[Startup] Running daily ingest synchronously...")
    try:
        daily_ingest(continue_on_error=True)
    except Exception as e:
        logger.exception("[Startup] Daily ingest failed: %s", e)
    
    # Then run backfill
    logger.info("[Startup] Running backfill...")
    try:
        missing_data_backfill()
    except Exception as e:
        logger.exception("[Startup] Backfill failed: %s", e)
    
    logger.info("[Startup] Initialization complete. Entering scheduler loop...")
    logger.info("="*80)
    
    # Schedule jobs with Shanghai timezone
    try:
        shanghai_tz = ZoneInfo(TIMEZONE)
        
        # Daily job at 2:00 AM Shanghai time
        # Convert to local system time for scheduling
        now_shanghai = datetime.now(tz=shanghai_tz)
        now_local = datetime.now()
        tz_offset_hours = (now_shanghai.utcoffset().total_seconds() - now_local.astimezone().utcoffset().total_seconds()) / 3600
        
        local_hour = int(SYNC_HOUR - tz_offset_hours) % 24
        schedule_time = f"{local_hour:02d}:{SYNC_MINUTE:02d}"
        
        schedule.every().day.at(schedule_time).do(run_daily_job)
        logger.info("Scheduled daily job at %s (local time, %02d:%02d Shanghai time)", 
                   schedule_time, SYNC_HOUR, SYNC_MINUTE)
        
    except Exception as e:
        logger.warning("Timezone conversion failed, using system local time: %s", e)
        schedule.every().day.at(f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}").do(run_daily_job)
    
    # Backfill every 6 hours
    schedule.every(BACKFILL_INTERVAL_HOURS).hours.do(run_backfill_job)
    logger.info("Scheduled backfill job every %d hours", BACKFILL_INTERVAL_HOURS)
    
    # Run scheduler loop
    while True:
        schedule.run_pending()
        time.sleep(60)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description='TraderMate Data Sync Daemon')
    parser.add_argument('--daemon', action='store_true', help='Run as daemon with scheduler')
    parser.add_argument('--daily', action='store_true', help='Run daily ingest once')
    parser.add_argument('--backfill', action='store_true', help='Run backfill once')
    parser.add_argument('--init', action='store_true', help='Initialize sync status table')
    parser.add_argument('--lookback-days', type=int, default=LOOKBACK_DAYS, help='Backfill lookback days (used with --backfill)')
    parser.add_argument('--lookback-years', type=int, default=15, help='Init lookback years (used with --init)')
    parser.add_argument('--refresh-calendar', action='store_true', help='Refresh trade calendar')
    
    args = parser.parse_args()
    
    if args.init:
        initialize_sync_status_table(lookback_years=args.lookback_years)
    elif args.refresh_calendar:
        refresh_trade_calendar()
    elif args.daily:
        daily_ingest()
    elif args.backfill:
        missing_data_backfill(lookback_days=args.lookback_days)
    elif args.daemon:
        run_daemon()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()


# =============================================================================
# DataSyncDaemon Class (added to satisfy SyncStatusService dependency)
# =============================================================================

class DataSyncDaemon:
    """Minimal implementation to allow SyncStatusService to function.
    
    This is a temporary stub until full daemon class is implemented.
    """

    @staticmethod
    def find_missing_trade_dates(lookback_days: Optional[int] = None) -> List[date]:
        """
        Return missing trade dates for backfill.
        
        TODO: Implement actual missing date detection by querying sync logs.
        For now, return empty list (assumes no missing dates).
        """
        # Placeholder: in a real implementation, this would:
        # 1. Get list of expected trade dates for lookback period
        # 2. Compare with successfully synced dates from sync_log table
        # 3. Return dates that are missing
        return []
