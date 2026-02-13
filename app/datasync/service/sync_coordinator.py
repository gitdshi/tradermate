"""
Data Sync Coordinator - Refactored Architecture

Two main functions:
1. daily_ingest() - Incremental daily sync using daily APIs
2. missing_data_backfill() - Historical gap-filling using monthly batch APIs

Tracks granular step-level status in tradermate.data_sync_status table.
"""

import os
import time
import logging
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional, Dict
from enum import Enum

import pandas as pd
# Use DAO-provided engines and helpers; avoid direct SQLAlchemy imports in service layer
from app.domains.extdata.dao.data_sync_status_dao import engine_tm, engine_ak, engine_vn, text
from app.domains.extdata.dao.vnpy_dao import bulk_upsert_dbbardata as dao_bulk_upsert_dbbardata

# Import existing ingest functions
from app.datasync.service.tushare_ingest import (
    ingest_stock_basic,
    call_pro,
    upsert_daily,
    ingest_adj_factor,
    ingest_dividend,
    ingest_top10_holders,
    ingest_dividend_by_date_range,
    ingest_top10_holders_by_date_range,
    ingest_adj_factor_by_date_range,
    get_all_ts_codes,
    engine as tushare_engine,
)
from app.datasync.service.akshare_ingest import (
    ingest_index_daily as ak_ingest_index_daily,
    akshare_engine,
    INDEX_MAPPING,
)
from app.domains.extdata.dao.data_sync_status_dao import (
    ensure_tables,
    get_stock_daily_counts,
    get_adj_factor_counts,
    get_vnpy_counts,
    bulk_upsert_status,
    write_step_status,
    get_step_status,
    get_failed_steps,
    get_cached_trade_dates,
    upsert_trade_dates,
    get_stock_basic_count,
    get_adj_factor_count_for_date,
    get_stock_daily_ts_codes_for_date,
    truncate_trade_cal,
)
from app.domains.extdata.dao.tushare_dao import (
    upsert_dividend_df,
)
from app.domains.extdata.dao.tushare_dao import fetch_stock_daily_rows

try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False
    ak = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database URLs
TRADERMATE_DB_URL = os.getenv('TRADERMATE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/tradermate?charset=utf8mb4')
VNPY_DB_URL = os.getenv('VNPY_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/vnpy?charset=utf8mb4')

# Engines are provided by DAOs
tradermate_engine = engine_tm
vnpy_engine = engine_vn

# Config
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))


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
                # Cache via DAO bulk upsert
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
        # Use DAO to upsert fresh calendar (truncate then bulk insert via DAO)
        # Truncate cached calendar via DAO
        truncate_trade_cal()
        upsert_trade_dates([td.date() for td in df['trade_date']])
        logger.info('Refreshed trade calendar: %d dates cached', len(df))
    except Exception as e:
        logger.exception('Failed to refresh trade calendar: %s', e)


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
    For backfill mode: Use batch API by ann_date (no ts_code filter)
    """
    if use_batch:
        # Batch mode: fetch all dividends announced on this date
        try:
            target = sync_date.strftime('%Y%m%d')
            df = call_pro('dividend', ann_date=target)
            if df is None or df.empty:
                return SyncStatus.SUCCESS, 0, None
            
            # Insert rows
            from app.datasync.service.tushare_ingest import insert_sql
            # Use DAO to upsert dividend rows
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
    # Get symbols that have data for this date (via DAO)
    ts_codes = get_stock_daily_ts_codes_for_date(sync_date)
    
    if not ts_codes:
        return SyncStatus.SUCCESS, 0, 'No stock_daily data for this date'
    
    total_bars = 0
    total_symbols = 0
    for ts_code in ts_codes:
        try:
            bars = sync_symbol_to_vnpy(ts_code, start_date=sync_date)
            if bars > 0:
                total_bars += bars
                total_symbols += 1
        except Exception as e:
            logger.warning('VNPy sync failed for %s: %s', ts_code, e)
            continue
    
    if total_symbols > 0:
        return SyncStatus.SUCCESS, total_bars, None
    return SyncStatus.PARTIAL, 0, 'No symbols synced'


def sync_symbol_to_vnpy(ts_code: str, start_date: Optional[date] = None) -> int:
    """Sync a single symbol to vnpy.dbbardata."""
    symbol = ts_code.split('.')[0] if '.' in ts_code else ts_code
    suffix = ts_code.split('.')[-1] if '.' in ts_code else 'SZ'
    exchange = 'SSE' if suffix == 'SH' else 'SZSE'
    
    # Fetch rows from tushare via DAO
    rows = fetch_stock_daily_rows(ts_code, start_date)
    
    if not rows:
        return 0
    
    # Prepare rows for DAO bulk upsert into vnpy.dbbardata
    to_insert = []
    for row in rows:
        trade_date = row[0]
        dt = datetime.combine(trade_date, datetime.min.time())
        to_insert.append({
            'symbol': symbol,
            'exchange': exchange,
            'datetime': dt,
            'interval': 'd',
            'volume': float(row[5]) if row[5] else 0.0,
            'turnover': float(row[6]) if row[6] else 0.0,
            'open_interest': 0.0,
            'open_price': float(row[1]) if row[1] else 0.0,
            'high_price': float(row[2]) if row[2] else 0.0,
            'low_price': float(row[3]) if row[3] else 0.0,
            'close_price': float(row[4]) if row[4] else 0.0,
        })
    inserted = dao_bulk_upsert_dbbardata(to_insert)
    return inserted


# =============================================================================
# Main Functions
# =============================================================================

def daily_ingest(target_date: Optional[date] = None) -> Dict[str, Dict]:
    """Run incremental daily sync for a specific date.
    
    Steps:
    1. AkShare index daily
    2. Tushare stock_basic + stock_daily + adj_factor + dividend + top10_holders
    3. VNPy sync
    
    Returns dict with step results.
    """
    if target_date is None:
        # Get previous trade date
        trade_days = get_trade_calendar(date.today() - timedelta(days=10), date.today())
        target_date = trade_days[-1] if trade_days else date.today() - timedelta(days=1)
    
    logger.info("="*80)
    logger.info("Daily ingest starting for %s", target_date)
    logger.info("="*80)
    
    results = {}
    
    # Step 1: AkShare index
    logger.info("[Step 1/7] AkShare Index Daily")
    write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, SyncStatus.RUNNING.value)
    status, rows, err = run_akshare_index_step(target_date)
    write_step_status(target_date, SyncStep.AKSHARE_INDEX.value, status.value, rows, err)
    results['akshare_index'] = {'status': status.value, 'rows': rows, 'error': err}
    logger.info("[Step 1/7] akshare_index: %s (%d rows)", status.value, rows)
    
    # Step 2a: Stock basic
    logger.info("[Step 2a/7] Tushare Stock Basic")
    write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, SyncStatus.RUNNING.value)
    status, rows, err = run_tushare_stock_basic_step(target_date)
    write_step_status(target_date, SyncStep.TUSHARE_STOCK_BASIC.value, status.value, rows, err)
    results['tushare_stock_basic'] = {'status': status.value, 'rows': rows, 'error': err}
    logger.info("[Step 2a/7] tushare_stock_basic: %s (%d rows)", status.value, rows)
    
    # Step 2b: Stock daily
    logger.info("[Step 2b/7] Tushare Stock Daily")
    write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, SyncStatus.RUNNING.value)
    status, rows, err = run_tushare_stock_daily_step(target_date)
    write_step_status(target_date, SyncStep.TUSHARE_STOCK_DAILY.value, status.value, rows, err)
    results['tushare_stock_daily'] = {'status': status.value, 'rows': rows, 'error': err}
    logger.info("[Step 2b/7] tushare_stock_daily: %s (%d rows)", status.value, rows)
    
    # Step 2c: Adj factor
    logger.info("[Step 2c/7] Tushare Adj Factor")
    write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, SyncStatus.RUNNING.value)
    status, rows, err = run_tushare_adj_factor_step(target_date)
    write_step_status(target_date, SyncStep.TUSHARE_ADJ_FACTOR.value, status.value, rows, err)
    results['tushare_adj_factor'] = {'status': status.value, 'rows': rows, 'error': err}
    logger.info("[Step 2c/7] tushare_adj_factor: %s (%d rows)", status.value, rows)
    
    # Step 2d: Dividend (daily mode - sampled)
    logger.info("[Step 2d/7] Tushare Dividend")
    write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, SyncStatus.RUNNING.value)
    status, rows, err = run_tushare_dividend_step(target_date, use_batch=False)
    write_step_status(target_date, SyncStep.TUSHARE_DIVIDEND.value, status.value, rows, err)
    results['tushare_dividend'] = {'status': status.value, 'rows': rows, 'error': err}
    logger.info("[Step 2d/7] tushare_dividend: %s (%d rows)", status.value, rows)
    
    # Step 2e: Top10 holders
    logger.info("[Step 2e/7] Tushare Top10 Holders")
    write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, SyncStatus.RUNNING.value)
    status, rows, err = run_tushare_top10_holders_step(target_date)
    write_step_status(target_date, SyncStep.TUSHARE_TOP10_HOLDERS.value, status.value, rows, err)
    results['tushare_top10_holders'] = {'status': status.value, 'rows': rows, 'error': err}
    logger.info("[Step 2e/7] tushare_top10_holders: %s (%d rows)", status.value, rows)
    
    # Step 3: VNPy sync
    logger.info("[Step 3/7] VNPy Sync")
    write_step_status(target_date, SyncStep.VNPY_SYNC.value, SyncStatus.RUNNING.value)
    status, rows, err = run_vnpy_sync_step(target_date)
    write_step_status(target_date, SyncStep.VNPY_SYNC.value, status.value, rows, err)
    results['vnpy_sync'] = {'status': status.value, 'rows': rows, 'error': err}
    logger.info("[Step 3/7] vnpy_sync: %s (%d rows)", status.value, rows)
    
    logger.info("="*80)
    logger.info("Daily ingest finished for %s", target_date)
    logger.info("Results: %s", results)
    logger.info("="*80)
    
    return results


def missing_data_backfill(lookback_days: int = 60):
    """Scan for failed/pending steps and backfill using monthly batch APIs.
    
    For dividend/top10/adj_factor: uses date-range batch APIs
    For others: retries with daily APIs
    """
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
                    daily_ingest(target_date=d)
                except Exception as e:
                    logger.exception("Daily retry failed for %s on %s: %s", step_name, d, e)
    
    logger.info("Backfill complete")


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
    """Initialize data_sync_status table by scanning existing data.
    
    Infers historical completeness from row counts in existing tables.
    """
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

    # Use DAO to fetch aggregated counts
    logger.info('Querying aggregated counts via DAO')
    stock_daily_counts = get_stock_daily_counts(s, e)
    adj_factor_counts = get_adj_factor_counts(s, e)
    vnpy_counts = get_vnpy_counts(s, e)

    # Build rows list and call DAO for bulk insert
    rows_to_insert = []
    for td in trade_days:
        daily_count = stock_daily_counts.get(td, 0)
        status_daily = SyncStatus.SUCCESS if daily_count > 5000 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.TUSHARE_STOCK_DAILY.value, status_daily.value, daily_count, None, None, None))

        adj_count = adj_factor_counts.get(td, 0)
        status_adj = SyncStatus.SUCCESS if adj_count > 5000 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.TUSHARE_ADJ_FACTOR.value, status_adj.value, adj_count, None, None, None))

        vnpy_count = vnpy_counts.get(td, 0)
        status_vnpy = SyncStatus.SUCCESS if vnpy_count > 1000 else SyncStatus.PENDING
        rows_to_insert.append((td, SyncStep.VNPY_SYNC.value, status_vnpy.value, vnpy_count, None, None, None))

        for step in (SyncStep.AKSHARE_INDEX, SyncStep.TUSHARE_DIVIDEND, SyncStep.TUSHARE_TOP10_HOLDERS):
            rows_to_insert.append((td, step.value, SyncStatus.PENDING.value, 0, None, None, None))

    processed = bulk_upsert_status(rows_to_insert)
    logger.info("Initialization complete: %d step statuses inserted for %d dates", processed, len(trade_days))
