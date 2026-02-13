#!/usr/bin/env python3
"""
Validate and repair data_sync_status table.

Checks actual data presence in tushare/akshare/vnpy databases and updates
data_sync_status to reflect reality. This ensures backfill can work correctly.

Usage:
    PYTHONPATH=. python3 scripts/validate_sync_status.py --days 60
    PYTHONPATH=. python3 scripts/validate_sync_status.py --days 60 --fix
"""
import sys
import os
import argparse
from datetime import date, timedelta
from typing import List, Tuple

# Disable logging to prevent daemon startup
os.environ['LOG_LEVEL'] = 'ERROR'

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text, create_engine

# Read connection URLs from environment
TRADERMATE_DB_URL = os.getenv('TRADERMATE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/tradermate?charset=utf8mb4')
TUSHARE_DB_URL = os.getenv('TUSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8mb4')
AKSHARE_DB_URL = os.getenv('AKSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/akshare?charset=utf8mb4')
VNPY_DB_URL = os.getenv('VNPY_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/vnpy?charset=utf8mb4')

# Create engines directly
engine_tm = create_engine(TRADERMATE_DB_URL, pool_pre_ping=True)
engine_ts = create_engine(TUSHARE_DB_URL, pool_pre_ping=True)
engine_ak = create_engine(AKSHARE_DB_URL, pool_pre_ping=True)
engine_vn = create_engine(VNPY_DB_URL, pool_pre_ping=True)

# Import AkShare for trade calendar
try:
    import akshare as ak
    AKSHARE_AVAILABLE = True
except ImportError:
    AKSHARE_AVAILABLE = False


def get_trade_dates(start: date, end: date) -> List[date]:
    """Get trade dates from cached calendar or AkShare."""
    # Try cached calendar first
    try:
        with engine_ak.connect() as conn:
            res = conn.execute(text("""
                SELECT trade_date FROM trade_cal
                WHERE trade_date BETWEEN :s AND :e AND is_trade_day = 1
                ORDER BY trade_date ASC
            """), {'s': start, 'e': end})
            dates = [row[0] for row in res.fetchall()]
            if dates:
                return dates
    except Exception:
        pass
    
    if AKSHARE_AVAILABLE:
        try:
            import pandas as pd
            df = ak.tool_trade_date_hist_sina()
            if df is not None and not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                mask = (df['trade_date'].dt.date >= start) & (df['trade_date'].dt.date <= end)
                return df[mask]['trade_date'].dt.date.tolist()
        except Exception as e:
            print(f"Warning: AkShare trade calendar failed: {e}")
    
    # Fallback to weekdays
    days = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def check_akshare_index(trade_date: date) -> Tuple[bool, int]:
    """Check if akshare index_daily has data for date."""
    with engine_ak.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM index_daily WHERE trade_date = :d"
        ), {'d': trade_date})
        count = result.scalar() or 0
        return (count > 0, count)


def check_tushare_stock_basic() -> Tuple[bool, int]:
    """Check if tushare has stock_basic data."""
    with engine_ts.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stock_basic"))
        count = result.scalar() or 0
        return (count > 0, count)


def check_tushare_stock_daily(trade_date: date) -> Tuple[bool, int]:
    """Check if tushare stock_daily has data for date."""
    with engine_ts.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM stock_daily WHERE trade_date = :d"
        ), {'d': trade_date})
        count = result.scalar() or 0
        return (count > 0, count)


def check_tushare_adj_factor(trade_date: date) -> Tuple[bool, int]:
    """Check if tushare adj_factor has data for date."""
    with engine_ts.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM adj_factor WHERE trade_date = :d"
        ), {'d': trade_date})
        count = result.scalar() or 0
        return (count > 0, count)


def check_tushare_dividend(trade_date: date) -> Tuple[bool, int]:
    """Check if tushare dividend has data announced on date."""
    with engine_ts.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM stock_dividend WHERE ann_date = :d"
        ), {'d': trade_date})
        count = result.scalar() or 0
        # Dividend can have 0 rows and still be successful
        return (True, count)


def check_tushare_top10_holders(trade_date: date) -> Tuple[bool, int]:
    """Check if tushare top10_holders has data for date."""
    with engine_ts.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM top10_holders WHERE end_date = :d"
        ), {'d': trade_date})
        count = result.scalar() or 0
        # top10_holders can have 0 rows and still be successful
        return (True, count)


def check_vnpy_sync(trade_date: date) -> Tuple[bool, int]:
    """Check if vnpy dbbardata has data for date."""
    with engine_vn.connect() as conn:
        result = conn.execute(text(
            "SELECT COUNT(*) FROM dbbardata WHERE DATE(`datetime`) = :d"
        ), {'d': trade_date})
        count = result.scalar() or 0
        return (count > 0, count)


def get_step_status(sync_date: date, step_name: str):
    """Get status for a sync step."""
    with engine_tm.connect() as conn:
        res = conn.execute(text(
            "SELECT status FROM data_sync_status WHERE sync_date = :sd AND step_name = :step"
        ), {'sd': sync_date, 'step': step_name})
        row = res.fetchone()
        return row[0] if row else None


def write_step_status(sync_date: date, step_name: str, status: str, rows_synced: int, error_message):
    """Write or update a sync step status."""
    with engine_tm.begin() as conn:
        conn.execute(text("""
            INSERT INTO data_sync_status 
                (sync_date, step_name, status, rows_synced, error_message, started_at, finished_at)
            VALUES (:sd, :step, :status, :rows, :err, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                rows_synced = VALUES(rows_synced),
                error_message = VALUES(error_message),
                finished_at = VALUES(finished_at),
                updated_at = CURRENT_TIMESTAMP
        """), {
            'sd': sync_date,
            'step': step_name,
            'status': status,
            'rows': rows_synced,
            'err': error_message
        })


STEP_CHECKERS = {
    'akshare_index': check_akshare_index,
    'tushare_stock_daily': check_tushare_stock_daily,
    'tushare_adj_factor': check_tushare_adj_factor,
    'tushare_dividend': check_tushare_dividend,
    'tushare_top10_holders': check_tushare_top10_holders,
    'vnpy_sync': check_vnpy_sync,
}


def validate_and_fix(start_date: date, end_date: date, fix: bool = False):
    """Validate data_sync_status against actual data and optionally fix."""
    # Get trade dates
    trade_dates = get_trade_dates(start_date, end_date)
    print(f"Validating {len(trade_dates)} trade dates from {start_date} to {end_date}")
    
    # Check stock_basic once (not date-specific)
    stock_basic_exists, stock_basic_count = check_tushare_stock_basic()
    print(f"\nStock Basic: {stock_basic_count} records")
    
    # Track discrepancies
    discrepancies = []
    total_checks = 0
    total_fixed = 0
    
    for trade_date in trade_dates:
        print(f"\n--- Checking {trade_date} ---")
        
        for step_name, checker in STEP_CHECKERS.items():
            total_checks += 1
            
            # Get actual data status
            has_data, count = checker(trade_date)
            
            # Get recorded status
            recorded_status = get_step_status(trade_date, step_name)
            
            # Determine expected status
            if has_data and count > 0:
                expected_status = 'success'
            elif step_name in ['tushare_dividend', 'tushare_top10_holders']:
                # These steps can have 0 rows but still be successful
                expected_status = 'success'
            else:
                expected_status = None  # No data = no status (or should be pending/error)
            
            # Check for discrepancy
            if recorded_status != expected_status:
                discrepancy_type = f"{recorded_status or 'NULL'} -> {expected_status or 'NULL'}"
                discrepancies.append((trade_date, step_name, recorded_status, expected_status, count))
                print(f"  ❌ {step_name}: {discrepancy_type} (actual count: {count})")
                
                # Fix if requested
                if fix and expected_status:
                    try:
                        write_step_status(trade_date, step_name, expected_status, count, None)
                        total_fixed += 1
                        print(f"     ✓ Fixed to '{expected_status}'")
                    except Exception as e:
                        print(f"     ✗ Fix failed: {e}")
            else:
                print(f"  ✓ {step_name}: {recorded_status} (count: {count})")
        
        # Check stock_basic status for this date
        recorded_basic = get_step_status(trade_date, 'tushare_stock_basic')
        expected_basic = 'success' if stock_basic_exists else None
        if recorded_basic != expected_basic:
            discrepancies.append((trade_date, 'tushare_stock_basic', recorded_basic, expected_basic, stock_basic_count))
            print(f"  ❌ tushare_stock_basic: {recorded_basic or 'NULL'} -> {expected_basic or 'NULL'}")
            if fix and expected_basic:
                try:
                    write_step_status(trade_date, 'tushare_stock_basic', expected_basic, stock_basic_count, None)
                    total_fixed += 1
                    print(f"     ✓ Fixed to '{expected_basic}'")
                except Exception as e:
                    print(f"     ✗ Fix failed: {e}")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"Validation Summary:")
    print(f"  Total checks: {total_checks}")
    print(f"  Discrepancies: {len(discrepancies)}")
    if fix:
        print(f"  Fixed: {total_fixed}")
    print(f"{'='*60}")
    
    if discrepancies and not fix:
        print("\nRun with --fix to update data_sync_status")
    
    return discrepancies


def main():
    parser = argparse.ArgumentParser(description="Validate and repair data_sync_status")
    parser.add_argument('--days', type=int, default=60, help='Number of days to check (default: 60)')
    parser.add_argument('--fix', action='store_true', help='Fix discrepancies (update data_sync_status)')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    
    if args.start and args.end:
        from datetime import datetime
        start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
    else:
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
    
    validate_and_fix(start_date, end_date, fix=args.fix)


if __name__ == '__main__':
    main()
