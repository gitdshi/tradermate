"""
Import/Sync Tushare data to vnpy database.

This script reads from the tushare_data.stock_daily table and syncs bars
into vnpy_data.dbbardata table so they are visible in the Data Manager GUI.

Usage:
    python tradermate/scripts/import_tushare_to_vnpy.py --symbol 000001.SZ
    python tradermate/scripts/import_tushare_to_vnpy.py --all          # Sync all symbols
    python tradermate/scripts/import_tushare_to_vnpy.py --full-refresh # Re-sync all data
"""

import os
import sys
import argparse
from datetime import datetime, timedelta, date
from typing import List, Optional

# Add project root to path
sys.path.insert(0, str(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) )

from sqlalchemy import text
from app.infrastructure.db.connections import get_tushare_engine, get_vnpy_engine

# Exchange mapping
EXCHANGE_MAP = {
    'SZ': 'SZSE',
    'SH': 'SSE',
}


def map_exchange(ts_code: str) -> str:
    """Map tushare ts_code suffix to vnpy Exchange."""
    suffix = ts_code.split('.')[-1] if '.' in ts_code else 'SZ'
    return EXCHANGE_MAP.get(suffix, 'SZSE')


def get_symbol(ts_code: str) -> str:
    """Extract symbol from ts_code."""
    return ts_code.split('.')[0] if '.' in ts_code else ts_code


def get_all_ts_codes(tushare_engine) -> List[str]:
    """Get all unique ts_codes from tushare_data.stock_daily."""
    with tushare_engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT ts_code FROM stock_daily ORDER BY ts_code"))
        return [row[0] for row in result]


def get_last_sync_date(vnpy_engine, symbol: str, exchange: str, interval: str = 'd') -> Optional[date]:
    """Get the last synced date for a symbol from vnpy_data."""
    with vnpy_engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(`datetime`) FROM dbbardata 
            WHERE symbol = :symbol AND exchange = :exchange AND `interval` = :interval
        """), {'symbol': symbol, 'exchange': exchange, 'interval': interval})
        row = result.fetchone()
        if row and row[0]:
            return row[0].date() if hasattr(row[0], 'date') else row[0]
        return None


def sync_symbol(ts_code: str, tushare_engine, vnpy_engine, 
                start_date: Optional[date] = None, end_date: Optional[str] = None) -> int:
    """
    Sync a single symbol from tushare_data to vnpy_data.
    
    Returns: Number of bars synced
    """
    symbol = get_symbol(ts_code)
    exchange = map_exchange(ts_code)
    interval = 'd'
    
    # Build query
    query = """
        SELECT trade_date, open, high, low, close, vol, amount 
        FROM stock_daily 
        WHERE ts_code = :ts_code
    """
    params = {'ts_code': ts_code}
    
    if start_date:
        query += " AND trade_date >= :start_date"
        params['start_date'] = start_date
    if end_date:
        query += " AND trade_date <= :end_date"
        params['end_date'] = end_date
    
    query += " ORDER BY trade_date ASC"
    
    # Fetch from tushare_data
    with tushare_engine.connect() as conn:
        result = conn.execute(text(query), params)
        rows = result.fetchall()
    
    if not rows:
        return 0
    
    # Insert into vnpy_data
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
    with vnpy_engine.begin() as conn:
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
                'volume': float(row[5]) if row[5] else 0.0,
                'turnover': float(row[6]) if row[6] else 0.0,
                'open_interest': 0.0,
                'open_price': float(row[1]) if row[1] else 0.0,
                'high_price': float(row[2]) if row[2] else 0.0,
                'low_price': float(row[3]) if row[3] else 0.0,
                'close_price': float(row[4]) if row[4] else 0.0,
            })
            synced += 1
    
    return synced


def update_bar_overview(vnpy_engine, symbol: str, exchange: str, interval: str = 'd'):
    """Update the bar overview after sync."""
    with vnpy_engine.begin() as conn:
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


def main():
    parser = argparse.ArgumentParser(description="Sync Tushare data to vnpy database")
    parser.add_argument('--symbol', type=str, help='Single symbol to sync (e.g., 000001.SZ)')
    parser.add_argument('--all', action='store_true', help='Sync all symbols')
    parser.add_argument('--start', type=str, default=None, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=None, help='End date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of symbols')
    parser.add_argument('--full-refresh', action='store_true', help='Full refresh (ignore last sync)')
    args = parser.parse_args()
    
    if not args.symbol and not args.all:
        print("Please specify --symbol or --all")
        return
    
    # Create engines via infrastructure connections
    tushare_engine = get_tushare_engine()
    vnpy_engine = get_vnpy_engine()
    
    try:
        print(f"Tushare DB: {tushare_engine.url}")
    except Exception:
        pass
    try:
        print(f"VNPY DB: {vnpy_engine.url}")
    except Exception:
        pass
    
    total_bars = 0
    total_symbols = 0
    
    if args.symbol:
        ts_codes = [args.symbol]
    else:
        ts_codes = get_all_ts_codes(tushare_engine)
        if args.limit:
            ts_codes = ts_codes[:args.limit]
    
    print(f"Found {len(ts_codes)} symbols to sync")
    
    for i, ts_code in enumerate(ts_codes, 1):
        try:
            symbol = get_symbol(ts_code)
            exchange = map_exchange(ts_code)
            
            if args.full_refresh or args.start:
                start_date = datetime.strptime(args.start, '%Y-%m-%d').date() if args.start else None
            else:
                last_sync = get_last_sync_date(vnpy_engine, symbol, exchange)
                start_date = last_sync + timedelta(days=1) if last_sync else None
            
            print(f"[{i}/{len(ts_codes)}] Syncing {ts_code} (from {start_date or 'beginning'})...")
            
            count = sync_symbol(ts_code, tushare_engine, vnpy_engine, start_date, args.end)
            
            if count > 0:
                update_bar_overview(vnpy_engine, symbol, exchange)
                total_bars += count
                total_symbols += 1
                print(f"  ✓ Synced {count} bars")
            else:
                print(f"  - No new data")
                
        except Exception as e:
            print(f"  ✗ Error: {e}")
        
        if i % 100 == 0:
            print(f"Progress: {i}/{len(ts_codes)} symbols, {total_bars} bars synced")
    
    print(f"\n=== Sync Complete ===")
    print(f"Symbols synced: {total_symbols}")
    print(f"Total bars: {total_bars}")
    
    # Verify
    with vnpy_engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM dbbaroverview"))
        overview_count = result.fetchone()[0]
        print(f"Bar overviews in database: {overview_count}")


if __name__ == '__main__':
    main()
