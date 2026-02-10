"""
AkShare Data Ingestion Service

This module provides functions to fetch financial data from AkShare API
and store it in the akshare database.

AkShare is a free, open-source financial data library that doesn't require
API tokens or membership levels.

Key APIs used:
- stock_zh_index_daily: Index daily data (HS300, SSE50, etc.)
"""

import os
import time
import logging
import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from typing import Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database URL
AKSHARE_DB_URL = os.getenv('AKSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1:3306/akshare?charset=utf8mb4')

# Create engine
akshare_engine = create_engine(AKSHARE_DB_URL, pool_pre_ping=True)

# Rate limiting
CALLS_PER_MIN = int(os.getenv('AKSHARE_CALLS_PER_MIN', '30'))
_MIN_INTERVAL = 60.0 / max(1, CALLS_PER_MIN)
_last_call = 0.0


def rate_limit():
    """Simple rate limiter for API calls."""
    global _last_call
    elapsed = time.time() - _last_call
    if elapsed < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - elapsed)
    _last_call = time.time()


def audit_start(api_name: str, params: dict) -> int:
    """Start an audit record for an ingestion operation."""
    import json
    with akshare_engine.begin() as conn:
        result = conn.execute(text(
            "INSERT INTO ingest_audit (api_name, params, status, fetched_rows) "
            "VALUES (:api, :params, 'running', 0)"
        ), {"api": api_name, "params": json.dumps(params)})
        return result.lastrowid


def audit_finish(audit_id: int, status: str, rows: int):
    """Finish an audit record."""
    with akshare_engine.begin() as conn:
        conn.execute(text(
            "UPDATE ingest_audit SET status=:status, fetched_rows=:rows, finished_at=NOW() WHERE id=:id"
        ), {"status": status, "rows": rows, "id": audit_id})


# ============================================================================
# INDEX DATA FUNCTIONS
# ============================================================================

# AkShare index symbol mapping (to tushare-style codes)
INDEX_MAPPING = {
    'sh000300': '399300.SZ',   # HS300 (use SZ code for consistency with your DB)
    'sh000001': '000001.SH',   # SSE Composite
    'sz399001': '399001.SZ',   # SZSE Component
    'sh000016': '000016.SH',   # SSE 50
    'sh000905': '000905.SH',   # CSI 500
    'sh000852': '000852.SH',   # CSI 1000
}


def ingest_index_daily(symbol: str = 'sh000300', start_date: str = None) -> int:
    """
    Fetch index daily data from AkShare and store in akshare.index_daily.
    
    Args:
        symbol: AkShare index symbol (e.g., 'sh000300' for HS300)
        start_date: Start date in YYYY-MM-DD format (optional, fetches all if None)
    
    Returns:
        Number of rows ingested
    """
    params = {'symbol': symbol, 'start_date': start_date}
    audit_id = audit_start('index_daily', params)
    
    try:
        rate_limit()
        logger.info(f"Fetching index daily data for {symbol}...")
        
        # Fetch data from AkShare
        df = ak.stock_zh_index_daily(symbol=symbol)
        
        if df is None or df.empty:
            logger.warning(f"No data returned for {symbol}")
            audit_finish(audit_id, 'success', 0)
            return 0
        
        # Convert to standard format
        index_code = INDEX_MAPPING.get(symbol, symbol.upper())
        
        # Rename columns to match schema
        df = df.rename(columns={
            'date': 'trade_date',
            'volume': 'volume'
        })
        
        # Add index_code column
        df['index_code'] = index_code
        
        # Filter by start_date if provided
        if start_date:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df[df['trade_date'] >= start_date]
        
        # Convert trade_date to string for SQL
        df['trade_date'] = df['trade_date'].astype(str)
        
        # Upsert to database
        rows = 0
        upsert_sql = text("""
            INSERT INTO index_daily (index_code, trade_date, open, high, low, close, volume, amount)
            VALUES (:index_code, :trade_date, :open, :high, :low, :close, :volume, :amount)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), low=VALUES(low), 
                close=VALUES(close), volume=VALUES(volume), amount=VALUES(amount)
        """)
        
        with akshare_engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(upsert_sql, {
                    'index_code': index_code,
                    'trade_date': str(row['trade_date'])[:10],
                    'open': float(row['open']) if pd.notna(row['open']) else None,
                    'high': float(row['high']) if pd.notna(row['high']) else None,
                    'low': float(row['low']) if pd.notna(row['low']) else None,
                    'close': float(row['close']) if pd.notna(row['close']) else None,
                    'volume': int(row['volume']) if pd.notna(row['volume']) else None,
                    'amount': None  # AkShare doesn't provide amount for indexes
                })
                rows += 1
        
        logger.info(f"Ingested {rows} rows for index {index_code}")
        audit_finish(audit_id, 'success', rows)
        return rows
        
    except Exception as e:
        logger.exception(f"Error ingesting index {symbol}: {e}")
        audit_finish(audit_id, 'error', 0)
        raise


def ingest_all_indexes() -> dict:
    """Ingest daily data for all major indexes."""
    results = {}
    for ak_symbol, ts_code in INDEX_MAPPING.items():
        try:
            rows = ingest_index_daily(symbol=ak_symbol)
            results[ts_code] = {'status': 'success', 'rows': rows}
        except Exception as e:
            results[ts_code] = {'status': 'error', 'error': str(e)}
        time.sleep(1)  # Be nice to the API
    return results


# ============================================================================
# (stock ingestion removed — AkShare ingestion limited to index_daily)


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main CLI entry point."""
    import sys
    
    if len(sys.argv) < 2:
        print("""
AkShare Data Ingestion Tool

Usage:
    python akshare_ingest.py <command> [args]

Commands:
    index <symbol>      Ingest index daily data (default: sh000300 for HS300)
    index_all           Ingest all major indexes
    
Examples:
    python akshare_ingest.py index sh000300
    python akshare_ingest.py index_all
        """)
        return
    
    cmd = sys.argv[1]
    
    if cmd == 'index':
        symbol = sys.argv[2] if len(sys.argv) > 2 else 'sh000300'
        rows = ingest_index_daily(symbol=symbol)
        print(f"✅ Ingested {rows} rows for {symbol}")
        
    elif cmd == 'index_all':
        results = ingest_all_indexes()
        for code, res in results.items():
            status = '✅' if res['status'] == 'success' else '❌'
            print(f"{status} {code}: {res}")
        
    else:
        print(f"Unknown command: {cmd}")


if __name__ == '__main__':
    main()
