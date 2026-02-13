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
from typing import Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Engine is provided by the akshare DAO (DB URL moved to infrastructure.connections)
from app.domains.extdata.dao.akshare_dao import engine as akshare_engine  # type: ignore

# Rate limiting
DEFAULT_CALLS_PER_MIN = int(os.getenv('AKSHARE_CALLS_PER_MIN', '30'))

def _env_rate(name, default):
    try:
        return int(os.getenv(f'AKSHARE_RATE_{name}', str(default)))
    except Exception:
        return default

RATE_LIMITS = {
    'stock_zh_index_daily': _env_rate('stock_zh_index_daily', 60),
    # fallback
    '__default__': DEFAULT_CALLS_PER_MIN
}


def _min_interval_for(api_name: str) -> float:
    calls = RATE_LIMITS.get(api_name, RATE_LIMITS.get('__default__', DEFAULT_CALLS_PER_MIN))
    return 60.0 / max(1, int(calls))


def call_ak(api_name: str, fn, max_retries: int = 3, backoff_base: int = 5, **kwargs):
    """Call an AkShare function with per-endpoint rate limiting and retry/backoff."""
    if not hasattr(call_ak, '_last_call'):
        call_ak._last_call = {}

    # metrics hook
    if not hasattr(call_ak, '_metrics_hook'):
        call_ak._metrics_hook = None

    attempt = 0
    while attempt < max_retries:
        min_interval = _min_interval_for(api_name)
        last = call_ak._last_call.get(api_name, 0.0)
        elapsed = time.time() - last
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        try:
            start = time.time()
            res = fn(**kwargs)
            duration = time.time() - start
            call_ak._last_call[api_name] = time.time()
            hook = call_ak._metrics_hook
            if hook:
                try:
                    hook({
                        'api': api_name,
                        'attempt': attempt + 1,
                        'success': True,
                        'duration_s': duration,
                        'rate_config_calls_per_min': RATE_LIMITS.get(api_name, RATE_LIMITS.get('__default__')),
                        'next_allowed_in_s': _min_interval_for(api_name)
                    })
                except Exception:
                    logger.exception('metrics hook failed')
            return res
        except Exception as e:
            attempt += 1
            msg = str(e)
            is_rate = ('429' in msg) or ('频率' in msg) or ('rate limit' in msg.lower()) or ('限' in msg and '访问' in msg)
            duration = (time.time() - start) if 'start' in locals() else None
            if is_rate:
                sleep_time = backoff_base * (2 ** (attempt - 1))
                sleep_time = max(sleep_time, min_interval)
                logger.warning('AkShare rate-limit detected for %s: sleeping %ds (attempt %d/%d): %s', api_name, sleep_time, attempt, max_retries, msg)
                hook = call_ak._metrics_hook
                if hook:
                    try:
                        hook({
                            'api': api_name,
                            'attempt': attempt,
                            'success': False,
                            'rate_limited': True,
                            'duration_s': duration,
                            'rate_config_calls_per_min': RATE_LIMITS.get(api_name, RATE_LIMITS.get('__default__')),
                            'sleeping_for_s': sleep_time
                        })
                    except Exception:
                        logger.exception('metrics hook failed')
                time.sleep(sleep_time)
                continue
            logger.exception('AkShare API %s failed (attempt %d/%d): %s', api_name, attempt, max_retries, e)
            hook = call_ak._metrics_hook
            if hook:
                try:
                    hook({
                        'api': api_name,
                        'attempt': attempt,
                        'success': False,
                        'rate_limited': False,
                        'duration_s': duration,
                        'error': msg
                    })
                except Exception:
                    logger.exception('metrics hook failed')
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
            raise


def set_metrics_hook(fn):
    """Register a callable to receive metrics dicts for each AkShare API attempt."""
    call_ak._metrics_hook = fn


from app.domains.extdata.dao.akshare_dao import (
    audit_start,
    audit_finish,
    upsert_index_daily_rows,
)


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
        logger.info(f"Fetching index daily data for {symbol}...")
        # Fetch data from AkShare with per-endpoint rate limiting
        df = call_ak('stock_zh_index_daily', ak.stock_zh_index_daily, symbol=symbol)
        
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
        
        # Prepare rows and delegate upsert to DAO
        rows = []
        for _, row in df.iterrows():
            rows.append({
                'index_code': index_code,
                'trade_date': str(row['trade_date'])[:10],
                'open': float(row['open']) if pd.notna(row['open']) else None,
                'high': float(row['high']) if pd.notna(row['high']) else None,
                'low': float(row['low']) if pd.notna(row['low']) else None,
                'close': float(row['close']) if pd.notna(row['close']) else None,
                'volume': int(row['volume']) if pd.notna(row['volume']) else None,
                'amount': None
            })

        inserted = upsert_index_daily_rows(rows)
        logger.info('Ingested %d rows for index %s', inserted, index_code)
        audit_finish(audit_id, 'success', inserted)
        return inserted
        
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
