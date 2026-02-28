import os
import json
import time
import logging
import pandas as pd
import tushare as ts
import numpy as np

logging.basicConfig(level=logging.INFO)

# Tushare data is stored in the tushare database
# Must be provided via TUSHARE_DATABASE_URL environment variable
TUSHARE_DB_URL = os.getenv('TUSHARE_DATABASE_URL')
if not TUSHARE_DB_URL:
    raise ValueError("TUSHARE_DATABASE_URL must be set")
TS_TOKEN = os.getenv('TUSHARE_TOKEN', '')

# Use engine from tushare DAO
# Bring in helpers and SQL `text` from DAO modules to avoid direct SQLAlchemy imports in service layer
from app.domains.extdata.dao.tushare_dao import (
    engine as engine,
    audit_start,
    audit_finish,
    upsert_daily,
    upsert_financial_statement,
    upsert_daily_basic,
    upsert_adj_factor,
    upsert_moneyflow,
    upsert_top10_holders,
    upsert_margin,
    upsert_block_trade,
    upsert_dividend_df,
    upsert_index_daily_df,
    get_all_ts_codes as dao_get_all_ts_codes,
    get_max_trade_date as dao_get_max_trade_date,
)
from app.domains.extdata.dao.data_sync_status_dao import text  # reuse text() from DAO
# Tushare pro API
pro = ts.pro_api(TS_TOKEN) if TS_TOKEN else ts.pro_api()

# DAO for Tushare DB operations
# DAO imports are above (engine included)

# Rate limiting configuration: max calls per minute to Tushare API
DEFAULT_CALLS_PER_MIN = int(os.getenv('TUSHARE_CALLS_PER_MIN', '50'))

# Per-endpoint calls-per-minute overrides. Adjust as needed via env vars if desired.
# Example: TUSHARE_RATE_daily=60
def _env_rate(name, default):
    try:
        return int(os.getenv(f'TUSHARE_RATE_{name}', str(default)))
    except Exception:
        return default

RATE_LIMITS = {
    'daily': _env_rate('daily', 60),
    'index_daily': _env_rate('index_daily', 30),
    'stock_basic': _env_rate('stock_basic', 5),
    'adj_factor': _env_rate('adj_factor', 10),
    'dividend': _env_rate('dividend', 10),
    'top10_holders': _env_rate('top10_holders', 10),
    'daily_basic': _env_rate('daily_basic', 60),
    # fallback default
    '__default__': DEFAULT_CALLS_PER_MIN
}


def _min_interval_for(api_name: str) -> float:
    calls = RATE_LIMITS.get(api_name, RATE_LIMITS.get('__default__', DEFAULT_CALLS_PER_MIN))
    return 60.0 / max(1, int(calls))


def call_pro(api_name: str, max_retries: int = None, backoff_base: int = 5, **kwargs):
    """Wrapper around `pro.<api_name>(**kwargs)` that enforces a simple per-minute rate limit
    (spacing calls by at least `_MIN_INTERVAL`) and retries on transient errors including
    Tushare rate-limit responses. Returns the DataFrame from the API call or raises on final failure.
    """
    if max_retries is None:
        max_retries = int(os.getenv('MAX_RETRIES', '3'))
    # metrics hook (callable) can be set via set_metrics_hook
    if not hasattr(call_pro, '_metrics_hook'):
        call_pro._metrics_hook = None

    # per-endpoint last-call timestamps
    if not hasattr(call_pro, '_last_call'):
        call_pro._last_call = {}

    start_time = None
    attempt = 0
    while attempt < max_retries:
        # enforce per-endpoint spacing between calls
        min_interval = _min_interval_for(api_name)
        last = call_pro._last_call.get(api_name, 0.0)
        elapsed = time.time() - last
        if elapsed < min_interval:
            to_sleep = min_interval - elapsed
            logging.debug('Sleeping %.3fs to respect %s rate limit', to_sleep, api_name)
            time.sleep(to_sleep)
        try:
            start_time = time.time()
            func = getattr(pro, api_name, None)
            if func is None:
                raise AttributeError(f"Tushare pro has no api '{api_name}'")
            # filter out None kwargs to avoid sending empty params
            call_kwargs = {k: v for k, v in kwargs.items() if v is not None}
            df = func(**call_kwargs)
            call_pro._last_call[api_name] = time.time()
            duration = time.time() - start_time
            rows = 0
            if df is not None:
                try:
                    rows = len(df)
                except Exception:
                    rows = 0
            if call_pro._metrics_hook:
                try:
                    call_pro._metrics_hook(api_name, True, duration, rows)
                except Exception:
                    logging.exception('metrics hook failed for %s', api_name)
            return df
        except Exception as e:
            duration = time.time() - (start_time or time.time())
            if call_pro._metrics_hook:
                try:
                    call_pro._metrics_hook(api_name, False, duration, 0, error=str(e))
                except Exception:
                    logging.exception('metrics hook failed for %s', api_name)
            attempt += 1
            logging.exception('call_pro %s attempt %d failed: %s', api_name, attempt, e)
            if attempt >= max_retries:
                logging.error('call_pro %s exhausted retries', api_name)
                raise
            to_sleep = backoff_base * attempt
            logging.info('Sleeping %.1fs before retrying %s (attempt %d)', to_sleep, api_name, attempt)
            time.sleep(to_sleep)


def set_metrics_hook(fn):
    """Set an optional metrics hook callable: fn(api_name, success:bool, duration:float, rows:int, **extra)"""
    call_pro._metrics_hook = fn


def store_financial_statement(df: pd.DataFrame, statement_type: str):
    return upsert_financial_statement(df, statement_type)


def ingest_index_daily(ts_code=None, start_date=None, end_date=None):
    """Ingest index daily data (e.g., HS300 399300.SZ) from Tushare."""
    params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('index_daily', params)
    max_retries = int(os.getenv('MAX_RETRIES', '3'))
    attempt = 0
    while attempt < max_retries:
        try:
            df = call_pro('index_daily', ts_code=ts_code, start_date=start_date, end_date=end_date)
            rows = 0
            if df is not None and not df.empty:
                # Rename ts_code to index_code for consistency with table schema
                df = df.rename(columns={'ts_code': 'index_code'})
                # Delegate index_daily upsert to DAO
                rows = upsert_index_daily_df(df)
            audit_finish(aid, 'success', rows)
            logging.info('Ingested index_daily rows: %d for %s', rows, ts_code)
            return rows
        except Exception as e:
            attempt += 1
            logging.exception('index_daily ingest attempt %d failed for %s: %s', attempt, ts_code, e)
            if attempt < max_retries:
                logging.info('Sleeping 5s before retrying...')
                time.sleep(5)
            else:
                audit_finish(aid, 'error', 0)
                return 0


def ingest_daily(ts_code=None, start_date=None, end_date=None):
    params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('daily', params)
    max_retries = int(os.getenv('MAX_RETRIES', '3'))
    attempt = 0
    while attempt < max_retries:
        try:
            df = call_pro('daily', ts_code=ts_code, start_date=start_date, end_date=end_date)
            rows = upsert_daily(df)
            audit_finish(aid, 'success', rows)
            logging.info('Ingested daily rows: %d', rows)
            return
        except Exception as e:
            attempt += 1
            logging.exception('daily ingest attempt %d failed for %s: %s', attempt, ts_code, e)
            if attempt < max_retries:
                logging.info('Sleeping 5s before retrying...')
                time.sleep(5)
            else:
                audit_finish(aid, 'error', 0)
                return


def ingest_daily_basic(ts_code=None, trade_date=None, start_date=None, end_date=None):
    params = {'ts_code': ts_code, 'trade_date': trade_date, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('daily_basic', params)
    try:
        df = call_pro('daily_basic', ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        # write to daily_basic table via DAO
        rows = upsert_daily_basic(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested daily_basic rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('daily_basic ingest failed: %s', e)


def ingest_adj_factor(ts_code=None, trade_date=None, start_date=None, end_date=None):
    params = {'ts_code': ts_code, 'trade_date': trade_date, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('adj_factor', params)
    try:
        df = call_pro('adj_factor', ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date)
        rows = upsert_adj_factor(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested adj_factor rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('adj_factor ingest failed: %s', e)


def ingest_income(ts_code, start_date=None, end_date=None):
    params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('income', params)
    try:
        df = call_pro('income', ts_code=ts_code, start_date=start_date, end_date=end_date)
        rows = store_financial_statement(df, 'income')
        audit_finish(aid, 'success', rows)
        logging.info('Stored income rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('income ingest failed: %s', e)


def ingest_moneyflow(ts_code=None, start_date=None, end_date=None):
    params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('moneyflow', params)
    try:
        df = call_pro('moneyflow', ts_code=ts_code, start_date=start_date, end_date=end_date)
        rows = upsert_moneyflow(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested moneyflow rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('moneyflow ingest failed: %s', e)


def ingest_dividend(ts_code=None):
    params = {'ts_code': ts_code}
    aid = audit_start('dividend', params)
    try:
        df = call_pro('dividend', ts_code=ts_code)
        rows = 0
        if df is not None and not df.empty:
            # Parse dates and fill missing ann_date with imp_ann_date
            try:
                df['ann_date'] = pd.to_datetime(df.get('ann_date'), errors='coerce')
                df['imp_ann_date'] = pd.to_datetime(df.get('imp_ann_date'), errors='coerce')
                # Fill ann_date with imp_ann_date when missing
                df['ann_date'] = df['ann_date'].fillna(df['imp_ann_date'])
                # Drop rows where ann_date is still missing (cannot dedupe without a key)
                missing = df['ann_date'].isna().sum()
                if missing:
                    logging.warning('ingest_dividend: %d rows missing ann_date and imp_ann_date, skipping', missing)
                    df = df.dropna(subset=['ann_date'])
            except Exception:
                logging.exception('Failed to normalize dividend dates, proceeding with original dataframe')

            if df is not None and not df.empty:
                # Convert pandas timestamps/NaT to Python date or None so DAO doesn't send NaT to DB
                def _to_date_or_none(x):
                    try:
                        if pd.isna(x):
                            return None
                    except Exception:
                        pass
                    if isinstance(x, pd.Timestamp):
                        return x.date()
                    if hasattr(x, 'date'):
                        try:
                            return x.date()
                        except Exception:
                            return None
                    try:
                        return pd.to_datetime(x).date()
                    except Exception:
                        return None

                df['ann_date'] = df['ann_date'].apply(_to_date_or_none)
                df['imp_ann_date'] = df['imp_ann_date'].apply(_to_date_or_none)
                rows = upsert_dividend_df(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested dividend rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('dividend ingest failed: %s', e)


def ingest_top10_holders(ts_code=None):
    params = {'ts_code': ts_code}
    aid = audit_start('top10_holders', params)
    try:
        df = call_pro('top10_holders', ts_code=ts_code)
        rows = upsert_top10_holders(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested top10_holders rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('top10_holders ingest failed: %s', e)


def ingest_margin(ts_code=None, start_date=None, end_date=None):
    params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('margin', params)
    try:
        df = call_pro('margin', ts_code=ts_code, start_date=start_date, end_date=end_date)
        rows = upsert_margin(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested margin rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('margin ingest failed: %s', e)


def ingest_block_trade(ts_code=None, start_date=None, end_date=None):
    params = {'ts_code': ts_code, 'start_date': start_date, 'end_date': end_date}
    aid = audit_start('block_trade', params)
    try:
        df = call_pro('block_trade', ts_code=ts_code, start_date=start_date, end_date=end_date)
        rows = upsert_block_trade(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested block_trade rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('block_trade ingest failed: %s', e)


def ingest_repo(repo_date=None):
    params = {'repo_date': repo_date}
    aid = audit_start('repo', params)
    try:
        df = call_pro('repo', repo_date=repo_date) if repo_date else call_pro('repo')
        rows = 0
        if df is None:
            audit_finish(aid, 'error', 0)
            logging.warning('repo ingest: API returned None (possibly rate-limited or no permission)')
            return
        if df.empty:
            audit_finish(aid, 'success', 0)
            logging.info('Ingested repo rows: 0')
            return
        # Delegate repo upserts to DAO
        from app.domains.extdata.dao.tushare_dao import upsert_repo_df
        rows = upsert_repo_df(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested repo rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('repo ingest failed: %s', e)


def ingest_all_other_data(batch_size:int=None, sleep_between:float=0.5):
    """Run selected endpoint ingests for all ts_codes we have.
    This runner calls: `daily_basic`, `adj_factor`, `moneyflow`, `dividend`, `top10_holders`, `margin`, `block_trade`.
    """
    ts_codes = get_all_ts_codes()
    total = len(ts_codes)
    logging.info('Starting other-data ingest for %d symbols', total)
    batch_size = batch_size or int(os.getenv('BATCH_SIZE', '100'))
    for i in range(0, total, batch_size):
        chunk = ts_codes[i:i+batch_size]
        logging.info('Processing chunk %d - %d', i+1, i+len(chunk))
        for ts_code in chunk:
            try:
                ingest_daily_basic(ts_code=ts_code)
                ingest_adj_factor(ts_code=ts_code)
                ingest_moneyflow(ts_code=ts_code)
                ingest_dividend(ts_code=ts_code)
                ingest_top10_holders(ts_code=ts_code)
                ingest_margin(ts_code=ts_code)
                ingest_block_trade(ts_code=ts_code)
            except Exception as e:
                logging.exception('Error ingesting other data for %s: %s', ts_code, e)
            time.sleep(sleep_between)
    logging.info('Other-data ingest completed')


def ingest_stock_basic(exchange=None, list_status='L'):
    params = {'exchange': exchange, 'list_status': list_status}
    aid = audit_start('stock_basic', params)
    try:
        df = call_pro('stock_basic', exchange=exchange, list_status=list_status,
                      fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,list_status,list_date,delist_date,is_hs')
        rows = 0
        if df is None:
            audit_finish(aid, 'error', 0)
            logging.warning('stock_basic ingest: API returned None (possibly rate-limited or no permission)')
            return
        if df.empty:
            audit_finish(aid, 'success', 0)
            logging.info('Ingested stock_basic rows: 0')
            return
        # df is present and not empty; delegate to DAO
        from app.domains.extdata.dao.tushare_dao import upsert_stock_basic
        rows = upsert_stock_basic(df)
        audit_finish(aid, 'success', rows)
        logging.info('Ingested stock_basic rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('stock_basic ingest failed: %s', e)


def get_all_ts_codes():
    return dao_get_all_ts_codes()


def _fetch_existing_keys(table: str, key_date_col: str, start_date, end_date):
    """Return set of (ts_code, date) tuples that already exist in DB for the given date range."""
    # Delegate to DAO
    from app.domains.extdata.dao.tushare_dao import fetch_existing_keys as dao_fetch_existing_keys
    return dao_fetch_existing_keys(table, key_date_col, start_date, end_date)


def ingest_dividend_by_date_range(start_date: str, end_date: str, batch_size: int = None, sleep_between: float = 0.5):
    """Fetch `dividend` for all symbols for a given date range and insert only missing rows.

    start_date/end_date: 'YYYY-MM-DD' or 'YYYYMMDD' accepted. Will query DB for existing ann_date.
    """
    batch_size = batch_size or int(os.getenv('BATCH_SIZE', '100'))
    # normalize dates for DB query
    try:
        s_norm = pd.to_datetime(start_date).date().isoformat()
        e_norm = pd.to_datetime(end_date).date().isoformat()
    except Exception:
        s_norm = start_date
        e_norm = end_date

    existing = _fetch_existing_keys('stock_dividend', 'ann_date', s_norm, e_norm)

    ts_codes = get_all_ts_codes()
    total = len(ts_codes)
    logging.info('Starting dividend date-range ingest for %s - %s (symbols=%d)', s_norm, e_norm, total)

    # DB writes delegated to DAO upsert_dividend_df

    for i in range(0, total, batch_size):
        chunk = ts_codes[i:i+batch_size]
        for ts_code in chunk:
            try:
                df = call_pro('dividend', ts_code=ts_code, start_date=s_norm.replace('-', ''), end_date=e_norm.replace('-', ''))
                if df is None or df.empty:
                    continue
                rows_to_insert = []
                for r in df.to_dict(orient='records'):
                    # Prefer ann_date; if missing, use imp_ann_date as fallback
                    ann_raw = r.get('ann_date')
                    imp_raw = r.get('imp_ann_date')
                    ann_dt = None
                    try:
                        if ann_raw:
                            ann_dt = pd.to_datetime(ann_raw).date()
                        elif imp_raw:
                            ann_dt = pd.to_datetime(imp_raw).date()
                    except Exception:
                        ann_dt = None

                    if not ann_dt:
                        # Skip rows that have neither ann_date nor imp_ann_date
                        logging.debug('Skipping dividend row without ann_date and imp_ann_date for %s', r.get('ts_code'))
                        continue

                    ann = ann_dt.isoformat()
                    key = (r.get('ts_code'), ann)
                    if key in existing:
                        continue
                    rows_to_insert.append({
                        'ts_code': r.get('ts_code'),
                        'ann_date': ann_dt,
                        'imp_ann_date': (pd.to_datetime(r.get('imp_ann_date')).date() if r.get('imp_ann_date') else None),
                        'record_date': (pd.to_datetime(r.get('record_date')).date() if r.get('record_date') else None),
                        'ex_date': (pd.to_datetime(r.get('ex_date')).date() if r.get('ex_date') else None),
                        'pay_date': (pd.to_datetime(r.get('pay_date')).date() if r.get('pay_date') else None),
                        'div_cash': None if pd.isna(r.get('div_cash')) else float(r.get('div_cash')),
                        'div_stock': None if pd.isna(r.get('div_stock')) else float(r.get('div_stock')),
                        'bonus_ratio': None if pd.isna(r.get('bonus_ratio')) else float(r.get('bonus_ratio'))
                    })

                if not rows_to_insert:
                    continue
                # Bulk upsert via DAO
                df_rows = pd.DataFrame(rows_to_insert)
                upsert_dividend_df(df_rows)
                for params in rows_to_insert:
                    if params['ann_date']:
                        existing.add((params['ts_code'], params['ann_date'].isoformat()))
                logging.info('Inserted %d dividend rows for chunk symbol %s', len(rows_to_insert), ts_code)
            except Exception as e:
                logging.exception('Error fetching/dividend for %s: %s', ts_code, e)
            time.sleep(sleep_between)


def ingest_top10_holders_by_date_range(start_date: str, end_date: str, batch_size: int = None, sleep_between: float = 0.5):
    batch_size = batch_size or int(os.getenv('BATCH_SIZE', '100'))
    try:
        s_norm = pd.to_datetime(start_date).date().isoformat()
        e_norm = pd.to_datetime(end_date).date().isoformat()
    except Exception:
        s_norm = start_date
        e_norm = end_date

    existing = _fetch_existing_keys('top10_holders', 'end_date', s_norm, e_norm)
    ts_codes = get_all_ts_codes()
    total = len(ts_codes)
    logging.info('Starting top10_holders date-range ingest for %s - %s (symbols=%d)', s_norm, e_norm, total)

    # DB writes delegated to DAO upsert_top10_holders

    for i in range(0, total, batch_size):
        chunk = ts_codes[i:i+batch_size]
        for ts_code in chunk:
            try:
                df = call_pro('top10_holders', ts_code=ts_code, start_date=s_norm.replace('-', ''), end_date=e_norm.replace('-', ''))
                if df is None or df.empty:
                    continue
                rows_to_insert = []
                for r in df.to_dict(orient='records'):
                    endd = (pd.to_datetime(r.get('end_date')).date().isoformat() if r.get('end_date') else None)
                    key = (r.get('ts_code'), endd)
                    if endd and key in existing:
                        continue
                    rows_to_insert.append({
                        'ts_code': r.get('ts_code'),
                        'end_date': (pd.to_datetime(r.get('end_date')).date() if r.get('end_date') else None),
                        'holder_name': r.get('holder_name'),
                        'hold_amount': None if pd.isna(r.get('hold_amount')) else float(r.get('hold_amount')),
                        'hold_ratio': None if pd.isna(r.get('hold_ratio')) else float(r.get('hold_ratio'))
                    })

                if not rows_to_insert:
                    continue
                df_rows = pd.DataFrame(rows_to_insert)
                upsert_top10_holders(df_rows)
                for params in rows_to_insert:
                    if params['end_date']:
                        existing.add((params['ts_code'], params['end_date'].isoformat()))
                logging.info('Inserted %d top10_holders rows for symbol %s', len(rows_to_insert), ts_code)
            except Exception as e:
                logging.exception('Error fetching/top10_holders for %s: %s', ts_code, e)
            time.sleep(sleep_between)


def ingest_adj_factor_by_date_range(start_date: str, end_date: str, batch_size: int = None, sleep_between: float = 0.5):
    batch_size = batch_size or int(os.getenv('BATCH_SIZE', '100'))
    try:
        s_norm = pd.to_datetime(start_date).date().isoformat()
        e_norm = pd.to_datetime(end_date).date().isoformat()
    except Exception:
        s_norm = start_date
        e_norm = end_date

    existing = _fetch_existing_keys('adj_factor', 'trade_date', s_norm, e_norm)
    ts_codes = get_all_ts_codes()
    total = len(ts_codes)
    logging.info('Starting adj_factor date-range ingest for %s - %s (symbols=%d)', s_norm, e_norm, total)

    # DB writes delegated to DAO upsert_adj_factor

    for i in range(0, total, batch_size):
        chunk = ts_codes[i:i+batch_size]
        for ts_code in chunk:
            try:
                df = call_pro('adj_factor', ts_code=ts_code, start_date=s_norm.replace('-', ''), end_date=e_norm.replace('-', ''))
                if df is None or df.empty:
                    continue
                rows_to_insert = []
                for r in df.to_dict(orient='records'):
                    td = (pd.to_datetime(r.get('trade_date')).date().isoformat() if r.get('trade_date') else None)
                    key = (r.get('ts_code'), td)
                    if td and key in existing:
                        continue
                    rows_to_insert.append({
                        'ts_code': r.get('ts_code'),
                        'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                        'adj_factor': None if pd.isna(r.get('adj_factor')) else float(r.get('adj_factor'))
                    })

                if not rows_to_insert:
                    continue
                df_rows = pd.DataFrame(rows_to_insert)
                upsert_adj_factor(df_rows)
                for params in rows_to_insert:
                    if params['trade_date']:
                        existing.add((params['ts_code'], params['trade_date'].isoformat()))
                logging.info('Inserted %d adj_factor rows for symbol %s', len(rows_to_insert), ts_code)
            except Exception as e:
                logging.exception('Error fetching/adj_factor for %s: %s', ts_code, e)
            time.sleep(sleep_between)


def get_max_trade_date(ts_code):
    return dao_get_max_trade_date(ts_code)


def ingest_all_daily(batch_size:int=None, sleep_between:float=0.2):
    """Bulk ingest daily for all ts_code in stock_basic.

    - batch_size: number of symbols to process per loop iteration (uses env BATCH_SIZE if None)
    - sleep_between: seconds to sleep between individual symbol API calls to avoid rate limits
    """
    batch_size = batch_size or int(os.getenv('BATCH_SIZE', '100'))
    max_retries = int(os.getenv('MAX_RETRIES', '3'))

    ts_codes = get_all_ts_codes()
    total = len(ts_codes)
    logging.info('Starting bulk daily ingest for %d symbols (batch_size=%d)', total, batch_size)

    for i in range(0, total, batch_size):
        chunk = ts_codes[i:i+batch_size]
        logging.info('Processing chunk %d - %d', i+1, i+len(chunk))
        for ts_code in chunk:
            # determine resume point
            last_date = get_max_trade_date(ts_code)
            # If we have a last_date in DB, fetch only missing days (existing behavior)
            if last_date:
                try:
                    start_date = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime('%Y%m%d')
                except Exception:
                    start_date = None
                attempt = 0
                while attempt < max_retries:
                    try:
                        logging.info('Ingesting daily for %s (start_date=%s)', ts_code, start_date)
                        ingest_daily(ts_code=ts_code, start_date=start_date, end_date=None)
                        break
                    except Exception as e:
                        attempt += 1
                        logging.warning('Attempt %d failed for %s: %s', attempt, ts_code, e)
                        time.sleep(1 + attempt)
            else:
                # No data in DB for this ts_code: fetch full history once, then filter out any rows
                # that already exist (defensive) and bulk upsert missing rows. This is much faster
                # than iterating per-date across all symbols.
                attempt = 0
                while attempt < max_retries:
                    try:
                        logging.info('Fetching full history for %s', ts_code)
                        df = call_pro('daily', ts_code=ts_code)
                        if df is None or df.empty:
                            logging.info('No daily data returned for %s', ts_code)
                            break

                        # normalize trade_date once for all rows
                        try:
                            df['trade_date'] = pd.to_datetime(df.get('trade_date'), errors='coerce').dt.date
                        except Exception:
                            logging.exception('Failed to normalize trade_date for %s', ts_code)

                        # determine date window and fetch existing keys to avoid duplicates
                        try:
                            dates = df['trade_date'].dropna()
                            if dates.empty:
                                logging.info('No valid trade_date rows for %s', ts_code)
                                break
                            s_norm = dates.min().isoformat()
                            e_norm = dates.max().isoformat()
                        except Exception:
                            s_norm = None
                            e_norm = None

                        existing = set()
                        if s_norm and e_norm:
                            existing = _fetch_existing_keys('stock_daily', 'trade_date', s_norm, e_norm)

                        # filter out rows already present
                        def _row_key(r):
                            td = r.get('trade_date')
                            if td is None:
                                return None
                            try:
                                dstr = td.isoformat() if hasattr(td, 'isoformat') else str(td)
                            except Exception:
                                dstr = str(td)
                            return (r.get('ts_code'), dstr)

                        records = []
                        for r in df.to_dict(orient='records'):
                            key = _row_key(r)
                            if key is None:
                                continue
                            if key in existing:
                                continue
                            records.append(r)

                        if not records:
                            logging.info('No new daily rows to insert for %s', ts_code)
                            break

                        # Build a DataFrame of only new records and delegate to DAO upsert
                        df_new = pd.DataFrame(records)
                        rows = upsert_daily(df_new)
                        logging.info('Inserted %d new daily rows for %s', rows, ts_code)
                        break
                    except Exception as e:
                        attempt += 1
                        logging.warning('Attempt %d failed fetching full history for %s: %s', attempt, ts_code, e)
                        time.sleep(1 + attempt)
            time.sleep(sleep_between)

    logging.info('Bulk daily ingest completed')


def get_failed_ts_codes(limit:int=None):
    from app.domains.extdata.dao.tushare_dao import get_failed_ts_codes as dao_get_failed_ts_codes
    return dao_get_failed_ts_codes(limit=limit)


def retry_failed_daily(limit:int=None):
    codes = get_failed_ts_codes(limit=limit)
    logging.info('Retrying %d failed daily ts_code(s)', len(codes))
    for ts in codes:
        try:
            ingest_daily(ts_code=ts, start_date=None, end_date=None)
        except Exception as e:
            logging.exception('Retry failed for %s: %s', ts, e)


if __name__ == '__main__':
    # simple CLI: ingest daily for a sample ts_code
    if os.getenv('INGEST_STOCK_BASIC'):
        ingest_stock_basic()
    elif os.getenv('INGEST_ALL_DAILY'):
        ingest_all_daily()
    else:
        ingest_daily(ts_code=os.getenv('SAMPLE_TS', ''), start_date=None, end_date=None)
