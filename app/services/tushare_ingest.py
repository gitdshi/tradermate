import os
import json
import time
import logging
import pandas as pd
import tushare as ts
from sqlalchemy import create_engine, text
import numpy as np

logging.basicConfig(level=logging.INFO)

# Tushare data is stored in the tushare database
TUSHARE_DB_URL = os.getenv('TUSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1/tushare?charset=utf8mb4')
TS_TOKEN = os.getenv('TUSHARE_TOKEN', '')

engine = create_engine(TUSHARE_DB_URL, pool_pre_ping=True)
pro = ts.pro_api(TS_TOKEN) if TS_TOKEN else ts.pro_api()

# Rate limiting configuration: max calls per minute to Tushare API
CALLS_PER_MIN = int(os.getenv('TUSHARE_CALLS_PER_MIN', '50'))
_MIN_INTERVAL = 60.0 / max(1, CALLS_PER_MIN)


def call_pro(api_name: str, max_retries: int = None, backoff_base: int = 5, **kwargs):
    """Wrapper around `pro.<api_name>(**kwargs)` that enforces a simple per-minute rate limit
    (spacing calls by at least `_MIN_INTERVAL`) and retries on transient errors including
    Tushare rate-limit responses. Returns the DataFrame from the API call or raises on final failure.
    """
    if max_retries is None:
        max_retries = int(os.getenv('MAX_RETRIES', '3'))

    # simple per-process last-call timestamp stored on function
    if not hasattr(call_pro, '_last_call'):
        call_pro._last_call = 0.0

    attempt = 0
    while attempt < max_retries:
        # enforce spacing between calls
        elapsed = time.time() - call_pro._last_call
        if elapsed < _MIN_INTERVAL:
            to_sleep = _MIN_INTERVAL - elapsed
            logging.debug('Sleeping %.3fs to respect rate limit', to_sleep)
            time.sleep(to_sleep)

        try:
            fn = getattr(pro, api_name)
            res = fn(**kwargs)
            call_pro._last_call = time.time()
            return res
        except Exception as e:
            attempt += 1
            msg = str(e)
            # detect common Tushare rate-limit message (Chinese/English) and backoff
            if '每分钟最多访问' in msg or 'rate limit' in msg.lower() or 'limit' in msg.lower():
                sleep_time = backoff_base * attempt
                logging.warning('Tushare rate-limit detected for %s: sleeping %ds (attempt %d/%d): %s', api_name, sleep_time, attempt, max_retries, msg)
                time.sleep(sleep_time)
                continue
            logging.exception('Tushare API call %s failed (attempt %d/%d): %s', api_name, attempt, max_retries, e)
            if attempt < max_retries:
                time.sleep(5 * attempt)
                continue
            raise


def audit_start(api_name, params):
    with engine.begin() as conn:
        res = conn.execute(text(
            "INSERT INTO ingest_audit (api_name, params, status, fetched_rows) VALUES (:api, :params, 'running', 0)"
        ), {"api": api_name, "params": json.dumps(params)})
        return res.lastrowid


def audit_finish(audit_id, status, rows):
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE ingest_audit SET status=:status, fetched_rows=:rows, finished_at=NOW() WHERE id=:id"
        ), {"status": status, "rows": rows, "id": audit_id})


def upsert_daily(df: pd.DataFrame):
    if df.empty:
        return 0
    count = 0
    insert_sql = text(
        "INSERT INTO stock_daily (ts_code, trade_date, open, high, low, close, pre_close, change_amount, pct_change, vol, amount)"
        " VALUES (:ts_code, :trade_date, :open, :high, :low, :close, :pre_close, :change_amount, :pct_change, :vol, :amount)"
        " ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), pre_close=VALUES(pre_close), change_amount=VALUES(change_amount), pct_change=VALUES(pct_change), vol=VALUES(vol), amount=VALUES(amount)"
    )
    def clean(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        # numpy scalar -> python native
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (np.bool_,)):
            return bool(v)
        return v

    def round2(v):
        if v is None:
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        try:
            return round(float(v), 2)
        except Exception:
            return v

    with engine.begin() as conn:
        for r in df.to_dict(orient='records'):
            params = {
                'ts_code': clean(r.get('ts_code')),
                'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                'open': round2(clean(r.get('open'))),
                'high': round2(clean(r.get('high'))),
                'low': round2(clean(r.get('low'))),
                'close': round2(clean(r.get('close'))),
                'pre_close': round2(clean(r.get('pre_close'))),
                'change_amount': round2(clean(r.get('change') or r.get('change_amount'))),
                'pct_change': round2(clean(r.get('pct_chg') or r.get('pct_change'))),
                'vol': (int(r.get('vol')) if (r.get('vol') is not None and not pd.isna(r.get('vol'))) else None),
                'amount': round2(clean(r.get('amount')))
            }
            conn.execute(insert_sql, params)
            count += 1
    return count


def store_financial_statement(df: pd.DataFrame, statement_type: str):
    if df.empty:
        return 0
    count = 0
    insert_sql = text(
        "INSERT INTO financial_statement (ts_code, statement_type, ann_date, end_date, report_date, data) VALUES (:ts_code, :statement_type, :ann_date, :end_date, :report_date, :data)"
    )
    with engine.begin() as conn:
        for r in df.to_dict(orient='records'):
            ann_date = r.get('ann_date')
            end_date = r.get('end_date') or r.get('period')
            report_date = r.get('f_ann_date') or r.get('report_date')
            conn.execute(insert_sql, {
                'ts_code': r.get('ts_code'),
                'statement_type': statement_type,
                'ann_date': ann_date,
                'end_date': end_date,
                'report_date': report_date,
                'data': json.dumps(r, default=str)
            })
            count += 1
    return count


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
                upsert_sql = text(
                    "INSERT INTO index_daily (index_code, trade_date, open, high, low, close, vol, amount) "
                    "VALUES (:index_code, :trade_date, :open, :high, :low, :close, :vol, :amount) "
                    "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), "
                    "close=VALUES(close), vol=VALUES(vol), amount=VALUES(amount)"
                )
                def clean(v):
                    if pd.isna(v) or v is None:
                        return None
                    return v
                
                with engine.begin() as conn:
                    for _, row in df.iterrows():
                        conn.execute(upsert_sql, {
                            'index_code': row['index_code'],
                            'trade_date': str(row['trade_date']),
                            'open': clean(row.get('open')),
                            'high': clean(row.get('high')),
                            'low': clean(row.get('low')),
                            'close': clean(row.get('close')),
                            'vol': clean(row.get('vol')),
                            'amount': clean(row.get('amount'))
                        })
                        rows += 1
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
        # write to daily_basic table
        rows = 0
        if not df.empty:
            upsert_sql = text(
                "INSERT INTO daily_basic (ts_code, trade_date, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, total_mv, circ_mv)"
                " VALUES (:ts_code, :trade_date, :turnover_rate, :turnover_rate_f, :volume_ratio, :pe, :pe_ttm, :pb, :ps, :ps_ttm, :total_mv, :circ_mv)"
                " ON DUPLICATE KEY UPDATE turnover_rate=VALUES(turnover_rate), turnover_rate_f=VALUES(turnover_rate_f), volume_ratio=VALUES(volume_ratio), pe=VALUES(pe), pe_ttm=VALUES(pe_ttm), pb=VALUES(pb), ps=VALUES(ps), ps_ttm=VALUES(ps_ttm), total_mv=VALUES(total_mv), circ_mv=VALUES(circ_mv)"
            )
            def clean(v):
                try:
                    if pd.isna(v):
                        return None
                except Exception:
                    pass
                if isinstance(v, (np.integer,)):
                    return int(v)
                if isinstance(v, (np.floating,)):
                    return float(v)
                return v

            with engine.begin() as conn:
                def round2(v):
                    try:
                        if pd.isna(v):
                            return None
                    except Exception:
                        pass
                    try:
                        return round(float(v), 2)
                    except Exception:
                        return v

                for r in df.to_dict(orient='records'):
                    conn.execute(upsert_sql, {
                        'ts_code': clean(r.get('ts_code')),
                        'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                        'turnover_rate': round2(clean(r.get('turnover_rate'))),
                        'turnover_rate_f': round2(clean(r.get('turnover_rate_f'))),
                        'volume_ratio': round2(clean(r.get('volume_ratio'))),
                        'pe': round2(clean(r.get('pe'))),
                        'pe_ttm': round2(clean(r.get('pe_ttm'))),
                        'pb': round2(clean(r.get('pb'))),
                        'ps': round2(clean(r.get('ps'))),
                        'ps_ttm': round2(clean(r.get('ps_ttm'))),
                        'total_mv': round2(clean(r.get('total_mv'))),
                        'circ_mv': round2(clean(r.get('circ_mv')))
                    })
                    rows += 1
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
        rows = 0
        if not df.empty:
            upsert_sql = text(
                "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (:ts_code, :trade_date, :adj_factor) ON DUPLICATE KEY UPDATE adj_factor=VALUES(adj_factor)"
            )
            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    adj = r.get('adj_factor')
                    if pd.isna(adj):
                        adj = None
                    conn.execute(upsert_sql, {
                        'ts_code': r.get('ts_code'),
                        'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                        'adj_factor': adj
                    })
                    rows += 1
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
        rows = 0
        if not df.empty:
            upsert_sql = text(
                "INSERT INTO stock_moneyflow (ts_code, trade_date, net_mf, buy_small, sell_small, buy_medium, sell_medium, buy_large, sell_large, buy_huge, sell_huge)"
                " VALUES (:ts_code, :trade_date, :net_mf, :buy_small, :sell_small, :buy_medium, :sell_medium, :buy_large, :sell_large, :buy_huge, :sell_huge)"
                " ON DUPLICATE KEY UPDATE net_mf=VALUES(net_mf), buy_small=VALUES(buy_small), sell_small=VALUES(sell_small), buy_medium=VALUES(buy_medium), sell_medium=VALUES(sell_medium), buy_large=VALUES(buy_large), sell_large=VALUES(sell_large), buy_huge=VALUES(buy_huge), sell_huge=VALUES(sell_huge)"
            )
            def clean(v):
                try:
                    if pd.isna(v):
                        return None
                except Exception:
                    pass
                return None if v is None else float(v)

            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    conn.execute(upsert_sql, {
                        'ts_code': r.get('ts_code'),
                        'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                        'net_mf': clean(r.get('net_mf')),
                        'buy_small': clean(r.get('buy_sm_vol') or r.get('buy_small')),
                        'sell_small': clean(r.get('sell_sm_vol') or r.get('sell_small')),
                        'buy_medium': clean(r.get('buy_md_vol') or r.get('buy_medium')),
                        'sell_medium': clean(r.get('sell_md_vol') or r.get('sell_medium')),
                        'buy_large': clean(r.get('buy_lg_vol') or r.get('buy_large')),
                        'sell_large': clean(r.get('sell_lg_vol') or r.get('sell_large')),
                        'buy_huge': clean(r.get('buy_hu_vol') or r.get('buy_huge')),
                        'sell_huge': clean(r.get('sell_hu_vol') or r.get('sell_huge'))
                    })
                    rows += 1
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
        if not df.empty:
            insert_sql = text(
                "INSERT INTO stock_dividend (ts_code, ann_date, imp_ann_date, record_date, ex_date, pay_date, div_cash, div_stock, bonus_ratio)"
                " VALUES (:ts_code, :ann_date, :imp_ann_date, :record_date, :ex_date, :pay_date, :div_cash, :div_stock, :bonus_ratio)"
                " ON DUPLICATE KEY UPDATE div_cash=VALUES(div_cash), div_stock=VALUES(div_stock), bonus_ratio=VALUES(bonus_ratio)"
            )
            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    conn.execute(insert_sql, {
                        'ts_code': r.get('ts_code'),
                        'ann_date': (pd.to_datetime(r.get('ann_date')).date() if r.get('ann_date') else None),
                        'imp_ann_date': (pd.to_datetime(r.get('imp_ann_date')).date() if r.get('imp_ann_date') else None),
                        'record_date': (pd.to_datetime(r.get('record_date')).date() if r.get('record_date') else None),
                        'ex_date': (pd.to_datetime(r.get('ex_date')).date() if r.get('ex_date') else None),
                        'pay_date': (pd.to_datetime(r.get('pay_date')).date() if r.get('pay_date') else None),
                        'div_cash': None if pd.isna(r.get('div_cash')) else float(r.get('div_cash')),
                        'div_stock': None if pd.isna(r.get('div_stock')) else float(r.get('div_stock')),
                        'bonus_ratio': None if pd.isna(r.get('bonus_ratio')) else float(r.get('bonus_ratio'))
                    })
                    rows += 1
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
        rows = 0
        if not df.empty:
            insert_sql = text(
                "INSERT INTO top10_holders (ts_code, end_date, holder_name, hold_amount, hold_ratio)"
                " VALUES (:ts_code, :end_date, :holder_name, :hold_amount, :hold_ratio)"
                " ON DUPLICATE KEY UPDATE hold_amount=VALUES(hold_amount), hold_ratio=VALUES(hold_ratio)"
            )
            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    conn.execute(insert_sql, {
                        'ts_code': r.get('ts_code'),
                        'end_date': (pd.to_datetime(r.get('end_date')).date() if r.get('end_date') else None),
                        'holder_name': r.get('holder_name'),
                        'hold_amount': None if pd.isna(r.get('hold_amount')) else float(r.get('hold_amount')),
                        'hold_ratio': None if pd.isna(r.get('hold_ratio')) else float(r.get('hold_ratio'))
                    })
                    rows += 1
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
        rows = 0
        if not df.empty:
            insert_sql = text(
                "INSERT INTO stock_margin (ts_code, trade_date, financing_balance, financing_buy, financing_repay, securities_lend_balance)"
                " VALUES (:ts_code, :trade_date, :financing_balance, :financing_buy, :financing_repay, :securities_lend_balance)"
                " ON DUPLICATE KEY UPDATE financing_balance=VALUES(financing_balance), financing_buy=VALUES(financing_buy), financing_repay=VALUES(financing_repay), securities_lend_balance=VALUES(securities_lend_balance)"
            )
            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    conn.execute(insert_sql, {
                        'ts_code': r.get('ts_code'),
                        'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                        'financing_balance': None if pd.isna(r.get('financing_balance')) else float(r.get('financing_balance')),
                        'financing_buy': None if pd.isna(r.get('financing_buy')) else float(r.get('financing_buy')),
                        'financing_repay': None if pd.isna(r.get('financing_repay')) else float(r.get('financing_repay')),
                        'securities_lend_balance': None if pd.isna(r.get('securities_lend_balance')) else float(r.get('securities_lend_balance'))
                    })
                    rows += 1
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
        rows = 0
        if not df.empty:
            insert_sql = text(
                "INSERT INTO block_trade (ts_code, trade_date, trade_time, price, volume, amount, side)"
                " VALUES (:ts_code, :trade_date, :trade_time, :price, :volume, :amount, :side)"
            )
            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    conn.execute(insert_sql, {
                        'ts_code': r.get('ts_code'),
                        'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                        'trade_time': (pd.to_datetime(r.get('trade_time')) if r.get('trade_time') else None),
                        'price': None if pd.isna(r.get('price')) else float(r.get('price')),
                        'volume': (int(r.get('volume')) if (r.get('volume') is not None and not pd.isna(r.get('volume'))) else None),
                        'amount': None if pd.isna(r.get('amount')) else float(r.get('amount')),
                        'side': r.get('side')
                    })
                    rows += 1
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
        if not df.empty:
            insert_sql = text(
                "INSERT INTO repo (repo_date, instrument, rate, amount) VALUES (:repo_date, :instrument, :rate, :amount)"
            )
            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    conn.execute(insert_sql, {
                        'repo_date': (pd.to_datetime(r.get('repo_date')).date() if r.get('repo_date') else None),
                        'instrument': r.get('instrument'),
                        'rate': None if pd.isna(r.get('rate')) else float(r.get('rate')),
                        'amount': None if pd.isna(r.get('amount')) else float(r.get('amount'))
                    })
                    rows += 1
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
        if not df.empty:
            insert_sql = text(
                "INSERT INTO stock_basic (ts_code, symbol, name, area, industry, fullname, enname, market, exchange, list_status, list_date, delist_date, is_hs)"
                " VALUES (:ts_code, :symbol, :name, :area, :industry, :fullname, :enname, :market, :exchange, :list_status, :list_date, :delist_date, :is_hs)"
                " ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), name=VALUES(name), area=VALUES(area), industry=VALUES(industry), fullname=VALUES(fullname), enname=VALUES(enname), market=VALUES(market), exchange=VALUES(exchange), list_status=VALUES(list_status), list_date=VALUES(list_date), delist_date=VALUES(delist_date), is_hs=VALUES(is_hs)"
            )
            with engine.begin() as conn:
                for r in df.to_dict(orient='records'):
                    conn.execute(insert_sql, {
                        'ts_code': r.get('ts_code'),
                        'symbol': r.get('symbol'),
                        'name': r.get('name'),
                        'area': r.get('area'),
                        'industry': r.get('industry'),
                        'fullname': r.get('fullname'),
                        'enname': r.get('enname'),
                        'market': r.get('market'),
                        'exchange': r.get('exchange'),
                        'list_status': r.get('list_status'),
                        'list_date': pd.to_datetime(r.get('list_date')).date() if r.get('list_date') else None,
                        'delist_date': pd.to_datetime(r.get('delist_date')).date() if r.get('delist_date') else None,
                        'is_hs': r.get('is_hs')
                    })
                    rows += 1
        audit_finish(aid, 'success', rows)
        logging.info('Ingested stock_basic rows: %d', rows)
    except Exception as e:
        audit_finish(aid, 'error', 0)
        logging.exception('stock_basic ingest failed: %s', e)


def get_all_ts_codes():
    with engine.connect() as conn:
        res = conn.execute(text("SELECT ts_code FROM stock_basic ORDER BY ts_code"))
        return [r[0] for r in res.fetchall()]


def get_max_trade_date(ts_code):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT MAX(trade_date) FROM stock_daily WHERE ts_code=:ts"), {"ts": ts_code})
        row = res.fetchone()
        return row[0] if row is not None else None


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
            start_date = None
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
            time.sleep(sleep_between)

    logging.info('Bulk daily ingest completed')


def get_failed_ts_codes(limit:int=None):
    query = "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(params,'$.ts_code')) AS ts FROM ingest_audit WHERE api_name='daily' AND status='error'"
    if limit:
        query += f" LIMIT {int(limit)}"
    with engine.connect() as conn:
        res = conn.execute(text(query))
        return [r[0] for r in res.fetchall() if r[0]]


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
