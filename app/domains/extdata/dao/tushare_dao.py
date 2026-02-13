"""DAO helpers for Tushare DB operations used by datasync services."""
import logging
from datetime import date
from typing import Any
import pandas as pd
import numpy as np
import os
import json
from sqlalchemy import text
from app.infrastructure.db.connections import get_tushare_engine

logger = logging.getLogger(__name__)

engine = get_tushare_engine()


def audit_start(api_name: str, params: dict) -> int:
    with engine.begin() as conn:
        res = conn.execute(text(
            "INSERT INTO ingest_audit (api_name, params, status, fetched_rows) VALUES (:api, :params, 'running', 0)"
        ), {"api": api_name, "params": json.dumps(params)})
        try:
            return int(res.lastrowid)
        except Exception:
            return 0


def audit_finish(audit_id: int, status: str, rows: int):
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE ingest_audit SET status=:status, fetched_rows=:rows, finished_at=NOW() WHERE id=:id"
        ), {"status": status, "rows": rows, "id": audit_id})


def upsert_daily(df: pd.DataFrame) -> int:
    """Bulk upsert stock_daily rows from a DataFrame. Returns number of rows processed."""
    if df is None or df.empty:
        return 0
    count = 0
    insert_sql = (
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
            conn.execute(text(insert_sql), params)
            count += 1
    return count


def upsert_index_daily_df(df: pd.DataFrame) -> int:
    """Upsert index_daily rows from Tushare daily index API DataFrame."""
    if df is None or df.empty:
        return 0
    insert_sql = (
        "INSERT INTO index_daily (index_code, trade_date, open, high, low, close, vol, amount) "
        "VALUES (:index_code, :trade_date, :open, :high, :low, :close, :vol, :amount) "
        "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), "
        "close=VALUES(close), vol=VALUES(vol), amount=VALUES(amount)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient='records'):
            conn.execute(text(insert_sql), {
                'index_code': r.get('ts_code') or r.get('index_code'),
                'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                'open': None if pd.isna(r.get('open')) else float(r.get('open')),
                'high': None if pd.isna(r.get('high')) else float(r.get('high')),
                'low': None if pd.isna(r.get('low')) else float(r.get('low')),
                'close': None if pd.isna(r.get('close')) else float(r.get('close')),
                'vol': (int(r.get('vol')) if (r.get('vol') is not None and not pd.isna(r.get('vol'))) else None),
                'amount': None if pd.isna(r.get('amount')) else float(r.get('amount'))
            })
            rows += 1
    return rows


def get_all_ts_codes() -> list:
    """Return all ts_code values from stock_basic ordered."""
    with engine.connect() as conn:
        res = conn.execute(text("SELECT ts_code FROM stock_basic ORDER BY ts_code"))
        return [r[0] for r in res.fetchall()]


def get_max_trade_date(ts_code: str):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT MAX(trade_date) FROM stock_daily WHERE ts_code=:ts"), {"ts": ts_code})
        row = res.fetchone()
        return row[0] if row is not None else None


def upsert_dividend_df(df: pd.DataFrame) -> int:
    """Upsert rows from a dividend DataFrame into stock_dividend."""
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_dividend (ts_code, ann_date, imp_ann_date, record_date, ex_date, pay_date, div_cash, div_stock, bonus_ratio)"
        " VALUES (:ts_code, :ann_date, :imp_ann_date, :record_date, :ex_date, :pay_date, :div_cash, :div_stock, :bonus_ratio)"
        " ON DUPLICATE KEY UPDATE div_cash=VALUES(div_cash), div_stock=VALUES(div_stock), bonus_ratio=VALUES(bonus_ratio)"
    )
    rows = 0
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
    return rows


def upsert_financial_statement(df: pd.DataFrame, statement_type: str) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO financial_statement (ts_code, statement_type, ann_date, end_date, report_date, data) VALUES (:ts_code, :statement_type, :ann_date, :end_date, :report_date, :data)"
    )
    count = 0
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


def upsert_daily_basic(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
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

    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient='records'):
            conn.execute(insert_sql, {
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
    return rows


def upsert_adj_factor(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (:ts_code, :trade_date, :adj_factor) ON DUPLICATE KEY UPDATE adj_factor=VALUES(adj_factor)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient='records'):
            adj = r.get('adj_factor')
            if pd.isna(adj):
                adj = None
            conn.execute(insert_sql, {
                'ts_code': r.get('ts_code'),
                'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                'adj_factor': adj
            })
            rows += 1
    return rows


def upsert_moneyflow(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
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

    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient='records'):
            conn.execute(insert_sql, {
                'ts_code': r.get('ts_code'),
                'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                'net_mf': clean(r.get('net_mf')),
                'buy_small': clean(r.get('buy_sm_vol') or r.get('buy_small')),
                'sell_small': clean(r.get('sell_sm_vol') or r.get('sell_small')),
                'buy_medium': clean(r.get('buy_md_vol') or r.get('buy_medium')),
                'sell_medium': clean(r.get('buy_md_vol') or r.get('buy_medium')),
                'buy_large': clean(r.get('buy_lg_vol') or r.get('buy_large')),
                'sell_large': clean(r.get('buy_lg_vol') or r.get('buy_large')),
                'buy_huge': clean(r.get('buy_hu_vol') or r.get('buy_huge')),
                'sell_huge': clean(r.get('buy_hu_vol') or r.get('buy_huge'))
            })
            rows += 1
    return rows


def upsert_top10_holders(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO top10_holders (ts_code, end_date, holder_name, hold_amount, hold_ratio)"
        " VALUES (:ts_code, :end_date, :holder_name, :hold_amount, :hold_ratio)"
        " ON DUPLICATE KEY UPDATE hold_amount=VALUES(hold_amount), hold_ratio=VALUES(hold_ratio)"
    )
    rows = 0
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
    return rows


def upsert_margin(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_margin (ts_code, trade_date, financing_balance, financing_buy, financing_repay, securities_lend_balance)"
        " VALUES (:ts_code, :trade_date, :financing_balance, :financing_buy, :financing_repay, :securities_lend_balance)"
        " ON DUPLICATE KEY UPDATE financing_balance=VALUES(financing_balance), financing_buy=VALUES(financing_buy), financing_repay=VALUES(financing_repay), securities_lend_balance=VALUES(securities_lend_balance)"
    )
    rows = 0
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
    return rows


def upsert_block_trade(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO block_trade (ts_code, trade_date, trade_time, price, volume, amount, side)"
        " VALUES (:ts_code, :trade_date, :trade_time, :price, :volume, :amount, :side)"
    )
    rows = 0
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
    return rows


def upsert_stock_basic(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO stock_basic (ts_code, symbol, name, area, industry, fullname, enname, market, exchange, list_status, list_date, delist_date, is_hs)"
        " VALUES (:ts_code, :symbol, :name, :area, :industry, :fullname, :enname, :market, :exchange, :list_status, :list_date, :delist_date, :is_hs)"
        " ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), name=VALUES(name), area=VALUES(area), industry=VALUES(industry), fullname=VALUES(fullname), enname=VALUES(enname), market=VALUES(market), exchange=VALUES(exchange), list_status=VALUES(list_status), list_date=VALUES(list_date), delist_date=VALUES(delist_date), is_hs=VALUES(is_hs)"
    )
    rows = 0
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
                'list_date': (pd.to_datetime(r.get('list_date')).date() if r.get('list_date') else None),
                'delist_date': (pd.to_datetime(r.get('delist_date')).date() if r.get('delist_date') else None),
                'is_hs': r.get('is_hs')
            })
            rows += 1
    return rows


def upsert_repo_df(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    insert_sql = text(
        "INSERT INTO repo (repo_date, instrument, rate, amount) VALUES (:repo_date, :instrument, :rate, :amount)"
    )
    rows = 0
    with engine.begin() as conn:
        for r in df.to_dict(orient='records'):
            conn.execute(insert_sql, {
                'repo_date': (pd.to_datetime(r.get('repo_date')).date() if r.get('repo_date') else None),
                'instrument': r.get('instrument'),
                'rate': None if pd.isna(r.get('rate')) else float(r.get('rate')),
                'amount': None if pd.isna(r.get('amount')) else float(r.get('amount'))
            })
            rows += 1
    return rows


def fetch_stock_daily_rows(ts_code: str, start_date=None):
    """Fetch rows from stock_daily for a given ts_code and optional start_date. Returns list of rows."""
    q = "SELECT trade_date, open, high, low, close, vol, amount FROM stock_daily WHERE ts_code = :ts_code"
    params = {'ts_code': ts_code}
    if start_date is not None:
        q += " AND trade_date >= :start_date"
        params['start_date'] = start_date
    q += " ORDER BY trade_date ASC"
    with engine.connect() as conn:
        res = conn.execute(text(q), params)
        return res.fetchall()


def fetch_existing_keys(table: str, key_date_col: str, start_date=None, end_date=None):
    """Generic fetch of existing keys (ts_code, date) for a table between dates."""
    q = f"SELECT ts_code, {key_date_col} FROM {table} WHERE {key_date_col} BETWEEN :s AND :e"
    with engine.connect() as conn:
        res = conn.execute(text(q), {"s": start_date, "e": end_date})
        existing = set()
        for r in res.fetchall():
            ts = r[0]
            d = r[1]
            if d is None:
                continue
            try:
                dval = d if isinstance(d, (str,)) else d
            except Exception:
                dval = d
            if hasattr(dval, 'isoformat'):
                dstr = dval.isoformat()
            else:
                dstr = str(dval)
            existing.add((ts, dstr))
        return existing


def get_failed_ts_codes(limit: int = None):
    q = "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(params,'$.ts_code')) AS ts FROM ingest_audit WHERE api_name='daily' AND status='error'"
    if limit:
        q += f" LIMIT {int(limit)}"
    with engine.connect() as conn:
        res = conn.execute(text(q))
        return [r[0] for r in res.fetchall() if r[0]]

    def upsert_daily(df: pd.DataFrame) -> int:
        """Bulk upsert stock_daily rows from a DataFrame. Returns number of rows processed."""
        if df is None or df.empty:
            return 0
        count = 0
        insert_sql = (
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
                conn.execute(text(insert_sql), params)
                count += 1
        return count


    def upsert_index_daily_df(df: pd.DataFrame) -> int:
        """Upsert index_daily rows from Tushare daily index API DataFrame."""
        if df is None or df.empty:
            return 0
        insert_sql = (
            "INSERT INTO index_daily (index_code, trade_date, open, high, low, close, vol, amount) "
            "VALUES (:index_code, :trade_date, :open, :high, :low, :close, :vol, :amount) "
            "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), "
            "close=VALUES(close), vol=VALUES(vol), amount=VALUES(amount)"
        )
        rows = 0
        with engine.begin() as conn:
            for r in df.to_dict(orient='records'):
                conn.execute(text(insert_sql), {
                    'index_code': r.get('ts_code') or r.get('index_code'),
                    'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                    'open': None if pd.isna(r.get('open')) else float(r.get('open')),
                    'high': None if pd.isna(r.get('high')) else float(r.get('high')),
                    'low': None if pd.isna(r.get('low')) else float(r.get('low')),
                    'close': None if pd.isna(r.get('close')) else float(r.get('close')),
                    'vol': (int(r.get('vol')) if (r.get('vol') is not None and not pd.isna(r.get('vol'))) else None),
                    'amount': None if pd.isna(r.get('amount')) else float(r.get('amount'))
                })
                rows += 1
        return rows


    def get_all_ts_codes() -> list:
        """Return all ts_code values from stock_basic ordered."""
        with engine.connect() as conn:
            res = conn.execute(text("SELECT ts_code FROM stock_basic ORDER BY ts_code"))
            return [r[0] for r in res.fetchall()]


    def get_max_trade_date(ts_code: str):
        with engine.connect() as conn:
            res = conn.execute(text("SELECT MAX(trade_date) FROM stock_daily WHERE ts_code=:ts"), {"ts": ts_code})
            row = res.fetchone()
            return row[0] if row is not None else None


    def upsert_dividend_df(df: pd.DataFrame) -> int:
        """Upsert rows from a dividend DataFrame into stock_dividend."""
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO stock_dividend (ts_code, ann_date, imp_ann_date, record_date, ex_date, pay_date, div_cash, div_stock, bonus_ratio)"
            " VALUES (:ts_code, :ann_date, :imp_ann_date, :record_date, :ex_date, :pay_date, :div_cash, :div_stock, :bonus_ratio)"
            " ON DUPLICATE KEY UPDATE div_cash=VALUES(div_cash), div_stock=VALUES(div_stock), bonus_ratio=VALUES(bonus_ratio)"
        )
        rows = 0
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
        return rows


    def upsert_financial_statement(df: pd.DataFrame, statement_type: str) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO financial_statement (ts_code, statement_type, ann_date, end_date, report_date, data) VALUES (:ts_code, :statement_type, :ann_date, :end_date, :report_date, :data)"
        )
        count = 0
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


    def upsert_daily_basic(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
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

        rows = 0
        with engine.begin() as conn:
            for r in df.to_dict(orient='records'):
                conn.execute(insert_sql, {
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
        return rows


    def upsert_adj_factor(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (:ts_code, :trade_date, :adj_factor) ON DUPLICATE KEY UPDATE adj_factor=VALUES(adj_factor)"
        )
        rows = 0
        with engine.begin() as conn:
            for r in df.to_dict(orient='records'):
                adj = r.get('adj_factor')
                if pd.isna(adj):
                    adj = None
                conn.execute(insert_sql, {
                    'ts_code': r.get('ts_code'),
                    'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                    'adj_factor': adj
                })
                rows += 1
        return rows


    def upsert_moneyflow(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
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

        rows = 0
        with engine.begin() as conn:
            for r in df.to_dict(orient='records'):
                conn.execute(insert_sql, {
                    'ts_code': r.get('ts_code'),
                    'trade_date': (pd.to_datetime(r.get('trade_date')).date() if r.get('trade_date') else None),
                    'net_mf': clean(r.get('net_mf')),
                    'buy_small': clean(r.get('buy_sm_vol') or r.get('buy_small')),
                    'sell_small': clean(r.get('sell_sm_vol') or r.get('sell_small')),
                    'buy_medium': clean(r.get('buy_md_vol') or r.get('buy_medium')),
                    'sell_medium': clean(r.get('buy_md_vol') or r.get('buy_medium')),
                    'buy_large': clean(r.get('buy_lg_vol') or r.get('buy_large')),
                    'sell_large': clean(r.get('buy_lg_vol') or r.get('buy_large')),
                    'buy_huge': clean(r.get('buy_hu_vol') or r.get('buy_huge')),
                    'sell_huge': clean(r.get('buy_hu_vol') or r.get('buy_huge'))
                })
                rows += 1
        return rows


    def upsert_top10_holders(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO top10_holders (ts_code, end_date, holder_name, hold_amount, hold_ratio)"
            " VALUES (:ts_code, :end_date, :holder_name, :hold_amount, :hold_ratio)"
            " ON DUPLICATE KEY UPDATE hold_amount=VALUES(hold_amount), hold_ratio=VALUES(hold_ratio)"
        )
        rows = 0
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
        return rows


    def upsert_margin(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO stock_margin (ts_code, trade_date, financing_balance, financing_buy, financing_repay, securities_lend_balance)"
            " VALUES (:ts_code, :trade_date, :financing_balance, :financing_buy, :financing_repay, :securities_lend_balance)"
            " ON DUPLICATE KEY UPDATE financing_balance=VALUES(financing_balance), financing_buy=VALUES(financing_buy), financing_repay=VALUES(financing_repay), securities_lend_balance=VALUES(securities_lend_balance)"
        )
        rows = 0
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
        return rows


    def upsert_block_trade(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO block_trade (ts_code, trade_date, trade_time, price, volume, amount, side)"
            " VALUES (:ts_code, :trade_date, :trade_time, :price, :volume, :amount, :side)"
        )
        rows = 0
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
        return rows


    def upsert_stock_basic(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO stock_basic (ts_code, symbol, name, area, industry, fullname, enname, market, exchange, list_status, list_date, delist_date, is_hs)"
            " VALUES (:ts_code, :symbol, :name, :area, :industry, :fullname, :enname, :market, :exchange, :list_status, :list_date, :delist_date, :is_hs)"
            " ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), name=VALUES(name), area=VALUES(area), industry=VALUES(industry), fullname=VALUES(fullname), enname=VALUES(enname), market=VALUES(market), exchange=VALUES(exchange), list_status=VALUES(list_status), list_date=VALUES(list_date), delist_date=VALUES(delist_date), is_hs=VALUES(is_hs)"
        )
        rows = 0
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
                    'list_date': (pd.to_datetime(r.get('list_date')).date() if r.get('list_date') else None),
                    'delist_date': (pd.to_datetime(r.get('delist_date')).date() if r.get('delist_date') else None),
                    'is_hs': r.get('is_hs')
                })
                rows += 1
        return rows


    def upsert_repo_df(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO repo (repo_date, instrument, rate, amount) VALUES (:repo_date, :instrument, :rate, :amount)"
        )
        rows = 0
        with engine.begin() as conn:
            for r in df.to_dict(orient='records'):
                conn.execute(insert_sql, {
                    'repo_date': (pd.to_datetime(r.get('repo_date')).date() if r.get('repo_date') else None),
                    'instrument': r.get('instrument'),
                    'rate': None if pd.isna(r.get('rate')) else float(r.get('rate')),
                    'amount': None if pd.isna(r.get('amount')) else float(r.get('amount'))
                })
                rows += 1
        return rows


    def fetch_stock_daily_rows(ts_code: str, start_date=None):
        """Fetch rows from stock_daily for a given ts_code and optional start_date. Returns list of rows."""
        q = "SELECT trade_date, open, high, low, close, vol, amount FROM stock_daily WHERE ts_code = :ts_code"
        params = {'ts_code': ts_code}
        if start_date is not None:
            q += " AND trade_date >= :start_date"
            params['start_date'] = start_date
        q += " ORDER BY trade_date ASC"
        with engine.connect() as conn:
            res = conn.execute(text(q), params)
            return res.fetchall()


    def fetch_existing_keys(table: str, key_date_col: str, start_date=None, end_date=None):
        """Generic fetch of existing keys (ts_code, date) for a table between dates."""
        q = f"SELECT ts_code, {key_date_col} FROM {table} WHERE {key_date_col} BETWEEN :s AND :e"
        with engine.connect() as conn:
            res = conn.execute(text(q), {"s": start_date, "e": end_date})
            existing = set()
            for r in res.fetchall():
                ts = r[0]
                d = r[1]
                if d is None:
                    continue
                try:
                    dval = d if isinstance(d, (str,)) else d
                except Exception:
                    dval = d
                if hasattr(dval, 'isoformat'):
                    dstr = dval.isoformat()
                else:
                    dstr = str(dval)
                existing.add((ts, dstr))
            return existing


    def get_failed_ts_codes(limit: int = None):
        q = "SELECT DISTINCT JSON_UNQUOTE(JSON_EXTRACT(params,'$.ts_code')) AS ts FROM ingest_audit WHERE api_name='daily' AND status='error'"
        if limit:
            q += f" LIMIT {int(limit)}"
        with engine.connect() as conn:
            res = conn.execute(text(q))
            return [r[0] for r in res.fetchall() if r[0]]




    def upsert_top10_holders(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO top10_holders (ts_code, end_date, holder_name, hold_amount, hold_ratio)"
            " VALUES (:ts_code, :end_date, :holder_name, :hold_amount, :hold_ratio)"
            " ON DUPLICATE KEY UPDATE hold_amount=VALUES(hold_amount), hold_ratio=VALUES(hold_ratio)"
        )
        rows = 0
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
        return rows


    def upsert_margin(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO stock_margin (ts_code, trade_date, financing_balance, financing_buy, financing_repay, securities_lend_balance)"
            " VALUES (:ts_code, :trade_date, :financing_balance, :financing_buy, :financing_repay, :securities_lend_balance)"
            " ON DUPLICATE KEY UPDATE financing_balance=VALUES(financing_balance), financing_buy=VALUES(financing_buy), financing_repay=VALUES(financing_repay), securities_lend_balance=VALUES(securities_lend_balance)"
        )
        rows = 0
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
        return rows


    def upsert_block_trade(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO block_trade (ts_code, trade_date, trade_time, price, volume, amount, side)"
            " VALUES (:ts_code, :trade_date, :trade_time, :price, :volume, :amount, :side)"
        )
        rows = 0
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
        return rows


    def upsert_stock_basic(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO stock_basic (ts_code, symbol, name, area, industry, fullname, enname, market, exchange, list_status, list_date, delist_date, is_hs)"
            " VALUES (:ts_code, :symbol, :name, :area, :industry, :fullname, :enname, :market, :exchange, :list_status, :list_date, :delist_date, :is_hs)"
            " ON DUPLICATE KEY UPDATE symbol=VALUES(symbol), name=VALUES(name), area=VALUES(area), industry=VALUES(industry), fullname=VALUES(fullname), enname=VALUES(enname), market=VALUES(market), exchange=VALUES(exchange), list_status=VALUES(list_status), list_date=VALUES(list_date), delist_date=VALUES(delist_date), is_hs=VALUES(is_hs)"
        )
        rows = 0
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
                    'list_date': (pd.to_datetime(r.get('list_date')).date() if r.get('list_date') else None),
                    'delist_date': (pd.to_datetime(r.get('delist_date')).date() if r.get('delist_date') else None),
                    'is_hs': r.get('is_hs')
                })
                rows += 1
        return rows


    def upsert_repo_df(df: pd.DataFrame) -> int:
        if df is None or df.empty:
            return 0
        insert_sql = text(
            "INSERT INTO repo (repo_date, instrument, rate, amount) VALUES (:repo_date, :instrument, :rate, :amount)"
        )
        rows = 0
