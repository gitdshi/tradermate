"""
Tushare sync daemon (moved under app.datasync.service)

This module delegates to `tushare_ingest` helpers now located in the
`app.datasync.service` package.
"""

import os
import time
import logging
import argparse
from datetime import datetime, timedelta, date

import pandas as pd

from app.datasync.service import tushare_ingest as ti
from app.domains.extdata.dao.sync_log_dao import (
    write_tushare_stock_sync_log as dao_write_tushare_stock_sync_log,
    get_last_success_tushare_sync_date as dao_get_last_success_tushare_sync_date,
)

logging.basicConfig(level=logging.INFO)

ENGINE = ti.engine
CALL_PRO = ti.call_pro

SYNC_HOUR = os.getenv('SYNC_HOUR', '02:00')
DRY_RUN = os.getenv('DRY_RUN', '0') == '1'

ENDPOINTS = {
    'daily': lambda dt: ti.ingest_all_daily(start_date=None, sleep_between=0.02),
    'daily_by_date': None,
    'daily_basic': lambda dt: ti.ingest_all_other_data(),
    'adj_factor': lambda dt: ti.ingest_all_other_data(),
    'moneyflow': lambda dt: ti.ingest_all_other_data(),
    'dividend': lambda dt: ti.ingest_all_other_data(),
    'top10_holders': lambda dt: ti.ingest_all_other_data(),
    'margin': lambda dt: ti.ingest_all_other_data(),
    'block_trade': lambda dt: ti.ingest_all_other_data(),
    'repo': lambda dt: ti.ingest_repo(repo_date=dt.strftime('%Y-%m-%d'))
}


def get_trade_days(start_d: date, end_d: date):
    s = start_d.strftime('%Y%m%d')
    e = end_d.strftime('%Y%m%d')
    try:
        df = CALL_PRO('trade_cal', exchange='SSE', start_date=s, end_date=e)
        if df is None:
            raise Exception('trade_cal returned None')
        df = df[df['is_open'] == 1]
        col = 'calendar_date' if 'calendar_date' in df.columns else ('cal_date' if 'cal_date' in df.columns else None)
        dates = [str(pd.to_datetime(d).date()) for d in df[col]] if col else []
        return dates
    except Exception as exc:
        logging.warning('Could not use trade_cal (fallback to weekdays): %s', exc)
        days = []
        cur = start_d
        while cur <= end_d:
            if cur.weekday() < 5:
                days.append(str(cur))
            cur = cur + timedelta(days=1)
        return days


def write_sync_log(sync_date: date, endpoint: str, status: str, rows: int = 0, err: str = None):
    if DRY_RUN:
        logging.info('DRY RUN - skip writing sync log: %s %s %s', sync_date, endpoint, status)
        return
    dao_write_tushare_stock_sync_log(sync_date, endpoint, status, rows, err)


def get_last_success_date(endpoint: str):
    return dao_get_last_success_tushare_sync_date(endpoint)


def sync_daily_for_date(d: date):
    logging.info('Starting daily sync for %s', d)
    ts_codes = ti.get_all_ts_codes()
    total = len(ts_codes)
    rows_total = 0
    failures = 0
    for i, ts_code in enumerate(ts_codes, start=1):
        try:
            ti.ingest_daily(ts_code=ts_code, start_date=d.strftime('%Y%m%d'), end_date=d.strftime('%Y%m%d'))
        except Exception as e:
            failures += 1
            logging.warning('Failed daily for %s on %s: %s', ts_code, d, e)
        time.sleep(0.02)
        if i % 500 == 0:
            logging.info('Daily sync progress: %d/%d', i, total)
    status = 'success' if failures == 0 else 'partial' if failures < total else 'error'
    write_sync_log(d, 'daily', status, rows_total, f'failures={failures}' if failures else None)
    logging.info('Daily sync finished for %s: status=%s failures=%d', d, status, failures)


def run_sync_for_date(d: date, allowed_endpoints: list):
    logging.info('Running sync for date %s, endpoints: %s', d, allowed_endpoints)
    for ep in allowed_endpoints:
        try:
            if ep == 'daily':
                sync_daily_for_date(d)
            elif ep == 'repo':
                try:
                    if not DRY_RUN:
                        ti.ingest_repo(repo_date=d.strftime('%Y-%m-%d'))
                        write_sync_log(d, 'repo', 'success', 0, None)
                except Exception as e:
                    write_sync_log(d, 'repo', 'error', 0, str(e))
            else:
                try:
                    if ep == 'daily_basic':
                        ti.ingest_daily_basic()
                    if ep in ('daily_basic','adj_factor','moneyflow','dividend','top10_holders','margin','block_trade'):
                        ti.ingest_all_other_data()
                        write_sync_log(d, ep, 'success', 0, None)
                except Exception as e:
                    write_sync_log(d, ep, 'error', 0, str(e))
        except Exception as e:
            logging.exception('Error syncing endpoint %s for %s: %s', ep, d, e)
            write_sync_log(d, ep, 'error', 0, str(e))
