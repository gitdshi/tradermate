#!/usr/bin/env python3
"""Initialize/rebuild TraderMate market data after DB loss.

This script is intended for operational recovery when `tushare`/`akshare`/`vnpy`
data are missing or empty.

What it does:
1) Applies DB schemas from `mysql/init/*.sql` (idempotent).
2) Rebuilds stock universe (`stock_basic`) from Tushare.
3) Re-ingests full daily history via Tushare (`stock_daily`).
4) Re-ingests AkShare index history (`akshare.index_daily`).
5) Optionally backfills auxiliary Tushare datasets (adj/dividend/top10).
6) Syncs all available stock daily bars into `vnpy.dbbardata`.
7) Initializes `data_sync_status` from existing data.

Usage examples:
    PYTHONPATH=. .venv/bin/python3 scripts/init_market_data.py
    PYTHONPATH=. .venv/bin/python3 scripts/init_market_data.py --skip-aux
    PYTHONPATH=. .venv/bin/python3 scripts/init_market_data.py --stock-statuses L,D,P
    PYTHONPATH=. .venv/bin/python3 scripts/init_market_data.py --skip-schema --skip-vnpy
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

from sqlalchemy import text, create_engine
from sqlalchemy.engine.url import make_url


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.datasync.service.tushare_ingest import (
    ingest_stock_basic,
    ingest_all_daily,
    ingest_adj_factor_by_date_range,
    ingest_dividend_by_date_range,
    ingest_top10_holders_by_date_range,
)
from app.datasync.service.akshare_ingest import ingest_all_indexes
from app.datasync.service.vnpy_ingest import sync_all_to_vnpy
from app.datasync.service.data_sync_daemon import initialize_sync_status_table
from app.infrastructure.config import get_settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False

    for ch in sql_text:
        if ch == "'" and not in_double:
            in_single = not in_single
            buf.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            buf.append(ch)
            continue
        if ch == ';' and not in_single and not in_double:
            stmt = ''.join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            continue
        buf.append(ch)

    tail = ''.join(buf).strip()
    if tail:
        statements.append(tail)

    cleaned: list[str] = []
    for stmt in statements:
        lines = []
        for line in stmt.splitlines():
            s = line.strip()
            if s.startswith('--'):
                continue
            lines.append(line)
        normalized = '\n'.join(lines).strip()
        if normalized:
            cleaned.append(normalized)
    return cleaned


def get_server_engine():
    settings = get_settings()
    mysql_url = os.getenv('MYSQL_URL', settings.mysql_url)
    url = make_url(mysql_url)
    admin_url = url.set(database=None)
    return create_engine(admin_url, pool_pre_ping=True)


def apply_schema_files() -> None:
    schema_files = [
        ROOT / 'mysql' / 'init' / 'tradermate.sql',
        ROOT / 'mysql' / 'init' / 'tushare.sql',
        ROOT / 'mysql' / 'init' / 'akshare.sql',
        ROOT / 'mysql' / 'init' / 'vnpy.sql',
    ]
    engine = get_server_engine()
    with engine.begin() as conn:
        for file_path in schema_files:
            logger.info('Applying schema: %s', file_path.relative_to(ROOT))
            sql_text = file_path.read_text(encoding='utf-8')
            statements = split_sql_statements(sql_text)
            for stmt in statements:
                conn.exec_driver_sql(stmt)


def print_summary() -> None:
    settings = get_settings()
    engine = create_engine(settings.mysql_url + '/tradermate', pool_pre_ping=True)
    checks = [
        ('tushare.stock_basic', 'SELECT COUNT(*) FROM tushare.stock_basic'),
        ('tushare.stock_daily', 'SELECT COUNT(*) FROM tushare.stock_daily'),
        ('tushare.adj_factor', 'SELECT COUNT(*) FROM tushare.adj_factor'),
        ('akshare.index_daily', 'SELECT COUNT(*) FROM akshare.index_daily'),
        ('vnpy.dbbardata', 'SELECT COUNT(*) FROM vnpy.dbbardata'),
    ]
    logger.info('Recovery summary (row counts):')
    with engine.connect() as conn:
        for name, sql in checks:
            value = conn.execute(text(sql)).scalar() or 0
            logger.info('  %-22s %s', name + ':', f'{value:,}')


def main() -> int:
    parser = argparse.ArgumentParser(description='Initialize TraderMate market data')
    parser.add_argument('--start-date', default='2005-01-01', help='Start date for aux backfill (YYYY-MM-DD)')
    parser.add_argument('--skip-schema', action='store_true', help='Skip schema initialization SQL')
    parser.add_argument('--skip-aux', action='store_true', help='Skip adj/dividend/top10 backfill')
    parser.add_argument('--skip-vnpy', action='store_true', help='Skip vnpy full sync')
    parser.add_argument('--stock-statuses', default='L', help='Comma-separated stock_basic list_status values (e.g. L or L,D,P)')
    parser.add_argument('--batch-size', type=int, default=int(os.getenv('BATCH_SIZE', '100')))
    parser.add_argument('--sleep-between', type=float, default=0.02)
    args = parser.parse_args()

    end_date = date.today().isoformat()
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date().isoformat()

    logger.info('Starting market data initialization (start_date=%s, end_date=%s)', start_date, end_date)

    if not args.skip_schema:
        apply_schema_files()

    statuses = [s.strip().upper() for s in args.stock_statuses.split(',') if s.strip()]
    if not statuses:
        statuses = ['L']

    logger.info('Rebuilding Tushare stock_basic (statuses=%s)', ','.join(statuses))
    for status in statuses:
        try:
            ingest_stock_basic(list_status=status)
        except Exception as exc:
            logger.warning('stock_basic ingest failed for list_status=%s: %s', status, exc)
            if status == 'L':
                raise

    logger.info('Rebuilding full Tushare stock_daily history (this can take a long time)')
    ingest_all_daily(batch_size=args.batch_size, sleep_between=args.sleep_between)

    logger.info('Rebuilding AkShare index history')
    ingest_all_indexes()

    if not args.skip_aux:
        logger.info('Backfilling adj_factor from %s to %s', start_date, end_date)
        ingest_adj_factor_by_date_range(start_date, end_date, batch_size=args.batch_size, sleep_between=args.sleep_between)

        logger.info('Backfilling dividend from %s to %s', start_date, end_date)
        ingest_dividend_by_date_range(start_date, end_date, batch_size=args.batch_size, sleep_between=args.sleep_between)

        logger.info('Backfilling top10_holders from %s to %s', start_date, end_date)
        ingest_top10_holders_by_date_range(start_date, end_date, batch_size=args.batch_size, sleep_between=args.sleep_between)

    if not args.skip_vnpy:
        logger.info('Syncing all stock bars from tushare to vnpy')
        sync_all_to_vnpy(full_refresh=True)

    lookback_years = max(1, date.today().year - datetime.strptime(start_date, '%Y-%m-%d').year + 1)
    logger.info('Initializing data_sync_status table (lookback_years=%d)', lookback_years)
    initialize_sync_status_table(lookback_years=lookback_years)

    print_summary()
    logger.info('Market data initialization finished')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
