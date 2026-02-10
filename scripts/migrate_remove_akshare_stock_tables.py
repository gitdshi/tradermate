"""Idempotent migration: drop AkShare stock tables no longer used.

Run from project root:

    PYTHONPATH=. .venv/bin/python3 scripts/migrate_remove_akshare_stock_tables.py

This will drop `stock_basic` and `stock_daily` from the akshare database if they exist.
"""
import os
from sqlalchemy import create_engine, text

AKSHARE_DB_URL = os.getenv('AKSHARE_DATABASE_URL', 'mysql+pymysql://root:password@127.0.0.1:3306/akshare?charset=utf8mb4')

def main():
    engine = create_engine(AKSHARE_DB_URL)
    with engine.begin() as conn:
        print('Dropping akshare.stock_daily if exists...')
        conn.execute(text('DROP TABLE IF EXISTS stock_daily'))
        print('Dropping akshare.stock_basic if exists...')
        conn.execute(text('DROP TABLE IF EXISTS stock_basic'))
    print('Migration complete.')

if __name__ == '__main__':
    main()
