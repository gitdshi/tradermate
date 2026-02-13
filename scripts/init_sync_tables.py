"""Create required tables: tradermate.data_sync_status and akshare.trade_cal

This script delegates to the domain DAO to ensure tables exist.
Run:
    /usr/local/bin/python3 scripts/init_sync_tables.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.domains.extdata.dao.data_sync_status_dao import ensure_tables


def main():
    print('Ensuring sync tables via DAO')
    ensure_tables()
    print('Tables ensured')


if __name__ == '__main__':
    main()
"""
Remaining legacy SQL DDL removed — table creation delegated to DAO `ensure_tables()`.
"""
