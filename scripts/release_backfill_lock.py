#!/usr/bin/env python3
"""Utility to release the backfill DB lock (manual fallback)."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.domains.extdata.dao.data_sync_status_dao import release_backfill_lock, release_stale_backfill_lock

def main():
    # Try normal release first
    try:
        release_backfill_lock()
        print('Released backfill lock')
        return
    except Exception as e:
        print('Normal release failed:', e)

    # Try stale release
    try:
        released = release_stale_backfill_lock(int(os.getenv('BACKFILL_LOCK_STALE_HOURS', '6')))
        if released:
            print('Released stale backfill lock')
        else:
            print('No stale lock detected or failed to release')
    except Exception as e:
        print('Stale release attempt failed:', e)

if __name__ == '__main__':
    main()
