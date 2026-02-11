#!/usr/bin/env python3
"""Debug backtest submission."""
import sys
import os
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# Test queue connection
try:
    from app.api.worker.config import get_queue
    q = get_queue('backtest')
    print(f'Queue connected: {q.name}, {q.count} jobs')
except Exception as e:
    print(f'Queue error: {e}')
    traceback.print_exc()
    sys.exit(1)

# Test DB connection
try:
    from app.api.services.db import get_db_connection
    conn = get_db_connection()
    print('DB connected')
    conn.close()
except Exception as e:
    print(f'DB error: {e}')
    traceback.print_exc()
    sys.exit(1)

# Test submit_backtest
try:
    from app.api.services.backtest_service import get_backtest_service
    from datetime import date
    
    service = get_backtest_service()
    print('Service created')
    
    # Get strategy info
    code, class_name = service._get_strategy_from_db(8, 1)
    print(f'Strategy: {class_name}, code length: {len(code)}')
    
    job_id = service.submit_backtest(
        user_id=1,
        strategy_id=8,
        strategy_class_name=None,
        symbol='000001.SZ',
        start_date=date(2025, 1, 1),
        end_date=date(2025, 6, 30),
        initial_capital=100000.0
    )
    print(f'Job submitted: {job_id}')
except Exception as e:
    print(f'Service error: {e}')
    traceback.print_exc()
