#!/usr/bin/env python3
"""Test RQ integration."""
import sys
from pathlib import Path
from datetime import date

# Add project root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.api.services.backtest_service import get_backtest_service
from app.api.services.job_storage import get_job_storage
import time

def test_job_submission():
    """Test submitting a backtest job."""
    print("=" * 60)
    print("Testing RQ Integration")
    print("=" * 60)
    
    service = get_backtest_service()
    
    # Submit a test backtest
    print("\n1. Submitting backtest job...")
    try:
        job_id = service.submit_backtest(
            user_id=1,
            strategy_id=None,
            strategy_class_name="TripleMAStrategy",
            symbol="000001.SZ",
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
            initial_capital=100000.0,
            parameters={"fast_window": 5, "slow_window": 20}
        )
        print(f"✅ Job submitted: {job_id}")
    except Exception as e:
        print(f"❌ Failed to submit job: {e}")
        return
    
    # Check job status
    print("\n2. Checking job status...")
    try:
        status = service.get_job_status(job_id, user_id=1)
        print(f"✅ Job status: {status['status']}")
        print(f"   Progress: {status.get('progress', 0)}%")
    except Exception as e:
        print(f"❌ Failed to get status: {e}")
    
    # List user jobs
    print("\n3. Listing user jobs...")
    try:
        jobs = service.list_user_jobs(user_id=1, limit=5)
        print(f"✅ Found {len(jobs)} jobs")
        for job in jobs:
            print(f"   - {job['job_id']}: {job['status']}")
    except Exception as e:
        print(f"❌ Failed to list jobs: {e}")
    
    # Get queue stats
    print("\n4. Getting queue statistics...")
    try:
        job_storage = get_job_storage()
        stats = job_storage.get_queue_stats()
        print("✅ Queue stats:")
        for queue_name, queue_stats in stats.items():
            print(f"   {queue_name}: {queue_stats}")
    except Exception as e:
        print(f"❌ Failed to get stats: {e}")
    
    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("\nTo monitor the worker:")
    print("  docker-compose logs -f worker")
    print("\nTo check queue status:")
    print("  rq info --url redis://localhost:6379/0")
    print("=" * 60)

if __name__ == "__main__":
    test_job_submission()
