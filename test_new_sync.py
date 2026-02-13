#!/usr/bin/env python3
"""Test script for new sync coordinator."""

import sys
import os
from datetime import date, timedelta

# Add app to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("Testing new sync coordinator imports...")
try:
    from app.datasync.service.sync_coordinator import (
        daily_ingest,
        missing_data_backfill,
        initialize_sync_status_table,
        refresh_trade_calendar,
        SyncStep,
        SyncStatus
    )
    print("✓ Imports successful")
    
    print("\n1. Testing SyncStep enum:")
    for step in SyncStep:
        print(f"  - {step.value}")
    
    print("\n2. Testing SyncStatus enum:")
    for status in SyncStatus:
        print(f"  - {status.value}")
    
    print("\n3. Testing CLI integration:")
    from app.datasync.service.data_sync_daemon import main
    print("  ✓ CLI imports successful")
    
    print("\nAll tests passed! ✓")
    
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
