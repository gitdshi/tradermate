#!/usr/bin/env python3
"""Test if benchmark data appears in backtest results after refactoring to use AkShare."""
import sys
import time
from datetime import date
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

from app.api.services.backtest_service import BacktestService, get_benchmark_data

print("=" * 80)
print("TESTING BENCHMARK IN BACKTEST RESULTS")
print("=" * 80)

# Step 1: Test benchmark fetch directly
print("\n[1/3] Testing benchmark fetch from AkShare...")
try:
    bm_data = get_benchmark_data(date(2026, 1, 2), date(2026, 1, 15), "000300.SH")
    if bm_data:
        print(f"✓ Benchmark fetched successfully")
        print(f"  - Has 'prices': {bool(bm_data.get('prices'))}")
        print(f"  - Price count: {len(bm_data.get('prices', []))}")
        returns_val = bm_data.get('returns')
        print(f"  - Has 'returns': {returns_val is not None and len(returns_val) > 0}")
        print(f"  - Sample price: {bm_data.get('prices', [{}])[0] if bm_data.get('prices') else 'N/A'}")
    else:
        print("✗ Benchmark fetch returned None")
        sys.exit(1)
except Exception as e:
    print(f"✗ Benchmark fetch failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 2: Run a simple backtest
print("\n[2/3] Running sample backtest...")
service = BacktestService()
try:
    result = service.run_single_backtest(
        strategy_id=None,
        strategy_class='TripleMAStrategy',
        vt_symbol='000001.SZSE',  # VNPY uses SZSE not SZ
        start_date=date(2026, 1, 2),
        end_date=date(2026, 1, 15),
        parameters={},
        capital=100000.0
    )
    
    if result:
        print(f"✓ Backtest completed")
        print(f"  - Total return: {result.total_return}%")
        print(f"  - Total trades: {result.total_trades}")
    else:
        print("✗ Backtest returned None")
        sys.exit(1)
except Exception as e:
    print(f"✗ Backtest failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 3: Check benchmark fields in result
print("\n[3/3] Checking benchmark data in backtest result...")
has_benchmark = False
issues = []

if hasattr(result, 'benchmark_curve') and result.benchmark_curve:
    print(f"✓ benchmark_curve present: {len(result.benchmark_curve)} points")
    has_benchmark = True
else:
    print(f"✗ benchmark_curve missing or empty")
    issues.append("benchmark_curve missing")

if hasattr(result, 'benchmark_return') and result.benchmark_return is not None:
    print(f"✓ benchmark_return: {result.benchmark_return}%")
    has_benchmark = True
else:
    print(f"✗ benchmark_return missing")
    issues.append("benchmark_return missing")

if hasattr(result, 'benchmark_symbol') and result.benchmark_symbol:
    print(f"✓ benchmark_symbol: {result.benchmark_symbol}")
else:
    print(f"✗ benchmark_symbol missing")
    issues.append("benchmark_symbol missing")

if hasattr(result, 'alpha') and result.alpha is not None:
    print(f"✓ alpha: {result.alpha}")
else:
    print(f"✗ alpha missing")
    issues.append("alpha missing")

if hasattr(result, 'beta') and result.beta is not None:
    print(f"✓ beta: {result.beta}")
else:
    print(f"✗ beta missing")
    issues.append("beta missing")

print("\n" + "=" * 80)
if has_benchmark and not issues:
    print("✓ SUCCESS: Benchmark data is present in backtest results!")
    sys.exit(0)
elif has_benchmark:
    print(f"⚠ PARTIAL: Some benchmark fields missing: {', '.join(issues)}")
    sys.exit(1)
else:
    print("✗ FAILURE: No benchmark data in backtest results!")
    sys.exit(1)
