"""Backtests DAOs (tradermate DB + akshare DB where appropriate).

No cross-DB joins: benchmark/index reads (akshare) are separate from history persistence (tradermate).
"""
