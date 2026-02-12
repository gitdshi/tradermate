"""Market domain.

Responsibilities:
- Symbol metadata (name, exchange mappings)
- Price/history queries
- Index/benchmark metadata

No cross-DB joins: DAOs here target either tushare DB or akshare DB.
"""
