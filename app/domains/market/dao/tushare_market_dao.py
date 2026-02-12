"""Tushare market data DAO.

All SQL touching `tushare.stock_basic` and `tushare.stock_daily` for the Data API lives here.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

from app.infrastructure.db.connections import connection


class TushareMarketDao:
    def list_stock_basic(
        self,
        *,
        exchange: Optional[str] = None,
        keyword: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        query = (
            "SELECT ts_code, name, exchange, industry, list_date "
            "FROM stock_basic "
            "WHERE (list_status = 'L' OR list_status IS NULL)"
        )
        params: dict[str, Any] = {}

        if exchange:
            query += " AND exchange = :exchange"
            params["exchange"] = exchange
        if keyword:
            query += " AND (ts_code LIKE :kw OR name LIKE :kw)"
            params["kw"] = f"%{keyword}%"

        query += " ORDER BY ts_code LIMIT :limit OFFSET :offset"
        params["limit"] = int(limit)
        params["offset"] = int(offset)

        with connection("tushare") as conn:
            from sqlalchemy import text
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_stock_daily_history(
        self,
        *,
        ts_code: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        with connection("tushare") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT trade_date, open, high, low, close, vol, amount
                    FROM stock_daily
                    WHERE ts_code = :ts_code
                      AND trade_date >= :start_date
                      AND trade_date <= :end_date
                    ORDER BY trade_date ASC
                    """
                ),
                {
                    "ts_code": ts_code,
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                },
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def exchange_counts(self) -> dict[str, int]:
        with connection("tushare") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT exchange, COUNT(*) as count
                    FROM stock_basic
                    WHERE (list_status = 'L' OR list_status IS NULL)
                      AND exchange IS NOT NULL
                    GROUP BY exchange
                    """
                )
            ).fetchall()
            return {r.exchange: int(r.count) for r in rows}

    def stock_daily_date_range(self) -> dict[str, Any]:
        with connection("tushare") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date FROM stock_daily")
            ).fetchone()
            if not row:
                return {"min_date": None, "max_date": None}
            return {"min_date": row.min_date, "max_date": row.max_date}

    def sectors(self) -> list[dict[str, Any]]:
        with connection("tushare") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT industry, COUNT(*) as count
                    FROM stock_basic
                    WHERE (list_status = 'L' OR list_status IS NULL)
                      AND industry IS NOT NULL
                    GROUP BY industry
                    ORDER BY count DESC
                    """
                )
            ).fetchall()
            return [{"name": r.industry, "count": int(r.count)} for r in rows]

    def exchanges(self) -> list[dict[str, Any]]:
        with connection("tushare") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT exchange, COUNT(*) as count
                    FROM stock_basic
                    WHERE (list_status = 'L' OR list_status IS NULL)
                      AND exchange IS NOT NULL
                    GROUP BY exchange
                    ORDER BY count DESC
                    """
                )
            ).fetchall()
            return [{"exchange": r.exchange, "count": int(r.count)} for r in rows]

    def symbols_by_filter(
        self,
        *,
        industry: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        query = "SELECT ts_code, name, industry, exchange FROM stock_basic WHERE 1=1"
        params: dict[str, Any] = {"limit": int(limit)}
        if industry:
            query += " AND industry = :industry"
            params["industry"] = industry
        if exchange:
            query += " AND exchange = :exchange"
            params["exchange"] = exchange
        query += " ORDER BY ts_code LIMIT :limit"

        with connection("tushare") as conn:
            from sqlalchemy import text
            rows = conn.execute(text(query), params).fetchall()
            return [dict(r._mapping) for r in rows]
