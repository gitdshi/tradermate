"""AkShare benchmark DAO.

All SQL touching `akshare.index_daily` for benchmark series retrieval lives here.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

import numpy as np

from app.infrastructure.db.connections import connection


class AkshareBenchmarkDao:
    def get_index_series(self, *, index_code: str, start: date, end: date) -> list[dict[str, Any]]:
        with connection("akshare") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT trade_date, close
                    FROM index_daily
                    WHERE index_code = :index_code
                      AND trade_date >= :start_date
                      AND trade_date <= :end_date
                    ORDER BY trade_date ASC
                    """
                ),
                {
                    "index_code": index_code,
                    "start_date": start.strftime("%Y-%m-%d"),
                    "end_date": end.strftime("%Y-%m-%d"),
                },
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_benchmark_data(self, *, start: date, end: date, benchmark_symbol: str) -> Optional[dict[str, Any]]:
        """Return returns/total_return/prices for a benchmark index."""
        candidates = [benchmark_symbol]
        if benchmark_symbol and benchmark_symbol.endswith('.SH') and benchmark_symbol.startswith('000'):
            candidates.append(benchmark_symbol.replace('000', '399').replace('.SH', '.SZ'))
        if benchmark_symbol and (benchmark_symbol.endswith('.SH') or benchmark_symbol.endswith('.SZ')):
            alt = benchmark_symbol[:-3] + ('.SZ' if benchmark_symbol.endswith('.SH') else '.SH')
            candidates.append(alt)

        rows: list[dict[str, Any]] = []
        for idx_code in dict.fromkeys(candidates):
            rows = self.get_index_series(index_code=idx_code, start=start, end=end)
            if rows and len(rows) >= 2:
                break

        if not rows or len(rows) < 2:
            return None

        dates = [r.get('trade_date') for r in rows]
        closes = np.array([float(r.get('close')) for r in rows], dtype=float)
        daily_returns = np.diff(closes) / closes[:-1]
        total_return = (closes[-1] - closes[0]) / closes[0] * 100

        prices = []
        for dt_val, close_val in zip(dates, closes):
            if isinstance(dt_val, str):
                try:
                    dt_obj = datetime.strptime(dt_val, "%Y%m%d")
                except Exception:
                    try:
                        dt_obj = datetime.fromisoformat(dt_val)
                    except Exception:
                        dt_obj = None
            else:
                try:
                    dt_obj = datetime.combine(dt_val, datetime.min.time())
                except Exception:
                    dt_obj = None

            prices.append({"datetime": dt_obj.isoformat() if dt_obj else None, "close": float(close_val)})

        return {
            "returns": daily_returns,
            "total_return": float(total_return),
            "prices": prices,
        }
