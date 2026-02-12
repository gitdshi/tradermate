"""Bulk backtest results DAO.

All SQL for bulk child pagination/ordering and summary inputs lives here.
"""

from __future__ import annotations

from typing import Any, Optional

from app.infrastructure.db.connections import connection


class BulkResultsDao:
    def count_children(self, *, bulk_job_id: str, user_id: int) -> int:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT COUNT(*) as cnt FROM backtest_history WHERE bulk_job_id = :bjid AND user_id = :uid"),
                {"bjid": bulk_job_id, "uid": user_id},
            ).fetchone()
            return int(row.cnt) if row and hasattr(row, "cnt") else 0

    def list_children_page(
        self,
        *,
        bulk_job_id: str,
        user_id: int,
        page: int,
        page_size: int,
        sort_order: str,
    ) -> list[dict[str, Any]]:
        order_dir = "ASC" if sort_order == "asc" else "DESC"
        offset = (page - 1) * page_size

        with connection("tradermate") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    f"""
                    SELECT job_id, vt_symbol, status, result, error, parameters, created_at, completed_at
                    FROM backtest_history
                    WHERE bulk_job_id = :bjid AND user_id = :uid
                    ORDER BY
                        CASE WHEN result IS NOT NULL
                             THEN CAST(JSON_EXTRACT(result, '$.statistics.total_return') AS DOUBLE)
                             ELSE NULL END {order_dir}
                    LIMIT :lim OFFSET :off
                    """
                ),
                {"bjid": bulk_job_id, "uid": user_id, "lim": page_size, "off": offset},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def list_all_children(self, *, bulk_job_id: str, user_id: int) -> list[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT job_id, vt_symbol, status, result, error
                    FROM backtest_history
                    WHERE bulk_job_id = :bjid AND user_id = :uid
                    """
                ),
                {"bjid": bulk_job_id, "uid": user_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]
