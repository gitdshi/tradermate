"""Backtest history DAO.

All SQL touching `tradermate.backtest_history` lives here.
"""

from __future__ import annotations

from datetime import datetime
import numpy as np
from typing import Any, Optional
import json

from app.infrastructure.db.connections import connection


class BacktestHistoryDao:
    def upsert_history(
        self,
        *,
        user_id: int,
        job_id: str,
        strategy_id: Optional[int],
        strategy_class: Optional[str],
        strategy_version: Optional[int],
        vt_symbol: str,
        start_date: str,
        end_date: str,
        parameters: dict[str, Any],
        status: str,
        result: Optional[dict[str, Any]],
        error: Optional[str],
        created_at: datetime,
        completed_at: Optional[datetime],
        bulk_job_id: Optional[str] = None,
    ) -> None:
        def _json_default(o):
            # handle numpy types and datetimes
            try:
                if isinstance(o, np.ndarray):
                    return o.tolist()
            except Exception:
                pass
            try:
                # numpy scalar types
                if hasattr(o, 'item') and (isinstance(o, (np.generic,))):
                    return o.item()
            except Exception:
                pass
            if isinstance(o, datetime):
                return o.isoformat()
            # fallback
            return str(o)

        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text(
                    """
                    INSERT INTO backtest_history
                    (user_id, job_id, bulk_job_id, strategy_id, strategy_class, strategy_version, vt_symbol,
                     start_date, end_date, parameters, status, result, error, created_at, completed_at)
                    VALUES
                    (:user_id, :job_id, :bulk_job_id, :strategy_id, :strategy_class, :strategy_version, :vt_symbol,
                     :start_date, :end_date, :parameters, :status, :result, :error, :created_at, :completed_at)
                    ON DUPLICATE KEY UPDATE
                      status = :status,
                      result = :result,
                      error = :error,
                      completed_at = :completed_at
                    """
                ),
                {
                    "user_id": user_id,
                    "job_id": job_id,
                    "bulk_job_id": bulk_job_id,
                    "strategy_id": strategy_id,
                    "strategy_class": strategy_class,
                    "strategy_version": strategy_version,
                    "vt_symbol": vt_symbol,
                    "start_date": start_date,
                    "end_date": end_date,
                    "parameters": json.dumps(parameters or {}, default=_json_default),
                    "status": status,
                    "result": (json.dumps(result, default=_json_default) if result is not None else None),
                    "error": error,
                    "created_at": created_at,
                    "completed_at": completed_at,
                },
            )
            conn.commit()

    def get_child_result_json(self, job_id: str) -> Optional[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT result FROM backtest_history WHERE job_id = :jid LIMIT 1"),
                {"jid": job_id},
            ).fetchone()
            if not row or not row.result:
                return None
            try:
                return json.loads(row.result) if isinstance(row.result, str) else row.result
            except Exception:
                return None

    def get_job_row(self, job_id: str) -> Optional[dict[str, Any]]:
        """Fetch a backtest_history row by job_id."""
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    """
                    SELECT job_id, user_id, bulk_job_id, strategy_id, strategy_class,
                           strategy_version, vt_symbol, start_date, end_date,
                           parameters, status, result, error, created_at, completed_at
                    FROM backtest_history
                    WHERE job_id = :jid
                    LIMIT 1
                    """
                ),
                {"jid": job_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def delete_single(self, job_id: str, user_id: int) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text("DELETE FROM backtest_history WHERE job_id = :job_id AND user_id = :user_id"),
                {"job_id": job_id, "user_id": user_id},
            )
            conn.commit()

    def delete_bulk_children(self, bulk_job_id: str, user_id: int) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text("DELETE FROM backtest_history WHERE bulk_job_id = :bulk_job_id AND user_id = :user_id"),
                {"bulk_job_id": bulk_job_id, "user_id": user_id},
            )
            conn.commit()

    def count_for_user(self, user_id: int) -> int:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT COUNT(*) as total FROM backtest_history WHERE user_id = :user_id"),
                {"user_id": user_id},
            ).fetchone()
            return int(row.total) if row and hasattr(row, "total") else 0

    def list_for_user(self, *, user_id: int, limit: int, offset: int) -> list[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT id, job_id, strategy_id, strategy_class, strategy_version, vt_symbol,
                           start_date, end_date, status, result, created_at, completed_at
                    FROM backtest_history
                    WHERE user_id = :user_id
                    ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {"user_id": user_id, "limit": limit, "offset": offset},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_detail_for_user(self, *, job_id: str, user_id: int) -> Optional[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    """
                    SELECT id, job_id, strategy_id, strategy_class, strategy_version, vt_symbol,
                           start_date, end_date, parameters, status, result, error,
                           created_at, completed_at
                    FROM backtest_history
                    WHERE job_id = :job_id AND user_id = :user_id
                    """
                ),
                {"job_id": job_id, "user_id": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None
