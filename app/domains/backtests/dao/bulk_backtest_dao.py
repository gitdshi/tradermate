"""Bulk backtest DAO.

All SQL touching `tradermate.bulk_backtest` lives here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from app.infrastructure.db.connections import connection


class BulkBacktestDao:
    def insert_parent(
        self,
        *,
        user_id: int,
        job_id: str,
        strategy_id: Optional[int],
        strategy_class: Optional[str],
        strategy_version: Optional[int],
        symbols_json: str,
        start_date: str,
        end_date: str,
        parameters_json: str,
        initial_capital: float,
        rate: float,
        slippage: float,
        benchmark: str,
        total_symbols: int,
        created_at: datetime,
    ) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text(
                    """
                    INSERT INTO bulk_backtest
                    (user_id, job_id, strategy_id, strategy_class, strategy_version,
                     symbols, start_date, end_date, parameters, initial_capital,
                     rate, slippage, benchmark, status, total_symbols, created_at)
                    VALUES
                    (:user_id, :job_id, :strategy_id, :strategy_class, :strategy_version,
                     :symbols, :start_date, :end_date, :parameters, :initial_capital,
                     :rate, :slippage, :benchmark, 'queued', :total_symbols, :created_at)
                    """
                ),
                {
                    "user_id": user_id,
                    "job_id": job_id,
                    "strategy_id": strategy_id,
                    "strategy_class": strategy_class,
                    "strategy_version": strategy_version,
                    "symbols": symbols_json,
                    "start_date": start_date,
                    "end_date": end_date,
                    "parameters": parameters_json,
                    "initial_capital": initial_capital,
                    "rate": rate,
                    "slippage": slippage,
                    "benchmark": benchmark,
                    "total_symbols": total_symbols,
                    "created_at": created_at,
                },
            )
            conn.commit()

    def delete_bulk_parent(self, job_id: str, user_id: int) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text("DELETE FROM bulk_backtest WHERE job_id = :job_id AND user_id = :user_id"),
                {"job_id": job_id, "user_id": user_id},
            )
            conn.commit()

    def list_by_job_ids(self, job_ids: list[str]) -> list[dict[str, Any]]:
        if not job_ids:
            return []
        with connection("tradermate") as conn:
            from sqlalchemy import text
            placeholders = ",".join([f":id{i}" for i in range(len(job_ids))])
            params = {f"id{i}": jid for i, jid in enumerate(job_ids)}
            rows = conn.execute(
                text(
                    f"SELECT job_id, best_return, best_symbol, completed_count, total_symbols, status as bulk_status FROM bulk_backtest WHERE job_id IN ({placeholders})"
                ),
                params,
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_owner_user_id(self, job_id: str) -> Optional[int]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT user_id FROM bulk_backtest WHERE job_id = :jid"),
                {"jid": job_id},
            ).fetchone()
            return int(row.user_id) if row and hasattr(row, "user_id") else None

    def get_metrics(self, job_id: str) -> Optional[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT best_return, best_symbol, completed_count, total_symbols, status as bulk_status, best_symbol_name FROM bulk_backtest WHERE job_id = :jid"),
                {"jid": job_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def update_best_symbol_name(self, job_id: str, best_symbol_name: Optional[str]) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text("UPDATE bulk_backtest SET best_symbol_name = :bsn WHERE job_id = :jid"),
                {"bsn": best_symbol_name, "jid": job_id},
            )
            conn.commit()

    def update_progress(self, job_id: str, completed_count: int, best_return: Any, best_symbol: Any, best_symbol_name: Any) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text(
                    """
                    UPDATE bulk_backtest
                    SET completed_count = :cc,
                        best_return = :br,
                        best_symbol = :bs,
                        best_symbol_name = :bsn
                    WHERE job_id = :jid
                    """
                ),
                {"cc": completed_count, "br": best_return, "bs": best_symbol, "bsn": best_symbol_name, "jid": job_id},
            )
            conn.commit()

    def finish(self, job_id: str, status: str, completed_at: datetime, completed_count: int, best_return: Any, best_symbol: Any, best_symbol_name: Any) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text(
                    """
                    UPDATE bulk_backtest
                    SET status = :st,
                        completed_count = :cc,
                        best_return = :br,
                        best_symbol = :bs,
                        best_symbol_name = :bsn,
                        completed_at = :ca
                    WHERE job_id = :jid
                    """
                ),
                {"st": status, "cc": completed_count, "br": best_return, "bs": best_symbol, "bsn": best_symbol_name, "ca": completed_at, "jid": job_id},
            )
            conn.commit()
