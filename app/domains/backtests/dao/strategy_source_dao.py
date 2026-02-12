"""Strategy source DAO for backtests.

Backtest submission needs strategy code/class/version.
All SQL touching `tradermate.strategies` for this purpose lives here.
"""

from __future__ import annotations

from typing import Optional

from app.infrastructure.db.connections import connection


class StrategySourceDao:
    def get_strategy_source_for_user(self, strategy_id: int, user_id: int) -> tuple[str, str, Optional[int]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT code, class_name, version FROM strategies WHERE id = :id AND user_id = :user_id"),
                {"id": strategy_id, "user_id": user_id},
            ).fetchone()
            if not row:
                raise KeyError("Strategy not found")
            return row.code, row.class_name, getattr(row, "version", None)

    def get_strategy_code_by_class_name(self, class_name: str) -> str:
        """Load strategy code by class_name (no user filter).

        Used by workers when only class name is available.
        """
        if not class_name:
            raise KeyError("Strategy not found")
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT code FROM strategies WHERE class_name = :classname LIMIT 1"),
                {"classname": class_name},
            ).fetchone()
            if not row or not row.code:
                raise KeyError("Strategy not found")
            return row.code
