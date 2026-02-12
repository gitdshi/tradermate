"""Strategy history DAO.

All SQL touching `tradermate.strategy_history` lives here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from app.infrastructure.db.connections import connection


class StrategyHistoryDao:
    def insert_history(
        self,
        strategy_id: int,
        strategy_name: Optional[str],
        class_name: Optional[str],
        description: Optional[str],
        version: Optional[int],
        parameters: Optional[str],
        code: Optional[str],
        created_at: datetime,
    ) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text(
                    """
                    INSERT INTO strategy_history
                      (strategy_id, strategy_name, class_name, description, version, parameters, code, created_at)
                    VALUES
                      (:sid, :sname, :class, :description, :version, :parameters, :code, :created_at)
                    """
                ),
                {
                    "sid": strategy_id,
                    "sname": strategy_name,
                    "class": class_name,
                    "description": description,
                    "version": version,
                    "parameters": parameters,
                    "code": code,
                    "created_at": created_at,
                },
            )
            conn.commit()

    def rotate_keep_latest(self, strategy_id: int, keep: int = 5) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text("SELECT id FROM strategy_history WHERE strategy_id = :sid ORDER BY created_at DESC"),
                {"sid": strategy_id},
            ).fetchall()
            keep_ids = [r.id for r in rows[:keep]]
            if not keep_ids:
                return

            params: dict[str, Any] = {"sid": strategy_id}
            placeholders = []
            for i, hid in enumerate(keep_ids):
                key = f"k{i}"
                params[key] = hid
                placeholders.append(f":{key}")

            conn.execute(
                text(
                    f"DELETE FROM strategy_history WHERE strategy_id = :sid AND id NOT IN ({','.join(placeholders)})"
                ),
                params,
            )
            conn.commit()

    def list_history(self, strategy_id: int) -> list[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT id, created_at, LENGTH(code) as size,
                           strategy_name, class_name, description, version, parameters
                    FROM strategy_history
                    WHERE strategy_id = :sid
                    ORDER BY created_at DESC
                    """
                ),
                {"sid": strategy_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def get_history(self, strategy_id: int, history_id: int) -> Optional[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    """
                    SELECT id, code, strategy_name, class_name, description, version, parameters
                    FROM strategy_history
                    WHERE id = :hid AND strategy_id = :sid
                    """
                ),
                {"hid": history_id, "sid": strategy_id},
            ).fetchone()
            return dict(row._mapping) if row else None
