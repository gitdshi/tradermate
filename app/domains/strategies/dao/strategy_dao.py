"""Strategies DAO.

All SQL touching `tradermate.strategies` lives here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from app.infrastructure.db.connections import connection


class StrategyDao:
    def list_for_user(self, user_id: int) -> list[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            rows = conn.execute(
                text(
                    """
                    SELECT id, name, class_name, description, version, is_active, created_at, updated_at
                    FROM strategies
                    WHERE user_id = :user_id
                    ORDER BY updated_at DESC
                    """
                ),
                {"user_id": user_id},
            ).fetchall()
            return [dict(r._mapping) for r in rows]

    def name_exists_for_user(self, user_id: int, name: str) -> bool:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT 1 FROM strategies WHERE user_id = :uid AND name = :name LIMIT 1"),
                {"uid": user_id, "name": name},
            ).fetchone()
            return bool(row)

    def insert_strategy(
        self,
        user_id: int,
        name: str,
        class_name: str,
        description: Optional[str],
        parameters_json: str,
        code: str,
        created_at: datetime,
        updated_at: datetime,
    ) -> int:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            result = conn.execute(
                text(
                    """
                    INSERT INTO strategies (user_id, name, class_name, description, parameters, code, version, is_active, created_at, updated_at)
                    VALUES (:user_id, :name, :class_name, :description, :parameters, :code, 1, 1, :created_at, :updated_at)
                    """
                ),
                {
                    "user_id": user_id,
                    "name": name,
                    "class_name": class_name,
                    "description": description,
                    "parameters": parameters_json,
                    "code": code,
                    "created_at": created_at,
                    "updated_at": updated_at,
                },
            )
            conn.commit()
            return int(result.lastrowid)

    def get_for_user(self, strategy_id: int, user_id: int) -> Optional[dict[str, Any]]:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    """
                    SELECT id, user_id, name, class_name, description, parameters, code, version, is_active, created_at, updated_at
                    FROM strategies
                    WHERE id = :sid AND user_id = :uid
                    """
                ),
                {"sid": strategy_id, "uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def get_existing_for_update(self, strategy_id: int, user_id: int) -> Optional[dict[str, Any]]:
        """Fetch minimal fields used for update comparisons."""
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    """
                    SELECT id, code, class_name, name, description, version, parameters
                    FROM strategies
                    WHERE id = :sid AND user_id = :uid
                    """
                ),
                {"sid": strategy_id, "uid": user_id},
            ).fetchone()
            return dict(row._mapping) if row else None

    def update_strategy(self, strategy_id: int, user_id: int, set_clause: str, params: dict[str, Any]) -> None:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            conn.execute(
                text(f"UPDATE strategies SET {set_clause} WHERE id = :sid AND user_id = :uid"),
                {**params, "sid": strategy_id, "uid": user_id},
            )
            conn.commit()

    def delete_for_user(self, strategy_id: int, user_id: int) -> bool:
        with connection("tradermate") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text("SELECT 1 FROM strategies WHERE id = :sid AND user_id = :uid"),
                {"sid": strategy_id, "uid": user_id},
            ).fetchone()
            if not row:
                return False
            conn.execute(
                text("DELETE FROM strategies WHERE id = :sid AND user_id = :uid"),
                {"sid": strategy_id, "uid": user_id},
            )
            conn.commit()
            return True
