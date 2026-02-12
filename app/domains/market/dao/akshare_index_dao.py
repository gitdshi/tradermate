"""AkShare index DAO.

All SQL touching `akshare.index_daily` should live here.
"""

from __future__ import annotations

from sqlalchemy import text

from app.infrastructure.db.connections import connection


class AkshareIndexDao:
    def list_index_codes(self) -> list[str]:
        with connection("akshare") as conn:
            rows = conn.execute(
                text("SELECT DISTINCT index_code FROM index_daily ORDER BY index_code")
            ).fetchall()
            return [r.index_code if hasattr(r, "index_code") else list(r)[0] for r in rows]
