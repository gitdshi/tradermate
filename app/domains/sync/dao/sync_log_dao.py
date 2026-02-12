"""Sync log DAO.

All SQL touching `tushare.sync_log` lives here.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from app.infrastructure.db.connections import connection


class SyncLogDao:
    def get_latest_per_endpoint(self, endpoints: list[str]) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        with connection("tushare") as conn:
            from sqlalchemy import text
            for ep in endpoints:
                row = conn.execute(
                    text(
                        """
                        SELECT sync_date, status, rows_synced, error_message, started_at, finished_at
                        FROM sync_log
                        WHERE endpoint = :ep
                        ORDER BY sync_date DESC, finished_at DESC
                        LIMIT 1
                        """
                    ),
                    {"ep": ep},
                ).fetchone()
                if row:
                    latest[ep] = {
                        "sync_date": row[0].isoformat() if row[0] else None,
                        "status": row[1],
                        "rows_synced": row[2] or 0,
                        "error_message": row[3],
                        "started_at": row[4].isoformat() if row[4] else None,
                        "finished_at": row[5].isoformat() if row[5] else None,
                    }
                else:
                    latest[ep] = {
                        "sync_date": None,
                        "status": "unknown",
                        "rows_synced": 0,
                        "error_message": None,
                        "started_at": None,
                        "finished_at": None,
                    }
        return latest

    def last_finished_at(self) -> Optional[datetime]:
        with connection("tushare") as conn:
            from sqlalchemy import text
            row = conn.execute(text("SELECT MAX(finished_at) as max_finished FROM sync_log")).fetchone()
            if not row:
                return None
            return row.max_finished if hasattr(row, "max_finished") else row[0]

    def running_count_last_day(self) -> int:
        with connection("tushare") as conn:
            from sqlalchemy import text
            row = conn.execute(
                text(
                    "SELECT COUNT(*) as cnt FROM sync_log WHERE status='running' AND started_at >= NOW() - INTERVAL 1 DAY"
                )
            ).fetchone()
            return int(row.cnt) if row and hasattr(row, "cnt") else 0
