"""DAO for sync_log operations used by datasync daemon."""
from __future__ import annotations
from datetime import date, datetime, timezone
from typing import List, Optional, Tuple, Any
import logging

from sqlalchemy import text
from app.infrastructure.db.connections import get_tushare_engine, connection

logger = logging.getLogger(__name__)

engine = get_tushare_engine()


def write_sync_log(sync_date: date, endpoint: str, status: str,
                   rows_synced: int = 0, error_message: Optional[str] = None):
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO sync_log (sync_date, endpoint, status, rows_synced, error_message, started_at, finished_at)
            VALUES (:sync_date, :endpoint, :status, :rows_synced, :error_message, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                rows_synced = VALUES(rows_synced),
                error_message = VALUES(error_message),
                finished_at = NOW()
            """
        ), {
            'sync_date': sync_date,
            'endpoint': endpoint,
            'status': status,
            'rows_synced': rows_synced,
            'error_message': error_message
        })


def get_sync_status(sync_date: date, endpoint: str) -> Optional[str]:
    with engine.connect() as conn:
        res = conn.execute(text("SELECT status FROM sync_log WHERE sync_date=:d AND endpoint=:ep"), {'d': sync_date, 'ep': endpoint})
        row = res.fetchone()
        return row[0] if row else None


def find_failed_syncs(start: date, end: date) -> List[Tuple[date, str]]:
    with engine.connect() as conn:
        res = conn.execute(text(
            "SELECT sync_date, endpoint FROM sync_log WHERE sync_date >= :start AND sync_date <= :end AND status IN ('error','partial') ORDER BY sync_date ASC, endpoint"
        ), {'start': start, 'end': end})
        return [(row[0], row[1]) for row in res.fetchall()]


def write_tushare_stock_sync_log(sync_date: date, endpoint: str, status: str, rows: int = 0, err: Optional[str] = None):
    """Write to the legacy `tushare_stock_sync_log` table in the Tushare DB."""
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO tushare_stock_sync_log (sync_date, endpoint, status, rows_synced, error_message, started_at, finished_at)
            VALUES (:sd, :ep, :st, :rows, :err, NOW(), NOW())
            ON DUPLICATE KEY UPDATE status=VALUES(status), rows_synced=VALUES(rows_synced), error_message=VALUES(error_message), finished_at=NOW()
            """
        ), {'sd': sync_date.strftime('%Y-%m-%d'), 'ep': endpoint, 'st': status, 'rows': rows, 'err': err})


def get_last_success_tushare_sync_date(endpoint: str):
    with engine.connect() as conn:
        res = conn.execute(text("SELECT MAX(sync_date) FROM tushare_stock_sync_log WHERE endpoint=:ep AND status='success'"), {'ep': endpoint})
        row = res.fetchone()
        return row[0] if row and row[0] else None


class SyncLogDao:
    def get_latest_per_endpoint(self, endpoints: list[str]) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        with connection("tushare") as conn:
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
                    def _to_utc_iso(dt: datetime | None) -> str | None:
                        if not dt:
                            return None
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        return dt.astimezone(timezone.utc).isoformat()

                    latest[ep] = {
                        "sync_date": row[0].isoformat() if row[0] else None,
                        "status": row[1],
                        "rows_synced": row[2] or 0,
                        "error_message": row[3],
                        "started_at": _to_utc_iso(row[4]),
                        "finished_at": _to_utc_iso(row[5]),
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
            row = conn.execute(text("SELECT MAX(finished_at) as max_finished FROM sync_log")).fetchone()
            if not row:
                return None
            val = row.max_finished if hasattr(row, "max_finished") else row[0]
            if val is None:
                return None
            if val.tzinfo is None:
                val = val.replace(tzinfo=timezone.utc)
            else:
                val = val.astimezone(timezone.utc)
            return val

    def running_count_last_day(self) -> int:
        with connection("tushare") as conn:
            row = conn.execute(
                text(
                    "SELECT COUNT(*) as cnt FROM sync_log WHERE status='running' AND started_at >= NOW() - INTERVAL 1 DAY"
                )
            ).fetchone()
            return int(row.cnt) if row and hasattr(row, "cnt") else 0
