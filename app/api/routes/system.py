"""System status routes."""
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

from fastapi import APIRouter, Depends
from sqlalchemy import text

from app.api.middleware.auth import get_current_user
from app.api.models.user import TokenData
from app.api.services.db import get_tushare_engine
from app.services.data_sync_daemon import DataSyncDaemon, BACKFILL_DAYS, REQUIRED_ENDPOINTS, SYNC_HOUR, SYNC_MINUTE

router = APIRouter(prefix="/system", tags=["system"])


def _to_iso(value: Optional[datetime]) -> Optional[str]:
    if not value:
        return None
    return value.isoformat()


def _status_from_last_run(last_run_at: Optional[datetime], running_count: int) -> str:
    if running_count > 0:
        return "running"
    if not last_run_at:
        return "unknown"
    if datetime.utcnow() - last_run_at <= timedelta(hours=26):
        return "idle"
    return "stale"


@router.get("/sync-status")
async def get_sync_status(
    current_user: TokenData = Depends(get_current_user)
) -> Dict[str, Any]:
    engine = get_tushare_engine()

    latest: Dict[str, Dict[str, Any]] = {}
    last_finished: Optional[datetime] = None
    running_count = 0

    with engine.connect() as conn:
        # Latest per endpoint
        for ep in REQUIRED_ENDPOINTS:
            row = conn.execute(text("""
                SELECT sync_date, status, rows_synced, error_message, started_at, finished_at
                FROM sync_log
                WHERE endpoint = :ep
                ORDER BY sync_date DESC, finished_at DESC
                LIMIT 1
            """), {"ep": ep}).fetchone()
            if row:
                latest[ep] = {
                    "sync_date": row[0].isoformat() if row[0] else None,
                    "status": row[1],
                    "rows_synced": row[2] or 0,
                    "error_message": row[3],
                    "started_at": _to_iso(row[4]),
                    "finished_at": _to_iso(row[5])
                }
            else:
                latest[ep] = {
                    "sync_date": None,
                    "status": "unknown",
                    "rows_synced": 0,
                    "error_message": None,
                    "started_at": None,
                    "finished_at": None
                }

        # Last finished sync
        row = conn.execute(text("SELECT MAX(finished_at) FROM sync_log"))
        last_finished = row.scalar()

        # Running jobs (within 24h)
        running_count = conn.execute(text(
            "SELECT COUNT(*) FROM sync_log WHERE status='running' AND started_at >= NOW() - INTERVAL 1 DAY"
        )).scalar() or 0

    # Missing trade dates based on required endpoints
    daemon = DataSyncDaemon()
    missing_dates = daemon.find_missing_trade_dates(lookback_days=BACKFILL_DAYS)
    missing_date_strings = [d.isoformat() for d in missing_dates]

    daemon_status = _status_from_last_run(last_finished, running_count)

    return {
        "daemon": {
            "status": daemon_status,
            "running_jobs": running_count,
            "last_run_at": _to_iso(last_finished),
            "next_run_local": f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}"
        },
        "sync": {
            "latest": latest,
            "missing_dates": missing_date_strings,
            "lookback_days": BACKFILL_DAYS
        },
        "consistency": {
            "missing_count": len(missing_date_strings),
            "is_consistent": len(missing_date_strings) == 0
        }
    }
