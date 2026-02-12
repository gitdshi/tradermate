"""Sync domain service."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from app.domains.sync.dao.sync_log_dao import SyncLogDao
from app.services.data_sync_daemon import DataSyncDaemon, BACKFILL_DAYS, REQUIRED_ENDPOINTS, SYNC_HOUR, SYNC_MINUTE


def _status_from_last_run(last_run_at: Optional[datetime], running_count: int) -> str:
    if running_count > 0:
        return "running"
    if not last_run_at:
        return "unknown"
    if datetime.utcnow() - last_run_at <= timedelta(hours=26):
        return "idle"
    return "stale"


class SyncStatusService:
    def __init__(self) -> None:
        self._dao = SyncLogDao()

    def get_sync_status(self) -> Dict[str, Any]:
        latest = self._dao.get_latest_per_endpoint(list(REQUIRED_ENDPOINTS))
        last_finished = self._dao.last_finished_at()
        running_count = self._dao.running_count_last_day()

        daemon = DataSyncDaemon()
        missing_dates = daemon.find_missing_trade_dates(lookback_days=BACKFILL_DAYS)
        missing_date_strings = [d.isoformat() for d in missing_dates]

        daemon_status = _status_from_last_run(last_finished, running_count)

        return {
            "daemon": {
                "status": daemon_status,
                "running_jobs": running_count,
                "last_run_at": last_finished.isoformat() if last_finished else None,
                "next_run_local": f"{SYNC_HOUR:02d}:{SYNC_MINUTE:02d}",
            },
            "sync": {
                "latest": latest,
                "missing_dates": missing_date_strings,
                "lookback_days": BACKFILL_DAYS,
            },
            "consistency": {
                "missing_count": len(missing_date_strings),
                "is_consistent": len(missing_date_strings) == 0,
            },
        }
