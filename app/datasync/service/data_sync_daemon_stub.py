"""Stub for DataSyncDaemon to allow API to start.

This is a temporary workaround until the full DataSyncDaemon class is implemented.
"""

from typing import List, Optional
from datetime import date, timedelta


class DataSyncDaemon:
    """Minimal implementation to satisfy SyncStatusService dependency."""

    @staticmethod
    def find_missing_trade_dates(lookback_days: Optional[int] = None) -> List[date]:
        """
        Return missing trade dates for backfill.

        TODO: Implement actual missing date detection based on sync logs.
        For now, return empty list (no missing dates).
        """
        if lookback_days is None:
            lookback_days = 365  # default
        # TODO: Calculate actual missing dates
        return []
