"""Jobs/Queue domain service.

Combines Redis job metadata with DB projections for bulk jobs.
Routes should call this service (no SQL in routes).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.api.services.backtest_service import get_backtest_service
from app.api.services.job_storage import get_job_storage
from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
from app.domains.backtests.dao.bulk_backtest_dao import BulkBacktestDao


class JobsService:
    def __init__(self) -> None:
        self._backtest_service = get_backtest_service()
        self._job_storage = get_job_storage()
        self._bulk_dao = BulkBacktestDao()
        self._history_dao = BacktestHistoryDao()

    def list_jobs(self, *, user_id: int, status: Optional[str], limit: int) -> List[Dict[str, Any]]:
        jobs = self._backtest_service.list_user_jobs(user_id=user_id, status=status, limit=limit)

        bulk_ids = [j["job_id"] for j in jobs if j.get("job_id", "").startswith("bulk_")]
        if not bulk_ids:
            return jobs

        bulk_rows = self._bulk_dao.list_by_job_ids(bulk_ids)
        bulk_map = {r["job_id"]: r for r in bulk_rows}

        # For each bulk job, fetch best child result JSON (from backtest_history)
        best_child_map: Dict[str, Any] = {}
        for r in bulk_rows:
            best_symbol = r.get("best_symbol")
            if not best_symbol:
                continue
            child_id = f"{r['job_id']}__{best_symbol}"
            best_child_map[r["job_id"]] = self._history_dao.get_child_result_json(child_id)

        for j in jobs:
            row = bulk_map.get(j.get("job_id"))
            if not row:
                continue
            j.setdefault("result", {})
            if row.get("best_return") is not None:
                j["result"]["best_return"] = float(row["best_return"])
            j["result"]["best_symbol"] = row.get("best_symbol")
            j["result"]["completed_count"] = row.get("completed_count")
            j["result"]["total_symbols"] = row.get("total_symbols")

            best_stats = best_child_map.get(j.get("job_id"))
            if isinstance(best_stats, dict):
                stats = best_stats.get("statistics", {}) if isinstance(best_stats.get("statistics"), dict) else {}
                j["result"]["best_annual_return"] = stats.get("annual_return")
                j["result"]["best_sharpe_ratio"] = stats.get("sharpe_ratio")
                j["result"]["best_max_drawdown"] = stats.get("max_drawdown_percent") or stats.get("max_drawdown")
                j["result"]["best_symbol_name"] = best_stats.get("symbol_name")

        return jobs

    def delete_job_and_results(self, *, job_id: str, user_id: int) -> None:
        # Delete from Redis storage first
        deleted = self._job_storage.delete_job(job_id)
        if not deleted:
            raise RuntimeError("Failed to delete job")

        is_bulk = job_id.startswith("bulk_")
        if is_bulk:
            self._history_dao.delete_bulk_children(job_id, user_id)
            self._bulk_dao.delete_bulk_parent(job_id, user_id)

            # Clean up child Redis keys
            for key in self._job_storage.redis.scan_iter(match=f"tradermate:job:{job_id}__*", count=200):
                self._job_storage.redis.delete(key)
            for key in self._job_storage.redis.scan_iter(match=f"tradermate:result:{job_id}__*", count=200):
                self._job_storage.redis.delete(key)
        else:
            self._history_dao.delete_single(job_id, user_id)
