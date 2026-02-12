"""Backtests service.

Used by queue/backtest routes for bulk result pagination and summary aggregation.
"""

from __future__ import annotations

from typing import Any, Dict, Optional
import json

from app.domains.backtests.dao.bulk_backtest_dao import BulkBacktestDao
from app.domains.backtests.dao.bulk_results_dao import BulkResultsDao
from app.domains.market.service import MarketService


class BulkBacktestQueryService:
    def __init__(self) -> None:
        self._bulk = BulkBacktestDao()
        self._results = BulkResultsDao()
        self._market = MarketService()

    def get_results_page(
        self,
        *,
        bulk_job_id: str,
        user_id: int,
        page: int,
        page_size: int,
        sort_order: str,
    ) -> dict[str, Any]:
        total = self._results.count_children(bulk_job_id=bulk_job_id, user_id=user_id)
        rows = self._results.list_children_page(
            bulk_job_id=bulk_job_id,
            user_id=user_id,
            page=page,
            page_size=page_size,
            sort_order=sort_order,
        )

        out = []
        for r in rows:
            parsed_result = None
            if r.get("result"):
                try:
                    parsed_result = json.loads(r["result"]) if isinstance(r["result"], str) else r["result"]
                except Exception:
                    parsed_result = None

            # parameters column in DB might be JSON or TEXT; normalize
            parsed_parameters: dict[str, Any] = {}
            try:
                if parsed_result and isinstance(parsed_result, dict) and parsed_result.get("parameters") is not None:
                    parsed_parameters = parsed_result.get("parameters")
                else:
                    rawp = r.get("parameters")
                    if rawp:
                        parsed_parameters = json.loads(rawp) if isinstance(rawp, str) else rawp
            except Exception:
                parsed_parameters = {}

            symbol_name = ""
            if parsed_result and isinstance(parsed_result, dict):
                symbol_name = parsed_result.get("symbol_name") or ""
            if not symbol_name:
                symbol_name = self._market.resolve_symbol_name(r.get("vt_symbol") or "")

            out.append(
                {
                    "job_id": r.get("job_id"),
                    "symbol": r.get("vt_symbol"),
                    "status": r.get("status"),
                    "error": r.get("error"),
                    "created_at": r.get("created_at").isoformat() if r.get("created_at") else None,
                    "completed_at": r.get("completed_at").isoformat() if r.get("completed_at") else None,
                    "statistics": parsed_result.get("statistics") if isinstance(parsed_result, dict) else None,
                    "symbol_name": symbol_name,
                    "parameters": parsed_parameters,
                }
            )

        return {
            "results": out,
            "total": total,
            "page": page,
            "page_size": page_size,
            "sort_order": sort_order,
        }

    def get_summary(self, *, bulk_job_id: str, user_id: int) -> dict[str, Any]:
        owner_uid = self._bulk.get_owner_user_id(bulk_job_id)
        if owner_uid != user_id:
            raise KeyError("Job not found")

        rows = self._results.list_all_children(bulk_job_id=bulk_job_id, user_id=user_id)
        total = len(rows)
        completed = []
        failed_list = []

        for r in rows:
            status = r.get("status")
            res = r.get("result")
            if status in ("completed", "finished") and res:
                try:
                    parsed = json.loads(res) if isinstance(res, str) else res
                    stats = parsed.get("statistics", {}) if isinstance(parsed, dict) else {}
                    symbol_name = parsed.get("symbol_name") or ""
                    if not symbol_name:
                        symbol_name = self._market.resolve_symbol_name(r.get("vt_symbol") or "")
                    completed.append(
                        {
                            "symbol": r.get("vt_symbol"),
                            "symbol_name": symbol_name,
                            "total_return": stats.get("total_return"),
                            "annual_return": stats.get("annual_return"),
                            "sharpe_ratio": stats.get("sharpe_ratio"),
                            "max_drawdown": stats.get("max_drawdown_percent") or stats.get("max_drawdown"),
                            "total_trades": stats.get("total_trades"),
                            "winning_rate": stats.get("winning_rate"),
                            "profit_factor": stats.get("profit_factor"),
                        }
                    )
                except Exception:
                    failed_list.append({"symbol": r.get("vt_symbol"), "symbol_name": "", "error": "Parse error"})
            elif status == "failed":
                symbol_name = ""
                if res:
                    try:
                        parsed = json.loads(res) if isinstance(res, str) else res
                        if isinstance(parsed, dict):
                            symbol_name = parsed.get("symbol_name") or ""
                    except Exception:
                        symbol_name = ""
                if not symbol_name:
                    symbol_name = self._market.resolve_symbol_name(r.get("vt_symbol") or "")
                failed_list.append(
                    {
                        "symbol": r.get("vt_symbol"),
                        "symbol_name": symbol_name,
                        "error": r.get("error") or "Unknown error",
                    }
                )

        n = len(completed)
        winning = [c for c in completed if c.get("total_return") is not None and c["total_return"] > 0]
        losing = [c for c in completed if c.get("total_return") is not None and c["total_return"] <= 0]

        def safe_avg(vals):
            filtered = [v for v in vals if v is not None]
            return sum(filtered) / len(filtered) if filtered else None

        avg_metrics = {
            "total_return": safe_avg([c["total_return"] for c in completed]),
            "annual_return": safe_avg([c["annual_return"] for c in completed]),
            "sharpe_ratio": safe_avg([c["sharpe_ratio"] for c in completed]),
            "max_drawdown": safe_avg([c["max_drawdown"] for c in completed]),
            "winning_rate": safe_avg([c["winning_rate"] for c in completed]),
            "profit_factor": safe_avg([c["profit_factor"] for c in completed]),
            "total_trades": safe_avg([c["total_trades"] for c in completed]),
        }

        sorted_by_return = sorted(
            [c for c in completed if c.get("total_return") is not None],
            key=lambda x: x["total_return"],
            reverse=True,
        )
        top10 = sorted_by_return[:10]
        bottom10 = sorted_by_return[-10:][::-1] if len(sorted_by_return) > 10 else sorted_by_return[::-1]

        buckets = {"<-20%": 0, "-20%~-10%": 0, "-10%~0%": 0, "0%~10%": 0, "10%~20%": 0, ">20%": 0}
        for c in completed:
            ret = c.get("total_return")
            if ret is None:
                continue
            if ret < -20:
                buckets["<-20%"] += 1
            elif ret < -10:
                buckets["-20%~-10%"] += 1
            elif ret < 0:
                buckets["-10%~0%"] += 1
            elif ret < 10:
                buckets["0%~10%"] += 1
            elif ret < 20:
                buckets["10%~20%"] += 1
            else:
                buckets[">20%"] += 1

        return {
            "job_id": bulk_job_id,
            "total_symbols": total,
            "completed_count": n,
            "failed_count": len(failed_list),
            "winning_count": len(winning),
            "losing_count": len(losing),
            "win_rate": len(winning) / n * 100 if n > 0 else 0,
            "avg_metrics": avg_metrics,
            "top10": top10,
            "bottom10": bottom10,
            "return_distribution": buckets,
            "failed_symbols": failed_list,
        }
