"""Queue monitoring and management routes."""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict, Any
from sqlalchemy import text

from app.api.middleware.auth import get_current_user
from app.api.models.user import TokenData
from app.api.services.backtest_service import get_backtest_service
from app.api.services.job_storage import get_job_storage

router = APIRouter(prefix="/queue", tags=["queue"])


@router.get("/stats")
async def get_queue_stats(
    current_user: TokenData = Depends(get_current_user)
):
    """Get queue statistics."""
    job_storage = get_job_storage()
    stats = job_storage.get_queue_stats()
    
    return {
        "queues": stats,
        "timestamp": "now"
    }


@router.get("/jobs")
async def list_jobs(
    status: Optional[str] = None,
    limit: int = 50,
    current_user: TokenData = Depends(get_current_user)
) -> List[Dict[str, Any]]:
    """List user's jobs, enriched with bulk metrics from DB."""
    service = get_backtest_service()
    jobs = service.list_user_jobs(
        user_id=current_user.user_id,
        status=status,
        limit=limit
    )

    # Enrich bulk jobs with best symbol's full metrics from backtest_history
    bulk_ids = [j["job_id"] for j in jobs if j.get("job_id", "").startswith("bulk_")]
    if bulk_ids:
        from app.api.services.db import get_db_connection
        import json as _json
        conn = get_db_connection()
        try:
            placeholders = ",".join([f":id{i}" for i in range(len(bulk_ids))])
            params = {f"id{i}": bid for i, bid in enumerate(bulk_ids)}
            # Get bulk_backtest row + best child's full statistics
            rows = conn.execute(
                text(f"SELECT job_id, best_return, best_symbol, completed_count, total_symbols, status as bulk_status FROM bulk_backtest WHERE job_id IN ({placeholders})"),
                params
            ).fetchall()
            bulk_map = {r.job_id: r for r in rows}

            # Fetch best child result for each bulk job
            best_child_map: Dict[str, Any] = {}
            for r in rows:
                if r.best_symbol:
                    child_id = f"{r.job_id}__{r.best_symbol}"
                    child_row = conn.execute(
                        text("SELECT result FROM backtest_history WHERE job_id = :cid LIMIT 1"),
                        {"cid": child_id}
                    ).fetchone()
                    if child_row and child_row.result:
                        try:
                            parsed = _json.loads(child_row.result) if isinstance(child_row.result, str) else child_row.result
                            # store full parsed result so we can access symbol_name and statistics
                            best_child_map[r.job_id] = parsed
                        except Exception:
                            pass

            for j in jobs:
                row = bulk_map.get(j.get("job_id"))
                if row:
                    if not j.get("result"):
                        j["result"] = {}
                    if row.best_return is not None:
                        j["result"]["best_return"] = float(row.best_return)
                    j["result"]["best_symbol"] = row.best_symbol
                    j["result"]["completed_count"] = row.completed_count
                    j["result"]["total_symbols"] = row.total_symbols
                    # Attach best child's full statistics
                    best_stats = best_child_map.get(j.get("job_id"))
                    if best_stats:
                        stats = best_stats.get("statistics", {}) if isinstance(best_stats, dict) else {}
                        j["result"]["best_annual_return"] = stats.get("annual_return")
                        j["result"]["best_sharpe_ratio"] = stats.get("sharpe_ratio")
                        j["result"]["best_max_drawdown"] = stats.get("max_drawdown_percent") or stats.get("max_drawdown")
                        # human-readable symbol name from saved child result
                        j["result"]["best_symbol_name"] = best_stats.get("symbol_name") if isinstance(best_stats, dict) else None
        except Exception as e:
            print(f"[Queue] Error enriching bulk jobs: {e}")
        finally:
            conn.close()

    return jobs


@router.get("/jobs/{job_id}")
async def get_job_detail(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get job details and result."""
    service = get_backtest_service()
    job = service.get_job_status(job_id, current_user.user_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job


@router.post("/jobs/{job_id}/cancel")
async def cancel_job(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """Cancel a running job."""
    service = get_backtest_service()
    success = service.cancel_job(job_id, current_user.user_id)
    
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Job cannot be cancelled (not found or already finished)"
        )
    
    return {"message": "Job cancelled", "job_id": job_id}


@router.delete("/jobs/{job_id}")
async def delete_job(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """Delete a job and its results. For bulk jobs, cascade-deletes all children."""
    # First check if user owns the job
    service = get_backtest_service()
    job = service.get_job_status(job_id, current_user.user_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Delete from Redis storage
    job_storage = get_job_storage()
    deleted = job_storage.delete_job(job_id)
    
    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete job")
    
    from app.api.services.db import get_db_connection
    from sqlalchemy import text
    conn = get_db_connection()
    try:
        is_bulk = job_id.startswith("bulk_")
        if is_bulk:
            # Cascade-delete all child backtest_history rows
            conn.execute(
                text("DELETE FROM backtest_history WHERE bulk_job_id = :bulk_job_id AND user_id = :user_id"),
                {"bulk_job_id": job_id, "user_id": current_user.user_id}
            )
            # Delete the bulk_backtest parent row
            conn.execute(
                text("DELETE FROM bulk_backtest WHERE job_id = :job_id AND user_id = :user_id"),
                {"job_id": job_id, "user_id": current_user.user_id}
            )
            # Also clean up child Redis keys
            for key in job_storage.redis.scan_iter(match=f"tradermate:job:{job_id}__*", count=200):
                job_storage.redis.delete(key)
            for key in job_storage.redis.scan_iter(match=f"tradermate:result:{job_id}__*", count=200):
                job_storage.redis.delete(key)
        else:
            # Single job – delete from backtest_history
            conn.execute(
                text("DELETE FROM backtest_history WHERE job_id = :job_id AND user_id = :user_id"),
                {"job_id": job_id, "user_id": current_user.user_id}
            )
        conn.commit()
    except Exception as e:
        print(f"Error deleting from database: {e}")
    finally:
        conn.close()
    
    return {"message": "Job deleted", "job_id": job_id}


@router.post("/backtest")
async def submit_backtest_to_queue(
    request: Dict[str, Any],
    current_user: TokenData = Depends(get_current_user)
):
    """Submit a backtest job to RQ queue for async processing."""
    from datetime import datetime as dt
    
    service = get_backtest_service()
    
    # Parse dates
    start_date = dt.strptime(request["start_date"], "%Y-%m-%d").date()
    end_date = dt.strptime(request["end_date"], "%Y-%m-%d").date()
    
    # Get symbol - handle both 'symbol' and 'vt_symbol' keys
    symbol = request.get("symbol") or request.get("vt_symbol")
    if not symbol:
        raise HTTPException(status_code=400, detail="Symbol is required")
    
    job_id = service.submit_backtest(
        user_id=current_user.user_id,
        strategy_id=request.get("strategy_id"),
        strategy_class_name=request.get("strategy_class"),
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        initial_capital=request.get("initial_capital", 100000.0),
        rate=request.get("rate", 0.0001),
        slippage=request.get("slippage", 0.0),
        size=request.get("size", 1),
        pricetick=request.get("pricetick", 0.01),
        parameters=request.get("parameters"),
        symbol_name=request.get("symbol_name", ""),
        strategy_name=request.get("strategy_name", ""),
        benchmark=request.get("benchmark", "399300.SZ"),
    )
    
    return {"job_id": job_id, "status": "queued", "message": "Backtest job submitted to queue"}





@router.post("/bulk-backtest")
async def submit_bulk_backtest(
    request: Dict[str, Any],
    current_user: TokenData = Depends(get_current_user)
):
    """Submit a bulk backtest job – one strategy against multiple symbols."""
    from datetime import datetime as dt

    service = get_backtest_service()

    symbols = request.get("symbols")
    if not symbols or not isinstance(symbols, list) or len(symbols) == 0:
        raise HTTPException(status_code=400, detail="symbols must be a non-empty list")

    start_date = dt.strptime(request["start_date"], "%Y-%m-%d").date()
    end_date = dt.strptime(request["end_date"], "%Y-%m-%d").date()

    job_id = service.submit_batch_backtest(
        user_id=current_user.user_id,
        strategy_id=request.get("strategy_id"),
        strategy_class_name=request.get("strategy_class"),
        symbols=symbols,
        start_date=start_date,
        end_date=end_date,
        initial_capital=request.get("initial_capital", 100000.0),
        rate=request.get("rate", 0.0001),
        slippage=request.get("slippage", 0.0),
        size=request.get("size", 1),
        pricetick=request.get("pricetick", 0.01),
        parameters=request.get("parameters"),
        strategy_name=request.get("strategy_name", ""),
        benchmark=request.get("benchmark", "399300.SZ"),
    )

    return {"job_id": job_id, "status": "queued", "message": "Bulk backtest submitted"}


@router.get("/bulk-jobs/{job_id}/results")
async def get_bulk_job_results(
    job_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    sort_order: str = Query("desc", regex="^(asc|desc)$"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Get paginated child results for a bulk backtest job.
    Ordered by total_return extracted from the result JSON.
    """
    from app.api.services.db import get_db_connection
    from sqlalchemy import text
    import json as _json

    conn = get_db_connection()
    try:
        # Count total children
        count_row = conn.execute(
            text("SELECT COUNT(*) as cnt FROM backtest_history WHERE bulk_job_id = :bjid AND user_id = :uid"),
            {"bjid": job_id, "uid": current_user.user_id}
        ).fetchone()
        total = count_row.cnt if count_row else 0

        # Order by total_return extracted from JSON result column
        order_dir = "ASC" if sort_order == "asc" else "DESC"
        offset = (page - 1) * page_size

        rows = conn.execute(
            text(f"""
                SELECT job_id, vt_symbol, status, result, error, created_at, completed_at
                FROM backtest_history
                WHERE bulk_job_id = :bjid AND user_id = :uid
                ORDER BY
                    CASE WHEN result IS NOT NULL
                         THEN CAST(JSON_EXTRACT(result, '$.statistics.total_return') AS DOUBLE)
                         ELSE NULL END {order_dir}
                LIMIT :lim OFFSET :off
            """),
            {"bjid": job_id, "uid": current_user.user_id, "lim": page_size, "off": offset}
        ).fetchall()

        results = []
        for r in rows:
            parsed_result = None
            if r.result:
                try:
                    parsed_result = _json.loads(r.result) if isinstance(r.result, str) else r.result
                except Exception:
                    parsed_result = None
            # Parse parameters from stored DB row if present
            parsed_parameters = {}
            try:
                if r.result:
                    # prefer parameters inside result JSON
                    parsed_parameters = parsed_result.get("parameters") if isinstance(parsed_result, dict) and parsed_result.get("parameters") is not None else json.loads(r.parameters) if r.parameters else {}
                else:
                    parsed_parameters = json.loads(r.parameters) if r.parameters else {}
            except Exception:
                parsed_parameters = {}

            results.append({
                "job_id": r.job_id,
                "symbol": r.vt_symbol,
                "status": r.status,
                "error": r.error,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "statistics": parsed_result.get("statistics") if parsed_result else None,
                "symbol_name": parsed_result.get("symbol_name", "") if parsed_result else "",
                "parameters": parsed_parameters,
            })

        return {
            "results": results,
            "total": total,
            "page": page,
            "page_size": page_size,
            "sort_order": sort_order,
        }

    finally:
        conn.close()


@router.get("/bulk-jobs/{job_id}/summary")
async def get_bulk_job_summary(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """
    Get aggregated summary statistics for a completed bulk backtest job.
    Computes winning/losing counts, averages, and top 10 from child results.
    """
    from app.api.services.db import get_db_connection
    import json as _json

    conn = get_db_connection()
    try:
        # Verify ownership
        owner = conn.execute(
            text("SELECT user_id FROM bulk_backtest WHERE job_id = :jid"),
            {"jid": job_id}
        ).fetchone()
        if not owner or owner.user_id != current_user.user_id:
            raise HTTPException(status_code=404, detail="Job not found")

        # Load all completed children with their statistics
        rows = conn.execute(
            text("""
                SELECT job_id, vt_symbol, status, result, error
                FROM backtest_history
                WHERE bulk_job_id = :bjid AND user_id = :uid
            """),
            {"bjid": job_id, "uid": current_user.user_id}
        ).fetchall()

        total = len(rows)
        completed = []
        failed_list = []

        for r in rows:
            if r.status in ("completed", "finished") and r.result:
                try:
                    parsed = _json.loads(r.result) if isinstance(r.result, str) else r.result
                    stats = parsed.get("statistics", {})
                    completed.append({
                        "symbol": r.vt_symbol,
                        "symbol_name": parsed.get("symbol_name", "") if parsed else "",
                        "total_return": stats.get("total_return"),
                        "annual_return": stats.get("annual_return"),
                        "sharpe_ratio": stats.get("sharpe_ratio"),
                        "max_drawdown": stats.get("max_drawdown_percent") or stats.get("max_drawdown"),
                        "total_trades": stats.get("total_trades"),
                        "winning_rate": stats.get("winning_rate"),
                        "profit_factor": stats.get("profit_factor"),
                    })
                except Exception:
                    failed_list.append({"symbol": r.vt_symbol, "error": "Parse error"})
            elif r.status == "failed":
                # Try to extract symbol_name from result JSON if available
                symbol_name = None
                if r.result:
                    try:
                        parsed = _json.loads(r.result) if isinstance(r.result, str) else r.result
                        symbol_name = parsed.get("symbol_name") if isinstance(parsed, dict) else None
                    except Exception:
                        symbol_name = None

                failed_list.append({
                    "symbol": r.vt_symbol,
                    "symbol_name": symbol_name or "",
                    "error": r.error or "Unknown error",
                })

        # Compute aggregates from completed results
        n = len(completed)
        winning = [c for c in completed if c["total_return"] is not None and c["total_return"] > 0]
        losing = [c for c in completed if c["total_return"] is not None and c["total_return"] <= 0]

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

        # Top 10 and bottom 10 by total_return
        sorted_by_return = sorted(
            [c for c in completed if c["total_return"] is not None],
            key=lambda x: x["total_return"], reverse=True
        )
        top10 = sorted_by_return[:10]
        bottom10 = sorted_by_return[-10:][::-1] if len(sorted_by_return) > 10 else sorted_by_return[::-1]

        # Distribution buckets for returns
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
            "job_id": job_id,
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

    finally:
        conn.close()
