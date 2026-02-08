"""Queue monitoring and management routes."""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict, Any

from app.api.middleware.auth import get_current_user
from app.api.models.user import TokenData
from app.api.services.backtest_service_v2 import get_backtest_service_v2
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
    """List user's jobs."""
    service = get_backtest_service_v2()
    jobs = service.list_user_jobs(
        user_id=current_user.user_id,
        status=status,
        limit=limit
    )
    
    return jobs


@router.get("/jobs/{job_id}")
async def get_job_detail(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get job details and result."""
    service = get_backtest_service_v2()
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
    service = get_backtest_service_v2()
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
    service = get_backtest_service_v2()
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
    
    service = get_backtest_service_v2()
    
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

    service = get_backtest_service_v2()

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
            results.append({
                "job_id": r.job_id,
                "symbol": r.vt_symbol,
                "status": r.status,
                "error": r.error,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "statistics": parsed_result.get("statistics") if parsed_result else None,
                "symbol_name": parsed_result.get("symbol_name", "") if parsed_result else "",
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
