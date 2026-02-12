"""Queue monitoring and management routes."""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict, Any

from app.api.middleware.auth import get_current_user
from app.api.models.user import TokenData
from app.api.services.backtest_service import get_backtest_service
from app.api.services.job_storage import get_job_storage

from app.domains.jobs.service import JobsService
from app.domains.backtests.service import BulkBacktestQueryService

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
    return JobsService().list_jobs(user_id=current_user.user_id, status=status, limit=limit)


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
    
    try:
        JobsService().delete_job_and_results(job_id=job_id, user_id=current_user.user_id)
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    
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
    sort_order: str = Query("desc", pattern="^(asc|desc)$"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    Get paginated child results for a bulk backtest job.
    Ordered by total_return extracted from the result JSON.
    """
    try:
        return BulkBacktestQueryService().get_results_page(
            bulk_job_id=job_id,
            user_id=current_user.user_id,
            page=page,
            page_size=page_size,
            sort_order=sort_order,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")


@router.get("/bulk-jobs/{job_id}/summary")
async def get_bulk_job_summary(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """
    Get aggregated summary statistics for a completed bulk backtest job.
    Computes winning/losing counts, averages, and top 10 from child results.
    """
    try:
        return BulkBacktestQueryService().get_summary(bulk_job_id=job_id, user_id=current_user.user_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Job not found")
