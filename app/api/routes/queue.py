"""Queue monitoring and management routes."""
from fastapi import APIRouter, Depends, HTTPException
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
    """Delete a job and its results."""
    # First check if user owns the job
    service = get_backtest_service_v2()
    job = service.get_job_status(job_id, current_user.user_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Delete from storage
    job_storage = get_job_storage()
    deleted = job_storage.delete_job(job_id)
    
    if not deleted:
        raise HTTPException(status_code=500, detail="Failed to delete job")
    
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
    )
    
    return {"job_id": job_id, "status": "queued", "message": "Backtest job submitted to queue"}
