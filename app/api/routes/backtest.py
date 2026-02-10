"""Backtest routes."""
from datetime import datetime
from typing import List, Optional
import uuid
import json
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from sqlalchemy import text

from app.api.models.user import TokenData
from app.api.services.db import get_db_connection
from app.api.models.backtest import (
    BacktestRequest,
    BatchBacktestRequest,
    BacktestResult,
    BacktestJob,
    BatchBacktestJob,
    BacktestStatus,
)
from app.api.middleware.auth import get_current_user
from app.api.services.backtest_service import BacktestService
from app.api.worker.tasks import save_backtest_to_db
from app.api.services.job_storage import get_job_storage

router = APIRouter(prefix="/backtest", tags=["Backtest"])

# In-memory job store (replace with Redis in production)
_jobs: dict[str, BacktestJob] = {}
_batch_jobs: dict[str, BatchBacktestJob] = {}


class BacktestSubmitResponse(BaseModel):
    """Response after submitting a backtest."""
    job_id: str
    status: BacktestStatus
    message: str


@router.post("", response_model=BacktestSubmitResponse)
async def submit_backtest(
    request: BacktestRequest,
    background_tasks: BackgroundTasks,
    current_user: TokenData = Depends(get_current_user)
):
    """Submit a single backtest job."""
    job_id = str(uuid.uuid4())
    
    job = BacktestJob(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        progress=0.0,
        message="Queued for execution",
        created_at=datetime.utcnow()
    )
    _jobs[job_id] = job
    
    # Run backtest in background
    background_tasks.add_task(
        run_backtest_task,
        job_id,
        request,
        current_user.user_id
    )
    
    return BacktestSubmitResponse(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        message="Backtest queued successfully"
    )


@router.post("/batch", response_model=BacktestSubmitResponse)
async def submit_batch_backtest(
    request: BatchBacktestRequest,
    background_tasks: BackgroundTasks,
    current_user: TokenData = Depends(get_current_user)
):
    """Submit a batch backtest job."""
    job_id = str(uuid.uuid4())
    
    job = BatchBacktestJob(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        total_symbols=len(request.symbols),
        completed_symbols=0,
        progress=0.0,
        created_at=datetime.utcnow()
    )
    _batch_jobs[job_id] = job
    
    background_tasks.add_task(
        run_batch_backtest_task,
        job_id,
        request,
        current_user.user_id
    )
    
    return BacktestSubmitResponse(
        job_id=job_id,
        status=BacktestStatus.PENDING,
        message=f"Batch backtest queued for {len(request.symbols)} symbols"
    )


@router.get("/{job_id}", response_model=BacktestJob)
async def get_backtest_status(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """Get backtest job status and results."""
    if job_id in _jobs:
        return _jobs[job_id]
    
    raise HTTPException(status_code=404, detail="Job not found")


@router.get("/batch/{job_id}", response_model=BatchBacktestJob)
async def get_batch_backtest_status(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """Get batch backtest job status and results."""
    if job_id in _batch_jobs:
        return _batch_jobs[job_id]
    
    raise HTTPException(status_code=404, detail="Batch job not found")


@router.get("/history/list")
async def list_backtest_history(
    limit: int = 50,
    offset: int = 0,
    current_user: TokenData = Depends(get_current_user)
):
    """List past backtest runs for current user from database."""
    conn = get_db_connection()
    try:
        # Get total count
        count_result = conn.execute(
            text("SELECT COUNT(*) as total FROM backtest_history WHERE user_id = :user_id"),
            {"user_id": current_user.user_id}
        )
        total = count_result.fetchone().total
        
        # Get paginated results
        result = conn.execute(
            text("""
                SELECT id, job_id, strategy_id, strategy_class, strategy_version, vt_symbol,
                       start_date, end_date, status, result, created_at, completed_at
                FROM backtest_history
                WHERE user_id = :user_id
                ORDER BY created_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"user_id": current_user.user_id, "limit": limit, "offset": offset}
        )
        rows = result.fetchall()
        
        history = []
        for row in rows:
            # Extract key metrics from result JSON
            total_return = None
            sharpe_ratio = None
            if row.result:
                try:
                    result_data = json.loads(row.result) if isinstance(row.result, str) else row.result
                    stats = result_data.get("statistics", {})
                    total_return = stats.get("total_return")
                    sharpe_ratio = stats.get("sharpe_ratio")
                except:
                    pass
            
            history.append({
                "id": row.id,
                "job_id": row.job_id,
                "strategy_id": row.strategy_id,
                "strategy_class": row.strategy_class,
                "strategy_version": row.strategy_version,
                "vt_symbol": row.vt_symbol,
                "start_date": str(row.start_date) if row.start_date else None,
                "end_date": str(row.end_date) if row.end_date else None,
                "status": row.status,
                "total_return": total_return,
                "sharpe_ratio": sharpe_ratio,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "completed_at": row.completed_at.isoformat() if row.completed_at else None,
            })
        
        return {"total": total, "jobs": history}
    finally:
        conn.close()


@router.get("/history/{job_id}")
async def get_backtest_history_detail(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """Get detailed backtest result from database by job_id."""
    conn = get_db_connection()
    try:
        result = conn.execute(
            text("""
                SELECT id, job_id, strategy_id, strategy_class, strategy_version, vt_symbol,
                       start_date, end_date, parameters, status, result, error,
                       created_at, completed_at
                FROM backtest_history
                WHERE job_id = :job_id AND user_id = :user_id
            """),
            {"job_id": job_id, "user_id": current_user.user_id}
        )
        row = result.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="Backtest not found")
        
        # Parse result JSON
        result_data = None
        if row.result:
            try:
                result_data = json.loads(row.result) if isinstance(row.result, str) else row.result
            except:
                pass
        
        return {
            "id": row.id,
            "job_id": row.job_id,
            "strategy_id": row.strategy_id,
            "strategy_class": row.strategy_class,
            "strategy_version": row.strategy_version,
            "vt_symbol": row.vt_symbol,
            "start_date": str(row.start_date) if row.start_date else None,
            "end_date": str(row.end_date) if row.end_date else None,
            "parameters": json.loads(row.parameters) if row.parameters else {},
            "status": row.status,
            "result": result_data,
            "error": row.error,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        }
    finally:
        conn.close()


@router.delete("/{job_id}")
async def cancel_backtest(
    job_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """Cancel a pending or running backtest."""
    if job_id in _jobs:
        job = _jobs[job_id]
        if job.status in (BacktestStatus.PENDING, BacktestStatus.RUNNING):
            job.status = BacktestStatus.CANCELLED
            job.message = "Cancelled by user"
            return {"message": "Job cancelled"}
        raise HTTPException(status_code=400, detail="Job cannot be cancelled")
    
    raise HTTPException(status_code=404, detail="Job not found")


# Background task functions
async def run_backtest_task(job_id: str, request: BacktestRequest, user_id: int):
    """Run a single backtest in background."""
    job = _jobs[job_id]
    job.status = BacktestStatus.RUNNING
    job.started_at = datetime.utcnow()
    job.message = "Running backtest..."
    
    try:
        service = BacktestService()
        result = service.run_single_backtest(
            strategy_id=request.strategy_id,
            strategy_class=request.strategy_class,
            vt_symbol=request.vt_symbol,
            start_date=request.start_date,
            end_date=request.end_date,
            parameters=request.parameters,
            capital=request.capital,
            rate=request.rate,
            slippage=request.slippage,
            size=request.size,
            benchmark=getattr(request, 'benchmark', None)
        )
        
        job.status = BacktestStatus.COMPLETED
        job.completed_at = datetime.utcnow()
        job.progress = 100.0
        job.result = result
        job.message = "Backtest completed successfully"
        # Persist to database and job storage for history and UI
        try:
            # Convert result to serializable dict
            res_dict = result.dict() if hasattr(result, 'dict') else result
        except Exception:
            res_dict = None

        try:
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=request.strategy_id,
                strategy_class=request.strategy_class,
                symbol=request.vt_symbol,
                start_date=str(request.start_date),
                end_date=str(request.end_date),
                parameters=(res_dict.get('parameters') if isinstance(res_dict, dict) and res_dict.get('parameters') is not None else request.parameters),
                status='completed',
                result=res_dict,
            )
        except Exception:
            # Don't block the response on DB persistence
            pass

        try:
            # Save result to Redis job storage for UI consistency
            js = get_job_storage()
            js.save_job_metadata(job_id, {
                'job_id': job_id,
                'user_id': user_id,
                'type': 'backtest',
                'status': 'finished',
                'created_at': datetime.utcnow().isoformat(),
                'updated_at': datetime.utcnow().isoformat(),
                'parameters': res_dict.get('parameters') if isinstance(res_dict, dict) else request.parameters,
            })
            js.save_result(job_id, res_dict or {})
        except Exception:
            pass
        
    except Exception as e:
        job.status = BacktestStatus.FAILED
        job.completed_at = datetime.utcnow()
        job.error = str(e)
        job.message = f"Backtest failed: {str(e)}"
        try:
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=request.strategy_id,
                strategy_class=request.strategy_class,
                symbol=request.vt_symbol,
                start_date=str(request.start_date),
                end_date=str(request.end_date),
                parameters=request.parameters,
                status='failed',
                result=None,
                error=str(e),
            )
        except Exception:
            pass


async def run_batch_backtest_task(job_id: str, request: BatchBacktestRequest, user_id: int):
    """Run batch backtest in background."""
    job = _batch_jobs[job_id]
    job.status = BacktestStatus.RUNNING
    
    try:
        service = BacktestService()
        results = []
        errors = []
        
        for i, symbol in enumerate(request.symbols):
            if job.status == BacktestStatus.CANCELLED:
                break
            
            try:
                result = service.run_single_backtest(
                    strategy_id=request.strategy_id,
                    strategy_class=request.strategy_class,
                    vt_symbol=symbol,
                    start_date=request.start_date,
                    end_date=request.end_date,
                    parameters=request.parameters,
                    capital=request.capital,
                    rate=request.rate,
                    slippage=request.slippage,
                    size=request.size,
                    benchmark=getattr(request, 'benchmark', None)
                )
                if result:
                    results.append(result)
                    # Persist child result into backtest_history for batch runs
                    try:
                        child_job_id = f"{job_id}__{symbol}"
                        res_dict = result.dict() if hasattr(result, 'dict') else result
                        save_backtest_to_db(
                            job_id=child_job_id,
                            user_id=user_id,
                            strategy_id=request.strategy_id,
                            strategy_class=request.strategy_class,
                            symbol=symbol,
                            start_date=str(request.start_date),
                            end_date=str(request.end_date),
                            parameters=(res_dict.get('parameters') if isinstance(res_dict, dict) and res_dict.get('parameters') is not None else request.parameters),
                            status='completed',
                            result=res_dict,
                        )
                    except Exception:
                        pass
            except Exception as e:
                errors.append({"symbol": symbol, "error": str(e)})
            
            job.completed_symbols = i + 1
            job.progress = (i + 1) / len(request.symbols) * 100
        
        # Sort by total_return and keep top N
        results.sort(key=lambda r: r.total_return, reverse=True)
        job.results = results[:request.top_n]
        job.errors = errors
        job.status = BacktestStatus.COMPLETED
        job.completed_at = datetime.utcnow()
        
    except Exception as e:
        job.status = BacktestStatus.FAILED
        job.completed_at = datetime.utcnow()
