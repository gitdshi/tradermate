"""Backtest service with RQ worker integration."""
from datetime import date, datetime
from typing import Optional, Dict, Any, List
import uuid

from app.api.models.backtest import BacktestStatus
from app.api.worker.config import get_queue
from app.api.worker.tasks import (
    run_backtest_task,
    run_bulk_backtest_task,
    run_optimization_task
)
from app.api.services.job_storage import get_job_storage
from app.api.services.db import get_db_connection
from sqlalchemy import text


class BacktestServiceV2:
    """Service for managing backtests with RQ workers."""
    
    def __init__(self):
        self.job_storage = get_job_storage()
        self.builtin_strategies = {
            "TripleMAStrategy": "app.strategies.triple_ma_strategy",
            "TurtleTradingStrategy": "app.strategies.turtle_trading",
        }
    
    def submit_backtest(
        self,
        user_id: int,
        strategy_id: Optional[int],
        strategy_class_name: Optional[str],
        symbol: str,
        start_date: date,
        end_date: date,
        initial_capital: float = 100000.0,
        rate: float = 0.0001,
        slippage: float = 0.0,
        size: int = 1,
        pricetick: float = 0.01,
        parameters: Optional[Dict[str, Any]] = None,
        symbol_name: str = "",
        strategy_name: str = "",
        benchmark: str = "399300.SZ",
    ) -> str:
        """
        Submit a backtest job to RQ queue.
        
        Returns:
            Job ID
        """
        # Generate job ID
        job_id = f"bt_{uuid.uuid4().hex[:16]}"
        
        # Get strategy code if custom strategy
        strategy_code = None
        strategy_version = None
        if strategy_id:
            strategy_code, strategy_class_name, strategy_version = self._get_strategy_from_db(strategy_id, user_id)
        
        # If symbol_name not provided, try to fetch from stock_basic table
        if not symbol_name:
            conn = get_db_connection()
            try:
                result_row = conn.execute(
                    text("SELECT name FROM stock_basic WHERE ts_code = :s OR symbol = :s LIMIT 1"),
                    {"s": symbol}
                ).fetchone()
                if result_row:
                    # SQLAlchemy Row proxies allow attribute access
                    symbol_name = result_row.name if hasattr(result_row, 'name') else list(result_row)[0]
            except Exception:
                # Ignore DB lookup errors and leave symbol_name empty
                pass
            finally:
                conn.close()
        # Save job metadata
        metadata = {
            "job_id": job_id,
            "user_id": user_id,
            "type": "backtest",
            "status": "queued",
            "strategy_id": strategy_id,
            "strategy_class": strategy_class_name,
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "symbol": symbol,
            "symbol_name": symbol_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "initial_capital": initial_capital,
            "rate": rate,
            "slippage": slippage,
            "benchmark": benchmark,
            "parameters": parameters or {},
            "created_at": datetime.now().isoformat(),
            "progress": 0,
        }
        self.job_storage.save_job_metadata(job_id, metadata)
        
        # Enqueue job using RQ 2.x API
        queue = get_queue('backtest')
        queue.enqueue(
            run_backtest_task,
            strategy_code=strategy_code,
            strategy_class_name=strategy_class_name,
            symbol=symbol,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            initial_capital=initial_capital,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            parameters=parameters,
            benchmark=benchmark,
            job_id=job_id,
            user_id=user_id,
            strategy_id=strategy_id,
            job_timeout=3600,
            result_ttl=86400 * 7,
        )
        
        return job_id
    
    def submit_batch_backtest(
        self,
        user_id: int,
        strategy_id: Optional[int],
        strategy_class_name: Optional[str],
        symbols: List[str],
        start_date: date,
        end_date: date,
        initial_capital: float = 100000.0,
        rate: float = 0.0001,
        slippage: float = 0.0,
        size: int = 1,
        pricetick: float = 0.01,
        parameters: Optional[Dict[str, Any]] = None,
        strategy_name: str = "",
        benchmark: str = "399300.SZ",
    ) -> str:
        """
        Submit a bulk backtest job to RQ queue.
        
        Returns:
            Job ID
        """
        # Generate job ID
        job_id = f"bulk_{uuid.uuid4().hex[:16]}"
        
        # Get strategy code if custom strategy
        strategy_code = None
        strategy_version = None
        if strategy_id:
            strategy_code, strategy_class_name, strategy_version = self._get_strategy_from_db(strategy_id, user_id)
        
        # Save job metadata
        metadata = {
            "job_id": job_id,
            "user_id": user_id,
            "type": "bulk_backtest",
            "status": "queued",
            "strategy_id": strategy_id,
            "strategy_class": strategy_class_name,
            "strategy_name": strategy_name,
            "strategy_version": strategy_version,
            "symbols": symbols,
            "total_symbols": len(symbols),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "initial_capital": initial_capital,
            "rate": rate,
            "slippage": slippage,
            "benchmark": benchmark,
            "parameters": parameters or {},
            "created_at": datetime.now().isoformat(),
            "progress": 0,
        }
        self.job_storage.save_job_metadata(job_id, metadata)
        
        # Insert into bulk_backtest DB table
        import json as _json
        conn = get_db_connection()
        try:
            conn.execute(
                text("""
                    INSERT INTO bulk_backtest
                    (user_id, job_id, strategy_id, strategy_class, strategy_version,
                     symbols, start_date, end_date, parameters, initial_capital,
                     rate, slippage, benchmark, status, total_symbols, created_at)
                    VALUES
                    (:user_id, :job_id, :strategy_id, :strategy_class, :strategy_version,
                     :symbols, :start_date, :end_date, :parameters, :initial_capital,
                     :rate, :slippage, :benchmark, 'queued', :total_symbols, :created_at)
                """),
                {
                    "user_id": user_id,
                    "job_id": job_id,
                    "strategy_id": strategy_id,
                    "strategy_class": strategy_class_name,
                    "strategy_version": strategy_version,
                    "symbols": _json.dumps(symbols),
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                    "parameters": _json.dumps(parameters) if parameters else "{}",
                    "initial_capital": initial_capital,
                    "rate": rate,
                    "slippage": slippage,
                    "benchmark": benchmark,
                    "total_symbols": len(symbols),
                    "created_at": datetime.now(),
                }
            )
            conn.commit()
        except Exception as e:
            print(f"[Service] Error inserting bulk_backtest row: {e}")
        finally:
            conn.close()
        
        # Enqueue job using RQ 2.x API
        queue = get_queue('backtest')
        queue.enqueue(
            run_bulk_backtest_task,
            strategy_code=strategy_code,
            strategy_class_name=strategy_class_name,
            symbols=symbols,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            initial_capital=initial_capital,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            parameters=parameters,
            benchmark=benchmark,
            job_id=job_id,
            user_id=user_id,
            strategy_id=strategy_id,
            job_timeout=7200,  # 2 hours
            result_ttl=86400 * 7,
        )
        
        return job_id
    
    def submit_optimization(
        self,
        user_id: int,
        strategy_id: Optional[int],
        strategy_class_name: Optional[str],
        symbol: str,
        start_date: date,
        end_date: date,
        optimization_settings: Dict[str, Any],
        initial_capital: float = 100000.0,
        rate: float = 0.0001,
        slippage: float = 0.0,
        size: int = 1,
        pricetick: float = 0.01,
    ) -> str:
        """
        Submit a parameter optimization job to RQ queue.
        
        Returns:
            Job ID
        """
        # Generate job ID
        job_id = f"opt_{uuid.uuid4().hex[:16]}"
        
        # Get strategy code if custom strategy
        strategy_code = None
        if strategy_id:
            strategy_code, strategy_class_name, _ = self._get_strategy_from_db(strategy_id, user_id)
        
        # Save job metadata
        metadata = {
            "job_id": job_id,
            "user_id": user_id,
            "type": "optimization",
            "status": "queued",
            "strategy_class": strategy_class_name,
            "symbol": symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "initial_capital": initial_capital,
            "optimization_settings": optimization_settings,
            "created_at": datetime.now().isoformat(),
            "progress": 0,
        }
        self.job_storage.save_job_metadata(job_id, metadata)
        
        # Enqueue job using RQ 2.x API
        queue = get_queue('optimization')
        queue.enqueue(
            run_optimization_task,
            strategy_code=strategy_code,
            strategy_class_name=strategy_class_name,
            symbol=symbol,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            initial_capital=initial_capital,
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            optimization_settings=optimization_settings,
            job_id=job_id,
            job_timeout=14400,  # 4 hours
            result_ttl=86400 * 7,
        )
        
        return job_id
    
    def get_job_status(self, job_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        """
        Get job status and result.
        
        Args:
            job_id: Job ID
            user_id: User ID (for authorization)
        
        Returns:
            Job status dict or None
        """
        # Get metadata
        metadata = self.job_storage.get_job_metadata(job_id)

        if not metadata:
            return None

        # Check authorization
        if metadata.get("user_id") != user_id:
            return None

        # Get result if completed
        result = None
        if metadata.get("status") in ["completed", "failed", "finished"]:
            result = self.job_storage.get_result(job_id)

        # Build response merging metadata fields so frontend can access symbol_name, strategy_name, etc.
        response: Dict[str, Any] = {
            "job_id": job_id,
            "status": metadata.get("status"),
            "type": metadata.get("type"),
            "progress": metadata.get("progress", 0),
            "progress_message": metadata.get("progress_message", ""),
            "created_at": metadata.get("created_at"),
            "updated_at": metadata.get("updated_at"),
            "result": result,
        }

        # Merge selected metadata fields at top level for backwards compatibility
        for key in ["symbol", "symbol_name", "strategy_id", "strategy_class", "strategy_name", "strategy_version", "start_date", "end_date", "initial_capital", "rate", "slippage", "benchmark", "parameters", "symbols", "total_symbols"]:
            if key in metadata:
                response[key] = metadata.get(key)

        return response
    
    def list_user_jobs(
        self, 
        user_id: int,
        status: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        List jobs for a user.
        
        Args:
            user_id: User ID
            status: Optional status filter
            limit: Maximum number of jobs
        
        Returns:
            List of job metadata
        """
        return self.job_storage.list_user_jobs(user_id, status, limit)
    
    def cancel_job(self, job_id: str, user_id: int) -> bool:
        """
        Cancel a job.
        
        Args:
            job_id: Job ID
            user_id: User ID (for authorization)
        
        Returns:
            True if cancelled, False otherwise
        """
        # Get metadata
        metadata = self.job_storage.get_job_metadata(job_id)
        
        if not metadata or metadata.get("user_id") != user_id:
            return False
        
        # Determine queue
        job_type = metadata.get("type", "")
        if "optimization" in job_type:
            queue = get_queue('optimization')
        else:
            queue = get_queue('backtest')
        
        return self.job_storage.cancel_job(job_id, queue)
    
    def _get_strategy_from_db(self, strategy_id: int, user_id: int) -> tuple:
        """
        Get strategy code, class name, and version from database.
        
        Args:
            strategy_id: Strategy ID
            user_id: User ID (for authorization)
        
        Returns:
            Tuple of (code, class_name, version)
        """
        conn = get_db_connection()
        try:
            result = conn.execute(
                text(
                    "SELECT code, class_name, version FROM strategies "
                    "WHERE id = :id AND user_id = :user_id"
                ),
                {"id": strategy_id, "user_id": user_id}
            )
            row = result.fetchone()
            
            if not row:
                raise ValueError(f"Strategy {strategy_id} not found or access denied")
            
            return row.code, row.class_name, row.version
            
        finally:
            conn.close()


# Singleton instance
_backtest_service_v2 = None


def get_backtest_service_v2() -> BacktestServiceV2:
    """Get BacktestServiceV2 singleton instance."""
    global _backtest_service_v2
    if _backtest_service_v2 is None:
        _backtest_service_v2 = BacktestServiceV2()
    return _backtest_service_v2
