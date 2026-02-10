"""Backtest models."""
from datetime import datetime, date
from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field


class BacktestStatus(str, Enum):
    """Backtest job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BacktestRequest(BaseModel):
    """Single backtest request."""
    strategy_id: Optional[int] = None
    strategy_class: Optional[str] = None  # Use built-in strategy by class name
    vt_symbol: str = Field(..., description="Symbol in format '000001.SZSE'")
    start_date: date
    end_date: date
    parameters: Dict[str, Any] = Field(default_factory=dict)
    capital: float = 100000.0
    rate: float = 0.0001  # Commission rate
    slippage: float = 0.0
    size: int = 1  # Contract size
    benchmark: Optional[str] = None


class BatchBacktestRequest(BaseModel):
    """Batch backtest request."""
    strategy_id: Optional[int] = None
    strategy_class: Optional[str] = None
    symbols: List[str] = Field(..., description="List of vt_symbols")
    start_date: date
    end_date: date
    parameters: Dict[str, Any] = Field(default_factory=dict)
    capital: float = 100000.0
    rate: float = 0.0001
    slippage: float = 0.0
    size: int = 1
    top_n: int = 10  # Return top N results
    benchmark: Optional[str] = None


class BacktestResult(BaseModel):
    """Backtest result statistics."""
    symbol: str
    start_date: date
    end_date: date
    total_days: int = 0
    profit_days: int = 0
    loss_days: int = 0
    capital: float = 100000.0
    end_balance: float = 0.0
    total_return: float = 0.0
    annual_return: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_percent: float = 0.0
    sharpe_ratio: float = 0.0
    total_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    
    # Benchmark comparison (HS300)
    alpha: Optional[float] = None
    beta: Optional[float] = None
    benchmark_return: Optional[float] = None
    benchmark_symbol: str = "000300.SH"
    
    # Daily data for charts
    daily_returns: Optional[List[Dict[str, Any]]] = None
    equity_curve: Optional[List[Dict[str, Any]]] = None
    trades: Optional[List[Dict[str, Any]]] = None
    stock_price_curve: Optional[List[Dict[str, Any]]] = None
    benchmark_curve: Optional[List[Dict[str, Any]]] = None
    symbol_name: Optional[str] = None
    # Parameters used for the backtest (merged defaults + overrides)
    parameters: Optional[Dict[str, Any]] = None


class BacktestJob(BaseModel):
    """Backtest job status."""
    job_id: str
    status: BacktestStatus
    progress: float = 0.0  # 0-100
    message: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    result: Optional[BacktestResult] = None
    error: Optional[str] = None


class BatchBacktestJob(BaseModel):
    """Batch backtest job status."""
    job_id: str
    status: BacktestStatus
    total_symbols: int
    completed_symbols: int = 0
    progress: float = 0.0
    created_at: datetime
    completed_at: Optional[datetime] = None
    results: List[BacktestResult] = Field(default_factory=list)
    errors: List[Dict[str, str]] = Field(default_factory=list)
