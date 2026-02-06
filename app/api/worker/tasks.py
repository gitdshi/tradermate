"""Background Tasks for RQ Workers."""
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import traceback
import numpy as np
import json

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rq import get_current_job
from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine
from app.backtest.ts_utils import moving_average, pct_change
from app.api.services.strategy_service import compile_strategy
from app.api.services.db import get_db_connection, get_tushare_connection
from app.api.services.job_storage import get_job_storage
from sqlalchemy import text


def convert_to_vnpy_symbol(ts_symbol: str) -> str:
    """
    Convert Tushare symbol format to VNPy format.
    
    Tushare: 000001.SZ, 600000.SH
    VNPy: 000001.SZSE, 600000.SSE
    """
    if not ts_symbol or "." not in ts_symbol:
        return ts_symbol
    
    code, exchange = ts_symbol.rsplit(".", 1)
    exchange_map = {
        "SZ": "SZSE",  # Shenzhen Stock Exchange
        "SH": "SSE",   # Shanghai Stock Exchange
        "BJ": "BSE",   # Beijing Stock Exchange
    }
    vnpy_exchange = exchange_map.get(exchange.upper(), exchange)
    return f"{code}.{vnpy_exchange}"


def get_benchmark_data_for_worker(start_date: str, end_date: str, benchmark_symbol: str = "399300.SZ") -> Optional[Dict]:
    """
    Fetch HS300 benchmark data for the given period (worker version).
    """
    conn = get_tushare_connection()
    
    try:
        # Convert date format - handle both string and date objects
        if isinstance(start_date, str):
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_dt = datetime.combine(start_date, datetime.min.time())
        
        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_dt = datetime.combine(end_date, datetime.min.time())
        
        # Note: index_daily uses 'index_code' column, not 'ts_code'
        query = """
            SELECT trade_date, close
            FROM index_daily
            WHERE index_code = :index_code
              AND trade_date >= :start_date
              AND trade_date <= :end_date
            ORDER BY trade_date ASC
        """
        
        result = conn.execute(text(query), {
            "index_code": benchmark_symbol,
            "start_date": start_dt.strftime("%Y%m%d"),
            "end_date": end_dt.strftime("%Y%m%d")
        })
        rows = result.fetchall()
        
        if not rows or len(rows) < 2:
            return None
        
        # Extract dates and closes
        dates = [row.trade_date for row in rows]
        closes = np.array([float(row.close) for row in rows])
        daily_returns = np.diff(closes) / closes[:-1]
        total_return = (closes[-1] - closes[0]) / closes[0] * 100
        
        # Format prices for chart
        prices = []
        for date_val, close_val in zip(dates, closes):
            # Handle both string (YYYYMMDD) and datetime.date objects from DB
            if isinstance(date_val, str):
                dt_obj = datetime.strptime(date_val, "%Y%m%d")
            else:
                # Already a date/datetime object
                dt_obj = datetime.combine(date_val, datetime.min.time()) if not isinstance(date_val, datetime) else date_val
            prices.append({
                "datetime": dt_obj.isoformat(),
                "close": float(close_val)
            })
        
        return {
            "returns": daily_returns,
            "total_return": float(total_return),
            "prices": prices
        }
        
    except Exception as e:
        print(f"[Worker] Error fetching benchmark data: {e}")
        return None
    finally:
        conn.close()


def calculate_alpha_beta_for_worker(strategy_returns: np.ndarray, benchmark_returns: np.ndarray) -> tuple:
    """Calculate alpha and beta using linear regression."""
    if len(strategy_returns) < 2 or len(benchmark_returns) < 2:
        return None, None
    
    min_len = min(len(strategy_returns), len(benchmark_returns))
    strategy_returns = strategy_returns[:min_len]
    benchmark_returns = benchmark_returns[:min_len]
    
    mask = ~(np.isnan(strategy_returns) | np.isnan(benchmark_returns))
    strategy_returns = strategy_returns[mask]
    benchmark_returns = benchmark_returns[mask]
    
    if len(strategy_returns) < 2:
        return None, None
    
    try:
        beta, alpha = np.polyfit(benchmark_returns, strategy_returns, 1)
        alpha_annualized = alpha * 252
        return float(alpha_annualized), float(beta)
    except Exception:
        return None, None


def save_backtest_to_db(job_id: str, user_id: int, strategy_id: Optional[int], 
                        strategy_class: str, symbol: str, start_date: str, 
                        end_date: str, parameters: Dict, status: str, 
                        result: Dict, error: str = None):
    """Save backtest result to database for permanent storage."""
    conn = get_db_connection()
    try:
        now = datetime.utcnow()
        conn.execute(
            text("""
                INSERT INTO backtest_history 
                (user_id, job_id, strategy_id, strategy_class, vt_symbol, 
                 start_date, end_date, parameters, status, result, error, 
                 created_at, completed_at)
                VALUES 
                (:user_id, :job_id, :strategy_id, :strategy_class, :vt_symbol,
                 :start_date, :end_date, :parameters, :status, :result, :error,
                 :created_at, :completed_at)
                ON DUPLICATE KEY UPDATE
                status = :status, result = :result, error = :error, completed_at = :completed_at
            """),
            {
                "user_id": user_id,
                "job_id": job_id,
                "strategy_id": strategy_id,
                "strategy_class": strategy_class,
                "vt_symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "parameters": json.dumps(parameters) if parameters else "{}",
                "status": status,
                "result": json.dumps(result) if result else None,
                "error": error,
                "created_at": now,
                "completed_at": now if status in ["completed", "failed"] else None
            }
        )
        conn.commit()
        print(f"[Worker] Saved backtest {job_id} to database")
    except Exception as e:
        print(f"[Worker] Error saving to database: {e}")
    finally:
        conn.close()


def run_backtest_task(
    strategy_code: Optional[str],
    strategy_class_name: str,
    symbol: str,
    start_date: str,
    end_date: str,
    initial_capital: float,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    parameters: Optional[Dict[str, Any]] = None,
    user_id: int = None,
    strategy_id: int = None
) -> Dict[str, Any]:
    """
    Run backtest task in background worker.
    
    Args:
        strategy_code: Custom strategy code (if not builtin)
        strategy_class_name: Name of strategy class
        symbol: Trading symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        initial_capital: Initial capital
        rate: Commission rate
        slippage: Slippage
        size: Contract size
        pricetick: Price tick
        parameters: Strategy parameters
        user_id: User ID for DB storage
        strategy_id: Strategy ID for DB storage
    
    Returns:
        Dict with backtest results
    """
    # Get job_id from RQ context (RQ uses job_id kwarg as the actual job ID)
    current_job = get_current_job()
    job_id = current_job.id if current_job else None
    
    try:
        # Convert symbol to VNPy format
        vnpy_symbol = convert_to_vnpy_symbol(symbol)
        
        print(f"[Worker] Starting backtest job {job_id}")
        print(f"[Worker] Strategy: {strategy_class_name}, Symbol: {symbol} -> {vnpy_symbol}")
        
        # Load strategy class
        if strategy_code:
            # Compile custom strategy
            strategy_class = compile_strategy(strategy_code, strategy_class_name)
        else:
            # Load builtin strategy
            from app.strategies.triple_ma_strategy import TripleMAStrategy
            from app.strategies.turtle_trading import TurtleTradingStrategy
            
            builtin_strategies = {
                'TripleMAStrategy': TripleMAStrategy,
                'TurtleTradingStrategy': TurtleTradingStrategy,
            }
            strategy_class = builtin_strategies.get(strategy_class_name)
            
            if not strategy_class:
                raise ValueError(f"Unknown builtin strategy: {strategy_class_name}")
        
        # Initialize backtest engine
        engine = BacktestingEngine()
        engine.set_parameters(
            vt_symbol=vnpy_symbol,
            interval=Interval.DAILY,
            start=datetime.strptime(start_date, "%Y-%m-%d"),
            end=datetime.strptime(end_date, "%Y-%m-%d"),
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=initial_capital,
        )
        
        # Add strategy
        if parameters:
            engine.add_strategy(strategy_class, parameters)
        else:
            engine.add_strategy(strategy_class, {})
        
        # Load data
        print(f"[Worker] Loading data for {symbol}...")
        engine.load_data()
        
        # Run backtest
        print(f"[Worker] Running backtest...")
        engine.run_backtesting()
        
        # Calculate results
        print(f"[Worker] Calculating results...")
        df = engine.calculate_result()
        statistics = engine.calculate_statistics()
        
        # Build equity curve data for charts
        equity_curve = None
        strategy_daily_returns = None
        if df is not None and not df.empty and "balance" in df.columns:
            # Convert DataFrame index (datetime) to ISO strings for JSON serialization
            equity_data = []
            for idx, row in df.iterrows():
                dt_str = idx.isoformat() if hasattr(idx, 'isoformat') else str(idx)
                equity_data.append({
                    "datetime": dt_str,
                    "balance": float(row["balance"]),
                    "net_pnl": float(row.get("net_pnl", 0))
                })
            equity_curve = equity_data
            
            # Calculate daily returns for alpha/beta
            balance_values = df["balance"].values
            if len(balance_values) > 1:
                strategy_daily_returns = np.diff(balance_values) / balance_values[:-1]
        
        # Calculate alpha and beta against HS300 benchmark
        alpha = None
        beta = None
        benchmark_return = None
        benchmark_data = get_benchmark_data_for_worker(start_date, end_date, "399300.SZ")
        if benchmark_data and strategy_daily_returns is not None:
            alpha, beta = calculate_alpha_beta_for_worker(strategy_daily_returns, benchmark_data["returns"])
            benchmark_return = benchmark_data["total_return"]
        
        # Build trade list
        trades = []
        if engine.trades:
            for t in list(engine.trades.values())[:100]:
                trades.append({
                    "datetime": t.datetime.isoformat() if t.datetime else None,
                    "symbol": t.symbol,
                    "direction": str(t.direction.value) if hasattr(t.direction, 'value') else str(t.direction),
                    "offset": str(t.offset.value) if hasattr(t.offset, 'value') else str(t.offset),
                    "price": float(t.price),
                    "volume": float(t.volume)
                })
        
        # Build stock price curve from historical data
        stock_price_curve = []
        if engine.history_data:
            for bar in engine.history_data:
                stock_price_curve.append({
                    "datetime": bar.datetime.isoformat() if bar.datetime else None,
                    "close": float(bar.close_price)
                })
        
        # Add benchmark curve if available
        benchmark_curve = None
        if benchmark_data and "prices" in benchmark_data:
            benchmark_curve = benchmark_data["prices"]
        
        # Build result with all metrics
        result = {
            "job_id": job_id,
            "status": "completed",
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "statistics": {
                "total_return": float(statistics.get("total_return", 0)),
                "annual_return": float(statistics.get("annual_return", 0)),
                "max_drawdown": float(statistics.get("max_drawdown", 0)),
                "max_drawdown_percent": float(statistics.get("max_ddpercent", 0)),
                "sharpe_ratio": float(statistics.get("sharpe_ratio", 0)),
                "total_trades": int(statistics.get("total_trade_count", 0)),
                "winning_rate": float(statistics.get("winning_rate", 0)),
                "profit_factor": float(statistics.get("profit_factor", 0)),
                "total_days": int(statistics.get("total_days", 0)),
                "profit_days": int(statistics.get("profit_days", 0)),
                "loss_days": int(statistics.get("loss_days", 0)),
                "end_balance": float(statistics.get("end_balance", 0)),
                # Benchmark comparison
                "alpha": alpha,
                "beta": beta,
                "benchmark_return": benchmark_return,
                "benchmark_symbol": "399300.SZ"
            },
            "equity_curve": equity_curve,
            "trades": trades,
            "stock_price_curve": stock_price_curve,
            "benchmark_curve": benchmark_curve,
            "completed_at": datetime.now().isoformat(),
        }
        
        # Save to database for permanent storage
        if user_id:
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_class=strategy_class_name,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                parameters=parameters or {},
                status="completed",
                result=result
            )
        
        # Update job storage for API status tracking
        job_storage = get_job_storage()
        job_storage.update_job_status(job_id, "finished")
        job_storage.save_result(job_id, result)
        
        print(f"[Worker] Backtest job {job_id} completed successfully")
        return result
        
    except Exception as e:
        error_msg = f"Backtest failed: {str(e)}"
        print(f"[Worker] ERROR: {error_msg}")
        traceback.print_exc()
        
        # Update job storage with error
        try:
            job_storage = get_job_storage()
            job_storage.update_job_status(job_id, "failed", error=error_msg)
        except Exception:
            pass
        
        # Save failed job to database
        if user_id:
            save_backtest_to_db(
                job_id=job_id,
                user_id=user_id,
                strategy_id=strategy_id,
                strategy_class=strategy_class_name,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                parameters=parameters or {},
                status="failed",
                result=None,
                error=error_msg
            )
        
        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }


def run_batch_backtest_task(
    strategy_code: Optional[str],
    strategy_class_name: str,
    symbols: list[str],
    start_date: str,
    end_date: str,
    initial_capital: float,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    parameters: Optional[Dict[str, Any]] = None,
    job_id: str = None
) -> Dict[str, Any]:
    """
    Run batch backtest task in background worker.
    
    Args:
        strategy_code: Custom strategy code (if not builtin)
        strategy_class_name: Name of strategy class
        symbols: List of trading symbols
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        initial_capital: Initial capital
        rate: Commission rate
        slippage: Slippage
        size: Contract size
        pricetick: Price tick
        parameters: Strategy parameters
        job_id: Job ID for tracking
    
    Returns:
        Dict with batch backtest results
    """
    try:
        print(f"[Worker] Starting batch backtest job {job_id}")
        print(f"[Worker] Strategy: {strategy_class_name}, Symbols: {len(symbols)}")
        
        results = []
        successful = 0
        failed = 0
        
        for symbol in symbols:
            try:
                print(f"[Worker] Processing {symbol}...")
                result = run_backtest_task(
                    strategy_code=strategy_code,
                    strategy_class_name=strategy_class_name,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    initial_capital=initial_capital,
                    rate=rate,
                    slippage=slippage,
                    size=size,
                    pricetick=pricetick,
                    parameters=parameters,
                    job_id=f"{job_id}_{symbol}"
                )
                
                if result["status"] == "completed":
                    successful += 1
                else:
                    failed += 1
                
                results.append(result)
                
            except Exception as e:
                print(f"[Worker] Error processing {symbol}: {e}")
                failed += 1
                results.append({
                    "symbol": symbol,
                    "status": "failed",
                    "error": str(e)
                })
        
        return {
            "job_id": job_id,
            "status": "completed",
            "total_symbols": len(symbols),
            "successful": successful,
            "failed": failed,
            "results": results,
            "completed_at": datetime.now().isoformat(),
        }
        
    except Exception as e:
        error_msg = f"Batch backtest failed: {str(e)}"
        print(f"[Worker] ERROR: {error_msg}")
        traceback.print_exc()
        
        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }


def run_optimization_task(
    strategy_code: Optional[str],
    strategy_class_name: str,
    symbol: str,
    start_date: str,
    end_date: str,
    initial_capital: float,
    rate: float,
    slippage: float,
    size: int,
    pricetick: float,
    optimization_settings: Dict[str, Any],
    job_id: str = None
) -> Dict[str, Any]:
    """
    Run parameter optimization task in background worker.
    
    Args:
        strategy_code: Custom strategy code (if not builtin)
        strategy_class_name: Name of strategy class
        symbol: Trading symbol
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        initial_capital: Initial capital
        rate: Commission rate
        slippage: Slippage
        size: Contract size
        pricetick: Price tick
        optimization_settings: Parameter ranges for optimization
        job_id: Job ID for tracking
    
    Returns:
        Dict with optimization results
    """
    try:
        print(f"[Worker] Starting optimization job {job_id}")
        print(f"[Worker] Strategy: {strategy_class_name}, Symbol: {symbol}")
        
        # Load strategy class
        if strategy_code:
            strategy_class = compile_strategy(strategy_code, strategy_class_name)
        else:
            from app.strategies.triple_ma_strategy import TripleMAStrategy
            from app.strategies.turtle_trading import TurtleTradingStrategy
            
            builtin_strategies = {
                'TripleMAStrategy': TripleMAStrategy,
                'TurtleTradingStrategy': TurtleTradingStrategy,
            }
            strategy_class = builtin_strategies.get(strategy_class_name)
            
            if not strategy_class:
                raise ValueError(f"Unknown builtin strategy: {strategy_class_name}")
        
        # Initialize backtest engine
        engine = BacktestingEngine()
        engine.set_parameters(
            vt_symbol=symbol,
            interval=Interval.DAILY,
            start=datetime.strptime(start_date, "%Y-%m-%d"),
            end=datetime.strptime(end_date, "%Y-%m-%d"),
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=pricetick,
            capital=initial_capital,
        )
        
        # Add strategy
        engine.add_strategy(strategy_class, {})
        
        # Load data
        print(f"[Worker] Loading data for {symbol}...")
        engine.load_data()
        
        # Run optimization
        print(f"[Worker] Running optimization...")
        optimization_result = engine.run_ga_optimization(
            optimization_setting=optimization_settings,
            max_workers=4  # Use 4 worker processes
        )
        
        # Format results
        results = []
        for params, stats in optimization_result:
            results.append({
                "parameters": params,
                "statistics": {
                    "total_return": float(stats.get("total_return", 0)),
                    "annual_return": float(stats.get("annual_return", 0)),
                    "max_drawdown": float(stats.get("max_drawdown", 0)),
                    "sharpe_ratio": float(stats.get("sharpe_ratio", 0)),
                }
            })
        
        # Sort by sharpe ratio
        results.sort(key=lambda x: x["statistics"]["sharpe_ratio"], reverse=True)
        
        return {
            "job_id": job_id,
            "status": "completed",
            "symbol": symbol,
            "total_combinations": len(results),
            "best_parameters": results[0]["parameters"] if results else {},
            "best_statistics": results[0]["statistics"] if results else {},
            "top_10_results": results[:10],
            "completed_at": datetime.now().isoformat(),
        }
        
    except Exception as e:
        error_msg = f"Optimization failed: {str(e)}"
        print(f"[Worker] ERROR: {error_msg}")
        traceback.print_exc()
        
        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }
