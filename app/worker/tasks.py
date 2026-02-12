"""Background Tasks for RQ Workers."""
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import traceback
import numpy as np
import json

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rq import get_current_job
from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine
from app.utils.ts_utils import moving_average, pct_change
from app.api.services.strategy_service import compile_strategy
from app.api.services.job_storage_service import get_job_storage

from app.domains.market.service import MarketService
from app.domains.backtests.dao.akshare_benchmark_dao import AkshareBenchmarkDao
from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
from app.domains.backtests.dao.bulk_backtest_dao import BulkBacktestDao
from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao


def convert_to_vnpy_symbol(ts_symbol: str) -> str:
    """
    Convert Tushare symbol format to VNPy format.
    """
    if not ts_symbol or "." not in ts_symbol:
        return ts_symbol
    
    code, exchange = ts_symbol.rsplit(".", 1)
    exchange_map = {
        "SZ": "SZSE",
        "SH": "SSE",
        "BJ": "BSE",
    }
    vnpy_exchange = exchange_map.get(exchange.upper(), exchange)
    return f"{code}.{vnpy_exchange}"


def resolve_symbol_name(input_symbol: str) -> str:
    try:
        return MarketService().resolve_symbol_name(input_symbol)
    except Exception:
        return ""


def get_benchmark_data_for_worker(start_date: str, end_date: str, benchmark_symbol: str = "399300.SZ") -> Optional[Dict]:
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date() if isinstance(start_date, str) else start_date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if isinstance(end_date, str) else end_date
        return AkshareBenchmarkDao().get_benchmark_data(start=start_dt, end=end_dt, benchmark_symbol=benchmark_symbol)
    except Exception as e:
        print(f"[Worker] Error fetching benchmark data: {e}")
        return None


def calculate_alpha_beta_for_worker(strategy_returns: np.ndarray, benchmark_returns: np.ndarray) -> tuple:
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
                        result: Dict, error: str = None, strategy_version: int = None):
    """Save backtest result to database for permanent storage."""
    try:
        now = datetime.utcnow()
        BacktestHistoryDao().upsert_history(
            user_id=user_id,
            job_id=job_id,
            strategy_id=strategy_id,
            strategy_class=strategy_class,
            strategy_version=strategy_version,
            vt_symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            parameters=parameters or {},
            status=status,
            result=result,
            error=error,
            created_at=now,
            completed_at=now if status in ["completed", "failed"] else None,
        )
        print(f"[Worker] Saved backtest {job_id} to database")
    except Exception as e:
        print(f"[Worker] Error saving to database: {e}")


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
    benchmark: str = "399300.SZ",
    user_id: int = None,
    strategy_id: int = None
) -> Dict[str, Any]:
    current_job = get_current_job()
    job_id = current_job.id if current_job else None
    
    try:
        vnpy_symbol = convert_to_vnpy_symbol(symbol)
        
        print(f"[Worker] Starting backtest job {job_id}")
        print(f"[Worker] Strategy: {strategy_class_name}, Symbol: {symbol} -> {vnpy_symbol}")
        
        strategy_class = None
        if strategy_code:
            strategy_class = compile_strategy(strategy_code, strategy_class_name)
        else:
            source_dao = StrategySourceDao()
            if strategy_id is not None and user_id is not None:
                strategy_code_db, strategy_class_name_db, _sv = source_dao.get_strategy_source_for_user(strategy_id, user_id)
                if strategy_class_name_db:
                    strategy_class_name = strategy_class_name_db
                strategy_class = compile_strategy(strategy_code_db, strategy_class_name)
            elif strategy_class_name:
                strategy_code_db = source_dao.get_strategy_code_by_class_name(strategy_class_name)
                strategy_class = compile_strategy(strategy_code_db, strategy_class_name)
            else:
                raise ValueError("No strategy code provided and no matching strategy found in database; jobs must include `strategy_code` or a valid `strategy_id`")
        
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
        
        if not strategy_class:
            raise RuntimeError(f"Strategy class '{strategy_class_name}' not loaded or compiled successfully")

        if parameters:
            engine.add_strategy(strategy_class, parameters)
        else:
            engine.add_strategy(strategy_class, {})

        print(f"[Worker] Loading data for {symbol}...")
        engine.load_data()
        
        print(f"[Worker] Running backtest...")
        engine.run_backtesting()
        
        print(f"[Worker] Calculating results...")
        df = engine.calculate_result()
        statistics = engine.calculate_statistics()

        equity_curve = None
        strategy_daily_returns = None
        if df is not None and not df.empty and "balance" in df.columns:
            equity_data = []
            for idx, row in df.iterrows():
                dt_str = idx.isoformat() if hasattr(idx, 'isoformat') else str(idx)
                equity_data.append({
                    "datetime": dt_str,
                    "balance": float(row["balance"]),
                    "net_pnl": float(row.get("net_pnl", 0))
                })
            equity_curve = equity_data
            
            balance_values = df["balance"].values
            if len(balance_values) > 1:
                strategy_daily_returns = np.diff(balance_values) / balance_values[:-1]

        alpha = None
        beta = None
        benchmark_return = None
        benchmark_data = get_benchmark_data_for_worker(start_date, end_date, benchmark)
        if benchmark_data and strategy_daily_returns is not None:
            alpha, beta = calculate_alpha_beta_for_worker(strategy_daily_returns, benchmark_data["returns"])
            benchmark_return = benchmark_data["total_return"]

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

        stock_price_curve = []
        if engine.history_data:
            for bar in engine.history_data:
                stock_price_curve.append({
                    "datetime": bar.datetime.isoformat() if bar.datetime else None,
                    "open": float(bar.open_price),
                    "high": float(bar.high_price),
                    "low": float(bar.low_price),
                    "close": float(bar.close_price)
                })

        benchmark_curve = None
        if benchmark_data and "prices" in benchmark_data:
            benchmark_curve = benchmark_data["prices"]

        symbol_name = resolve_symbol_name(symbol) or resolve_symbol_name(vnpy_symbol)

        result = {
            "job_id": job_id,
            "status": "completed",
            "symbol": symbol,
            "symbol_name": symbol_name,
            "start_date": start_date,
            "end_date": end_date,
            "initial_capital": initial_capital,
            "benchmark": benchmark,
            "parameters": parameters or {},
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
                "alpha": alpha,
                "beta": beta,
                "benchmark_return": benchmark_return,
                "benchmark_symbol": benchmark
            },
            "equity_curve": equity_curve,
            "trades": trades,
            "stock_price_curve": stock_price_curve,
            "benchmark_curve": benchmark_curve,
            "completed_at": datetime.now().isoformat(),
        }
        
        if user_id:
            _strategy_version = None
            try:
                _meta = get_job_storage().get_job_metadata(job_id)
                if _meta:
                    _sv = _meta.get("strategy_version")
                    _strategy_version = int(_sv) if _sv is not None else None
            except Exception:
                pass
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
                result=result,
                strategy_version=_strategy_version,
            )
        
        job_storage = get_job_storage()
        job_storage.update_job_status(job_id, "finished")
        job_storage.save_result(job_id, result)
        
        print(f"[Worker] Backtest job {job_id} completed successfully")
        return result
        
    except Exception as e:
        error_msg = f"Backtest failed: {str(e)}"
        print(f"[Worker] ERROR: {error_msg}")
        traceback.print_exc()
        
        try:
            job_storage = get_job_storage()
            job_storage.update_job_status(job_id, "failed", error=error_msg)
        except Exception:
            pass
        
        if user_id:
            _strategy_version = None
            try:
                _meta = get_job_storage().get_job_metadata(job_id)
                if _meta:
                    _sv = _meta.get("strategy_version")
                    _strategy_version = int(_sv) if _sv is not None else None
            except Exception:
                pass
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
                error=error_msg,
                strategy_version=_strategy_version,
            )
        
        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }


def run_bulk_backtest_task(
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
    benchmark: str = "399300.SZ",
    bulk_job_id: str = None,
    user_id: int = None,
    strategy_id: int = None,
) -> Dict[str, Any]:
    current_job = get_current_job()
    job_id = current_job.id if current_job else bulk_job_id

    job_storage = get_job_storage()
    total = len(symbols)
    successful = 0
    failed_count = 0
    best_return = None
    best_symbol = None
    best_symbol_name = None

    try:
        print(f"[Worker] Starting bulk backtest job {job_id}")
        print(f"[Worker] Strategy: {strategy_class_name}, Symbols: {total}")
        job_storage.update_job_status(job_id, "started")

        _strategy_version = None
        try:
            _meta = job_storage.get_job_metadata(job_id)
            if _meta:
                sv = _meta.get("strategy_version")
                _strategy_version = int(sv) if sv is not None else None
        except Exception:
            pass

        for idx, symbol in enumerate(symbols):
            child_job_id = f"{job_id}__{symbol}"
            try:
                print(f"[Worker] [{idx+1}/{total}] Processing {symbol}...")
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
                    benchmark=benchmark,
                    user_id=None,
                    strategy_id=strategy_id,
                )

                child_status = result.get("status", "failed")
                if child_status == "completed":
                    successful += 1
                    ret = result.get("statistics", {}).get("total_return")
                    if ret is not None and (best_return is None or ret > best_return):
                        best_return = ret
                        best_symbol = symbol
                        try:
                            best_symbol_name = result.get("symbol_name") or None
                        except Exception:
                            best_symbol_name = None
                else:
                    failed_count += 1

                _save_bulk_child(child_job_id, job_id, user_id, strategy_id,
                                 strategy_class_name, _strategy_version,
                                 symbol, start_date, end_date, parameters,
                                 child_status, result, result.get("error"))

            except Exception as e:
                print(f"[Worker] Error processing {symbol}: {e}")
                failed_count += 1
                _save_bulk_child(child_job_id, job_id, user_id, strategy_id,
                                 strategy_class_name, _strategy_version,
                                 symbol, start_date, end_date, parameters,
                                 "failed", None, str(e))

            completed = idx + 1
            pct = int(completed / total * 100)
            job_storage.update_progress(job_id, pct, f"{completed}/{total} symbols done")

            _update_bulk_row(job_id, completed, best_return, best_symbol, best_symbol_name)

        summary = {
            "job_id": job_id,
            "status": "completed",
            "total_symbols": total,
            "successful": successful,
            "failed": failed_count,
            "best_return": best_return,
            "best_symbol": best_symbol,
            "best_symbol_name": best_symbol_name,
            "parameters": parameters or {},
            "completed_at": datetime.now().isoformat(),
        }
        job_storage.update_job_status(job_id, "finished")
        job_storage.save_result(job_id, summary)

        _finish_bulk_row(job_id, "completed", best_return, best_symbol, best_symbol_name, total)

        print(f"[Worker] Bulk backtest {job_id} done: {successful} ok, {failed_count} failed")
        return summary

    except Exception as e:
        error_msg = f"Bulk backtest failed: {str(e)}"
        print(f"[Worker] ERROR: {error_msg}")
        traceback.print_exc()
        try:
            job_storage.update_job_status(job_id, "failed", error=error_msg)
        except Exception:
            pass
        if job_id:
            _finish_bulk_row(job_id, "failed", best_return, best_symbol, best_symbol_name, successful + failed_count)
        return {
            "job_id": job_id,
            "status": "failed",
            "error": error_msg,
            "traceback": traceback.format_exc(),
            "failed_at": datetime.now().isoformat(),
        }


def _save_bulk_child(child_job_id, bulk_job_id, user_id, strategy_id,
                     strategy_class, strategy_version, symbol, start_date,
                     end_date, parameters, status, result, error=None):
    try:
        now = datetime.utcnow()
        BacktestHistoryDao().upsert_history(
            user_id=user_id,
            job_id=child_job_id,
            bulk_job_id=bulk_job_id,
            strategy_id=strategy_id,
            strategy_class=strategy_class,
            strategy_version=strategy_version,
            vt_symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            parameters=parameters or {},
            status=status,
            result=result,
            error=error,
            created_at=now,
            completed_at=now if status in ("completed", "failed") else None,
        )
    except Exception as e:
        print(f"[Worker] Error saving bulk child {child_job_id}: {e}")


def _update_bulk_row(job_id, completed_count, best_return, best_symbol, best_symbol_name=None):
    try:
        BulkBacktestDao().update_progress(job_id, completed_count, best_return, best_symbol, best_symbol_name)
    except Exception as e:
        print(f"[Worker] Error updating bulk row: {e}")


def _finish_bulk_row(job_id, status, best_return, best_symbol, best_symbol_name, completed_count):
    try:
        BulkBacktestDao().finish(
            job_id,
            status,
            datetime.utcnow(),
            completed_count,
            best_return,
            best_symbol,
            best_symbol_name,
        )
    except Exception as e:
        print(f"[Worker] Error finishing bulk row: {e}")


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
    try:
        print(f"[Worker] Starting optimization job {job_id}")
        print(f"[Worker] Strategy: {strategy_class_name}, Symbol: {symbol}")
        
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
        
        engine.add_strategy(strategy_class, {})
        
        print(f"[Worker] Loading data for {symbol}...")
        engine.load_data()
        
        print(f"[Worker] Running optimization...")
        optimization_result = engine.run_ga_optimization(
            optimization_setting=optimization_settings,
            max_workers=4  # Use 4 worker processes
        )
        
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

