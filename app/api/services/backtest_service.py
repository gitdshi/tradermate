"""Unified backtest service combining job-based APIs and sync run methods.

Includes BacktestServiceV2 (RQ job integration) and BacktestService which
extends it with synchronous backtest execution helpers. Exposes
`get_backtest_service()` and compatibility alias `get_backtest_service_v2()`.
"""
from datetime import date, datetime
from typing import Optional, Dict, Any, List
import uuid
import sys
from pathlib import Path
import numpy as np
import json

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode

from app.api.models.backtest import BacktestResult, BacktestStatus
from app.api.worker.config import get_queue
from app.api.worker.tasks import (
    run_backtest_task,
    run_bulk_backtest_task,
    run_optimization_task,
)
from app.api.services.job_storage import get_job_storage

from app.domains.backtests.dao.akshare_benchmark_dao import AkshareBenchmarkDao
from app.domains.backtests.dao.backtest_history_dao import BacktestHistoryDao
from app.domains.backtests.dao.bulk_backtest_dao import BulkBacktestDao
from app.domains.backtests.dao.strategy_source_dao import StrategySourceDao
from app.domains.market.service import MarketService


def calculate_alpha_beta(strategy_returns: np.ndarray, benchmark_returns: np.ndarray) -> tuple:
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


def get_benchmark_data(start_date: date, end_date: date, benchmark_symbol: str = "399300.SZ") -> Optional[Dict]:
    try:
        return AkshareBenchmarkDao().get_benchmark_data(start=start_date, end=end_date, benchmark_symbol=benchmark_symbol)
    except Exception as e:
        print(f"Error fetching benchmark data: {e}")
        return None


def get_stock_name(ts_code: str) -> Optional[str]:
    try:
        return MarketService().resolve_symbol_name(ts_code) or None
    except Exception as e:
        print(f"Error fetching stock name: {e}")
        return None


class BacktestServiceV2:
    """Service for managing backtests with RQ workers."""

    def __init__(self):
        self.job_storage = get_job_storage()
        self.builtin_strategies = {
            "TripleMAStrategy": "app.strategies.triple_ma_strategy",
            "TurtleTradingStrategy": "app.strategies.turtle_trading",
        }

    def submit_backtest(self, user_id: int, strategy_id: Optional[int], strategy_class_name: Optional[str],
                        symbol: str, start_date: date, end_date: date, initial_capital: float = 100000.0,
                        rate: float = 0.0001, slippage: float = 0.0, size: int = 1, pricetick: float = 0.01,
                        parameters: Optional[Dict[str, Any]] = None, symbol_name: str = "", strategy_name: str = "",
                        benchmark: str = "399300.SZ") -> str:
        job_id = f"bt_{uuid.uuid4().hex[:16]}"
        strategy_code = None
        strategy_version = None
        if strategy_id:
            strategy_code, strategy_class_name, strategy_version = self._get_strategy_from_db(strategy_id, user_id)

        if not symbol_name:
            symbol_name = MarketService().resolve_symbol_name(symbol) or ""

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

    def submit_batch_backtest(self, user_id: int, strategy_id: Optional[int], strategy_class_name: Optional[str],
                              symbols: List[str], start_date: date, end_date: date, initial_capital: float = 100000.0,
                              rate: float = 0.0001, slippage: float = 0.0, size: int = 1, pricetick: float = 0.01,
                              parameters: Optional[Dict[str, Any]] = None, strategy_name: str = "",
                              benchmark: str = "399300.SZ") -> str:
        job_id = f"bulk_{uuid.uuid4().hex[:16]}"
        strategy_code = None
        strategy_version = None
        if strategy_id:
            strategy_code, strategy_class_name, strategy_version = self._get_strategy_from_db(strategy_id, user_id)

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

        try:
            BulkBacktestDao().insert_parent(
                user_id=user_id,
                job_id=job_id,
                strategy_id=strategy_id,
                strategy_class=strategy_class_name,
                strategy_version=strategy_version,
                symbols_json=json.dumps(symbols),
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                parameters_json=json.dumps(parameters) if parameters else "{}",
                initial_capital=initial_capital,
                rate=rate,
                slippage=slippage,
                benchmark=benchmark,
                total_symbols=len(symbols),
                created_at=datetime.now(),
            )
        except Exception as e:
            print(f"[Service] Error inserting bulk_backtest row: {e}")

        queue = get_queue('backtest')
        queue.enqueue(
            run_bulk_backtest_task,
            kwargs={
                "strategy_code": strategy_code,
                "strategy_class_name": strategy_class_name,
                "symbols": symbols,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "initial_capital": initial_capital,
                "rate": rate,
                "slippage": slippage,
                "size": size,
                "pricetick": pricetick,
                "parameters": parameters,
                "benchmark": benchmark,
                "bulk_job_id": job_id,
                "user_id": user_id,
                "strategy_id": strategy_id,
            },
            job_id=job_id,
            job_timeout=7200,
            result_ttl=86400 * 7,
        )

        return job_id

    def submit_optimization(self, user_id: int, strategy_id: Optional[int], strategy_class_name: Optional[str],
                            symbol: str, start_date: date, end_date: date, optimization_settings: Dict[str, Any],
                            initial_capital: float = 100000.0, rate: float = 0.0001, slippage: float = 0.0,
                            size: int = 1, pricetick: float = 0.01) -> str:
        job_id = f"opt_{uuid.uuid4().hex[:16]}"
        strategy_code = None
        if strategy_id:
            strategy_code, strategy_class_name, _ = self._get_strategy_from_db(strategy_id, user_id)

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
            job_timeout=14400,
            result_ttl=86400 * 7,
        )

        return job_id

    def get_job_status(self, job_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        metadata = self.job_storage.get_job_metadata(job_id)
        if not metadata:
            return self._get_child_job_from_db(job_id, user_id)
        if metadata.get("user_id") != user_id:
            return None
        result = None
        if metadata.get("status") in ["completed", "failed", "finished"]:
            result = self.job_storage.get_result(job_id)
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
        if job_id.startswith("bulk_"):
            try:
                row = BulkBacktestDao().get_metrics(job_id)
                if row and row.get("best_return") is not None:
                    if not response["result"]:
                        response["result"] = {}
                    response["result"]["best_return"] = float(row.get("best_return"))
                    response["result"]["best_symbol"] = row.get("best_symbol")
                    response["result"]["completed_count"] = row.get("completed_count")
            except Exception:
                pass

        for key in ["symbol", "symbol_name", "strategy_id", "strategy_class", "strategy_name", "strategy_version", "start_date", "end_date", "initial_capital", "rate", "slippage", "benchmark", "parameters", "symbols", "total_symbols"]:
            if key in metadata:
                response[key] = metadata.get(key)

        return response

    def list_user_jobs(self, user_id: int, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        return self.job_storage.list_user_jobs(user_id, status, limit)

    def cancel_job(self, job_id: str, user_id: int) -> bool:
        metadata = self.job_storage.get_job_metadata(job_id)
        if not metadata or metadata.get("user_id") != user_id:
            return False
        job_type = metadata.get("type", "")
        if "optimization" in job_type:
            queue = get_queue('optimization')
        else:
            queue = get_queue('backtest')
        return self.job_storage.cancel_job(job_id, queue)

    def _get_strategy_from_db(self, strategy_id: int, user_id: int) -> tuple:
        try:
            return StrategySourceDao().get_strategy_source_for_user(strategy_id, user_id)
        except KeyError:
            raise ValueError(f"Strategy {strategy_id} not found or access denied")

    def _get_child_job_from_db(self, job_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        import json as _json
        try:
            row = BacktestHistoryDao().get_job_row(job_id)
            if not row or row.get("user_id") != user_id:
                return None

            result_data = None
            if row.get("result"):
                try:
                    result_data = _json.loads(row["result"]) if isinstance(row["result"], str) else row["result"]
                except Exception:
                    result_data = None

            symbol_name = MarketService().resolve_symbol_name(row.get("vt_symbol") or "")

            strategy_name = row.get("strategy_class") or ""
            params = {}
            if row.get("parameters"):
                try:
                    params = _json.loads(row["parameters"]) if isinstance(row["parameters"], str) else row["parameters"]
                except Exception:
                    params = {}

            return {
                "job_id": job_id,
                "status": row.get("status") or "completed",
                "type": "backtest",
                "progress": 100 if row.get("status") == "completed" else 0,
                "progress_message": "",
                "created_at": row.get("created_at").isoformat() if row.get("created_at") else None,
                "updated_at": row.get("completed_at").isoformat() if row.get("completed_at") else None,
                "result": result_data,
                "symbol": row.get("vt_symbol"),
                "symbol_name": symbol_name,
                "strategy_id": row.get("strategy_id"),
                "strategy_class": row.get("strategy_class"),
                "strategy_name": strategy_name,
                "strategy_version": row.get("strategy_version"),
                "start_date": row.get("start_date").isoformat() if row.get("start_date") else None,
                "end_date": row.get("end_date").isoformat() if row.get("end_date") else None,
                "parameters": params,
                "bulk_job_id": row.get("bulk_job_id"),
            }
        except Exception as e:
            print(f"[Service] Error loading child job {job_id} from DB: {e}")
            return None


class BacktestService(BacktestServiceV2):
    """Service for running synchronous backtests and exposing job APIs."""

    def __init__(self):
        super().__init__()
        self.builtin_strategies = self._load_builtin_strategies()

    def _load_builtin_strategies(self) -> Dict[str, type]:
        strategies = {}
        try:
            from app.strategies.triple_ma_strategy import TripleMAStrategy
            strategies["TripleMAStrategy"] = TripleMAStrategy
        except Exception:
            pass
        try:
            from app.strategies.turtle_trading import TurtleTradingStrategy
            strategies["TurtleTradingStrategy"] = TurtleTradingStrategy
        except Exception:
            pass
        return strategies

    def _get_strategy_class(self, strategy_id: Optional[int] = None, strategy_class: Optional[str] = None, user_id: Optional[int] = None):
        if strategy_class and strategy_class in self.builtin_strategies:
            return self.builtin_strategies[strategy_class]
        if strategy_id:
            from app.api.services.strategy_service import compile_strategy
            # Prefer user-scoped load if possible
            if user_id is not None:
                code, class_name, _ = StrategySourceDao().get_strategy_source_for_user(strategy_id, user_id)
            else:
                # fallback by class_name not supported here; require user_id
                raise ValueError("user_id is required to load strategy code")
            return compile_strategy(code, class_name)
        raise ValueError(f"Strategy not found: id={strategy_id}, class={strategy_class}")

    def run_single_backtest(self, strategy_id: Optional[int], strategy_class: Optional[str], vt_symbol: str,
                            start_date: date, end_date: date, parameters: Dict[str, Any], capital: float = 100000.0,
                            rate: float = 0.0001, slippage: float = 0.0, size: int = 1, benchmark: Optional[str] = None) -> Optional[BacktestResult]:
        strategy_cls = self._get_strategy_class(strategy_id, strategy_class)
        engine = BacktestingEngine()
        engine.set_parameters(
            vt_symbol=vt_symbol,
            interval=Interval.DAILY,
            start=datetime.combine(start_date, datetime.min.time()),
            end=datetime.combine(end_date, datetime.min.time()),
            rate=rate,
            slippage=slippage,
            size=size,
            pricetick=0.01,
            capital=capital,
            mode=BacktestingMode.BAR,
        )
        setting = strategy_cls.get_class_parameters()
        setting.update(parameters)
        engine.add_strategy(strategy_cls, setting)
        engine.load_data()
        if not engine.history_data:
            return None
        engine.run_backtesting()
        try:
            df = engine.calculate_result()
        except Exception:
            df = None
        stats = engine.calculate_statistics(output=False)
        if not stats:
            return None
        result = BacktestResult(
            symbol=vt_symbol,
            start_date=start_date,
            end_date=end_date,
            total_days=int(stats.get("total_days", 0)),
            profit_days=int(stats.get("profit_days", 0)),
            loss_days=int(stats.get("loss_days", 0)),
            capital=capital,
            end_balance=float(stats.get("end_balance", 0)),
            total_return=float(stats.get("total_return", 0)),
            annual_return=float(stats.get("annual_return", 0)),
            max_drawdown=float(stats.get("max_drawdown", 0)),
            max_drawdown_percent=float(stats.get("max_ddpercent", 0)),
            sharpe_ratio=float(stats.get("sharpe_ratio", 0)),
            total_trades=int(stats.get("total_trade_count", 0)),
            parameters=setting,
        )
        stock_name = get_stock_name(vt_symbol)
        if stock_name:
            result.symbol_name = stock_name
        strategy_daily_returns = None
        if df is not None and not df.empty:
            result.daily_returns = df[["net_pnl"]].reset_index().to_dict(orient="records") if "net_pnl" in df.columns else None
            result.equity_curve = df[["balance"]].reset_index().to_dict(orient="records") if "balance" in df.columns else None
            if "balance" in df.columns:
                balance_series = df["balance"].values
                strategy_daily_returns = np.diff(balance_series) / balance_series[:-1]
        if engine.history_data:
            stock_prices = []
            for bar in engine.history_data:
                stock_prices.append({
                    "datetime": bar.datetime.isoformat() if bar.datetime else None,
                    "close": bar.close_price
                })
            result.stock_price_curve = stock_prices
        bm_symbol = benchmark or "000300.SH"
        benchmark_data = get_benchmark_data(start_date, end_date, bm_symbol)
        if benchmark_data and strategy_daily_returns is not None:
            alpha, beta = calculate_alpha_beta(strategy_daily_returns, benchmark_data["returns"])
            result.alpha = alpha
            result.beta = beta
            result.benchmark_return = benchmark_data["total_return"]
            result.benchmark_symbol = bm_symbol
            if "prices" in benchmark_data:
                result.benchmark_curve = benchmark_data["prices"]
        if engine.trades:
            trades = []
            for t in list(engine.trades.values())[:100]:
                trades.append({
                    "datetime": t.datetime.isoformat() if t.datetime else None,
                    "symbol": t.symbol,
                    "direction": str(t.direction.value) if hasattr(t.direction, 'value') else str(t.direction),
                    "offset": str(t.offset.value) if hasattr(t.offset, 'value') else str(t.offset),
                    "price": t.price,
                    "volume": t.volume
                })
            result.trades = trades
        return result


# Singleton accessor for unified service
_backtest_service = None


def get_backtest_service() -> BacktestService:
    global _backtest_service
    if _backtest_service is None:
        _backtest_service = BacktestService()
    return _backtest_service


def get_backtest_service_v2() -> BacktestService:
    return get_backtest_service()

