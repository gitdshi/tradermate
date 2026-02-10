"""Backtest service for running backtests."""
from datetime import date, datetime
from typing import Optional, Dict, Any
import sys
from pathlib import Path
import numpy as np

# Ensure project root is importable
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode

from app.api.models.backtest import BacktestResult
from app.api.services.db import get_db_connection, get_tushare_connection
from sqlalchemy import text
import json


def calculate_alpha_beta(strategy_returns: np.ndarray, benchmark_returns: np.ndarray) -> tuple:
    """
    Calculate alpha and beta using linear regression.
    
    Args:
        strategy_returns: Array of strategy daily returns
        benchmark_returns: Array of benchmark daily returns (same length)
    
    Returns:
        Tuple of (alpha, beta)
    """
    if len(strategy_returns) < 2 or len(benchmark_returns) < 2:
        return None, None
    
    # Ensure same length
    min_len = min(len(strategy_returns), len(benchmark_returns))
    strategy_returns = strategy_returns[:min_len]
    benchmark_returns = benchmark_returns[:min_len]
    
    # Remove NaN values
    mask = ~(np.isnan(strategy_returns) | np.isnan(benchmark_returns))
    strategy_returns = strategy_returns[mask]
    benchmark_returns = benchmark_returns[mask]
    
    if len(strategy_returns) < 2:
        return None, None
    
    try:
        # Linear regression: strategy_return = alpha + beta * benchmark_return
        # Using numpy polyfit for simplicity
        beta, alpha = np.polyfit(benchmark_returns, strategy_returns, 1)
        
        # Annualize alpha (assuming daily returns, 252 trading days)
        alpha_annualized = alpha * 252
        
        return float(alpha_annualized), float(beta)
    except Exception:
        return None, None


def get_benchmark_data(start_date: date, end_date: date, benchmark_symbol: str = "399300.SZ") -> Optional[Dict]:
    """
    Fetch HS300 benchmark data for the given period.
    
    Args:
        start_date: Start date
        end_date: End date
        benchmark_symbol: Benchmark index symbol (default: HS300 - 399300.SZ)
    
    Returns:
        Dict with 'returns' (daily returns array) and 'total_return' (cumulative return)
    """
    # Benchmark (index) data now comes from the AkShare DB directly
    from app.api.services.db import get_akshare_connection
    conn = get_akshare_connection()
    
    try:
        # Try a few index_code variants (caller may pass Tushare-style codes)
        candidates = [benchmark_symbol]
        # Heuristic: map Tushare '000300.SH' -> AkShare '399300.SZ'
        if benchmark_symbol and benchmark_symbol.endswith('.SH') and benchmark_symbol.startswith('000'):
            candidates.append(benchmark_symbol.replace('000', '399').replace('.SH', '.SZ'))
        # Also try switching .SH/.SZ suffixes if present
        if benchmark_symbol and (benchmark_symbol.endswith('.SH') or benchmark_symbol.endswith('.SZ')):
            alt = benchmark_symbol[:-3] + ('.SZ' if benchmark_symbol.endswith('.SH') else '.SH')
            candidates.append(alt)

        rows = []
        for idx_code in dict.fromkeys(candidates):
            query = """
                SELECT trade_date, close
                FROM index_daily
                WHERE index_code = :index_code
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                ORDER BY trade_date ASC
            """
            result = conn.execute(text(query), {
                "index_code": idx_code,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            })
            rows = result.fetchall()
            if rows and len(rows) >= 2:
                break

        if not rows or len(rows) < 2:
            return None

        # Extract dates and closes (trade_date may be a date object)
        dates = [row.trade_date for row in rows]
        closes = np.array([float(row.close) for row in rows], dtype=float)
        
        # Calculate daily returns
        daily_returns = np.diff(closes) / closes[:-1]
        
        # Calculate total return
        total_return = (closes[-1] - closes[0]) / closes[0] * 100
        
        # Format prices for chart; handle DATE objects and strings
        prices = []
        for dt_val, close_val in zip(dates, closes):
            if isinstance(dt_val, str):
                # Try common formats
                try:
                    dt_obj = datetime.strptime(dt_val, "%Y%m%d")
                except Exception:
                    try:
                        dt_obj = datetime.fromisoformat(dt_val)
                    except Exception:
                        dt_obj = None
            else:
                # likely a datetime.date object
                try:
                    dt_obj = datetime.combine(dt_val, datetime.min.time())
                except Exception:
                    dt_obj = None

            prices.append({
                "datetime": dt_obj.isoformat() if dt_obj else None,
                "close": float(close_val)
            })
        
        return {
            "returns": daily_returns,
            "total_return": float(total_return),
            "closes": closes.tolist(),
            "prices": prices
        }
        
    except Exception as e:
        print(f"Error fetching benchmark data: {e}")
        return None
    finally:
        conn.close()


def get_stock_name(ts_code: str) -> Optional[str]:
    """
    Get stock name from stock_basic table.
    
    Args:
        ts_code: Stock code (e.g., '000001.SZ')
    
    Returns:
        Stock name or None if not found
    """
    conn = get_tushare_connection()
    
    try:
        query = """
            SELECT name
            FROM stock_basic
            WHERE ts_code = :ts_code
            LIMIT 1
        """
        
        result = conn.execute(text(query), {"ts_code": ts_code})
        row = result.fetchone()
        
        return row.name if row else None
        
    except Exception as e:
        print(f"Error fetching stock name: {e}")
        return None
    finally:
        conn.close()


class BacktestService:
    """Service for running backtests."""
    
    def __init__(self):
        self.builtin_strategies = self._load_builtin_strategies()
    
    def _load_builtin_strategies(self) -> Dict[str, type]:
        """Load built-in strategy classes."""
        strategies = {}
        
        try:
            from app.strategies.triple_ma_strategy import TripleMAStrategy
            strategies["TripleMAStrategy"] = TripleMAStrategy
        except ImportError:
            pass
        
        try:
            from app.strategies.turtle_trading import TurtleTradingStrategy
            strategies["TurtleTradingStrategy"] = TurtleTradingStrategy
        except ImportError:
            pass
        
        return strategies
    
    def _get_strategy_class(
        self,
        strategy_id: Optional[int] = None,
        strategy_class: Optional[str] = None,
        user_id: Optional[int] = None
    ):
        """Get strategy class by ID or class name."""
        # If class name provided, use built-in
        if strategy_class and strategy_class in self.builtin_strategies:
            return self.builtin_strategies[strategy_class]
        
        # If strategy_id provided, load from database
        if strategy_id:
            conn = get_db_connection()
            try:
                result = conn.execute(
                    text("SELECT code, class_name FROM strategies WHERE id = :id"),
                    {"id": strategy_id}
                )
                row = result.fetchone()
                
                if row:
                    # Compile and return the strategy class
                    namespace = {}
                    
                    # Add required imports to namespace
                    from vnpy_ctastrategy import CtaTemplate
                    namespace["CtaTemplate"] = CtaTemplate
                    
                    exec(row.code, namespace)
                    
                    if row.class_name in namespace:
                        return namespace[row.class_name]
            finally:
                conn.close()
        
        raise ValueError(f"Strategy not found: id={strategy_id}, class={strategy_class}")
    
    def run_single_backtest(
        self,
        strategy_id: Optional[int],
        strategy_class: Optional[str],
        vt_symbol: str,
        start_date: date,
        end_date: date,
        parameters: Dict[str, Any],
        capital: float = 100000.0,
        rate: float = 0.0001,
        slippage: float = 0.0,
        size: int = 1
    ) -> Optional[BacktestResult]:
        """Run a single backtest."""
        # Get strategy class
        strategy_cls = self._get_strategy_class(strategy_id, strategy_class)
        
        # Create engine
        engine = BacktestingEngine()
        
        # Set parameters
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
        
        # Get default parameters and merge with provided
        setting = strategy_cls.get_class_parameters()
        setting.update(parameters)
        
        engine.add_strategy(strategy_cls, setting)
        
        # Load data
        engine.load_data()
        
        if not engine.history_data:
            return None
        
        # Run backtest
        engine.run_backtesting()
        
        # Calculate results
        try:
            df = engine.calculate_result()
        except Exception:
            df = None
        
        stats = engine.calculate_statistics(output=False)
        
        if not stats:
            return None
        
        # Build result
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
        
        # Get stock name
        stock_name = get_stock_name(vt_symbol)
        if stock_name:
            result.symbol_name = stock_name
        
        # Add daily returns and equity curve if available
        strategy_daily_returns = None
        if df is not None and not df.empty:
            result.daily_returns = df[["net_pnl"]].reset_index().to_dict(orient="records") if "net_pnl" in df.columns else None
            result.equity_curve = df[["balance"]].reset_index().to_dict(orient="records") if "balance" in df.columns else None
            
            # Calculate daily percentage returns for alpha/beta calculation
            if "balance" in df.columns:
                balance_series = df["balance"].values
                strategy_daily_returns = np.diff(balance_series) / balance_series[:-1]
        
        # Add stock price curve from historical data
        if engine.history_data:
            stock_prices = []
            for bar in engine.history_data:
                stock_prices.append({
                    "datetime": bar.datetime.isoformat() if bar.datetime else None,
                    "close": bar.close_price
                })
            result.stock_price_curve = stock_prices
        
        # Calculate alpha and beta against HS300 benchmark
        benchmark_data = get_benchmark_data(start_date, end_date, "000300.SH")
        if benchmark_data and strategy_daily_returns is not None:
            alpha, beta = calculate_alpha_beta(strategy_daily_returns, benchmark_data["returns"])
            result.alpha = alpha
            result.beta = beta
            result.benchmark_return = benchmark_data["total_return"]
            result.benchmark_symbol = "000300.SH"
            
            # Add benchmark price curve
            if "prices" in benchmark_data:
                result.benchmark_curve = benchmark_data["prices"]
        
        # Add trade list
        if engine.trades:
            trades = []
            for t in list(engine.trades.values())[:100]:  # Limit to 100 trades
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
    
    def run_optimization(
        self,
        strategy_id: Optional[int],
        strategy_class: Optional[str],
        vt_symbol: str,
        start_date: date,
        end_date: date,
        optimization_params: Dict[str, list],
        capital: float = 100000.0
    ):
        """Run parameter optimization."""
        import itertools
        
        strategy_cls = self._get_strategy_class(strategy_id, strategy_class)
        
        # Generate all parameter combinations
        param_names = list(optimization_params.keys())
        param_values = list(optimization_params.values())
        combinations = list(itertools.product(*param_values))
        
        results = []
        
        for combo in combinations:
            params = dict(zip(param_names, combo))
            
            try:
                result = self.run_single_backtest(
                    strategy_id=strategy_id,
                    strategy_class=strategy_class,
                    vt_symbol=vt_symbol,
                    start_date=start_date,
                    end_date=end_date,
                    parameters=params,
                    capital=capital
                )
                
                if result:
                    results.append({
                        "parameters": params,
                        "total_return": result.total_return,
                        "sharpe_ratio": result.sharpe_ratio,
                        "max_drawdown": result.max_drawdown,
                        "total_trades": result.total_trades
                    })
            except Exception:
                continue
        
        # Sort by total_return
        results.sort(key=lambda x: x["total_return"], reverse=True)
        
        return results
