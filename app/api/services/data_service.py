"""Data service for market data operations."""
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import pandas as pd
from sqlalchemy import text

from app.api.config import get_settings
from app.api.services.db import get_tushare_connection, get_vnpy_engine
from app.backtest.ts_utils import moving_average, pct_change

settings = get_settings()


class DataService:
    """Service for fetching and processing market data."""
    
    def get_symbols(
        self,
        exchange: Optional[str] = None,
        keyword: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get list of available symbols."""
        conn = get_tushare_connection()
        
        try:
            query = """
                SELECT ts_code, name, exchange, industry, list_date
                FROM stock_basic
                WHERE (list_status = 'L' OR list_status IS NULL)
            """
            params = {}
            
            if exchange:
                # Map SZSE/SSE to tushare format SZ/SH
                exch_map = {"SZSE": "SZ", "SSE": "SH", "SZ": "SZ", "SH": "SH"}
                exch = exch_map.get(exchange.upper(), exchange)
                query += " AND exchange = :exchange"
                params["exchange"] = exch
            
            if keyword:
                query += " AND (ts_code LIKE :kw OR name LIKE :kw)"
                params["kw"] = f"%{keyword}%"
            
            query += " ORDER BY ts_code LIMIT :limit OFFSET :offset"
            params["limit"] = limit
            params["offset"] = offset
            
            result = conn.execute(text(query), params)
            rows = result.fetchall()
            
            symbols = []
            for row in rows:
                ts_code = row.ts_code
                symbol = ts_code.split(".")[0]
                suffix = ts_code.split(".")[1] if "." in ts_code else "SZ"
                vt_exchange = "SZSE" if suffix == "SZ" else "SSE"
                
                symbols.append({
                    "symbol": symbol,
                    "name": row.name,
                    "exchange": vt_exchange,
                    "vt_symbol": f"{symbol}.{vt_exchange}",
                    "industry": row.industry,
                    "list_date": row.list_date
                })
            
            return symbols
            
        finally:
            conn.close()
    
    def get_history(
        self,
        vt_symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "daily"
    ) -> List[Dict[str, Any]]:
        """Get historical OHLC data for a symbol."""
        # Parse vt_symbol
        parts = vt_symbol.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid vt_symbol format: {vt_symbol}")
        
        symbol, exchange = parts
        ts_suffix = "SZ" if exchange == "SZSE" else "SH"
        ts_code = f"{symbol}.{ts_suffix}"
        
        conn = get_tushare_connection()
        
        try:
            query = """
                SELECT trade_date, open, high, low, close, vol, amount
                FROM stock_daily
                WHERE ts_code = :ts_code
                  AND trade_date >= :start_date
                  AND trade_date <= :end_date
                ORDER BY trade_date ASC
            """
            
            result = conn.execute(text(query), {
                "ts_code": ts_code,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            })
            rows = result.fetchall()
            
            bars = []
            for row in rows:
                trade_date = row.trade_date
                if isinstance(trade_date, str):
                    try:
                        dt = datetime.strptime(trade_date, "%Y-%m-%d")
                    except:
                        dt = datetime.strptime(trade_date, "%Y%m%d")
                else:
                    dt = datetime.combine(trade_date, datetime.min.time())
                
                bars.append({
                    "datetime": dt,
                    "open": float(row.open) if row.open else 0.0,
                    "high": float(row.high) if row.high else 0.0,
                    "low": float(row.low) if row.low else 0.0,
                    "close": float(row.close) if row.close else 0.0,
                    "volume": float(row.vol) if row.vol else 0.0,
                    "amount": float(row.amount) if row.amount else 0.0
                })
            
            return bars
            
        finally:
            conn.close()
    
    def get_indicators(
        self,
        vt_symbol: str,
        start_date: date,
        end_date: date,
        indicators: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Compute technical indicators for a symbol."""
        # Get raw data
        bars = self.get_history(vt_symbol, start_date, end_date)
        if not bars:
            return {}
        
        # Convert to DataFrame
        df = pd.DataFrame(bars)
        df.set_index("datetime", inplace=True)
        
        result = {}
        
        for indicator in indicators:
            if indicator.startswith("ma_"):
                # Moving average
                try:
                    window = int(indicator.split("_")[1])
                    ma = moving_average(df["close"], window)
                    result[indicator] = [
                        {"datetime": idx, "value": v, "name": indicator}
                        for idx, v in ma.dropna().items()
                    ]
                except (ValueError, IndexError):
                    continue
                    
            elif indicator.startswith("ema_"):
                # Exponential moving average
                try:
                    window = int(indicator.split("_")[1])
                    ema = moving_average(df["close"], window, method="EMA")
                    result[indicator] = [
                        {"datetime": idx, "value": v, "name": indicator}
                        for idx, v in ema.dropna().items()
                    ]
                except (ValueError, IndexError):
                    continue
                    
            elif indicator == "returns":
                # Daily returns
                returns = pct_change(df["close"])
                result[indicator] = [
                    {"datetime": idx, "value": v, "name": indicator}
                    for idx, v in returns.dropna().items()
                ]
                
            elif indicator == "volume_ma_20":
                # Volume 20-day MA
                vol_ma = moving_average(df["volume"], 20)
                result[indicator] = [
                    {"datetime": idx, "value": v, "name": indicator}
                    for idx, v in vol_ma.dropna().items()
                ]
        
        return result
    
    def get_market_overview(self) -> Dict[str, Any]:
        """Get market overview statistics."""
        conn = get_tushare_connection()
        
        try:
            # Get counts by exchange
            result = conn.execute(text("""
                SELECT exchange, COUNT(*) as count
                FROM stock_basic
                WHERE list_status = 'L'
                GROUP BY exchange
            """))
            exchange_counts = {row.exchange: row.count for row in result.fetchall()}
            
            # Get data date range
            result = conn.execute(text("""
                SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date
                FROM stock_daily
            """))
            date_range = result.fetchone()
            
            return {
                "exchanges": exchange_counts,
                "total_symbols": sum(exchange_counts.values()),
                "data_start_date": date_range.min_date if date_range else None,
                "data_end_date": date_range.max_date if date_range else None
            }
            
        finally:
            conn.close()
    
    def get_sectors(self) -> List[Dict[str, Any]]:
        """Get sector information."""
        conn = get_tushare_connection()
        
        try:
            result = conn.execute(text("""
                SELECT industry, COUNT(*) as count
                FROM stock_basic
                WHERE list_status = 'L' AND industry IS NOT NULL
                GROUP BY industry
                ORDER BY count DESC
            """))
            
            return [
                {"name": row.industry, "count": row.count}
                for row in result.fetchall()
            ]
            
        finally:
            conn.close()

    def get_exchanges(self) -> List[Dict[str, Any]]:
        """Get exchange-level groupings with counts."""
        conn = get_tushare_connection()
        try:
            result = conn.execute(text("""
                SELECT exchange, COUNT(*) as count
                FROM stock_basic
                WHERE list_status = 'L'
                GROUP BY exchange
                ORDER BY count DESC
            """))
            exchange_map = {"SZ": "SZSE", "SH": "SSE", "BJ": "BSE"}
            return [
                {"code": exchange_map.get(row.exchange, row.exchange),
                 "name": row.exchange, "count": row.count}
                for row in result.fetchall()
            ]
        finally:
            conn.close()

    def get_symbols_by_filter(
        self,
        industry: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Get symbols filtered by industry and/or exchange."""
        conn = get_tushare_connection()
        try:
            query = """
                SELECT ts_code, name, exchange, industry
                FROM stock_basic
                WHERE (list_status = 'L' OR list_status IS NULL)
            """
            params: Dict[str, Any] = {}

            if industry:
                query += " AND industry = :industry"
                params["industry"] = industry

            if exchange:
                exch_map = {"SZSE": "SZ", "SSE": "SH", "BSE": "BJ",
                            "SZ": "SZ", "SH": "SH", "BJ": "BJ"}
                query += " AND exchange = :exchange"
                params["exchange"] = exch_map.get(exchange.upper(), exchange)

            query += " ORDER BY ts_code LIMIT :limit"
            params["limit"] = limit

            rows = conn.execute(text(query), params).fetchall()
            exchange_map = {"SZ": "SZSE", "SH": "SSE", "BJ": "BSE"}
            symbols = []
            for row in rows:
                ts_code = row.ts_code
                code = ts_code.split(".")[0]
                suffix = ts_code.split(".")[1] if "." in ts_code else "SZ"
                vt_exchange = exchange_map.get(suffix, suffix)
                symbols.append({
                    "ts_code": ts_code,
                    "symbol": code,
                    "name": row.name,
                    "exchange": vt_exchange,
                    "vt_symbol": f"{code}.{vt_exchange}",
                    "industry": row.industry,
                })
            return symbols
        finally:
            conn.close()
