"""Data service for market data operations."""
from datetime import date, datetime
from typing import List, Optional, Dict, Any
import pandas as pd

from app.api.config import get_settings
from app.backtest.ts_utils import moving_average, pct_change

from app.domains.market.service import MarketService

settings = get_settings()


class DataService:
    """Service for fetching and processing market data."""
    def __init__(self) -> None:
        self._market = MarketService()
    
    def get_symbols(
        self,
        exchange: Optional[str] = None,
        keyword: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get list of available symbols."""
        return self._market.list_symbols(exchange=exchange, keyword=keyword, limit=limit, offset=offset)
    
    def get_history(
        self,
        vt_symbol: str,
        start_date: date,
        end_date: date,
        interval: str = "daily"
    ) -> List[Dict[str, Any]]:
        """Get historical OHLC data for a symbol."""
        return self._market.get_history(vt_symbol, start_date, end_date)
    
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
        return self._market.market_overview()
    
    def get_sectors(self) -> List[Dict[str, Any]]:
        """Get sector information."""
        return self._market.sectors()

    def get_exchanges(self) -> List[Dict[str, Any]]:
        """Get exchange-level groupings with counts."""
        return self._market.exchanges()

    def get_symbols_by_filter(
        self,
        industry: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Get symbols filtered by industry and/or exchange (exchange derived from ts_code suffix)."""
        return self._market.symbols_by_filter(industry=industry, exchange=exchange, limit=limit)

    def get_indexes(self) -> List[Dict[str, str]]:
        """Return available index codes from akshare.index_daily with friendly labels."""
        return MarketService().list_benchmark_indexes()
