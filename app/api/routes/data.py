"""Data and history routes."""
from datetime import date, datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.api.middleware.auth import get_current_user, get_current_user_optional
from app.api.models.user import TokenData
from app.api.services.data_service import DataService

router = APIRouter(prefix="/data", tags=["Data"])


class SymbolInfo(BaseModel):
    """Symbol information."""
    symbol: str
    name: str
    exchange: str
    vt_symbol: str
    industry: Optional[str] = None
    list_date: Optional[date] = None


class OHLCBar(BaseModel):
    """OHLC bar data."""
    datetime: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: Optional[float] = None


class IndicatorData(BaseModel):
    """Indicator data."""
    datetime: datetime
    value: float
    name: str


@router.get("/symbols", response_model=List[SymbolInfo])
async def list_symbols(
    exchange: Optional[str] = Query(None, description="Filter by exchange (SZSE, SSE)"),
    keyword: Optional[str] = Query(None, description="Search by symbol or name"),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    current_user: Optional[TokenData] = Depends(get_current_user_optional)
):
    """List available symbols."""
    service = DataService()
    return service.get_symbols(exchange=exchange, keyword=keyword, limit=limit, offset=offset)


@router.get("/history/{vt_symbol}", response_model=List[OHLCBar])
async def get_history(
    vt_symbol: str,
    start_date: date = Query(..., description="Start date"),
    end_date: date = Query(..., description="End date"),
    interval: str = Query("daily", description="Interval: daily, weekly, monthly"),
    current_user: Optional[TokenData] = Depends(get_current_user_optional)
):
    """Get historical OHLC data for a symbol."""
    service = DataService()
    
    try:
        bars = service.get_history(vt_symbol, start_date, end_date, interval)
        return bars
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch history: {str(e)}")


@router.get("/indicators/{vt_symbol}")
async def get_indicators(
    vt_symbol: str,
    start_date: date = Query(...),
    end_date: date = Query(...),
    indicators: str = Query("ma_10,ma_20,ma_60", description="Comma-separated indicator names"),
    current_user: Optional[TokenData] = Depends(get_current_user_optional)
):
    """Get computed indicators for a symbol."""
    service = DataService()
    indicator_list = [i.strip() for i in indicators.split(",")]
    
    try:
        data = service.get_indicators(vt_symbol, start_date, end_date, indicator_list)
        return data
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/overview")
async def get_market_overview(
    current_user: Optional[TokenData] = Depends(get_current_user_optional)
):
    """Get market overview statistics."""
    service = DataService()
    return service.get_market_overview()


@router.get("/sectors")
async def get_sectors(
    current_user: Optional[TokenData] = Depends(get_current_user_optional)
):
    """Get sector information."""
    service = DataService()
    return service.get_sectors()


@router.get("/exchanges")
async def get_exchanges(
    current_user: Optional[TokenData] = Depends(get_current_user_optional)
):
    """Get exchange-level stock groupings (SSE, SZSE, BSE)."""
    service = DataService()
    return service.get_exchanges()


@router.get("/symbols-by-filter")
async def get_symbols_by_filter(
    industry: Optional[str] = Query(None, description="Filter by industry name"),
    exchange: Optional[str] = Query(None, description="Filter by exchange: SSE, SZSE, BSE"),
    limit: int = Query(500, le=2000),
    current_user: Optional[TokenData] = Depends(get_current_user_optional)
):
    """
    Get symbol list filtered by industry and/or exchange.
    Returns ts_code, name, industry, exchange for use in bulk backtest symbol picker.
    """
    service = DataService()
    return service.get_symbols_by_filter(industry=industry, exchange=exchange, limit=limit)
