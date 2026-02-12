"""Market domain service.

Domain services orchestrate DAOs and provide a stable API to other domains.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from app.domains.market.dao.akshare_index_dao import AkshareIndexDao
from app.domains.market.dao.tushare_market_dao import TushareMarketDao
from app.domains.market.dao.tushare_symbol_dao import TushareSymbolDao


class MarketService:
    def __init__(self) -> None:
        self._symbol_dao = TushareSymbolDao()
        self._index_dao = AkshareIndexDao()
        self._market_dao = TushareMarketDao()

    def resolve_symbol_name(self, input_symbol: str) -> str:
        return self._symbol_dao.get_symbol_name(input_symbol)

    def list_benchmark_indexes(self) -> list[dict[str, str]]:
        """Return index codes with friendly labels for UI dropdown."""
        codes = self._index_dao.list_index_codes()
        name_map = {
            "399300.SZ": "HS300 (沪深300)",
            "000300.SH": "HS300 (沪深300)",
            "000016.SH": "SSE50 (上证50)",
            "000905.SH": "CSI500 (中证500)",
            "399006.SZ": "ChiNext (创业板指)",
            "000001.SH": "SSE Composite (上证综指)",
            "399001.SZ": "SZSE Component (深证成指)",
            "000852.SH": "CSI1000 (中证1000)",
            "000688.SH": "STAR Market (科创板)",
            "399005.SZ": "Small/Mid Cap Index (中小板指)",
        }
        return [{"value": c, "label": name_map.get(c, c)} for c in codes]

    # ----- Data API helpers (tushare DB) -----

    def list_symbols(
        self,
        *,
        exchange: Optional[str] = None,
        keyword: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        # Map UI exchange codes to tushare exchange column.
        # Existing code historically mixed SZSE/SSE with SZ/SH; preserve behavior.
        exch = None
        if exchange:
            exch_map = {"SZSE": "SZ", "SSE": "SH", "SZ": "SZ", "SH": "SH"}
            exch = exch_map.get(exchange.upper(), exchange)

        rows = self._market_dao.list_stock_basic(exchange=exch, keyword=keyword, limit=limit, offset=offset)

        symbols: list[dict[str, Any]] = []
        for row in rows:
            ts_code = row.get("ts_code")
            symbol = ts_code.split(".")[0] if ts_code else ""
            suffix = ts_code.split(".")[1] if ts_code and "." in ts_code else "SZ"
            vt_exchange = "SZSE" if suffix == "SZ" else "SSE"

            symbols.append(
                {
                    "symbol": symbol,
                    "name": row.get("name"),
                    "exchange": vt_exchange,
                    "vt_symbol": f"{symbol}.{vt_exchange}",
                    "industry": row.get("industry"),
                    "list_date": row.get("list_date"),
                }
            )
        return symbols

    def get_history(self, vt_symbol: str, start_date: date, end_date: date) -> list[dict[str, Any]]:
        parts = vt_symbol.split(".")
        if len(parts) != 2:
            raise ValueError(f"Invalid vt_symbol format: {vt_symbol}")
        symbol, exchange = parts
        ts_suffix = "SZ" if exchange == "SZSE" else "SH"
        ts_code = f"{symbol}.{ts_suffix}"

        rows = self._market_dao.get_stock_daily_history(ts_code=ts_code, start_date=start_date, end_date=end_date)

        bars: list[dict[str, Any]] = []
        for row in rows:
            trade_date = row.get("trade_date")
            if isinstance(trade_date, str):
                try:
                    dt = datetime.strptime(trade_date, "%Y-%m-%d")
                except Exception:
                    dt = datetime.strptime(trade_date, "%Y%m%d")
            else:
                dt = datetime.combine(trade_date, datetime.min.time())

            bars.append(
                {
                    "datetime": dt,
                    "open": float(row.get("open") or 0.0),
                    "high": float(row.get("high") or 0.0),
                    "low": float(row.get("low") or 0.0),
                    "close": float(row.get("close") or 0.0),
                    "volume": float(row.get("vol") or 0.0),
                    "amount": float(row.get("amount") or 0.0),
                }
            )
        return bars

    def market_overview(self) -> dict[str, Any]:
        exchange_counts = self._market_dao.exchange_counts()
        date_range = self._market_dao.stock_daily_date_range()
        return {
            "exchanges": exchange_counts,
            "total_symbols": sum(exchange_counts.values()),
            "data_start_date": date_range.get("min_date"),
            "data_end_date": date_range.get("max_date"),
        }

    def sectors(self) -> list[dict[str, Any]]:
        return self._market_dao.sectors()

    def exchanges(self) -> list[dict[str, Any]]:
        rows = self._market_dao.exchanges()
        name_map = {"SZSE": "深圳证券交易所", "SSE": "上海证券交易所", "BSE": "北京证券交易所"}
        return [
            {"code": r.get("exchange"), "name": name_map.get(r.get("exchange"), r.get("exchange")), "count": r.get("count")}
            for r in rows
        ]

    def symbols_by_filter(
        self,
        *,
        industry: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        rows = self._market_dao.symbols_by_filter(industry=industry, exchange=exchange.upper() if exchange else None, limit=limit)
        exchange_map = {"SZ": "SZSE", "SH": "SSE", "BJ": "BSE"}
        symbols: list[dict[str, Any]] = []
        for row in rows:
            ts_code = row.get("ts_code")
            code = ts_code.split(".")[0] if ts_code else ""
            suffix = ts_code.split(".")[1] if ts_code and "." in ts_code else "SZ"
            vt_exchange = exchange_map.get(suffix, suffix)
            symbols.append(
                {
                    "ts_code": ts_code,
                    "symbol": code,
                    "name": row.get("name"),
                    "exchange": vt_exchange,
                    "vt_symbol": f"{code}.{vt_exchange}",
                    "industry": row.get("industry"),
                }
            )
        return symbols
