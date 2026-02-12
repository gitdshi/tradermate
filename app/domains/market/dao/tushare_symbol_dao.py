"""Tushare symbol DAO.

All SQL touching `tushare.stock_basic` should live here.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text

from app.infrastructure.db.connections import connection


@dataclass(frozen=True)
class SymbolNameResult:
    symbol: str
    symbol_name: str


class TushareSymbolDao:
    """DAO for `tushare.stock_basic` symbol lookups."""

    def get_symbol_name(self, input_symbol: str) -> str:
        """Resolve a human-readable symbol name.

        Accepts:
        - ts_code: '000001.SZ'
        - vt_symbol: '000001.SZSE'
        - numeric: '000001'

        Returns empty string if not found.
        """
        if not input_symbol:
            return ""

        with connection("tushare") as conn:
            # 1) Direct match against ts_code or symbol
            row = conn.execute(
                text("SELECT name FROM stock_basic WHERE ts_code = :s OR symbol = :s LIMIT 1"),
                {"s": input_symbol},
            ).fetchone()
            if row:
                return row.name if hasattr(row, "name") else list(row)[0]

            # 2) If input looks like a vt_symbol (e.g. '000001.SZSE'), convert suffix
            if "." in input_symbol:
                code, suffix = input_symbol.rsplit(".", 1)
                rev_map = {"SZSE": "SZ", "SSE": "SH", "BSE": "BJ"}
                ts_suffix = rev_map.get(suffix.upper())
                if ts_suffix:
                    alt = f"{code}.{ts_suffix}"
                    row2 = conn.execute(
                        text("SELECT name FROM stock_basic WHERE ts_code = :s OR symbol = :sym LIMIT 1"),
                        {"s": alt, "sym": code},
                    ).fetchone()
                    if row2:
                        return row2.name if hasattr(row2, "name") else list(row2)[0]

            # 3) Numeric-only fallback
            numeric = "".join(ch for ch in input_symbol if ch.isdigit())
            if numeric:
                row3 = conn.execute(
                    text("SELECT name FROM stock_basic WHERE symbol = :sym LIMIT 1"),
                    {"sym": numeric},
                ).fetchone()
                if row3:
                    return row3.name if hasattr(row3, "name") else list(row3)[0]

        return ""
