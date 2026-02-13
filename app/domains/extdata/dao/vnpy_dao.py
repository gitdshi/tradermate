"""DAO helpers for vnpy DB operations used by datasync services."""
import os
from datetime import datetime, date
from typing import List, Optional, Tuple
from sqlalchemy import text
from app.infrastructure.db.connections import get_vnpy_engine

engine = get_vnpy_engine()


def get_last_sync_date(symbol: str, exchange: str, interval: str = 'd') -> Optional[date]:
    with engine.connect() as conn:
        res = conn.execute(text("SELECT last_sync_date FROM sync_status WHERE symbol=:symbol AND exchange=:exchange AND `interval`=:interval"), {'symbol': symbol, 'exchange': exchange, 'interval': interval})
        row = res.fetchone()
        return row[0] if row else None


def update_sync_status(symbol: str, exchange: str, interval: str, sync_date: date, count: int):
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO sync_status (symbol, exchange, `interval`, last_sync_date, last_sync_count)
            VALUES (:symbol, :exchange, :interval, :sync_date, :count)
            ON DUPLICATE KEY UPDATE
                last_sync_date = VALUES(last_sync_date),
                last_sync_count = VALUES(last_sync_count),
                updated_at = CURRENT_TIMESTAMP
            """
        ), {'symbol': symbol, 'exchange': exchange, 'interval': interval, 'sync_date': sync_date, 'count': count})


def bulk_upsert_dbbardata(rows: List[dict]):
    """Bulk upsert into dbbardata. Each row is a dict with keys matching the insert params."""
    if not rows:
        return 0
    insert_sql = (
        "INSERT INTO dbbardata "
        "(symbol, exchange, `datetime`, `interval`, volume, turnover, open_interest, open_price, high_price, low_price, close_price) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE volume=VALUES(volume), turnover=VALUES(turnover), open_price=VALUES(open_price), high_price=VALUES(high_price), low_price=VALUES(low_price), close_price=VALUES(close_price)"
    )
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        params = [(
            r['symbol'], r['exchange'], r['datetime'], r['interval'], r['volume'], r['turnover'], r.get('open_interest', 0.0), r['open_price'], r['high_price'], r['low_price'], r['close_price']
        ) for r in rows]
        cur.executemany(insert_sql, params)
        raw.commit()
        return cur.rowcount
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            raw.close()
        except Exception:
            pass


def get_bar_stats(symbol: str, exchange: str, interval: str = 'd') -> Tuple[int, Optional[datetime], Optional[datetime]]:
    with engine.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*), MIN(`datetime`), MAX(`datetime`) FROM dbbardata WHERE symbol=:symbol AND exchange=:exchange AND `interval`=:interval"), {'symbol': symbol, 'exchange': exchange, 'interval': interval})
        row = res.fetchone()
        return (int(row[0]) if row and row[0] is not None else 0, row[1] if row else None, row[2] if row else None)


def upsert_dbbaroverview(symbol: str, exchange: str, interval: str, count: int, start: Optional[datetime], end: Optional[datetime]):
    with engine.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO dbbaroverview (symbol, exchange, `interval`, count, start, end)
            VALUES (:symbol, :exchange, :interval, :count, :start, :end)
            ON DUPLICATE KEY UPDATE
                count = VALUES(count), start = VALUES(start), end = VALUES(end)
            """
        ), {'symbol': symbol, 'exchange': exchange, 'interval': interval, 'count': count, 'start': start, 'end': end})
