"""DAO layer for extdata domain: centralized DB operations for sync status and trade calendar."""
from datetime import date, timedelta
import os
import logging
from typing import Dict, List, Tuple, Any

from sqlalchemy import text
from app.infrastructure.db.connections import get_tradermate_engine, get_tushare_engine, get_vnpy_engine, get_akshare_engine

logger = logging.getLogger(__name__)

# Engines provided by infrastructure connection helpers
engine_tm = get_tradermate_engine()
engine_ts = get_tushare_engine()
engine_vn = get_vnpy_engine()
engine_ak = get_akshare_engine()


DATA_SYNC_STATUS_SQL = """
CREATE TABLE IF NOT EXISTS data_sync_status (
    id INT PRIMARY KEY AUTO_INCREMENT,
    sync_date DATE NOT NULL,
    step_name ENUM('akshare_index','tushare_stock_basic','tushare_stock_daily','tushare_adj_factor','tushare_dividend','tushare_top10_holders','vnpy_sync') NOT NULL,
    status ENUM('pending','running','success','partial','error') DEFAULT 'pending',
    rows_synced INT DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_date_step (sync_date, step_name),
    KEY idx_status_date (status, sync_date),
    KEY idx_date (sync_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


TRADE_CAL_SQL = """
CREATE TABLE IF NOT EXISTS trade_cal (
    trade_date DATE PRIMARY KEY,
    is_trade_day TINYINT NOT NULL DEFAULT 1,
    source VARCHAR(32) DEFAULT 'akshare',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_tables():
    """Ensure `data_sync_status` and `trade_cal` exist."""
    logger.info('Ensuring data_sync_status table in tradermate DB')
    with engine_tm.begin() as conn:
        conn.execute(text(DATA_SYNC_STATUS_SQL))

    logger.info('Ensuring trade_cal table in akshare DB')
    with engine_ak.begin() as conn:
        conn.execute(text(TRADE_CAL_SQL))


def get_stock_daily_counts(start: date, end: date) -> Dict[date, int]:
    """Return mapping trade_date -> count from `stock_daily` between start and end."""
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM stock_daily
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """), {'s': start, 'e': end})
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_adj_factor_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_ts.connect() as conn:
        res = conn.execute(text("""
            SELECT trade_date, COUNT(*) as cnt
            FROM adj_factor
            WHERE trade_date BETWEEN :s AND :e
            GROUP BY trade_date
        """), {'s': start, 'e': end})
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_vnpy_counts(start: date, end: date) -> Dict[date, int]:
    res_map: Dict[date, int] = {}
    with engine_vn.connect() as conn:
        res = conn.execute(text("""
            SELECT DATE(`datetime`) as dt, COUNT(*) as cnt
            FROM dbbardata
            WHERE DATE(`datetime`) BETWEEN :s AND :e
            GROUP BY dt
        """), {'s': start, 'e': end})
        for row in res.fetchall():
            res_map[row[0]] = int(row[1])
    return res_map


def get_index_daily_count_for_date(d: date) -> int:
    with engine_ak.connect() as conn:
        res = conn.execute(text("SELECT COUNT(1) FROM index_daily WHERE trade_date = :d"), {'d': d})
        row = res.fetchone()
        return int(row[0] if row and row[0] is not None else 0)


def get_stock_basic_count() -> int:
    with engine_ts.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM stock_basic"))
        row = res.fetchone()
        return int(row[0] if row and row[0] is not None else 0)


def get_adj_factor_count_for_date(d: date) -> int:
    with engine_ts.connect() as conn:
        res = conn.execute(text("SELECT COUNT(*) FROM adj_factor WHERE trade_date = :d"), {'d': d})
        row = res.fetchone()
        return int(row[0] if row and row[0] is not None else 0)


def get_stock_daily_ts_codes_for_date(d: date) -> List[str]:
    with engine_ts.connect() as conn:
        res = conn.execute(text("SELECT DISTINCT ts_code FROM stock_daily WHERE trade_date = :d ORDER BY ts_code"), {'d': d})
        return [r[0] for r in res.fetchall()]


def bulk_upsert_status(rows: List[Tuple[Any, ...]], chunk_size: int = 1000) -> int:
    """Bulk upsert rows into `data_sync_status`.

    `rows` is a list of tuples matching (sync_date, step_name, status, rows_synced, error_message, started_at, finished_at)
    Returns number of rows processed.
    """
    insert_sql = (
        "INSERT INTO data_sync_status "
        "(sync_date, step_name, status, rows_synced, error_message, started_at, finished_at) VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE status=VALUES(status), rows_synced=VALUES(rows_synced), error_message=VALUES(error_message), finished_at=VALUES(finished_at), updated_at=CURRENT_TIMESTAMP"
    )

    processed = 0
    raw_conn = engine_tm.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for i in range(0, len(rows), chunk_size):
            chunk = rows[i:i+chunk_size]
            params = [(
                r[0].strftime('%Y-%m-%d') if hasattr(r[0], 'strftime') else r[0],
                r[1], r[2], r[3], r[4], r[5], r[6]
            ) for r in chunk]
            cursor.executemany(insert_sql, params)
            raw_conn.commit()
            processed += len(chunk)
            logger.debug('bulk_upsert_status: inserted %d rows', processed)
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            raw_conn.close()
        except Exception:
            pass

    return processed


def write_step_status(sync_date: date, step_name: str, status: str,
                      rows_synced: int = 0, error_message: Any = None):
    """Insert or update a single step status row."""
    with engine_tm.begin() as conn:
        conn.execute(text("""
            INSERT INTO data_sync_status 
                (sync_date, step_name, status, rows_synced, error_message, started_at, finished_at)
            VALUES (:sd, :step, :status, :rows, :err, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                rows_synced = VALUES(rows_synced),
                error_message = VALUES(error_message),
                finished_at = VALUES(finished_at),
                updated_at = CURRENT_TIMESTAMP
        """), {
            'sd': sync_date,
            'step': step_name,
            'status': status,
            'rows': rows_synced,
            'err': error_message
        })


def get_step_status(sync_date: date, step_name: str) -> Any:
    with engine_tm.connect() as conn:
        res = conn.execute(text(
            "SELECT status FROM data_sync_status WHERE sync_date = :sd AND step_name = :step"
        ), {'sd': sync_date, 'step': step_name})
        row = res.fetchone()
        return row[0] if row else None


def get_failed_steps(lookback_days: int = 60) -> List[Tuple[date, str]]:
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=lookback_days)
    with engine_tm.connect() as conn:
        res = conn.execute(text("""
            SELECT sync_date, step_name FROM data_sync_status
            WHERE sync_date >= :start AND sync_date <= :end
              AND status IN ('error', 'partial', 'pending')
            ORDER BY sync_date ASC, step_name
        """), {'start': start, 'end': end})
        return [(row[0], row[1]) for row in res.fetchall()]


def get_cached_trade_dates(start_date: date, end_date: date) -> List[date]:
    """Return cached trade dates from akshare.trade_cal between start and end."""
    with engine_ak.connect() as conn:
        res = conn.execute(text("""
            SELECT trade_date FROM trade_cal
            WHERE trade_date BETWEEN :s AND :e AND is_trade_day = 1
            ORDER BY trade_date ASC
        """), {'s': start_date, 'e': end_date})
        return [row[0] for row in res.fetchall()]


def upsert_trade_dates(dates: List[date]):
    """Bulk insert trade dates into akshare.trade_cal (INSERT IGNORE semantics)."""
    if not dates:
        return 0
    raw = engine_ak.raw_connection()
    try:
        cur = raw.cursor()
        params = [(d.strftime('%Y-%m-%d'), 1, 'akshare') for d in dates]
        cur.executemany("INSERT IGNORE INTO trade_cal (trade_date, is_trade_day, source) VALUES (%s, %s, %s)", params)
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


def truncate_trade_cal():
    """Truncate the akshare.trade_cal table."""
    with engine_ak.begin() as conn:
        conn.execute(text("TRUNCATE TABLE trade_cal"))
