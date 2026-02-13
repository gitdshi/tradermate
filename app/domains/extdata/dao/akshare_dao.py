"""DAO helpers for AkShare DB operations used by datasync services."""
import os
import json
import logging
from typing import Any
from sqlalchemy import text
from app.infrastructure.db.connections import get_akshare_engine

logger = logging.getLogger(__name__)

engine = get_akshare_engine()


def audit_start(api_name: str, params: dict) -> int:
    with engine.begin() as conn:
        res = conn.execute(text(
            "INSERT INTO ingest_audit (api_name, params, status, fetched_rows) VALUES (:api, :params, 'running', 0)"
        ), {"api": api_name, "params": json.dumps(params)})
        try:
            return int(res.lastrowid)
        except Exception:
            return 0


def audit_finish(audit_id: int, status: str, rows: int):
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE ingest_audit SET status=:status, fetched_rows=:rows, finished_at=NOW() WHERE id=:id"
        ), {"status": status, "rows": rows, "id": audit_id})


def upsert_index_daily_rows(rows: list) -> int:
    """Bulk upsert rows into akshare.index_daily.

    `rows` should be a list of dicts with keys: index_code, trade_date, open, high, low, close, volume, amount
    Returns number of rows inserted/updated (as reported by cursor.rowcount)
    """
    if not rows:
        return 0
    insert_sql = (
        "INSERT INTO index_daily (index_code, trade_date, open, high, low, close, volume, amount) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low), close=VALUES(close), volume=VALUES(volume), amount=VALUES(amount)"
    )
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        params = [(
            r.get('index_code'),
            str(r.get('trade_date'))[:10],
            r.get('open'),
            r.get('high'),
            r.get('low'),
            r.get('close'),
            r.get('volume'),
            r.get('amount')
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
