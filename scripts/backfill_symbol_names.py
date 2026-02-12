"""Backfill missing `symbol_name` in `backtest_history` and `bulk_backtest`.

Usage:
    PYTHONPATH=. .venv/bin/python3 scripts/backfill_symbol_names.py
"""
import json
from app.infrastructure.db.connections import get_tradermate_connection
from sqlalchemy import text
from app.worker.tasks import resolve_symbol_name


def backfill_children(conn):
    # Select recent children where result JSON missing symbol_name or empty
    rows = conn.execute(text(
        "SELECT id, job_id, vt_symbol, result FROM backtest_history WHERE result IS NOT NULL AND result NOT LIKE '%\"symbol_name\"%' ORDER BY id DESC"
    )).fetchall()

    updated = 0
    for r in rows:
        try:
            res_json = json.loads(r.result) if r.result else None
        except Exception:
            res_json = None
        if not res_json:
            continue

        existing = res_json.get("symbol_name")
        if existing:
            continue

        # Try to resolve name
        name = resolve_symbol_name(res_json.get("symbol") or r.vt_symbol)
        if not name:
            continue

        res_json["symbol_name"] = name
        conn.execute(text("UPDATE backtest_history SET result = :res WHERE id = :id"), {"res": json.dumps(res_json), "id": r.id})
        updated += 1

    conn.commit()
    print(f"Updated {updated} child rows")


def backfill_bulk_best_names(conn):
    rows = conn.execute(text("SELECT id, job_id, best_symbol, best_symbol_name FROM bulk_backtest WHERE best_symbol IS NOT NULL LIMIT 1000")).fetchall()
    updated = 0
    for r in rows:
        if r.best_symbol_name:
            continue
        name = resolve_symbol_name(r.best_symbol)
        if not name:
            continue
        conn.execute(text("UPDATE bulk_backtest SET best_symbol_name = :name WHERE id = :id"), {"name": name, "id": r.id})
        updated += 1
    conn.commit()
    print(f"Updated {updated} bulk_backtest rows")


def main():
    conn = get_tradermate_connection()
    try:
        backfill_children(conn)
        backfill_bulk_best_names(conn)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
