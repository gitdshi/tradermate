"""Connection helpers.

Design goals:
- Make it explicit which DB a DAO targets (tradermate vs tushare vs akshare)
- Provide a small context manager to ensure connections are closed

Note: This is a thin wrapper around the existing engine factory functions in
`app.api.services.db` to keep the refactor incremental.
"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Literal

from typing import Any as Connection

from app.api.services import db as legacy_db

DatabaseName = Literal["tradermate", "tushare", "akshare"]


def _get_connection(db: DatabaseName) -> Connection:
    if db == "tradermate":
        return legacy_db.get_db_connection()
    if db == "tushare":
        return legacy_db.get_tushare_connection()
    if db == "akshare":
        return legacy_db.get_akshare_connection()
    # Defensive: keep mypy happy
    raise ValueError(f"Unknown db: {db}")


@contextmanager
def connection(db: DatabaseName) -> Iterator[Connection]:
    """Yield a SQLAlchemy connection and always close it."""
    conn: Connection | None = None
    try:
        conn = _get_connection(db)
        yield conn
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
