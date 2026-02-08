"""Run bulk backtest migration."""
from app.api.services.db import get_db_connection
from sqlalchemy import text

conn = get_db_connection()

# 1) Add bulk_job_id column
try:
    conn.execute(text("ALTER TABLE backtest_history ADD COLUMN bulk_job_id VARCHAR(36) AFTER job_id"))
    conn.commit()
    print("OK Added bulk_job_id column")
except Exception as e:
    if "Duplicate column" in str(e):
        print("SKIP bulk_job_id already exists")
    else:
        print("ERR", e)

# 2) Add index
try:
    conn.execute(text("ALTER TABLE backtest_history ADD INDEX idx_bulk_job_id (bulk_job_id)"))
    conn.commit()
    print("OK Added idx_bulk_job_id index")
except Exception as e:
    if "Duplicate" in str(e):
        print("SKIP idx_bulk_job_id already exists")
    else:
        print("ERR index", e)

# 3) Create bulk_backtest table
try:
    conn.execute(text(
        "CREATE TABLE IF NOT EXISTS bulk_backtest ("
        "id INT AUTO_INCREMENT PRIMARY KEY, "
        "user_id INT NOT NULL, "
        "job_id VARCHAR(36) NOT NULL UNIQUE, "
        "strategy_id INT, "
        "strategy_class VARCHAR(100), "
        "strategy_version INT, "
        "symbols JSON NOT NULL, "
        "start_date DATE, "
        "end_date DATE, "
        "parameters JSON, "
        "initial_capital DOUBLE DEFAULT 100000, "
        "rate DOUBLE DEFAULT 0.0001, "
        "slippage DOUBLE DEFAULT 0, "
        "benchmark VARCHAR(50) DEFAULT '399300.SZ', "
        "status VARCHAR(20) NOT NULL DEFAULT 'queued', "
        "total_symbols INT DEFAULT 0, "
        "completed_count INT DEFAULT 0, "
        "best_return DOUBLE, "
        "best_symbol VARCHAR(50), "
        "created_at DATETIME NOT NULL, "
        "completed_at DATETIME, "
        "FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE, "
        "FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE SET NULL, "
        "INDEX idx_user_id (user_id), "
        "INDEX idx_job_id (job_id), "
        "INDEX idx_status (status), "
        "INDEX idx_created_at (created_at)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    ))
    conn.commit()
    print("OK Created bulk_backtest table")
except Exception as e:
    print("ERR table", e)

conn.close()
print("DONE")
