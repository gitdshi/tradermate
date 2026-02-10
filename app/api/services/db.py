"""Database connection utilities."""
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection

from app.api.config import get_settings

settings = get_settings()

# Create engines
_tradermate_engine = None
_tushare_engine = None


def get_tradermate_engine():
    """Get SQLAlchemy engine for tradermate database."""
    global _tradermate_engine
    if _tradermate_engine is None:
        _tradermate_engine = create_engine(settings.tradermate_db_url, pool_pre_ping=True)
    return _tradermate_engine


def get_vnpy_engine():
    """Backward compatibility alias for get_tradermate_engine."""
    return get_tradermate_engine()


def get_tushare_engine():
    """Get SQLAlchemy engine for tushare database."""
    global _tushare_engine
    if _tushare_engine is None:
        _tushare_engine = create_engine(settings.tushare_db_url, pool_pre_ping=True)
    return _tushare_engine


def get_db_connection() -> Connection:
    """Get a connection to the tradermate database."""
    engine = get_tradermate_engine()
    return engine.connect()


def get_tushare_connection() -> Connection:
    """Get a connection to the tushare database."""
    engine = get_tushare_engine()
    return engine.connect()


def init_db():
    """Initialize database tables if they don't exist."""
    conn = get_db_connection()
    
    try:
        # Create users table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(50) NOT NULL UNIQUE,
                email VARCHAR(100) UNIQUE,
                hashed_password VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at DATETIME NOT NULL,
                INDEX idx_username (username),
                INDEX idx_email (email)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        
        # Create strategies table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS strategies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                name VARCHAR(100) NOT NULL,
                class_name VARCHAR(100) NOT NULL,
                description TEXT,
                parameters JSON,
                code LONGTEXT NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE KEY unique_user_strategy (user_id, name),
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        
        # Create backtest_history table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS backtest_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                job_id VARCHAR(36) NOT NULL UNIQUE,
                strategy_id INT,
                strategy_class VARCHAR(100),
                vt_symbol VARCHAR(50),
                start_date DATE,
                end_date DATE,
                parameters JSON,
                status VARCHAR(20) NOT NULL,
                result JSON,
                error TEXT,
                created_at DATETIME NOT NULL,
                completed_at DATETIME,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                INDEX idx_user_id (user_id),
                INDEX idx_job_id (job_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        
        # Create watchlists table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS watchlists (
                id INT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                name VARCHAR(100) NOT NULL,
                symbols JSON,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                INDEX idx_user_id (user_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

        # Strategy history - stores historical snapshots of DB strategy code
        # If the old table `strategy_code_history` exists, migrate it by renaming
        try:
            exists_old = conn.execute(text("""
                SELECT TABLE_NAME FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'strategy_code_history'
            """)).fetchone()
            exists_new = conn.execute(text("""
                SELECT TABLE_NAME FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'strategy_history'
            """)).fetchone()

            if exists_old and not exists_new:
                # Rename old table to new name to preserve history
                conn.execute(text("RENAME TABLE strategy_code_history TO strategy_history"))
                print("Renamed strategy_code_history to strategy_history")
        except Exception:
            # ignore rename failures here; creation below will ensure the new table exists
            pass

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS strategy_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                strategy_id INT NOT NULL,
                strategy_name VARCHAR(200),
                class_name VARCHAR(200),
                description TEXT,
                version INT,
                parameters JSON,
                code LONGTEXT,
                created_at DATETIME NOT NULL,
                FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE CASCADE,
                INDEX idx_strategy_id (strategy_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        # Ensure any missing columns are added to existing table (migration-safe)
        try:
            cols = [
                ("strategy_name", "VARCHAR(200)"),
                ("class_name", "VARCHAR(200)"),
                ("description", "TEXT"),
                ("version", "INT"),
                ("parameters", "JSON"),
            ]
            for col_name, col_def in cols:
                exists = conn.execute(text("""
                    SELECT COLUMN_NAME FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'strategy_history' AND COLUMN_NAME = :col
                """), {"col": col_name}).fetchone()
                if not exists:
                    conn.execute(text(f"ALTER TABLE strategy_history ADD COLUMN {col_name} {col_def}"))
                    print(f"Added missing column {col_name} to strategy_history")
        except Exception:
            # best-effort migration; ignore errors
            pass
        
        conn.commit()
        print("Database tables initialized successfully")
        
    except Exception as e:
        print(f"Error initializing database: {e}")
        raise
    finally:
        conn.close()
