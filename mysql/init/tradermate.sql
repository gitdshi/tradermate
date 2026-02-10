-- =============================================================================
-- TraderMate Backend API Database
-- Database: tradermate - stores user accounts, strategies, backtest results
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tradermate CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE tradermate;

-- -----------------------------------------------------------------------------
-- Users table - stores user accounts and authentication
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(100) UNIQUE,
    hashed_password VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_username (username),
    INDEX idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='User accounts';

-- -----------------------------------------------------------------------------
-- Strategies table - stores user-created trading strategies
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    class_name VARCHAR(100) NOT NULL,
    description TEXT,
    parameters JSON,
    code LONGTEXT NOT NULL,
    version INT NOT NULL DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_strategy (user_id, name),
    INDEX idx_user_id (user_id),
    INDEX idx_class_name (class_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='User trading strategies';

-- -----------------------------------------------------------------------------
-- Backtest history table - stores backtest execution history and results
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS backtest_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL UNIQUE,
    bulk_job_id VARCHAR(36),
    strategy_id INT,
    strategy_class VARCHAR(100),
    strategy_version INT,
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
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE SET NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_job_id (job_id),
    INDEX idx_bulk_job_id (bulk_job_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Backtest execution history';

-- -----------------------------------------------------------------------------
-- Bulk backtest table - tracks multi-symbol bulk backtest jobs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bulk_backtest (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL UNIQUE,
    strategy_id INT,
    strategy_class VARCHAR(100),
    strategy_version INT,
    symbols JSON NOT NULL,
    start_date DATE,
    end_date DATE,
    parameters JSON,
    initial_capital DOUBLE DEFAULT 100000,
    rate DOUBLE DEFAULT 0.0001,
    slippage DOUBLE DEFAULT 0,
    benchmark VARCHAR(50) DEFAULT '399300.SZ',
    status VARCHAR(20) NOT NULL DEFAULT 'queued',
    total_symbols INT DEFAULT 0,
    completed_count INT DEFAULT 0,
    best_return DOUBLE,
    best_symbol VARCHAR(50),
    created_at DATETIME NOT NULL,
    completed_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (strategy_id) REFERENCES strategies(id) ON DELETE SET NULL,
    INDEX idx_user_id (user_id),
    INDEX idx_job_id (job_id),
    INDEX idx_status (status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Bulk backtest jobs tracking';

-- -----------------------------------------------------------------------------
-- Watchlists table - stores user stock watchlists
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS watchlists (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,
    symbols JSON,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE KEY unique_user_watchlist (user_id, name),
    INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='User stock watchlists';

-- -----------------------------------------------------------------------------
-- Optimization results table - stores parameter optimization results
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS optimization_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    job_id VARCHAR(36) NOT NULL UNIQUE,
    strategy_class VARCHAR(100),
    vt_symbol VARCHAR(50),
    start_date DATE,
    end_date DATE,
    parameter_grid JSON,
    status VARCHAR(20) NOT NULL,
    best_params JSON,
    best_result JSON,
    all_results JSON,
    error TEXT,
    created_at DATETIME NOT NULL,
    completed_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    INDEX idx_user_id (user_id),
    INDEX idx_job_id (job_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Strategy optimization results';

-- Strategy history - stores historical snapshots of strategy code
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='History snapshots for DB strategy code';

-- -----------------------------------------------------------------------------
-- Insert default test user (password: admin123)
-- Password hash generated with bcrypt for 'admin123'
-- -----------------------------------------------------------------------------
INSERT INTO users (username, email, hashed_password, is_active, created_at) 
VALUES (
    'admin', 
    'admin@tradermate.local', 
    '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY5GyYqVvmvhxKe',
    TRUE,
    NOW()
) ON DUPLICATE KEY UPDATE username=username;

-- -----------------------------------------------------------------------------
-- Create indexes for query optimization
-- -----------------------------------------------------------------------------
CREATE INDEX idx_backtest_user_date ON backtest_history(user_id, created_at DESC);
CREATE INDEX idx_strategies_user_active ON strategies(user_id, is_active);
