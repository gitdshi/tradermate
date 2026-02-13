-- AkShare Database Schema
-- Separate database for AkShare data

CREATE DATABASE IF NOT EXISTS akshare DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE akshare;

-- Audit table for tracking ingestion
CREATE TABLE IF NOT EXISTS ingest_audit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    api_name VARCHAR(64) NOT NULL,
    params JSON,
    status VARCHAR(32) DEFAULT 'running',
    fetched_rows INT DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    INDEX idx_audit_api (api_name),
    INDEX idx_audit_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Stock basic info
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code VARCHAR(32) PRIMARY KEY,
    symbol VARCHAR(16) NOT NULL,
    name VARCHAR(64),
    area VARCHAR(32),
    industry VARCHAR(64),
    market VARCHAR(16),
    list_date DATE,
    delist_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_stock_symbol (symbol),
    INDEX idx_stock_industry (industry)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Stock daily OHLCV data
CREATE TABLE IF NOT EXISTS stock_daily (
    ts_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    close DECIMAL(16,4),
    volume BIGINT,
    amount DECIMAL(20,4),
    turnover DECIMAL(12,4),
    PRIMARY KEY (ts_code, trade_date),
    INDEX idx_daily_date (trade_date),
    INDEX idx_daily_ts (ts_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Index daily data (HS300, SSE50, CSI500, etc.)
CREATE TABLE IF NOT EXISTS index_daily (
    index_code VARCHAR(32) NOT NULL,
    trade_date DATE NOT NULL,
    open DECIMAL(16,4),
    high DECIMAL(16,4),
    low DECIMAL(16,4),
    close DECIMAL(16,4),
    volume BIGINT,
    amount DECIMAL(20,4),
    PRIMARY KEY (index_code, trade_date),
    INDEX idx_index_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Trade calendar table - persisted from AkShare tool_trade_date_hist_sina
CREATE TABLE IF NOT EXISTS trade_cal (
    trade_date DATE NOT NULL PRIMARY KEY,
    is_trade_day TINYINT NOT NULL DEFAULT 1,
    source VARCHAR(32) DEFAULT 'akshare',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_trade_date (trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Trade calendar from AkShare, refreshed monthly';

-- Sync log for tracking data sync to tushare DB
CREATE TABLE IF NOT EXISTS sync_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(64) NOT NULL,
    sync_type VARCHAR(32) NOT NULL,  -- 'full' or 'incremental'
    source_rows INT DEFAULT 0,
    synced_rows INT DEFAULT 0,
    status VARCHAR(32) DEFAULT 'running',
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP NULL,
    error_message TEXT,
    INDEX idx_sync_table (table_name),
    INDEX idx_sync_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
