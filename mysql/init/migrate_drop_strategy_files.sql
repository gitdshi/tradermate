-- Migration: Drop obsolete strategy file tables
-- Date: 2026-02-09
-- Reason: Removed strategy file generation and sync functionality - now DB-only
--
-- These tables are no longer used:
-- - strategy_files: tracked physical .py files (removed)
-- - strategy_file_history: stored file snapshots (removed)
--
-- Strategy code is now stored exclusively in:
-- - strategies.code (current version)
-- - strategy_code_history (historical snapshots)

USE tradermate;

-- Drop tables in correct order (child first due to foreign keys)
DROP TABLE IF EXISTS strategy_file_history;
DROP TABLE IF EXISTS strategy_files;

-- Verify tables are dropped
SELECT 'Migration complete: strategy_files tables dropped' as status;
