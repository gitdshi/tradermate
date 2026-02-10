#!/usr/bin/env python3
"""Drop obsolete strategy_files and strategy_file_history tables.

After removing file-based strategy functionality, these tables are no longer needed.
Strategy code is now stored exclusively in strategies.code and strategy_history.
"""
import sys
from pathlib import Path

# Add project root to path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.api.services.db import get_db_connection
from sqlalchemy import text


def main():
    print("=" * 80)
    print("Migration: Drop obsolete strategy_files tables")
    print("=" * 80)
    
    conn = get_db_connection()
    
    try:
        # Check if tables exist
        result = conn.execute(text("""
            SELECT TABLE_NAME 
            FROM information_schema.TABLES 
            WHERE TABLE_SCHEMA = 'tradermate' 
            AND TABLE_NAME IN ('strategy_files', 'strategy_file_history')
        """))
        existing_tables = [row.TABLE_NAME for row in result.fetchall()]
        
        if not existing_tables:
            print("✓ Tables already dropped - nothing to do")
            return
        
        print(f"\nFound obsolete tables: {', '.join(existing_tables)}")
        
        # Drop tables (child first)
        if 'strategy_file_history' in existing_tables:
            print("\nDropping strategy_file_history...")
            conn.execute(text("DROP TABLE IF EXISTS strategy_file_history"))
            print("✓ Dropped strategy_file_history")
        
        if 'strategy_files' in existing_tables:
            print("\nDropping strategy_files...")
            conn.execute(text("DROP TABLE IF EXISTS strategy_files"))
            print("✓ Dropped strategy_files")
        
        conn.commit()
        
        print("\n" + "=" * 80)
        print("Migration complete!")
        print("=" * 80)
        print("\nStrategy data is now stored exclusively in:")
        print("  - strategies.code (current version)")
        print("  - strategy_history (historical snapshots)")
        
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
