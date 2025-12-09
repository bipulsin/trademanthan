#!/usr/bin/env python3
"""
Migration Script: Add no_entry_reason field to intraday_stock_options
Date: 2025-01-XX
Description: Adds no_entry_reason field to store short description of why trade was not entered

Run this script to add the new column to the database:
    python3 backend/migrations/add_no_entry_reason.py
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from sqlalchemy import text
from backend.database import engine, SessionLocal

def run_migration():
    """Run the migration to add no_entry_reason field"""
    db = SessionLocal()
    try:
        print("🔄 Starting migration: Add no_entry_reason field to intraday_stock_options")
        
        # SQL statement to add no_entry_reason column
        migration_statement = """
        ALTER TABLE intraday_stock_options 
        ADD COLUMN IF NOT EXISTS no_entry_reason VARCHAR(255);
        """
        
        try:
            db.execute(text(migration_statement.strip()))
            print(f"✅ Executed: Added no_entry_reason column")
        except Exception as e:
            # Check if error is "column already exists" - that's OK
            error_str = str(e).lower()
            if 'already exists' in error_str or 'duplicate column' in error_str:
                print(f"⚠️  Column already exists (skipping)")
            else:
                print(f"❌ Error executing statement: {e}")
                raise
        
        db.commit()
        print("✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {str(e)}")
        db.rollback()
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()
    
    return True

if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)

