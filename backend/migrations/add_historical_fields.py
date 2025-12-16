#!/usr/bin/env python3
"""
Migration Script: Add Previous Hour VWAP and Option VWAP Fields to Historical Market Data
Date: 2025-12-15
Description: Adds stock_vwap_previous_hour, stock_vwap_previous_hour_time, and option_vwap fields to historical_market_data table

Run this script to add the new columns to the database:
    python3 backend/migrations/add_historical_fields.py
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from sqlalchemy import text
from backend.database import engine, SessionLocal

def run_migration():
    """Run the migration to add previous hour VWAP and option VWAP fields"""
    db = SessionLocal()
    try:
        print("🔄 Starting migration: Add Previous Hour VWAP and Option VWAP Fields to Historical Market Data")
        
        # SQL statements to add new columns
        migration_statements = [
            # Add stock_vwap_previous_hour column
            """
            ALTER TABLE historical_market_data 
            ADD COLUMN IF NOT EXISTS stock_vwap_previous_hour FLOAT;
            """,
            
            # Add stock_vwap_previous_hour_time column
            """
            ALTER TABLE historical_market_data 
            ADD COLUMN IF NOT EXISTS stock_vwap_previous_hour_time TIMESTAMP;
            """,
            
            # Add option_vwap column
            """
            ALTER TABLE historical_market_data 
            ADD COLUMN IF NOT EXISTS option_vwap FLOAT;
            """
        ]
        
        for statement in migration_statements:
            try:
                db.execute(text(statement.strip()))
                print(f"✅ Executed: {statement.strip()[:50]}...")
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

