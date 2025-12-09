#!/usr/bin/env python3
"""
Migration Script: Add VWAP Slope Fields to Historical Market Data
Date: 2025-01-XX
Description: Adds VWAP slope fields (angle, status, direction, time) to historical_market_data table

Run this script to add the new columns to the database:
    python3 backend/migrations/add_vwap_slope_to_historical.py
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from sqlalchemy import text
from backend.database import engine, SessionLocal

def run_migration():
    """Run the migration to add VWAP slope fields"""
    db = SessionLocal()
    try:
        print("🔄 Starting migration: Add VWAP Slope Fields to Historical Market Data")
        
        # SQL statements to add VWAP slope columns
        migration_statements = [
            # Add vwap_slope_angle column
            """
            ALTER TABLE historical_market_data 
            ADD COLUMN IF NOT EXISTS vwap_slope_angle FLOAT;
            """,
            
            # Add vwap_slope_status column
            """
            ALTER TABLE historical_market_data 
            ADD COLUMN IF NOT EXISTS vwap_slope_status VARCHAR(20);
            """,
            
            # Add vwap_slope_direction column
            """
            ALTER TABLE historical_market_data 
            ADD COLUMN IF NOT EXISTS vwap_slope_direction VARCHAR(20);
            """,
            
            # Add vwap_slope_time column
            """
            ALTER TABLE historical_market_data 
            ADD COLUMN IF NOT EXISTS vwap_slope_time TIMESTAMP;
            """
        ]
        
        for statement in migration_statements:
            try:
                db.execute(text(statement.strip()))
                print(f"✅ Executed: {statement.strip()[:60]}...")
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

