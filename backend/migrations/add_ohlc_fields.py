#!/usr/bin/env python3
"""
Migration Script: Add OHLC and Previous Hour VWAP Fields
Date: 2025-11-25
Description: Adds fields for option OHLC candles and previous hour stock VWAP

Run this script to add the new columns to the database:
    python3 backend/migrations/add_ohlc_fields.py
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from backend.database import engine, SessionLocal

def run_migration():
    """Run the migration to add OHLC fields"""
    db = SessionLocal()
    try:
        print("🔄 Starting migration: Add OHLC and Previous Hour VWAP Fields")
        
        # Read SQL migration file
        migration_file = os.path.join(os.path.dirname(__file__), 'add_ohlc_fields.sql')
        with open(migration_file, 'r') as f:
            migration_sql = f.read()
        
        # Execute migration
        # Split by semicolon and execute each statement
        statements = [s.strip() for s in migration_sql.split(';') if s.strip() and not s.strip().startswith('--')]
        
        for statement in statements:
            if statement:
                try:
                    db.execute(text(statement))
                    print(f"✅ Executed: {statement[:50]}...")
                except Exception as e:
                    # Check if error is "column already exists" - that's OK
                    if 'already exists' in str(e).lower() or 'duplicate column' in str(e).lower():
                        print(f"⚠️  Column already exists (skipping): {statement[:50]}...")
                    else:
                        print(f"❌ Error executing statement: {e}")
                        print(f"   Statement: {statement[:100]}...")
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

