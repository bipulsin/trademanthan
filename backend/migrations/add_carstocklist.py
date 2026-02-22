#!/usr/bin/env python3
"""
Migration Script: Create carstocklist table for CAR GPT
Run: python3 backend/migrations/add_carstocklist.py
"""

import sys
import os

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from sqlalchemy import text
from backend.database import engine, SessionLocal

def run_migration():
    db = SessionLocal()
    try:
        print("🔄 Starting migration: Create carstocklist table")
        migration_file = os.path.join(os.path.dirname(__file__), 'add_carstocklist.sql')
        with open(migration_file, 'r') as f:
            migration_sql = f.read()
        statements = [s.strip() for s in migration_sql.split(';') if s.strip() and not s.strip().startswith('--')]
        for statement in statements:
            if statement:
                try:
                    db.execute(text(statement))
                    print(f"✅ Executed: {statement[:60]}...")
                except Exception as e:
                    if 'already exists' in str(e).lower():
                        print(f"⚠️  Table/index already exists (skipping)")
                    else:
                        raise
        db.commit()
        print("✅ Migration complete")
    finally:
        db.close()

if __name__ == "__main__":
    run_migration()
