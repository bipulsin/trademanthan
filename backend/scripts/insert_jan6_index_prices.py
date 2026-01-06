#!/usr/bin/env python3
"""
One-time script to insert index prices for January 6th, 2026 at 9:15 AM
NIFTY50: 26189.70
BANKNIFTY: 59957.80
"""

import sys
import os
from pathlib import Path

# Add parent directory to path to import modules
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir.parent))

import pytz
from datetime import datetime
from backend.database import SessionLocal, create_tables
from backend.models.trading import IndexPrice

def insert_index_prices():
    """Insert index prices for Jan 6, 2026 at 9:15 AM"""
    
    # Create tables if needed
    create_tables()
    
    # Create database session
    db = SessionLocal()
    
    try:
        ist = pytz.timezone('Asia/Kolkata')
        price_time = ist.localize(datetime(2026, 1, 6, 9, 15, 0))
        
        # Check if records already exist for this time
        existing_nifty = db.query(IndexPrice).filter(
            IndexPrice.index_name == 'NIFTY50',
            IndexPrice.price_time == price_time
        ).first()
        
        existing_banknifty = db.query(IndexPrice).filter(
            IndexPrice.index_name == 'BANKNIFTY',
            IndexPrice.price_time == price_time
        ).first()
        
        if existing_nifty:
            print(f"⚠️ NIFTY50 record already exists for {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
            print(f"   Existing: LTP={existing_nifty.ltp}, ID={existing_nifty.id}")
            print("   Updating existing record...")
            existing_nifty.ltp = 26189.70
            existing_nifty.day_open = 26189.70
            existing_nifty.trend = 'neutral'
            existing_nifty.change = 0.0
            existing_nifty.change_percent = 0.0
            existing_nifty.is_special_time = True
            existing_nifty.is_market_open = True
            print("✅ Updated existing NIFTY50 record")
        else:
            # Insert NIFTY50
            nifty_price = IndexPrice(
                index_name='NIFTY50',
                instrument_key='NSE_INDEX|Nifty 50',
                ltp=26189.70,
                day_open=26189.70,  # At 9:15 AM, this is the opening price
                close_price=None,
                trend='neutral',
                change=0.0,
                change_percent=0.0,
                price_time=price_time,
                is_market_open=True,
                is_special_time=True  # 9:15 AM is a special time
            )
            db.add(nifty_price)
            print(f"✅ Added NIFTY50: ₹26189.70 at {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        if existing_banknifty:
            print(f"⚠️ BANKNIFTY record already exists for {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
            print(f"   Existing: LTP={existing_banknifty.ltp}, ID={existing_banknifty.id}")
            print("   Updating existing record...")
            existing_banknifty.ltp = 59957.80
            existing_banknifty.day_open = 59957.80
            existing_banknifty.trend = 'neutral'
            existing_banknifty.change = 0.0
            existing_banknifty.change_percent = 0.0
            existing_banknifty.is_special_time = True
            existing_banknifty.is_market_open = True
            print("✅ Updated existing BANKNIFTY record")
        else:
            # Insert BANKNIFTY
            banknifty_price = IndexPrice(
                index_name='BANKNIFTY',
                instrument_key='NSE_INDEX|Nifty Bank',
                ltp=59957.80,
                day_open=59957.80,  # At 9:15 AM, this is the opening price
                close_price=None,
                trend='neutral',
                change=0.0,
                change_percent=0.0,
                price_time=price_time,
                is_market_open=True,
                is_special_time=True  # 9:15 AM is a special time
            )
            db.add(banknifty_price)
            print(f"✅ Added BANKNIFTY: ₹59957.80 at {price_time.strftime('%Y-%m-%d %H:%M:%S IST')}")
        
        # Commit changes
        db.commit()
        print("\n✅ Successfully inserted/updated index prices for January 6th, 2026 at 9:15 AM")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Error inserting index prices: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("Inserting Index Prices for January 6th, 2026 at 9:15 AM")
    print("=" * 60)
    print("NIFTY50: ₹26189.70")
    print("BANKNIFTY: ₹59957.80")
    print("=" * 60)
    print()
    
    success = insert_index_prices()
    
    if success:
        print("\n✅ Script completed successfully")
        sys.exit(0)
    else:
        print("\n❌ Script failed")
        sys.exit(1)

