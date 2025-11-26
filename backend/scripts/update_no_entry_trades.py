#!/usr/bin/env python3
"""
Manual script to update VWAP slope, candle size, and other hourly data
for all 'no_entry' stocks from today
"""
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Change to project root directory
os.chdir(project_root)

# Import using absolute paths
from backend.database import SessionLocal
from backend.services.vwap_updater import update_vwap_for_all_open_positions
import asyncio
import pytz
from datetime import datetime

async def main():
    """Run the update for all no_entry trades from today"""
    print("=" * 80)
    print("🔄 MANUAL UPDATE: VWAP Slope & Candle Size for Today's 'no_entry' Stocks")
    print("=" * 80)
    print()
    
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    print(f"⏰ Current Time: {now.strftime('%Y-%m-%d %H:%M:%S IST')}")
    print()
    
    # Run the update function
    try:
        await update_vwap_for_all_open_positions()
        print()
        print("=" * 80)
        print("✅ UPDATE COMPLETE")
        print("=" * 80)
    except Exception as e:
        print()
        print("=" * 80)
        print(f"❌ ERROR: {str(e)}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())

