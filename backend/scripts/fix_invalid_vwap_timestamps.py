#!/usr/bin/env python3
"""
Script to fix invalid stock_vwap_previous_hour_time timestamps in the database
Invalid timestamps are those set to 1970-01-01 (epoch) or before 2020-01-01

This script:
1. Finds all trades with invalid stock_vwap_previous_hour_time
2. Calculates the correct previous hour time based on alert_time
3. Updates the database with correct timestamps
"""

import sys
import os

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, parent_dir)

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption
from datetime import datetime, timedelta
import pytz

def is_invalid_timestamp(timestamp):
    """Check if timestamp is invalid (epoch or before 2020)"""
    if timestamp is None:
        return True
    
    # Check if it's epoch (1970-01-01) or before 2020-01-01
    epoch_date = datetime(1970, 1, 1, tzinfo=pytz.UTC)
    min_valid_date = datetime(2020, 1, 1, tzinfo=pytz.UTC)
    
    # Convert to UTC for comparison
    if timestamp.tzinfo is None:
        timestamp_utc = pytz.UTC.localize(timestamp)
    else:
        timestamp_utc = timestamp.astimezone(pytz.UTC)
    
    return timestamp_utc.date() <= epoch_date.date() or timestamp_utc.date() < min_valid_date.date()

def calculate_previous_hour_time(alert_time):
    """Calculate the previous hour time based on alert time"""
    ist = pytz.timezone('Asia/Kolkata')
    
    # Ensure alert_time is timezone-aware
    if alert_time.tzinfo is None:
        alert_time = ist.localize(alert_time)
    elif alert_time.tzinfo != ist:
        alert_time = alert_time.astimezone(ist)
    
    # Round down to the nearest hour, then subtract 1 hour
    alert_hour = alert_time.replace(minute=0, second=0, microsecond=0)
    previous_hour = alert_hour - timedelta(hours=1)
    
    return previous_hour

def main():
    db = SessionLocal()
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist).replace(hour=0, minute=0, second=0, microsecond=0)
    
    print("=" * 80)
    print("FIXING INVALID stock_vwap_previous_hour_time TIMESTAMPS")
    print("=" * 80)
    print()
    
    # Find all trades with invalid timestamps
    all_trades = db.query(IntradayStockOption).filter(
        IntradayStockOption.trade_date >= today.replace(year=2024, month=1, day=1)  # Check from 2024 onwards
    ).all()
    
    invalid_trades = []
    for trade in all_trades:
        if trade.stock_vwap_previous_hour_time and is_invalid_timestamp(trade.stock_vwap_previous_hour_time):
            invalid_trades.append(trade)
    
    print(f"Found {len(invalid_trades)} trades with invalid stock_vwap_previous_hour_time")
    print()
    
    if len(invalid_trades) == 0:
        print("✅ No invalid timestamps found!")
        db.close()
        return
    
    # Show preview of trades to be fixed
    print("Preview of trades to be fixed:")
    print("-" * 80)
    for trade in invalid_trades[:10]:  # Show first 10
        print(f"  {trade.stock_name} | Alert: {trade.alert_time} | Invalid Time: {trade.stock_vwap_previous_hour_time}")
    if len(invalid_trades) > 10:
        print(f"  ... and {len(invalid_trades) - 10} more")
    print()
    
    # Ask for confirmation
    response = input("Do you want to fix these timestamps? (yes/no): ")
    if response.lower() not in ['yes', 'y']:
        print("❌ Operation cancelled")
        db.close()
        return
    
    # Fix timestamps
    fixed_count = 0
    error_count = 0
    
    for trade in invalid_trades:
        try:
            if not trade.alert_time:
                print(f"⚠️ Skipping {trade.stock_name} - no alert_time")
                continue
            
            # Calculate correct previous hour time
            correct_time = calculate_previous_hour_time(trade.alert_time)
            
            # Update the trade
            old_time = trade.stock_vwap_previous_hour_time
            trade.stock_vwap_previous_hour_time = correct_time
            
            print(f"✅ Fixed {trade.stock_name} | Alert: {trade.alert_time.strftime('%Y-%m-%d %H:%M')} | "
                  f"Old: {old_time.strftime('%Y-%m-%d %H:%M') if old_time else 'None'} | "
                  f"New: {correct_time.strftime('%Y-%m-%d %H:%M')}")
            
            fixed_count += 1
            
        except Exception as e:
            print(f"❌ Error fixing {trade.stock_name}: {str(e)}")
            error_count += 1
    
    # Commit changes
    if fixed_count > 0:
        try:
            db.commit()
            print()
            print("=" * 80)
            print(f"✅ Successfully fixed {fixed_count} timestamps")
            if error_count > 0:
                print(f"⚠️ {error_count} errors occurred")
            print("=" * 80)
        except Exception as e:
            db.rollback()
            print()
            print("=" * 80)
            print(f"❌ Error committing changes: {str(e)}")
            print("Changes rolled back")
            print("=" * 80)
    else:
        print()
        print("⚠️ No timestamps were fixed")
    
    db.close()

if __name__ == "__main__":
    main()

