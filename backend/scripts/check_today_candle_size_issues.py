#!/usr/bin/env python3
"""Check today's trades with candle size issues"""
import sys
import os
from datetime import datetime
import pytz

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption

def main():
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist).date()
    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=ist)
    today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=ist)

    db = SessionLocal()
    try:
        # Get all today's records
        all_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today_start,
            IntradayStockOption.trade_date < today_end
        ).order_by(IntradayStockOption.alert_time).all()

        print("=" * 100)
        print(f"Today's Stock Alert Data Analysis - {today.strftime('%Y-%m-%d')}")
        print("=" * 100)
        print()

        # Count by no_entry_reason
        candle_size_no_entry = []
        other_no_entry = []
        entered = []

        for rec in all_records:
            if rec.no_entry_reason == "Candle size":
                candle_size_no_entry.append(rec)
            elif rec.no_entry_reason:
                other_no_entry.append(rec)
            elif rec.status in ['bought', 'sold']:
                entered.append(rec)

        print(f"📊 Summary:")
        print(f"   Total Records: {len(all_records)}")
        print(f"   ❌ No Entry - Candle size: {len(candle_size_no_entry)}")
        print(f"   ❌ No Entry - Other reasons: {len(other_no_entry)}")
        print(f"   ✅ Entered (bought/sold): {len(entered)}")
        print()

        if candle_size_no_entry:
            print("=" * 100)
            print("Records with 'Candle size' as no_entry_reason:")
            print("=" * 100)
            for rec in candle_size_no_entry[:20]:  # Show first 20
                alert_time_str = rec.alert_time.strftime('%H:%M:%S') if rec.alert_time else 'N/A'
                is_10_15 = rec.alert_time and rec.alert_time.hour == 10 and rec.alert_time.minute == 15
                print(f"  • {rec.stock_name} | Alert: {alert_time_str} | Status: {rec.status} | "
                      f"Candle Size Ratio: {rec.candle_size_ratio} | Status: {rec.candle_size_status} | "
                      f"Instrument Key: {'✅' if rec.instrument_key else '❌'} | "
                      f"Option Contract: {rec.option_contract or 'N/A'} | "
                      f"10:15 Alert: {'Yes' if is_10_15 else 'No'}")
            if len(candle_size_no_entry) > 20:
                print(f"  ... and {len(candle_size_no_entry) - 20} more")
            print()

        # Analyze why candle size failed
        print("=" * 100)
        print("Analysis of Candle Size Calculation Issues:")
        print("=" * 100)
        
        missing_instrument_key = sum(1 for r in candle_size_no_entry if not r.instrument_key)
        missing_option_contract = sum(1 for r in candle_size_no_entry if not r.option_contract)
        missing_ratio = sum(1 for r in candle_size_no_entry if r.candle_size_ratio is None)
        skipped_status = sum(1 for r in candle_size_no_entry if r.candle_size_status == "Skipped")
        pending_status = sum(1 for r in candle_size_no_entry if r.candle_size_status == "Pending")
        fail_status = sum(1 for r in candle_size_no_entry if r.candle_size_status == "Fail")
        
        print(f"   Missing Instrument Key: {missing_instrument_key}")
        print(f"   Missing Option Contract: {missing_option_contract}")
        print(f"   Missing Candle Size Ratio: {missing_ratio}")
        print(f"   Status = 'Skipped': {skipped_status}")
        print(f"   Status = 'Pending': {pending_status}")
        print(f"   Status = 'Fail': {fail_status}")
        print()

    finally:
        db.close()

if __name__ == "__main__":
    main()
