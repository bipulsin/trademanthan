#!/usr/bin/env python3
"""Check VWAP slope data for today's trades"""
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
        all_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today_start,
            IntradayStockOption.trade_date < today_end
        ).order_by(IntradayStockOption.alert_time).all()

        print("=" * 100)
        print(f"VWAP Slope Data for Today's Trades - {today.strftime('%Y-%m-%d')}")
        print("=" * 100)
        print()

        for rec in all_records:
            alert_time_str = rec.alert_time.strftime('%H:%M:%S') if rec.alert_time else 'N/A'
            is_10_15 = rec.alert_time and rec.alert_time.hour == 10 and rec.alert_time.minute == 15
            
            print(f"Stock: {rec.stock_name} | Alert: {alert_time_str} | Status: {rec.status}")
            print(f"  10:15 Alert: {'Yes' if is_10_15 else 'No'}")
            print(f"  VWAP Slope Angle: {rec.vwap_slope_angle}")
            print(f"  VWAP Slope Status: {rec.vwap_slope_status}")
            print(f"  VWAP Slope Direction: {rec.vwap_slope_direction}")
            print(f"  VWAP Slope Time: {rec.vwap_slope_time}")
            print()

        # Summary
        with_vwap = sum(1 for r in all_records if r.vwap_slope_angle is not None)
        without_vwap = sum(1 for r in all_records if r.vwap_slope_angle is None)
        print(f"Summary: {with_vwap} with VWAP slope, {without_vwap} without VWAP slope")
        
    finally:
        db.close()

if __name__ == "__main__":
    main()
