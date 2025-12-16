#!/usr/bin/env python3
"""Check VWAP slope details for 10:15 AM records"""
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
        records = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today_start,
            IntradayStockOption.trade_date < today_end,
            IntradayStockOption.alert_time >= today_start.replace(hour=10, minute=15),
            IntradayStockOption.alert_time < today_start.replace(hour=10, minute=16)
        ).all()

        print("=" * 100)
        print(f"10:15 AM Records VWAP Slope Details - {today.strftime('%Y-%m-%d')}")
        print("=" * 100)
        print()

        for rec in records:
            print(f"Stock: {rec.stock_name} | Status: {rec.status}")
            print(f"  VWAP Slope Angle: {rec.vwap_slope_angle}")
            print(f"  VWAP Slope Status: {rec.vwap_slope_status}")
            print(f"  VWAP Slope Direction: {rec.vwap_slope_direction}")
            print(f"  VWAP Slope Time: {rec.vwap_slope_time}")
            print(f"  Stock VWAP: {rec.stock_vwap}")
            print(f"  Stock VWAP Previous Hour: {rec.stock_vwap_previous_hour}")
            print(f"  Stock VWAP Previous Hour Time: {rec.stock_vwap_previous_hour_time}")
            print()

    finally:
        db.close()

if __name__ == "__main__":
    main()
