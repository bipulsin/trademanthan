#!/usr/bin/env python3
"""Debug today's 10:15 AM alerts: candle size status and related fields"""
import sys
import os
from datetime import datetime, timedelta
import pytz

# Ensure backend package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption


def main():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    today = now.date()

    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=ist)
    today_end = datetime.combine(today, datetime.max.time()).replace(tzinfo=ist)

    ten_fifteen_start = today_start.replace(hour=10, minute=15, second=0, microsecond=0)
    ten_fifteen_end = ten_fifteen_start + timedelta(minutes=1)

    print("=" * 80)
    print("Debug 10:15 AM alerts for today (IST)")
    print("Date:", today.strftime("%Y-%m-%d"))
    print("Window:", ten_fifteen_start, "to", ten_fifteen_end)
    print("=" * 80)

    db = SessionLocal()
    try:
        records = (
            db.query(IntradayStockOption)
            .filter(
                IntradayStockOption.alert_time >= ten_fifteen_start,
                IntradayStockOption.alert_time < ten_fifteen_end,
            )
            .order_by(IntradayStockOption.alert_time)
            .all()
        )

        if not records:
            print("No records found for 10:15 AM alerts today.")
            return

        for rec in records:
            print("-" * 80)
            print(f"ID: {rec.id} | Stock: {rec.stock_name} | Status: {rec.status}")
            print(f"Alert Time: {rec.alert_time} | Trade Date: {rec.trade_date}")
            print(f"Option Contract: {rec.option_contract} | Instrument Key: {rec.instrument_key}")
            print(f"VWAP Slope: angle={rec.vwap_slope_angle}, status={rec.vwap_slope_status}, direction={rec.vwap_slope_direction}, time={rec.vwap_slope_time}")
            print(f"Candle Size: ratio={rec.candle_size_ratio}, status={rec.candle_size_status}")
            print(f"No Entry Reason: {rec.no_entry_reason}")

        print("-" * 80)
        print(f"Total 10:15 alerts today: {len(records)}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
