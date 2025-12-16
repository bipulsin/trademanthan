#!/usr/bin/env python3
"""Debug today's 10:15 AM alerts using timezone-safe filtering"""
import sys
import os
from datetime import datetime
import pytz

# Ensure backend package is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import SessionLocal
from models.trading import IntradayStockOption


def main():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    today = now.date()

    print("=" * 80)
    print("Debug 10:15 AM alerts for today (IST) - using Python-side time filtering")
    print("Date:", today.strftime("%Y-%m-%d"))
    print("=" * 80)

    db = SessionLocal()
    try:
        # Get all today records and filter in Python by hour/minute in IST
        records = (
            db.query(IntradayStockOption)
            .filter(IntradayStockOption.trade_date >= datetime.combine(today, datetime.min.time()))
            .filter(IntradayStockOption.trade_date < datetime.combine(today, datetime.max.time()))
            .order_by(IntradayStockOption.alert_time)
            .all()
        )

        ten_fifteen_records = []
        for rec in records:
            if not rec.alert_time:
                continue
            alert_time = rec.alert_time
            # Normalize to IST
            if alert_time.tzinfo is None:
                alert_time = ist.localize(alert_time)
            else:
                alert_time = alert_time.astimezone(ist)

            if alert_time.hour == 10 and alert_time.minute == 15:
                ten_fifteen_records.append((rec, alert_time))

        if not ten_fifteen_records:
            print("No records found with alert_time at 10:15 IST for today.")
            return

        for rec, alert_time in ten_fifteen_records:
            print("-" * 80)
            print(f"ID: {rec.id} | Stock: {rec.stock_name} | Status: {rec.status}")
            print(f"Alert Time (IST): {alert_time} | Raw Alert Time: {rec.alert_time}")
            print(f"Trade Date: {rec.trade_date}")
            print(f"Option Contract: {rec.option_contract} | Instrument Key: {rec.instrument_key}")
            print(f"VWAP Slope: angle={rec.vwap_slope_angle}, status={rec.vwap_slope_status}, direction={rec.vwap_slope_direction}, time={rec.vwap_slope_time}")
            print(f"Candle Size: ratio={rec.candle_size_ratio}, status={rec.candle_size_status}")
            print(f"No Entry Reason: {rec.no_entry_reason}")

        print("-" * 80)
        print(f"Total 10:15 alerts today (by IST hour/min): {len(ten_fifteen_records)}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
