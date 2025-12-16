#!/usr/bin/env python3
"""Check EXIDEIND trade details and exit conditions"""
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
            IntradayStockOption.stock_name == 'EXIDEIND'
        ).order_by(IntradayStockOption.alert_time).all()

        print("=" * 100)
        print(f"EXIDEIND Trade Details")
        print("=" * 100)
        print()

        for rec in records:
            print(f"ID: {rec.id}")
            print(f"Alert Time: {rec.alert_time}")
            print(f"Status: {rec.status}")
            print(f"Exit Reason: {rec.exit_reason}")
            print(f"Option Type: {rec.option_type}")
            print(f"Buy Price: {rec.buy_price}")
            print(f"Sell Price: {rec.sell_price}")
            print(f"Sell Time: {rec.sell_time}")
            print(f"PnL: {rec.pnl}")
            print(f"Stock LTP: {rec.stock_ltp}")
            print(f"Stock VWAP: {rec.stock_vwap}")
            print()

    finally:
        db.close()

if __name__ == "__main__":
    main()
