#!/usr/bin/env python3
"""
Query historical market data for EXIDEIND for today
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from backend.database import SessionLocal
from backend.models.trading import HistoricalMarketData
from datetime import datetime
import pytz

def query_exideind_historical():
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow = today.replace(day=today.day + 1) if today.day < 28 else today.replace(month=today.month + 1, day=1)
    
    db = SessionLocal()
    try:
        records = db.query(HistoricalMarketData).filter(
            HistoricalMarketData.stock_name == 'EXIDEIND',
            HistoricalMarketData.scan_date >= today,
            HistoricalMarketData.scan_date < tomorrow
        ).order_by(HistoricalMarketData.scan_date.asc()).all()
        
        print(f'Found {len(records)} records for EXIDEIND today ({today.strftime("%Y-%m-%d")}):')
        print('=' * 120)
        
        if len(records) == 0:
            print('No records found.')
        else:
            for r in records:
                print(f'ID: {r.id}')
                print(f'  Stock: {r.stock_name}')
                print(f'  Scan Date: {r.scan_date.strftime("%Y-%m-%d %H:%M:%S")} ({r.scan_time})')
                print(f'  Stock LTP: {r.stock_ltp}')
                print(f'  Stock VWAP: {r.stock_vwap}')
                print(f'  Stock VWAP Previous Hour: {r.stock_vwap_previous_hour}')
                if r.stock_vwap_previous_hour_time:
                    print(f'  Stock VWAP Previous Hour Time: {r.stock_vwap_previous_hour_time.strftime("%Y-%m-%d %H:%M:%S")}')
                else:
                    print(f'  Stock VWAP Previous Hour Time: None')
                print(f'  Option Contract: {r.option_contract}')
                print(f'  Option Instrument Key: {r.option_instrument_key}')
                print(f'  Option LTP: {r.option_ltp}')
                print(f'  Option VWAP: {r.option_vwap}')
                print(f'  VWAP Slope Angle: {r.vwap_slope_angle}')
                print(f'  VWAP Slope Status: {r.vwap_slope_status}')
                print(f'  VWAP Slope Direction: {r.vwap_slope_direction}')
                if r.vwap_slope_time:
                    print(f'  VWAP Slope Time: {r.vwap_slope_time.strftime("%Y-%m-%d %H:%M:%S")}')
                else:
                    print(f'  VWAP Slope Time: None')
                print(f'  Created At: {r.created_at.strftime("%Y-%m-%d %H:%M:%S")}')
                print('-' * 120)
    finally:
        db.close()

if __name__ == "__main__":
    query_exideind_historical()

