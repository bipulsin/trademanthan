#!/usr/bin/env python3
"""
Comprehensive script to check for missed alerts (both bullish and bearish)
and verify the data flow from database to API endpoint
"""
import sys
import os
from datetime import datetime, timedelta
import pytz

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption

def check_missed_alerts():
    """Check for missed alerts (both bullish and bearish)"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Expected alert times today
    expected_times = [
        (10, 15),  # 10:15 AM
        (11, 15),  # 11:15 AM
        (12, 15),  # 12:15 PM
        (13, 15),  # 1:15 PM
        (14, 15),  # 2:15 PM
        (15, 15),  # 3:15 PM
    ]
    
    db = SessionLocal()
    try:
        # Get all trades for today
        all_trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today
        ).order_by(IntradayStockOption.alert_time.asc()).all()
        
        print(f"📊 Complete Alert Analysis for {today.strftime('%Y-%m-%d')}")
        print("=" * 70)
        
        # Group by alert type and time
        bullish_by_time = {}
        bearish_by_time = {}
        
        for trade in all_trades:
            if trade.alert_time:
                hour = trade.alert_time.hour
                minute = trade.alert_time.minute
                time_key = (hour, minute)
                
                if trade.alert_type == 'Bullish':
                    if time_key not in bullish_by_time:
                        bullish_by_time[time_key] = []
                    bullish_by_time[time_key].append(trade)
                elif trade.alert_type == 'Bearish':
                    if time_key not in bearish_by_time:
                        bearish_by_time[time_key] = []
                    bearish_by_time[time_key].append(trade)
        
        bullish_count = sum(1 for t in all_trades if t.alert_type == 'Bullish')
        bearish_count = sum(1 for t in all_trades if t.alert_type == 'Bearish')
        
        print(f"\n📈 Summary:")
        print(f"  Total trades: {len(all_trades)}")
        print(f"  - Bullish: {bullish_count}")
        print(f"  - Bearish: {bearish_count}")
        
        print(f"\n🔍 Checking Bullish alerts by time:")
        for hour, minute in expected_times:
            time_key = (hour, minute)
            time_str = f"{hour:02d}:{minute:02d}"
            if time_key in bullish_by_time:
                stocks = [t.stock_name for t in bullish_by_time[time_key]]
                print(f"  ✅ {time_str} - Found {len(stocks)} stocks: {', '.join(stocks[:5])}{'...' if len(stocks) > 5 else ''}")
            else:
                print(f"  ❌ {time_str} - MISSING (no bullish alerts received)")
        
        print(f"\n🔍 Checking Bearish alerts by time:")
        for hour, minute in expected_times:
            time_key = (hour, minute)
            time_str = f"{hour:02d}:{minute:02d}"
            if time_key in bearish_by_time:
                stocks = [t.stock_name for t in bearish_by_time[time_key]]
                print(f"  ✅ {time_str} - Found {len(stocks)} stocks: {', '.join(stocks[:5])}{'...' if len(stocks) > 5 else ''}")
            else:
                print(f"  ❌ {time_str} - MISSING (no bearish alerts received)")
        
        # Check if data would be returned by /scan/latest endpoint
        print(f"\n🔍 Verifying /scan/latest endpoint data:")
        from datetime import timedelta
        current_hour = now.hour
        current_minute = now.minute
        
        if current_hour > 9 or (current_hour == 9 and current_minute >= 0):
            filter_date_start = today
            filter_date_end = today + timedelta(days=1)
            print(f"  Date filter: {filter_date_start.strftime('%Y-%m-%d')} to {filter_date_end.strftime('%Y-%m-%d')}")
        else:
            filter_date_start = today - timedelta(days=1)
            filter_date_end = today
            print(f"  Date filter: {filter_date_start.strftime('%Y-%m-%d')} to {filter_date_end.strftime('%Y-%m-%d')}")
        
        bullish_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bullish',
            IntradayStockOption.trade_date >= filter_date_start,
            IntradayStockOption.trade_date < filter_date_end
        ).count()
        
        bearish_records = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bearish',
            IntradayStockOption.trade_date >= filter_date_start,
            IntradayStockOption.trade_date < filter_date_end
        ).count()
        
        print(f"  Bullish records that would be returned: {bullish_records}")
        print(f"  Bearish records that would be returned: {bearish_records}")
        
        if bullish_count == 0 and bearish_count == 0:
            print(f"\n⚠️  CRITICAL: No alerts found for today!")
            print(f"   This suggests Chartink is not sending webhooks at all.")
        elif bullish_count > 0 and bearish_count == 0:
            print(f"\n⚠️  WARNING: Only bullish alerts found, no bearish alerts!")
            print(f"   Chartink may not be configured to send bearish webhooks.")
        elif bullish_count == 0 and bearish_count > 0:
            print(f"\n⚠️  WARNING: Only bearish alerts found, no bullish alerts!")
            print(f"   Chartink may not be configured to send bullish webhooks.")
        
        # List all missing times
        missing_bullish = []
        missing_bearish = []
        for hour, minute in expected_times:
            time_key = (hour, minute)
            time_str = f"{hour:02d}:{minute:02d}"
            if time_key not in bullish_by_time:
                missing_bullish.append(time_str)
            if time_key not in bearish_by_time:
                missing_bearish.append(time_str)
        
        if missing_bullish:
            print(f"\n❌ Missing Bullish alerts at: {', '.join(missing_bullish)}")
        if missing_bearish:
            print(f"\n❌ Missing Bearish alerts at: {', '.join(missing_bearish)}")
        
        if missing_bullish or missing_bearish:
            print(f"\n💡 Recommendation:")
            print(f"   1. Verify Chartink webhook configuration")
            print(f"   2. Check Chartink logs to see if webhooks were sent")
            print(f"   3. Verify webhook URLs are correct:")
            print(f"      - Bullish: https://trademanthan.in/scan/chartink-webhook-bullish")
            print(f"      - Bearish: https://trademanthan.in/scan/chartink-webhook-bearish")
            print(f"   4. Check backend logs: tail -f /tmp/uvicorn.log")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_missed_alerts()

