#!/usr/bin/env python3
"""
Script to check for missed bearish alerts in the database
and verify webhook processing
"""
import sys
import os
from datetime import datetime, timedelta
import pytz

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption

def check_missed_bearish_alerts():
    """Check for bearish alerts that should have been received today"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Expected alert times today
    expected_times = [
        (11, 15),  # 11:15 AM
        (12, 15),  # 12:15 PM
        (13, 15),  # 1:15 PM
        (14, 15),  # 2:15 PM
        (15, 15),  # 3:15 PM
    ]
    
    db = SessionLocal()
    try:
        # Get all bearish trades for today
        bearish_trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.alert_type == 'Bearish',
            IntradayStockOption.trade_date >= today
        ).all()
        
        print(f"📊 Bearish Alerts Analysis for {today.strftime('%Y-%m-%d')}")
        print("=" * 60)
        
        # Group by alert time
        alerts_by_time = {}
        for trade in bearish_trades:
            if trade.alert_time:
                hour = trade.alert_time.hour
                minute = trade.alert_time.minute
                time_key = (hour, minute)
                if time_key not in alerts_by_time:
                    alerts_by_time[time_key] = []
                alerts_by_time[time_key].append(trade)
        
        print(f"\n✅ Found {len(bearish_trades)} bearish trades today:")
        for trade in bearish_trades:
            alert_time_str = trade.alert_time.strftime('%H:%M') if trade.alert_time else 'N/A'
            print(f"  - {trade.stock_name} at {alert_time_str} (status: {trade.status})")
        
        print(f"\n🔍 Checking expected alert times:")
        for hour, minute in expected_times:
            time_key = (hour, minute)
            time_str = f"{hour:02d}:{minute:02d}"
            if time_key in alerts_by_time:
                stocks = [t.stock_name for t in alerts_by_time[time_key]]
                print(f"  ✅ {time_str} - Found {len(stocks)} stocks: {', '.join(stocks)}")
            else:
                print(f"  ❌ {time_str} - MISSING (no bearish alerts received)")
        
        # Check if any bearish alerts were received via auto-detect endpoint
        print(f"\n📋 All trades today (for comparison):")
        all_today = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today
        ).order_by(IntradayStockOption.alert_time.asc()).all()
        
        bullish_count = sum(1 for t in all_today if t.alert_type == 'Bullish')
        bearish_count = sum(1 for t in all_today if t.alert_type == 'Bearish')
        
        print(f"  Total trades: {len(all_today)}")
        print(f"  - Bullish: {bullish_count}")
        print(f"  - Bearish: {bearish_count}")
        
        if bearish_count == 0:
            print(f"\n⚠️  WARNING: No bearish alerts found for today!")
            print(f"   This suggests Chartink may not be sending bearish webhooks,")
            print(f"   or they are being sent to the wrong endpoint.")
            print(f"   Please verify Chartink webhook configuration:")
            print(f"   - Bearish alerts should go to: /scan/chartink-webhook-bearish")
            print(f"   - Or use /scan/chartink-webhook with 'bearish' in alert/scan name")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_missed_bearish_alerts()

