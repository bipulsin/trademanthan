#!/usr/bin/env python3
"""
Script to analyze webhook reception and processing failures
"""
import sys
import os
from datetime import datetime
import pytz

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from backend.database import SessionLocal
from backend.models.trading import IntradayStockOption

def analyze_webhook_reception():
    """Analyze webhook reception patterns"""
    ist = pytz.timezone('Asia/Kolkata')
    now = datetime.now(ist)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    db = SessionLocal()
    try:
        # Get all trades for today
        all_trades = db.query(IntradayStockOption).filter(
            IntradayStockOption.trade_date >= today
        ).order_by(IntradayStockOption.created_date_time.asc()).all()
        
        print(f"📊 Webhook Reception Analysis for {today.strftime('%Y-%m-%d')}")
        print("=" * 70)
        
        # Group by creation time to see when webhooks were received
        print(f"\n📥 Webhook Reception Timeline:")
        for trade in all_trades:
            if trade.created_date_time and trade.alert_time:
                created = trade.created_date_time
                alert_time = trade.alert_time
                time_diff = (created - alert_time).total_seconds() / 60  # minutes
                print(f"  {trade.stock_name} ({trade.alert_type}):")
                print(f"    Alert Time: {alert_time.strftime('%H:%M')}")
                print(f"    Received At: {created.strftime('%Y-%m-%d %H:%M:%S')} IST")
                print(f"    Time Diff: {time_diff:.1f} minutes")
        
        # Check for gaps
        expected_times = [
            (10, 15), (11, 15), (12, 15), (13, 15), (14, 15), (15, 15)
        ]
        
        print(f"\n🔍 Missing Webhook Times:")
        received_times = set()
        for trade in all_trades:
            if trade.alert_time:
                received_times.add((trade.alert_time.hour, trade.alert_time.minute))
        
        for hour, minute in expected_times:
            if (hour, minute) not in received_times:
                print(f"  ❌ {hour:02d}:{minute:02d} - No webhooks received")
            else:
                count = sum(1 for t in all_trades if t.alert_time and t.alert_time.hour == hour and t.alert_time.minute == minute)
                print(f"  ✅ {hour:02d}:{minute:02d} - {count} webhook(s) received")
        
    finally:
        db.close()

if __name__ == "__main__":
    analyze_webhook_reception()

