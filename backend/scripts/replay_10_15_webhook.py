"""
Script to replay the 10:15 AM bullish webhook that was lost due to stock ranker crash
Fetches current data for the stocks and saves with 10:15 AM timestamp
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

import requests
from datetime import datetime
import pytz

# The 22 stocks from 10:15 AM webhook (after filtering NIFTY/BANKNIFTY)
STOCKS_10_15_AM = [
    "ADANIENSOL", "ADANIPORTS", "ALKEM", "ANGELONE", "ASTRAL", "AUROPHARMA",
    "BSE", "CANBK", "HAL", "HCLTECH", "ICICIPRULI", "INDUSINDBK", "INFY",
    "KPITTECH", "LTIM", "MFSL", "NATIONALUM", "OIL", "PERSISTENT", "RELIANCE",
    "SUNPHARMA", "UPL"
]

# Webhook payload format
webhook_data = {
    "scan_name": "Bullish Intraday Stock Options",
    "scan_url": "https://chartink.com/scan",
    "alert_name": "Bullish Alert - 10:15 AM (Replayed)",
    "triggered_at": "2025-11-12T10:15:00+05:30",  # 10:15 AM IST
    "stocks": ",".join(STOCKS_10_15_AM),
    "trigger_prices": ",".join(["0"] * len(STOCKS_10_15_AM))  # Prices not available from logs
}

print("=" * 70)
print("🔄 REPLAYING 10:15 AM BULLISH WEBHOOK")
print("=" * 70)
print(f"\n📊 Stocks to process: {len(STOCKS_10_15_AM)}")
print(f"📅 Alert Time: 10:15 AM IST (2025-11-12)")
print(f"\nStock List:")
for i, stock in enumerate(STOCKS_10_15_AM, 1):
    print(f"  {i}. {stock}")

print("\n" + "=" * 70)
print("Sending webhook to backend...")
print("=" * 70)

try:
    # Send to local backend (scan router)
    response = requests.post(
        "http://localhost:8000/scan/chartink-webhook-bullish",
        json=webhook_data,
        timeout=60  # Give it time to process
    )
    
    if response.status_code == 200:
        result = response.json()
        print("\n✅ SUCCESS!")
        print(f"Status: {result.get('status')}")
        print(f"Message: {result.get('message')}")
        if 'stocks_processed' in result:
            print(f"Stocks Processed: {result.get('stocks_processed')}")
        if 'alert_type' in result:
            print(f"Alert Type: {result.get('alert_type')}")
    else:
        print(f"\n❌ ERROR: {response.status_code}")
        print(f"Response: {response.text}")
        
except Exception as e:
    print(f"\n❌ EXCEPTION: {str(e)}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 70)
print("Done! Check scan.html to verify the 10:15 AM alert now appears.")
print("=" * 70)

