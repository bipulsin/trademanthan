#!/usr/bin/env python3
"""
Manual script to process webhook alerts for specific stocks at a given time.
This is useful for reprocessing missed webhooks or testing.

Usage:
    python3 backend/scripts/manual_process_webhook.py --stocks "PNBHOUSING,INDUSINDBK" --time "11:15 AM" --type bullish
"""

import sys
import os
import argparse
from datetime import datetime
import pytz

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

from backend.database import SessionLocal
from backend.routers.scan import process_webhook_data
import asyncio
import json


def parse_time(time_str: str, trading_date: datetime) -> datetime:
    """Parse time string like '11:15 AM' or '11:15' into datetime"""
    try:
        # Try parsing with AM/PM
        from dateutil import parser
        parsed = parser.parse(time_str, fuzzy=True)
        # Use the hour and minute from parsed time, but use trading_date as base
        return trading_date.replace(
            hour=parsed.hour,
            minute=parsed.minute,
            second=0,
            microsecond=0
        )
    except Exception as e:
        print(f"Error parsing time '{time_str}': {e}")
        raise


def create_webhook_payload(stocks: list, trigger_prices: list, triggered_at: datetime, alert_type: str = 'bullish') -> dict:
    """Create webhook payload in Chartink format"""
    
    # Format time as Chartink would (e.g., "11:15 am")
    time_str = triggered_at.strftime("%I:%M %p").lower()
    
    # Determine scan name based on type
    scan_name = "Bullish Breakout" if alert_type.lower() == 'bullish' else "Bearish Breakdown"
    alert_name = f"Alert for {scan_name}"
    
    # Convert stocks and prices to comma-separated strings
    stocks_str = ",".join(stocks)
    prices_str = ",".join([str(price) for price in trigger_prices])
    
    payload = {
        "stocks": stocks_str,
        "trigger_prices": prices_str,
        "triggered_at": time_str,
        "scan_name": scan_name,
        "scan_url": scan_name.lower().replace(" ", "-"),
        "alert_name": alert_name
    }
    
    return payload


async def main():
    parser = argparse.ArgumentParser(description='Manually process webhook alerts for specific stocks')
    parser.add_argument('--stocks', type=str, required=True, help='Comma-separated list of stock names (e.g., "PNBHOUSING,INDUSINDBK")')
    parser.add_argument('--time', type=str, default=None, help='Time in format "11:15 AM" or "11:15" (default: current time)')
    parser.add_argument('--type', type=str, choices=['bullish', 'bearish'], default='bullish', help='Alert type: bullish or bearish (default: bullish)')
    parser.add_argument('--trigger-prices', type=str, default=None, help='Comma-separated trigger prices (default: will fetch from API)')
    parser.add_argument('--date', type=str, default=None, help='Date in format YYYY-MM-DD (default: today)')
    
    args = parser.parse_args()
    
    # Parse stocks
    stock_list = [s.strip() for s in args.stocks.split(',')]
    print(f"📋 Stocks to process: {stock_list}")
    
    # Parse date
    ist = pytz.timezone('Asia/Kolkata')
    if args.date:
        trading_date = datetime.strptime(args.date, '%Y-%m-%d').replace(tzinfo=ist)
        trading_date = trading_date.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        now = datetime.now(ist)
        trading_date = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # Parse time
    if args.time:
        triggered_at = parse_time(args.time, trading_date)
    else:
        now = datetime.now(ist)
        triggered_at = now.replace(second=0, microsecond=0)
    
    print(f"📅 Trading date: {trading_date.strftime('%Y-%m-%d')}")
    print(f"⏰ Triggered at: {triggered_at.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"🏷️  Alert type: {args.type}")
    
    # Parse trigger prices if provided
    trigger_prices = []
    if args.trigger_prices:
        trigger_prices = [float(p.strip()) for p in args.trigger_prices.split(',')]
        if len(trigger_prices) != len(stock_list):
            print(f"⚠️ Warning: Number of trigger prices ({len(trigger_prices)}) doesn't match number of stocks ({len(stock_list)})")
            print(f"   Using provided prices for first {len(trigger_prices)} stocks, 0.0 for others")
            trigger_prices.extend([0.0] * (len(stock_list) - len(trigger_prices)))
    else:
        # Fetch current LTP from API (will be done in process_webhook_data)
        trigger_prices = [0.0] * len(stock_list)
        print(f"ℹ️  No trigger prices provided - will fetch from API during processing")
    
    # Create webhook payload
    payload = create_webhook_payload(stock_list, trigger_prices, triggered_at, args.type)
    print(f"\n📦 Webhook payload:")
    print(json.dumps(payload, indent=2))
    
    # Get database session
    db = SessionLocal()
    
    try:
        print(f"\n🔄 Processing webhook...")
        result = await process_webhook_data(payload, db, forced_type=args.type)
        
        print(f"\n✅ Processing complete!")
        if hasattr(result, 'body'):
            # JSONResponse object
            import json as json_module
            result_dict = json_module.loads(result.body.decode())
            print(json_module.dumps(result_dict, indent=2))
        else:
            print(result)
            
    except Exception as e:
        print(f"\n❌ Error processing webhook: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        db.close()
    
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

