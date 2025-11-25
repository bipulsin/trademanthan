#!/usr/bin/env python3
"""
Script to replay a Chartink webhook payload
Useful for testing or recovering missed alerts

Usage:
    python3 replay_webhook.py --type bullish --stocks "STOCK1,STOCK2" --prices "100.0,200.0" --time "10:15 am"
    python3 replay_webhook.py --file webhook_payload.json
"""

import argparse
import json
import sys
import os
import requests
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def create_webhook_payload(stocks, prices, time_str, scan_name="Bullish Breakout", alert_type="bullish"):
    """Create a webhook payload from command line arguments"""
    return {
        "stocks": stocks,
        "trigger_prices": prices,
        "triggered_at": time_str,
        "scan_name": scan_name,
        "scan_url": f"{alert_type.lower()}-breakout",
        "alert_name": f"Alert for {scan_name}"
    }

def replay_webhook(payload, endpoint_url):
    """Send webhook payload to backend endpoint"""
    print(f"\n📤 Sending webhook to: {endpoint_url}")
    print(f"📦 Payload: {json.dumps(payload, indent=2)}\n")
    
    try:
        response = requests.post(
            endpoint_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        print(f"✅ Response Status: {response.status_code}")
        print(f"📄 Response Body: {json.dumps(response.json(), indent=2)}")
        
        if response.status_code in [200, 202]:
            print("\n✅ Webhook sent successfully!")
            return True
        else:
            print(f"\n❌ Webhook failed with status {response.status_code}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error sending webhook: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Replay Chartink webhook payload")
    
    # Input options
    parser.add_argument("--file", type=str, help="Path to JSON file containing webhook payload")
    parser.add_argument("--type", type=str, choices=["bullish", "bearish"], default="bullish", 
                       help="Alert type (bullish or bearish)")
    parser.add_argument("--stocks", type=str, help="Comma-separated list of stock names")
    parser.add_argument("--prices", type=str, help="Comma-separated list of trigger prices")
    parser.add_argument("--time", type=str, default=None, 
                       help="Alert time (e.g., '10:15 am'). Defaults to current time")
    parser.add_argument("--scan-name", type=str, default=None, help="Scan name")
    parser.add_argument("--url", type=str, default="http://localhost:8000", 
                       help="Backend URL (default: http://localhost:8000)")
    
    args = parser.parse_args()
    
    # Determine endpoint URL
    if args.type == "bullish":
        endpoint_url = f"{args.url}/scan/chartink-webhook-bullish"
        default_scan_name = "Bullish Breakout"
    else:
        endpoint_url = f"{args.url}/scan/chartink-webhook-bearish"
        default_scan_name = "Bearish Breakdown"
    
    scan_name = args.scan_name or default_scan_name
    
    # Get payload
    if args.file:
        # Load from file
        try:
            with open(args.file, 'r') as f:
                payload = json.load(f)
            print(f"📂 Loaded payload from: {args.file}")
        except Exception as e:
            print(f"❌ Error reading file: {str(e)}")
            return 1
    elif args.stocks and args.prices:
        # Create from command line arguments
        time_str = args.time or datetime.now().strftime("%I:%M %p").lower()
        payload = create_webhook_payload(
            stocks=args.stocks,
            prices=args.prices,
            time_str=time_str,
            scan_name=scan_name,
            alert_type=args.type
        )
    else:
        print("❌ Error: Must provide either --file or both --stocks and --prices")
        parser.print_help()
        return 1
    
    # Replay webhook
    success = replay_webhook(payload, endpoint_url)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())

