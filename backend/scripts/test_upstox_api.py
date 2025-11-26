#!/usr/bin/env python3
"""
Test Upstox API connectivity and token validity
"""

import sys
import os

# Add parent directory to path
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(script_dir))
sys.path.insert(0, parent_dir)

from backend.services.upstox_service import upstox_service
from backend.services.token_manager import get_token_info, load_upstox_token
from datetime import datetime
import pytz

def main():
    print("=" * 80)
    print("UPSTOX API CONNECTIVITY TEST")
    print("=" * 80)
    print()
    
    # Check token info
    print("1. TOKEN STATUS:")
    print("-" * 80)
    token_info = get_token_info()
    print(f"  Has Token: {token_info.get('has_token')}")
    print(f"  Source: {token_info.get('source')}")
    print(f"  Updated At: {token_info.get('updated_at')}")
    expires_at = token_info.get('expires_at')
    if expires_at:
        exp_dt = datetime.fromtimestamp(expires_at)
        now = datetime.now()
        is_expired = now.timestamp() > expires_at
        print(f"  Expires At: {exp_dt.strftime('%Y-%m-%d %H:%M:%S IST')}")
        print(f"  Is Expired: {'✅ YES' if is_expired else '❌ NO'}")
        if not is_expired:
            hours_left = (exp_dt - now).total_seconds() / 3600
            print(f"  Time Until Expiry: {hours_left:.2f} hours")
    else:
        print(f"  Expires At: Not stored (will decode from JWT)")
    print()
    
    # Check if token is loaded in service
    print("2. SERVICE TOKEN STATUS:")
    print("-" * 80)
    has_token = hasattr(upstox_service, 'access_token') and upstox_service.access_token
    print(f"  Token Loaded in Service: {'✅ YES' if has_token else '❌ NO'}")
    if has_token:
        token_preview = upstox_service.access_token[:30] + "..." if len(upstox_service.access_token) > 30 else upstox_service.access_token
        print(f"  Token Preview: {token_preview}")
    print()
    
    # Test 1: User Profile Endpoint
    print("3. TEST 1: User Profile Endpoint (/v2/user/profile)")
    print("-" * 80)
    try:
        import requests
        test_url = "https://api.upstox.com/v2/user/profile"
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {upstox_service.access_token}"
        }
        response = requests.get(test_url, headers=headers, timeout=5)
        print(f"  Status Code: {response.status_code}")
        if response.status_code == 200:
            print("  ✅ User Profile Endpoint: WORKING")
        elif response.status_code == 401:
            print("  ❌ User Profile Endpoint: TOKEN EXPIRED (401 Unauthorized)")
        else:
            print(f"  ⚠️ User Profile Endpoint: FAILED ({response.status_code})")
            print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
    print()
    
    # Test 2: Market Data Endpoint (NIFTY 50)
    print("4. TEST 2: Market Data Endpoint (/v2/market-quote/quotes)")
    print("-" * 80)
    try:
        test_url = "https://api.upstox.com/v2/market-quote/quotes"
        test_params = {"instrument_key": "NSE_INDEX|Nifty 50"}
        response = requests.get(test_url, headers=headers, params=test_params, timeout=5)
        print(f"  Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                print("  ✅ Market Data Endpoint: WORKING")
                nifty_data = data.get('data', {}).get('NSE_INDEX|Nifty 50', {})
                if nifty_data:
                    ltp = nifty_data.get('last_price', 'N/A')
                    print(f"  NIFTY 50 LTP: ₹{ltp}")
            else:
                print(f"  ⚠️ Market Data Endpoint: API returned error")
                print(f"  Response: {json.dumps(data, indent=2)[:300]}")
        elif response.status_code == 401:
            print("  ❌ Market Data Endpoint: TOKEN EXPIRED (401 Unauthorized)")
        else:
            print(f"  ⚠️ Market Data Endpoint: FAILED ({response.status_code})")
            print(f"  Response: {response.text[:200]}")
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
    print()
    
    # Test 3: Check Index Trends (uses upstox_service)
    print("5. TEST 3: Check Index Trends (via upstox_service)")
    print("-" * 80)
    try:
        result = upstox_service.check_index_trends()
        if result and result.get('nifty_trend'):
            print("  ✅ Index Trends Check: WORKING")
            print(f"  NIFTY Trend: {result.get('nifty_trend')}")
            print(f"  BANKNIFTY Trend: {result.get('banknifty_trend')}")
            nifty_data = result.get('nifty_data', {})
            banknifty_data = result.get('banknifty_data', {})
            if nifty_data:
                print(f"  NIFTY LTP: ₹{nifty_data.get('ltp', 'N/A')}")
            if banknifty_data:
                print(f"  BANKNIFTY LTP: ₹{banknifty_data.get('ltp', 'N/A')}")
        else:
            print("  ❌ Index Trends Check: FAILED")
            print(f"  Result: {result}")
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    print()
    
    # Test 4: Get Stock LTP and VWAP
    print("6. TEST 4: Get Stock LTP and VWAP (RELIANCE)")
    print("-" * 80)
    try:
        result = upstox_service.get_stock_ltp_and_vwap("RELIANCE")
        if result and result.get('ltp') and result.get('ltp') > 0:
            print("  ✅ Stock Data Fetch: WORKING")
            print(f"  RELIANCE LTP: ₹{result.get('ltp', 0):.2f}")
            print(f"  RELIANCE VWAP: ₹{result.get('vwap', 0):.2f}")
        else:
            print("  ❌ Stock Data Fetch: FAILED")
            print(f"  Result: {result}")
    except Exception as e:
        print(f"  ❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    print()
    
    print("=" * 80)
    print("SUMMARY:")
    print("=" * 80)
    print("If all tests show ✅, Upstox API is working properly.")
    print("If any test shows ❌ TOKEN EXPIRED, refresh the token via OAuth.")
    print("=" * 80)

if __name__ == "__main__":
    import json
    import requests
    main()

